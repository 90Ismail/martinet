// courier consumes delivery jobs from mq (our message queue) and performs
// the HTTP calls.
//
// For every job:
//  1. ask the breaker table (fed by pouls) whether the endpoint is healthy
//  2. POST the signed payload (or skip if the breaker is open)
//  3. append a DeliveryAttempt to the `attempts` log
//  4. route the job: done / next retry tier / dead letter queue
//  5. only then ack the mq message (its lease)
//
// Step 5 last makes delivery at-least-once: a courier killed between 2 and 5
// leaves the message unacked, its lease expires, mq redelivers it to another
// courier, and
// the receiver sees the same Martinet-Delivery-Id twice. That is the honest
// contract; "exactly once" over HTTP is not possible (DDIA ch. 8, 11).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	martinetv1 "github.com/90Ismail/martinet/gen/martinet/v1"
	mqv1 "github.com/90Ismail/martinet/gen/mq/v1"
	"github.com/90Ismail/martinet/internal/breaker"
	"github.com/90Ismail/martinet/internal/delivery"
	"github.com/90Ismail/martinet/internal/eventlog"
	"github.com/90Ismail/martinet/internal/platform"
	"github.com/90Ismail/martinet/internal/queue"
	"github.com/90Ismail/martinet/internal/retry"
	"github.com/90Ismail/martinet/internal/topics"
)

var (
	attempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "martinet_courier_attempts_total", Help: "Delivery attempts by outcome.",
	}, []string{"outcome"})
	routed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "martinet_courier_routed_total", Help: "Jobs routed after an attempt.",
	}, []string{"decision"})
	latency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "martinet_courier_http_seconds", Help: "Webhook HTTP latency.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
	})
	endToEnd = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "martinet_courier_end_to_end_seconds", Help: "Event received -> successful delivery.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 16),
	})
)

type courier struct {
	log      *slog.Logger
	worker   string
	broker   *queue.Client
	attempts eventlog.Publisher
	bq       eventlog.Publisher // optional: Pub/Sub topic that streams into BigQuery
	bqTopic  string
	sender   *delivery.Sender
	breakers *breaker.Table
	policy   retry.Policy
}

func (c *courier) process(ctx context.Context, m *mqv1.Message) {
	var job martinetv1.DeliveryJob
	if err := proto.Unmarshal(m.Body, &job); err != nil || job.Event == nil {
		c.log.Error("undecodable job, dead-lettering", "err", err)
		_ = c.broker.DeadLetter(ctx, m.Receipt, "undecodable job")
		return
	}
	host := delivery.Host(job.Url)

	var res delivery.Result
	switch c.breakers.Check(host) {
	case breaker.Skip:
		res = delivery.Result{Outcome: martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_CIRCUIT_OPEN, Err: "circuit open"}
	default:
		res = c.sender.Send(ctx, &job)
		latency.Observe(res.Latency.Seconds())
	}
	attempts.WithLabelValues(res.Outcome.String()).Inc()

	decision := c.policy.Next(res.Outcome, job.Attempt)
	rec := &martinetv1.DeliveryAttempt{
		DeliveryId: job.DeliveryId, EventId: job.Event.Id, TenantId: job.Event.TenantId,
		SubscriptionId: job.SubscriptionId, Endpoint: host, Attempt: job.Attempt, Outcome: res.Outcome,
		HttpStatus: int32(res.HTTPStatus), Error: res.Err, LatencyMs: res.Latency.Milliseconds(),
		AttemptedAtMs: time.Now().UnixMilli(), DeadLettered: decision == retry.DeadLetter, Worker: c.worker,
	}
	b, _ := proto.Marshal(rec)
	if err := c.attempts.Publish(ctx, topics.Attempts, host, b, nil); err != nil {
		// Without the attempt record, status and health would silently lie.
		c.log.Warn("attempt log unavailable, nacking", "delivery", job.DeliveryId, "err", err)
		_ = c.broker.Nack(ctx, m.Receipt, time.Second)
		return
	}

	if c.bq != nil {
		// Best effort analytics copy; Kafka stays the source of truth.
		if js, err := protojson.Marshal(rec); err == nil {
			_ = c.bq.Publish(ctx, c.bqTopic, "", js, nil)
		}
	}

	var err error
	switch decision {
	case retry.Done:
		endToEnd.Observe(time.Since(time.UnixMilli(job.Event.ReceivedAtMs)).Seconds())
		err = c.broker.Ack(ctx, m.Receipt)
	case retry.Retry:
		tier := c.policy.TierFor(job.Attempt)
		if res.Outcome != martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_CIRCUIT_OPEN {
			job.Attempt++ // skipped calls don't spend the budget
		}
		next, _ := proto.Marshal(&job)
		// Send the next attempt first, then ack this one: a crash in between
		// duplicates the job (fine, at-least-once) instead of losing it.
		if err = c.broker.Retry(ctx, job.DeliveryId, job.SubscriptionId, job.Attempt, tier.Delay, next); err == nil {
			err = c.broker.Ack(ctx, m.Receipt)
		}
	case retry.DeadLetter:
		reason := fmt.Sprintf("%s after %d attempts: %s", res.Outcome, job.Attempt, res.Err)
		err = c.broker.DeadLetter(ctx, m.Receipt, reason)
		c.log.Warn("dead-lettered", "delivery", job.DeliveryId, "reason", reason)
	}
	if err != nil {
		c.log.Error("routing failed; lease will expire and redeliver", "delivery", job.DeliveryId, "err", err)
		return
	}
	routed.WithLabelValues(decision.String()).Inc()
}

func main() {
	log := platform.Logger("courier")
	ctx, stop := platform.SignalContext()
	defer stop()

	policy := retry.DefaultPolicy()
	policy.MaxAttempts = uint32(platform.EnvInt("MAX_ATTEMPTS", int(policy.MaxAttempts)))
	broker, err := queue.Dial(platform.Env("MQ_ADDR", "localhost:7072"))
	if err != nil {
		log.Error("mq", "err", err)
		return
	}
	defer broker.Close()

	cfg := eventlog.FromEnv()
	attemptLog, err := eventlog.NewKafkaPublisher(cfg.Brokers)
	if err != nil {
		log.Error("attempt log", "err", err)
		return
	}
	defer attemptLog.Close()

	// Each courier pod reads the whole compacted health topic, so it uses
	// its own consumer group (pod name): broadcast, not load balancing.
	hostname, _ := os.Hostname()
	table := breaker.New(uint32(platform.EnvInt("HALF_OPEN_PROBE_EVERY", 10)))
	healthCons, err := eventlog.NewKafkaConsumer(cfg.Brokers, "courier-health-"+hostname, log, topics.EndpointHealth)
	if err != nil {
		log.Error("health consumer", "err", err)
		return
	}
	defer healthCons.Close()
	go func() {
		if err := table.Follow(ctx, healthCons, log); err != nil {
			log.Warn("health follower stopped", "err", err)
		}
	}()

	concurrency := platform.EnvInt("CONCURRENCY", 64)
	httpTimeout := platform.EnvDuration("HTTP_TIMEOUT", 10*time.Second)

	var bq eventlog.Publisher
	bqTopic := platform.Env("BQ_ATTEMPTS_TOPIC", "") // set on GCP, see terraform/envs/gcp/bigquery.tf
	if bqTopic != "" {
		if bq, err = eventlog.NewPubSubPublisher(ctx, cfg.ProjectID); err != nil {
			log.Error("bigquery export", "err", err)
			return
		}
		defer bq.Close()
	}

	c := &courier{
		log: log, worker: hostname, broker: broker, attempts: attemptLog,
		sender:   delivery.NewSender(httpTimeout),
		breakers: table, policy: policy, bq: bq, bqTopic: bqTopic,
	}
	platform.ServeMetrics(ctx, log, platform.Env("METRICS_ADDR", ":9100"), map[string]http.Handler{
		"/debug/breakers": http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			for host, st := range table.Snapshot() {
				fmt.Fprintf(w, "%s\t%s\n", host, st)
			}
		}),
	})
	log.Info("courier running", "concurrency", concurrency, "max_attempts", policy.MaxAttempts)
	// Each worker long-polls mq. The lease (visibility timeout) is longer
	// than the HTTP timeout, so a live worker always acks before it expires.
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				msgs, err := broker.Receive(ctx, 1, 3*httpTimeout, 5*time.Second)
				if err != nil {
					if ctx.Err() == nil {
						log.Warn("receive failed", "err", err)
						time.Sleep(time.Second)
					}
					continue
				}
				for _, m := range msgs {
					// Detach from ctx so SIGTERM mid-request finishes the job.
					c.process(context.WithoutCancel(ctx), m)
				}
			}
		}()
	}
	<-ctx.Done()
	wg.Wait()
	log.Info("courier drained")
}

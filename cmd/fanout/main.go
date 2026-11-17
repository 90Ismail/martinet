// fanout reads the `events` log, finds matching subscriptions, and enqueues
// one DeliveryJob per (event, subscription) on the message queue (cmd/mq).
//
// This is the hand-off between the two broker styles (DDIA ch. 11):
//
//	Kafka = durable, ordered, replayable log            (what happened)
//	mq    = our own competing-consumer work queue       (what to do next)
//
// Per-endpoint work cannot live on a Kafka partition: one slow endpoint
// would block every other tenant on that partition (head-of-line blocking).
// Individually-acked queue messages don't have that problem.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/proto"

	martinetv1 "github.com/90Ismail/martinet/gen/martinet/v1"
	"github.com/90Ismail/martinet/internal/eventlog"
	"github.com/90Ismail/martinet/internal/kv"
	"github.com/90Ismail/martinet/internal/platform"
	"github.com/90Ismail/martinet/internal/queue"
	"github.com/90Ismail/martinet/internal/topics"
)

var (
	eventsSeen = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "martinet_fanout_events_total", Help: "Events processed by result.",
	}, []string{"result"})
	jobsEnqueued = promauto.NewCounter(prometheus.CounterOpts{
		Name: "martinet_fanout_jobs_total", Help: "Delivery jobs enqueued.",
	})
)

// errPoison marks events that can never be processed (bad bytes). They go to
// events.dlq instead of blocking the partition forever.
var errPoison = errors.New("poison event")

// subCache memoizes subscription lookups per tenant for a short TTL.
// Trade-off: a new subscription can miss up to `ttl` of events. Fine for
// webhooks; a stricter system would version subscriptions in the log.
type subCache struct {
	mu   sync.Mutex
	ttl  time.Duration
	data map[string]cacheEntry
	kv   *kv.Client
}

type cacheEntry struct {
	subs []*martinetv1.Subscription
	at   time.Time
}

func (c *subCache) get(ctx context.Context, tenant string) ([]*martinetv1.Subscription, error) {
	c.mu.Lock()
	e, ok := c.data[tenant]
	c.mu.Unlock()
	if ok && time.Since(e.at) < c.ttl {
		return e.subs, nil
	}
	kvs, err := c.kv.ScanPrefix(ctx, kv.SubscriptionPrefix(tenant), 0)
	if err != nil {
		return nil, err
	}
	subs := make([]*martinetv1.Subscription, 0, len(kvs))
	for _, p := range kvs {
		var s martinetv1.Subscription
		if proto.Unmarshal(p.Value, &s) == nil {
			subs = append(subs, &s)
		}
	}
	c.mu.Lock()
	c.data[tenant] = cacheEntry{subs: subs, at: time.Now()}
	c.mu.Unlock()
	return subs, nil
}

// Matches reports whether a subscription wants an event type. Patterns:
// exact ("order.created"), prefix wildcard ("order.*"), or "*" / empty = all.
func Matches(patterns []string, eventType string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		switch {
		case p == "*" || p == eventType:
			return true
		case strings.HasSuffix(p, ".*") && strings.HasPrefix(eventType, strings.TrimSuffix(p, "*")):
			return true
		}
	}
	return false
}

type fanout struct {
	log    *slog.Logger
	subs   *subCache
	broker *queue.Client
	dlq    eventlog.Publisher
}

func (f *fanout) handle(ctx context.Context, r eventlog.Record) error {
	var ev martinetv1.Event
	if err := proto.Unmarshal(r.Value, &ev); err != nil || ev.Id == "" || ev.TenantId == "" {
		return f.poison(ctx, r, fmt.Errorf("%w: %v", errPoison, err))
	}
	subs, err := f.subs.get(ctx, ev.TenantId)
	if err != nil {
		return err // transient: carnet down -> retry in place, don't commit
	}
	n := 0
	for _, s := range subs {
		if s.Paused || !Matches(s.EventTypes, ev.Type) {
			continue
		}
		job := &martinetv1.DeliveryJob{
			// Deterministic id: if fanout crashes after enqueueing but
			// before committing the offset, the replay produces the SAME
			// delivery ids, which receivers use to dedupe.
			DeliveryId:      kv.DeliveryID(ev.Id, s.Id),
			Event:           &ev,
			SubscriptionId:  s.Id,
			Url:             s.Url,
			Secret:          s.Secret,
			Attempt:         1,
			FirstEnqueuedMs: time.Now().UnixMilli(),
		}
		b, _ := proto.Marshal(job)
		if err := f.broker.Enqueue(ctx, job.DeliveryId, s.Id, 1, b); err != nil {
			return fmt.Errorf("enqueue %s: %w", job.DeliveryId, err)
		}
		n++
	}
	jobsEnqueued.Add(float64(n))
	eventsSeen.WithLabelValues("ok").Inc()
	f.log.Debug("fanned out", "event", ev.Id, "tenant", ev.TenantId, "jobs", n)
	return nil
}

func (f *fanout) poison(ctx context.Context, r eventlog.Record, cause error) error {
	eventsSeen.WithLabelValues("poison").Inc()
	f.log.Warn("poison event -> events.dlq", "partition", r.Partition, "offset", r.Offset, "err", cause)
	return f.dlq.Publish(ctx, topics.EventsDLQ, r.Key, r.Value, map[string]string{
		"x-error":            cause.Error(),
		"x-source-partition": fmt.Sprint(r.Partition),
		"x-source-offset":    fmt.Sprint(r.Offset),
	})
}

func main() {
	log := platform.Logger("fanout")
	ctx, stop := platform.SignalContext()
	defer stop()

	store, err := kv.Dial(platform.Env("CARNET_ADDR", "localhost:7070"))
	if err != nil {
		log.Error("carnet", "err", err)
		return
	}
	defer store.Close()
	broker, err := queue.Dial(platform.Env("MQ_ADDR", "localhost:7072"))
	if err != nil {
		log.Error("mq", "err", err)
		return
	}
	defer broker.Close()

	cfg := eventlog.FromEnv()
	dlq, err := eventlog.NewPublisher(ctx, cfg, log)
	if err != nil {
		log.Error("dlq publisher", "err", err)
		return
	}
	defer dlq.Close()
	cons, err := eventlog.NewConsumer(ctx, cfg, log, platform.Env("FANOUT_GROUP", "fanout"), topics.Events)
	if err != nil {
		log.Error("consumer", "err", err)
		return
	}
	defer cons.Close()

	platform.ServeMetrics(ctx, log, platform.Env("METRICS_ADDR", ":9100"), nil)
	f := &fanout{
		log:    log,
		subs:   &subCache{ttl: platform.EnvDuration("SUB_CACHE_TTL", 5*time.Second), data: map[string]cacheEntry{}, kv: store},
		broker: broker,
		dlq:    dlq,
	}
	log.Info("fanout running", "backend", cfg.Backend)
	if err := cons.Run(ctx, f.handle); err != nil {
		log.Error("consumer stopped", "err", err)
	}
}

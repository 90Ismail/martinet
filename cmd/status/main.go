// status is the read side (CQRS, DDIA ch. 11-12):
//
//   - a projector consumes the `attempts` log and materializes it into carnet
//     (att/<delivery_id>/<attempt>), a derived view that can be thrown away
//     and rebuilt by replaying the log from offset 0;
//   - martinet.v1.Status answers "what happened to delivery X / event Y?";
//   - martinet.v1.Admin lists and redrives the mq dead letter queue.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	martinetv1 "github.com/90Ismail/martinet/gen/martinet/v1"
	"github.com/90Ismail/martinet/internal/eventlog"
	"github.com/90Ismail/martinet/internal/kv"
	"github.com/90Ismail/martinet/internal/platform"
	"github.com/90Ismail/martinet/internal/queue"
	"github.com/90Ismail/martinet/internal/topics"
)

var projected = promauto.NewCounter(prometheus.CounterOpts{
	Name: "martinet_status_projected_total", Help: "Attempt records materialized into carnet.",
})

// ---- projector

func project(store *kv.Client) eventlog.Handler {
	return func(ctx context.Context, r eventlog.Record) error {
		var a martinetv1.DeliveryAttempt
		if err := proto.Unmarshal(r.Value, &a); err != nil || a.DeliveryId == "" {
			return nil // poison: skip
		}
		// Idempotent: the key is fully determined by the record, so replays
		// and redeliveries overwrite with identical bytes.
		return store.Put(ctx, kv.AttemptKey(a.DeliveryId, a.Attempt), r.Value, false)
	}
}

// ---- Status service

type statusServer struct {
	martinetv1.UnimplementedStatusServer
	kv *kv.Client
}

func summarize(id string, kvs []kv.KV) *martinetv1.DeliveryStatus {
	st := &martinetv1.DeliveryStatus{DeliveryId: id}
	for _, p := range kvs {
		var a martinetv1.DeliveryAttempt
		if proto.Unmarshal(p.Value, &a) != nil {
			continue
		}
		st.Attempts = append(st.Attempts, &a)
		st.LastOutcome = a.Outcome
		st.DeadLettered = st.DeadLettered || a.DeadLettered
	}
	return st
}

func (s *statusServer) GetDelivery(ctx context.Context, req *martinetv1.GetDeliveryRequest) (*martinetv1.DeliveryStatus, error) {
	kvs, err := s.kv.ScanPrefix(ctx, kv.AttemptPrefix(req.DeliveryId), 0)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "store: %v", err)
	}
	if len(kvs) == 0 {
		return nil, status.Errorf(codes.NotFound, "no attempts recorded for %s (yet)", req.DeliveryId)
	}
	return summarize(req.DeliveryId, kvs), nil
}

func (s *statusServer) ListEventDeliveries(ctx context.Context, req *martinetv1.ListEventDeliveriesRequest) (*martinetv1.ListEventDeliveriesResponse, error) {
	kvs, err := s.kv.ScanPrefix(ctx, kv.EventAttemptsPrefix(req.EventId), 0)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "store: %v", err)
	}
	// One range scan returns every attempt of every delivery of the event,
	// already grouped because keys sort by delivery id then attempt.
	out := &martinetv1.ListEventDeliveriesResponse{}
	var cur string
	var group []kv.KV
	flush := func() {
		if cur != "" {
			out.Deliveries = append(out.Deliveries, summarize(cur, group))
		}
	}
	for _, p := range kvs {
		id := strings.TrimPrefix(p.Key, "att/")
		id = id[:strings.LastIndexByte(id, '/')]
		if id != cur {
			flush()
			cur, group = id, nil
		}
		group = append(group, p)
	}
	flush()
	return out, nil
}

// ---- Admin service (DLQ)

type adminServer struct {
	martinetv1.UnimplementedAdminServer
	q   *queue.Client
	log *slog.Logger
}

func (a *adminServer) ListDeadLetters(ctx context.Context, req *martinetv1.ListDeadLettersRequest) (*martinetv1.ListDeadLettersResponse, error) {
	limit := int(req.Limit)
	if limit == 0 {
		limit = 50
	}
	msgs, err := a.q.PeekDLQ(ctx, limit)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "mq: %v", err)
	}
	depth, _ := a.q.Depth(ctx, queue.DLQ)
	out := &martinetv1.ListDeadLettersResponse{QueueDepth: uint32(depth)}
	for _, m := range msgs {
		var job martinetv1.DeliveryJob
		if proto.Unmarshal(m.Body, &job) != nil {
			continue
		}
		job.Secret = ""
		at, _ := strconv.ParseInt(m.Attributes[queue.AttrDeadAt], 10, 64)
		out.Items = append(out.Items, &martinetv1.DeadLetter{Job: &job, Reason: m.Attributes[queue.AttrReason], DeadLetteredAtMs: at})
	}
	return out, nil
}

func (a *adminServer) ReplayDeadLetters(ctx context.Context, req *martinetv1.ReplayDeadLettersRequest) (*martinetv1.ReplayDeadLettersResponse, error) {
	key, val := "", ""
	switch {
	case req.DeliveryId != "":
		key, val = queue.AttrDeliveryID, req.DeliveryId
	case req.SubscriptionId != "":
		key, val = queue.AttrSubscriptionID, req.SubscriptionId
	}
	n, err := a.q.Redrive(ctx, int(req.Limit), key, val)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "mq: %v", err)
	}
	a.log.Info("dlq redrive", "moved", n)
	return &martinetv1.ReplayDeadLettersResponse{Replayed: uint32(n)}, nil
}

func main() {
	log := platform.Logger("status")
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
	cons, err := eventlog.NewKafkaConsumer(cfg.Brokers, platform.Env("STATUS_GROUP", "status-projector"), log, topics.Attempts)
	if err != nil {
		log.Error("consumer", "err", err)
		return
	}
	defer cons.Close()

	gs, hs := platform.GRPCServer()
	martinetv1.RegisterStatusServer(gs, &statusServer{kv: store})
	martinetv1.RegisterAdminServer(gs, &adminServer{q: broker, log: log})
	if err := platform.ServeGRPC(ctx, log, platform.Env("GRPC_ADDR", ":9091"), gs, hs); err != nil {
		log.Error("grpc", "err", err)
		return
	}
	platform.ServeMetrics(ctx, log, platform.Env("METRICS_ADDR", ":9100"), nil)

	log.Info("status running")
	if err := cons.Run(ctx, func(ctx context.Context, r eventlog.Record) error {
		if err := project(store)(ctx, r); err != nil {
			return fmt.Errorf("project: %w", err)
		}
		projected.Inc()
		return nil
	}); err != nil {
		log.Error("projector stopped", "err", err)
	}
}

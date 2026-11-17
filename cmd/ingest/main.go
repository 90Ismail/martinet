// ingest accepts events and subscriptions over gRPC (martinet.v1.Ingest)
// and HTTP/JSON, deduplicates by idempotency key, and appends events to the
// `events` log keyed by tenant (so each tenant's events stay ordered).
package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	martinetv1 "github.com/90Ismail/martinet/gen/martinet/v1"
	"github.com/90Ismail/martinet/internal/eventlog"
	"github.com/90Ismail/martinet/internal/ids"
	"github.com/90Ismail/martinet/internal/kv"
	"github.com/90Ismail/martinet/internal/platform"
	"github.com/90Ismail/martinet/internal/signing"
	"github.com/90Ismail/martinet/internal/topics"
)

var (
	published = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "martinet_ingest_events_total", Help: "Events accepted, by result.",
	}, []string{"result"})
	publishLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "martinet_ingest_publish_seconds", Help: "Latency of appending to the event log.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
	})
)

type server struct {
	martinetv1.UnimplementedIngestServer
	log *slog.Logger
	kv  *kv.Client
	pub eventlog.Publisher
}

const maxPayload = 256 << 10

func (s *server) PublishEvent(ctx context.Context, req *martinetv1.PublishEventRequest) (*martinetv1.PublishEventResponse, error) {
	if req.TenantId == "" || req.Type == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id and type are required")
	}
	if len(req.Payload) > maxPayload {
		return nil, status.Errorf(codes.InvalidArgument, "payload exceeds %d bytes", maxPayload)
	}
	if req.Payload != nil && !json.Valid(req.Payload) {
		return nil, status.Error(codes.InvalidArgument, "payload must be JSON")
	}
	eventID := ids.New("evt")

	// Idempotency (DDIA ch. 11 "exactly-once" = at-least-once + dedupe):
	// the first request with a key claims it atomically in carnet; retries
	// with the same key get the original event id back.
	var idemKey string
	if req.IdempotencyKey != "" {
		idemKey = kv.IdempotencyKey(req.TenantId, req.IdempotencyKey)
		written, existing, err := s.kv.PutIfAbsent(ctx, idemKey, []byte(eventID))
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "idempotency store: %v", err)
		}
		if !written {
			published.WithLabelValues("duplicate").Inc()
			return &martinetv1.PublishEventResponse{EventId: string(existing), Duplicate: true}, nil
		}
	}

	now := time.Now().UnixMilli()
	ev := &martinetv1.Event{
		Id: eventID, TenantId: req.TenantId, Type: req.Type, Payload: req.Payload,
		OccurredAtMs: req.OccurredAtMs, ReceivedAtMs: now, IdempotencyKey: req.IdempotencyKey,
	}
	if ev.OccurredAtMs == 0 {
		ev.OccurredAtMs = now
	}
	b, err := proto.Marshal(ev)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	start := time.Now()
	if err := s.pub.Publish(ctx, topics.Events, req.TenantId, b, map[string]string{"type": req.Type}); err != nil {
		// Release the key so the client's retry can succeed; otherwise it
		// would be told "duplicate" for an event that never reached the log.
		if idemKey != "" {
			_ = s.kv.Delete(context.WithoutCancel(ctx), idemKey)
		}
		published.WithLabelValues("error").Inc()
		return nil, status.Errorf(codes.Unavailable, "event log: %v", err)
	}
	publishLatency.Observe(time.Since(start).Seconds())
	published.WithLabelValues("ok").Inc()
	return &martinetv1.PublishEventResponse{EventId: eventID}, nil
}

func (s *server) CreateSubscription(ctx context.Context, req *martinetv1.CreateSubscriptionRequest) (*martinetv1.Subscription, error) {
	if req.TenantId == "" || !(strings.HasPrefix(req.Url, "http://") || strings.HasPrefix(req.Url, "https://")) {
		return nil, status.Error(codes.InvalidArgument, "tenant_id and an http(s) url are required")
	}
	sub := &martinetv1.Subscription{
		Id: ids.New("sub"), TenantId: req.TenantId, Url: req.Url, EventTypes: req.EventTypes,
		Secret: signing.NewSecret(), CreatedAtMs: time.Now().UnixMilli(),
	}
	b, _ := proto.Marshal(sub)
	if err := s.kv.Put(ctx, kv.SubscriptionKey(sub.TenantId, sub.Id), b, true); err != nil {
		return nil, status.Errorf(codes.Unavailable, "store: %v", err)
	}
	s.log.Info("subscription created", "tenant", sub.TenantId, "id", sub.Id, "url", sub.Url)
	return sub, nil
}

func (s *server) ListSubscriptions(ctx context.Context, req *martinetv1.ListSubscriptionsRequest) (*martinetv1.ListSubscriptionsResponse, error) {
	kvs, err := s.kv.ScanPrefix(ctx, kv.SubscriptionPrefix(req.TenantId), 1000)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "store: %v", err)
	}
	out := &martinetv1.ListSubscriptionsResponse{}
	for _, p := range kvs {
		var sub martinetv1.Subscription
		if proto.Unmarshal(p.Value, &sub) == nil {
			sub.Secret = "" // never echo secrets in list views
			out.Subscriptions = append(out.Subscriptions, &sub)
		}
	}
	return out, nil
}

// ---- HTTP/JSON facade: POST /v1/events, POST /v1/subscriptions

type httpEvent struct {
	TenantID     string          `json:"tenant_id"`
	Type         string          `json:"type"`
	Payload      json.RawMessage `json:"payload"`
	OccurredAtMs int64           `json:"occurred_at_ms"`
}

func (s *server) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", func(w http.ResponseWriter, r *http.Request) {
		var in httpEvent
		if err := json.NewDecoder(io.LimitReader(r.Body, maxPayload+4096)).Decode(&in); err != nil {
			httpError(w, status.Error(codes.InvalidArgument, err.Error()))
			return
		}
		resp, err := s.PublishEvent(r.Context(), &martinetv1.PublishEventRequest{
			TenantId: in.TenantID, Type: in.Type, Payload: in.Payload, OccurredAtMs: in.OccurredAtMs,
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
		})
		if err != nil {
			httpError(w, err)
			return
		}
		code := http.StatusAccepted
		if resp.Duplicate {
			code = http.StatusOK
		}
		writeJSON(w, code, map[string]any{"event_id": resp.EventId, "duplicate": resp.Duplicate})
	})
	mux.HandleFunc("POST /v1/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		var in martinetv1.CreateSubscriptionRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			httpError(w, status.Error(codes.InvalidArgument, err.Error()))
			return
		}
		sub, err := s.CreateSubscription(r.Context(), &in)
		if err != nil {
			httpError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, sub)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, err error) {
	st, _ := status.FromError(err)
	code := map[codes.Code]int{
		codes.InvalidArgument: http.StatusBadRequest, codes.Unavailable: http.StatusServiceUnavailable,
		codes.NotFound: http.StatusNotFound,
	}[st.Code()]
	if code == 0 {
		code = http.StatusInternalServerError
	}
	writeJSON(w, code, map[string]string{"error": st.Message()})
}

func main() {
	log := platform.Logger("ingest")
	ctx, stop := platform.SignalContext()
	defer stop()

	store, err := kv.Dial(platform.Env("CARNET_ADDR", "localhost:7070"))
	if err != nil {
		log.Error("carnet", "err", err)
		return
	}
	defer store.Close()
	pub, err := eventlog.NewPublisher(ctx, eventlog.FromEnv(), log)
	if err != nil {
		log.Error("event log", "err", err)
		return
	}
	defer pub.Close()

	s := &server{log: log, kv: store, pub: pub}
	gs, hs := platform.GRPCServer()
	martinetv1.RegisterIngestServer(gs, s)
	if err := platform.ServeGRPC(ctx, log, platform.Env("GRPC_ADDR", ":9090"), gs, hs); err != nil {
		log.Error("grpc", "err", err)
		return
	}
	platform.ServeHTTP(ctx, log, platform.Env("HTTP_ADDR", ":8080"), s.httpHandler())
	platform.ServeMetrics(ctx, log, platform.Env("METRICS_ADDR", ":9100"), nil)
	<-ctx.Done()
	log.Info("shutting down")
	time.Sleep(500 * time.Millisecond) // let servers drain
}

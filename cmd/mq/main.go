// mq serves internal/mq over gRPC (proto/mq/v1/mq.proto).
package main

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mqv1 "github.com/90Ismail/martinet/gen/mq/v1"
	"github.com/90Ismail/martinet/internal/mq"
	"github.com/90Ismail/martinet/internal/platform"
)

var depth = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "martinet_mq_messages", Help: "Messages per queue and state.",
}, []string{"queue", "state"})

type server struct {
	mqv1.UnimplementedMQServer
	b *mq.Broker
}

func toProto(m mq.Message) *mqv1.Message {
	return &mqv1.Message{Id: m.ID, Queue: m.Queue, Body: m.Body, Attributes: m.Attributes,
		ReceiveCount: m.ReceiveCount, EnqueuedAtMs: m.EnqueuedAt.UnixMilli(), Receipt: m.Receipt}
}

func ms(n int64) time.Duration { return time.Duration(n) * time.Millisecond }

func (s *server) Send(_ context.Context, r *mqv1.SendRequest) (*mqv1.SendResponse, error) {
	if r.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue required")
	}
	return &mqv1.SendResponse{Id: s.b.Send(r.Queue, r.Body, r.Attributes, ms(r.DelayMs))}, nil
}

func (s *server) Receive(ctx context.Context, r *mqv1.ReceiveRequest) (*mqv1.ReceiveResponse, error) {
	max := int(r.Max)
	if max <= 0 {
		max = 1
	}
	vis := ms(r.VisibilityMs)
	if vis <= 0 {
		vis = 30 * time.Second
	}
	wait := min(ms(r.WaitMs), 20*time.Second)
	if dl, ok := ctx.Deadline(); ok {
		wait = min(wait, time.Until(dl)-100*time.Millisecond)
	}
	out := &mqv1.ReceiveResponse{}
	for _, m := range s.b.Receive(r.Queue, max, vis, wait) {
		out.Messages = append(out.Messages, toProto(m))
	}
	return out, nil
}

func receiptErr(err error) error {
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return nil
}

func (s *server) Ack(_ context.Context, r *mqv1.AckRequest) (*mqv1.AckResponse, error) {
	return &mqv1.AckResponse{}, receiptErr(s.b.Ack(r.Queue, r.Receipt))
}

func (s *server) Nack(_ context.Context, r *mqv1.NackRequest) (*mqv1.NackResponse, error) {
	return &mqv1.NackResponse{}, receiptErr(s.b.Nack(r.Queue, r.Receipt, ms(r.DelayMs)))
}

func (s *server) DeadLetter(_ context.Context, r *mqv1.DeadLetterRequest) (*mqv1.AckResponse, error) {
	return &mqv1.AckResponse{}, receiptErr(s.b.DeadLetter(r.Queue, r.Receipt, r.Reason))
}

func (s *server) Peek(_ context.Context, r *mqv1.PeekRequest) (*mqv1.PeekResponse, error) {
	out := &mqv1.PeekResponse{}
	for _, m := range s.b.Peek(r.Queue, int(r.Max)) {
		out.Messages = append(out.Messages, toProto(m))
	}
	return out, nil
}

func (s *server) Redrive(_ context.Context, r *mqv1.RedriveRequest) (*mqv1.RedriveResponse, error) {
	var match func(*mq.Message) bool
	if r.AttributeKey != "" {
		match = func(m *mq.Message) bool { return m.Attributes[r.AttributeKey] == r.AttributeValue }
	}
	return &mqv1.RedriveResponse{Moved: uint32(s.b.Redrive(r.Queue, int(r.Max), match))}, nil
}

func (s *server) Stats(context.Context, *mqv1.StatsRequest) (*mqv1.StatsResponse, error) {
	out := &mqv1.StatsResponse{}
	for _, q := range s.b.Stats() {
		out.Queues = append(out.Queues, &mqv1.QueueStats{Name: q.Name, Ready: uint64(q.Ready),
			Delayed: uint64(q.Delayed), InFlight: uint64(q.InFlight), Sent: q.Sent, Acked: q.Acked,
			DeadLettered: q.DeadLettered})
	}
	return out, nil
}

func main() {
	log := platform.Logger("mq")
	ctx, stop := platform.SignalContext()
	defer stop()

	b := mq.New(uint32(platform.EnvInt("MQ_MAX_RECEIVES", 10)))
	gs, hs := platform.GRPCServer()
	mqv1.RegisterMQServer(gs, &server{b: b})
	if err := platform.ServeGRPC(ctx, log, platform.Env("GRPC_ADDR", ":7072"), gs, hs); err != nil {
		log.Error("grpc", "err", err)
		return
	}
	platform.ServeMetrics(ctx, log, platform.Env("METRICS_ADDR", ":9100"), nil)
	go func() {
		for range time.Tick(5 * time.Second) {
			for _, q := range b.Stats() {
				depth.WithLabelValues(q.Name, "ready").Set(float64(q.Ready))
				depth.WithLabelValues(q.Name, "delayed").Set(float64(q.Delayed))
				depth.WithLabelValues(q.Name, "in_flight").Set(float64(q.InFlight))
			}
		}
	}()
	<-ctx.Done()
}

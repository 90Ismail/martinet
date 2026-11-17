package eventlog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaPublisher is an idempotent producer (no duplicates from producer
// retries) that waits for all in-sync replicas: with min.insync.replicas=2
// and RF=3, an acknowledged event survives the loss of any one broker.
type KafkaPublisher struct{ cl *kgo.Client }

func NewKafkaPublisher(brokers []string) (*KafkaPublisher, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), // murmur2 on key, like the Java client
	)
	if err != nil {
		return nil, err
	}
	return &KafkaPublisher{cl: cl}, nil
}

func (p *KafkaPublisher) Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: value}
	for k, v := range headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return p.cl.ProduceSync(ctx, rec).FirstErr()
}

func (p *KafkaPublisher) Close() { p.cl.Close() }

// KafkaConsumer is a consumer-group member with manual commits: offsets are
// committed only after the handler succeeded for every record in the batch.
// A crash therefore re-delivers (at-least-once), never skips.
type KafkaConsumer struct {
	cl  *kgo.Client
	log *slog.Logger
}

func NewKafkaConsumer(brokers []string, group string, log *slog.Logger, topics ...string) (*KafkaConsumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(), // never lose a partition mid-batch
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	return &KafkaConsumer{cl: cl, log: log}, nil
}

func (c *KafkaConsumer) Run(ctx context.Context, h Handler) error {
	for {
		fetches := c.cl.PollRecords(ctx, 500)
		if ctx.Err() != nil {
			c.cl.AllowRebalance()
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.log.Warn("fetch error", "topic", t, "partition", p, "err", err)
		})
		var failed error
		fetches.EachRecord(func(r *kgo.Record) {
			if failed != nil {
				return
			}
			rec := Record{Topic: r.Topic, Key: string(r.Key), Value: r.Value, Partition: r.Partition, Offset: r.Offset,
				Headers: map[string]string{}}
			for _, hd := range r.Headers {
				rec.Headers[hd.Key] = string(hd.Value)
			}
			// Retry transient handler errors in place: partition order is
			// preserved and a stuck record is visible in logs/lag metrics.
			for attempt := 0; ; attempt++ {
				err := h(ctx, rec)
				if err == nil {
					return
				}
				if ctx.Err() != nil || attempt >= 20 {
					failed = fmt.Errorf("record %s/%d@%d: %w", r.Topic, r.Partition, r.Offset, err)
					return
				}
				c.log.Warn("handler failed, retrying", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "err", err)
				time.Sleep(backoff(attempt))
			}
		})
		if failed != nil {
			c.cl.AllowRebalance()
			return failed
		}
		if err := c.cl.CommitUncommittedOffsets(context.Background()); err != nil {
			c.log.Warn("commit failed", "err", err)
		}
		c.cl.AllowRebalance()
	}
}

func (c *KafkaConsumer) Close() { c.cl.Close() }

func backoff(attempt int) time.Duration {
	d := 100 * time.Millisecond << min(attempt, 6)
	return min(d, 5*time.Second)
}

// TopicSpec describes a topic EnsureTopics should create.
type TopicSpec struct {
	Name       string
	Partitions int32
	Configs    map[string]*string
}

// EnsureTopics creates missing topics. Idempotent.
func EnsureTopics(ctx context.Context, brokers []string, replication int16, specs ...TopicSpec) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	for _, s := range specs {
		resp, err := adm.CreateTopic(ctx, s.Partitions, replication, s.Configs, s.Name)
		if err == nil {
			err = resp.Err
		}
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", s.Name, err)
		}
	}
	return nil
}

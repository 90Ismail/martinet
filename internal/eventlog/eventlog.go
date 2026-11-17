// Package eventlog abstracts the durable, partitioned event log.
//
// Two backends:
//   - Kafka (default, self-hosted on Kubernetes via Strimzi)
//   - Google Cloud Pub/Sub (EVENT_LOG=pubsub, for the GKE deployment)
//
// Both give per-key ordering (Kafka partition key / Pub/Sub ordering key)
// and at-least-once delivery. Consumers must therefore be idempotent.
package eventlog

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/90Ismail/martinet/internal/platform"
)

// Record is one message on the log.
type Record struct {
	Topic     string
	Key       string
	Value     []byte
	Partition int32
	Offset    int64
	Headers   map[string]string
}

// Handler processes a record. Returning an error means "not processed":
// Kafka will not commit past it, Pub/Sub will nack it for redelivery.
type Handler func(ctx context.Context, r Record) error

// Publisher appends records.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
	Close()
}

// Consumer reads records as part of a consumer group / subscription.
type Consumer interface {
	Run(ctx context.Context, h Handler) error
	Close()
}

// Config picks and configures a backend from the environment.
type Config struct {
	Backend   string // "kafka" | "pubsub"
	Brokers   []string
	ProjectID string
}

// FromEnv reads EVENT_LOG, KAFKA_BROKERS, GCP_PROJECT.
func FromEnv() Config {
	return Config{
		Backend:   platform.Env("EVENT_LOG", "kafka"),
		Brokers:   splitCSV(platform.Env("KAFKA_BROKERS", "localhost:9092")),
		ProjectID: platform.Env("GCP_PROJECT", ""),
	}
}

// NewPublisher builds a publisher for the configured backend.
func NewPublisher(ctx context.Context, cfg Config, log *slog.Logger) (Publisher, error) {
	switch cfg.Backend {
	case "kafka":
		return NewKafkaPublisher(cfg.Brokers)
	case "pubsub":
		return NewPubSubPublisher(ctx, cfg.ProjectID)
	}
	return nil, fmt.Errorf("eventlog: unknown backend %q", cfg.Backend)
}

// NewConsumer builds a consumer. group is the Kafka consumer group, or the
// Pub/Sub subscription id.
func NewConsumer(ctx context.Context, cfg Config, log *slog.Logger, group string, topics ...string) (Consumer, error) {
	switch cfg.Backend {
	case "kafka":
		return NewKafkaConsumer(cfg.Brokers, group, log, topics...)
	case "pubsub":
		return NewPubSubConsumer(ctx, cfg.ProjectID, group, log)
	}
	return nil, fmt.Errorf("eventlog: unknown backend %q", cfg.Backend)
}

func splitCSV(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}

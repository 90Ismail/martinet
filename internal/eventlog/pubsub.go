package eventlog

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"cloud.google.com/go/pubsub/v2"
)

// PubSubPublisher publishes to Google Cloud Pub/Sub with ordering keys.
//
// Mapping to Kafka concepts:
//
//	Kafka topic          -> Pub/Sub topic
//	partition key        -> ordering key (per-key FIFO, like a partition)
//	consumer group       -> subscription
//	events.dlq topic     -> subscription dead_letter_policy (see terraform/envs/gcp)
//
// Pub/Sub has no offsets to replay from; replay uses Seek to a snapshot or
// timestamp instead (configured with retain_acked_messages in Terraform).
type PubSubPublisher struct {
	client *pubsub.Client
	mu     sync.Mutex
	pubs   map[string]*pubsub.Publisher
}

func NewPubSubPublisher(ctx context.Context, project string) (*PubSubPublisher, error) {
	if project == "" {
		return nil, fmt.Errorf("pubsub: GCP_PROJECT is required")
	}
	c, err := pubsub.NewClient(ctx, project) // uses Workload Identity on GKE
	if err != nil {
		return nil, err
	}
	return &PubSubPublisher{client: c, pubs: map[string]*pubsub.Publisher{}}, nil
}

func (p *PubSubPublisher) publisher(topic string) *pubsub.Publisher {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pub, ok := p.pubs[topic]; ok {
		return pub
	}
	pub := p.client.Publisher(topic)
	pub.EnableMessageOrdering = true
	p.pubs[topic] = pub
	return pub
}

func (p *PubSubPublisher) Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	pub := p.publisher(topic)
	res := pub.Publish(ctx, &pubsub.Message{Data: value, OrderingKey: key, Attributes: headers})
	if _, err := res.Get(ctx); err != nil {
		// After a failure Pub/Sub pauses the ordering key so later messages
		// cannot overtake the failed one; resume once the caller retries.
		pub.ResumePublish(key)
		return err
	}
	return nil
}

func (p *PubSubPublisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pub := range p.pubs {
		pub.Stop()
	}
	_ = p.client.Close()
}

// PubSubConsumer receives from one subscription. Ack on success, Nack on
// error; after max_delivery_attempts Pub/Sub moves the message to the
// subscription's dead-letter topic by itself.
type PubSubConsumer struct {
	client *pubsub.Client
	sub    *pubsub.Subscriber
	log    *slog.Logger
}

func NewPubSubConsumer(ctx context.Context, project, subscription string, log *slog.Logger) (*PubSubConsumer, error) {
	if project == "" {
		return nil, fmt.Errorf("pubsub: GCP_PROJECT is required")
	}
	c, err := pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}
	sub := c.Subscriber(subscription)
	sub.ReceiveSettings.MaxOutstandingMessages = 500
	return &PubSubConsumer{client: c, sub: sub, log: log}, nil
}

func (c *PubSubConsumer) Run(ctx context.Context, h Handler) error {
	return c.sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		rec := Record{Key: m.OrderingKey, Value: m.Data, Headers: m.Attributes}
		if err := h(ctx, rec); err != nil {
			c.log.Warn("handler failed, nacking", "id", m.ID, "err", err)
			m.Nack()
			return
		}
		m.Ack()
	})
}

func (c *PubSubConsumer) Close() { _ = c.client.Close() }

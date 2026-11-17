// Package queue is the delivery-queue client used by fanout, courier and
// status. It talks to Martinet's own message queue (cmd/mq) over gRPC.
//
//	deliveries      work queue: one DeliveryJob per (event, subscription)
//	deliveries.dlq  dead letter queue: permanent failures, spent budgets,
//	                and poison messages that exceeded mq's max receives
//
// Retries don't need separate TTL queues: a failed job is re-sent with a
// delay (5s, 30s, 2m, ...) and mq keeps it invisible until then.
package queue

import (
	"context"
	"strconv"
	"time"

	"google.golang.org/grpc"

	mqv1 "github.com/90Ismail/martinet/gen/mq/v1"
	"github.com/90Ismail/martinet/internal/platform"
)

const (
	MainQueue = "deliveries"
	DLQ       = "deliveries.dlq"

	AttrDeliveryID     = "delivery_id"
	AttrSubscriptionID = "subscription_id"
	AttrAttempt        = "attempt"
	AttrReason         = "dead_reason"
	AttrDeadAt         = "dead_at_ms"
)

// Client wraps the generated MQ stub.
type Client struct {
	conn *grpc.ClientConn
	mq   mqv1.MQClient
}

func Dial(addr string) (*Client, error) {
	conn, err := platform.Dial(addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, mq: mqv1.NewMQClient(conn)}, nil
}

func (c *Client) Close()             { _ = c.conn.Close() }
func (c *Client) Raw() mqv1.MQClient { return c.mq }

func attrs(deliveryID, subID string, attempt uint32) map[string]string {
	return map[string]string{AttrDeliveryID: deliveryID, AttrSubscriptionID: subID, AttrAttempt: strconv.Itoa(int(attempt))}
}

// Enqueue sends a job for immediate delivery.
func (c *Client) Enqueue(ctx context.Context, deliveryID, subID string, attempt uint32, body []byte) error {
	_, err := c.mq.Send(ctx, &mqv1.SendRequest{Queue: MainQueue, Body: body, Attributes: attrs(deliveryID, subID, attempt)})
	return err
}

// Retry sends the (updated) job again, invisible for delay.
func (c *Client) Retry(ctx context.Context, deliveryID, subID string, attempt uint32, delay time.Duration, body []byte) error {
	_, err := c.mq.Send(ctx, &mqv1.SendRequest{Queue: MainQueue, Body: body, Attributes: attrs(deliveryID, subID, attempt),
		DelayMs: delay.Milliseconds()})
	return err
}

// Receive long-polls for up to max jobs, leased for visibility.
func (c *Client) Receive(ctx context.Context, max int, visibility, wait time.Duration) ([]*mqv1.Message, error) {
	r, err := c.mq.Receive(ctx, &mqv1.ReceiveRequest{Queue: MainQueue, Max: uint32(max),
		VisibilityMs: visibility.Milliseconds(), WaitMs: wait.Milliseconds()})
	if err != nil {
		return nil, err
	}
	return r.Messages, nil
}

func (c *Client) Ack(ctx context.Context, receipt string) error {
	_, err := c.mq.Ack(ctx, &mqv1.AckRequest{Queue: MainQueue, Receipt: receipt})
	return err
}

func (c *Client) Nack(ctx context.Context, receipt string, delay time.Duration) error {
	_, err := c.mq.Nack(ctx, &mqv1.NackRequest{Queue: MainQueue, Receipt: receipt, DelayMs: delay.Milliseconds()})
	return err
}

// DeadLetter moves a leased job to deliveries.dlq.
func (c *Client) DeadLetter(ctx context.Context, receipt, reason string) error {
	_, err := c.mq.DeadLetter(ctx, &mqv1.DeadLetterRequest{Queue: MainQueue, Receipt: receipt, Reason: reason})
	return err
}

// PeekDLQ lists dead letters without removing them.
func (c *Client) PeekDLQ(ctx context.Context, max int) ([]*mqv1.Message, error) {
	r, err := c.mq.Peek(ctx, &mqv1.PeekRequest{Queue: DLQ, Max: uint32(max)})
	if err != nil {
		return nil, err
	}
	return r.Messages, nil
}

// Redrive moves dead letters back to the work queue; filter by attribute.
func (c *Client) Redrive(ctx context.Context, max int, key, value string) (int, error) {
	r, err := c.mq.Redrive(ctx, &mqv1.RedriveRequest{Queue: MainQueue, Max: uint32(max), AttributeKey: key, AttributeValue: value})
	if err != nil {
		return 0, err
	}
	return int(r.Moved), nil
}

// Depth returns ready+delayed+in-flight for a queue.
func (c *Client) Depth(ctx context.Context, queue string) (int, error) {
	r, err := c.mq.Stats(ctx, &mqv1.StatsRequest{})
	if err != nil {
		return 0, err
	}
	for _, q := range r.Queues {
		if q.Name == queue {
			return int(q.Ready + q.Delayed + q.InFlight), nil
		}
	}
	return 0, nil
}

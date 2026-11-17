// Package kv is the Go client for carnet (the C++ LSM store) plus Martinet's
// key layout on top of it.
//
//	sub/<tenant>/<subscription_id>          -> Subscription (proto)
//	idem/<tenant>/<idempotency_key>         -> event id
//	att/<event_id>:<sub_id>/<attempt:05d>   -> DeliveryAttempt (proto)
//
// Delivery ids are "<event_id>:<sub_id>", so every attempt of every delivery
// of one event shares the prefix "att/<event_id>:" and one range scan finds
// them all -- the key design is the index.
package kv

import (
	"context"
	"errors"
	"fmt"
	"io"

	carnetv1 "github.com/90Ismail/martinet/gen/carnet/v1"
	"github.com/90Ismail/martinet/internal/platform"
	"google.golang.org/grpc"
)

// Client wraps the generated carnet stub.
type Client struct {
	conn *grpc.ClientConn
	c    carnetv1.CarnetClient
}

// Dial connects to carnet at addr (host:port).
func Dial(addr string) (*Client, error) {
	conn, err := platform.Dial(addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, c: carnetv1.NewCarnetClient(conn)}, nil
}

func (k *Client) Close() error { return k.conn.Close() }

// Raw exposes the stub for Stats and other rarely used RPCs.
func (k *Client) Raw() carnetv1.CarnetClient { return k.c }

// ErrNotFound is returned by Get for absent keys.
var ErrNotFound = errors.New("kv: not found")

func (k *Client) Get(ctx context.Context, key string) ([]byte, error) {
	r, err := k.c.Get(ctx, &carnetv1.GetRequest{Key: []byte(key)})
	if err != nil {
		return nil, err
	}
	if !r.Found {
		return nil, ErrNotFound
	}
	return r.Value, nil
}

func (k *Client) Put(ctx context.Context, key string, value []byte, sync bool) error {
	_, err := k.c.Put(ctx, &carnetv1.PutRequest{Key: []byte(key), Value: value, Sync: sync})
	return err
}

func (k *Client) Delete(ctx context.Context, key string) error {
	_, err := k.c.Delete(ctx, &carnetv1.DeleteRequest{Key: []byte(key), Sync: true})
	return err
}

// PutIfAbsent returns (true, nil) if it wrote, or (false, existing) if the
// key was already there. Atomic on the server.
func (k *Client) PutIfAbsent(ctx context.Context, key string, value []byte) (bool, []byte, error) {
	r, err := k.c.PutIfAbsent(ctx, &carnetv1.PutRequest{Key: []byte(key), Value: value, Sync: true})
	if err != nil {
		return false, nil, err
	}
	return r.Written, r.Existing, nil
}

// KV is one scanned pair.
type KV struct {
	Key   string
	Value []byte
}

// ScanPrefix returns every pair whose key starts with prefix (up to limit).
func (k *Client) ScanPrefix(ctx context.Context, prefix string, limit uint32) ([]KV, error) {
	stream, err := k.c.Scan(ctx, &carnetv1.ScanRequest{
		Start: []byte(prefix),
		End:   PrefixEnd([]byte(prefix)),
		Limit: limit,
	})
	if err != nil {
		return nil, err
	}
	var out []KV
	for {
		kv, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, KV{Key: string(kv.Key), Value: kv.Value})
	}
}

// PrefixEnd returns the smallest key greater than every key with prefix p.
func PrefixEnd(p []byte) []byte {
	end := append([]byte(nil), p...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil // all 0xff: unbounded
}

// ---- key layout

func SubscriptionKey(tenant, id string) string { return fmt.Sprintf("sub/%s/%s", tenant, id) }
func SubscriptionPrefix(tenant string) string  { return fmt.Sprintf("sub/%s/", tenant) }
func IdempotencyKey(tenant, key string) string { return fmt.Sprintf("idem/%s/%s", tenant, key) }
func AttemptKey(deliveryID string, attempt uint32) string {
	return fmt.Sprintf("att/%s/%05d", deliveryID, attempt)
}
func AttemptPrefix(deliveryID string) string    { return fmt.Sprintf("att/%s/", deliveryID) }
func EventAttemptsPrefix(eventID string) string { return fmt.Sprintf("att/%s:", eventID) }
func DeliveryID(eventID, subID string) string   { return eventID + ":" + subID }

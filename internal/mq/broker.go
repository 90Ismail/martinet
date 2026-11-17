// Package mq is a small in-memory message queue with SQS-like semantics.
// It replaces RabbitMQ in Martinet so the queue itself is something to learn
// from, not a black box.
//
//	Send ──▶ ready (FIFO) ──Receive──▶ in-flight (leased until deadline)
//	           ▲      ▲                    │      │      │
//	           │      └── lease expires ───┘      │      │ Ack: gone
//	           └── delayed (min-heap by time) ◀─ Nack(delay)
//	                                              │
//	               receive_count > maxReceives or DeadLetter()
//	                                              ▼
//	                                       "<queue>.dlq"
//
// Delivery is at-least-once: a consumer that crashes after doing the work
// but before Ack gets the message redelivered when its lease expires.
// State is in memory only: restarting mq loses queued messages (fine for
// the experiment; a real one would append every transition to a log).
package mq

import (
	"container/heap"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sort"
	"sync"
	"time"
)

// Message is one queued item.
type Message struct {
	ID           string
	Queue        string
	Body         []byte
	Attributes   map[string]string
	ReceiveCount uint32
	EnqueuedAt   time.Time
	Receipt      string
}

type delayed struct {
	msg     *Message
	visible time.Time
}

type delayHeap []delayed

func (h delayHeap) Len() int           { return len(h) }
func (h delayHeap) Less(i, j int) bool { return h[i].visible.Before(h[j].visible) }
func (h delayHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *delayHeap) Push(x any)        { *h = append(*h, x.(delayed)) }
func (h *delayHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

type lease struct {
	msg      *Message
	deadline time.Time
}

// Stats for one queue.
type Stats struct {
	Name                      string
	Ready, Delayed, InFlight  int
	Sent, Acked, DeadLettered uint64
}

type queue struct {
	name     string
	ready    []*Message
	delayed  delayHeap
	inflight map[string]lease
	notify   chan struct{} // closed on new ready messages (long polling)
	stats    Stats
}

// Broker owns all queues.
type Broker struct {
	mu          sync.Mutex
	queues      map[string]*queue
	maxReceives uint32
	now         func() time.Time
}

var ErrUnknownReceipt = errors.New("mq: unknown or expired receipt")

// New returns a broker; messages received more than maxReceives times are
// dead-lettered automatically (poison message protection).
func New(maxReceives uint32) *Broker {
	return &Broker{queues: map[string]*queue{}, maxReceives: maxReceives, now: time.Now}
}

// DLQName is the dead letter queue for q.
func DLQName(q string) string { return q + ".dlq" }

func newID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (b *Broker) q(name string) *queue {
	q, ok := b.queues[name]
	if !ok {
		q = &queue{name: name, inflight: map[string]lease{}, notify: make(chan struct{})}
		q.stats.Name = name
		b.queues[name] = q
	}
	return q
}

func (q *queue) wake() {
	close(q.notify)
	q.notify = make(chan struct{})
}

func (b *Broker) enqueue(q *queue, m *Message, delay time.Duration) {
	if delay > 0 {
		heap.Push(&q.delayed, delayed{msg: m, visible: b.now().Add(delay)})
		return
	}
	q.ready = append(q.ready, m)
	q.wake()
}

// Send adds a message, visible after delay.
func (b *Broker) Send(queue string, body []byte, attrs map[string]string, delay time.Duration) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	m := &Message{ID: newID(), Queue: queue, Body: body, Attributes: attrs, EnqueuedAt: b.now()}
	q := b.q(queue)
	q.stats.Sent++
	b.enqueue(q, m, delay)
	return m.ID
}

// promote moves due delayed messages and expired leases back to ready.
// Requires b.mu.
func (b *Broker) promote(q *queue) {
	now := b.now()
	moved := false
	for q.delayed.Len() > 0 && !q.delayed[0].visible.After(now) {
		q.ready = append(q.ready, heap.Pop(&q.delayed).(delayed).msg)
		moved = true
	}
	for r, l := range q.inflight {
		if now.After(l.deadline) {
			delete(q.inflight, r)
			q.ready = append(q.ready, l.msg)
			moved = true
		}
	}
	if moved {
		q.wake()
	}
}

func (b *Broker) deadLetter(q *queue, m *Message, reason string) {
	if m.Attributes == nil {
		m.Attributes = map[string]string{}
	}
	m.Attributes["dead_reason"] = reason
	m.Attributes["dead_at_ms"] = itoa(b.now().UnixMilli())
	q.stats.DeadLettered++
	d := b.q(DLQName(q.name))
	d.stats.Sent++
	d.ready = append(d.ready, m)
}

// Receive leases up to max messages for visibility, waiting up to wait for
// at least one to arrive.
func (b *Broker) Receive(queue string, max int, visibility, wait time.Duration) []Message {
	deadline := b.now().Add(wait)
	for {
		b.mu.Lock()
		q := b.q(queue)
		b.promote(q)
		var out []Message
		for len(out) < max && len(q.ready) > 0 {
			m := q.ready[0]
			q.ready = q.ready[1:]
			m.ReceiveCount++
			if b.maxReceives > 0 && m.ReceiveCount > b.maxReceives {
				b.deadLetter(q, m, "max receives exceeded")
				continue
			}
			m.Receipt = newID()
			q.inflight[m.Receipt] = lease{msg: m, deadline: b.now().Add(visibility)}
			out = append(out, *m)
		}
		notify := q.notify
		b.mu.Unlock()
		remaining := deadline.Sub(b.now())
		if len(out) > 0 || remaining <= 0 {
			return out
		}
		select { // long poll: wake on send, or re-check delayed every 100ms
		case <-notify:
		case <-time.After(min(remaining, 100*time.Millisecond)):
		}
	}
}

func (b *Broker) take(queue, receipt string) (*queue, *Message, error) {
	q := b.q(queue)
	l, ok := q.inflight[receipt]
	if !ok {
		return q, nil, ErrUnknownReceipt
	}
	delete(q.inflight, receipt)
	return q, l.msg, nil
}

// Ack deletes a leased message.
func (b *Broker) Ack(queue, receipt string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	q, _, err := b.take(queue, receipt)
	if err == nil {
		q.stats.Acked++
	}
	return err
}

// Nack returns a leased message, visible again after delay (retry backoff).
func (b *Broker) Nack(queue, receipt string, delay time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	q, m, err := b.take(queue, receipt)
	if err != nil {
		return err
	}
	b.enqueue(q, m, delay)
	return nil
}

// DeadLetter moves a leased message to the DLQ immediately.
func (b *Broker) DeadLetter(queue, receipt, reason string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	q, m, err := b.take(queue, receipt)
	if err != nil {
		return err
	}
	b.deadLetter(q, m, reason)
	return nil
}

// Peek returns copies of up to max ready messages without leasing them.
func (b *Broker) Peek(queue string, max int) []Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.q(queue)
	var out []Message
	for i := 0; i < len(q.ready) && (max <= 0 || i < max); i++ {
		out = append(out, *q.ready[i])
	}
	return out
}

// Redrive moves messages from queue's DLQ back to queue with a fresh
// receive count. match filters (nil = everything).
func (b *Broker) Redrive(queue string, max int, match func(*Message) bool) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	d, q := b.q(DLQName(queue)), b.q(queue)
	var keep []*Message
	moved := 0
	for _, m := range d.ready {
		if (max <= 0 || moved < max) && (match == nil || match(m)) {
			m.ReceiveCount = 0
			delete(m.Attributes, "dead_reason")
			delete(m.Attributes, "dead_at_ms")
			q.ready = append(q.ready, m)
			moved++
		} else {
			keep = append(keep, m)
		}
	}
	d.ready = keep
	if moved > 0 {
		q.wake()
	}
	return moved
}

// Stats returns a snapshot of every queue, sorted by name.
func (b *Broker) Stats() []Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []Stats
	for _, q := range b.queues {
		b.promote(q)
		s := q.stats
		s.Ready, s.Delayed, s.InFlight = len(q.ready), q.delayed.Len(), len(q.inflight)
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

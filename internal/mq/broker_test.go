package mq

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTest(maxReceives uint32) (*Broker, *clock) {
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	b := New(maxReceives)
	b.now = c.now
	return b, c
}

func TestSendReceiveAck(t *testing.T) {
	b, _ := newTest(5)
	b.Send("q", []byte("a"), nil, 0)
	b.Send("q", []byte("b"), nil, 0)
	got := b.Receive("q", 10, time.Second, 0)
	if len(got) != 2 || string(got[0].Body) != "a" {
		t.Fatalf("FIFO broken: %+v", got)
	}
	if err := b.Ack("q", got[0].Receipt); err != nil {
		t.Fatal(err)
	}
	if err := b.Ack("q", got[0].Receipt); err != ErrUnknownReceipt {
		t.Fatal("double ack must fail")
	}
}

func TestLeaseExpiryRedelivers(t *testing.T) {
	b, c := newTest(5)
	b.Send("q", []byte("x"), nil, 0)
	first := b.Receive("q", 1, 30*time.Second, 0)
	if len(b.Receive("q", 1, time.Second, 0)) != 0 {
		t.Fatal("leased message must be invisible")
	}
	c.advance(31 * time.Second) // consumer "crashed"
	again := b.Receive("q", 1, time.Second, 0)
	if len(again) != 1 || again[0].ID != first[0].ID || again[0].ReceiveCount != 2 {
		t.Fatalf("expected redelivery, got %+v", again)
	}
	if b.Ack("q", first[0].Receipt) == nil {
		t.Fatal("stale receipt must not ack the redelivered copy")
	}
}

func TestNackDelay(t *testing.T) {
	b, c := newTest(5)
	b.Send("q", []byte("x"), nil, 0)
	m := b.Receive("q", 1, time.Minute, 0)[0]
	_ = b.Nack("q", m.Receipt, 5*time.Second)
	if len(b.Receive("q", 1, time.Minute, 0)) != 0 {
		t.Fatal("visible too early")
	}
	c.advance(5 * time.Second)
	if len(b.Receive("q", 1, time.Minute, 0)) != 1 {
		t.Fatal("should be visible after delay")
	}
}

func TestPoisonMessageGoesToDLQAndRedrives(t *testing.T) {
	b, c := newTest(3)
	b.Send("q", []byte("poison"), map[string]string{"sub": "s1"}, 0)
	for i := 0; i < 3; i++ {
		if len(b.Receive("q", 1, time.Second, 0)) != 1 {
			t.Fatalf("receive %d", i)
		}
		c.advance(2 * time.Second) // never acked
	}
	if len(b.Receive("q", 1, time.Second, 0)) != 0 {
		t.Fatal("4th receive should dead-letter instead")
	}
	dlq := b.Peek(DLQName("q"), 0)
	if len(dlq) != 1 || dlq[0].Attributes["dead_reason"] == "" {
		t.Fatalf("expected message in DLQ, got %+v", dlq)
	}
	if n := b.Redrive("q", 0, func(m *Message) bool { return m.Attributes["sub"] == "s1" }); n != 1 {
		t.Fatal("redrive")
	}
	got := b.Receive("q", 1, time.Second, 0)
	if len(got) != 1 || got[0].ReceiveCount != 1 {
		t.Fatalf("redriven message should start fresh: %+v", got)
	}
}

func TestExplicitDeadLetter(t *testing.T) {
	b, _ := newTest(10)
	b.Send("q", []byte("x"), nil, 0)
	m := b.Receive("q", 1, time.Second, 0)[0]
	_ = b.DeadLetter("q", m.Receipt, "http 410")
	if s := b.Peek("q.dlq", 0); len(s) != 1 || s[0].Attributes["dead_reason"] != "http 410" {
		t.Fatal("explicit dead letter")
	}
}

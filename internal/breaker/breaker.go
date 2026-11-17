// Package breaker is the courier-side view of endpoint health. pouls (C++)
// computes the state; couriers tail the compacted endpoint-health topic into
// this in-memory table and consult it before every HTTP call.
package breaker

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	martinetv1 "github.com/90Ismail/martinet/gen/martinet/v1"
	"github.com/90Ismail/martinet/internal/eventlog"
)

// Verdict tells the courier whether to call an endpoint.
type Verdict int

const (
	Allow Verdict = iota
	Probe         // half-open: this call is one of the few probes let through
	Skip          // open: do not call, park the job on the retry ladder
)

// Table is safe for concurrent use.
type Table struct {
	mu          sync.RWMutex
	byHost      map[string]*martinetv1.EndpointHealth
	probeEveryN uint32
	now         func() time.Time
	rand        func(n uint32) uint32
}

// New returns a table. In HALF_OPEN, roughly one in probeEveryN deliveries
// is allowed through.
func New(probeEveryN uint32) *Table {
	return &Table{byHost: map[string]*martinetv1.EndpointHealth{}, probeEveryN: probeEveryN, now: time.Now, rand: rand.Uint32N}
}

// Apply ingests one EndpointHealth record.
func (t *Table) Apply(h *martinetv1.EndpointHealth) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.byHost[h.Endpoint] = h
}

// Check decides for a host. The half-open probe choice is random on
// purpose: a deterministic choice (say, hashing the delivery id) would mean
// a skipped job is skipped again on every pass through the retry ladder,
// because skips do not change the job's attempt number.
func (t *Table) Check(host string) Verdict {
	t.mu.RLock()
	h, ok := t.byHost[host]
	t.mu.RUnlock()
	if !ok {
		return Allow
	}
	switch h.State {
	case martinetv1.BreakerState_BREAKER_STATE_OPEN:
		// Safety valve: if pouls dies, do not keep an endpoint dark forever.
		if h.OpenUntilMs > 0 && t.now().UnixMilli() > h.OpenUntilMs+5*60_000 {
			return Allow
		}
		return Skip
	case martinetv1.BreakerState_BREAKER_STATE_HALF_OPEN:
		if t.rand(t.probeEveryN) == 0 {
			return Probe
		}
		return Skip
	default:
		return Allow
	}
}

// Snapshot returns a copy of all entries (for /debug endpoints).
func (t *Table) Snapshot() map[string]martinetv1.BreakerState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]martinetv1.BreakerState, len(t.byHost))
	for k, v := range t.byHost {
		out[k] = v.State
	}
	return out
}

// Follow tails the endpoint-health topic into the table until ctx ends.
func (t *Table) Follow(ctx context.Context, c eventlog.Consumer, log *slog.Logger) error {
	return c.Run(ctx, func(_ context.Context, r eventlog.Record) error {
		var h martinetv1.EndpointHealth
		if err := proto.Unmarshal(r.Value, &h); err != nil {
			log.Warn("bad endpoint-health record", "err", err)
			return nil // skip poison, never block the table
		}
		prev := t.state(h.Endpoint)
		t.Apply(&h)
		if prev != h.State {
			log.Info("breaker transition", "endpoint", h.Endpoint, "from", prev.String(), "to", h.State.String(),
				"failure_rate", h.FailureRate)
		}
		return nil
	})
}

func (t *Table) state(host string) martinetv1.BreakerState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if h, ok := t.byHost[host]; ok {
		return h.State
	}
	return martinetv1.BreakerState_BREAKER_STATE_CLOSED
}

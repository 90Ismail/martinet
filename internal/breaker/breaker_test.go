package breaker

import (
	"testing"
	"time"

	martinetv1 "github.com/90Ismail/martinet/gen/martinet/v1"
)

func TestUnknownHostAllowed(t *testing.T) {
	if New(10).Check("never-seen.example") != Allow {
		t.Fatal("unknown hosts must be allowed")
	}
}

func TestOpenSkipsHalfOpenProbes(t *testing.T) {
	tb := New(10)
	now := time.UnixMilli(1_000_000)
	tb.now = func() time.Time { return now }

	tb.Apply(&martinetv1.EndpointHealth{Endpoint: "h", State: martinetv1.BreakerState_BREAKER_STATE_OPEN, OpenUntilMs: now.UnixMilli() + 30_000})
	if tb.Check("h") != Skip {
		t.Fatal("open breaker must skip")
	}

	tb.Apply(&martinetv1.EndpointHealth{Endpoint: "h", State: martinetv1.BreakerState_BREAKER_STATE_HALF_OPEN})
	probes := 0
	for i := 0; i < 10_000; i++ {
		if tb.Check("h") == Probe {
			probes++
		}
	}
	if probes < 700 || probes > 1300 {
		t.Fatalf("expected ~10%% probes, got %d/10000", probes)
	}
}

func TestHalfOpenEventuallyProbesEveryJob(t *testing.T) {
	// Regression: a skipped job must get a fresh chance on its next pass.
	tb := New(10)
	tb.Apply(&martinetv1.EndpointHealth{Endpoint: "h", State: martinetv1.BreakerState_BREAKER_STATE_HALF_OPEN})
	for pass := 0; pass < 200; pass++ {
		if tb.Check("h") == Probe {
			return
		}
	}
	t.Fatal("job never probed in 200 passes")
}

func TestStaleOpenFailsOpen(t *testing.T) {
	tb := New(10)
	now := time.UnixMilli(10_000_000)
	tb.now = func() time.Time { return now }
	tb.Apply(&martinetv1.EndpointHealth{Endpoint: "h", State: martinetv1.BreakerState_BREAKER_STATE_OPEN, OpenUntilMs: now.UnixMilli() - 10*60_000})
	if tb.Check("h") != Allow {
		t.Fatal("a long-expired OPEN record (pouls down?) must not block forever")
	}
}

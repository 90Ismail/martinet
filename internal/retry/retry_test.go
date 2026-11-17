package retry

import (
	"errors"
	"testing"

	martinetv1 "github.com/90Ismail/martinet/gen/martinet/v1"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		status int
		err    error
		want   martinetv1.AttemptOutcome
	}{
		{200, nil, martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCESS},
		{204, nil, martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCESS},
		{429, nil, martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_RETRYABLE_FAILURE},
		{503, nil, martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_RETRYABLE_FAILURE},
		{408, nil, martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_RETRYABLE_FAILURE},
		{400, nil, martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_PERMANENT_FAILURE},
		{410, nil, martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_PERMANENT_FAILURE},
		{0, errors.New("connection refused"), martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_RETRYABLE_FAILURE},
	}
	for _, c := range cases {
		if got := Classify(c.status, c.err); got != c.want {
			t.Errorf("Classify(%d, %v) = %v, want %v", c.status, c.err, got, c.want)
		}
	}
}

func TestLadderClampsToLastTier(t *testing.T) {
	p := DefaultPolicy()
	if p.TierFor(1).Name != "5s" || p.TierFor(2).Name != "30s" {
		t.Fatal("early tiers wrong")
	}
	if p.TierFor(99).Name != p.Tiers[len(p.Tiers)-1].Name {
		t.Fatal("should clamp to last tier")
	}
}

func TestBudgetExhaustionDeadLetters(t *testing.T) {
	p := DefaultPolicy()
	retryable := martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_RETRYABLE_FAILURE
	if p.Next(retryable, p.MaxAttempts-1) != Retry {
		t.Fatal("should retry within budget")
	}
	if p.Next(retryable, p.MaxAttempts) != DeadLetter {
		t.Fatal("should dead-letter when budget spent")
	}
	if p.Next(martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_PERMANENT_FAILURE, 1) != DeadLetter {
		t.Fatal("permanent failures skip the ladder")
	}
	if p.Next(martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_CIRCUIT_OPEN, p.MaxAttempts) != Retry {
		t.Fatal("open breaker must not dead-letter")
	}
}

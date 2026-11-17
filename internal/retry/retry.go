// Package retry decides what happens after a delivery attempt: done, retry
// later (and on which tier of the ladder), or dead-letter.
package retry

import (
	"net/http"
	"time"

	martinetv1 "github.com/90Ismail/martinet/gen/martinet/v1"
)

// Tier is one rung of the retry ladder: the job is re-sent to mq with this
// delay and stays invisible until it elapses.
type Tier struct {
	Name  string        // label, e.g. "30s"
	Delay time.Duration // visibility delay
}

// Policy is the full ladder plus the attempt budget.
type Policy struct {
	Tiers       []Tier
	MaxAttempts uint32
}

// DefaultPolicy: 5s, 30s, 2m, 10m, then the last tier repeats until the
// attempt budget is spent. Tuned for demos; production would stretch the
// tail to hours.
func DefaultPolicy() Policy {
	return Policy{
		Tiers: []Tier{
			{"5s", 5 * time.Second},
			{"30s", 30 * time.Second},
			{"2m", 2 * time.Minute},
			{"10m", 10 * time.Minute},
		},
		MaxAttempts: 8,
	}
}

// TierFor returns the tier to wait in after `attempt` failed (1-based).
func (p Policy) TierFor(attempt uint32) Tier {
	i := int(attempt) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(p.Tiers) {
		i = len(p.Tiers) - 1
	}
	return p.Tiers[i]
}

// Decision is what the courier should do next.
type Decision int

const (
	Done Decision = iota
	Retry
	DeadLetter
)

func (d Decision) String() string {
	return [...]string{"done", "retry", "dead-letter"}[d]
}

// Classify maps an HTTP result to an attempt outcome.
//
//	2xx                  success
//	408, 425, 429, 5xx   retryable (the receiver may recover)
//	other 4xx            permanent (retrying the same bytes will not help)
//	network / timeout    retryable
func Classify(status int, err error) martinetv1.AttemptOutcome {
	if err != nil {
		// DNS failure, refused connection, TLS error, timeout: all transient
		// from our side of the network.
		return martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_RETRYABLE_FAILURE
	}
	switch {
	case status >= 200 && status < 300:
		return martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCESS
	case status == http.StatusRequestTimeout, status == http.StatusTooEarly, status == http.StatusTooManyRequests,
		status >= 500:
		return martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_RETRYABLE_FAILURE
	default:
		return martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_PERMANENT_FAILURE
	}
}

// Next decides the fate of a job after an attempt with the given outcome.
func (p Policy) Next(outcome martinetv1.AttemptOutcome, attempt uint32) Decision {
	switch outcome {
	case martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCESS:
		return Done
	case martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_PERMANENT_FAILURE:
		return DeadLetter
	case martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_CIRCUIT_OPEN:
		return Retry // not the endpoint's fault yet; does not spend budget
	default:
		if attempt >= p.MaxAttempts {
			return DeadLetter
		}
		return Retry
	}
}

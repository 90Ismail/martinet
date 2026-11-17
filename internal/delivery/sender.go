// Package delivery performs one signed HTTP webhook call.
package delivery

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	martinetv1 "github.com/90Ismail/martinet/gen/martinet/v1"
	"github.com/90Ismail/martinet/internal/retry"
	"github.com/90Ismail/martinet/internal/signing"
)

// Headers sent with every webhook.
const (
	HeaderEventID    = "Martinet-Event-Id"
	HeaderEventType  = "Martinet-Event-Type"
	HeaderDeliveryID = "Martinet-Delivery-Id" // receivers dedupe on this
	HeaderAttempt    = "Martinet-Attempt"
)

// Result of one attempt.
type Result struct {
	Outcome    martinetv1.AttemptOutcome
	HTTPStatus int
	Err        string
	Latency    time.Duration
}

// Sender is an HTTP client tuned for webhook fan-out: bounded timeouts,
// keep-alive pools per host, no redirects (a 3xx is the receiver's bug).
type Sender struct {
	client *http.Client
	now    func() time.Time
}

func NewSender(timeout time.Duration) *Sender {
	tr := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: timeout,
	}
	return &Sender{
		client: &http.Client{
			Transport: tr,
			Timeout:   timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		now: time.Now,
	}
}

// Host is the circuit-breaker key for a URL.
func Host(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return u.Host
}

// Send performs one attempt of job.
func (s *Sender) Send(ctx context.Context, job *martinetv1.DeliveryJob) Result {
	body := job.Event.GetPayload()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, job.Url, bytes.NewReader(body))
	if err != nil {
		return Result{Outcome: martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_PERMANENT_FAILURE, Err: "bad url: " + err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Martinet-Webhooks/1.0")
	req.Header.Set(HeaderEventID, job.Event.GetId())
	req.Header.Set(HeaderEventType, job.Event.GetType())
	req.Header.Set(HeaderDeliveryID, job.DeliveryId)
	req.Header.Set(HeaderAttempt, strconv.Itoa(int(job.Attempt)))
	req.Header.Set(signing.Header, signing.Sign(job.Secret, body, s.now()))

	start := time.Now()
	resp, err := s.client.Do(req)
	lat := time.Since(start)
	if err != nil {
		return Result{Outcome: retry.Classify(0, err), Err: err.Error(), Latency: lat}
	}
	// Drain (bounded) so the connection goes back to the keep-alive pool.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	r := Result{Outcome: retry.Classify(resp.StatusCode, nil), HTTPStatus: resp.StatusCode, Latency: lat}
	if r.Outcome != martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCESS {
		r.Err = fmt.Sprintf("http %d", resp.StatusCode)
	}
	return r
}

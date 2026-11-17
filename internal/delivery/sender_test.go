package delivery

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	martinetv1 "github.com/90Ismail/martinet/gen/martinet/v1"
	"github.com/90Ismail/martinet/internal/signing"
)

func job(url string) *martinetv1.DeliveryJob {
	return &martinetv1.DeliveryJob{
		DeliveryId: "evt_1:sub_1",
		Url:        url,
		Secret:     "whsec_test",
		Attempt:    2,
		Event:      &martinetv1.Event{Id: "evt_1", Type: "order.created", Payload: []byte(`{"n":1}`)},
	}
}

func TestSendSignsAndClassifies(t *testing.T) {
	var gotSigErr error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotSigErr = signing.Verify("whsec_test", body, r.Header.Get(signing.Header), time.Now(), time.Minute)
		if r.Header.Get(HeaderDeliveryID) != "evt_1:sub_1" || r.Header.Get(HeaderAttempt) != "2" {
			w.WriteHeader(400)
			return
		}
		w.WriteHeader(204)
	}))
	defer srv.Close()

	res := NewSender(2*time.Second).Send(context.Background(), job(srv.URL))
	if res.Outcome != martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCESS {
		t.Fatalf("outcome %v (%s)", res.Outcome, res.Err)
	}
	if gotSigErr != nil {
		t.Fatalf("receiver could not verify signature: %v", gotSigErr)
	}
}

func TestSendTimeoutIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()
	res := NewSender(50*time.Millisecond).Send(context.Background(), job(srv.URL))
	if res.Outcome != martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_RETRYABLE_FAILURE {
		t.Fatalf("timeout should be retryable, got %v", res.Outcome)
	}
}

func TestGoneIsPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(410) }))
	defer srv.Close()
	res := NewSender(time.Second).Send(context.Background(), job(srv.URL))
	if res.Outcome != martinetv1.AttemptOutcome_ATTEMPT_OUTCOME_PERMANENT_FAILURE || res.HTTPStatus != 410 {
		t.Fatalf("got %v %d", res.Outcome, res.HTTPStatus)
	}
}

func TestHost(t *testing.T) {
	if Host("https://api.example.com:8443/hooks?x=1") != "api.example.com:8443" {
		t.Fatal(Host("https://api.example.com:8443/hooks"))
	}
}

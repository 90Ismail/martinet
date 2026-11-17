// sink is a test webhook receiver with controllable failure modes. It
// verifies signatures, dedupes by delivery id (as real receivers should),
// and reports counts the chaos test uses to check "nothing lost".
//
//	POST /ok              always 204
//	POST /flaky?p=0.3     fails with 503 with probability p
//	POST /slow?ms=2000    sleeps, then 204
//	POST /down            always 503   (trips the circuit breaker)
//	POST /gone            always 410   (permanent failure -> DLQ at once)
//	GET  /stats           JSON counters
//	POST /mode/down?on=1  flip /flaky and /ok to 503 at runtime (outage drill)
package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/90Ismail/martinet/internal/delivery"
	"github.com/90Ismail/martinet/internal/platform"
	"github.com/90Ismail/martinet/internal/signing"
)

type sink struct {
	log      *slog.Logger
	secrets  []string // accepted secrets; empty = skip verification
	outage   atomic.Bool
	mu       sync.Mutex
	unique   map[string]struct{}
	received atomic.Int64
	dupes    atomic.Int64
	badSig   atomic.Int64
	failed   atomic.Int64
}

func (s *sink) accept(w http.ResponseWriter, r *http.Request) bool {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if len(s.secrets) > 0 {
		ok := false
		for _, sec := range s.secrets {
			if signing.Verify(sec, body, r.Header.Get(signing.Header), time.Now(), 5*time.Minute) == nil {
				ok = true
				break
			}
		}
		if !ok {
			s.badSig.Add(1)
			http.Error(w, "bad signature", http.StatusUnauthorized)
			return false
		}
	}
	s.received.Add(1)
	id := r.Header.Get(delivery.HeaderDeliveryID)
	s.mu.Lock()
	if _, seen := s.unique[id]; seen {
		s.dupes.Add(1)
	}
	s.unique[id] = struct{}{}
	s.mu.Unlock()
	return true
}

func (s *sink) fail(w http.ResponseWriter, code int) {
	s.failed.Add(1)
	w.WriteHeader(code)
}

func main() {
	log := platform.Logger("sink")
	ctx, stop := platform.SignalContext()
	defer stop()
	s := &sink{log: log, unique: map[string]struct{}{}}
	if sec := platform.Env("SINK_SECRET", ""); sec != "" {
		s.secrets = []string{sec}
	}

	mux := http.NewServeMux()
	ok := func(w http.ResponseWriter, r *http.Request) {
		if s.outage.Load() {
			s.fail(w, http.StatusServiceUnavailable)
			return
		}
		if s.accept(w, r) {
			w.WriteHeader(http.StatusNoContent)
		}
	}
	mux.HandleFunc("POST /ok", ok)
	mux.HandleFunc("POST /flaky", func(w http.ResponseWriter, r *http.Request) {
		p, _ := strconv.ParseFloat(r.URL.Query().Get("p"), 64)
		if s.outage.Load() || rand.Float64() < p {
			s.fail(w, http.StatusServiceUnavailable)
			return
		}
		ok(w, r)
	})
	mux.HandleFunc("POST /slow", func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		time.Sleep(time.Duration(ms) * time.Millisecond)
		ok(w, r)
	})
	mux.HandleFunc("POST /down", func(w http.ResponseWriter, _ *http.Request) { s.fail(w, http.StatusServiceUnavailable) })
	mux.HandleFunc("POST /gone", func(w http.ResponseWriter, _ *http.Request) { s.fail(w, http.StatusGone) })
	mux.HandleFunc("POST /mode/down", func(w http.ResponseWriter, r *http.Request) {
		s.outage.Store(r.URL.Query().Get("on") == "1")
		log.Info("outage mode", "on", s.outage.Load())
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		unique := len(s.unique)
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"received": s.received.Load(), "unique_deliveries": unique, "duplicates": s.dupes.Load(),
			"bad_signatures": s.badSig.Load(), "failed_responses": s.failed.Load(), "outage": s.outage.Load(),
		})
	})
	platform.ServeHTTP(ctx, log, platform.Env("HTTP_ADDR", ":8088"), mux)
	<-ctx.Done()
}

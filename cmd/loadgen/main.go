// loadgen publishes events at a target rate over gRPC.
//
//	loadgen -ingest localhost:9090 -tenants 20 -rate 200 -duration 60s
//
// It sends a fixed fraction of requests twice with the same idempotency key
// to exercise dedupe, and prints the ids it published so the chaos test can
// reconcile them against the sink and the DLQ afterwards.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"time"

	martinetv1 "github.com/90Ismail/martinet/gen/martinet/v1"
	"github.com/90Ismail/martinet/internal/platform"
)

func main() {
	addr := flag.String("ingest", "localhost:9090", "ingest gRPC address")
	tenants := flag.Int("tenants", 10, "number of tenants")
	prefix := flag.String("prefix", "tenant", "tenant id prefix (tenants are <prefix>-000 ...)")
	rate := flag.Int("rate", 100, "events per second")
	dur := flag.Duration("duration", 30*time.Second, "how long to run")
	dupFrac := flag.Float64("dup", 0.05, "fraction of requests re-sent with the same idempotency key")
	out := flag.String("out", "", "write published event ids here (one per line)")
	workers := flag.Int("workers", 16, "concurrent publishers")
	flag.Parse()

	conn, err := platform.Dial(*addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()
	client := martinetv1.NewIngestClient(conn)

	var idsOut *os.File
	if *out != "" {
		if idsOut, err = os.Create(*out); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer idsOut.Close()
	}

	types := []string{"order.created", "order.paid", "order.shipped", "invoice.issued", "user.signed_up"}
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var sent, ok, dup, failed atomic.Int64
	var mu sync.Mutex
	work := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range work {
				tenant := fmt.Sprintf("%s-%03d", *prefix, rand.IntN(*tenants))
				req := &martinetv1.PublishEventRequest{
					TenantId:       tenant,
					Type:           types[rand.IntN(len(types))],
					Payload:        []byte(fmt.Sprintf(`{"seq":%d,"amount_cents":%d}`, n, rand.IntN(100000))),
					IdempotencyKey: fmt.Sprintf("lg-%d-%d", time.Now().UnixNano(), n),
					OccurredAtMs:   time.Now().UnixMilli(),
				}
				times := 1
				if rand.Float64() < *dupFrac {
					times = 2 // client retry with the same key
				}
				for i := 0; i < times; i++ {
					rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
					resp, err := client.PublishEvent(rctx, req)
					rcancel()
					sent.Add(1)
					switch {
					case err != nil:
						failed.Add(1)
					case resp.Duplicate:
						dup.Add(1)
					default:
						ok.Add(1)
						if idsOut != nil {
							mu.Lock()
							fmt.Fprintln(idsOut, resp.EventId)
							mu.Unlock()
						}
					}
				}
			}
		}()
	}

	tick := time.NewTicker(time.Second / time.Duration(max(*rate, 1)))
	defer tick.Stop()
	report := time.NewTicker(5 * time.Second)
	defer report.Stop()
	start := time.Now()
	n := 0
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-report.C:
			fmt.Printf("t=%5.1fs sent=%d ok=%d dup=%d failed=%d\n", time.Since(start).Seconds(), sent.Load(), ok.Load(), dup.Load(), failed.Load())
		case <-tick.C:
			select {
			case work <- n:
				n++
			default: // workers saturated: drop this tick rather than queue unbounded
			}
		}
	}
	close(work)
	wg.Wait()
	fmt.Printf("done in %s: sent=%d accepted=%d deduplicated=%d failed=%d (%.0f/s)\n",
		time.Since(start).Round(time.Millisecond), sent.Load(), ok.Load(), dup.Load(), failed.Load(),
		float64(sent.Load())/time.Since(start).Seconds())
}

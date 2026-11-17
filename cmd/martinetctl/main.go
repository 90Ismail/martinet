// martinetctl is the operator CLI. Everything goes over gRPC.
//
//	martinetctl init                                  create Kafka topics
//	martinetctl subscribe -tenant t1 -url http://... [-types order.*]
//	martinetctl subs -tenant t1
//	martinetctl publish -tenant t1 -type order.created -data '{"id":1}' [-key k]
//	martinetctl status <delivery_id>
//	martinetctl event <event_id>
//	martinetctl dlq list [-n 20]
//	martinetctl dlq replay [-n 0] [-delivery id] [-sub id]
//	martinetctl health
//	martinetctl mq stats
//	martinetctl carnet stats
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	carnetv1 "github.com/90Ismail/martinet/gen/carnet/v1"
	martinetv1 "github.com/90Ismail/martinet/gen/martinet/v1"
	mqv1 "github.com/90Ismail/martinet/gen/mq/v1"
	"github.com/90Ismail/martinet/internal/eventlog"
	"github.com/90Ismail/martinet/internal/platform"
	"github.com/90Ismail/martinet/internal/topics"
	"google.golang.org/grpc"
)

var (
	ingestAddr = platform.Env("MARTINET_INGEST", "localhost:9090")
	statusAddr = platform.Env("MARTINET_STATUS", "localhost:9091")
	poulsAddr  = platform.Env("MARTINET_POULS", "localhost:7071")
	carnetAddr = platform.Env("MARTINET_CARNET", "localhost:7070")
	mqAddr     = platform.Env("MARTINET_MQ", "localhost:7072")
)

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "martinetctl: "+format+"\n", a...)
	os.Exit(1)
}

func dial(addr string) *grpc.ClientConn {
	c, err := platform.Dial(addr)
	if err != nil {
		die("dial %s: %v", addr, err)
	}
	return c
}

func ms(t int64) string {
	if t == 0 {
		return "-"
	}
	return time.UnixMilli(t).Format("15:04:05.000")
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, strings.TrimSpace(usage))
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd, args := os.Args[1], os.Args[2:]
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	defer tw.Flush()

	switch cmd {
	case "init":
		fs := flag.NewFlagSet("init", flag.ExitOnError)
		rf := fs.Int("rf", 1, "replication factor")
		_ = fs.Parse(args)
		cfg := eventlog.FromEnv()
		if err := eventlog.EnsureTopics(ctx, cfg.Brokers, int16(*rf), topics.Specs()...); err != nil {
			die("%v", err)
		}
		fmt.Println("topics ready:", topics.Events, topics.EventsDLQ, topics.Attempts, topics.EndpointHealth)

	case "subscribe":
		fs := flag.NewFlagSet("subscribe", flag.ExitOnError)
		tenant := fs.String("tenant", "", "tenant id")
		url := fs.String("url", "", "endpoint url")
		types := fs.String("types", "", "comma separated event types (order.* ok)")
		_ = fs.Parse(args)
		var tl []string
		if *types != "" {
			tl = strings.Split(*types, ",")
		}
		sub, err := martinetv1.NewIngestClient(dial(ingestAddr)).CreateSubscription(ctx,
			&martinetv1.CreateSubscriptionRequest{TenantId: *tenant, Url: *url, EventTypes: tl})
		if err != nil {
			die("%v", err)
		}
		fmt.Fprintf(tw, "id\t%s\nurl\t%s\nsecret\t%s\n", sub.Id, sub.Url, sub.Secret)

	case "subs":
		fs := flag.NewFlagSet("subs", flag.ExitOnError)
		tenant := fs.String("tenant", "", "tenant id")
		_ = fs.Parse(args)
		r, err := martinetv1.NewIngestClient(dial(ingestAddr)).ListSubscriptions(ctx, &martinetv1.ListSubscriptionsRequest{TenantId: *tenant})
		if err != nil {
			die("%v", err)
		}
		fmt.Fprintln(tw, "ID\tURL\tTYPES")
		for _, s := range r.Subscriptions {
			fmt.Fprintf(tw, "%s\t%s\t%s\n", s.Id, s.Url, strings.Join(s.EventTypes, ","))
		}

	case "publish":
		fs := flag.NewFlagSet("publish", flag.ExitOnError)
		tenant := fs.String("tenant", "", "tenant id")
		typ := fs.String("type", "", "event type")
		data := fs.String("data", "{}", "JSON payload")
		key := fs.String("key", "", "idempotency key")
		_ = fs.Parse(args)
		r, err := martinetv1.NewIngestClient(dial(ingestAddr)).PublishEvent(ctx, &martinetv1.PublishEventRequest{
			TenantId: *tenant, Type: *typ, Payload: []byte(*data), IdempotencyKey: *key,
		})
		if err != nil {
			die("%v", err)
		}
		fmt.Printf("event %s (duplicate=%v)\n", r.EventId, r.Duplicate)

	case "status", "event":
		if len(args) != 1 {
			die("usage: martinetctl %s <id>", cmd)
		}
		c := martinetv1.NewStatusClient(dial(statusAddr))
		var deliveries []*martinetv1.DeliveryStatus
		if cmd == "status" {
			d, err := c.GetDelivery(ctx, &martinetv1.GetDeliveryRequest{DeliveryId: args[0]})
			if err != nil {
				die("%v", err)
			}
			deliveries = []*martinetv1.DeliveryStatus{d}
		} else {
			r, err := c.ListEventDeliveries(ctx, &martinetv1.ListEventDeliveriesRequest{EventId: args[0]})
			if err != nil {
				die("%v", err)
			}
			deliveries = r.Deliveries
		}
		for _, d := range deliveries {
			fmt.Printf("\n%s  last=%s  dead_lettered=%v\n", d.DeliveryId,
				strings.TrimPrefix(d.LastOutcome.String(), "ATTEMPT_OUTCOME_"), d.DeadLettered)
			fmt.Fprintln(tw, "  #\tAT\tOUTCOME\tHTTP\tLATENCY\tWORKER\tERROR")
			for _, a := range d.Attempts {
				fmt.Fprintf(tw, "  %d\t%s\t%s\t%d\t%dms\t%s\t%s\n", a.Attempt, ms(a.AttemptedAtMs),
					strings.TrimPrefix(a.Outcome.String(), "ATTEMPT_OUTCOME_"), a.HttpStatus, a.LatencyMs, a.Worker, a.Error)
			}
			tw.Flush()
		}

	case "dlq":
		if len(args) < 1 {
			die("usage: martinetctl dlq list|replay")
		}
		fs := flag.NewFlagSet("dlq", flag.ExitOnError)
		n := fs.Int("n", 0, "max messages")
		delivery := fs.String("delivery", "", "only this delivery id")
		sub := fs.String("sub", "", "only this subscription id")
		_ = fs.Parse(args[1:])
		c := martinetv1.NewAdminClient(dial(statusAddr))
		switch args[0] {
		case "list":
			r, err := c.ListDeadLetters(ctx, &martinetv1.ListDeadLettersRequest{Limit: uint32(*n)})
			if err != nil {
				die("%v", err)
			}
			fmt.Fprintf(tw, "DLQ depth: %d\n\nDELIVERY\tURL\tDEAD AT\tREASON\n", r.QueueDepth)
			for _, it := range r.Items {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", it.Job.DeliveryId, it.Job.Url, ms(it.DeadLetteredAtMs), it.Reason)
			}
		case "replay":
			r, err := c.ReplayDeadLetters(ctx, &martinetv1.ReplayDeadLettersRequest{
				Limit: uint32(*n), DeliveryId: *delivery, SubscriptionId: *sub,
			})
			if err != nil {
				die("%v", err)
			}
			fmt.Printf("replayed %d, skipped %d\n", r.Replayed, r.Skipped)
		default:
			die("unknown dlq command %q", args[0])
		}

	case "health":
		r, err := martinetv1.NewPulseClient(dial(poulsAddr)).ListEndpointHealth(ctx, &martinetv1.ListEndpointHealthRequest{})
		if err != nil {
			die("%v", err)
		}
		fmt.Fprintf(tw, "watermark %s\n\nENDPOINT\tSTATE\tFAIL RATE\tN\tP99\tWINDOW\tOPEN UNTIL\n", ms(r.WatermarkMs))
		for _, h := range r.Endpoints {
			fmt.Fprintf(tw, "%s\t%s\t%.0f%%\t%d\t%.0fms\t%s\t%s\n", h.Endpoint,
				strings.TrimPrefix(h.State.String(), "BREAKER_STATE_"), h.FailureRate*100, h.Attempts,
				h.P99LatencyMs, ms(h.WindowStartMs), ms(h.OpenUntilMs))
		}

	case "mq":
		st, err := mqv1.NewMQClient(dial(mqAddr)).Stats(ctx, &mqv1.StatsRequest{})
		if err != nil {
			die("%v", err)
		}
		fmt.Fprintln(tw, "QUEUE\tREADY\tDELAYED\tIN FLIGHT\tSENT\tACKED\tDEAD-LETTERED")
		for _, q := range st.Queues {
			fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\n", q.Name, q.Ready, q.Delayed, q.InFlight, q.Sent, q.Acked, q.DeadLettered)
		}

	case "carnet":
		st, err := carnetv1.NewCarnetClient(dial(carnetAddr)).Stats(ctx, &carnetv1.StatsRequest{})
		if err != nil {
			die("%v", err)
		}
		fmt.Fprintf(tw, "last sequence\t%d\nmemtable\t%d bytes\nflushes\t%d\ncompactions\t%d\nbloom negatives\t%d\n\nLEVEL\tFILES\tBYTES\n",
			st.LastSequence, st.MemtableBytes, st.Flushes, st.Compactions, st.BloomNegatives)
		for _, l := range st.Levels {
			fmt.Fprintf(tw, "L%d\t%d\t%d\n", l.Level, l.Files, l.Bytes)
		}

	default:
		fmt.Fprintln(os.Stderr, strings.TrimSpace(usage))
		os.Exit(2)
	}
}

const usage = `
usage: martinetctl <command> [flags]

  init                       create Kafka topics (KAFKA_BROKERS)
  subscribe                  -tenant -url [-types]
  subs                       -tenant
  publish                    -tenant -type [-data] [-key]
  status <delivery_id>       attempt history for one delivery
  event <event_id>           every delivery of one event
  dlq list|replay            inspect or replay the dead letter queue
  health                     circuit breaker table (from pouls)
  mq stats                   message queue depths
  carnet stats               storage engine stats

addresses: MARTINET_INGEST, MARTINET_STATUS, MARTINET_POULS, MARTINET_CARNET, MARTINET_MQ
`

// Package topics names the event-log topics and how they are configured.
package topics

import "github.com/90Ismail/martinet/internal/eventlog"

const (
	Events         = "events"          // tenant events, key = tenant id
	EventsDLQ      = "events.dlq"      // poison events fanout could not process
	Attempts       = "attempts"        // every delivery attempt, key = endpoint host
	EndpointHealth = "endpoint-health" // breaker state per endpoint (compacted)
)

func ptr(s string) *string { return &s }

// Specs are applied by `martinetctl init` and the Strimzi KafkaTopic CRs.
func Specs() []eventlog.TopicSpec {
	week := ptr("604800000")
	return []eventlog.TopicSpec{
		{Name: Events, Partitions: 12, Configs: map[string]*string{"retention.ms": week, "min.insync.replicas": ptr("2")}},
		{Name: EventsDLQ, Partitions: 3, Configs: map[string]*string{"retention.ms": ptr("2592000000")}},
		{Name: Attempts, Partitions: 12, Configs: map[string]*string{"retention.ms": week}},
		// Compaction keeps the newest record per key forever: a new courier
		// replays this topic from the start to rebuild the full breaker table.
		{Name: EndpointHealth, Partitions: 3, Configs: map[string]*string{
			"cleanup.policy": ptr("compact"), "min.compaction.lag.ms": ptr("60000"),
		}},
	}
}

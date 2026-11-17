#!/usr/bin/env bash
# Walk-through against the docker-compose stack: one healthy endpoint, one
# flaky one, one that is gone. Shows retries, the DLQ, and replay.
set -euo pipefail
cd "$(dirname "$0")/.."
CTL=./bin/martinetctl
T="demo$(date +%s)"

say() { printf '\n\033[1;36m$ %s\033[0m\n' "$*"; }

say "subscribe three endpoints for tenant $T"
$CTL subscribe -tenant "$T" -url http://sink:8088/ok -types 'order.*'
$CTL subscribe -tenant "$T" -url 'http://sink:8088/flaky?p=0.6'
$CTL subscribe -tenant "$T" -url http://sink:8088/gone
sleep 6

say "publish order.created twice with the same idempotency key"
$CTL publish -tenant "$T" -type order.created -data '{"order_id":42,"total_cents":1999}' -key order-42
EV=$($CTL publish -tenant "$T" -type order.created -data '{"order_id":42,"total_cents":1999}' -key order-42 | awk '{print $2}')

say "wait for the flaky endpoint to be retried (5s, 30s tiers)"
sleep 12
$CTL event "$EV"

say "dead letter queue"
$CTL dlq list -n 10

say "circuit breakers (computed by pouls, C++)"
$CTL health

say "storage engine"
$CTL carnet stats

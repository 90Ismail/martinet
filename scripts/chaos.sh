#!/usr/bin/env bash
# Chaos test against the docker-compose stack.
#
# Invariant (the only promise a webhook system can honestly make):
#
#   for every accepted event E and every subscription S that matches it,
#   the receiver eventually got delivery E:S at least once,
#   OR delivery E:S is in the dead letter queue.
#
# While load is running we SIGKILL a courier (its leases expire and mq
# redelivers), restart carnet (WAL recovery), and SIGKILL fanout (Kafka
# offsets not yet committed are re-read). Duplicates are expected and
# reported; losses fail the test.
set -euo pipefail

cd "$(dirname "$0")/.."
CTL=./bin/martinetctl
LOADGEN=./bin/loadgen
RATE=${RATE:-80}
DURATION=${DURATION:-45s}
TENANTS=3
PREFIX="chaos$(date +%s)"
FLAKY_URL="http://sink:8088/flaky?p=0.25&run=${PREFIX}"

say() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }

queue_backlog() {
  $CTL mq stats | awk '$1 == "deliveries" { print $2 + $3 + $4 }'
}

say "reset receiver counters"
docker compose restart sink >/dev/null
sleep 2

say "subscribing $TENANTS tenants ($PREFIX-*) to /ok and /flaky (25% 503s)"
for i in $(seq 0 $((TENANTS - 1))); do
  t=$(printf '%s-%03d' "$PREFIX" "$i")
  $CTL subscribe -tenant "$t" -url "http://sink:8088/ok?run=${PREFIX}" >/dev/null
  $CTL subscribe -tenant "$t" -url "$FLAKY_URL" >/dev/null
done
sleep 6 # let fanout's subscription cache expire

say "load: $RATE events/s for $DURATION"
$LOADGEN -ingest localhost:9090 -tenants $TENANTS -prefix "$PREFIX" -rate "$RATE" -duration "$DURATION" \
  -out /tmp/martinet-chaos-ids.txt >/tmp/martinet-chaos-loadgen.log 2>&1 &
LG=$!

sleep 8;  say "SIGKILL one courier";      docker kill --signal KILL "$(docker compose ps -q courier | head -1)" >/dev/null
sleep 6;  docker compose up -d courier >/dev/null
sleep 6;  say "restart carnet (WAL replay)"; docker compose restart carnet >/dev/null
sleep 8;  say "SIGKILL fanout";           docker compose kill -s KILL fanout >/dev/null
sleep 4;  docker compose up -d fanout >/dev/null

wait $LG
tail -1 /tmp/martinet-chaos-loadgen.log
ACCEPTED=$(sort -u /tmp/martinet-chaos-ids.txt | wc -l | tr -d ' ')
EXPECTED=$((ACCEPTED * 2))

counts() {
  STATS=$(curl -fsS localhost:8088/stats)
  UNIQUE=$(echo "$STATS" | python3 -c 'import json,sys; print(json.load(sys.stdin)["unique_deliveries"])')
  DUPES=$(echo "$STATS" | python3 -c 'import json,sys; print(json.load(sys.stdin)["duplicates"])')
  DEAD=$($CTL dlq list -n 100000 | grep -c "run=${PREFIX}" || true)
}

say "draining (retries back off 5s -> 30s -> 2m)"
for _ in $(seq 1 100); do
  counts
  printf '\r  delivered=%-6s dead=%-4s expected=%-6s queue backlog=%-6s' "$UNIQUE" "$DEAD" "$EXPECTED" "$(queue_backlog)"
  [ $((UNIQUE + DEAD)) -ge "$EXPECTED" ] && break
  sleep 3
done
echo
sleep 2
counts

say "result"
printf '  accepted events          %s\n' "$ACCEPTED"
printf '  expected deliveries      %s  (2 subscriptions each)\n' "$EXPECTED"
printf '  delivered (unique ids)   %s\n' "$UNIQUE"
printf '  dead-lettered            %s\n' "$DEAD"
printf '  duplicate deliveries     %s  (at-least-once at work)\n' "$DUPES"

if [ $((UNIQUE + DEAD)) -eq "$EXPECTED" ]; then
  printf '\n\033[1;32mPASS\033[0m delivered + dead-lettered == expected: nothing lost\n'
else
  printf '\n\033[1;31mFAIL\033[0m %s delivered + %s dead != %s expected\n' "$UNIQUE" "$DEAD" "$EXPECTED"
  exit 1
fi

#!/usr/bin/env bash
# Checks that every point and span in Redpanda reached ClickHouse exactly once, and that no
# heartbeat series went silent (M3, spans since M4). Exit code 0 means all clear.
#
#   bash scripts/check-loss.sh                       check the last 10 minutes
#   bash scripts/check-loss.sh -window 30m           any losscheck flag passes through
#   bash scripts/check-loss.sh --outage <scenario>   break the pipeline first, then check that stretch
#   bash scripts/check-loss.sh --all                 every scenario in turn, then a summary (~18 min)
#
# Scenarios (each about 2-3 minutes; DOWN=<seconds> changes the 60s outage):
#   processor       kill the processor (SIGKILL), keep it down, start it
#   clickhouse      stop ClickHouse; 20s in, kill the processor while it retries; then start both
#   redpanda        stop Redpanda, then start it
#   redpanda-agent  stop Redpanda; 20s in, restart the agent; then start Redpanda
#   agent           restart the agent (Redpanda up): what it had in flight is sent again
#   span-replay     produce the newest 100 spans of each partition again (already stored): the
#                   processor must skip every one, and ClickHouse must not hold a second copy
#
# Every scenario also checks the step before Taproot (S0): the demo's collector hands spans to
# the agent and retries while the agent can't take them. Its counters of spans it gave up on
# (send_failed + enqueue_failed for exporter otlp_http/taproot; they count only after its
# retries run out, checked 2026-11-23) must not move, and its queue must drain.
#
# Restarting the agent stops measurement while it is down: a hole that isn't pipeline loss. For
# those scenarios the gap threshold is raised to the restart's length plus 10s for the agent to
# find its services again, and the report says so.
set -uo pipefail
cd "$(dirname "$0")/.."

C="docker compose -f deploy/docker-compose.yml"
DOWN=${DOWN:-60}
SCENARIOS="processor clickhouse redpanda redpanda-agent agent span-replay"
REPLAY_PER_PARTITION=100
CURL_IMAGE=curlimages/curl:8.11.1 # reads the collector's metrics from inside the demo network
cid() { $C ps -aq "$1"; }
say() { echo "[$(date +%H:%M:%S)] $*"; }
state() { docker inspect -f '{{.State.Status}}' "$(cid "$1")"; }

# Waits for a compose service's health check (or just "running" if it has none).
wait_ready() {
  local id st; id=$(cid "$1")
  for _ in $(seq 1 90); do
    st=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$id")
    if [ "$st" = "healthy" ] || [ "$st" = "running" ]; then return 0; fi
    sleep 2
  done
  say "$1 not ready after 3 minutes"; return 1
}

# Waits until the processor's consumer groups (metrics and spans) have caught up. It commits
# after every batch, so lag reads close to 0; under 500 allows for records arriving meanwhile.
wait_caught_up() {
  local m s
  for _ in $(seq 1 120); do
    m=$(docker exec "$(cid redpanda)" rpk group describe taproot-processor 2>/dev/null | awk '$1 == "TOTAL-LAG" {print $2}')
    s=$(docker exec "$(cid redpanda)" rpk group describe taproot-processor-spans 2>/dev/null | awk '$1 == "TOTAL-LAG" {print $2}')
    if [[ "$m" =~ ^[0-9]+$ ]] && [[ "$s" =~ ^[0-9]+$ ]] && [ "$m" -lt 500 ] && [ "$s" -lt 500 ]; then
      say "processor caught up (lag: metrics $m, spans $s)"; return 0
    fi
    sleep 2
  done
  say "processor still behind after 4 minutes (lag: metrics ${m:-unknown}, spans ${s:-unknown})"; return 1
}

# Prints "<sent> <gave_up> <queue>" for the collector's exporter to the agent, or nothing if
# the collector's metrics can't be read. Counters it hasn't incremented yet are absent: 0.
collector_counts() {
  local out
  out=$(docker run --rm --network opentelemetry-demo "$CURL_IMAGE" -s -m 10 http://otel-collector:8888/metrics 2>/dev/null)
  [ -z "$out" ] && return 1
  echo "$out" | awk '/exporter="otlp_http\/taproot"/ {
      if ($1 ~ /^otelcol_exporter_sent_spans/) s += $NF
      else if ($1 ~ /^otelcol_exporter_(send|enqueue)_failed_spans/) f += $NF
      else if ($1 ~ /^otelcol_exporter_queue_size/) q += $NF
    } END { printf "%d %d %d\n", s, f, q }'
}

# S0: compares a collector_counts snapshot from before the scenario with one taken now.
check_collector() {
  local before=$1 after sent0 fail0 sent1 fail1 queue
  after=$(collector_counts)
  if [ -z "$before" ] || [ -z "$after" ]; then
    echo "FAIL  S0 collector: could not read its metrics (otel-collector:8888)"; return 1
  fi
  read -r sent0 fail0 _ <<< "$before"
  read -r sent1 fail1 queue <<< "$after"
  if [ "$sent1" -lt "$sent0" ]; then
    echo "FAIL  S0 collector restarted during the scenario: its counters reset, so this can't be checked"; return 1
  fi
  if [ "$fail1" -eq "$fail0" ] && [ "$queue" -eq 0 ]; then
    echo "PASS  S0 collector gave up on 0 spans (sent $(( sent1 - sent0 )) during the scenario, queue empty)"; return 0
  fi
  echo "FAIL  S0 collector gave up on $(( fail1 - fail0 )) spans, queue $queue (sent $(( sent1 - sent0 )))"; return 1
}

# Restarts the agent and sets gap_allow to how long it was down plus 10s.
restart_agent() {
  local t0; t0=$(date +%s)
  docker restart "$(cid agent)" >/dev/null
  gap_allow=$(( $(date +%s) - t0 + 10 ))
  say "agent restarted after $(( gap_allow - 10 ))s; allowing heartbeat gaps up to ${gap_allow}s"
}

# Produces the newest REPLAY_PER_PARTITION spans of each partition again, unchanged (same key
# and value): spans the processor has already stored, as a crash between write and commit or an
# agent resend would deliver them. Sets replayed and replay_since.
replay_spans() {
  local n; n=$(( REPLAY_PER_PARTITION * 3 ))
  replay_since=$(date -u -d '-10 seconds' +%Y-%m-%dT%H:%M:%SZ)
  replayed=$(docker exec "$(cid redpanda)" sh -c \
    "rpk topic consume taproot.spans -o -$REPLAY_PER_PARTITION -n $n -f '%k\t%v\n' | tee /tmp/replay | rpk topic produce taproot.spans -f '%k\t%v\n' >/dev/null && wc -l < /tmp/replay")
  say "replayed ${replayed:-0} spans"
}

# Passes when the processor logged skipping at least as many spans as were replayed.
check_replay() {
  local skipped
  skipped=$($C logs --since "$replay_since" processor 2>&1 | grep -oE 'skipped [0-9]+ of [0-9]+ spans' | awk '{s += $2} END {print s + 0}')
  if [ "${replayed:-0}" -gt 0 ] && [ "$skipped" -ge "$replayed" ]; then
    echo "PASS  R1 processor skipped $skipped spans as already stored ($replayed replayed)"; return 0
  fi
  echo "FAIL  R1 processor skipped $skipped spans as already stored, but $replayed were replayed"; return 1
}

# Runs one scenario and the checks over its stretch. Returns non-zero if any check failed.
outage() {
  local scenario=$1; shift
  local start; start=$(date +%s)
  local before; before=$(collector_counts)
  gap_allow=""; replayed=""
  case "$scenario" in
    processor)
      say "kill processor"; docker kill "$(cid processor)" >/dev/null
      sleep 2; say "processor is $(state processor)"
      sleep $(( DOWN - 2 )); say "start processor"; docker start "$(cid processor)" >/dev/null ;;
    clickhouse)
      say "stop clickhouse"; docker stop "$(cid clickhouse)" >/dev/null
      sleep 20; say "kill processor (it should be retrying a batch)"
      $C logs --since 25s processor 2>&1 | grep -E 'write .* failed' | tail -1
      docker kill "$(cid processor)" >/dev/null
      sleep $(( DOWN - 20 )); say "start clickhouse"; docker start "$(cid clickhouse)" >/dev/null
      wait_ready clickhouse; say "start processor"; docker start "$(cid processor)" >/dev/null ;;
    redpanda)
      say "stop redpanda"; docker stop "$(cid redpanda)" >/dev/null
      sleep "$DOWN"; say "start redpanda"; docker start "$(cid redpanda)" >/dev/null
      wait_ready redpanda ;;
    redpanda-agent)
      say "stop redpanda"; docker stop "$(cid redpanda)" >/dev/null
      sleep 20; say "restart agent (its disk buffer must survive)"; restart_agent
      sleep $(( DOWN - 20 )); say "start redpanda"; docker start "$(cid redpanda)" >/dev/null
      wait_ready redpanda ;;
    agent)
      say "restart agent"; restart_agent
      sleep 20 ;;
    span-replay)
      say "replay already-stored spans into taproot.spans"; replay_spans ;;
    *) echo "unknown scenario: $scenario (one of: $SCENARIOS)"; return 2 ;;
  esac
  wait_caught_up
  say "waiting 35s so the outage is older than the checker's 30s settle"
  sleep 35
  # Cover from 30s before the outage began to now.
  local window=$(( $(date +%s) - start + 30 ))
  local args=(-window "${window}s")
  [ -n "$gap_allow" ] && args+=(-gap "${gap_allow}s")
  local rc=0
  go run ./cmd/losscheck "${args[@]}" "$@" || rc=1
  check_collector "$before" || rc=1
  if [ -n "$replayed" ]; then check_replay || rc=1; fi
  return $rc
}

case "${1:-}" in
  --outage)
    scenario="${2:?usage: check-loss.sh --outage <scenario> (one of: $SCENARIOS)}"
    shift 2
    outage "$scenario" "$@"
    exit $? ;;
  --all)
    shift
    summary=""; failed=0
    for s in $SCENARIOS; do
      echo; say "=== scenario: $s ==="
      if outage "$s" "$@"; then r=PASS; else r=FAIL; failed=1; fi
      summary+=$(printf '%-16s %s' "$s" "$r")$'\n'
      sleep 30 # keep each scenario's window clear of the one before
    done
    echo; echo "=== summary (outages of ${DOWN}s) ==="; printf '%s' "$summary"
    exit $failed ;;
esac

# A plain check skips the first minute after the agent started: it measures nothing until it has
# found its services, which shows as a gap that isn't loss.
started=$(docker inspect -f '{{.State.StartedAt}}' "$(cid agent)" 2>/dev/null)
after=""
if [ -n "$started" ]; then
  after=$(date -u -d "$started + 60 seconds" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null)
fi
if [ -n "$after" ]; then
  go run ./cmd/losscheck -after "$after" "$@"
else
  go run ./cmd/losscheck "$@"
fi

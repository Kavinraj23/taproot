#!/usr/bin/env bash
# Checks that every point in Redpanda reached ClickHouse exactly once, and that no heartbeat
# series went silent (M3). Exit code 0 means all clear.
#
#   bash scripts/check-loss.sh                       check the last 10 minutes
#   bash scripts/check-loss.sh -window 30m           any losscheck flag passes through
#   bash scripts/check-loss.sh --outage <scenario>   break the pipeline first, then check that stretch
#   bash scripts/check-loss.sh --all                 every scenario in turn, then a summary (~15 min)
#
# Scenarios (each about 2-3 minutes; DOWN=<seconds> changes the 60s outage):
#   processor       kill the processor (SIGKILL), keep it down, start it
#   clickhouse      stop ClickHouse; 20s in, kill the processor while it retries; then start both
#   redpanda        stop Redpanda, then start it
#   redpanda-agent  stop Redpanda; 20s in, restart the agent; then start Redpanda
#   agent           restart the agent (Redpanda up): what it had in flight is sent again
#
# Restarting the agent stops measurement while it is down: a hole that isn't pipeline loss. For
# those scenarios the gap threshold is raised to the restart's length plus 10s for the agent to
# find its services again, and the report says so.
set -uo pipefail
cd "$(dirname "$0")/.."

C="docker compose -f deploy/docker-compose.yml"
DOWN=${DOWN:-60}
SCENARIOS="processor clickhouse redpanda redpanda-agent agent"
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

# Waits until the processor's consumer group has caught up (it commits after every batch, so
# lag reads close to 0; under 500 allows for points arriving meanwhile).
wait_caught_up() {
  local lag
  for _ in $(seq 1 120); do
    lag=$(docker exec "$(cid redpanda)" rpk group describe taproot-processor 2>/dev/null | awk '$1 == "TOTAL-LAG" {print $2}')
    if [[ "$lag" =~ ^[0-9]+$ ]] && [ "$lag" -lt 500 ]; then say "processor caught up (lag $lag)"; return 0; fi
    sleep 2
  done
  say "processor still behind after 4 minutes (lag ${lag:-unknown})"; return 1
}

# Restarts the agent and sets gap_allow to how long it was down plus 10s.
restart_agent() {
  local t0; t0=$(date +%s)
  docker restart "$(cid agent)" >/dev/null
  gap_allow=$(( $(date +%s) - t0 + 10 ))
  say "agent restarted after $(( gap_allow - 10 ))s; allowing heartbeat gaps up to ${gap_allow}s"
}

# Runs one scenario and the check over its stretch. Returns the checker's exit code.
outage() {
  local scenario=$1; shift
  local start; start=$(date +%s)
  gap_allow=""
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
    *) echo "unknown scenario: $scenario (one of: $SCENARIOS)"; return 2 ;;
  esac
  wait_caught_up
  say "waiting 35s so the outage is older than the checker's 30s settle"
  sleep 35
  # Cover from 30s before the outage began to now.
  local window=$(( $(date +%s) - start + 30 ))
  local args=(-window "${window}s")
  [ -n "$gap_allow" ] && args+=(-gap "${gap_allow}s")
  go run ./cmd/losscheck "${args[@]}" "$@"
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

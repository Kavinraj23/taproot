#!/usr/bin/env bash
# Checks that every point in Redpanda reached ClickHouse exactly once, and that no heartbeat
# series went silent (M3). Exit code 0 means all clear.
#
#   bash scripts/check-loss.sh                       check the last 10 minutes
#   bash scripts/check-loss.sh -window 30m           any losscheck flag passes through
#   bash scripts/check-loss.sh --outage <scenario>   break the pipeline first, then check that stretch
#
# Scenarios (each about 2-3 minutes):
#   processor       kill the processor (SIGKILL), keep it down 60s, start it
#   clickhouse      stop ClickHouse; 20s in, kill the processor while it retries; 60s in, start both
#   redpanda        stop Redpanda for 60s, then start it
#   redpanda-agent  stop Redpanda; 20s in, restart the agent; 60s in, start Redpanda
set -uo pipefail
cd "$(dirname "$0")/.."

C="docker compose -f deploy/docker-compose.yml"
cid() { $C ps -aq "$1"; }
say() { echo "[$(date +%H:%M:%S)] $*"; }

# Waits for a compose service's health check (or just "running" if it has none).
wait_ready() {
  local id; id=$(cid "$1")
  for _ in $(seq 1 90); do
    st=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$id")
    [ "$st" = "healthy" ] || [ "$st" = "running" ] && return 0
    sleep 2
  done
  say "$1 not ready after 3 minutes"; return 1
}

# Waits until the processor's consumer group is nearly caught up. Auto-commit runs every 5s and
# the agent sends ~40 points/s, so lag rarely reads exactly 0; under 500 is "caught up".
wait_caught_up() {
  for _ in $(seq 1 90); do
    lag=$(docker exec "$(cid redpanda)" rpk group describe taproot-processor 2>/dev/null | awk '$1 == "TOTAL-LAG" {print $2}')
    if [[ "$lag" =~ ^[0-9]+$ ]] && [ "$lag" -lt 500 ]; then say "processor caught up (lag $lag)"; return 0; fi
    sleep 2
  done
  say "processor still behind after 3 minutes (lag ${lag:-unknown})"; return 1
}

state() { docker inspect -f '{{.State.Status}}' "$(cid "$1")"; }

if [ "${1:-}" = "--outage" ]; then
  scenario="${2:?usage: check-loss.sh --outage processor|clickhouse|redpanda|redpanda-agent}"
  shift 2
  start=$(date +%s)
  case "$scenario" in
    processor)
      say "kill processor"; docker kill "$(cid processor)" >/dev/null
      sleep 2; say "processor is $(state processor)"
      sleep 58; say "start processor"; docker start "$(cid processor)" >/dev/null ;;
    clickhouse)
      say "stop clickhouse"; docker stop "$(cid clickhouse)" >/dev/null
      sleep 20; say "kill processor (it should be retrying a batch)"
      $C logs --since 25s processor 2>&1 | grep -E 'write .* failed' | tail -1
      docker kill "$(cid processor)" >/dev/null
      sleep 40; say "start clickhouse"; docker start "$(cid clickhouse)" >/dev/null
      wait_ready clickhouse; say "start processor"; docker start "$(cid processor)" >/dev/null ;;
    redpanda)
      say "stop redpanda"; docker stop "$(cid redpanda)" >/dev/null
      sleep 60; say "start redpanda"; docker start "$(cid redpanda)" >/dev/null
      wait_ready redpanda ;;
    redpanda-agent)
      say "stop redpanda"; docker stop "$(cid redpanda)" >/dev/null
      sleep 20; say "restart agent (its memory buffer goes with it)"; docker restart "$(cid agent)" >/dev/null
      sleep 40; say "start redpanda"; docker start "$(cid redpanda)" >/dev/null
      wait_ready redpanda ;;
    *) echo "unknown scenario: $scenario"; exit 2 ;;
  esac
  wait_caught_up
  say "waiting 35s so the outage is older than the checker's 30s settle"
  sleep 35
  # Cover from a minute before the outage began to now.
  window=$(( $(date +%s) - start + 60 ))
  set -- -window "${window}s" "$@"
fi

go run ./cmd/losscheck "$@"

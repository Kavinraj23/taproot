#!/usr/bin/env bash
# Live checks that the agent and processor compose services are moving points into ClickHouse.
# Exit code 0 means every check passed.
set -uo pipefail
cd "$(dirname "$0")/.."

C="docker compose -f deploy/docker-compose.yml"
HOST=docker-vm
failed=0
pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1: $2"; failed=1; }
# Retries an empty response (a blip while a freshly started stack is busy); errors still come through.
q() {
  local r
  for _ in 1 2 3; do
    r=$(curl -s -m 10 -u taproot:taproot 'http://localhost:8123/' --data-binary "$1")
    [ -n "$r" ] && break
    sleep 1
  done
  echo "$r"
}

# A1. Both services running and never restarted.
for svc in agent processor; do
  id=$($C ps -q "$svc" 2>/dev/null)
  if [ -z "$id" ]; then fail "A1 $svc" "no container"; continue; fi
  st=$(docker inspect -f '{{.State.Status}} {{.RestartCount}}' "$id")
  [ "$st" = "running 0" ] && pass "A1 $svc running, 0 restarts" || fail "A1 $svc" "status/restarts = $st"
done

# A2. About one new row per second for this host.
count="SELECT count() FROM taproot.metrics WHERE labels['host'] = '$HOST'"
c1=$(q "$count"); sleep 10; c2=$(q "$count")
if [[ "$c1" =~ ^[0-9]+$ && "$c2" =~ ^[0-9]+$ ]]; then
  d=$((c2 - c1))
  [ "$d" -ge 7 ] && [ "$d" -le 13 ] && pass "A2 $d new rows in 10s" || fail "A2 rows" "$d new rows in 10s, want 10 +/- 3"
else
  fail "A2 rows" "could not query ClickHouse ($c1 / $c2)"
fi

# A3. Newest point is fresh (ClickHouse and the agent share the Docker VM clock).
lag=$(q "SELECT dateDiff('second', max(ts), now64()) FROM taproot.metrics WHERE labels['host'] = '$HOST'")
[[ "$lag" =~ ^-?[0-9]+$ ]] && [ "$lag" -le 5 ] && pass "A3 newest point ${lag}s old" || fail "A3 freshness" "newest point ${lag}s old, want <= 5"

# A4. No failed writes or skipped records.
bad=$($C logs processor 2>&1 | grep -cE 'write .* failed|skip record')
[ "$bad" = "0" ] && pass "A4 no write failures or skipped records" || fail "A4 processor logs" "$bad problem lines"

exit $failed

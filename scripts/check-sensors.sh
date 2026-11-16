#!/usr/bin/env bash
# Live checks that discovery and the sensors measure the demo's Postgres, Valkey and Kafka with no
# configuration (M2). Exit code 0 means every check passed.
#
#   bash scripts/check-sensors.sh                      read-only checks C1-C4
#   bash scripts/check-sensors.sh --restart kafka      also C5: restart a container, time re-discovery
set -uo pipefail
cd "$(dirname "$0")/.."

AGENT=deploy-agent-1
# The demo's three services: kind and container.
SERVICES="postgres:astronomy-db valkey:valkey-cart kafka:kafka"
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
# Newest point across the table: the clock the agent stamps points with (the Docker VM's).
NOW="(SELECT max(ts) FROM taproot.metrics)"

restart=""
if [ "${1:-}" = "--restart" ]; then
  restart="${2:?usage: check-sensors.sh --restart <container>}"
fi

# C1. Each service is up, measured about once a second.
for s in $SERVICES; do
  kind=${s%%:*}; inst=${s#*:}
  r=$(q "SELECT count(), min(value) FROM taproot.metrics WHERE metric = '${kind}_up'
         AND labels['instance'] = '$inst' AND ts > $NOW - INTERVAL 10 SECOND FORMAT TSV")
  n=${r%%$'\t'*}; lo=${r##*$'\t'}
  if [[ "$n" =~ ^[0-9]+$ ]] && [ "$n" -ge 7 ] && [ "$n" -le 13 ] && [ "$lo" = "1" ]; then
    pass "C1 $kind $inst up, $n points in 10s"
  else
    fail "C1 $kind $inst" "$n points in 10s (want 10 +/- 3), lowest ${kind}_up ${lo:-none} (want 1)"
  fi
done

# C2. A headline metric per service arrives too, not just _up.
for m in postgres:astronomy-db:postgres_connections valkey:valkey-cart:valkey_connected_clients \
         kafka:kafka:kafka_consumergroup_lag; do
  kind=${m%%:*}; rest=${m#*:}; inst=${rest%%:*}; metric=${rest#*:}
  n=$(q "SELECT count() FROM taproot.metrics WHERE metric = '$metric'
         AND labels['instance'] = '$inst' AND ts > $NOW - INTERVAL 10 SECOND")
  [[ "$n" =~ ^[0-9]+$ ]] && [ "$n" -ge 1 ] && pass "C2 $metric for $inst, $n points in 10s" \
    || fail "C2 $metric for $inst" "${n:-no} points in 10s"
done

# C3. Taproot's own containers (compose project "deploy") are not measured.
own=$(q "SELECT arrayStringConcat(groupUniqArray(labels['instance']), ', ') FROM taproot.metrics
         WHERE labels['instance'] LIKE 'deploy-%' AND ts > $NOW - INTERVAL 60 SECOND")
[ -z "$own" ] && pass "C3 nothing from Taproot's own containers measured" \
  || fail "C3 own containers" "measured in the last minute: $own"

# C4. 1-minute rollups exist for all three in the last complete minute.
r=$(q "SELECT arrayStringConcat(arraySort(groupUniqArray(labels['service'])), ',') FROM taproot.metrics_1m
       WHERE minute = toStartOfMinute($NOW) - INTERVAL 1 MINUTE AND labels['service'] IN ('postgres', 'valkey', 'kafka')")
[ "$r" = "kafka,postgres,valkey" ] && pass "C4 rollups for kafka, postgres, valkey in the last full minute" \
  || fail "C4 rollups" "services with rollups in the last full minute: '${r}'"

# C5. Restart a container; the agent must log it found again and measure it (up = 1) within 90s.
# Most of that is the service starting: Kafka's JVM takes about a minute before it serves clients.
if [ -n "$restart" ]; then
  kind=""
  for s in $SERVICES; do [ "${s#*:}" = "$restart" ] && kind=${s%%:*}; done
  if [ -z "$kind" ]; then
    fail "C5 restart $restart" "not one of the demo's measured containers ($SERVICES)"
  else
    # Count "found" lines before, so clock differences between Windows and the Docker VM don't matter.
    found() { docker logs "$AGENT" 2>&1 | grep -cE "found +$kind +$restart "; }
    before=$(found)
    docker restart "$restart" >/dev/null
    start=$(date +%s); ok=""
    while [ $(( $(date +%s) - start )) -le 90 ]; do
      if [ "$(found)" -gt "$before" ]; then
        up=$(q "SELECT count() FROM taproot.metrics WHERE metric = '${kind}_up' AND labels['instance'] = '$restart'
                AND value = 1 AND ts > $NOW - INTERVAL 1 SECOND")
        if [[ "$up" =~ ^[0-9]+$ ]] && [ "$up" -ge 1 ]; then ok=$(( $(date +%s) - start )); break; fi
      fi
      sleep 1
    done
    [ -n "$ok" ] && pass "C5 restarted $restart: found again and measured after ${ok}s" \
      || fail "C5 restart $restart" "not found again and measured within 90s"
  fi
fi

exit $failed

#!/usr/bin/env bash
# Live checks for Taproot's Grafana setup. Exit code 0 means every check passed.
# Check 4 needs the pipeline (agent + processor) running so recent points exist.
#
#   GRAFANA_URL  (default http://localhost:3001)
#   GRAFANA_AUTH (default admin:admin)
set -uo pipefail

URL="${GRAFANA_URL:-http://localhost:3001}"
AUTH="${GRAFANA_AUTH:-admin:admin}"
PLUGIN=grafana-clickhouse-datasource
PLUGIN_VERSION=4.14.0
DS_UID=taproot-clickhouse
DASH_UID=taproot-host-cpu

failed=0
pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1: $2"; failed=1; }
# Retries an empty response (a blip while a freshly started stack is busy); errors still come through.
get() {
  local r
  for _ in 1 2 3; do
    r=$(curl -s -m 10 -u "$AUTH" "$URL$1")
    [ -n "$r" ] && break
    sleep 1
  done
  echo "$r"
}
# json EXPR: evaluate a Python expression over the JSON on stdin (bound to d).
json() { python -c "import sys, json; d = json.load(sys.stdin); print($1)" 2>/dev/null; }

# 1. Plugin installed at the pinned version.
v=$(get "/api/plugins/$PLUGIN/settings" | json "d['info']['version']")
[ "$v" = "$PLUGIN_VERSION" ] && pass "1 plugin $PLUGIN $v" || fail "1 plugin" "version '${v:-missing}', want $PLUGIN_VERSION"

# 2. Data source can reach and log in to ClickHouse.
s=$(get "/api/datasources/uid/$DS_UID/health" | json "d.get('status')")
[ "$s" = "OK" ] && pass "2 data source health" || fail "2 data source health" "status '${s:-missing}'"

# 3. Dashboard provisioned into the Taproot folder.
dash=$(get "/api/dashboards/uid/$DASH_UID")
meta=$(echo "$dash" | json "'%s|%s' % (d['meta']['folderTitle'], d['meta']['provisioned'])")
[ "$meta" = "Taproot|True" ] && pass "3 dashboard provisioned in Taproot" || fail "3 dashboard" "folder|provisioned = '${meta:-missing}'"

# 4. The dashboard's own panel query returns rows for the last 5 minutes.
body=$(echo "$dash" | python -c "
import sys, json
d = json.load(sys.stdin)
q = dict(d['dashboard']['panels'][0]['targets'][0], refId='A', datasource={'uid': '$DS_UID'})
print(json.dumps({'queries': [q], 'from': 'now-5m', 'to': 'now'}))" 2>/dev/null)
if [ -z "$body" ]; then
  fail "4 panel query" "could not read the panel query from the dashboard"
else
  res=$(curl -s -u "$AUTH" -H 'Content-Type: application/json' "$URL/api/ds/query" --data-binary "$body")
  out=$(echo "$res" | json "'%s|%d' % (d['results']['A'].get('error', ''), sum(len(f['data']['values'][0]) for f in d['results']['A'].get('frames', []) if f['data']['values']))")
  err=${out%|*}; rows=${out##*|}
  if [ -z "$out" ]; then fail "4 panel query" "unexpected response: $(echo "$res" | head -c 300)"
  elif [ -n "$err" ]; then fail "4 panel query" "$err"
  elif [ "$rows" -lt 1 ]; then fail "4 panel query" "no rows in the last 5 minutes (is the pipeline running?)"
  else pass "4 panel query returned $rows rows"; fi
fi

# 5. Exactly one of each, so restarts don't duplicate provisioned objects.
nds=$(get "/api/datasources" | json "sum(1 for x in d if x['type'] == '$PLUGIN')")
ndash=$(get "/api/search?query=Host%20CPU&type=dash-db" | json "sum(1 for x in d if x['title'] == 'Host CPU')")
[ "$nds" = "1" ] && [ "$ndash" = "1" ] && pass "5 one data source, one dashboard" || fail "5 duplicates" "$nds data sources, $ndash dashboards"

exit $failed

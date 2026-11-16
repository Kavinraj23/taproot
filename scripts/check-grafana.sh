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

# 3-5 for each dashboard: "uid|title".
for d in "taproot-host-cpu|Host CPU" "taproot-services|Services"; do
  uid=${d%%|*}; title=${d#*|}

  # 3. Dashboard provisioned into the Taproot folder.
  dash=$(get "/api/dashboards/uid/$uid")
  meta=$(echo "$dash" | json "'%s|%s' % (d['meta']['folderTitle'], d['meta']['provisioned'])")
  [ "$meta" = "Taproot|True" ] && pass "3 $title provisioned in Taproot" || fail "3 $title" "folder|provisioned = '${meta:-missing}'"

  # 4. Every panel query returns rows for the last 5 minutes (catches misspelt metric names).
  queries=$(echo "$dash" | python -c "
import sys, json
d = json.load(sys.stdin)
for p in d['dashboard']['panels']:
    for t in p['targets']:
        q = dict(t, refId='A', datasource={'uid': '$DS_UID'})
        name = p['title'] + (' (%s)' % t['refId'] if len(p['targets']) > 1 else '')
        print(name + '	' + json.dumps({'queries': [q], 'from': 'now-5m', 'to': 'now'}))" 2>/dev/null)
  if [ -z "$queries" ]; then
    fail "4 $title panel queries" "could not read the panel queries from the dashboard"
  fi
  while IFS=$'	' read -r ptitle body; do
    [ -z "$body" ] && continue
    res=""
    for _ in 1 2 3; do # retry a blank answer, as get() does
      res=$(curl -s -m 10 -u "$AUTH" -H 'Content-Type: application/json' "$URL/api/ds/query" --data-binary "$body")
      [ -n "$res" ] && break
      sleep 1
    done
    out=$(echo "$res" | json "'%s|%d' % (d['results']['A'].get('error', ''), sum(len(f['data']['values'][0]) for f in d['results']['A'].get('frames', []) if f['data']['values']))")
    err=${out%|*}; rows=${out##*|}
    if [ -z "$out" ]; then fail "4 $title / $ptitle" "unexpected response: $(echo "$res" | head -c 300)"
    elif [ -n "$err" ]; then fail "4 $title / $ptitle" "$err"
    elif [ "$rows" -lt 1 ]; then fail "4 $title / $ptitle" "no rows in the last 5 minutes (is the pipeline running?)"
    else pass "4 $title / $ptitle returned $rows rows"; fi
  done <<< "$queries"

  # 5. Exactly one of each dashboard, so restarts don't duplicate provisioned objects.
  n=$(get "/api/search?query=$(echo "$title" | sed 's/ /%20/g')&type=dash-db" | json "sum(1 for x in d if x['title'] == '$title')")
  [ "$n" = "1" ] && pass "5 one $title dashboard" || fail "5 $title duplicates" "$n dashboards"
done

# 5. One data source.
nds=$(get "/api/datasources" | json "sum(1 for x in d if x['type'] == '$PLUGIN')")
[ "$nds" = "1" ] && pass "5 one data source" || fail "5 data source duplicates" "$nds data sources"

exit $failed

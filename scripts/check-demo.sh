#!/usr/bin/env bash
# Live checks that the OTel demo is up with Taproot's memory override.
# Exit code 0 means every check passed.
set -uo pipefail

DEMO_URL="${DEMO_URL:-http://localhost:8080}"
LIMIT=67108864 # 64 MiB, from deploy/otel-demo/compose.taproot.yaml
failed=0
pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1: $2"; failed=1; }

# B1. Shop front end answers.
code=$(curl -s -o /dev/null -w '%{http_code}' "$DEMO_URL/")
[ "$code" = "200" ] && pass "B1 shop front end 200" || fail "B1 shop front end" "HTTP $code"

# B2. Product catalog works (the memory check's symptom was a 500 here).
body=$(curl -s -w '\n%{http_code}' "$DEMO_URL/api/products")
code=${body##*$'\n'}
n=$(echo "${body%$'\n'*}" | python -c "import sys, json; print(len(json.load(sys.stdin)))" 2>/dev/null)
[ "$code" = "200" ] && [ "${n:-0}" -gt 0 ] && pass "B2 /api/products returned $n products" || fail "B2 /api/products" "HTTP $code, ${n:-no} products"

# B3. The two Go services are healthy and carry our raised memory limit.
for c in product-catalog checkout; do
  st=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}} {{.HostConfig.Memory}}' "$c" 2>/dev/null)
  case "$st" in
    "healthy $LIMIT"|"running $LIMIT") pass "B3 $c ${st% *}, 64 MiB limit" ;;
    *) fail "B3 $c" "status/limit = ${st:-missing}" ;;
  esac
done

# B4. Total container memory, against the memory check's ceiling for the 6 GB Docker cap.
total=$(docker stats --no-stream --format '{{.MemUsage}}' | python -c "
import sys
units = {'B': 1, 'KiB': 2**10, 'MiB': 2**20, 'GiB': 2**30}
tot = 0
for line in sys.stdin:
    used = line.split('/')[0].strip()
    for u in sorted(units, key=len, reverse=True):
        if used.endswith(u):
            tot += float(used[:-len(u)]) * units[u]
            break
print(f'{tot / 2**30:.2f}')")
count=$(docker ps -q | wc -l)
# Informational only: container memory leaves out the VM's kernel, file cache and swap (see B5).
echo "INFO  B4 $count containers using $total GiB inside containers"

# B5. The Docker VM itself isn't thrashing. A sustained run once filled its swap with only
# 4.2 GiB used inside containers (docs/decisions/0004-demo-without-observability-stack.md).
# Swap use and even swap traffic are poor signals: Linux trades idle pages for file cache with memory
# to spare. Memory pressure (PSI) measures what matters, the share of time every task was stalled
# waiting on memory. Healthy runs show well under 1%; thrashing shows tens of percent.
read -r avail swap full60 <<<"$(docker run --rm alpine sh -c '
  p=$(awk "/^full/ {sub(\"avg60=\", \"\", \$3); print \$3}" /proc/pressure/memory)
  free -m | awk -v p="$p" "/^Mem:/ {m=\$7} /^Swap:/ {s=\$3} END {print m, s, p}"')"
if [ -z "${full60:-}" ]; then
  fail "B5 VM memory" "could not read /proc/pressure/memory"
elif python -c "import sys; sys.exit(0 if float('$full60') <= 5 and $avail >= 1000 else 1)"; then
  pass "B5 VM memory: ${avail} MB available, stalled on memory ${full60}% of the last minute (${swap} MB in swap)"
else
  fail "B5 VM memory" "${avail} MB available (want >= 1000), stalled on memory ${full60}% of the last minute (want <= 5%)"
fi

# B6. The demo's own observability backends stay off; Taproot replaces them.
running=$(docker ps --format '{{.Names}}' | grep -xE 'opensearch|jaeger|prometheus|grafana|opamp-server' | tr '\n' ' ')
[ -z "$running" ] && pass "B6 demo observability stack not running" || fail "B6 demo observability stack" "running: $running"

exit $failed

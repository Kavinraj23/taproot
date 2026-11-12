#!/usr/bin/env bash
# Starts everything: Taproot's stack (with agent and processor), then the OpenTelemetry demo.
#   TAPROOT_DEMO_DIR (default ../otel-demo)
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
DEMO_DIR="${TAPROOT_DEMO_DIR:-../otel-demo}"

bash scripts/demo-setup.sh

echo "Starting Taproot stack..."
# depends_on makes compose wait for the topic and a healthy ClickHouse before the pipeline starts.
docker compose -f deploy/docker-compose.yml up -d --build

echo "Starting OpenTelemetry demo..."
# The demo's own `make start-no-o11y` file set, plus Taproot's memory override. Its observability
# stack (OpenSearch, Jaeger, Prometheus, Grafana) duplicates Taproot and doesn't fit in the
# 6 GB Docker cap alongside it (docs/decisions/0004-demo-without-observability-stack.md).
# DEMO_VERSION in the environment beats the env files, so images are pinned even on a fresh clone.
(
  cd "$DEMO_DIR"
  DEMO_VERSION=3.1.0 docker compose --env-file .env --env-file .env.override \
    -f compose.yaml -f compose.full.yaml -f compose.extras.yaml \
    -f "$ROOT/deploy/otel-demo/compose.taproot.yaml" \
    up -d --remove-orphans
)

echo "Waiting for the demo shop..."
for _ in $(seq 1 60); do
  if [ "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/api/products)" = "200" ]; then
    break
  fi
  sleep 5
done

cat <<'EOF'

Up.
  Taproot Grafana  http://localhost:3001   (Dashboards > Taproot > Host CPU)
  Demo shop        http://localhost:8080
  Feature flags    http://localhost:8080/feature/
Checks: bash scripts/check-pipeline.sh; bash scripts/check-grafana.sh; bash scripts/check-demo.sh
EOF

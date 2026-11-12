#!/usr/bin/env bash
# Stops the OpenTelemetry demo, then Taproot's stack. Data volumes are kept.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
DEMO_DIR="${TAPROOT_DEMO_DIR:-../otel-demo}"

if [ -d "$DEMO_DIR" ]; then
  (
    cd "$DEMO_DIR"
    # Same file set as up.sh; --remove-orphans also clears the demo's observability containers
    # left from runs that included it.
    DEMO_VERSION=3.1.0 docker compose --env-file .env --env-file .env.override \
      -f compose.yaml -f compose.full.yaml -f compose.extras.yaml \
      -f "$ROOT/deploy/otel-demo/compose.taproot.yaml" \
      down --remove-orphans
  )
fi
docker compose -f deploy/docker-compose.yml down

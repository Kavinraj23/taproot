#!/usr/bin/env bash
# Makes sure the OpenTelemetry demo is cloned at the pinned release next to this repo.
#   TAPROOT_DEMO_DIR (default ../otel-demo)
set -euo pipefail
cd "$(dirname "$0")/.."

DEMO_TAG=3.1.0
DEMO_DIR="${TAPROOT_DEMO_DIR:-../otel-demo}"

if [ ! -d "$DEMO_DIR/.git" ]; then
  echo "Cloning opentelemetry-demo $DEMO_TAG into $DEMO_DIR"
  git clone --quiet --depth 1 --branch "$DEMO_TAG" https://github.com/open-telemetry/opentelemetry-demo "$DEMO_DIR"
  exit 0
fi

tag=$(git -C "$DEMO_DIR" describe --tags --exact-match 2>/dev/null || echo "not on a tag")
if [ "$tag" != "$DEMO_TAG" ]; then
  echo "WARNING: $DEMO_DIR is at '$tag', not $DEMO_TAG. Images are still pinned to $DEMO_TAG," >&2
  echo "         but compose files may differ. Fix with: git -C $DEMO_DIR checkout $DEMO_TAG" >&2
fi

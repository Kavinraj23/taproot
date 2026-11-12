# Taproot

A mini observability system: discovers running services with no configuration, collects metrics and traces without losing data, and identifies the root cause of failures with evidence. See `docs/design.md`.

Status: **M1 (walking skeleton) done.** Host CPU travels agent → Redpanda → processor → ClickHouse → Grafana, alongside the OpenTelemetry demo.

## Run

Needs Docker Desktop (WSL2) with the Docker VM capped at 6 GB. In `%UserProfile%\.wslconfig`:

```
[wsl2]
memory=6GB
autoMemoryReclaim=gradual
```

Then, from Git Bash or any bash:

```
bash scripts/up.sh     # Taproot stack + the OTel demo (clones it to ../otel-demo at 3.1.0 if missing)
bash scripts/down.sh   # stop everything; data volumes are kept
make check             # or run scripts/check-{pipeline,grafana,demo}.sh directly
```

| What | Where |
| --- | --- |
| Taproot Grafana (admin/admin) | http://localhost:3001 → Dashboards → Taproot → Host CPU |
| Demo shop | http://localhost:8080 |
| Demo feature flags (failure scenarios) | http://localhost:8080/feature/ |

The demo runs without its own observability stack (OpenSearch, Jaeger, Prometheus, Grafana) so that everything fits in memory. See `docs/decisions/0004-demo-without-observability-stack.md`.

On a 16 GB laptop, Windows itself is the tight side (about 0.5 GB free with the stack up), so close heavy apps such as browsers during demos. Measurements are in `docs/experiments/2026-11-04-memory-check.md`.

## Develop

```
make test   # Go and Python unit tests
make lint
TAPROOT_BROKERS=localhost:9092 go test -tags integration ./internal/sink/
TAPROOT_CLICKHOUSE_ADDR=localhost:9000 go test -tags integration ./internal/store/
```

# Taproot

A mini observability system: discovers running services with no configuration, collects metrics and traces without losing data, and identifies the root cause of failures with evidence. See `docs/design.md`.

Status: **M2 (discovery and sensors) done.** With no target configuration, the agent finds the demo's Postgres, Valkey and Kafka by scanning `/proc` (named through Docker, re-discovered within seconds of a restart) and measures each every second: connections and transactions, cache clients and hits/misses, Kafka consumer lag and throughput. Points travel agent → Redpanda → processor → ClickHouse, which keeps 1-second data for 48 hours and 1-minute rollups for 30 days, and Grafana shows them. M1 (walking skeleton: host CPU end to end, alongside the OpenTelemetry demo) is done too.

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
make check             # or run scripts/check-{pipeline,grafana,demo,sensors}.sh directly
bash scripts/check-sensors.sh --restart kafka   # also restart a demo container and time re-discovery
```

| What | Where |
| --- | --- |
| Taproot Grafana (admin/admin) | http://localhost:3001 → Dashboards → Taproot → Services (Postgres, Valkey, Kafka) and Host CPU |
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
# throwaway Postgres: docker run -d --name pg-it -e POSTGRES_PASSWORD=pw -p 55432:5432 postgres:18.4
TAPROOT_PG_TEST_ADDR=localhost:55432 go test -tags integration ./internal/sensor/
# throwaway Valkey: docker run -d --name vk-it -p 56379:6379 valkey/valkey:9.0.4-alpine
TAPROOT_VALKEY_TEST_ADDR=localhost:56379 go test -tags integration ./internal/sensor/
# Kafka sensor against Taproot's own Redpanda (stack running)
TAPROOT_KAFKA_TEST_ADDR=localhost:9092 go test -tags integration ./internal/sensor/
```

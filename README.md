# Taproot

A mini observability system: discovers running services with no configuration, collects metrics and traces without losing data, and identifies the root cause of failures with evidence. See `docs/design.md`.

Status: scaffold only (pre-M1).

```
make test   # Go and Python tests
make up     # start Redpanda, ClickHouse, Postgres, Grafana
make down
```

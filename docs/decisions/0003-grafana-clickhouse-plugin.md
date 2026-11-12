# 0003: Grafana ClickHouse plugin pinned to 4.14.0

- **Decision:** Grafana reads ClickHouse through `grafana-clickhouse-datasource` pinned to 4.14.0. Grafana itself stays at 11.4.0. The data source and the "Host CPU" dashboard are provisioned from files in `deploy/grafana/provisioning/`, so they come back identically on every start and can't be edited in the UI.
- **Why:** Every plugin release from 4.15 onward requires Grafana 11.6 or later (checked against the grafana.com versions API on 2026-11-11). Pinning keeps Grafana at the version the memory check measured. The panel query uses the plugin's time series format (`format: 0`), which turns the `host` column into one line per host. The table format draws all hosts as one line.
- **Revisit:** When M4/M5 evaluate Grafana's trace view and node graph for ClickHouse data, upgrade Grafana to 11.6 or later and the plugin to the current 4.x together.
- **Replaces:** Nothing. Adds detail to section 6 (Showing results: Grafana connected to ClickHouse).

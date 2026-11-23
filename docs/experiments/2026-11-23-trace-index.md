# Does the trace index pay off? (M4, task 7)

Task 4 found that looking a trace up through `spans_trace_index` read no fewer rows than scanning `spans`, at 15k spans. This re-measures at a size where it can matter.

## Setup
- `m4-traces` after task 6, full demo + Taproot, default load. `taproot.spans`: **748,820 rows, 11 active parts, 111 granules** (19:18–21:13 UTC, one day partition). Span rate ~112/s (see the 2026-11-23 section of `2026-11-18-span-volume.md`).
- 5 trace IDs with a checkout span, one from each fifth of the table's age.
- For each, the two queries `store.Trace` runs (index, then the time slice of `spans` with bounds widened to whole seconds) and a plain `SELECT … FROM spans WHERE trace_id = ?`. `read_rows`, `read_bytes` and duration come from `system.query_log`. Each query ran once, cold apart from the OS cache.

## Results

| Step | avg rows read | avg bytes | avg ms |
|---|---|---|---|
| 1. index (`spans_trace_index`) | 16,426 | 809 KiB | 32.6 |
| 2. time slice of `spans` | 216,643 | 9.0 MiB | 20.2 |
| plain scan by `trace_id` | 749,782 | 29.3 MiB | 23.8 |

Step 2 per trace: 739,731 · 106,697 · 65,536 · 106,106 · 65,146 rows.

- **Normal traces read 7–11× fewer rows** (65k–107k of 750k). `EXPLAIN indexes = 1` shows the saving is mostly **whole parts skipped by their min/max `start`** (1 of 11 parts), then the `(service, start)` key trims granules inside the part (14 of 21). Parts are written in time order, so a short time range lands in one or two of them.
- **One trace read 740k rows: a 1 h 51 min "trace".** The demo's flag clients (payment, ad, recommendation, …) hold a 10-minute streaming `flagd.evaluation.v2.Service/EventStream` RPC, and each reconnect stays in the trace of the request that first opened it. Its index card spans hours, so the slice is the whole table. 62 of 146,497 traces (0.04%) last over a minute.
- **Not faster yet in wall time.** Two round trips (≈ 53 ms) against one scan (≈ 24 ms). At 750k rows ClickHouse scans 29 MiB in about the time the extra query costs.

## What it means
- The design's option C (design section 5, decision 0008) works the way it claims: rows read stay near one part's worth, while a scan grows with the table. At 48 h of this rate (~19M rows, ~2.8 GB compressed) the scan would read ~25× today's bytes, and the slice about the same as now. That's a projection, not measured here.
- Long-lived streaming spans are the weak spot. If they matter later, the fix is in the index, not the lookup: record each trace's range from its *start* times only, or cap a span's contribution. A `bloom_filter` skip index on `trace_id` would avoid relying on time at all. Neither is changed now.
- The lookup is interactive (one trace at a time), so 50 ms is fine. Nothing needs to change for M4.

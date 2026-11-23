# Span volume from the OTel demo (M4, task 1)

How many spans the demo produces, so the span path (agent spool, Redpanda, ClickHouse) can be sized before it's built.

## Setup
- Full demo (decision 0004 file set) plus Taproot's stack, on `main` at `97eb6d2`, measured a few minutes after Docker Desktop started, with all services healthy and the shop answering 200.
- Load: the demo's default load generator, `LOCUST_USERS=5`.
- Method: the collector's `debug` exporter logs one line per export with a `"spans": N` count. The sum over a window, divided by its length, gives the rate. The trace pipeline has no batch processor, so each line is one incoming OTLP request (up to about 100 spans each).
  `docker logs otel-collector --since 5m 2>&1 | grep 'otelcol.signal": "traces"' | grep -o '"spans": [0-9]*'`
- The collector's log sampler only applies above 10 identical lines per second. About 1.4 exports per second came in, so nothing was sampled away.

## Results

| Window | Spans/s |
|---|---|
| 2 min | 10.8 |
| 5 min, per minute (full minutes only) | 2.4 (warming up), 7.6, 9.3, 4.7 |

Steady state is **about 5–11 spans/s**. The largest single export held 102 spans.

## What it means
- **Agent spool (task 2):** at about 600 bytes of JSON per span, that's about 6 KB/s. A separate 256 MB span spool holds about 12 hours, longer than the metrics spool's 9 hours, so the same cap works.
- **ClickHouse:** 48 hours is about 1–2 million spans, likely 100–200 MB compressed. That fits easily.
- **Agent memory (64 MiB limit, 23 MiB used):** an OTLP request of about 100 spans is a few tens of KB, so there's no reason to raise the limit now. Check it again in task 2 under the Kafka overload flag.
- **Caveat:** this is the default light load. Re-measure in task 2 from the agent's own counters over a longer steady period, and with `kafkaQueueProblems` on.

## 2026-11-23: re-measured, 10× more (M4 task 7)

Measured from ClickHouse (`count()` per service over a full 10-minute window, every span stored exactly once per task 7's checks), on `m4-traces` after task 6, default load (`LOCUST_USERS=5`).

| Service | spans/s |
|---|---|
| frontend-proxy | 32.7 |
| frontend | 27.0 |
| flagd | 13.5 |
| frontend-web (browser spans, new since 11-18) | 12.8 |
| product-catalog | 10.1 |
| image-provider | 4.7 |
| all others | ~11 |
| **total** | **112.3** |

- **Size:** 1,354 bytes per span record on `taproot.spans` (average of 1,200), not the ~600 estimated above. So **~148 KB/s**.
- **Agent span spool (256 MB): about 29 minutes** of a Redpanda outage, not 12 hours. Task 7's 60-second outages are well inside it, but a long one would hit the spool's full-buffer behaviour (decision 0007 defers its priorities). **Raised to 1 GB (~2 hours) the same day** (decision 0008, span buffer size).
- **ClickHouse:** 109 MiB compressed for 756k spans. 48 hours at this rate is ~19M spans, **~2.8 GB compressed**. The Docker disk has 938 GB free, so 48 h retention stays.
- **Pipeline at this rate:** processor span lag 0, processor 28 MiB of 128 at ~10% CPU, agent 25 MiB of 64.
- Why it differs from 11-18: a `frontend-web` service (browser-side spans) now reports, and frontend/frontend-proxy are several times busier. The cause isn't investigated. The load generator settings are unchanged.

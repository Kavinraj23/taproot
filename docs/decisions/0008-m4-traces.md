# 0008: Traces (M4): spans through the agent, span tables, dedup by trace and span ID

- **Decision:**
  - **Span path.** demo services → the demo's OTel collector → the Taproot agent (an OTLP receiver) → a disk spool → Redpanda topic `taproot.spans` → the processor → ClickHouse. The collector gets an exporter to the agent through the demo's own extension seam, `otelcol-config-extras.yml`. The agent answers a span export only after the spans are in its spool. (Built in tasks 2–3.)
  - **Span model** (`model.Span`): trace ID, span ID, parent span ID (lowercase hex), service, name, kind, start (ns), duration, status (unset/ok/error), span attributes and resource attributes as string maps. Events and links are left out for now.
  - **Storage** (design section 5, option C). `spans` is a MergeTree sorted by `(service, start)` and partitioned by day. `spans_trace_index` (AggregatingMergeTree, `ORDER BY trace_id`) holds each trace's earliest start and latest end. A materialized view fills it from `spans`. A trace lookup reads the index first, then only that time slice of `spans`. Both tables keep 48 hours, like raw metrics (decision 0006).
  - **Dedup.** A span's identity is `model.Span.Key`: trace ID plus span ID, because a span ID is only unique inside its trace. `store.InsertNewSpans` follows `InsertNew` (decision 0007): drop repeats in the batch, look up which keys are already stored (one query over the batch's services and start range, using the sort key), and insert only the rest.
- **Why:**
  - *Through the agent, not the collector's Kafka exporter straight into Redpanda:* design section 5 (Transport) has agents send to the queue and buffer locally on disk. Going through the agent reuses M3's lossless spool. The collector only buffers in memory, and it would skip the agent the design (and Instana's model) puts on every host. The cost is an OTLP receiver in the agent. Between the collector and the agent, the collector's own retry queue (in memory) covers short agent restarts.
  - *Index table, not a skip index on trace_id:* this is what design section 5 chose. RED metrics and the engine read by service and time, and the small index keeps trace lookups cheap.
  - *Min/max in the index:* repeats don't change a min or a max, so the index can't be skewed by a duplicate even before dedup.
  - *Volume:* about 5–11 spans/s at the demo's default load (`docs/experiments/2026-11-18-span-volume.md`), so one lookup per batch costs little.
- **Ownership:** on 2026-11-18 the user extended the decision 0007 waiver of hand-written piece 2 to span dedup. Claude wrote `InsertNewSpans` and explains it back at the end of M4, together with the M3 explain-back. Piece 1 (context propagation in the Python worker, task 6) stays hand-written.
- **Naming:** the plan called the index `trace_index`. It is named after its spans table (`spans_trace_index`), so tests can create their own pairs side by side.
- **Replaces:** Nothing. Adds detail to design.md section 5 (Transport, Trace assembly, Storage, and "Data arriving twice") and section 9 (ownership).

## Task 2: the agent's span intake (2026-11-18)

- **Decision:**
  - **OTLP over HTTP, not gRPC.** The agent serves `POST /v1/traces` on `:4318` (`internal/otlp`), protobuf or JSON, gzip or not, up to 16 MiB per request. It decodes with the collector's own `pdata` types, because OTLP/JSON writes IDs in hex, which plain protojson would read as base64.
  - **"OK" only after the disk.** The receiver answers 200 only once every span of the export is appended to the span spool. A failed append gets 503 + `Retry-After`, and the collector resends. A body that can never work (not OTLP, too big, wrong type) gets 400/413/415, and the collector drops it instead of retrying forever. Spans without a trace or span ID are skipped and counted.
  - **A separate spool and topic.** Spans go to their own spool (`/var/lib/taproot/buffer/spans`, inside the existing volume, same 256 MB cap) and to the topic `taproot.spans` (3 partitions, keyed by trace ID). A flood of spans can't push metrics out of their buffer, and the "buffer full" priorities 0007 defers can be set per kind later. `sink.Append` is the shared write path, and `Emit` (points) is built on it.
  - **Collector wiring without editing the demo.** `deploy/otel-demo/otelcol-config-extras.yml` replaces the demo's empty extras file via `OTEL_COLLECTOR_CONFIG_EXTRAS` in `scripts/up.sh`. It adds `otlp_http/taproot` → `http://taproot-agent:4318` next to the demo's `debug` and `span_metrics` exporters. `up.sh` (re)connects the agent to the demo network with the alias `taproot-agent`.
- **Why:**
  - *HTTP:* it's the same OTLP data with the same collector support, without adding gRPC to the agent, and it can be tested with `httptest` and poked with `curl`.
  - *Answer after the disk:* this is the clerk-and-safe rule. The collector keeps unacknowledged exports in its in-memory queue and retries with backoff, so the handoff is at-least-once. A retry after a partial append re-sends spans already on disk, and the processor's dedup (`InsertNewSpans`) drops them.
- **Checked on 2026-11-18:** 19 demo services' spans reached `taproot.spans` (including checkout, accounting and fraud-detection). With the agent stopped for 20s, the collector logged 30 retries with backoff. After the restart, the agent received 3463 spans in its first minute, the backlog included. Agent memory was 17 MiB of 64. Counting every span end to end through an outage is task 7.
- **Limits:** the collector's queue is in memory, so spans waiting there are lost if the collector itself restarts during an agent outage. That's outside Taproot (it's the demo's collector), and in production the app SDKs would send to the agent directly. The rate right after a full restart (about 40 spans/s, mostly frontend and flagd) is higher than the steady 5–11/s measured before. Both are far below what the spool and Redpanda can take.

## Task 3: the processor stores spans (2026-11-20)

- **Decision:**
  - **One loop, two kinds.** `processor.Run` is generic: `Loop[T]` takes the record's `Decode` alongside Poll/Write/Commit, and `Config` stays as `Loop[model.Point]`. Commit-after-write, poison-skipping and write retry with backoff are the same code for points and spans. Every existing processor test passes with only `Decode: Decode` added.
  - **Two loops in one process.** `cmd/processor` runs the metrics loop and a span loop in goroutines, sharing one ClickHouse connection. Each has its own consumer group (`taproot-processor`, unchanged, so metrics keep their committed offsets; `taproot-processor-spans`), so a stalled span batch doesn't hold up metrics, and the reverse.
  - **`DecodeSpan` rejects** bad JSON, trace IDs that aren't 32 lowercase hex, span IDs that aren't 16, all-zero IDs, an empty service, and a zero start. Rejected records are logged, skipped and committed like any poison record. The agent writes lowercase hex, so anything else is corruption that would break dedup.
  - **First start reads from the beginning** (`AtStart`), so spans already in the topic get stored.
- **Checked on 2026-11-20:** 18698 distinct spans in `taproot.spans`, all in ClickHouse, and `count() - uniqExact(trace_id, span_id)` = 0. The processor was killed for 30s mid-stream and caught up after restarting, still with 0 duplicates and 0 missing. Metrics `check-loss` 0/0 over the same window. Processor memory 28 MiB of 128. A real order trace (137 spans): load-generator → frontend-proxy → frontend → checkout → cart, product-catalog, currency, shipping → quote, payment, email → `publish orders`.
- **Limits:**
  - The Docker reset on 2026-11-18 wiped the task 2 backlog, including the collector resends. So this run had no repeats in the topic, and the processor kill didn't land between a write and its commit. Dedup on real repeats is still proven only by the store tests. Task 7's outage proof should create repeats on purpose.
  - **Kafka breaks the demo's traces.** In demo 3.1.0, accounting and fraud-detection start a new trace at `receive orders` (empty parent), so no stored trace contains both checkout and those consumers. Tying them back needs span links (left out of `model.Span`) or the propagation task 6 teaches. This matters for the root-cause walk: right now the trace graph can't connect checkout to its Kafka consumers. (Checkout does put `traceparent` on every `orders` message; the demo's consumers just don't use it as their parent. Taproot's own order workers do: see Task 6.)

## Task 4: looking up one trace (2026-11-23)

- **Decision:**
  - **Two reads.** `store.Trace` first reads the trace's time range from `spans_trace_index` (`min(start_min)`, `max(end_max)`, aggregated because index rows merge in the background), then only that time slice of `spans`, filtered by trace ID. No index row means `ErrTraceNotFound`.
  - **Whole-second bounds.** The slice is `[start of the first span's second, end of the last end's second + 1s)`. clickhouse-go binds a `time.Time` at second precision, so exact bounds would miss a span starting late in the trace's last second. The integration test for this fails if the widening is removed (checked).
  - **Tree on read, in `internal/trace`.** `Build` hangs each span under its parent. Siblings are ordered by start, then span ID. A span whose parent isn't stored is shown as a root marked "(parent not stored)", not hidden. Corrupt parent loops can't hang it, and every span appears once. `Render` prints offset, duration, branches, and markers for errors, producer/consumer spans and orphans.
  - **CLI `cmd/trace`.** `go run ./cmd/trace <id>` prints the tree. `-service <name>` lists the newest traces that include that service (`RecentTraces`, which reads `spans` in its own (service, start) order). IDs are lowercased and checked with `model.ValidTraceID`, the same rule the processor's `DecodeSpan` now uses (moved from the processor to `model`). Exit codes: 0 found, 1 not found or error, 2 bad usage.
  - **Grafana's trace view is deferred to M5**, together with the Grafana 11.6+ upgrade the node graph also needs (decision 0003).
- **Checked on 2026-11-23:** a healthy order trace (73 spans, 12 services, 166 ms) shows load-generator → frontend → checkout → shipping → quote, payment, email → `publish orders` with no orphans. Printed span counts equal `count()` in ClickHouse. A fraud-detection trace starts at its own `receive orders` consumer span (Task 3 Limits). Bad ID → exit 2, unknown ID → exit 1, uppercase ID → found.
- **Limits:**
  - **The index doesn't pay off yet at this size.** With 15,370 spans stored (just after a restart), the lookup read 2,827 index rows + 14,881 span rows, while a plain `WHERE trace_id = …` scan read 14,972. The table is only about two 8,192-row blocks per part, and a time filter can only skip whole blocks. The saving should show when the table holds many blocks (48 h of spans). Re-measure then (task 7) before claiming it. **Task 7:** at 749k spans, normal traces read 7–11× fewer rows through the index, but it's not yet faster in wall time, and traces stretched by long streaming spans defeat it (`docs/experiments/2026-11-23-trace-index.md`).
  - **Startup orphans.** In the first two minutes after `up.sh`, three checkout traces lacked checkout's `PlaceOrder` server span, so its children printed as orphans. In those traces frontend's checkout call timed out at 10.5 s. The collector also couldn't resolve `taproot-agent` until `up.sh` connected the alias (37 retried exports). Every trace after that was whole. Whether the span was dropped upstream or never ended is for task 7's end-to-end span count. **Task 7:** checkout panicked (nil pointer) and restarted 5 times while the demo started, and the spans it hadn't exported died with the process. That's a loss inside a demo service, before Taproot.

## Task 5: RED metrics per service (2026-11-23)

- **Decision:**
  - **A rollup by materialized view, like `metrics_1m` (0006).** `spans_red_1m_mv` fills `spans_red_1m` on every insert into `spans`: one row per service, operation and minute (by span **start**) with `requests`, `errors`, `duration_sum_ns`, `duration_max_ns` and `duration_q`. It's an AggregatingMergeTree kept 30 days (`RollupRetentionDays`), so the M6 baseline can look back past the 48 hours raw spans live. There's no backfill: it starts at the first insert after the processor creates it.
  - **A request is a `server` or `consumer` span**: work a service was asked to do, by a caller or a queue. Client, internal and producer spans are the service's own steps and outgoing calls. Counting them would count the callee's work twice. An error is status `error`.
  - **Percentiles as a t-digest sketch** (`AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), UInt64)`), the user's choice over fixed latency buckets and over no percentiles. Percentiles can't be added up across rows, but sketches merge, so a minute that arrived in several batches, or a service's several operations, gives one answer. Queries use `quantilesTDigestMerge(...)(duration_q)`. It's the one non-`Simple` column, and every query must still aggregate (0006).
  - **Read path:** `store.RED(ctx, from, to)` returns per-service, per-minute `REDMinute` (requests, errors, avg, max, p50/p95/p99), with operations merged. M6's engine calls this. The Grafana dashboard **Service RED** (`taproot-red`) shows requests/s, error % and p95 per service, stopping at the last full minute, plus an operations table. `check-grafana.sh` checks it.
- **Why the rollup is exact:** the view sees each inserted batch as is (0006's warning). `InsertNewSpans` drops repeats before insert (task 1), so a replayed batch never reaches it. Integration test "replay doesn't double count" covers this.
- **Checked on 2026-11-23:**
  - Rollup vs raw over 20:09–20:12 UTC: `sum(requests)` equals the count of raw server+consumer spans for all 17 services, exactly.
  - With the demo's `paymentFailure` flag at 50% (20:09–20:14), payment and checkout errors went from 0 to 6 of 16 requests (100%, 50%, 40%, 20% per minute) and back to 0 before and after. The flag file was restored at 20:14:31.
  - Every Grafana check passes. Processor memory was 10 MiB of 128.
  - t-digest p50/p95/p99 for 1..100 ms came within ±2 ms in tests.
- **Limits:**
  - **frontend showed 0 errors while checkout failed.** Its `POST /api/checkout` server spans didn't get status error for a failed order, so RED on frontend can't see this failure. Only the services that set error status can. M6 shouldn't rely on the entry service's error % alone. Latency and the downstream services carry the signal.
  - **Low rates make error % noisy.** checkout handles 2–5 orders a minute in the demo, so one failure is 20–50%. M6's baseline needs minimum counts (or longer windows), not just a percentage threshold.
  - **Some services record a request twice.** frontend's Next.js instrumentation nests two `POST /api/checkout` server spans per request, and flagd has a server `resolveX` inside its gRPC server span. Their request counts are about 2× real requests. A baseline compares a service with itself, so this doesn't matter for M6. Counting only entry spans would need the parent's service, which the view can't see (the parent may be in another batch).
  - **Operation names come from instrumentation** (44 server operations, mostly route templates). An instrumentation that puts IDs in span names would multiply rollup rows (cardinality, design section 4). Watch `uniqExact(service, operation)`.

## Task 6: the order workers carry the trace across Kafka (2026-11-23)

- **Decision:**
  - **Two hand-instrumented Python services from one image (`worker/`).** `order-auditor` reads checkout's `orders` topic on the demo's Kafka, decodes each `OrderResult` (Taproot's own minimal `worker/proto/orders.proto`, same field numbers; unknown fields are skipped), checks it (item count, total; an order with no items is an `audit.finding` event), and sends a JSON summary to `orders.audited`. `order-ledger` reads that and keeps running totals. Two hops, so the code shows both halves of propagation: extract on every consume, inject on the auditor's send.
  - **Propagation by hand with the OpenTelemetry SDK** (design section 5, Instrumentation option B), in `worker/taproot_worker/propagation.py`. A getter/setter pair adapts confluent-kafka's `(str, bytes)` header list to OTel's propagators (W3C `traceparent` + `baggage`). Header names match without case, and an unreadable value counts as absent, so a broken slip means a new trace, never a crash.
  - **Child, not link.** Each consumer span (`process orders`, `process orders.audited`) is a child of the span that sent the message, so checkout → auditor → ledger is one trace. OTel's messaging conventions also allow a new trace with a span link, which is what the demo's Java consumers do (Task 3 Limits). A child fits one message → one unit of work, and makes the trace lookup show the whole path.
  - **Attach, then start the span.** The extracted context is attached around each message's work, not only passed as the span's `context=`. Passing it only as `context=` sets the parent but drops the baggage (checkout's `session.id`) for the next hop. A test caught this.
  - **At-least-once offsets.** `enable.auto.offset.store=false` and `store_offsets` after a message is handled (or skipped as unreadable), committed in the background. A crash re-reads at most unfinished messages. A re-read order is audited again, which is harmless here (the ledger's totals are in memory and not the point). The auditor starts new groups at `latest` (no replay of the demo's history). The ledger starts at `earliest`, because its topic is created by the auditor's first send and could get messages before the ledger joins.
  - **Deploy through the demo's compose override** (`deploy/otel-demo/compose.taproot.yaml`, 96M each), on the demo network, exporting OTLP/HTTP to the demo's collector. So their spans take the same road as every demo service's (collector → agent → Redpanda → processor). `up.sh` sets the build path relative to the demo dir, as it does for the collector extras.
- **Ownership:** hand-written piece 1 (learning target 1). On 2026-11-23 the user asked Claude to write it ("write this too"), so CLAUDE.md records the waiver. Claude explains it back at the end of M4 with understanding checks, together with the M3 and M4 dedup explain-back.
- **Checked on 2026-11-23:**
  - 12 pytest tests pass (no broker: fakes with confluent-kafka's method names, in-memory span exporter).
  - Live, a checkout trace (78 spans, 14 services, no orphans) runs frontend → checkout → `publish orders` → order-auditor `process orders` → `publish orders.audited` → order-ledger `process orders.audited` → `record order`. This is the M4 proof ("a hop through my worker across Kafka").
  - Every order checkout published in the counted window (20:39–20:40 UTC: 6 of 6, a small sample; task 7 counts at length) reached both workers in the same trace, with 0 worker error spans and 0 auditor spans without a parent.
  - Taproot's Kafka sensor found both consumer groups by itself (lag 0).
  - Worker memory: 32 and 29 MiB of 96. Metrics `check-loss` 0/0.
- **Limits:** none of the workers' messages are deduplicated, and the ledger's totals reset on restart. Both are fine for a service whose job is to be traced, not to keep books.

## Task 7 and closing M4 (2026-11-23)

- **Decision:**
  - **The loss check covers spans.** `cmd/losscheck` compares, over the same window, the distinct spans in `taproot.spans` (Redpanda) that *started* in the window with the stored ones, by `model.Span.Key` (S1 missing, S2 duplicates), next to the metric checks (L1–L3). `losscheck.CompareKeys` is the exact comparison both use. A negative control against a scratch copy of the table, with 9 spans removed and 1 doubled, made S1 and S2 fail as they should.
  - **The step before Taproot is checked too (S0).** The demo collector's own metrics are served on the demo network (Taproot's extras file, which repeats the demo's periodic reader, since lists are replaced). `check-loss.sh` reads `otelcol_exporter_send_failed_spans` + `enqueue_failed_spans` for `otlp_http/taproot` before and after each scenario: they must not move, and the queue must drain. Checked first: with the agent stopped 27 s, the queue grew to 284 batches while `send_failed` stayed absent (it counts only after retries run out). Then it drained to 0 with ~6,800 more spans sent.
  - **A deliberate double delivery (`span-replay`).** The newest 100 spans of each partition are produced again unchanged. The processor must log skipping all of them, and S2 must stay 0.
- **M4's proof** (design section 8): "look up one trace ID and see its full path, including a hop through my worker across Kafka". Done in task 6: `go run ./cmd/trace <id>` shows frontend → checkout → `publish orders` → order-auditor → `publish orders.audited` → order-ledger in one trace.
- **Checked on 2026-11-23**, `bash scripts/check-loss.sh --all`, 60 s outages, every check PASS:

  | Scenario | points (Redpanda = ClickHouse) | spans (Redpanda = ClickHouse) | collector sent, gave up |
  |---|---|---|---|
  | processor killed | 5,412 | 14,997 | 11,707, 0 |
  | ClickHouse stopped, processor killed mid-retry | 6,027 | 16,211 | 13,489, 0 |
  | Redpanda stopped | 5,658 | 15,553 | 12,827, 0 |
  | Redpanda stopped + agent restarted | 5,682 | 17,045 | 14,919, 0 |
  | agent restarted | 3,632 | 9,480 | 6,717, 0 |
  | span replay | 2,871 | 7,562 | 4,471, 0 |

  The replay was 300 spans, all skipped as already stored (twice: once alone, once in `--all`). These were the only skips. The agent restarts caused no resends, because the agent answers only after its disk append, so a cut-off request is retried whole.
- **Limits and findings:**
  - **Losses before the collector aren't seen.** A demo service that dies before exporting (checkout's startup panics, task 4) loses spans Taproot never received. S0 covers the collector → agent step onward.
  - **Span volume is 10× the 11-18 estimate** (112/s, 1,354 bytes each; `docs/experiments/2026-11-18-span-volume.md`, 2026-11-23 section). The agent's 256 MB span spool covered only ~29 minutes of a Redpanda outage. Resolved below (span buffer size).
  - **The trace index** helps normal traces (7–11× fewer rows) but isn't faster yet at 750k rows, and long streaming spans defeat it (`docs/experiments/2026-11-23-trace-index.md`).
  - **Still owed:** the end-of-M4 explain-back (M3 commit-after-write and dedup, M4 span dedup, trace propagation) with understanding checks, and the user's break-it test.

## Span buffer size (2026-11-23)

- **Decision:** the agent's span spool cap (`TAPROOT_SPAN_BUFFER_MAX_MB`) goes from 256 MB to **1 GB**, about 2 hours of a Redpanda outage at ~112 spans/s × 1.35 KB. The metrics spool stays at 256 MB (~9 h). Raw spans are still kept 48 h. The agent logs both caps at start.
- **Why:** the user chose this over shorter span retention (24 h) or sampling noisy spans at the collector. It keeps every span and the design as written. The earlier worry about 48 h of spans (~2.8 GB compressed) is withdrawn: the Docker disk has 938 GB free. The spool reads one record at a time from 4 MiB segments, so a bigger cap costs disk, not agent memory.
- **Replaces:** the 256 MB span spool cap from Task 2 ("same 256 MB cap").

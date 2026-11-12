# 0002: Backend processor for M1

- **Decision:** The processor reads `taproot.metrics` as consumer group `taproot-processor` and writes to `taproot.metrics` in ClickHouse using clickhouse-go v2 (native protocol, port 9000). The table is a plain `MergeTree`, sorted by `(metric, ts)` and partitioned by day. Offsets are auto-committed on franz-go's timer. A failed write is retried with backoff (1s, doubling, capped at 30s) before anything new is polled. A record that won't decode is logged and skipped.
- **Why:** clickhouse-go is ClickHouse's official client, and the engine will reuse it for queries. Auto-commit and a table without dedup are deliberate M1 placeholders. Commit-after-write and dedup keys are the hand-written M3 piece (learning target 2), so M1 doesn't pre-empt them.
- **Known gap until M3:** auto-commit can commit offsets for records that were polled but not yet written. If the processor dies in that window, those points are lost. A restart can also re-read records that were written but not yet committed, which stores them twice. Both are what M3 fixes.
- **Replaces:** Nothing. Adds detail to section 5 (Storage) and section 6 (Backend processor).

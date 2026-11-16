# 0002: Backend processor for M1

- **Decision:** The processor reads `taproot.metrics` as consumer group `taproot-processor` and writes to `taproot.metrics` in ClickHouse using clickhouse-go v2 (native protocol, port 9000). The table is a plain `MergeTree`, sorted by `(metric, ts)` and partitioned by day. Offsets are auto-committed on franz-go's timer. A failed write is retried with backoff (1s, doubling, capped at 30s) before anything new is polled. A record that won't decode is logged and skipped.
- **Why:** clickhouse-go is ClickHouse's official client, and the engine will reuse it for queries. Auto-commit and a table without dedup are deliberate M1 placeholders. Commit-after-write and dedup keys are the hand-written M3 piece (learning target 2), so M1 doesn't pre-empt them.
- **Known gap until M3:** auto-commit can commit offsets for records that were polled but not yet written. If the processor dies in that window, those points are lost. A restart can also re-read records that were written but not yet committed, which stores them twice. Both are what M3 fixes.
- **Replaces:** Nothing. Adds detail to section 5 (Storage) and section 6 (Backend processor).

## Correction (2026-11-16, M3 task 1)

- **The loss half of "Known gap" is wrong.** franz-go's default auto-commit lags one poll: offsets from a poll only become eligible to commit when the *next* poll starts (`updateUncommitted` in franz-go v1.22.3, `consumer_group.go`: "This one-poll lag is INTENTIONAL and is what makes default autocommit at-least-once"). `processor.Run` doesn't poll again until the current batch is written, so a batch that's polled but not yet written is never committed. A crash can't lose it.
- **Evidence:** `docs/experiments/2026-11-16-m3-before.md`. ClickHouse was stopped and the processor killed while retrying a batch: 0 missing. Killing the processor at any other time: 102 duplicates, 0 missing.
- **The duplicate half stands.** Up to one auto-commit interval (5s) of written-but-uncommitted records is re-read and stored again after a crash or a lost group session.
- **What this means for M3:** the pipeline is already at-least-once, but only because of a library default plus the loop's shape. Neither is visible in our code, and either could change (greedy auto-commit, or a processor that polls ahead while writing). Commit-after-write makes the guarantee explicit and shrinks the duplicate window from 5s to about one batch. Dedup removes what's left.

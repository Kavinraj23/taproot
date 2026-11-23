package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"taproot/internal/model"
)

func (s *Store) spanTableName() string {
	if s.cfg.SpanTable == "" {
		return "spans"
	}
	return s.cfg.SpanTable
}
func (s *Store) spanTable() string       { return s.name(s.spanTableName()) }
func (s *Store) traceIndexTable() string { return s.name(s.spanTableName() + "_trace_index") }
func (s *Store) traceIndexView() string  { return s.name(s.spanTableName() + "_trace_index_mv") }
func (s *Store) redTable() string        { return s.name(s.spanTableName() + "_red_1m") }
func (s *Store) redView() string         { return s.name(s.spanTableName() + "_red_1m_mv") }

// EnsureSpanSchema creates the spans table, the trace index and the view that fills it, with
// the same 48-hour retention as raw metrics. Safe to run on every start. (Design section 5,
// Storage option C; docs/decisions/0008.)
//
// Spans are sorted by (service, start): RED metrics and the root-cause engine read one
// service over a time range. Looking up one trace in that order would scan everything, so the
// trace index maps each trace ID to its time range: first find the range, then read only
// that slice of the spans table. Plain MergeTree: InsertNewSpans keeps duplicates out.
func (s *Store) EnsureSpanSchema(ctx context.Context) error {
	if err := s.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", s.cfg.Database)); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	spans := `CREATE TABLE IF NOT EXISTS ` + s.spanTable() + ` (
    trace_id       String,
    span_id        String,
    parent_span_id String,
    service        LowCardinality(String),
    name           LowCardinality(String),
    kind           LowCardinality(String),
    start          DateTime64(9, 'UTC'),
    duration_ns    UInt64,
    status         LowCardinality(String),
    attributes     Map(LowCardinality(String), String),
    resource       Map(LowCardinality(String), String)
) ENGINE = MergeTree
PARTITION BY toDate(start)
ORDER BY (service, start)
` + fmt.Sprintf("TTL toDateTime(start) + INTERVAL %d HOUR", RawRetentionHours)
	if err := s.conn.Exec(ctx, spans); err != nil {
		return fmt.Errorf("create spans table: %w", err)
	}

	// One row per trace per insert batch until background merges combine them, so a lookup
	// must aggregate: SELECT min(start_min), max(end_max) FROM ... WHERE trace_id = ?
	// min and max don't care about repeats, so this table can't be skewed by a duplicate.
	index := `CREATE TABLE IF NOT EXISTS ` + s.traceIndexTable() + ` (
    trace_id  String,
    start_min SimpleAggregateFunction(min, DateTime64(9, 'UTC')),
    end_max   SimpleAggregateFunction(max, DateTime64(9, 'UTC'))
) ENGINE = AggregatingMergeTree
ORDER BY trace_id
` + fmt.Sprintf("TTL toDateTime(start_min) + INTERVAL %d HOUR", RawRetentionHours)
	if err := s.conn.Exec(ctx, index); err != nil {
		return fmt.Errorf("create trace index: %w", err)
	}

	view := `CREATE MATERIALIZED VIEW IF NOT EXISTS ` + s.traceIndexView() + ` TO ` + s.traceIndexTable() + ` AS
SELECT trace_id, min(start) AS start_min, max(start + toIntervalNanosecond(duration_ns)) AS end_max
FROM ` + s.spanTable() + `
GROUP BY trace_id`
	if err := s.conn.Exec(ctx, view); err != nil {
		return fmt.Errorf("create trace index view: %w", err)
	}
	return s.ensureRED(ctx)
}

// ensureRED creates the per-minute RED rollup (rate, errors, duration per service and
// operation) and the view that fills it from every insert into spans, like metrics_1m
// (docs/decisions/0006, 0008 task 5). Kept 30 days, so baselines can look back past the
// 48 hours raw spans live.
//
// A request is a server or consumer span: work a service was asked to do, by a caller or a
// queue. Client, internal and producer spans are its own steps and outgoing calls.
//
// Percentiles can't be added up like counts, so each row keeps a t-digest sketch of its
// durations, and sketches merge. A minute arriving in several batches is several rows until
// background merges, so every query must aggregate:
//
//	SELECT service, minute, sum(requests), sum(errors),
//	       quantilesTDigestMerge(0.5, 0.95, 0.99)(duration_q)
//	FROM spans_red_1m GROUP BY service, minute
//
// The view sees each inserted batch as is, so it relies on InsertNewSpans dropping repeats
// before insert: a replayed span would otherwise be counted twice.
func (s *Store) ensureRED(ctx context.Context) error {
	table := `CREATE TABLE IF NOT EXISTS ` + s.redTable() + ` (
    service         LowCardinality(String),
    operation       LowCardinality(String),
    minute          DateTime('UTC'),
    requests        SimpleAggregateFunction(sum, UInt64),
    errors          SimpleAggregateFunction(sum, UInt64),
    duration_sum_ns SimpleAggregateFunction(sum, UInt64),
    duration_max_ns SimpleAggregateFunction(max, UInt64),
    duration_q      AggregateFunction(quantilesTDigest(0.5, 0.95, 0.99), UInt64)
) ENGINE = AggregatingMergeTree
PARTITION BY toDate(minute)
ORDER BY (service, operation, minute)
` + fmt.Sprintf("TTL minute + INTERVAL %d DAY", RollupRetentionDays)
	if err := s.conn.Exec(ctx, table); err != nil {
		return fmt.Errorf("create RED rollup: %w", err)
	}
	view := `CREATE MATERIALIZED VIEW IF NOT EXISTS ` + s.redView() + ` TO ` + s.redTable() + ` AS
SELECT service, name AS operation, toStartOfMinute(start) AS minute,
       count() AS requests, countIf(status = 'error') AS errors,
       sum(duration_ns) AS duration_sum_ns, max(duration_ns) AS duration_max_ns,
       quantilesTDigestState(0.5, 0.95, 0.99)(duration_ns) AS duration_q
FROM ` + s.spanTable() + `
WHERE kind IN ('server', 'consumer')
GROUP BY service, operation, minute`
	if err := s.conn.Exec(ctx, view); err != nil {
		return fmt.Errorf("create RED view: %w", err)
	}
	return nil
}

// REDMinute is one service's requests in one minute: how many, how many failed, how long
// they took. P50/P95/P99 are approximate (t-digest).
type REDMinute struct {
	Service                 string
	Minute                  time.Time
	Requests, Errors        uint64
	Avg, Max, P50, P95, P99 time.Duration
}

// RED returns each service's RED numbers per minute for minutes in [from, to), by service
// then minute. A service's operations are merged.
func (s *Store) RED(ctx context.Context, from, to time.Time) ([]REDMinute, error) {
	rows, err := s.conn.Query(ctx, `SELECT service, minute, sum(requests) AS n, sum(errors),
       sum(duration_sum_ns), max(duration_max_ns),
       arrayMap(x -> toFloat64(x), quantilesTDigestMerge(0.5, 0.95, 0.99)(duration_q))
FROM `+s.redTable()+`
WHERE minute >= ? AND minute < ?
GROUP BY service, minute
ORDER BY service, minute`, from.UTC(), to.UTC())
	if err != nil {
		return nil, fmt.Errorf("read RED: %w", err)
	}
	defer rows.Close()
	var out []REDMinute
	for rows.Next() {
		var m REDMinute
		var sum, max uint64
		var q []float64
		if err := rows.Scan(&m.Service, &m.Minute, &m.Requests, &m.Errors, &sum, &max, &q); err != nil {
			return nil, fmt.Errorf("scan RED: %w", err)
		}
		if m.Requests > 0 {
			m.Avg = time.Duration(sum / m.Requests)
		}
		m.Max = time.Duration(max)
		if len(q) == 3 {
			m.P50, m.P95, m.P99 = time.Duration(q[0]), time.Duration(q[1]), time.Duration(q[2])
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read RED: %w", err)
	}
	return out, nil
}

// InsertSpans writes spans as one batch.
func (s *Store) InsertSpans(ctx context.Context, spans []model.Span) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+s.spanTable()+
		" (trace_id, span_id, parent_span_id, service, name, kind, start, duration_ns, status, attributes, resource)")
	if err != nil {
		return fmt.Errorf("prepare span insert: %w", err)
	}
	defer batch.Abort() // no-op once sent
	for _, sp := range spans {
		if err := batch.Append(sp.TraceID, sp.SpanID, sp.ParentSpanID, sp.Service, sp.Name, sp.Kind,
			sp.Start, uint64(sp.Duration), sp.Status, orEmpty(sp.Attributes), orEmpty(sp.Resource)); err != nil {
			return fmt.Errorf("append span: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send span batch: %w", err)
	}
	return nil
}

// InsertNewSpans writes the spans that aren't stored yet, and returns how many it wrote.
// Same idea as InsertNew for metric points: a replayed batch or a span the agent sent twice
// is dropped here, before it reaches the table (and, in task 5, the RED view).
//
// Identity is model.Span.Key: trace ID and span ID. One lookup per batch reads the stored
// span keys of the batch's services over the batch's time span, using the (service, start)
// sort key.
func (s *Store) InsertNewSpans(ctx context.Context, spans []model.Span) (int, error) {
	if len(spans) == 0 {
		return 0, nil
	}
	fresh := make([]model.Span, 0, len(spans))
	seen := make(map[string]bool, len(spans))
	services := map[string]bool{}
	from, to := spans[0].Start, spans[0].Start
	for _, sp := range spans {
		k := sp.Key()
		if seen[k] {
			continue // the same span twice in one batch
		}
		seen[k] = true
		fresh = append(fresh, sp)
		services[sp.Service] = true
		if sp.Start.Before(from) {
			from = sp.Start
		}
		if sp.Start.After(to) {
			to = sp.Start
		}
	}

	stored, err := s.storedSpanKeys(ctx, services, from, to)
	if err != nil {
		return 0, err
	}
	out := fresh[:0]
	for _, sp := range fresh {
		if !stored[sp.Key()] {
			out = append(out, sp)
		}
	}
	if len(out) == 0 {
		return 0, nil
	}
	if err := s.InsertSpans(ctx, out); err != nil {
		return 0, err
	}
	return len(out), nil
}

// storedSpanKeys returns the Keys of stored spans of these services that started between from
// and to. Bounds are whole seconds containing [from, to], as in storedKeys.
func (s *Store) storedSpanKeys(ctx context.Context, services map[string]bool, from, to time.Time) (map[string]bool, error) {
	names := make([]string, 0, len(services))
	for n := range services {
		names = append(names, n)
	}
	lo := from.UTC().Truncate(time.Second)
	hi := to.UTC().Truncate(time.Second).Add(time.Second)
	rows, err := s.conn.Query(ctx, "SELECT trace_id, span_id FROM "+s.spanTable()+
		" WHERE service IN ? AND start >= ? AND start < ?", names, lo, hi)
	if err != nil {
		return nil, fmt.Errorf("look up stored spans: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var sp model.Span
		if err := rows.Scan(&sp.TraceID, &sp.SpanID); err != nil {
			return nil, fmt.Errorf("scan stored span: %w", err)
		}
		out[sp.Key()] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("look up stored spans: %w", err)
	}
	return out, nil
}

// ErrTraceNotFound means no span of the trace is stored: never seen, past the 48-hour
// retention, or not arrived yet.
var ErrTraceNotFound = errors.New("trace not found")

const spanColumns = `trace_id, span_id, parent_span_id, service, name, kind, start, duration_ns, status, attributes, resource`

// Trace returns every stored span of one trace, by start. Two reads, like a card catalogue:
// the trace index gives the trace's time range, then only that slice of spans is searched
// (the spans table is sorted by service and time, not by trace). Bounds are widened to whole
// seconds: clickhouse-go binds a time.Time at second precision, so a span starting late in
// the trace's last second would otherwise fall outside.
func (s *Store) Trace(ctx context.Context, traceID string) ([]model.Span, error) {
	var first, last time.Time
	var n uint64
	// Aggregate: the index holds one row per trace per insert until background merges.
	if err := s.conn.QueryRow(ctx, "SELECT min(start_min), max(end_max), count() FROM "+s.traceIndexTable()+
		" WHERE trace_id = ?", traceID).Scan(&first, &last, &n); err != nil {
		return nil, fmt.Errorf("look up trace index: %w", err)
	}
	if n == 0 {
		return nil, ErrTraceNotFound
	}
	lo := first.UTC().Truncate(time.Second)
	hi := last.UTC().Truncate(time.Second).Add(time.Second)
	rows, err := s.conn.Query(ctx, "SELECT "+spanColumns+" FROM "+s.spanTable()+
		" WHERE start >= ? AND start < ? AND trace_id = ? ORDER BY start, span_id", lo, hi, traceID)
	if err != nil {
		return nil, fmt.Errorf("read trace: %w", err)
	}
	spans, err := scanSpans(rows)
	if err != nil {
		return nil, err
	}
	if len(spans) == 0 {
		return nil, ErrTraceNotFound // the index outlived its spans (TTLs run separately)
	}
	return spans, nil
}

// TraceSummary is one line of RecentTraces: a trace, when the service first appeared in it,
// and the name of that first span.
type TraceSummary struct {
	TraceID string
	Start   time.Time
	Name    string
}

// RecentTraces returns up to limit traces with a span of service that started in the last
// since, newest first. It reads the spans table in its own order: service, then time.
func (s *Store) RecentTraces(ctx context.Context, service string, since time.Duration, limit int) ([]TraceSummary, error) {
	rows, err := s.conn.Query(ctx, "SELECT trace_id, min(start) AS first, argMin(name, start) FROM "+s.spanTable()+
		" WHERE service = ? AND start >= now64(9) - toIntervalSecond(?)"+
		" GROUP BY trace_id ORDER BY first DESC, trace_id LIMIT ?", service, int64(since.Seconds()), limit)
	if err != nil {
		return nil, fmt.Errorf("recent traces: %w", err)
	}
	defer rows.Close()
	var out []TraceSummary
	for rows.Next() {
		var t TraceSummary
		if err := rows.Scan(&t.TraceID, &t.Start, &t.Name); err != nil {
			return nil, fmt.Errorf("scan recent trace: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("recent traces: %w", err)
	}
	return out, nil
}

func scanSpans(rows driver.Rows) ([]model.Span, error) {
	defer rows.Close()
	var out []model.Span
	for rows.Next() {
		var sp model.Span
		var dur uint64
		if err := rows.Scan(&sp.TraceID, &sp.SpanID, &sp.ParentSpanID, &sp.Service, &sp.Name, &sp.Kind,
			&sp.Start, &dur, &sp.Status, &sp.Attributes, &sp.Resource); err != nil {
			return nil, fmt.Errorf("scan span: %w", err)
		}
		sp.Duration = time.Duration(dur)
		out = append(out, sp)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read spans: %w", err)
	}
	return out, nil
}

func orEmpty(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

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
	return nil
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

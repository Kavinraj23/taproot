// Package store writes metric points to ClickHouse.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"taproot/internal/model"
)

// Config says where the metrics table lives.
type Config struct {
	Addr     string // native protocol, e.g. localhost:9000
	User     string
	Password string
	Database string
	Table    string
	// SpanTable is the spans table (M4); empty means "spans". Its trace index is named after
	// it: spans_trace_index.
	SpanTable string
}

// Store is a ClickHouse connection bound to one metrics table.
type Store struct {
	conn driver.Conn
	cfg  Config
}

// Open connects and checks the connection. It doesn't select a database, so it works before
// EnsureSchema has created it; every query names the database explicitly.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{Username: cfg.User, Password: cfg.Password},
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	return &Store{conn: conn, cfg: cfg}, nil
}

// How long each resolution is kept (docs/decisions/0006-metric-rollups-and-retention.md).
const (
	RawRetentionHours   = 48 // 1-second points
	RollupRetentionDays = 30 // 1-minute rollups
)

func (s *Store) name(table string) string { return fmt.Sprintf("`%s`.`%s`", s.cfg.Database, table) }
func (s *Store) table() string            { return s.name(s.cfg.Table) }
func (s *Store) rollupTable() string      { return s.name(s.cfg.Table + "_1m") }
func (s *Store) rollupView() string       { return s.name(s.cfg.Table + "_1m_mv") }

// EnsureSchema creates the database, the 1-second metrics table, its 1-minute rollup table and
// the view that fills it, and sets retention on both tables. Safe to run on every start.
//
// The raw table is sorted by (metric, ts) because queries read one metric over a time range,
// and partitioned by day so expired data goes in whole parts. Plain MergeTree: duplicates are
// kept out by InsertNew, which skips points already stored, so the rollup view never sees them.
func (s *Store) EnsureSchema(ctx context.Context) error {
	if err := s.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", s.cfg.Database)); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	rawTTL := fmt.Sprintf("TTL toDateTime(ts) + INTERVAL %d HOUR", RawRetentionHours)
	raw := `CREATE TABLE IF NOT EXISTS ` + s.table() + ` (
    metric LowCardinality(String),
    labels Map(LowCardinality(String), String),
    ts     DateTime64(9, 'UTC'),
    value  Float64
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (metric, ts)
` + rawTTL
	if err := s.conn.Exec(ctx, raw); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	// CREATE ... IF NOT EXISTS leaves a table made before retention existed untouched.
	if err := s.conn.Exec(ctx, "ALTER TABLE "+s.table()+" MODIFY "+rawTTL); err != nil {
		return fmt.Errorf("set raw retention: %w", err)
	}

	// One row per series per minute. A minute that arrives in several insert batches is several
	// rows until background merges combine them, so every query must aggregate:
	//   SELECT minute, sum(sum)/sum(count) AS avg, min(min), max(max)
	//   FROM metrics_1m WHERE ... GROUP BY metric, labels, minute
	rollup := `CREATE TABLE IF NOT EXISTS ` + s.rollupTable() + ` (
    metric LowCardinality(String),
    labels Map(LowCardinality(String), String),
    minute DateTime('UTC'),
    sum    SimpleAggregateFunction(sum, Float64),
    count  SimpleAggregateFunction(sum, UInt64),
    min    SimpleAggregateFunction(min, Float64),
    max    SimpleAggregateFunction(max, Float64)
) ENGINE = AggregatingMergeTree
PARTITION BY toDate(minute)
ORDER BY (metric, labels, minute)
` + fmt.Sprintf("TTL minute + INTERVAL %d DAY", RollupRetentionDays)
	if err := s.conn.Exec(ctx, rollup); err != nil {
		return fmt.Errorf("create rollup table: %w", err)
	}

	// Runs on every insert into the raw table, over just the inserted rows. Labels are sorted:
	// a Map keeps insertion order and Go maps iterate randomly, so one series arrives with its
	// labels in different orders, and unsorted they would be separate rollup rows.
	view := `CREATE MATERIALIZED VIEW IF NOT EXISTS ` + s.rollupView() + ` TO ` + s.rollupTable() + ` AS
SELECT metric, mapSort(labels) AS labels, toStartOfMinute(ts) AS minute,
       sum(value) AS sum, count() AS count, min(value) AS min, max(value) AS max
FROM ` + s.table() + `
GROUP BY metric, labels, minute`
	if err := s.conn.Exec(ctx, view); err != nil {
		return fmt.Errorf("create rollup view: %w", err)
	}
	return nil
}

// Insert writes pts as one batch.
func (s *Store) Insert(ctx context.Context, pts []model.Point) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+s.table()+" (metric, labels, ts, value)")
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer batch.Abort() // no-op once sent
	for _, p := range pts {
		labels := p.Labels
		if labels == nil {
			labels = map[string]string{}
		}
		if err := batch.Append(p.Metric, labels, p.Timestamp, p.Value); err != nil {
			return fmt.Errorf("append point: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}

// InsertNew writes the points of pts that aren't stored yet, and returns how many it wrote.
// A replayed batch (the processor crashed after writing it but before marking it done) or a
// point the agent sent twice is dropped here, before it reaches the table and the rollup view.
//
// Identity is model.Point.Key: metric, sorted labels and timestamp. One lookup per batch
// reads the stored points of the batch's metrics over the batch's time span; it uses the
// (metric, ts) sort key.
func (s *Store) InsertNew(ctx context.Context, pts []model.Point) (int, error) {
	if len(pts) == 0 {
		return 0, nil
	}
	fresh := make([]model.Point, 0, len(pts))
	keys := make([]string, 0, len(pts))
	seen := make(map[string]bool, len(pts))
	metrics := map[string]bool{}
	from, to := pts[0].Timestamp, pts[0].Timestamp
	for _, p := range pts {
		k := p.Key()
		if seen[k] {
			continue // the same point twice in one batch
		}
		seen[k] = true
		fresh = append(fresh, p)
		keys = append(keys, k)
		metrics[p.Metric] = true
		if p.Timestamp.Before(from) {
			from = p.Timestamp
		}
		if p.Timestamp.After(to) {
			to = p.Timestamp
		}
	}

	stored, err := s.storedKeys(ctx, metrics, from, to)
	if err != nil {
		return 0, err
	}
	out := fresh[:0]
	for i, p := range fresh {
		if !stored[keys[i]] {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return 0, nil
	}
	if err := s.Insert(ctx, out); err != nil {
		return 0, err
	}
	return len(out), nil
}

// storedKeys returns the Keys of stored points of these metrics between from and to.
// clickhouse-go binds a time.Time parameter at second precision, so the bounds are whole
// seconds that contain [from, to]; extra rows only cost a little reading.
func (s *Store) storedKeys(ctx context.Context, metrics map[string]bool, from, to time.Time) (map[string]bool, error) {
	names := make([]string, 0, len(metrics))
	for m := range metrics {
		names = append(names, m)
	}
	lo := from.UTC().Truncate(time.Second)
	hi := to.UTC().Truncate(time.Second).Add(time.Second)
	rows, err := s.conn.Query(ctx, "SELECT metric, labels, ts FROM "+s.table()+
		" WHERE metric IN ? AND ts >= ? AND ts < ?", names, lo, hi)
	if err != nil {
		return nil, fmt.Errorf("look up stored points: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var p model.Point
		if err := rows.Scan(&p.Metric, &p.Labels, &p.Timestamp); err != nil {
			return nil, fmt.Errorf("scan stored point: %w", err)
		}
		out[p.Key()] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("look up stored points: %w", err)
	}
	return out, nil
}

// Close closes the connection.
func (s *Store) Close() error { return s.conn.Close() }

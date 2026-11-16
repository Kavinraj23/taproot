// Package store writes metric points to ClickHouse.
package store

import (
	"context"
	"fmt"

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
// kept until dedup lands in M3.
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

// Close closes the connection.
func (s *Store) Close() error { return s.conn.Close() }

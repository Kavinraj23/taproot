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

func (s *Store) table() string { return fmt.Sprintf("`%s`.`%s`", s.cfg.Database, s.cfg.Table) }

// EnsureSchema creates the database and metrics table if they don't exist, so it is safe to
// run on every start.
//
// Sorted by (metric, ts) because queries read one metric over a time range. Partitioned by
// day so dropping old 1-second data (M2) is a cheap whole-partition drop. Plain MergeTree:
// duplicates are kept until dedup lands in M3.
func (s *Store) EnsureSchema(ctx context.Context) error {
	if err := s.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", s.cfg.Database)); err != nil {
		return fmt.Errorf("create database: %w", err)
	}
	ddl := `CREATE TABLE IF NOT EXISTS ` + s.table() + ` (
    metric LowCardinality(String),
    labels Map(LowCardinality(String), String),
    ts     DateTime64(9, 'UTC'),
    value  Float64
) ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (metric, ts)`
	if err := s.conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("create table: %w", err)
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

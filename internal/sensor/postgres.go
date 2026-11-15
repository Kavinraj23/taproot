package sensor

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"taproot/internal/discovery"
)

// pgStats is one round of numbers read from Postgres, before they become readings.
type pgStats struct {
	connsByState    map[string]float64
	maxConns        float64
	xactCommit      float64
	xactRollback    float64
	blocksHit       float64
	blocksRead      float64
	deadlocks       float64
	locksWaiting    float64
	longestQuerySec float64
}

// Client connections by state (active, idle, idle in transaction, ...). Background workers are
// left out: they aren't load.
const pgConnsQuery = `
SELECT coalesce(state, 'unknown'), count(*)::float8
FROM pg_stat_activity
WHERE backend_type = 'client backend'
GROUP BY 1`

// Everything else in one round trip. The totals from pg_stat_database are counters since the
// stats were last reset; the agent turns them into rates. Our own query is left out of the
// longest-query check.
const pgTotalsQuery = `
SELECT
  current_setting('max_connections')::float8,
  coalesce(sum(xact_commit), 0)::float8,
  coalesce(sum(xact_rollback), 0)::float8,
  coalesce(sum(blks_hit), 0)::float8,
  coalesce(sum(blks_read), 0)::float8,
  coalesce(sum(deadlocks), 0)::float8,
  (SELECT count(*) FROM pg_locks WHERE NOT granted)::float8,
  (SELECT coalesce(extract(epoch FROM max(now() - query_start)), 0)
     FROM pg_stat_activity
     WHERE state = 'active' AND backend_type = 'client backend' AND pid <> pg_backend_pid())::float8
FROM pg_stat_database`

// pgReadings maps one round of stats to readings.
func pgReadings(s pgStats) []Reading {
	rs := make([]Reading, 0, len(s.connsByState)+8)
	for state, n := range s.connsByState {
		rs = append(rs, Reading{Metric: "postgres_connections", Kind: Gauge, Value: n, Labels: map[string]string{"state": state}})
	}
	return append(rs,
		Reading{Metric: "postgres_connections_max", Kind: Gauge, Value: s.maxConns},
		Reading{Metric: "postgres_xact_commit", Kind: Counter, Value: s.xactCommit},
		Reading{Metric: "postgres_xact_rollback", Kind: Counter, Value: s.xactRollback},
		Reading{Metric: "postgres_blocks_hit", Kind: Counter, Value: s.blocksHit},
		Reading{Metric: "postgres_blocks_read", Kind: Counter, Value: s.blocksRead},
		Reading{Metric: "postgres_deadlocks", Kind: Counter, Value: s.deadlocks},
		Reading{Metric: "postgres_locks_waiting", Kind: Gauge, Value: s.locksWaiting},
		Reading{Metric: "postgres_longest_query_seconds", Kind: Gauge, Value: s.longestQuerySec},
	)
}

// NewPostgres returns a Factory for Postgres collectors that log in as user.
func NewPostgres(user, password string) Factory {
	return func(_ discovery.Service, addr string) (Collector, error) {
		host, portStr, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		port, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil {
			return nil, err
		}
		cfg, err := pgx.ParseConfig("sslmode=disable")
		if err != nil {
			return nil, err
		}
		cfg.Host, cfg.Port = host, uint16(port)
		cfg.User, cfg.Password = user, password
		cfg.Database = "postgres"
		cfg.ConnectTimeout = 2 * time.Second
		cfg.RuntimeParams["application_name"] = "taproot"
		return &postgresCollector{cfg: cfg}, nil
	}
}

// postgresCollector keeps one connection open, reconnecting after any error.
type postgresCollector struct {
	cfg  *pgx.ConnConfig
	conn *pgx.Conn
}

func (c *postgresCollector) Collect(ctx context.Context) ([]Reading, error) {
	if c.conn == nil {
		conn, err := pgx.ConnectConfig(ctx, c.cfg)
		if err != nil {
			return nil, err
		}
		c.conn = conn
	}
	s, err := c.query(ctx)
	if err != nil {
		c.Close() // a broken connection is replaced on the next tick
		return nil, err
	}
	return pgReadings(s), nil
}

func (c *postgresCollector) query(ctx context.Context) (pgStats, error) {
	s := pgStats{connsByState: map[string]float64{}}
	rows, err := c.conn.Query(ctx, pgConnsQuery)
	if err != nil {
		return s, err
	}
	for rows.Next() {
		var state string
		var n float64
		if err := rows.Scan(&state, &n); err != nil {
			rows.Close()
			return s, err
		}
		s.connsByState[state] = n
	}
	if err := rows.Err(); err != nil {
		return s, err
	}
	err = c.conn.QueryRow(ctx, pgTotalsQuery).Scan(&s.maxConns, &s.xactCommit, &s.xactRollback,
		&s.blocksHit, &s.blocksRead, &s.deadlocks, &s.locksWaiting, &s.longestQuerySec)
	return s, err
}

func (c *postgresCollector) Close() error {
	if c.conn == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := c.conn.Close(ctx)
	c.conn = nil
	return err
}

//go:build integration

package store

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"taproot/internal/model"
)

// Run with: TAPROOT_CLICKHOUSE_ADDR=localhost:9000 go test -tags integration ./internal/store/
func TestRoundTrip(t *testing.T) {
	addr := os.Getenv("TAPROOT_CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("TAPROOT_CLICKHOUSE_ADDR not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := Config{
		Addr: addr, User: "taproot", Password: "taproot", Database: "taproot",
		Table: fmt.Sprintf("metrics_it_%d", time.Now().UnixNano()),
	}
	s, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	defer func() {
		if err := s.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+s.table()); err != nil {
			t.Errorf("drop test table: %v", err)
		}
	}()

	for i := 0; i < 2; i++ { // safe to run on every start
		if err := s.EnsureSchema(ctx); err != nil {
			t.Fatalf("EnsureSchema #%d: %v", i+1, err)
		}
	}

	base := time.Date(2026, 11, 11, 9, 30, 0, 123456789, time.UTC)
	want := []model.Point{
		{Metric: "host_cpu_usage_percent", Labels: map[string]string{"host": "h1"}, Timestamp: base, Value: 1.5},
		{Metric: "host_cpu_usage_percent", Labels: map[string]string{"host": "h1"}, Timestamp: base.Add(time.Second), Value: 2.5},
		{Metric: "m", Labels: map[string]string{"a": "1", "b": "2"}, Timestamp: base.Add(2 * time.Second), Value: 3.5},
	}
	if err := s.Insert(ctx, want); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	rows, err := s.conn.Query(ctx, "SELECT metric, labels, ts, value FROM "+s.table()+" ORDER BY ts")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()
	var got []model.Point
	for rows.Next() {
		var p model.Point
		if err := rows.Scan(&p.Metric, &p.Labels, &p.Timestamp, &p.Value); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, p)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Metric != w.Metric || g.Value != w.Value || !g.Timestamp.Equal(w.Timestamp) || !reflect.DeepEqual(g.Labels, w.Labels) {
			t.Errorf("row %d = %+v, want %+v", i, g, w)
		}
	}
}

//go:build integration

package store

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
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
		for _, name := range []string{s.rollupView(), s.rollupTable(), s.table()} {
			if err := s.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+name); err != nil {
				t.Errorf("drop %s: %v", name, err)
			}
		}
	}()

	for i := 0; i < 2; i++ { // safe to run on every start
		if err := s.EnsureSchema(ctx); err != nil {
			t.Fatalf("EnsureSchema #%d: %v", i+1, err)
		}
	}

	// Recent, so the 48-hour retention keeps it; nanoseconds check DateTime64(9) precision.
	base := time.Now().UTC().Truncate(time.Second).Add(123456789)
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

// openTestStore opens a Store on a uniquely named table and drops it, its rollup table and its
// view afterwards. It doesn't run EnsureSchema.
func openTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	addr := os.Getenv("TAPROOT_CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("TAPROOT_CLICKHOUSE_ADDR not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	s, err := Open(ctx, Config{
		Addr: addr, User: "taproot", Password: "taproot", Database: "taproot",
		Table: fmt.Sprintf("metrics_it_%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		for _, name := range []string{s.rollupView(), s.rollupTable(), s.table()} {
			if err := s.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+name); err != nil {
				t.Errorf("drop %s: %v", name, err)
			}
		}
		s.Close()
	})
	return s, ctx
}

// minuteRow is one rolled-up minute of one series, aggregated the way every query must.
type minuteRow struct {
	host          string
	minute        time.Time
	count         uint64
	sum, min, max float64
}

func rollups(t *testing.T, s *Store, ctx context.Context) []minuteRow {
	t.Helper()
	rows, err := s.conn.Query(ctx, `SELECT labels['host'], minute, sum(count), sum(sum), min(min), max(max)
FROM `+s.rollupTable()+` GROUP BY metric, labels, minute ORDER BY labels['host'], minute`)
	if err != nil {
		t.Fatalf("select rollups: %v", err)
	}
	defer rows.Close()
	var out []minuteRow
	for rows.Next() {
		var r minuteRow
		if err := rows.Scan(&r.host, &r.minute, &r.count, &r.sum, &r.min, &r.max); err != nil {
			t.Fatalf("scan rollup: %v", err)
		}
		out = append(out, r)
	}
	return out
}

func pt(host string, at time.Time, v float64) model.Point {
	return model.Point{Metric: "m", Labels: map[string]string{"host": host}, Timestamp: at, Value: v}
}

func checkRollups(t *testing.T, got, want []minuteRow) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("rollups = %+v, want %+v", got, want)
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.host != w.host || !g.minute.Equal(w.minute) || g.count != w.count || g.sum != w.sum || g.min != w.min || g.max != w.max {
			t.Errorf("rollup %d = %+v, want %+v", i, g, w)
		}
	}
}

func TestEnsureSchemaCreatesRollups(t *testing.T) {
	s, ctx := openTestStore(t)
	for i := 0; i < 2; i++ {
		if err := s.EnsureSchema(ctx); err != nil {
			t.Fatalf("EnsureSchema #%d: %v", i+1, err)
		}
	}
	var n uint64
	if err := s.conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = ? AND name IN (?, ?)",
		s.cfg.Database, s.cfg.Table+"_1m", s.cfg.Table+"_1m_mv").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("found %d of the rollup table and view, want 2", n)
	}
}

func TestRollupValues(t *testing.T) {
	s, ctx := openTestStore(t)
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	m0 := time.Date(2026, 11, 11, 9, 30, 0, 0, time.UTC)
	m1 := m0.Add(time.Minute)
	if err := s.Insert(ctx, []model.Point{
		pt("h1", m0.Add(1*time.Second), 1), pt("h1", m0.Add(2*time.Second), 2), pt("h1", m0.Add(59*time.Second), 6),
		pt("h1", m1, 10),
		pt("h2", m0.Add(5*time.Second), 5),
	}); err != nil {
		t.Fatal(err)
	}
	checkRollups(t, rollups(t, s, ctx), []minuteRow{
		{host: "h1", minute: m0, count: 3, sum: 9, min: 1, max: 6},
		{host: "h1", minute: m1, count: 1, sum: 10, min: 10, max: 10},
		{host: "h2", minute: m0, count: 1, sum: 5, min: 5, max: 5},
	})
}

func TestRollupSplitAcrossBatches(t *testing.T) {
	s, ctx := openTestStore(t)
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	m0 := time.Date(2026, 11, 11, 9, 30, 0, 0, time.UTC)
	if err := s.Insert(ctx, []model.Point{pt("h1", m0.Add(10*time.Second), 4), pt("h1", m0.Add(11*time.Second), 8)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert(ctx, []model.Point{pt("h1", m0.Add(40*time.Second), 0)}); err != nil {
		t.Fatal(err)
	}
	checkRollups(t, rollups(t, s, ctx), []minuteRow{
		{host: "h1", minute: m0, count: 3, sum: 12, min: 0, max: 8},
	})
}

func TestRollupLabelOrderDoesNotSplitSeries(t *testing.T) {
	s, ctx := openTestStore(t)
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	// ClickHouse Maps keep insertion order and Go maps iterate in random order, so one series
	// arrives with its labels in different orders. Written in SQL to force both orders.
	if err := s.conn.Exec(ctx, `INSERT INTO `+s.table()+` (metric, labels, ts, value) VALUES
  ('m', map('host','h1','service','v'), '2026-11-16 09:30:01', 1),
  ('m', map('service','v','host','h1'), '2026-11-16 09:30:02', 3)`); err != nil {
		t.Fatal(err)
	}
	var series, count uint64
	var sum float64
	if err := s.conn.QueryRow(ctx, `SELECT count(), sum(c), sum(s) FROM (
  SELECT sum(count) AS c, sum(sum) AS s FROM `+s.rollupTable()+` GROUP BY metric, labels, minute)`).Scan(&series, &count, &sum); err != nil {
		t.Fatal(err)
	}
	if series != 1 || count != 2 || sum != 4 {
		t.Errorf("rollup = %d series, count %d, sum %v; want 1 series, count 2, sum 4", series, count, sum)
	}
}

func showCreate(t *testing.T, s *Store, ctx context.Context, name string) string {
	t.Helper()
	var ddl string
	if err := s.conn.QueryRow(ctx, "SHOW CREATE TABLE "+name).Scan(&ddl); err != nil {
		t.Fatalf("show create %s: %v", name, err)
	}
	return ddl
}

func TestEnsureSchemaAddsTTLToExistingTable(t *testing.T) {
	s, ctx := openTestStore(t)
	// The table as it was before rollups: no TTL.
	if err := s.conn.Exec(ctx, `CREATE TABLE `+s.table()+` (
    metric LowCardinality(String), labels Map(LowCardinality(String), String),
    ts DateTime64(9, 'UTC'), value Float64
) ENGINE = MergeTree PARTITION BY toDate(ts) ORDER BY (metric, ts)`); err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if ddl := showCreate(t, s, ctx, s.table()); !strings.Contains(ddl, "TTL") || !strings.Contains(ddl, "toIntervalHour(48)") {
		t.Errorf("raw table has no 48-hour TTL:\n%s", ddl)
	}
	if ddl := showCreate(t, s, ctx, s.rollupTable()); !strings.Contains(ddl, "TTL") || !strings.Contains(ddl, "toIntervalDay(30)") {
		t.Errorf("rollup table has no 30-day TTL:\n%s", ddl)
	}
}

func TestRetentionKeepsSummaryOfOldPoints(t *testing.T) {
	s, ctx := openTestStore(t)
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Minute)
	recent := time.Now().UTC().Truncate(time.Minute)
	if err := s.Insert(ctx, []model.Point{pt("h1", old, 1), pt("h1", recent, 2)}); err != nil {
		t.Fatal(err)
	}
	if err := s.conn.Exec(ctx, "OPTIMIZE TABLE "+s.table()+" FINAL"); err != nil {
		t.Fatal(err)
	}

	var raw uint64
	if err := s.conn.QueryRow(ctx, "SELECT count() FROM "+s.table()).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw != 1 {
		t.Errorf("raw table has %d points after TTL, want 1 (only the recent one)", raw)
	}
	checkRollups(t, rollups(t, s, ctx), []minuteRow{
		{host: "h1", minute: old, count: 1, sum: 1, min: 1, max: 1},
		{host: "h1", minute: recent, count: 1, sum: 2, min: 2, max: 2},
	})
}

// M3 dedup cases (docs/decisions/0007): InsertNew stores only points not already stored.

func countRows(t *testing.T, s *Store, ctx context.Context) uint64 {
	t.Helper()
	var n uint64
	if err := s.conn.QueryRow(ctx, "SELECT count() FROM "+s.table()).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func dedupPoints(base time.Time) []model.Point {
	return []model.Point{
		{Metric: "cpu", Labels: map[string]string{"host": "h1"}, Timestamp: base, Value: 1},
		{Metric: "cpu", Labels: map[string]string{"host": "h1"}, Timestamp: base.Add(time.Second), Value: 2},
		{Metric: "up", Labels: map[string]string{"service": "kafka", "instance": "kafka"}, Timestamp: base, Value: 1},
	}
}

func insertNew(t *testing.T, s *Store, ctx context.Context, pts []model.Point) int {
	t.Helper()
	n, err := s.InsertNew(ctx, pts)
	if err != nil {
		t.Fatalf("InsertNew: %v", err)
	}
	return n
}

// Dedup case 1: a replayed batch writes nothing.
func TestInsertNewReplayWritesNothing(t *testing.T) {
	s, ctx := openTestStore(t)
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	pts := dedupPoints(time.Now().UTC().Truncate(time.Second).Add(250 * time.Millisecond))
	if n := insertNew(t, s, ctx, pts); n != 3 {
		t.Fatalf("first InsertNew wrote %d, want 3", n)
	}
	if n := insertNew(t, s, ctx, pts); n != 0 {
		t.Fatalf("replay wrote %d, want 0", n)
	}
	if n := countRows(t, s, ctx); n != 3 {
		t.Fatalf("table has %d rows, want 3", n)
	}
}

// Dedup case 2: only the new points of a partly stored batch are written.
func TestInsertNewWritesOnlyNew(t *testing.T) {
	s, ctx := openTestStore(t)
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Second)
	pts := dedupPoints(base)
	if err := s.Insert(ctx, pts[:1]); err != nil {
		t.Fatal(err)
	}
	extra := model.Point{Metric: "cpu", Labels: map[string]string{"host": "h2"}, Timestamp: base, Value: 3}
	if n := insertNew(t, s, ctx, append(pts, extra)); n != 3 {
		t.Fatalf("wrote %d, want 3", n)
	}
	if n := countRows(t, s, ctx); n != 4 {
		t.Fatalf("table has %d rows, want 4", n)
	}
}

// Dedup case 3: the same point twice in one batch is written once.
func TestInsertNewDedupsWithinBatch(t *testing.T) {
	s, ctx := openTestStore(t)
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	p := dedupPoints(time.Now().UTC())[0]
	if n := insertNew(t, s, ctx, []model.Point{p, p}); n != 1 {
		t.Fatalf("wrote %d, want 1", n)
	}
}

// Dedup case 4: label order doesn't matter (a Map keeps insertion order in ClickHouse).
func TestInsertNewIgnoresLabelOrder(t *testing.T) {
	s, ctx := openTestStore(t)
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	ts := time.Now().UTC()
	labels := map[string]string{}
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		labels[k] = k
	}
	// Insert the same series 5 times: Go map order is random, so the stored order varies.
	if n := insertNew(t, s, ctx, []model.Point{{Metric: "m", Labels: labels, Timestamp: ts}}); n != 1 {
		t.Fatalf("first wrote %d, want 1", n)
	}
	for i := 0; i < 5; i++ {
		if n := insertNew(t, s, ctx, []model.Point{{Metric: "m", Labels: labels, Timestamp: ts}}); n != 0 {
			t.Fatalf("repeat %d wrote %d, want 0", i, n)
		}
	}
}

// Dedup case 5: points at the very edges of a second are still found, though clickhouse-go
// binds time parameters at second precision.
func TestInsertNewSecondEdges(t *testing.T) {
	s, ctx := openTestStore(t)
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	sec := time.Now().UTC().Truncate(time.Second)
	pts := []model.Point{
		{Metric: "m", Labels: map[string]string{}, Timestamp: sec.Add(time.Nanosecond)},
		{Metric: "m", Labels: map[string]string{}, Timestamp: sec.Add(2*time.Second - time.Nanosecond)},
	}
	if n := insertNew(t, s, ctx, pts); n != 2 {
		t.Fatalf("first wrote %d, want 2", n)
	}
	for i := range pts { // one at a time, so each is the batch's min and max
		if n := insertNew(t, s, ctx, pts[i:i+1]); n != 0 {
			t.Errorf("edge point %d written again", i)
		}
	}
}

// Dedup case 6: a replay doesn't double-count the 1-minute rollup.
func TestInsertNewRollupNotDoubled(t *testing.T) {
	s, ctx := openTestStore(t)
	if err := s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	minute := time.Now().UTC().Truncate(time.Minute)
	pts := []model.Point{
		{Metric: "host_cpu_usage_percent", Labels: map[string]string{"host": "h1"}, Timestamp: minute.Add(time.Second), Value: 10},
		{Metric: "host_cpu_usage_percent", Labels: map[string]string{"host": "h1"}, Timestamp: minute.Add(2 * time.Second), Value: 20},
	}
	insertNew(t, s, ctx, pts)
	insertNew(t, s, ctx, pts)
	rows := rollups(t, s, ctx)
	if len(rows) != 1 || rows[0].count != 2 || rows[0].sum != 30 {
		t.Fatalf("rollups = %+v, want one minute with count 2, sum 30", rows)
	}
}

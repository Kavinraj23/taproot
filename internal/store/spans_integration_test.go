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

// openSpanStore opens a Store on uniquely named span tables, runs EnsureSpanSchema, and drops
// the span table, trace index and its view afterwards.
// Run with: TAPROOT_CLICKHOUSE_ADDR=localhost:9000 go test -tags integration ./internal/store/
func openSpanStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	addr := os.Getenv("TAPROOT_CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("TAPROOT_CLICKHOUSE_ADDR not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	s, err := Open(ctx, Config{
		Addr: addr, User: "taproot", Password: "taproot", Database: "taproot",
		Table:     "metrics_unused",
		SpanTable: fmt.Sprintf("spans_it_%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		for _, name := range []string{s.traceIndexView(), s.traceIndexTable(), s.spanTable()} {
			if err := s.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+name); err != nil {
				t.Errorf("drop %s: %v", name, err)
			}
		}
		s.Close()
	})
	if err := s.EnsureSpanSchema(ctx); err != nil {
		t.Fatalf("EnsureSpanSchema: %v", err)
	}
	return s, ctx
}

const trace1 = "4bf92f3577b34da6a3ce929d0e0e4736"

// Recent, so the 48-hour retention keeps it; the odd nanoseconds check DateTime64(9) precision.
var spanBase = time.Now().UTC().Truncate(time.Second).Add(123456789)

// threeSpans is one checkout request: frontend calls checkout, which calls payment.
func threeSpans() []model.Span {
	return []model.Span{
		{TraceID: trace1, SpanID: "aaaaaaaaaaaaaaa1", Service: "frontend", Name: "POST /api/checkout",
			Kind: "server", Start: spanBase, Duration: 310 * time.Millisecond, Status: "ok",
			Attributes: map[string]string{"http.route": "/api/checkout"},
			Resource:   map[string]string{"service.name": "frontend", "host.name": "h1"}},
		{TraceID: trace1, SpanID: "aaaaaaaaaaaaaaa2", ParentSpanID: "aaaaaaaaaaaaaaa1", Service: "checkout",
			Name: "PlaceOrder", Kind: "server", Start: spanBase.Add(10 * time.Millisecond),
			Duration: 280 * time.Millisecond, Status: "unset",
			Attributes: map[string]string{}, Resource: map[string]string{"service.name": "checkout"}},
		{TraceID: trace1, SpanID: "aaaaaaaaaaaaaaa3", ParentSpanID: "aaaaaaaaaaaaaaa2", Service: "checkout",
			Name: "orders publish", Kind: "producer", Start: spanBase.Add(400 * time.Millisecond),
			Duration: 3 * time.Millisecond, Status: "error",
			Attributes: map[string]string{"messaging.destination.name": "orders"},
			Resource:   map[string]string{"service.name": "checkout"}},
	}
}

func readSpans(t *testing.T, s *Store, ctx context.Context) []model.Span {
	t.Helper()
	rows, err := s.conn.Query(ctx, `SELECT trace_id, span_id, parent_span_id, service, name, kind,
       start, duration_ns, status, attributes, resource
FROM `+s.spanTable()+` ORDER BY start, trace_id`)
	if err != nil {
		t.Fatalf("select spans: %v", err)
	}
	defer rows.Close()
	var out []model.Span
	for rows.Next() {
		var sp model.Span
		var dur uint64
		if err := rows.Scan(&sp.TraceID, &sp.SpanID, &sp.ParentSpanID, &sp.Service, &sp.Name, &sp.Kind,
			&sp.Start, &dur, &sp.Status, &sp.Attributes, &sp.Resource); err != nil {
			t.Fatalf("scan span: %v", err)
		}
		sp.Duration = time.Duration(dur)
		out = append(out, sp)
	}
	return out
}

func countSpans(t *testing.T, s *Store, ctx context.Context) uint64 {
	t.Helper()
	var n uint64
	if err := s.conn.QueryRow(ctx, "SELECT count() FROM "+s.spanTable()).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// 1. Every field comes back as written.
func TestSpanRoundTrip(t *testing.T) {
	s, ctx := openSpanStore(t)
	want := threeSpans()
	if err := s.InsertSpans(ctx, want); err != nil {
		t.Fatalf("InsertSpans: %v", err)
	}
	got := readSpans(t, s, ctx)
	if len(got) != len(want) {
		t.Fatalf("got %d spans, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if !g.Start.Equal(w.Start) {
			t.Errorf("span %d start = %v, want %v", i, g.Start, w.Start)
		}
		g.Start, w.Start = time.Time{}, time.Time{}
		if !reflect.DeepEqual(g, w) {
			t.Errorf("span %d = %+v\nwant     %+v", i, g, w)
		}
	}
}

// 2. The trace index knows where in time the trace lives: earliest start, latest end.
func TestTraceIndex(t *testing.T) {
	s, ctx := openSpanStore(t)
	spans := threeSpans()
	// Two batches, like spans arriving from different services at different times.
	if err := s.InsertSpans(ctx, spans[:1]); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertSpans(ctx, spans[1:]); err != nil {
		t.Fatal(err)
	}
	var start, end time.Time
	if err := s.conn.QueryRow(ctx, "SELECT min(start_min), max(end_max) FROM "+s.traceIndexTable()+
		" WHERE trace_id = ?", trace1).Scan(&start, &end); err != nil {
		t.Fatal(err)
	}
	wantEnd := spans[2].End() // publish ends last: 400ms + 3ms, after frontend's 310ms
	if !start.Equal(spanBase) || !end.Equal(wantEnd) {
		t.Errorf("trace range = %v .. %v, want %v .. %v", start, end, spanBase, wantEnd)
	}
}

// 3. A replayed batch (processor crashed after writing, before committing) is not stored again.
func TestInsertNewSpansReplay(t *testing.T) {
	s, ctx := openSpanStore(t)
	if n, err := s.InsertNewSpans(ctx, threeSpans()); err != nil || n != 3 {
		t.Fatalf("first InsertNewSpans = %d, %v; want 3, nil", n, err)
	}
	if n, err := s.InsertNewSpans(ctx, threeSpans()); err != nil || n != 0 {
		t.Fatalf("replayed InsertNewSpans = %d, %v; want 0, nil", n, err)
	}
	if n := countSpans(t, s, ctx); n != 3 {
		t.Errorf("table has %d spans, want 3", n)
	}
}

// 4. The same span twice in one batch (the agent resent it) is stored once.
func TestInsertNewSpansRepeatInBatch(t *testing.T) {
	s, ctx := openSpanStore(t)
	sp := threeSpans()[0]
	if n, err := s.InsertNewSpans(ctx, []model.Span{sp, sp}); err != nil || n != 1 {
		t.Fatalf("InsertNewSpans = %d, %v; want 1, nil", n, err)
	}
	if n := countSpans(t, s, ctx); n != 1 {
		t.Errorf("table has %d spans, want 1", n)
	}
}

// 5. A batch that overlaps what's stored writes only the new part.
func TestInsertNewSpansPartial(t *testing.T) {
	s, ctx := openSpanStore(t)
	spans := threeSpans()
	if _, err := s.InsertNewSpans(ctx, spans[:2]); err != nil {
		t.Fatal(err)
	}
	if n, err := s.InsertNewSpans(ctx, spans); err != nil || n != 1 {
		t.Fatalf("InsertNewSpans = %d, %v; want 1, nil", n, err)
	}
	if n := countSpans(t, s, ctx); n != 3 {
		t.Errorf("table has %d spans, want 3", n)
	}
}

// 6. A span ID is only unique inside its trace: the same span ID in another trace is a new span.
func TestInsertNewSpansSameIDOtherTrace(t *testing.T) {
	s, ctx := openSpanStore(t)
	a := threeSpans()[0]
	b := a
	b.TraceID = "0af7651916cd43dd8448eb211c80319c"
	if _, err := s.InsertNewSpans(ctx, []model.Span{a}); err != nil {
		t.Fatal(err)
	}
	if n, err := s.InsertNewSpans(ctx, []model.Span{b}); err != nil || n != 1 {
		t.Fatalf("InsertNewSpans = %d, %v; want 1, nil", n, err)
	}
	if n := countSpans(t, s, ctx); n != 2 {
		t.Errorf("table has %d spans, want 2", n)
	}
}

// 7. Safe to run on every start, and both tables expire after the raw retention.
func TestEnsureSpanSchema(t *testing.T) {
	s, ctx := openSpanStore(t) // already ran once
	if err := s.EnsureSpanSchema(ctx); err != nil {
		t.Fatalf("second EnsureSpanSchema: %v", err)
	}
	wantTTL := fmt.Sprintf("toIntervalHour(%d)", RawRetentionHours) // how ClickHouse prints it
	for _, name := range []string{s.spanTable(), s.traceIndexTable()} {
		if ddl := showCreate(t, s, ctx, name); !strings.Contains(ddl, "TTL") || !strings.Contains(ddl, wantTTL) {
			t.Errorf("%s has no %s TTL:\n%s", name, wantTTL, ddl)
		}
	}
}

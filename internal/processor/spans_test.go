package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"taproot/internal/model"
)

func span(i int) model.Span {
	return model.Span{
		TraceID:      "4bf92f3577b34da6a3ce929d0e0e4736",
		SpanID:       fmt.Sprintf("aa000000000000%02x", i),
		ParentSpanID: "aa00000000000000",
		Service:      "checkout",
		Name:         "PlaceOrder",
		Kind:         "server",
		Start:        base.Add(time.Duration(i) * time.Millisecond),
		Duration:     280 * time.Millisecond,
		Status:       "ok",
		Attributes:   map[string]string{"rpc.method": "PlaceOrder"},
		Resource:     map[string]string{"service.name": "checkout"},
	}
}

func spanRec(t *testing.T, s model.Span, offset int64) *kgo.Record {
	t.Helper()
	v, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Value: v, Partition: 0, Offset: offset}
}

// spanHarness drives a Loop[model.Span] with scripted polls and write errors, like harness.
type spanHarness struct {
	writeErrs []error
	writes    [][]model.Span
	commits   [][]*kgo.Record
	events    []string
	waits     []time.Duration
	logs      []string
}

func runSpans(t *testing.T, polls [][]*kgo.Record, writeErrs ...error) *spanHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := &spanHarness{writeErrs: writeErrs}
	polled := 0
	loop := Loop[model.Span]{
		Decode: DecodeSpan,
		Poll: func(ctx context.Context) ([]*kgo.Record, error) {
			if polled >= len(polls) {
				cancel()
				<-ctx.Done()
				return nil, ctx.Err()
			}
			polled++
			h.events = append(h.events, "poll")
			return polls[polled-1], nil
		},
		Write: func(_ context.Context, spans []model.Span) error {
			i := len(h.writes)
			h.writes = append(h.writes, append([]model.Span(nil), spans...))
			h.events = append(h.events, "write")
			if i < len(h.writeErrs) {
				return h.writeErrs[i]
			}
			return nil
		},
		Commit: func(_ context.Context, recs []*kgo.Record) error {
			h.commits = append(h.commits, append([]*kgo.Record(nil), recs...))
			h.events = append(h.events, "commit")
			return nil
		},
		Wait: func(ctx context.Context, d time.Duration) error {
			h.waits = append(h.waits, d)
			return ctx.Err()
		},
		Logf: func(format string, args ...any) { h.logs = append(h.logs, fmt.Sprintf(format, args...)) },
	}
	done := make(chan struct{})
	go func() { defer close(done); Run(ctx, loop) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	return h
}

func sameSpans(t *testing.T, got, want []model.Span) {
	t.Helper()
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

// Span case 1: a span comes back exactly as the agent sent it.
func TestDecodeSpanValid(t *testing.T) {
	got, err := DecodeSpan(spanRec(t, span(1), 0))
	if err != nil {
		t.Fatal(err)
	}
	sameSpans(t, []model.Span{got}, []model.Span{span(1)})
}

// Span case 2: records that would make a meaningless or undeduplicatable row are rejected.
func TestDecodeSpanRejects(t *testing.T) {
	mod := func(f func(*model.Span)) model.Span { s := span(1); f(&s); return s }
	cases := map[string]*kgo.Record{
		"bad json":          {Value: []byte("not json")},
		"short trace id":    spanRec(t, mod(func(s *model.Span) { s.TraceID = "4bf9" }), 0),
		"upper span id":     spanRec(t, mod(func(s *model.Span) { s.SpanID = "AA00000000000001" }), 0),
		"non-hex span id":   spanRec(t, mod(func(s *model.Span) { s.SpanID = "zz00000000000001" }), 0),
		"empty service":     spanRec(t, mod(func(s *model.Span) { s.Service = "" }), 0),
		"missing start":     spanRec(t, mod(func(s *model.Span) { s.Start = time.Time{} }), 0),
		"all-zero trace id": spanRec(t, mod(func(s *model.Span) { s.TraceID = strings.Repeat("0", 32) }), 0),
	}
	for name, r := range cases {
		if _, err := DecodeSpan(r); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

// Span case 3: a bad span in the middle is skipped and logged; the good ones are written in
// order, and all three offsets are committed after the write.
func TestSpanLoopSkipsPoison(t *testing.T) {
	recs := []*kgo.Record{spanRec(t, span(1), 0), {Value: []byte("not json"), Offset: 1}, spanRec(t, span(2), 2)}
	h := runSpans(t, [][]*kgo.Record{recs})
	if len(h.writes) != 1 {
		t.Fatalf("Write called %d times, want 1", len(h.writes))
	}
	sameSpans(t, h.writes[0], []model.Span{span(1), span(2)})
	if got := strings.Join(h.events, ","); got != "poll,write,commit" {
		t.Errorf("events = %s, want poll,write,commit", got)
	}
	if len(h.commits) != 1 || len(h.commits[0]) != 3 {
		t.Errorf("commits = %v, want one commit of all 3 records", h.commits)
	}
	if len(h.logs) != 1 || !strings.Contains(h.logs[0], "offset 1") {
		t.Errorf("logs = %q, want one skip for offset 1", h.logs)
	}
}

// Span case 4: ClickHouse down for two attempts: retried with backoff, committed once, after.
func TestSpanLoopRetriesThenCommits(t *testing.T) {
	recs := []*kgo.Record{spanRec(t, span(1), 0)}
	h := runSpans(t, [][]*kgo.Record{recs}, errWrite, errWrite)
	if got := strings.Join(h.events, ","); got != "poll,write,write,write,commit" {
		t.Errorf("events = %s, want poll,write,write,write,commit", got)
	}
	if !reflect.DeepEqual(h.waits, []time.Duration{time.Second, 2 * time.Second}) {
		t.Errorf("waits = %v, want [1s 2s]", h.waits)
	}
}

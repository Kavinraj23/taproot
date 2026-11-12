package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"taproot/internal/model"
)

var (
	errWrite = errors.New("clickhouse unavailable")
	errFetch = errors.New("fetch failed")
	base     = time.Date(2026, 11, 11, 9, 30, 0, 123456789, time.UTC)
)

func pt(i int) model.Point {
	return model.Point{
		Metric:    "host_cpu_usage_percent",
		Labels:    map[string]string{"host": "h1"},
		Timestamp: base.Add(time.Duration(i) * time.Second),
		Value:     float64(i) + 0.5,
	}
}

func rec(t *testing.T, p model.Point, offset int64) *kgo.Record {
	t.Helper()
	v, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Value: v, Partition: 1, Offset: offset}
}

func badRec(offset int64) *kgo.Record {
	return &kgo.Record{Value: []byte("not json"), Partition: 2, Offset: offset}
}

// harness scripts Poll results and Write errors. Once the polls run out, it cancels the
// context so Run returns, and every later Poll reports the cancellation.
type harness struct {
	t      *testing.T
	cancel context.CancelFunc

	polls  []pollResult
	polled int

	writeErrs []error // one per Write call; nil or missing means success
	writes    [][]model.Point
	events    []string // "poll" / "write" in call order

	waits []time.Duration
	logs  []string
}

type pollResult struct {
	recs []*kgo.Record
	err  error
}

func run(t *testing.T, polls []pollResult, writeErrs ...error) *harness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := &harness{t: t, cancel: cancel, polls: polls, writeErrs: writeErrs}

	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, h.config())
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	return h
}

func (h *harness) config() Config {
	return Config{
		Poll: func(ctx context.Context) ([]*kgo.Record, error) {
			if h.polled >= len(h.polls) {
				h.cancel()
				<-ctx.Done()
				return nil, ctx.Err()
			}
			p := h.polls[h.polled]
			h.polled++
			h.events = append(h.events, "poll")
			return p.recs, p.err
		},
		Write: func(_ context.Context, pts []model.Point) error {
			i := len(h.writes)
			h.writes = append(h.writes, append([]model.Point(nil), pts...))
			h.events = append(h.events, "write")
			if i < len(h.writeErrs) {
				return h.writeErrs[i]
			}
			return nil
		},
		Wait: func(ctx context.Context, d time.Duration) error {
			h.waits = append(h.waits, d)
			return ctx.Err()
		},
		Logf: func(format string, args ...any) { h.logs = append(h.logs, fmt.Sprintf(format, args...)) },
	}
}

func samePoints(t *testing.T, got, want []model.Point) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d points, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Metric != w.Metric || g.Value != w.Value || !g.Timestamp.Equal(w.Timestamp) || !reflect.DeepEqual(g.Labels, w.Labels) {
			t.Errorf("point %d = %+v, want %+v", i, g, w)
		}
	}
}

// Case 1
func TestDecodeValid(t *testing.T) {
	got, err := Decode(rec(t, pt(0), 0))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	samePoints(t, []model.Point{got}, []model.Point{pt(0)})
	if got.Timestamp.Nanosecond() != 123456789 {
		t.Errorf("lost sub-second precision: %v", got.Timestamp)
	}
}

// Case 2
func TestDecodeInvalidJSON(t *testing.T) {
	if _, err := Decode(badRec(0)); err == nil {
		t.Fatal("want error for invalid JSON")
	}
}

// Case 3
func TestDecodeMissingFields(t *testing.T) {
	noMetric := pt(0)
	noMetric.Metric = ""
	if _, err := Decode(rec(t, noMetric, 0)); err == nil {
		t.Error("want error for empty metric")
	}
	noTS := &kgo.Record{Value: []byte(`{"metric":"m","labels":{},"value":1}`)}
	if _, err := Decode(noTS); err == nil {
		t.Error("want error for missing ts")
	}
}

// Case 4
func TestWritesBatchInOrder(t *testing.T) {
	h := run(t, []pollResult{{recs: []*kgo.Record{rec(t, pt(0), 0), rec(t, pt(1), 1), rec(t, pt(2), 2)}}})
	if len(h.writes) != 1 {
		t.Fatalf("Write called %d times, want 1", len(h.writes))
	}
	samePoints(t, h.writes[0], []model.Point{pt(0), pt(1), pt(2)})
}

// Case 5
func TestBadRecordSkippedAndLogged(t *testing.T) {
	h := run(t, []pollResult{{recs: []*kgo.Record{rec(t, pt(0), 0), badRec(7), rec(t, pt(1), 1)}}})
	if len(h.writes) != 1 {
		t.Fatalf("Write called %d times, want 1", len(h.writes))
	}
	samePoints(t, h.writes[0], []model.Point{pt(0), pt(1)})
	found := false
	for _, l := range h.logs {
		if strings.Contains(l, "partition 2") && strings.Contains(l, "offset 7") {
			found = true
		}
	}
	if !found {
		t.Errorf("logs = %q, want the bad record's partition 2 and offset 7", h.logs)
	}
}

// Case 6
func TestNothingToWrite(t *testing.T) {
	h := run(t, []pollResult{{recs: []*kgo.Record{badRec(0), badRec(1)}}, {recs: nil}})
	if len(h.writes) != 0 {
		t.Fatalf("Write called %d times, want 0", len(h.writes))
	}
}

// Case 7
func TestWriteRetriedUntilSuccess(t *testing.T) {
	recs := []*kgo.Record{rec(t, pt(0), 0), rec(t, pt(1), 1)}
	h := run(t, []pollResult{{recs: recs}, {recs: []*kgo.Record{rec(t, pt(2), 2)}}}, errWrite, errWrite)

	if len(h.writes) != 4 { // 3 attempts for batch 1, then batch 2
		t.Fatalf("Write called %d times, want 4", len(h.writes))
	}
	for i := 0; i < 3; i++ {
		samePoints(t, h.writes[i], []model.Point{pt(0), pt(1)})
	}
	if !reflect.DeepEqual(h.waits, []time.Duration{time.Second, 2 * time.Second}) {
		t.Errorf("waits = %v, want [1s 2s]", h.waits)
	}
	want := "poll,write,write,write,poll,write"
	if got := strings.Join(h.events, ","); got != want {
		t.Errorf("events = %s, want %s", got, want)
	}
}

// Case 8
func TestBackoff(t *testing.T) {
	want := []time.Duration{1, 2, 4, 8, 16, 30, 30}
	for i, w := range want {
		if got := backoff(i + 1); got != w*time.Second {
			t.Errorf("backoff(%d) = %v, want %v", i+1, got, w*time.Second)
		}
	}
	if got := backoff(100); got != 30*time.Second {
		t.Errorf("backoff(100) = %v, want 30s (no overflow)", got)
	}
}

// Case 9
func TestCancelDuringRetryStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	writes := 0
	cfg := Config{
		Poll: func(context.Context) ([]*kgo.Record, error) {
			return []*kgo.Record{rec(t, pt(0), 0)}, nil
		},
		Write: func(context.Context, []model.Point) error { writes++; return errWrite },
		Wait: func(context.Context, time.Duration) error {
			cancel() // shutdown arrives while waiting to retry
			return context.Canceled
		},
		Logf: func(string, ...any) {},
	}
	done := make(chan struct{})
	go func() { defer close(done); Run(ctx, cfg) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if writes != 1 {
		t.Errorf("Write called %d times, want 1", writes)
	}
}

// Case 10
func TestPollErrorLoggedAndContinues(t *testing.T) {
	h := run(t, []pollResult{{err: errFetch}, {recs: []*kgo.Record{rec(t, pt(0), 0)}}})
	if len(h.writes) != 1 {
		t.Fatalf("Write called %d times, want 1 (loop should continue after poll error)", len(h.writes))
	}
	found := false
	for _, l := range h.logs {
		if strings.Contains(l, errFetch.Error()) {
			found = true
		}
	}
	if !found {
		t.Errorf("logs = %q, want the poll error", h.logs)
	}
}

// Case 11
func TestCancelDuringPollReturns(t *testing.T) {
	h := run(t, nil) // first Poll cancels and blocks until ctx is done
	if len(h.writes) != 0 {
		t.Errorf("Write called %d times, want 0", len(h.writes))
	}
	for _, l := range h.logs {
		if strings.Contains(l, context.Canceled.Error()) {
			t.Errorf("shutdown logged as an error: %q", l)
		}
	}
}

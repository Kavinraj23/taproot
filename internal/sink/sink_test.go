package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"taproot/internal/model"
)

var errBroker = errors.New("broker unreachable")

func point(labels map[string]string) model.Point {
	return model.Point{
		Metric:    "host_cpu_usage_percent",
		Labels:    labels,
		Timestamp: time.Date(2026, 11, 11, 9, 30, 0, 123456789, time.UTC),
		Value:     22.39,
	}
}

// fakeProducer keeps each promise so the test decides when and how delivery finishes.
type fakeProducer struct {
	mu       sync.Mutex
	records  []*kgo.Record
	promises []func(*kgo.Record, error)
	calls    []string
	flushErr error
}

func (f *fakeProducer) TryProduce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, r)
	f.promises = append(f.promises, promise)
}

func (f *fakeProducer) Flush(context.Context) error {
	f.calls = append(f.calls, "flush")
	return f.flushErr
}

func (f *fakeProducer) Close() { f.calls = append(f.calls, "close") }

// deliver finishes the i-th produced record with err (nil means acknowledged).
func (f *fakeProducer) deliver(i int, err error) {
	f.mu.Lock()
	r, p := f.records[i], f.promises[i]
	f.mu.Unlock()
	p(r, err)
}

type logs struct {
	mu    sync.Mutex
	lines []string
}

func (l *logs) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func newTestSink() (*Sink, *fakeProducer, *logs) {
	fp, lg := &fakeProducer{}, &logs{}
	return New(fp, lg.logf), fp, lg
}

func countContaining(lines []string, sub string) int {
	n := 0
	for _, l := range lines {
		if strings.Contains(l, sub) {
			n++
		}
	}
	return n
}

// Case 1
func TestKeySingleLabel(t *testing.T) {
	got := string(Key(point(map[string]string{"host": "h1"})))
	if want := "host_cpu_usage_percent{host=h1}"; got != want {
		t.Fatalf("Key = %q, want %q", got, want)
	}
}

// Case 2
func TestKeySortsLabels(t *testing.T) {
	a := map[string]string{}
	a["b"], a["a"] = "2", "1"
	b := map[string]string{}
	b["a"], b["b"] = "1", "2"
	pa, pb := model.Point{Metric: "m", Labels: a}, model.Point{Metric: "m", Labels: b}

	for i := 0; i < 20; i++ { // map order is random; repeat to catch unsorted output
		ka, kb := string(Key(pa)), string(Key(pb))
		if ka != "m{a=1,b=2}" || kb != ka {
			t.Fatalf("keys %q and %q, want both %q", ka, kb, "m{a=1,b=2}")
		}
	}
}

// Case 3
func TestKeyNoLabels(t *testing.T) {
	if got := string(Key(model.Point{Metric: "m"})); got != "m{}" {
		t.Fatalf("Key = %q, want %q", got, "m{}")
	}
}

// Case 4
func TestEncodeRoundTrip(t *testing.T) {
	p := point(map[string]string{"host": "h1"})
	r, err := Encode(p)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if string(r.Key) != string(Key(p)) {
		t.Errorf("record key = %q, want %q", r.Key, Key(p))
	}
	if r.Topic != "" {
		t.Errorf("record topic = %q, want empty (client default fills it)", r.Topic)
	}
	var got model.Point
	if err := json.Unmarshal(r.Value, &got); err != nil {
		t.Fatalf("unmarshal value: %v", err)
	}
	if got.Metric != p.Metric || got.Value != p.Value || got.Labels["host"] != "h1" || len(got.Labels) != 1 {
		t.Errorf("decoded %+v, want %+v", got, p)
	}
	if !got.Timestamp.Equal(p.Timestamp) {
		t.Errorf("timestamp = %v, want %v", got.Timestamp, p.Timestamp)
	}
}

// Case 5
func TestEmitSuccessIsQuiet(t *testing.T) {
	s, fp, lg := newTestSink()
	s.Emit(point(map[string]string{"host": "h1"}))
	if len(fp.records) != 1 {
		t.Fatalf("produced %d records, want 1", len(fp.records))
	}
	fp.deliver(0, nil)
	if len(lg.lines) != 0 {
		t.Errorf("unexpected logs: %q", lg.lines)
	}
}

// Case 6
func TestRepeatedFailuresLogOnce(t *testing.T) {
	s, fp, lg := newTestSink()
	for i := 0; i < 3; i++ {
		s.Emit(point(nil))
		fp.deliver(i, errBroker)
	}
	if n := countContaining(lg.lines, "failing"); n != 1 || len(lg.lines) != 1 {
		t.Fatalf("logs = %q, want exactly one failing line", lg.lines)
	}
	if !strings.Contains(lg.lines[0], errBroker.Error()) {
		t.Errorf("failing line %q does not include the error", lg.lines[0])
	}
	if s.dropped != 3 {
		t.Errorf("dropped = %d, want 3", s.dropped)
	}
}

// Case 7
func TestRecoveryReportsDropCount(t *testing.T) {
	s, fp, lg := newTestSink()
	for i := 0; i < 3; i++ {
		s.Emit(point(nil))
		fp.deliver(i, errBroker)
	}
	s.Emit(point(nil))
	fp.deliver(3, nil)

	if len(lg.lines) != 2 || !strings.Contains(lg.lines[1], "recovered; dropped 3 points") {
		t.Fatalf("logs = %q, want failing line then recovered with 3 dropped", lg.lines)
	}
	if s.dropped != 0 {
		t.Errorf("dropped = %d after recovery, want 0", s.dropped)
	}
}

// Case 8
func TestBufferFullCountsAsDrop(t *testing.T) {
	s, fp, lg := newTestSink()
	s.Emit(point(nil))
	fp.deliver(0, kgo.ErrMaxBuffered)
	if countContaining(lg.lines, "failing") != 1 || s.dropped != 1 {
		t.Fatalf("logs = %q, dropped = %d; want one failing line and 1 drop", lg.lines, s.dropped)
	}
}

// Case 9
func TestEachOutageIsReported(t *testing.T) {
	s, fp, lg := newTestSink()
	for i, err := range []error{errBroker, nil, errBroker} {
		s.Emit(point(nil))
		fp.deliver(i, err)
	}
	if n := countContaining(lg.lines, "failing"); n != 2 {
		t.Fatalf("logs = %q, want 2 failing lines", lg.lines)
	}
}

// Case 10
func TestCloseFlushesThenCloses(t *testing.T) {
	s, fp, lg := newTestSink()
	fp.flushErr = context.DeadlineExceeded
	s.Close(time.Second)
	if strings.Join(fp.calls, ",") != "flush,close" {
		t.Errorf("calls = %v, want [flush close]", fp.calls)
	}
	if countContaining(lg.lines, context.DeadlineExceeded.Error()) != 1 {
		t.Errorf("logs = %q, want the flush error logged", lg.lines)
	}
}

// Case 11: run with -race.
func TestConcurrentPromises(t *testing.T) {
	s, fp, lg := newTestSink()
	const n = 200
	for i := 0; i < n; i++ {
		s.Emit(point(nil))
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fp.deliver(i, errBroker)
		}(i)
	}
	wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dropped != n {
		t.Errorf("dropped = %d, want %d", s.dropped, n)
	}
	if countContaining(lg.lines, "failing") != 1 {
		t.Errorf("logs = %q, want one failing line", lg.lines)
	}
}

// A point that can't be encoded (JSON has no NaN) is dropped, not sent, and goes
// through the same failing/dropped reporting.
func TestUnencodablePointIsDropped(t *testing.T) {
	s, fp, lg := newTestSink()
	p := point(nil)
	p.Value = math.NaN()
	s.Emit(p)
	if len(fp.records) != 0 {
		t.Fatalf("produced %d records, want 0", len(fp.records))
	}
	if countContaining(lg.lines, "failing") != 1 || s.dropped != 1 {
		t.Fatalf("logs = %q, dropped = %d; want one failing line and 1 drop", lg.lines, s.dropped)
	}
}

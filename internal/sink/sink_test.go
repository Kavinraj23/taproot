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
	"taproot/internal/spool"
)

var errTooLarge = errors.New("MESSAGE_TOO_LARGE")

func point(labels map[string]string) model.Point {
	return model.Point{
		Metric:    "host_cpu_usage_percent",
		Labels:    labels,
		Timestamp: time.Date(2026, 11, 11, 9, 30, 0, 123456789, time.UTC),
		Value:     22.39,
	}
}

func pointAt(i int) model.Point {
	p := point(map[string]string{"host": "h1"})
	p.Timestamp = p.Timestamp.Add(time.Duration(i) * time.Second)
	return p
}

// fakeProducer records each Produce. While hold is set, Produce blocks until it is closed or
// ctx is done (like a full client buffer). The test finishes promises with deliver.
type fakeProducer struct {
	mu       sync.Mutex
	records  []*kgo.Record
	promises []func(*kgo.Record, error)
	calls    []string
	hold     chan struct{}
	produced chan struct{} // one send per Produce
}

func newFake() *fakeProducer { return &fakeProducer{produced: make(chan struct{}, 10000)} }

func (f *fakeProducer) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	f.mu.Lock()
	hold := f.hold
	f.mu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			promise(r, ctx.Err())
			return
		}
	}
	f.mu.Lock()
	f.records = append(f.records, r)
	f.promises = append(f.promises, promise)
	f.mu.Unlock()
	f.produced <- struct{}{}
}

func (f *fakeProducer) Flush(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "flush")
	return nil
}

func (f *fakeProducer) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "close")
}

func (f *fakeProducer) setHold(c chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hold = c
}

// waitProduced waits until n more records have been produced.
func (f *fakeProducer) waitProduced(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-f.produced:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d records produced", i, n)
		}
	}
}

func (f *fakeProducer) record(i int) *kgo.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.records[i]
}

func (f *fakeProducer) deliver(i int, err error) {
	f.mu.Lock()
	r, p := f.records[i], f.promises[i]
	f.mu.Unlock()
	p(r, err)
}

func (f *fakeProducer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
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

func (l *logs) count(sub string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line, sub) {
			n++
		}
	}
	return n
}

// rig is a running sink over a spool in dir; stop shuts it down like the agent does.
type rig struct {
	s    *Sink
	fp   *fakeProducer
	sp   *spool.Spool
	lg   *logs
	stop func()
}

func newRig(t *testing.T, dir string) *rig {
	t.Helper()
	sp, err := spool.Open(dir, spool.Options{Logf: t.Logf})
	if err != nil {
		t.Fatal(err)
	}
	fp, lg := newFake(), &logs{}
	s := New(fp, sp, lg.logf)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	return &rig{s: s, fp: fp, sp: sp, lg: lg, stop: func() { cancel(); s.Close(time.Second) }}
}

func decodeTS(t *testing.T, r *kgo.Record) time.Time {
	t.Helper()
	var p model.Point
	if err := json.Unmarshal(r.Value, &p); err != nil {
		t.Fatalf("value %s: %v", r.Value, err)
	}
	return p.Timestamp
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

// Case 5: Emit goes to disk; Run sends it with the series key and the JSON point.
func TestEmitIsSentFromDisk(t *testing.T) {
	r := newRig(t, t.TempDir())
	defer r.stop()
	p := pointAt(0)
	r.s.Emit(p)
	r.fp.waitProduced(t, 1)
	rec := r.fp.record(0)
	if string(rec.Key) != string(Key(p)) {
		t.Errorf("key = %q, want %q", rec.Key, Key(p))
	}
	if ts := decodeTS(t, rec); !ts.Equal(p.Timestamp) {
		t.Errorf("ts = %v, want %v", ts, p.Timestamp)
	}
}

// Case 6: a confirmed point is crossed off and not sent again after a restart.
func TestAckedPointNotResent(t *testing.T) {
	dir := t.TempDir()
	r := newRig(t, dir)
	r.s.Emit(pointAt(0))
	r.s.Emit(pointAt(1))
	r.fp.waitProduced(t, 2)
	r.fp.deliver(0, nil) // only the first is confirmed
	r.stop()

	r2 := newRig(t, dir)
	defer r2.stop()
	r2.fp.waitProduced(t, 1)
	if ts := decodeTS(t, r2.fp.record(0)); !ts.Equal(pointAt(1).Timestamp) {
		t.Fatalf("resent %v, want only the unconfirmed point %v", ts, pointAt(1).Timestamp)
	}
	time.Sleep(50 * time.Millisecond)
	if n := r2.fp.count(); n != 1 {
		t.Fatalf("%d records resent, want 1", n)
	}
}

// Case 7: a permanent delivery error is logged once per episode and the point is crossed off,
// so one bad record can't block everything behind it.
func TestPermanentErrorLoggedAndSkipped(t *testing.T) {
	dir := t.TempDir()
	r := newRig(t, dir)
	r.s.Emit(pointAt(0))
	r.s.Emit(pointAt(1))
	r.fp.waitProduced(t, 2)
	r.fp.deliver(0, errTooLarge)
	r.fp.deliver(1, errTooLarge)
	if n := r.lg.count("MESSAGE_TOO_LARGE"); n != 1 {
		t.Errorf("error logged %d times, want once: %q", n, r.lg.lines)
	}
	r.stop()

	r2 := newRig(t, dir)
	defer r2.stop()
	time.Sleep(100 * time.Millisecond)
	if n := r2.fp.count(); n != 0 {
		t.Fatalf("%d failed records resent, want 0", n)
	}
}

// Case 8: a point cut off by shutdown is not crossed off: it is sent on the next start.
func TestShutdownErrorKeepsPoint(t *testing.T) {
	dir := t.TempDir()
	r := newRig(t, dir)
	r.s.Emit(pointAt(0))
	r.fp.waitProduced(t, 1)
	r.fp.deliver(0, context.Canceled)
	r.stop()

	r2 := newRig(t, dir)
	defer r2.stop()
	r2.fp.waitProduced(t, 1)
}

// Case 9: while Redpanda can't take more (Produce blocks), Emit still returns at once and
// points wait on disk.
func TestBlockedProducerDoesNotBlockEmit(t *testing.T) {
	r := newRig(t, t.TempDir())
	defer r.stop()
	release := make(chan struct{})
	r.fp.setHold(release)

	start := time.Now()
	for i := 0; i < 200; i++ {
		r.s.Emit(pointAt(i))
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("200 Emits took %v while blocked", d)
	}
	if n := r.sp.Pending(); n != 200 {
		t.Fatalf("Pending = %d, want 200", n)
	}
	r.fp.setHold(nil)
	close(release)
	r.fp.waitProduced(t, 200)
}

// Case 10: a growing backlog is reported once when it starts and once when it clears.
func TestBacklogLoggedOnceEachWay(t *testing.T) {
	r := newRig(t, t.TempDir())
	defer r.stop()
	release := make(chan struct{})
	r.fp.setHold(release)
	n := behindAt + 10
	for i := 0; i < n; i++ {
		r.s.Emit(pointAt(i))
	}
	r.s.checkBacklog()
	r.s.checkBacklog()
	if c := r.lg.count("waiting on disk"); c != 1 {
		t.Fatalf("behind logged %d times, want 1: %q", c, r.lg.lines)
	}
	r.fp.setHold(nil)
	close(release)
	r.fp.waitProduced(t, n)
	for i := 0; i < n; i++ {
		r.fp.deliver(i, nil)
	}
	r.s.checkBacklog()
	r.s.checkBacklog()
	if c := r.lg.count("caught up"); c != 1 {
		t.Fatalf("caught up logged %d times, want 1: %q", c, r.lg.lines)
	}
}

// Case 11: Close flushes the producer before closing it.
func TestCloseFlushesThenCloses(t *testing.T) {
	r := newRig(t, t.TempDir())
	r.stop()
	if got := strings.Join(r.fp.calls, ","); got != "flush,close" {
		t.Fatalf("calls = %s, want flush,close", got)
	}
}

// Case 12: a point that can't be encoded is logged and dropped, not written to disk.
func TestUnencodablePointIsDropped(t *testing.T) {
	r := newRig(t, t.TempDir())
	defer r.stop()
	p := pointAt(0)
	p.Value = math.NaN()
	r.s.Emit(p)
	if r.sp.Pending() != 0 {
		t.Fatalf("Pending = %d, want 0", r.sp.Pending())
	}
	if r.lg.count("encode") != 1 {
		t.Fatalf("logs = %q, want one encode error", r.lg.lines)
	}
}

// Framing round-trips keys and values of any content.
func TestFrameRoundTrip(t *testing.T) {
	for _, c := range [][2]string{{"", ""}, {"k", "v"}, {"m{a=1}", `{"x":"\u0000"}`}} {
		k, v, err := unframe(frame([]byte(c[0]), []byte(c[1])))
		if err != nil || string(k) != c[0] || string(v) != c[1] {
			t.Errorf("round trip of %q: got %q %q %v", c, k, v, err)
		}
	}
	if _, _, err := unframe([]byte{0, 0, 0, 9, 'a'}); err == nil {
		t.Error("want error for a key longer than the record")
	}
}

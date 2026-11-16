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

	writeErrs  []error // one per Write call; nil or missing means success
	writes     [][]model.Point
	commitErrs []error // one per Commit call; nil or missing means success
	commits    [][]*kgo.Record
	events     []string // "poll" / "write" / "commit" in call order

	waits []time.Duration
	logs  []string
}

type pollResult struct {
	recs []*kgo.Record
	err  error
}

func run(t *testing.T, polls []pollResult, writeErrs ...error) *harness {
	t.Helper()
	return runH(t, &harness{polls: polls, writeErrs: writeErrs})
}

// runH runs a prepared harness, for tests that also script Commit errors.
func runH(t *testing.T, h *harness) *harness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.t, h.cancel = t, cancel

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
		Commit: func(_ context.Context, recs []*kgo.Record) error {
			i := len(h.commits)
			h.commits = append(h.commits, append([]*kgo.Record(nil), recs...))
			h.events = append(h.events, "commit")
			if i < len(h.commitErrs) {
				return h.commitErrs[i]
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
	want := "poll,write,write,write,commit,poll,write,commit"
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
	writes, commits := 0, 0
	cfg := Config{
		Poll: func(context.Context) ([]*kgo.Record, error) {
			return []*kgo.Record{rec(t, pt(0), 0)}, nil
		},
		Write:  func(context.Context, []model.Point) error { writes++; return errWrite },
		Commit: func(context.Context, []*kgo.Record) error { commits++; return nil },
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
	if commits != 0 { // M3 commit case 3: an unwritten batch is never marked done
		t.Errorf("Commit called %d times, want 0", commits)
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

// M3 commit-after-write cases (docs/decisions/0007).

var errCommit = errors.New("commit failed")

func offsets(recs []*kgo.Record) []int64 {
	out := make([]int64, len(recs))
	for i, r := range recs {
		out[i] = r.Offset
	}
	return out
}

// Commit case 1: a batch is marked done only after it is written, with the same records.
func TestCommitAfterWrite(t *testing.T) {
	recs := []*kgo.Record{rec(t, pt(0), 0), rec(t, pt(1), 1)}
	h := run(t, []pollResult{{recs: recs}})
	if got := strings.Join(h.events, ","); got != "poll,write,commit" {
		t.Fatalf("events = %s, want poll,write,commit", got)
	}
	if !reflect.DeepEqual(offsets(h.commits[0]), []int64{0, 1}) {
		t.Errorf("committed offsets %v, want [0 1]", offsets(h.commits[0]))
	}
}

// Commit case 2: retries don't commit; the one commit follows the successful write.
func TestCommitOnlyAfterSuccessfulRetry(t *testing.T) {
	h := run(t, []pollResult{{recs: []*kgo.Record{rec(t, pt(0), 0)}}}, errWrite, errWrite)
	if got := strings.Join(h.events, ","); got != "poll,write,write,write,commit" {
		t.Fatalf("events = %s, want poll,write,write,write,commit", got)
	}
}

// Commit case 4: a batch of only bad records is not written but is committed, so it isn't
// re-read forever.
func TestCommitPoisonOnlyBatch(t *testing.T) {
	h := run(t, []pollResult{{recs: []*kgo.Record{badRec(3), badRec(4)}}})
	if len(h.writes) != 0 {
		t.Fatalf("Write called %d times, want 0", len(h.writes))
	}
	if len(h.commits) != 1 || !reflect.DeepEqual(offsets(h.commits[0]), []int64{3, 4}) {
		t.Fatalf("commits = %v, want one commit of offsets [3 4]", h.commits)
	}
}

// Commit case 5: skipped bad records are committed along with the written ones.
func TestCommitIncludesSkippedRecords(t *testing.T) {
	h := run(t, []pollResult{{recs: []*kgo.Record{rec(t, pt(0), 0), badRec(7), rec(t, pt(1), 1)}}})
	if len(h.commits) != 1 || !reflect.DeepEqual(offsets(h.commits[0]), []int64{0, 7, 1}) {
		t.Fatalf("commits = %v, want one commit of offsets [0 7 1]", h.commits)
	}
}

// Commit case 6: a failed commit is logged and the loop goes on; the next commit covers it.
func TestCommitErrorLoggedAndContinues(t *testing.T) {
	h := runH(t, &harness{
		polls:      []pollResult{{recs: []*kgo.Record{rec(t, pt(0), 0)}}, {recs: []*kgo.Record{rec(t, pt(1), 1)}}},
		commitErrs: []error{errCommit},
	})
	if got := strings.Join(h.events, ","); got != "poll,write,commit,poll,write,commit" {
		t.Fatalf("events = %s, want two full rounds", got)
	}
	found := false
	for _, l := range h.logs {
		if strings.Contains(l, errCommit.Error()) {
			found = true
		}
	}
	if !found {
		t.Errorf("logs = %q, want the commit error", h.logs)
	}
}

// Commit case 7: each batch gets its own commit, in order.
func TestCommitPerBatchInOrder(t *testing.T) {
	h := run(t, []pollResult{{recs: []*kgo.Record{rec(t, pt(0), 0)}}, {recs: []*kgo.Record{rec(t, pt(1), 1)}}})
	if len(h.commits) != 2 || h.commits[0][0].Offset != 0 || h.commits[1][0].Offset != 1 {
		t.Fatalf("commits = %v, want offsets 0 then 1", h.commits)
	}
}

// An empty poll has nothing to mark done.
func TestNoCommitForEmptyPoll(t *testing.T) {
	h := run(t, []pollResult{{recs: nil}})
	if len(h.commits) != 0 {
		t.Fatalf("commits = %v, want none", h.commits)
	}
}

package losscheck

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"taproot/internal/model"
)

var t0 = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

func pt(metric string, labels map[string]string, ts time.Time) model.Point {
	return model.Point{Metric: metric, Labels: labels, Timestamp: ts, Value: 1}
}

func st(metric string, labels map[string]string, ts time.Time, copies uint64) Stored {
	return Stored{Metric: metric, Labels: labels, TS: ts, Copies: copies}
}

func h(host string) map[string]string { return map[string]string{"host": host} }

// 1
func TestCompareIdentical(t *testing.T) {
	sent := []model.Point{pt("cpu", h("a"), t0), pt("cpu", h("a"), t0.Add(time.Second)), pt("mem", h("a"), t0)}
	stored := []Stored{st("cpu", h("a"), t0, 1), st("cpu", h("a"), t0.Add(time.Second), 1), st("mem", h("a"), t0, 1)}
	missing, dups, _ := Compare(sent, stored)
	if missing != 0 || dups != 0 {
		t.Fatalf("missing %d dups %d, want 0 0", missing, dups)
	}
}

// 2
func TestCompareMissing(t *testing.T) {
	sent := []model.Point{pt("cpu", h("a"), t0), pt("cpu", h("a"), t0.Add(time.Second))}
	stored := []Stored{st("cpu", h("a"), t0, 1)}
	missing, dups, sample := Compare(sent, stored)
	if missing != 1 || dups != 0 {
		t.Fatalf("missing %d dups %d, want 1 0", missing, dups)
	}
	if len(sample) != 1 || !strings.Contains(sample[0], `cpu{host="a"}`) || !strings.Contains(sample[0], "12:00:01") {
		t.Fatalf("sample %q should name cpu{host=\"a\"} at 12:00:01", sample)
	}
}

// 3
func TestCompareDuplicates(t *testing.T) {
	sent := []model.Point{pt("cpu", h("a"), t0)}
	stored := []Stored{st("cpu", h("a"), t0, 3)}
	if missing, dups, _ := Compare(sent, stored); missing != 0 || dups != 2 {
		t.Fatalf("missing %d dups %d, want 0 2", missing, dups)
	}
}

// 4
func TestCompareLabelOrder(t *testing.T) {
	// Go maps have no order; build them differently anyway to make the intent clear.
	a := map[string]string{"service": "kafka", "instance": "kafka"}
	b := map[string]string{"instance": "kafka", "service": "kafka"}
	if missing, _, _ := Compare([]model.Point{pt("up", a, t0)}, []Stored{st("up", b, t0, 1)}); missing != 0 {
		t.Fatalf("missing %d, want 0", missing)
	}
}

// 5
func TestCompareResentPointCountsOnce(t *testing.T) {
	sent := []model.Point{pt("cpu", h("a"), t0), pt("cpu", h("a"), t0)}
	stored := []Stored{st("cpu", h("a"), t0, 1)}
	if missing, dups, _ := Compare(sent, stored); missing != 0 || dups != 0 {
		t.Fatalf("missing %d dups %d, want 0 0", missing, dups)
	}
}

// 6
func TestCompareIgnoresStoredOnly(t *testing.T) {
	sent := []model.Point{pt("cpu", h("a"), t0)}
	stored := []Stored{st("cpu", h("a"), t0, 1), st("cpu", h("a"), t0.Add(-time.Hour), 1)}
	if missing, dups, _ := Compare(sent, stored); missing != 0 || dups != 0 {
		t.Fatalf("missing %d dups %d, want 0 0", missing, dups)
	}
}

// 7
func TestCompareDifferentLabelValues(t *testing.T) {
	sent := []model.Point{pt("cpu", h("a"), t0), pt("cpu", h("b"), t0)}
	stored := []Stored{st("cpu", h("a"), t0, 1)}
	if missing, _, _ := Compare(sent, stored); missing != 1 {
		t.Fatalf("missing %d, want 1", missing)
	}
}

// 8
func TestCompareNanosecondsMatter(t *testing.T) {
	sent := []model.Point{pt("cpu", h("a"), t0.Add(time.Nanosecond))}
	stored := []Stored{st("cpu", h("a"), t0, 1)}
	if missing, _, _ := Compare(sent, stored); missing != 1 {
		t.Fatalf("missing %d, want 1", missing)
	}
}

// steady returns n timestamps one second apart starting at from.
func steady(from time.Time, n int) []time.Time {
	out := make([]time.Time, n)
	for i := range out {
		out[i] = from.Add(time.Duration(i) * time.Second)
	}
	return out
}

// 9
func TestGapsIgnoreClockJump(t *testing.T) {
	s := append(steady(t0, 10), steady(t0.Add(13*time.Second), 10)...) // one 4s step
	if g := Gaps(map[string][]time.Time{"x": s}, 5*time.Second); len(g) != 0 {
		t.Fatalf("gaps %v, want none", g)
	}
}

// 10
func TestGapsOneHole(t *testing.T) {
	s := append(steady(t0, 10), steady(t0.Add(74*time.Second), 10)...) // 9s -> 74s: 65s hole
	got := Gaps(map[string][]time.Time{"x": s}, 5*time.Second)
	want := []Gap{{From: t0.Add(9 * time.Second), To: t0.Add(74 * time.Second), Series: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// 11
func TestGapsMergeSameOutage(t *testing.T) {
	series := map[string][]time.Time{}
	for i, name := range []string{"a", "b", "c"} {
		off := time.Duration(i) * 300 * time.Millisecond
		series[name] = append(steady(t0.Add(off), 10), steady(t0.Add(74*time.Second+off), 10)...)
	}
	got := Gaps(series, 5*time.Second)
	if len(got) != 1 || got[0].Series != 3 {
		t.Fatalf("got %v, want one gap across 3 series", got)
	}
	if !got[0].From.Equal(t0.Add(9*time.Second)) || !got[0].To.Equal(t0.Add(74*time.Second+600*time.Millisecond)) {
		t.Fatalf("got %v, want 12:00:09 to 12:01:14.6", got[0])
	}
}

// 12
func TestGapsTwoOutages(t *testing.T) {
	s := steady(t0, 5)                                  // 0..4
	s = append(s, steady(t0.Add(30*time.Second), 5)...) // hole 4 -> 30
	s = append(s, steady(t0.Add(90*time.Second), 5)...) // hole 34 -> 90
	got := Gaps(map[string][]time.Time{"x": s}, 5*time.Second)
	want := []Gap{
		{From: t0.Add(4 * time.Second), To: t0.Add(30 * time.Second), Series: 1},
		{From: t0.Add(34 * time.Second), To: t0.Add(90 * time.Second), Series: 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// 13
func TestGapsTinySeries(t *testing.T) {
	got := Gaps(map[string][]time.Time{"empty": nil, "one": {t0}}, 5*time.Second)
	if len(got) != 0 {
		t.Fatalf("got %v, want none", got)
	}
}

// 14
func TestGapsUnsortedInput(t *testing.T) {
	s := append(steady(t0, 10), steady(t0.Add(74*time.Second), 10)...)
	rev := make([]time.Time, len(s))
	for i := range s {
		rev[len(s)-1-i] = s[i]
	}
	a := Gaps(map[string][]time.Time{"x": s}, 5*time.Second)
	b := Gaps(map[string][]time.Time{"x": rev}, 5*time.Second)
	if !reflect.DeepEqual(a, b) || len(a) != 1 {
		t.Fatalf("sorted %v, unsorted %v", a, b)
	}
}

// A (user's pick): a label value containing "," and "=" must not pass for two separate labels.
// {a: "1,b=2"} and {a: "1", b: "2"} are different series, so the point is missing.
func TestCompareLabelValueLooksLikeTwoLabels(t *testing.T) {
	sent := []model.Point{pt("m", map[string]string{"a": "1,b=2"}, t0)}
	stored := []Stored{st("m", map[string]string{"a": "1", "b": "2"}, t0, 1)}
	if missing, _, _ := Compare(sent, stored); missing != 1 {
		t.Fatalf("missing %d, want 1: the two label sets were treated as the same series", missing)
	}
}

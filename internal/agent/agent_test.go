package agent

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"taproot/internal/cpu"
	"taproot/internal/model"
)

var (
	r0 = cpu.Times{User: 4705, Nice: 150, System: 1120, Idle: 16250, IOWait: 520, SoftIRQ: 25}
	r1 = cpu.Times{User: 4755, Nice: 150, System: 1140, Idle: 16500, IOWait: 530, SoftIRQ: 30} // 22.39% after r0

	reset      = cpu.Times{User: 100, System: 50, Idle: 800}
	afterReset = cpu.Times{User: 150, System: 70, Idle: 900} // 41.18% after reset

	errRead = errors.New("read failed")
)

type result struct {
	times cpu.Times
	err   error
}

// harness drives Run with scripted readings and a fake clock that moves one second per Now call.
type harness struct {
	t      *testing.T
	reads  []result
	next   int
	clock  time.Time
	points []model.Point
	logs   []string
}

func newHarness(t *testing.T, reads ...result) *harness {
	return &harness{t: t, reads: reads, clock: time.Date(2026, 11, 8, 14, 0, 0, 0, time.UTC)}
}

func ok(t cpu.Times) result { return result{times: t} }
func fail() result          { return result{err: errRead} }

func (h *harness) config() Config {
	return Config{
		Read: func() (cpu.Times, error) {
			if h.next >= len(h.reads) {
				h.t.Errorf("Run read more often than scripted (%d reads)", len(h.reads))
				return cpu.Times{}, errRead
			}
			r := h.reads[h.next]
			h.next++
			return r.times, r.err
		},
		Now: func() time.Time {
			n := h.clock
			h.clock = h.clock.Add(time.Second)
			return n
		},
		Host: "test",
		Emit: func(p model.Point) { h.points = append(h.points, p) },
		Logf: func(format string, args ...any) { h.logs = append(h.logs, format) },
	}
}

// runTicks sends n ticks, closes the channel, and runs to completion.
func (h *harness) runTicks(n int) {
	ticks := make(chan time.Time, n)
	for i := 0; i < n; i++ {
		ticks <- time.Time{}
	}
	close(ticks)
	Run(context.Background(), ticks, h.config())
}

func (h *harness) wantPoints(values ...float64) {
	h.t.Helper()
	if len(h.points) != len(values) {
		h.t.Fatalf("got %d points, want %d: %+v", len(h.points), len(values), h.points)
	}
	for i, want := range values {
		p := h.points[i]
		if p.Metric != "host_cpu_usage_percent" {
			h.t.Errorf("point %d metric = %q", i, p.Metric)
		}
		if p.Labels["host"] != "test" {
			h.t.Errorf("point %d labels = %v, want host=test", i, p.Labels)
		}
		if math.Abs(p.Value-want) > 0.01 {
			h.t.Errorf("point %d value = %.4f, want %.2f", i, p.Value, want)
		}
	}
}

func TestBaselineOnly(t *testing.T) {
	h := newHarness(t, ok(r0))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	Run(ctx, make(chan time.Time), h.config())
	h.wantPoints()
}

func TestOneTick(t *testing.T) {
	h := newHarness(t, ok(r0), ok(r1))
	h.runTicks(1)
	h.wantPoints(22.39)
	// Now is called once per read: the baseline is the first call, this tick the second.
	want := time.Date(2026, 11, 8, 14, 0, 1, 0, time.UTC)
	if !h.points[0].Timestamp.Equal(want) {
		t.Errorf("timestamp = %v, want the time of that read, %v", h.points[0].Timestamp, want)
	}
}

func TestResetInTheMiddle(t *testing.T) {
	h := newHarness(t, ok(r0), ok(r1), ok(reset), ok(afterReset))
	h.runTicks(3)
	h.wantPoints(22.39, 41.18)
}

func TestReadErrorKeepsGoing(t *testing.T) {
	h := newHarness(t, ok(r0), fail(), ok(r1))
	h.runTicks(2)
	h.wantPoints(22.39) // r1 compared with r0, across the gap
	if len(h.logs) != 1 {
		t.Errorf("logged %d messages, want 1: %v", len(h.logs), h.logs)
	}
}

// The very first (baseline) read fails: no baseline exists, so the first good reading
// becomes the baseline (no point) and the one after it produces the first point.
func TestBaselineReadFails(t *testing.T) {
	h := newHarness(t, fail(), ok(r0), ok(r1))
	h.runTicks(2)
	h.wantPoints(22.39)
	if len(h.logs) != 1 {
		t.Errorf("logged %d messages, want 1: %v", len(h.logs), h.logs)
	}
}

func TestStopsOnCancel(t *testing.T) {
	h := newHarness(t, ok(r0))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Run(ctx, make(chan time.Time), h.config()) // no ticks ever arrive
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
}

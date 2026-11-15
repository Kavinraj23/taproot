package sensor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"taproot/internal/discovery"
	"taproot/internal/model"
)

// fakeCollector returns scripted results, one per Collect call (the last one repeats).
type fakeCollector struct {
	addr    string
	results []fakeResult
	calls   int
	closed  bool
}

type fakeResult struct {
	readings []Reading
	err      error
}

func (f *fakeCollector) Collect(context.Context) ([]Reading, error) {
	r := f.results[min(f.calls, len(f.results)-1)]
	f.calls++
	return r.readings, r.err
}

func (f *fakeCollector) Close() error { f.closed = true; return nil }

// harness wires a Manager to fakes. Each started instance gets its own unbuffered tick channel,
// so a send returns only once that instance's loop has picked it up and finished the previous
// tick; Stop/Apply(Removed) wait for loops to exit, so recorded data is final afterwards.
type harness struct {
	t   *testing.T
	m   *Manager
	mu  sync.Mutex
	pts []model.Point
	log []string

	ticks      []chan time.Time
	collectors []*fakeCollector
	script     []fakeResult
	clock      time.Time
}

func newHarness(t *testing.T, script ...fakeResult) *harness {
	h := &harness{t: t, script: script, clock: time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)}
	h.m = NewManager(ManagerConfig{
		Factories: map[string]Factory{
			"postgres": func(svc discovery.Service, addr string) (Collector, error) {
				c := &fakeCollector{addr: addr, results: h.script}
				h.mu.Lock()
				h.collectors = append(h.collectors, c)
				h.mu.Unlock()
				return c, nil
			},
		},
		Emit: func(p model.Point) { h.mu.Lock(); h.pts = append(h.pts, p); h.mu.Unlock() },
		Now: func() time.Time {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.clock = h.clock.Add(time.Second)
			return h.clock
		},
		Host: "docker-vm",
		NewTicker: func(time.Duration) (<-chan time.Time, func()) {
			ch := make(chan time.Time)
			h.mu.Lock()
			h.ticks = append(h.ticks, ch)
			h.mu.Unlock()
			return ch, func() {}
		},
		Logf: func(f string, a ...any) { h.mu.Lock(); h.log = append(h.log, fmt.Sprintf(f, a...)); h.mu.Unlock() },
	})
	t.Cleanup(h.m.Stop)
	return h
}

func (h *harness) tick(i int) {
	h.t.Helper()
	h.mu.Lock()
	if i >= len(h.ticks) {
		h.mu.Unlock()
		h.t.Fatalf("instance %d has no ticker (only %d started)", i, len(h.ticks))
	}
	ch := h.ticks[i]
	h.mu.Unlock()
	select {
	case ch <- time.Now():
	case <-time.After(2 * time.Second):
		h.t.Fatalf("instance %d did not take a tick", i)
	}
}

func (h *harness) points() []model.Point {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]model.Point(nil), h.pts...)
}

func (h *harness) logs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.log...)
}

func find(pts []model.Point, metric string) []model.Point {
	var out []model.Point
	for _, p := range pts {
		if p.Metric == metric {
			out = append(out, p)
		}
	}
	return out
}

var pgSvc = discovery.Service{Kind: "postgres", PID: 412, Name: "postgres", Port: 5432, Container: "astronomy-db"}

func added(s ...discovery.Service) discovery.Changes   { return discovery.Changes{Added: s} }
func removed(s ...discovery.Service) discovery.Changes { return discovery.Changes{Removed: s} }
func updated(s ...discovery.Service) discovery.Changes { return discovery.Changes{Updated: s} }

func TestManagerEmitsGaugesWithLabelsAndUp(t *testing.T) {
	h := newHarness(t, fakeResult{readings: []Reading{
		{Metric: "postgres_connections", Kind: Gauge, Value: 3, Labels: map[string]string{"state": "active"}},
	}})
	h.m.Apply(added(pgSvc))
	h.tick(0)
	h.m.Stop()

	pts := h.points()
	conns := find(pts, "postgres_connections")
	if len(conns) != 1 || conns[0].Value != 3 {
		t.Fatalf("postgres_connections = %+v, want one point of 3", conns)
	}
	want := map[string]string{"host": "docker-vm", "service": "postgres", "instance": "astronomy-db", "state": "active"}
	for k, v := range want {
		if conns[0].Labels[k] != v {
			t.Errorf("label %s = %q, want %q (labels %v)", k, conns[0].Labels[k], v, conns[0].Labels)
		}
	}
	if up := find(pts, "postgres_up"); len(up) != 1 || up[0].Value != 1 || up[0].Labels["instance"] != "astronomy-db" {
		t.Errorf("postgres_up = %+v, want one point of 1 for astronomy-db", up)
	}
	if h.collectors[0].addr != "astronomy-db:5432" {
		t.Errorf("collector addr = %q, want astronomy-db:5432", h.collectors[0].addr)
	}
}

func TestManagerCounterBecomesRate(t *testing.T) {
	h := newHarness(t,
		fakeResult{readings: []Reading{{Metric: "postgres_xact_commit", Kind: Counter, Value: 100}}},
		fakeResult{readings: []Reading{{Metric: "postgres_xact_commit", Kind: Counter, Value: 141}}},
	)
	h.m.Apply(added(pgSvc))
	h.tick(0)
	h.tick(0)
	h.m.Stop()

	pts := h.points()
	if raw := find(pts, "postgres_xact_commit"); len(raw) != 0 {
		t.Errorf("raw counter emitted: %+v", raw)
	}
	r := find(pts, "postgres_xact_commit_per_sec")
	if len(r) != 1 || r[0].Value != 41 {
		t.Errorf("postgres_xact_commit_per_sec = %+v, want one point of 41 (none on the first tick)", r)
	}
}

func TestManagerCollectErrorsReportDownAndLogOnChange(t *testing.T) {
	gauge := []Reading{{Metric: "postgres_connections_max", Kind: Gauge, Value: 100}}
	h := newHarness(t,
		fakeResult{err: errors.New("connection refused")},
		fakeResult{err: errors.New("connection refused")},
		fakeResult{readings: gauge},
	)
	h.m.Apply(added(pgSvc))
	h.tick(0)
	h.tick(0)
	h.tick(0)
	h.m.Stop()

	pts := h.points()
	up := find(pts, "postgres_up")
	if len(up) != 3 || up[0].Value != 0 || up[1].Value != 0 || up[2].Value != 1 {
		t.Errorf("postgres_up = %+v, want 0, 0, 1", up)
	}
	if g := find(pts, "postgres_connections_max"); len(g) != 1 {
		t.Errorf("gauges while down = %+v, want only the one after recovery", g)
	}
	if logs := h.logs(); len(logs) != 2 {
		t.Errorf("logs = %q, want 2 (first failure, recovery)", logs)
	}
}

func TestManagerRemovedStopsAndCloses(t *testing.T) {
	h := newHarness(t, fakeResult{readings: []Reading{{Metric: "x", Kind: Gauge, Value: 1}}})
	h.m.Apply(added(pgSvc))
	h.tick(0)
	h.m.Apply(removed(pgSvc))
	if !h.collectors[0].closed {
		t.Error("collector not closed after Removed")
	}
	n := len(h.points())
	// The loop has exited: a tick would never be taken.
	select {
	case h.ticks[0] <- time.Now():
		t.Error("removed instance still taking ticks")
	case <-time.After(50 * time.Millisecond):
	}
	if len(h.points()) != n {
		t.Error("points emitted after Removed")
	}
}

func TestManagerUpdatedRestartsWithNewAddr(t *testing.T) {
	h := newHarness(t, fakeResult{readings: nil})
	h.m.Apply(added(pgSvc))
	moved := pgSvc
	moved.Port = 6543
	h.m.Apply(updated(moved))
	h.m.Stop()

	if len(h.collectors) != 2 {
		t.Fatalf("collectors built = %d, want 2", len(h.collectors))
	}
	if !h.collectors[0].closed {
		t.Error("old collector not closed")
	}
	if h.collectors[1].addr != "astronomy-db:6543" {
		t.Errorf("new addr = %q, want astronomy-db:6543", h.collectors[1].addr)
	}
}

func TestManagerWaitsForPortAndName(t *testing.T) {
	h := newHarness(t, fakeResult{readings: nil})
	noPort := pgSvc
	noPort.Port = 0
	noName := pgSvc
	noName.PID, noName.Container = 900, ""
	h.m.Apply(added(noPort, noName))
	if len(h.collectors) != 0 {
		t.Errorf("collectors built = %d, want 0 without port or container name", len(h.collectors))
	}
	if logs := h.logs(); len(logs) != 2 {
		t.Errorf("logs = %q, want one line per skipped service", logs)
	}
	h.m.Apply(updated(pgSvc)) // port appeared
	if len(h.collectors) != 1 {
		t.Errorf("collectors built = %d, want 1 once the port is known", len(h.collectors))
	}
}

func TestManagerIgnoresKindsWithoutSensor(t *testing.T) {
	h := newHarness(t, fakeResult{readings: nil})
	h.m.Apply(added(discovery.Service{Kind: "valkey", PID: 500, Port: 6379, Container: "valkey-cart"}))
	if len(h.collectors) != 0 || len(h.logs()) != 0 {
		t.Errorf("collectors = %d, logs = %q; want none for a kind without a sensor", len(h.collectors), h.logs())
	}
}

func TestManagerStopClosesAll(t *testing.T) {
	h := newHarness(t, fakeResult{readings: nil})
	second := pgSvc
	second.PID, second.Container = 900, "other-db"
	h.m.Apply(added(pgSvc, second))
	done := make(chan struct{})
	go func() { h.m.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return")
	}
	for i, c := range h.collectors {
		if !c.closed {
			t.Errorf("collector %d not closed", i)
		}
	}
}

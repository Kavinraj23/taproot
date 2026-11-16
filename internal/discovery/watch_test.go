package discovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type step struct {
	svcs []Service
	err  error
}

// watchHarness runs Watch with a scripted Discover. The tick and trigger channels are unbuffered,
// so a send only goes through once Watch has finished the previous round; stop() waits for Watch
// to return, so everything it recorded is complete. Recorded slices are only read after stop().
type watchHarness struct {
	t        *testing.T
	ticks    chan time.Time
	triggers chan struct{}
	cancel   context.CancelFunc
	done     chan struct{}

	steps   []step
	changes []Changes
	logs    []string

	// Fake time for retries: every After call hands its channel to afters, and Now reads now.
	afters chan chan time.Time
	mu     sync.Mutex
	now    time.Time
}

func startWatch(t *testing.T, steps ...step) *watchHarness {
	t.Helper()
	h := &watchHarness{
		t:        t,
		ticks:    make(chan time.Time),
		triggers: make(chan struct{}),
		done:     make(chan struct{}),
		steps:    steps,
		afters:   make(chan chan time.Time, 100),
		now:      time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC),
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() {
		defer close(h.done)
		Watch(ctx, WatchConfig{
			Discover: h.discover,
			Ticks:    h.ticks,
			Triggers: h.triggers,
			OnChange: func(c Changes) { h.changes = append(h.changes, c) },
			Logf:     func(f string, a ...any) { h.logs = append(h.logs, fmt.Sprintf(f, a...)) },
			After: func(time.Duration) <-chan time.Time {
				ch := make(chan time.Time, 1)
				h.afters <- ch
				return ch
			},
			Now: func() time.Time { h.mu.Lock(); defer h.mu.Unlock(); return h.now },
		})
	}()
	t.Cleanup(func() { cancel(); <-h.done })
	return h
}

func (h *watchHarness) discover(context.Context) ([]Service, error) {
	if len(h.steps) == 0 {
		h.t.Error("Discover called more often than scripted")
		return nil, errors.New("unscripted")
	}
	s := h.steps[0]
	h.steps = h.steps[1:]
	return s.svcs, s.err
}

func (h *watchHarness) tick()    { h.ticks <- time.Now() }
func (h *watchHarness) trigger() { h.triggers <- struct{}{} }

// setNow moves the fake clock to start + d.
func (h *watchHarness) setNow(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.now = time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC).Add(d)
}

// nextRetry waits for Watch to schedule a retry and returns its channel; nil if none comes.
func (h *watchHarness) nextRetry() chan time.Time {
	select {
	case ch := <-h.afters:
		return ch
	case <-time.After(time.Second):
		return nil
	}
}

// pendingRetries counts retries scheduled but not yet taken; call after stop().
func (h *watchHarness) pendingRetries() int { return len(h.afters) }

// stop ends Watch and waits for it, so the recorded changes and logs are final.
func (h *watchHarness) stop() {
	h.t.Helper()
	h.cancel()
	select {
	case <-h.done:
	case <-time.After(2 * time.Second):
		h.t.Fatal("Watch did not return after cancel")
	}
}

func TestWatchInitialDiscoverReportsAllAdded(t *testing.T) {
	h := startWatch(t, step{svcs: []Service{pg(412, 5432), vk(500)}})
	h.stop()
	if len(h.changes) != 1 || !sameChanges(h.changes[0], Changes{Added: []Service{pg(412, 5432), vk(500)}}) {
		t.Errorf("changes = %+v, want one change with both added", h.changes)
	}
}

func TestWatchQuietWhenNothingChanged(t *testing.T) {
	h := startWatch(t, step{svcs: []Service{pg(412, 5432)}}, step{svcs: []Service{pg(412, 5432)}})
	h.tick()
	h.stop()
	if len(h.changes) != 1 {
		t.Errorf("changes = %+v, want only the initial one", h.changes)
	}
}

func TestWatchReportsAddedOnTickAndRemovedOnTrigger(t *testing.T) {
	h := startWatch(t,
		step{svcs: []Service{pg(412, 5432)}},
		step{svcs: []Service{pg(412, 5432), vk(500)}},
		step{svcs: []Service{vk(500)}},
	)
	h.tick()
	h.trigger()
	h.stop()
	if len(h.changes) != 3 {
		t.Fatalf("changes = %+v, want 3", h.changes)
	}
	if !sameChanges(h.changes[1], Changes{Added: []Service{vk(500)}}) {
		t.Errorf("tick change = %+v, want valkey added", h.changes[1])
	}
	if !sameChanges(h.changes[2], Changes{Removed: []Service{pg(412, 5432)}}) {
		t.Errorf("trigger change = %+v, want postgres removed", h.changes[2])
	}
}

func TestWatchKeepsListWhenDiscoverFails(t *testing.T) {
	h := startWatch(t,
		step{svcs: []Service{pg(412, 5432), vk(500)}},
		step{err: errors.New("proc unreadable")},
		step{svcs: []Service{pg(412, 5432), vk(500), kf(300)}},
	)
	h.tick() // fails
	h.tick() // succeeds: diffed against the list from before the failure
	h.stop()
	if len(h.changes) != 2 {
		t.Fatalf("changes = %+v, want initial + one (no mass removal on failure)", h.changes)
	}
	if !sameChanges(h.changes[1], Changes{Added: []Service{kf(300)}}) {
		t.Errorf("change after recovery = %+v, want only kafka added", h.changes[1])
	}
	if len(h.logs) != 1 {
		t.Errorf("logs = %q, want one line for the failure", h.logs)
	}
}

func TestWatchFirstDiscoverFails(t *testing.T) {
	h := startWatch(t, step{err: errors.New("boom")}, step{svcs: []Service{pg(412, 5432)}})
	h.tick()
	h.stop()
	if len(h.changes) != 1 || !sameChanges(h.changes[0], Changes{Added: []Service{pg(412, 5432)}}) {
		t.Errorf("changes = %+v, want postgres added after recovery", h.changes)
	}
	if len(h.logs) != 1 {
		t.Errorf("logs = %q, want one line", h.logs)
	}
}

func TestWatchReturnsWhenTicksClosed(t *testing.T) {
	h := startWatch(t, step{svcs: nil})
	close(h.ticks)
	select {
	case <-h.done:
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return after ticks closed")
	}
}

func kafkaAt(port int) Service { return Service{Kind: "kafka", PID: 49061, Name: "java", Port: port} }

func TestWatchRetriesSoonWhenPortMissing(t *testing.T) {
	h := startWatch(t, step{svcs: []Service{kafkaAt(0)}}, step{svcs: []Service{kafkaAt(9092)}})
	retry := h.nextRetry()
	if retry == nil {
		t.Fatal("no retry scheduled for a service without a port")
	}
	h.setNow(2 * time.Second)
	retry <- time.Now() // no tick: the retry alone runs the second round
	if h.nextRetry() != nil {
		t.Error("retry scheduled after the port appeared")
	}
	h.stop()
	if len(h.changes) != 2 || len(h.changes[1].Updated) != 1 || h.changes[1].Updated[0].Port != 9092 {
		t.Errorf("changes = %+v, want added, then updated to port 9092", h.changes)
	}
}

func TestWatchStopsRetryingAfterWindow(t *testing.T) {
	// The initial round, 15 retries (2s, 4s, ... 30s), then one tick as a barrier.
	steps := make([]step, 17)
	for i := range steps {
		steps[i] = step{svcs: []Service{kafkaAt(0)}}
	}
	h := startWatch(t, steps...)
	retries := 0
	for i := 1; ; i++ {
		retry := h.nextRetry()
		if retry == nil {
			break
		}
		retries++
		h.setNow(time.Duration(i) * 2 * time.Second)
		retry <- time.Now()
	}
	h.setNow(40 * time.Second)
	h.tick()
	h.stop()
	if retries != 15 {
		t.Errorf("retries = %d, want 15 (every 2s for 30s)", retries)
	}
	if n := h.pendingRetries(); n != 0 {
		t.Errorf("%d retries scheduled after the 30s window", n)
	}
}

func TestWatchNoRetryWhenPortsKnown(t *testing.T) {
	h := startWatch(t, step{svcs: []Service{kafkaAt(9092)}}, step{})
	h.tick() // barrier: the first round is done
	h.stop()
	if n := h.pendingRetries(); n != 0 {
		t.Errorf("%d retries scheduled with every port known", n)
	}
}

func TestWatchNewPortlessServiceStartsNewWindow(t *testing.T) {
	other := Service{Kind: "valkey", PID: 777, Name: "valkey-server"}
	h := startWatch(t,
		step{svcs: []Service{kafkaAt(0)}},        // t=0: window opens
		step{svcs: []Service{kafkaAt(0)}},        // t=40s: window over, no retry
		step{svcs: []Service{kafkaAt(0), other}}, // t=41s: a new port-less service, new window
		step{},                                   // barrier
	)
	if h.nextRetry() == nil {
		t.Fatal("no retry in the first window")
	}
	h.setNow(40 * time.Second)
	h.tick()
	h.setNow(41 * time.Second)
	h.tick()
	h.setNow(42 * time.Second)
	h.tick()
	h.stop()
	if n := h.pendingRetries(); n != 1 {
		t.Errorf("retries scheduled after the first window = %d, want 1 (for the new service)", n)
	}
}

func TestWatchRetriesSoonWhenOnlyOtherPortOpen(t *testing.T) {
	// Kafka opens its controller port 9093 before its client port 9092.
	h := startWatch(t, step{svcs: []Service{kafkaAt(9093)}}, step{svcs: []Service{kafkaAt(9092)}})
	retry := h.nextRetry()
	if retry == nil {
		t.Fatal("no retry scheduled for a service not yet on its usual port")
	}
	retry <- time.Now()
	if h.nextRetry() != nil {
		t.Error("retry scheduled once the usual port was open")
	}
	h.stop()
	if len(h.changes) != 2 || len(h.changes[1].Updated) != 1 || h.changes[1].Updated[0].Port != 9092 {
		t.Errorf("changes = %+v, want added on 9093, then updated to 9092", h.changes)
	}
}

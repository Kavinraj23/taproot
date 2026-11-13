package discovery

import (
	"context"
	"errors"
	"fmt"
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
}

func startWatch(t *testing.T, steps ...step) *watchHarness {
	t.Helper()
	h := &watchHarness{
		t:        t,
		ticks:    make(chan time.Time),
		triggers: make(chan struct{}),
		done:     make(chan struct{}),
		steps:    steps,
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

package rate

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)

func at(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }

func TestTracker(t *testing.T) {
	var tr Tracker

	if _, ok := tr.Next("a", 100, at(0)); ok {
		t.Error("first reading: ok = true, want false")
	}
	if got, ok := tr.Next("a", 141, at(1)); !ok || got != 41 {
		t.Errorf("100→141 over 1s = %v, %v; want 41, true", got, ok)
	}
	if got, ok := tr.Next("a", 191, at(3)); !ok || got != 25 {
		t.Errorf("141→191 over 2s = %v, %v; want 25, true", got, ok)
	}
	if _, ok := tr.Next("a", 191, at(3)); ok {
		t.Error("same timestamp: ok = true, want false")
	}
}

func TestTrackerReset(t *testing.T) {
	var tr Tracker
	tr.Next("a", 500, at(0))
	if _, ok := tr.Next("a", 20, at(1)); ok {
		t.Error("counter went down: ok = true, want false (reset)")
	}
	if got, ok := tr.Next("a", 30, at(2)); !ok || got != 10 {
		t.Errorf("after reset 20→30 = %v, %v; want 10, true", got, ok)
	}
}

func TestTrackerKeysIndependent(t *testing.T) {
	var tr Tracker
	tr.Next("a", 100, at(0))
	tr.Next("b", 5000, at(0))
	if got, _ := tr.Next("a", 110, at(1)); got != 10 {
		t.Errorf("a = %v, want 10", got)
	}
	if got, _ := tr.Next("b", 5500, at(1)); got != 500 {
		t.Errorf("b = %v, want 500", got)
	}
}

func TestTrackerForget(t *testing.T) {
	var tr Tracker
	tr.Next("a", 100, at(0))
	tr.Forget("a")
	if _, ok := tr.Next("a", 200, at(1)); ok {
		t.Error("after Forget: ok = true, want false (first reading again)")
	}
}

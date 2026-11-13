package discovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"taproot/internal/docker"
)

const settle = 20 * time.Millisecond

// countTriggers drains triggers until ctx ends and returns how many arrived.
func countTriggers(ctx context.Context, triggers <-chan struct{}) <-chan int {
	out := make(chan int, 1)
	go func() {
		n := 0
		for {
			select {
			case <-triggers:
				n++
			case <-ctx.Done():
				out <- n
				return
			}
		}
	}()
	return out
}

// runRing runs Ring with the given events func for d, then returns triggers counted and logs.
func runRing(t *testing.T, d time.Duration, events func(context.Context, func(docker.Event)) error) (int, []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	triggers := make(chan struct{}, 1)
	counted := countTriggers(ctx, triggers)

	var mu sync.Mutex
	var logs []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		Ring(ctx, events, triggers, settle, func(f string, a ...any) {
			mu.Lock()
			logs = append(logs, fmt.Sprintf(f, a...))
			mu.Unlock()
		}, time.Millisecond)
	}()
	n := <-counted
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Ring did not return after cancel")
	}
	mu.Lock()
	defer mu.Unlock()
	return n, logs
}

// blockUntilDone keeps the stream "open" like a healthy Docker connection.
func blockUntilDone(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }

func TestRingDebouncesBurst(t *testing.T) {
	n, _ := runRing(t, 300*time.Millisecond, func(ctx context.Context, fn func(docker.Event)) error {
		for i := 0; i < 5; i++ {
			fn(docker.Event{Action: "start", ContainerID: fmt.Sprint(i)})
			time.Sleep(time.Millisecond)
		}
		return blockUntilDone(ctx)
	})
	if n != 1 {
		t.Errorf("triggers = %d, want 1 for a burst", n)
	}
}

func TestRingSeparateEvents(t *testing.T) {
	n, _ := runRing(t, 400*time.Millisecond, func(ctx context.Context, fn func(docker.Event)) error {
		fn(docker.Event{Action: "start", ContainerID: "a"})
		time.Sleep(100 * time.Millisecond)
		fn(docker.Event{Action: "die", ContainerID: "b"})
		return blockUntilDone(ctx)
	})
	if n != 2 {
		t.Errorf("triggers = %d, want 2 for events far apart", n)
	}
}

func TestRingReconnectsAndLogsOnce(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	n, logs := runRing(t, 300*time.Millisecond, func(ctx context.Context, fn func(docker.Event)) error {
		mu.Lock()
		calls++
		c := calls
		mu.Unlock()
		if c <= 2 {
			return errors.New("socket missing")
		}
		fn(docker.Event{Action: "start", ContainerID: "a"})
		return blockUntilDone(ctx)
	})
	if n != 1 {
		t.Errorf("triggers = %d, want 1 after reconnecting", n)
	}
	if len(logs) != 2 {
		t.Errorf("logs = %q, want 2 (first failure, recovery)", logs)
	}
}

func TestRingReturnsOnCancel(t *testing.T) {
	runRing(t, 30*time.Millisecond, func(ctx context.Context, fn func(docker.Event)) error {
		return blockUntilDone(ctx)
	}) // runRing fails the test if Ring doesn't return
}

package discovery

import (
	"context"
	"time"

	"taproot/internal/docker"
)

// maxBackoff caps the wait between reconnects to the Docker event stream.
const maxBackoff = 60 * time.Second

// Ring turns Docker container events into rescan triggers. It waits until no event has arrived
// for `settle` before sending, so a burst (the demo starting 20 containers) becomes one rescan,
// and the program in a just-started container gets a moment to open its port. Sends never
// block: if a rescan is already queued, it covers this one too.
//
// When the event stream fails, Ring reconnects, waiting `backoff` at first and doubling up to
// maxBackoff. It logs the first failure and the recovery, not every retry; the periodic rescan
// in Watch covers the gap. It returns when ctx is done.
func Ring(ctx context.Context, events func(context.Context, func(docker.Event)) error,
	triggers chan<- struct{}, settle time.Duration, logf func(string, ...any), backoff time.Duration) {

	rang := make(chan struct{}, 1)
	go debounce(ctx, rang, triggers, settle)

	wait := backoff
	failing := false
	for ctx.Err() == nil {
		err := events(ctx, func(docker.Event) {
			if failing {
				logf("docker events back")
				failing = false
			}
			wait = backoff
			select {
			case rang <- struct{}{}:
			default:
			}
		})
		if ctx.Err() != nil {
			return
		}
		if !failing {
			logf("docker events unavailable, relying on periodic rescans: %v", err)
			failing = true
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(wait*2, maxBackoff)
	}
}

// debounce sends one trigger once `settle` has passed with no new ring.
func debounce(ctx context.Context, rang <-chan struct{}, triggers chan<- struct{}, settle time.Duration) {
	var timer <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-rang:
			timer = time.After(settle) // restart the quiet period
		case <-timer:
			timer = nil
			select {
			case triggers <- struct{}{}:
			default:
			}
		}
	}
}

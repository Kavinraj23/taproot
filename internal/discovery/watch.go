package discovery

import (
	"context"
	"time"
)

// WatchConfig holds everything Watch depends on, so tests can drive it.
type WatchConfig struct {
	Discover func(ctx context.Context) ([]Service, error)
	Ticks    <-chan time.Time // backstop rounds
	Triggers <-chan struct{}  // early rounds, e.g. a container started (see Ring)
	OnChange func(Changes)
	Logf     func(format string, args ...any)
}

// Watch discovers once straight away, then again on every tick or trigger, and calls OnChange
// whenever the service list differs from the last good round. A failed round is logged and
// otherwise ignored: the last good list stays current, so a hiccup never reads as every service
// disappearing. It returns when ctx is done or Ticks is closed.
func Watch(ctx context.Context, cfg WatchConfig) {
	var current []Service
	round := func() {
		svcs, err := cfg.Discover(ctx)
		if err != nil {
			cfg.Logf("discovery round failed, keeping last known services: %v", err)
			return
		}
		if c := Diff(current, svcs); !c.Empty() {
			cfg.OnChange(c)
		}
		current = svcs
	}

	round()
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case _, open := <-cfg.Ticks:
			if !open {
				return
			}
			round()
		case <-cfg.Triggers:
			round()
		}
	}
}

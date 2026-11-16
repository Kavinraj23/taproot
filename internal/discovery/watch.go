package discovery

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"
)

// WatchConfig holds everything Watch depends on, so tests can drive it.
type WatchConfig struct {
	Discover func(ctx context.Context) ([]Service, error)
	Ticks    <-chan time.Time // backstop rounds
	Triggers <-chan struct{}  // early rounds, e.g. a container started (see Ring)
	OnChange func(Changes)
	Logf     func(format string, args ...any)

	// A service found while still starting may have no port yet, or only a secondary one (see
	// Settled). While any service is like that, Watch looks again every RetryEvery, for at most
	// RetryFor from when that set of unsettled services appeared. Defaults 2s and 30s.
	RetryEvery, RetryFor time.Duration
	After                func(time.Duration) <-chan time.Time // default time.After
	Now                  func() time.Time                     // default time.Now
}

// Watch discovers once straight away, then again on every tick, trigger or port retry, and calls
// OnChange whenever the service list differs from the last good round. A failed round is logged
// and otherwise ignored: the last good list stays current, so a hiccup never reads as every
// service disappearing. It returns when ctx is done or Ticks is closed.
func Watch(ctx context.Context, cfg WatchConfig) {
	if cfg.RetryEvery <= 0 {
		cfg.RetryEvery = 2 * time.Second
	}
	if cfg.RetryFor <= 0 {
		cfg.RetryFor = 30 * time.Second
	}
	if cfg.After == nil {
		cfg.After = time.After
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	var current []Service
	var retry <-chan time.Time // nil (never fires) unless a retry is due
	unsettled, since := "", time.Time{}

	round := func() {
		retry = nil
		svcs, err := cfg.Discover(ctx)
		if err != nil {
			cfg.Logf("discovery round failed, keeping last known services: %v", err)
			return
		}
		if c := Diff(current, svcs); !c.Empty() {
			cfg.OnChange(c)
		}
		current = svcs

		key := unsettledKey(svcs)
		if key == "" {
			unsettled = ""
			return
		}
		if key != unsettled { // a new set of unsettled services opens a new window
			unsettled, since = key, cfg.Now()
		}
		if cfg.Now().Sub(since) < cfg.RetryFor {
			retry = cfg.After(cfg.RetryEvery)
		}
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
		case <-retry:
			round()
		}
	}
}

// unsettledKey names the services not yet on their usual port, "" if there are none.
func unsettledKey(svcs []Service) string {
	var ks []string
	for _, s := range svcs {
		if !Settled(s) {
			ks = append(ks, fmt.Sprintf("%s/%d", s.Kind, s.PID))
		}
	}
	slices.Sort(ks)
	return strings.Join(ks, ",")
}

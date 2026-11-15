package sensor

import (
	"context"
	"fmt"
	"maps"
	"net"
	"strconv"
	"sync"
	"time"

	"taproot/internal/discovery"
	"taproot/internal/model"
	"taproot/internal/rate"
)

// ManagerConfig holds everything a Manager depends on, so tests can swap in fakes.
type ManagerConfig struct {
	Factories map[string]Factory // by service kind; kinds without one are ignored
	Emit      func(model.Point)
	Now       func() time.Time
	Host      string
	Interval  time.Duration // between collections; default 1s
	// NewTicker returns a tick channel and its stop func; default wraps time.NewTicker.
	NewTicker func(time.Duration) (<-chan time.Time, func())
	Logf      func(format string, args ...any)
}

// Manager runs one collection loop per service instance.
type Manager struct {
	cfg ManagerConfig

	mu        sync.Mutex
	instances map[instanceKey]*instance
}

type instanceKey struct {
	kind string
	pid  int
}

type instance struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// NewManager returns a Manager with no instances running.
func NewManager(cfg ManagerConfig) *Manager {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.NewTicker == nil {
		cfg.NewTicker = func(d time.Duration) (<-chan time.Time, func()) {
			t := time.NewTicker(d)
			return t.C, t.Stop
		}
	}
	return &Manager{cfg: cfg, instances: map[instanceKey]*instance{}}
}

// Apply starts collectors for added services, stops them for removed ones, and restarts them
// for updated ones (the port or container may have changed). Stopping waits for the loop to end.
func (m *Manager) Apply(c discovery.Changes) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range c.Removed {
		m.stopLocked(key(s))
	}
	for _, s := range c.Updated {
		m.stopLocked(key(s))
		m.startLocked(s)
	}
	for _, s := range c.Added {
		m.startLocked(s)
	}
}

// Stop ends every collection loop and closes its collector.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.instances {
		m.stopLocked(k)
	}
}

func key(s discovery.Service) instanceKey { return instanceKey{s.Kind, s.PID} }

func instanceName(s discovery.Service) string {
	if s.Container != "" {
		return s.Container
	}
	return "pid-" + strconv.Itoa(s.PID)
}

func (m *Manager) startLocked(s discovery.Service) {
	factory, ok := m.cfg.Factories[s.Kind]
	if !ok {
		return
	}
	// Reached by container name over Docker's DNS, so both are needed (decision 0005).
	if s.Port == 0 || s.Container == "" {
		m.cfg.Logf("%s %s: not measuring yet, need a container name and port (have %q, %d)",
			s.Kind, instanceName(s), s.Container, s.Port)
		return
	}
	coll, err := factory(s, net.JoinHostPort(s.Container, strconv.Itoa(s.Port)))
	if err != nil {
		m.cfg.Logf("%s %s: can't create sensor: %v", s.Kind, instanceName(s), err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	inst := &instance{cancel: cancel, done: make(chan struct{})}
	m.instances[key(s)] = inst
	// The ticker exists before Apply returns, so the instance is fully started by then.
	ticks, stopTicks := m.cfg.NewTicker(m.cfg.Interval)
	go func() {
		defer close(inst.done)
		defer coll.Close()
		defer stopTicks()
		m.run(ctx, s, coll, ticks)
	}()
}

func (m *Manager) stopLocked(k instanceKey) {
	inst, ok := m.instances[k]
	if !ok {
		return
	}
	inst.cancel()
	<-inst.done
	delete(m.instances, k)
}

// run collects on every tick until ctx ends.
func (m *Manager) run(ctx context.Context, s discovery.Service, coll Collector, ticks <-chan time.Time) {
	base := map[string]string{"host": m.cfg.Host, "service": s.Kind, "instance": instanceName(s)}
	var rates rate.Tracker
	failing := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
		}

		// Timestamp at read time, not send time, so a resent point keeps its identity.
		at := m.cfg.Now().UTC()
		cctx, cancel := context.WithTimeout(ctx, m.cfg.Interval*9/10)
		readings, err := coll.Collect(cctx)
		cancel()
		if err != nil && ctx.Err() != nil {
			return // stopped mid-collection: the error is ours, not the service being down
		}

		up := 1.0
		if err != nil {
			up = 0
			if !failing {
				m.cfg.Logf("%s %s: measuring failed, reporting it as down: %v", s.Kind, base["instance"], err)
			}
			failing = true
		} else if failing {
			m.cfg.Logf("%s %s: measuring again", s.Kind, base["instance"])
			failing = false
		}
		m.cfg.Emit(model.Point{Metric: s.Kind + "_up", Labels: maps.Clone(base), Timestamp: at, Value: up})
		if err != nil {
			continue
		}

		for _, r := range readings {
			labels := maps.Clone(base)
			maps.Copy(labels, r.Labels)
			metric, value := r.Metric, r.Value
			if r.Kind == Counter {
				perSec, ok := rates.Next(seriesKey(r), r.Value, at)
				if !ok {
					continue
				}
				metric, value = r.Metric+"_per_sec", perSec
			}
			m.cfg.Emit(model.Point{Metric: metric, Labels: labels, Timestamp: at, Value: value})
		}
	}
}

// seriesKey tells counters with the same metric but different labels apart.
func seriesKey(r Reading) string {
	if len(r.Labels) == 0 {
		return r.Metric
	}
	return fmt.Sprintf("%s%v", r.Metric, r.Labels) // fmt prints maps with sorted keys
}

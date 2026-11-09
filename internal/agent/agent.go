// Package agent runs the collection loop: read, compute, emit.
package agent

import (
	"context"
	"time"

	"taproot/internal/cpu"
	"taproot/internal/model"
)

const cpuMetric = "host_cpu_usage_percent"

// Config holds everything Run depends on, so tests can swap in fakes.
type Config struct {
	Read func() (cpu.Times, error)
	Now  func() time.Time
	Host string
	Emit func(model.Point)
	Logf func(format string, args ...any)
}

// Run takes one reading straight away as the baseline, then one more per tick, emitting a CPU
// usage point whenever there is a percentage to report. A failed read is logged and skipped; the
// last good reading stays the baseline. It returns when ctx is done or ticks is closed.
func Run(ctx context.Context, ticks <-chan time.Time, cfg Config) {
	var sampler cpu.Sampler

	sample := func() {
		times, err := cfg.Read()
		if err != nil {
			cfg.Logf("read cpu counters: %v", err)
			return
		}
		// The timestamp is taken at read time, not send time, so a resent point keeps its identity.
		at := cfg.Now().UTC()
		pct, ok := sampler.Next(times)
		if !ok {
			return
		}
		cfg.Emit(model.Point{
			Metric:    cpuMetric,
			Labels:    map[string]string{"host": cfg.Host},
			Timestamp: at,
			Value:     pct,
		})
	}

	sample()
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case _, open := <-ticks:
			if !open {
				return
			}
			sample()
		}
	}
}

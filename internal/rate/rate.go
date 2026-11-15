// Package rate turns cumulative counters (totals that only go up) into per-second rates.
package rate

import "time"

type sample struct {
	value float64
	at    time.Time
}

// Tracker remembers the previous reading of each series. The zero value is ready to use. It is
// not safe for concurrent use.
type Tracker struct {
	prev map[string]sample
}

// Next records value for key and returns the rate since the previous reading. ok is false when
// there is nothing to report: the first reading of a key, a counter that went down (the process
// restarted; value becomes the new baseline), or no time passing. The same rule as cpu.Usage.
func (t *Tracker) Next(key string, value float64, at time.Time) (perSec float64, ok bool) {
	if t.prev == nil {
		t.prev = map[string]sample{}
	}
	p, seen := t.prev[key]
	t.prev[key] = sample{value, at}
	if !seen || value < p.value {
		return 0, false
	}
	dt := at.Sub(p.at).Seconds()
	if dt <= 0 {
		return 0, false
	}
	return (value - p.value) / dt, true
}

// Forget drops key's previous reading, so its next reading is treated as the first.
func (t *Tracker) Forget(key string) { delete(t.prev, key) }

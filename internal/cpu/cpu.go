// Package cpu turns readings of /proc/stat into a host CPU usage percentage.
package cpu

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Times holds the cumulative CPU time counters (in clock ticks since boot) from the
// aggregate "cpu" line of /proc/stat. The guest and guest_nice columns are left out:
// the kernel already counts them inside User and Nice.
type Times struct {
	User, Nice, System, Idle, IOWait, IRQ, SoftIRQ, Steal uint64
}

// minFields is how many counters a line must have; columns after Idle are missing on old kernels.
const minFields = 4

// Parse reads the aggregate "cpu" line from the text of /proc/stat. Per-core lines (cpu0, cpu1, ...)
// are ignored.
func Parse(statText string) (Times, error) {
	for _, line := range strings.Split(statText, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "cpu" {
			continue
		}
		return parseFields(fields[1:])
	}
	return Times{}, errors.New("no aggregate cpu line found")
}

func parseFields(fields []string) (Times, error) {
	if len(fields) < minFields {
		return Times{}, fmt.Errorf("cpu line has %d counters, need at least %d", len(fields), minFields)
	}
	var t Times
	dst := []*uint64{&t.User, &t.Nice, &t.System, &t.Idle, &t.IOWait, &t.IRQ, &t.SoftIRQ, &t.Steal}
	for i := 0; i < len(dst) && i < len(fields); i++ {
		v, err := strconv.ParseUint(fields[i], 10, 64)
		if err != nil {
			return Times{}, fmt.Errorf("cpu counter %d: %w", i+1, err)
		}
		*dst[i] = v
	}
	return t, nil
}

// Read parses the /proc/stat-format file at path.
func Read(path string) (Times, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Times{}, err
	}
	return Parse(string(b))
}

// Usage returns the CPU busy percentage (0-100) between two readings. ok is false when there is
// nothing to report: a counter went down (a reset, so the caller should keep cur as the new
// baseline) or no time passed between the readings. Time waiting on I/O counts as idle.
func Usage(prev, cur Times) (pct float64, ok bool) {
	if cur.User < prev.User || cur.Nice < prev.Nice || cur.System < prev.System ||
		cur.Idle < prev.Idle || cur.IOWait < prev.IOWait || cur.IRQ < prev.IRQ ||
		cur.SoftIRQ < prev.SoftIRQ || cur.Steal < prev.Steal {
		return 0, false
	}
	// Take deltas before summing so large absolute counters can't overflow the total.
	idle := (cur.Idle - prev.Idle) + (cur.IOWait - prev.IOWait)
	busy := (cur.User - prev.User) + (cur.Nice - prev.Nice) + (cur.System - prev.System) +
		(cur.IRQ - prev.IRQ) + (cur.SoftIRQ - prev.SoftIRQ) + (cur.Steal - prev.Steal)
	total := idle + busy
	if total == 0 {
		return 0, false
	}
	return float64(busy) / float64(total) * 100, true
}

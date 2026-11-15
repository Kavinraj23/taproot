package sensor

import "testing"

func byMetric(rs []Reading) map[string][]Reading {
	out := map[string][]Reading{}
	for _, r := range rs {
		out[r.Metric] = append(out[r.Metric], r)
	}
	return out
}

func TestPgReadings(t *testing.T) {
	s := pgStats{
		connsByState:    map[string]float64{"active": 3, "idle": 7},
		maxConns:        100,
		xactCommit:      41203,
		xactRollback:    12,
		blocksHit:       900000,
		blocksRead:      3000,
		deadlocks:       1,
		locksWaiting:    2,
		longestQuerySec: 1.5,
	}
	got := byMetric(pgReadings(s))

	conns := got["postgres_connections"]
	if len(conns) != 2 {
		t.Fatalf("postgres_connections = %+v, want 2 (one per state)", conns)
	}
	states := map[string]float64{}
	for _, r := range conns {
		if r.Kind != Gauge {
			t.Errorf("connections kind = %v, want Gauge", r.Kind)
		}
		states[r.Labels["state"]] = r.Value
	}
	if states["active"] != 3 || states["idle"] != 7 {
		t.Errorf("connections by state = %v, want active 3, idle 7", states)
	}

	want := []struct {
		metric string
		kind   Kind
		value  float64
	}{
		{"postgres_connections_max", Gauge, 100},
		{"postgres_xact_commit", Counter, 41203},
		{"postgres_xact_rollback", Counter, 12},
		{"postgres_blocks_hit", Counter, 900000},
		{"postgres_blocks_read", Counter, 3000},
		{"postgres_deadlocks", Counter, 1},
		{"postgres_locks_waiting", Gauge, 2},
		{"postgres_longest_query_seconds", Gauge, 1.5},
	}
	for _, w := range want {
		rs := got[w.metric]
		if len(rs) != 1 || rs[0].Kind != w.kind || rs[0].Value != w.value {
			t.Errorf("%s = %+v, want one %v of %v", w.metric, rs, w.kind, w.value)
		}
	}
}

func TestPgReadingsNoActiveQueries(t *testing.T) {
	got := byMetric(pgReadings(pgStats{connsByState: map[string]float64{}}))
	if rs := got["postgres_longest_query_seconds"]; len(rs) != 1 || rs[0].Value != 0 {
		t.Errorf("longest query = %+v, want 0", rs)
	}
}

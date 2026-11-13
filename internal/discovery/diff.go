package discovery

import (
	"reflect"
	"slices"
)

// Changes is what differs between two discovery rounds.
type Changes struct {
	Added   []Service // new instances
	Removed []Service // instances that are gone
	Updated []Service // same instance, new details (e.g. its port appeared), in their new form
}

// Empty reports whether nothing changed.
func (c Changes) Empty() bool { return len(c.Added)+len(c.Removed)+len(c.Updated) == 0 }

type serviceKey struct {
	kind string
	pid  int
}

// Diff compares two rounds. An instance is identified by kind and PID, so a restarted container
// (new process, new PID) shows up as one removed and one added. Each list is sorted by PID.
func Diff(prev, cur []Service) Changes {
	before := make(map[serviceKey]Service, len(prev))
	for _, s := range prev {
		before[serviceKey{s.Kind, s.PID}] = s
	}
	var c Changes
	seen := make(map[serviceKey]bool, len(cur))
	for _, s := range cur {
		k := serviceKey{s.Kind, s.PID}
		seen[k] = true
		old, ok := before[k]
		switch {
		case !ok:
			c.Added = append(c.Added, s)
		case !reflect.DeepEqual(old, s):
			c.Updated = append(c.Updated, s)
		}
	}
	for _, s := range prev {
		if !seen[serviceKey{s.Kind, s.PID}] {
			c.Removed = append(c.Removed, s)
		}
	}
	byPID := func(a, b Service) int { return a.PID - b.PID }
	slices.SortFunc(c.Added, byPID)
	slices.SortFunc(c.Removed, byPID)
	slices.SortFunc(c.Updated, byPID)
	return c
}

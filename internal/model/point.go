package model

import (
	"sort"
	"strconv"
	"strings"
	"time"
)

// Point is one metric measurement. Metric, Labels and Timestamp together identify it,
// so a resent copy of the same point can be recognised as a duplicate.
type Point struct {
	Metric    string            `json:"metric"`
	Labels    map[string]string `json:"labels"`
	Timestamp time.Time         `json:"ts"`
	Value     float64           `json:"value"`
}

// Key is the point's identity for dedup: metric, labels sorted by name, and the timestamp in
// Unix nanoseconds, e.g. "host_cpu_usage_percent"{"host"="h1"}@1763294400000000005.
// Everything is quoted, so a value such as `1,b=2` can't pass for two labels. The value is
// not part of the identity: a resent point is the same point.
func (p Point) Key() string {
	names := make([]string, 0, len(p.Labels))
	for n := range p.Labels {
		names = append(names, n)
	}
	sort.Strings(names)

	var b strings.Builder
	b.WriteString(strconv.Quote(p.Metric))
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Quote(n))
		b.WriteByte('=')
		b.WriteString(strconv.Quote(p.Labels[n]))
	}
	b.WriteString("}@")
	b.WriteString(strconv.FormatInt(p.Timestamp.UnixNano(), 10))
	return b.String()
}

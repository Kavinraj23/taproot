// Package losscheck answers "did every point the agent sent reach ClickHouse, exactly once?".
//
// It compares two lists, like checking a bank statement against your own spreadsheet:
// what Redpanda holds (what the agent delivered) against what ClickHouse stored. That
// comparison is exact and can't be fooled by the Docker VM's clock jumping forward.
// Losses before Redpanda (the agent dropping points) never reach Redpanda, so they are
// found separately, as long silences in series that report every tick (Gaps).
//
// This is verification code only. Its idea of "the same point" is private and is not
// the pipeline's dedup key.
package losscheck

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"taproot/internal/model"
)

// maxSample is how many missing points Compare names, so a big loss stays readable.
const maxSample = 5

// Stored is one distinct point in ClickHouse and how many copies of it are there.
type Stored struct {
	Metric string
	Labels map[string]string
	TS     time.Time
	Copies uint64
}

// Gap is a stretch of time in which Series series went silent. Holes in different series
// that overlap in time are one Gap, so one outage is one line.
type Gap struct {
	From, To time.Time
	Series   int
}

func (g Gap) String() string {
	return fmt.Sprintf("%d series silent %s -> %s (%v)", g.Series,
		g.From.Format("15:04:05.000"), g.To.Format("15:04:05.000"), g.To.Sub(g.From).Round(100*time.Millisecond))
}

// Report is the checker's verdict over one window.
type Report struct {
	From, To      time.Time
	Sent          int // distinct points Redpanda holds for the window
	Stored        int // distinct points ClickHouse holds for the window
	Missing       int
	Duplicates    int // extra copies beyond the first
	MissingSample []string
	Gaps          []Gap
}

// OK is true when nothing was lost, nothing was stored twice, and no heartbeat went silent.
func (r Report) OK() bool { return r.Missing == 0 && r.Duplicates == 0 && len(r.Gaps) == 0 }

// series names a series: metric plus labels sorted by name, e.g. cpu{host=a,zone=b}.
func series(metric string, labels map[string]string) string {
	names := make([]string, 0, len(labels))
	for n := range labels {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString(metric)
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n + "=" + labels[n])
	}
	b.WriteByte('}')
	return b.String()
}

// id is one point's identity for this check: its series plus its exact timestamp.
func id(metric string, labels map[string]string, ts time.Time) string {
	return series(metric, labels) + "@" + ts.UTC().Format(time.RFC3339Nano)
}

// Compare counts points Redpanda holds that ClickHouse lacks (missing) and extra stored
// copies (duplicates). A point sent twice counts once: a resend is not a storage duplicate.
// Stored points with no sent counterpart are ignored; they come from outside the read range.
func Compare(sent []model.Point, stored []Stored) (missing, dups int, sample []string) {
	have := make(map[string]bool, len(stored))
	for _, s := range stored {
		have[id(s.Metric, s.Labels, s.TS)] = true
		if s.Copies > 1 {
			dups += int(s.Copies - 1)
		}
	}
	seen := make(map[string]bool, len(sent))
	for _, p := range sent {
		k := id(p.Metric, p.Labels, p.Timestamp)
		if seen[k] {
			continue
		}
		seen[k] = true
		if !have[k] {
			missing++
			if len(sample) < maxSample {
				sample = append(sample, k)
			}
		}
	}
	sort.Strings(sample)
	return missing, dups, sample
}

// Gaps finds holes longer than min between consecutive points of each series, then merges
// holes that overlap in time. Input order doesn't matter; the result is sorted by From.
func Gaps(byseries map[string][]time.Time, min time.Duration) []Gap {
	type hole struct {
		from, to time.Time
		series   string
	}
	var holes []hole
	for name, ts := range byseries {
		s := append([]time.Time(nil), ts...)
		sort.Slice(s, func(i, j int) bool { return s[i].Before(s[j]) })
		for i := 1; i < len(s); i++ {
			if s[i].Sub(s[i-1]) > min {
				holes = append(holes, hole{s[i-1], s[i], name})
			}
		}
	}
	sort.Slice(holes, func(i, j int) bool { return holes[i].from.Before(holes[j].from) })

	var out []Gap
	var names map[string]bool
	for _, h := range holes {
		if n := len(out); n > 0 && !h.from.After(out[n-1].To) {
			if h.to.After(out[n-1].To) {
				out[n-1].To = h.to
			}
			names[h.series] = true
			out[n-1].Series = len(names)
			continue
		}
		names = map[string]bool{h.series: true}
		out = append(out, Gap{From: h.from, To: h.to, Series: 1})
	}
	return out
}

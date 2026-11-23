// Package trace turns one trace's flat list of spans into the tree of who called whom, and
// prints it. Pure: the spans come from store.Trace (design section 5, assemble on read).
package trace

import (
	"fmt"
	"io"
	"sort"
	"time"

	"taproot/internal/model"
)

// Node is one span with the spans it called. Orphan means its parent isn't in the trace: a
// parent dropped upstream, one that hasn't arrived yet, or a corrupt parent ID.
type Node struct {
	Span     model.Span
	Children []*Node
	Orphan   bool
}

// Build hangs each span under its parent. Spans without a parent are roots; spans whose
// parent isn't there become roots marked Orphan, keeping their own children. Siblings and
// roots are ordered by start, ties by span ID. Every span appears exactly once, even if
// corrupt parent IDs form a loop.
func Build(spans []model.Span) []*Node {
	sorted := append([]model.Span(nil), spans...)
	sort.SliceStable(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		if !a.Start.Equal(b.Start) {
			return a.Start.Before(b.Start)
		}
		return a.SpanID < b.SpanID
	})

	nodes := make([]*Node, len(sorted))
	known := make(map[string]bool, len(sorted))
	children := map[string][]*Node{} // parent span ID -> its spans, already in order
	for i, sp := range sorted {
		nodes[i] = &Node{Span: sp}
		known[sp.SpanID] = true
	}
	for _, n := range nodes {
		if p := n.Span.ParentSpanID; p != "" {
			children[p] = append(children[p], n)
		}
	}

	placed := make(map[*Node]bool, len(nodes))
	var attach func(n *Node)
	attach = func(n *Node) {
		placed[n] = true
		for _, c := range children[n.Span.SpanID] {
			if placed[c] {
				continue // a loop back to a span already in the tree
			}
			n.Children = append(n.Children, c)
			attach(c)
		}
	}

	var roots []*Node
	for _, n := range nodes {
		p := n.Span.ParentSpanID
		if p == "" || !known[p] {
			n.Orphan = p != ""
			roots = append(roots, n)
			attach(n)
		}
	}
	// Spans not reached from any root sit on a parent loop: start each loop at its earliest span.
	for _, n := range nodes {
		if !placed[n] {
			n.Orphan = true
			roots = append(roots, n)
			attach(n)
		}
	}
	sort.SliceStable(roots, func(i, j int) bool {
		a, b := roots[i].Span, roots[j].Span
		if !a.Start.Equal(b.Start) {
			return a.Start.Before(b.Start)
		}
		return a.SpanID < b.SpanID
	})
	return roots
}

// Render prints a header (span count, services, the trace's whole duration) and one line per
// span: offset from the trace's first start, duration, tree branches, service, name, and
// markers for errors, messaging spans and orphans.
func Render(w io.Writer, traceID string, spans []model.Span, roots []*Node) {
	var first, last time.Time
	services := map[string]bool{}
	for i, sp := range spans {
		if i == 0 || sp.Start.Before(first) {
			first = sp.Start
		}
		if i == 0 || sp.End().After(last) {
			last = sp.End()
		}
		services[sp.Service] = true
	}
	fmt.Fprintf(w, "trace %s  %s, %s, %s\n", traceID,
		plural(len(spans), "span"), plural(len(services), "service"), human(last.Sub(first)))

	var line func(n *Node, prefix, branch, childPrefix string)
	line = func(n *Node, prefix, branch, childPrefix string) {
		sp := n.Span
		marks := ""
		if sp.Status == "error" {
			marks += "  ERROR"
		}
		if sp.Kind == "producer" || sp.Kind == "consumer" {
			marks += "  (" + sp.Kind + ")"
		}
		if n.Orphan {
			marks += "  (parent not stored)"
		}
		fmt.Fprintf(w, "%7s %7s  %s%s%s  %s%s\n",
			"+"+human(sp.Start.Sub(first)), human(sp.Duration), prefix, branch, sp.Service, sp.Name, marks)
		for i, c := range n.Children {
			if i == len(n.Children)-1 {
				line(c, prefix+childPrefix, "└─ ", "   ")
			} else {
				line(c, prefix+childPrefix, "├─ ", "│  ")
			}
		}
	}
	for _, r := range roots {
		line(r, "", "", "")
	}
}

// human rounds a duration for reading: 0ms, 2.5ms, 432ms, 1.5s.
func human(d time.Duration) string {
	switch {
	case d == 0:
		return "0ms"
	case d < 10*time.Millisecond:
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Round(time.Millisecond)/time.Millisecond)
	default:
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
}

func plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return fmt.Sprintf("%d %ss", n, word)
}

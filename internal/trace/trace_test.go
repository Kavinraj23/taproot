package trace

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"taproot/internal/model"
)

const tid = "4bf92f3577b34da6a3ce929d0e0e4736"

var t0 = time.Date(2026, 11, 20, 6, 27, 53, 123456789, time.UTC)

func sp(id, parent, service, name string, at, dur time.Duration) model.Span {
	return model.Span{TraceID: tid, SpanID: id, ParentSpanID: parent, Service: service, Name: name,
		Kind: "server", Start: t0.Add(at), Duration: dur, Status: "ok"}
}

// checkoutTrace is one order: frontend calls checkout, which charges payment (and fails) and
// publishes the order to Kafka. Shuffled, as rows can come back in any order.
func checkoutTrace() []model.Span {
	pay := sp("aaaaaaaaaaaaaaa3", "aaaaaaaaaaaaaaa2", "payment", "Charge", 20*time.Millisecond, 2500*time.Microsecond)
	pay.Status = "error"
	pub := sp("aaaaaaaaaaaaaaa4", "aaaaaaaaaaaaaaa2", "checkout", "orders publish", 400*time.Millisecond, 3*time.Millisecond)
	pub.Kind = "producer"
	return []model.Span{
		pub,
		sp("aaaaaaaaaaaaaaa2", "aaaaaaaaaaaaaaa1", "checkout", "PlaceOrder", 10*time.Millisecond, 280*time.Millisecond),
		pay,
		sp("aaaaaaaaaaaaaaa1", "", "frontend", "POST /api/checkout", 0, 310*time.Millisecond),
	}
}

// ids flattens the forest depth-first as "id@depth", marking orphans with "!".
func ids(roots []*Node) []string {
	var out []string
	var walk func(n *Node, depth int)
	walk = func(n *Node, depth int) {
		s := n.Span.SpanID[len(n.Span.SpanID)-1:] + "@" + string(rune('0'+depth))
		if n.Orphan {
			s += "!"
		}
		out = append(out, s)
		for _, c := range n.Children {
			walk(c, depth+1)
		}
	}
	for _, r := range roots {
		walk(r, 0)
	}
	return out
}

func eq(t *testing.T, got []string, want string) {
	t.Helper()
	if g := strings.Join(got, " "); g != want {
		t.Errorf("tree = %s\nwant   %s", g, want)
	}
}

// 1. A chain hangs each span under its parent, whatever order the rows came in.
func TestBuildChain(t *testing.T) {
	eq(t, ids(Build(checkoutTrace())), "1@0 2@1 3@2 4@2")
}

// 2. Siblings are ordered by start, ties broken by span ID, so output is stable.
func TestBuildSiblingOrder(t *testing.T) {
	spans := []model.Span{
		sp("aaaaaaaaaaaaaaa1", "", "frontend", "root", 0, time.Second),
		sp("aaaaaaaaaaaaaaa4", "aaaaaaaaaaaaaaa1", "c", "late", 30*time.Millisecond, time.Millisecond),
		sp("aaaaaaaaaaaaaaa3", "aaaaaaaaaaaaaaa1", "c", "tie-b", 10*time.Millisecond, time.Millisecond),
		sp("aaaaaaaaaaaaaaa2", "aaaaaaaaaaaaaaa1", "c", "tie-a", 10*time.Millisecond, time.Millisecond),
	}
	eq(t, ids(Build(spans)), "1@0 2@1 3@1 4@1")
}

// 3. A span whose parent isn't stored becomes a root marked orphan, keeping its own children.
func TestBuildOrphan(t *testing.T) {
	spans := []model.Span{
		sp("aaaaaaaaaaaaaaa1", "", "frontend", "root", 0, time.Second),
		sp("aaaaaaaaaaaaaaa2", "ffffffffffffffff", "checkout", "lost parent", 5*time.Millisecond, time.Millisecond),
		sp("aaaaaaaaaaaaaaa3", "aaaaaaaaaaaaaaa2", "payment", "child", 6*time.Millisecond, time.Millisecond),
	}
	eq(t, ids(Build(spans)), "1@0 2@0! 3@1")
}

// 4. Two spans without a parent are two roots, by start.
func TestBuildTwoRoots(t *testing.T) {
	spans := []model.Span{
		sp("aaaaaaaaaaaaaaa2", "", "fraud-detection", "process orders", 50*time.Millisecond, time.Millisecond),
		sp("aaaaaaaaaaaaaaa1", "", "fraud-detection", "receive orders", 0, time.Millisecond),
	}
	eq(t, ids(Build(spans)), "1@0 2@0")
}

// 5. Corrupt parents that form a loop don't hang, and every span still appears exactly once.
func TestBuildCycle(t *testing.T) {
	spans := []model.Span{
		sp("aaaaaaaaaaaaaaa1", "aaaaaaaaaaaaaaa2", "a", "x", 0, time.Millisecond),
		sp("aaaaaaaaaaaaaaa2", "aaaaaaaaaaaaaaa1", "b", "y", time.Millisecond, time.Millisecond),
		sp("aaaaaaaaaaaaaaa3", "aaaaaaaaaaaaaaa3", "c", "self", 2*time.Millisecond, time.Millisecond),
	}
	done := make(chan []*Node)
	go func() { done <- Build(spans) }()
	select {
	case roots := <-done:
		eq(t, ids(roots), "1@0! 2@1 3@0!")
	case <-time.After(2 * time.Second):
		t.Fatal("Build did not return")
	}
}

// 6. No spans: no roots, and Render still prints a header.
func TestBuildEmpty(t *testing.T) {
	roots := Build(nil)
	if len(roots) != 0 {
		t.Fatalf("roots = %d, want 0", len(roots))
	}
	var b bytes.Buffer
	Render(&b, tid, nil, roots)
	if !strings.Contains(b.String(), "0 spans") {
		t.Errorf("Render = %q, want a 0 spans header", b.String())
	}
}

// 7. The printed tree: offset from the trace's start, duration, branches, and markers.
func TestRender(t *testing.T) {
	spans := checkoutTrace()
	var b bytes.Buffer
	Render(&b, tid, spans, Build(spans))
	want := `trace 4bf92f3577b34da6a3ce929d0e0e4736  4 spans, 3 services, 403ms
   +0ms   310ms  frontend  POST /api/checkout
  +10ms   280ms  └─ checkout  PlaceOrder
  +20ms   2.5ms     ├─ payment  Charge  ERROR
 +400ms   3.0ms     └─ checkout  orders publish  (producer)
`
	if b.String() != want {
		t.Errorf("Render =\n%s\nwant\n%s", b.String(), want)
	}
}

// Orphans say so, and deeper levels keep the parent's vertical line.
func TestRenderOrphanAndDepth(t *testing.T) {
	spans := []model.Span{
		sp("aaaaaaaaaaaaaaa1", "", "frontend", "root", 0, 2*time.Second),
		sp("aaaaaaaaaaaaaaa2", "aaaaaaaaaaaaaaa1", "a", "first", time.Millisecond, 1500*time.Millisecond),
		sp("aaaaaaaaaaaaaaa3", "aaaaaaaaaaaaaaa2", "a", "deep", 2*time.Millisecond, 400*time.Microsecond),
		sp("aaaaaaaaaaaaaaa4", "aaaaaaaaaaaaaaa1", "b", "second", 1600*time.Millisecond, 50*time.Millisecond),
		sp("aaaaaaaaaaaaaaa5", "ffffffffffffffff", "c", "stray", 1700*time.Millisecond, time.Millisecond),
	}
	var b bytes.Buffer
	Render(&b, tid, spans, Build(spans))
	got := b.String()
	for _, line := range []string{
		"├─ a  first",
		"│  └─ a  deep",
		"└─ b  second",
		"c  stray  (parent not stored)",
		"5 spans, 4 services, 2.0s",
	} {
		if !strings.Contains(got, line) {
			t.Errorf("Render missing %q in:\n%s", line, got)
		}
	}
}

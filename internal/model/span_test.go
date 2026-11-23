package model

import (
	"testing"
	"time"
)

// Key: one span's identity for dedup (M4, docs/decisions/0008). A span ID is only unique
// inside its trace, so the trace ID is part of the identity.
func TestSpanKey(t *testing.T) {
	a := Span{TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "00f067aa0ba902b7"}
	if got, want := a.Key(), "4bf92f3577b34da6a3ce929d0e0e4736/00f067aa0ba902b7"; got != want {
		t.Errorf("Key() = %q, want %q", got, want)
	}

	b := a
	b.TraceID = "0af7651916cd43dd8448eb211c80319c"
	if a.Key() == b.Key() {
		t.Errorf("same span ID in different traces share key %q", a.Key())
	}

	// Everything except the IDs may differ: a resent span is the same span.
	c := a
	c.Name, c.Duration = "other", time.Second
	if a.Key() != c.Key() {
		t.Errorf("Key() changed with non-ID fields: %q vs %q", a.Key(), c.Key())
	}
}

func TestSpanEnd(t *testing.T) {
	start := time.Date(2026, 11, 18, 10, 0, 1, 200_000_000, time.UTC)
	s := Span{Start: start, Duration: 250 * time.Millisecond}
	if got, want := s.End(), start.Add(250*time.Millisecond); !got.Equal(want) {
		t.Errorf("End() = %v, want %v", got, want)
	}
}

// ValidTraceID / ValidSpanID: the agent writes IDs as lowercase hex (M4 task 4 moved this
// check here from the processor so the trace CLI uses the same rule).
func TestValidIDs(t *testing.T) {
	cases := []struct {
		id    string
		trace bool
		span  bool
	}{
		{"4bf92f3577b34da6a3ce929d0e0e4736", true, false},
		{"00f067aa0ba902b7", false, true},
		{"4BF92F3577B34DA6A3CE929D0E0E4736", false, false}, // uppercase
		{"4bf92f3577b34da6a3ce929d0e0e473", false, false},  // 31 chars
		{"zbf92f3577b34da6a3ce929d0e0e4736", false, false}, // not hex
		{"00000000000000000000000000000000", false, false}, // W3C's invalid trace ID
		{"0000000000000000", false, false},                 // W3C's invalid span ID
		{"", false, false},
	}
	for _, c := range cases {
		if got := ValidTraceID(c.id); got != c.trace {
			t.Errorf("ValidTraceID(%q) = %v, want %v", c.id, got, c.trace)
		}
		if got := ValidSpanID(c.id); got != c.span {
			t.Errorf("ValidSpanID(%q) = %v, want %v", c.id, got, c.span)
		}
	}
}

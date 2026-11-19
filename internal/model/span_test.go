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

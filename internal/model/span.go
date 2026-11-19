package model

import "time"

// Span is one unit of work in one service, part of a trace: one line in the parcel's logbook.
// IDs are lowercase hex, as OTLP and W3C traceparent headers show them: 32 characters for a
// trace ID, 16 for a span ID. ParentSpanID is empty for a trace's first span.
//
// Kind is the OTLP span kind in lowercase (server, client, producer, consumer, internal) and
// Status is unset, ok or error. Attributes are the span's own; Resource describes what
// produced it (service.name, host.name, container.id, ...). Events and links are left out
// for now (docs/decisions/0008).
type Span struct {
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id,omitempty"`
	Service      string            `json:"service"`
	Name         string            `json:"name"`
	Kind         string            `json:"kind"`
	Start        time.Time         `json:"start"`
	Duration     time.Duration     `json:"duration_ns"`
	Status       string            `json:"status"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	Resource     map[string]string `json:"resource,omitempty"`
}

// Key is the span's identity for dedup: trace ID and span ID. A span ID is only unique inside
// its trace, so both are needed. Nothing else is part of it: a resent span is the same span.
func (s Span) Key() string { return s.TraceID + "/" + s.SpanID }

// End is when the span finished.
func (s Span) End() time.Time { return s.Start.Add(s.Duration) }

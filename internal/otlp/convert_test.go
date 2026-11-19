package otlp

import (
	"reflect"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"taproot/internal/model"
)

var (
	traceA  = pcommon.TraceID([16]byte{0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36})
	spanA1  = pcommon.SpanID([8]byte{0xaa, 0, 0, 0, 0, 0, 0, 0x01})
	spanA2  = pcommon.SpanID([8]byte{0xaa, 0, 0, 0, 0, 0, 0, 0x02})
	t0      = time.Date(2026, 11, 18, 10, 0, 1, 200_000_001, time.UTC)
	traceAx = "4bf92f3577b34da6a3ce929d0e0e4736"
)

// newSpan adds a span to a new scope under rs, with sensible defaults the test then changes.
func newSpan(rs ptrace.ResourceSpans, trace pcommon.TraceID, id pcommon.SpanID) ptrace.Span {
	sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	sp.SetTraceID(trace)
	sp.SetSpanID(id)
	sp.SetName("op")
	sp.SetStartTimestamp(pcommon.NewTimestampFromTime(t0))
	sp.SetEndTimestamp(pcommon.NewTimestampFromTime(t0.Add(time.Millisecond)))
	return sp
}

func resource(td ptrace.Traces, service string) ptrace.ResourceSpans {
	rs := td.ResourceSpans().AppendEmpty()
	if service != "" {
		rs.Resource().Attributes().PutStr("service.name", service)
	}
	return rs
}

// twoSpans is checkout handling PlaceOrder (a server span) and publishing to Kafka (a child).
func twoSpans() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := resource(td, "checkout")
	rs.Resource().Attributes().PutStr("host.name", "h1")

	s1 := newSpan(rs, traceA, spanA1)
	s1.SetName("PlaceOrder")
	s1.SetKind(ptrace.SpanKindServer)
	s1.SetEndTimestamp(pcommon.NewTimestampFromTime(t0.Add(280 * time.Millisecond)))
	s1.Status().SetCode(ptrace.StatusCodeOk)
	s1.Attributes().PutStr("rpc.method", "PlaceOrder")

	s2 := newSpan(rs, traceA, spanA2)
	s2.SetParentSpanID(spanA1)
	s2.SetName("orders publish")
	s2.SetKind(ptrace.SpanKindProducer)
	s2.SetStartTimestamp(pcommon.NewTimestampFromTime(t0.Add(250 * time.Millisecond)))
	s2.SetEndTimestamp(pcommon.NewTimestampFromTime(t0.Add(253 * time.Millisecond)))
	s2.Status().SetCode(ptrace.StatusCodeError)
	return td
}

func wantTwoSpans() []model.Span {
	res := map[string]string{"service.name": "checkout", "host.name": "h1"}
	return []model.Span{
		{TraceID: traceAx, SpanID: "aa00000000000001", Service: "checkout", Name: "PlaceOrder",
			Kind: "server", Start: t0, Duration: 280 * time.Millisecond, Status: "ok",
			Attributes: map[string]string{"rpc.method": "PlaceOrder"}, Resource: res},
		{TraceID: traceAx, SpanID: "aa00000000000002", ParentSpanID: "aa00000000000001", Service: "checkout",
			Name: "orders publish", Kind: "producer", Start: t0.Add(250 * time.Millisecond),
			Duration: 3 * time.Millisecond, Status: "error", Attributes: map[string]string{}, Resource: res},
	}
}

func checkSpans(t *testing.T, got, want []model.Span) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d spans, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		g, w := got[i], want[i]
		if !g.Start.Equal(w.Start) {
			t.Errorf("span %d start = %v, want %v", i, g.Start, w.Start)
		}
		g.Start, w.Start = time.Time{}, time.Time{}
		if !reflect.DeepEqual(g, w) {
			t.Errorf("span %d = %+v\nwant     %+v", i, g, w)
		}
	}
}

// 1. Every field of every span comes across.
func TestSpansFields(t *testing.T) {
	got, skipped := Spans(twoSpans())
	if skipped != 0 {
		t.Errorf("skipped = %d, want 0", skipped)
	}
	checkSpans(t, got, wantTwoSpans())
}

// 2. Kind and status names.
func TestSpansKindAndStatus(t *testing.T) {
	kinds := map[ptrace.SpanKind]string{
		ptrace.SpanKindUnspecified: "internal",
		ptrace.SpanKindInternal:    "internal",
		ptrace.SpanKindServer:      "server",
		ptrace.SpanKindClient:      "client",
		ptrace.SpanKindProducer:    "producer",
		ptrace.SpanKindConsumer:    "consumer",
	}
	for k, want := range kinds {
		td := ptrace.NewTraces()
		newSpan(resource(td, "s"), traceA, spanA1).SetKind(k)
		got, _ := Spans(td)
		if got[0].Kind != want {
			t.Errorf("kind %v → %q, want %q", k, got[0].Kind, want)
		}
	}
	statuses := map[ptrace.StatusCode]string{
		ptrace.StatusCodeUnset: "unset",
		ptrace.StatusCodeOk:    "ok",
		ptrace.StatusCodeError: "error",
	}
	for c, want := range statuses {
		td := ptrace.NewTraces()
		newSpan(resource(td, "s"), traceA, spanA1).Status().SetCode(c)
		got, _ := Spans(td)
		if got[0].Status != want {
			t.Errorf("status %v → %q, want %q", c, got[0].Status, want)
		}
	}
}

// 3. Attributes that aren't strings become text.
func TestSpansAttributeTypes(t *testing.T) {
	td := ptrace.NewTraces()
	sp := newSpan(resource(td, "s"), traceA, spanA1)
	sp.Attributes().PutInt("http.status_code", 503)
	sp.Attributes().PutBool("error", true)
	sp.Attributes().PutDouble("ratio", 0.5)
	arr := sp.Attributes().PutEmptySlice("tags")
	arr.AppendEmpty().SetStr("a")
	arr.AppendEmpty().SetStr("b")

	got, _ := Spans(td)
	want := map[string]string{"http.status_code": "503", "error": "true", "ratio": "0.5", "tags": `["a","b"]`}
	if !reflect.DeepEqual(got[0].Attributes, want) {
		t.Errorf("attributes = %v, want %v", got[0].Attributes, want)
	}
}

// 4. A span from a process that didn't name itself.
func TestSpansNoServiceName(t *testing.T) {
	td := ptrace.NewTraces()
	newSpan(resource(td, ""), traceA, spanA1)
	got, _ := Spans(td)
	if got[0].Service != "unknown_service" {
		t.Errorf("service = %q, want unknown_service", got[0].Service)
	}
}

// 5. A span without a trace ID or span ID can't be identified (or deduplicated): skip it.
func TestSpansSkipsMissingIDs(t *testing.T) {
	td := ptrace.NewTraces()
	rs := resource(td, "s")
	newSpan(rs, pcommon.NewTraceIDEmpty(), spanA1)
	newSpan(rs, traceA, pcommon.NewSpanIDEmpty())
	newSpan(rs, traceA, spanA2)
	got, skipped := Spans(td)
	if skipped != 2 {
		t.Errorf("skipped = %d, want 2", skipped)
	}
	if len(got) != 1 || got[0].SpanID != "aa00000000000002" {
		t.Errorf("kept %+v, want only span aa00000000000002", got)
	}
}

// 6. A clock oddity (end before start) gives a zero duration, not a huge unsigned one.
func TestSpansEndBeforeStart(t *testing.T) {
	td := ptrace.NewTraces()
	sp := newSpan(resource(td, "s"), traceA, spanA1)
	sp.SetEndTimestamp(pcommon.NewTimestampFromTime(t0.Add(-time.Second)))
	got, _ := Spans(td)
	if got[0].Duration != 0 {
		t.Errorf("duration = %v, want 0", got[0].Duration)
	}
}

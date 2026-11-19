// Package otlp is the agent's span intake: it receives OTLP trace exports (the format every
// OpenTelemetry SDK and collector speaks) over HTTP and turns them into model.Span records.
package otlp

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"taproot/internal/model"
)

// Spans flattens an OTLP export into spans. OTLP nests spans by resource (what produced them,
// e.g. service.name=checkout) and scope (which instrumentation library); each span here carries
// its resource's attributes itself. Spans with no trace ID or span ID can't be identified or
// deduplicated, so they are left out and counted in skipped.
func Spans(td ptrace.Traces) (spans []model.Span, skipped int) {
	spans = make([]model.Span, 0, td.SpanCount())
	for _, rs := range td.ResourceSpans().All() {
		res := attrs(rs.Resource().Attributes())
		service := res["service.name"]
		if service == "" {
			service = "unknown_service" // what the OTel SDKs report when no name is set
		}
		for _, ss := range rs.ScopeSpans().All() {
			for _, sp := range ss.Spans().All() {
				if sp.TraceID().IsEmpty() || sp.SpanID().IsEmpty() {
					skipped++
					continue
				}
				parent := ""
				if !sp.ParentSpanID().IsEmpty() {
					parent = sp.ParentSpanID().String()
				}
				start, end := sp.StartTimestamp().AsTime(), sp.EndTimestamp().AsTime()
				spans = append(spans, model.Span{
					TraceID:      sp.TraceID().String(),
					SpanID:       sp.SpanID().String(),
					ParentSpanID: parent,
					Service:      service,
					Name:         sp.Name(),
					Kind:         kind(sp.Kind()),
					Start:        start,
					Duration:     max(end.Sub(start), time.Duration(0)),
					Status:       status(sp.Status().Code()),
					Attributes:   attrs(sp.Attributes()),
					Resource:     res,
				})
			}
		}
	}
	return spans, skipped
}

// attrs turns OTLP attributes into text. Ints, bools and doubles print as themselves; arrays
// and maps as JSON.
func attrs(m pcommon.Map) map[string]string {
	out := make(map[string]string, m.Len())
	for k, v := range m.All() {
		out[k] = v.AsString()
	}
	return out
}

func kind(k ptrace.SpanKind) string {
	switch k {
	case ptrace.SpanKindServer:
		return "server"
	case ptrace.SpanKindClient:
		return "client"
	case ptrace.SpanKindProducer:
		return "producer"
	case ptrace.SpanKindConsumer:
		return "consumer"
	default: // internal, and unspecified (which OTel says to treat as internal)
		return "internal"
	}
}

func status(c ptrace.StatusCode) string {
	switch c {
	case ptrace.StatusCodeOk:
		return "ok"
	case ptrace.StatusCodeError:
		return "error"
	default:
		return "unset"
	}
}

package otlp

import (
	"bytes"
	"compress/gzip"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"taproot/internal/model"
)

// fakeAppend records what the handler hands over, and fails while err is set.
type fakeAppend struct {
	mu    sync.Mutex
	calls int
	got   []model.Span
	err   error
}

func (f *fakeAppend) append(spans []model.Span) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return f.err
	}
	f.got = append(f.got, spans...)
	return nil
}

func protoBody(t *testing.T, td ptrace.Traces) []byte {
	t.Helper()
	b, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func post(t *testing.T, h http.Handler, path, contentType, encoding string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func newHandler(f *fakeAppend) http.Handler { return Handler(f.append, func(string, ...any) {}) }

// 7. The collector's default: protobuf. 200 only once the spans are handed over.
func TestReceiverProtobuf(t *testing.T) {
	f := &fakeAppend{}
	w := post(t, newHandler(f), "/v1/traces", "application/x-protobuf", "", protoBody(t, twoSpans()))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/x-protobuf" {
		t.Errorf("response Content-Type = %q, want application/x-protobuf", ct)
	}
	if err := ptraceotlp.NewExportResponse().UnmarshalProto(w.Body.Bytes()); err != nil {
		t.Errorf("response is not an ExportTraceServiceResponse: %v", err)
	}
	checkSpans(t, f.got, wantTwoSpans())
}

// 8. OTLP/JSON (hex IDs, not base64) gives the same spans.
func TestReceiverJSON(t *testing.T) {
	body, err := ptraceotlp.NewExportRequestFromTraces(twoSpans()).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeAppend{}
	w := post(t, newHandler(f), "/v1/traces", "application/json", "", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("response Content-Type = %q, want application/json", ct)
	}
	checkSpans(t, f.got, wantTwoSpans())
}

// 9. The collector gzips by default.
func TestReceiverGzip(t *testing.T) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write(protoBody(t, twoSpans()))
	zw.Close()
	f := &fakeAppend{}
	w := post(t, newHandler(f), "/v1/traces", "application/x-protobuf", "gzip", buf.Bytes())
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body)
	}
	checkSpans(t, f.got, wantTwoSpans())
}

// 10. The disk buffer refused: tell the collector to come back later, so it retries.
func TestReceiverAppendFails(t *testing.T) {
	f := &fakeAppend{err: errors.New("disk full")}
	w := post(t, newHandler(f), "/v1/traces", "application/x-protobuf", "", protoBody(t, twoSpans()))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("no Retry-After header")
	}
}

// 11. A body that isn't OTLP: retrying can't fix it, so 400, and nothing is buffered.
func TestReceiverGarbage(t *testing.T) {
	f := &fakeAppend{}
	w := post(t, newHandler(f), "/v1/traces", "application/x-protobuf", "", []byte("\xff\xff\xffnot otlp"))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	if f.calls != 0 {
		t.Errorf("append called %d times, want 0", f.calls)
	}
}

// 12. Wrong method, wrong path, oversized body.
func TestReceiverRejects(t *testing.T) {
	f := &fakeAppend{}
	h := newHandler(f)

	req := httptest.NewRequest(http.MethodGet, "/v1/traces", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET status = %d, want 405", w.Code)
	}

	if w := post(t, h, "/v1/metrics", "application/x-protobuf", "", protoBody(t, twoSpans())); w.Code != http.StatusNotFound {
		t.Errorf("/v1/metrics status = %d, want 404", w.Code)
	}

	big := make([]byte, maxBody+1)
	if w := post(t, h, "/v1/traces", "application/x-protobuf", "", big); w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized status = %d, want 413", w.Code)
	}
	if f.calls != 0 {
		t.Errorf("append called %d times, want 0", f.calls)
	}
}

// 13. Only unidentifiable spans: nothing to keep, and nothing a retry would change.
func TestReceiverAllInvalid(t *testing.T) {
	td := ptrace.NewTraces()
	newSpan(resource(td, "s"), pcommon.NewTraceIDEmpty(), spanA1)
	f := &fakeAppend{}
	w := post(t, newHandler(f), "/v1/traces", "application/x-protobuf", "", protoBody(t, td))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if f.calls != 0 {
		t.Errorf("append called %d times, want 0", f.calls)
	}
}

// Break-it test (M4 task 2): one export mixing spans without IDs and good spans, sent as JSON
// with a charset parameter, as some SDKs do. The good spans must all be kept, the bad ones
// dropped, and the export acknowledged (a retry would only bring the same bad spans back).
func TestReceiverMixedValidAndInvalid(t *testing.T) {
	td := twoSpans()
	rs := td.ResourceSpans().At(0)
	newSpan(rs, pcommon.NewTraceIDEmpty(), spanA1) // no trace ID
	newSpan(rs, traceA, pcommon.NewSpanIDEmpty())  // no span ID
	body, err := ptraceotlp.NewExportRequestFromTraces(td).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeAppend{}
	w := post(t, newHandler(f), "/v1/traces", "application/json; charset=utf-8", "", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body)
	}
	if f.calls != 1 {
		t.Errorf("append called %d times, want 1 (one export, one append)", f.calls)
	}
	checkSpans(t, f.got, wantTwoSpans())
}

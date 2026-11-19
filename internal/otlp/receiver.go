package otlp

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"taproot/internal/model"
)

// maxBody caps one export request, compressed or not. The demo's largest is a few tens of KB
// (about 100 spans); the cap only stops a runaway sender from exhausting the agent's memory.
const maxBody = 16 << 20

// logEvery is how often the handler reports how many spans it took in.
const logEvery = time.Minute

// Handler serves OTLP/HTTP trace exports on POST /v1/traces.
//
// The rule (docs/decisions/0008): it answers 200 only after appendSpans has put every span
// on disk. If appendSpans fails, it answers 503 and the collector sends the same export again
// later. So a span the collector was told "got it" for is never lost. A retry after a partial
// append stores some spans twice, and the processor's dedup drops the copies.
//
// Status codes follow the OTLP/HTTP spec: 503 means "retry"; 400, 413 and 415 mean "this
// request will never work", so the collector drops it instead of retrying forever.
func Handler(appendSpans func([]model.Span) error, logf func(format string, args ...any)) http.Handler {
	h := &handler{appendSpans: appendSpans, logf: logf, since: time.Now()}
	mux := http.NewServeMux()
	mux.Handle("/v1/traces", h)
	return mux
}

type handler struct {
	appendSpans func([]model.Span) error
	logf        func(format string, args ...any)

	mu       sync.Mutex
	received int // spans handed to appendSpans since the last report
	skipped  int // spans with no trace or span ID since the last report
	since    time.Time
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}
	isJSON, err := encoding(r.Header.Get("Content-Type"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
		return
	}

	body, status, err := readBody(w, r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}
	req := ptraceotlp.NewExportRequest()
	if isJSON {
		err = req.UnmarshalJSON(body)
	} else {
		err = req.UnmarshalProto(body)
	}
	if err != nil {
		http.Error(w, "not an OTLP trace export: "+err.Error(), http.StatusBadRequest)
		return
	}

	spans, skipped := Spans(req.Traces())
	if len(spans) > 0 {
		if err := h.appendSpans(spans); err != nil {
			// Not on disk (or not all of it): the collector must keep it and try again.
			w.Header().Set("Retry-After", "1")
			http.Error(w, "disk buffer unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	h.count(len(spans), skipped)
	respond(w, isJSON)
}

// encoding reports whether the body is OTLP/JSON (true) or protobuf (false).
func encoding(contentType string) (bool, error) {
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false, fmt.Errorf("bad Content-Type %q", contentType)
	}
	switch mt {
	case "application/x-protobuf":
		return false, nil
	case "application/json":
		return true, nil
	}
	return false, fmt.Errorf("unsupported Content-Type %q: want application/x-protobuf or application/json", mt)
}

// readBody reads the request, gunzipping it if needed, and caps both the compressed and the
// uncompressed size at maxBody.
func readBody(w http.ResponseWriter, r *http.Request) ([]byte, int, error) {
	var src io.Reader = http.MaxBytesReader(w, r.Body, maxBody)
	switch r.Header.Get("Content-Encoding") {
	case "", "identity":
	case "gzip":
		zr, err := gzip.NewReader(src)
		if err != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("bad gzip body: %w", err)
		}
		defer zr.Close()
		src = io.LimitReader(zr, maxBody+1)
	default:
		return nil, http.StatusUnsupportedMediaType, fmt.Errorf("unsupported Content-Encoding %q", r.Header.Get("Content-Encoding"))
	}
	body, err := io.ReadAll(src)
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig) || len(body) > maxBody:
		return nil, http.StatusRequestEntityTooLarge, fmt.Errorf("body over %d bytes", maxBody)
	case err != nil:
		return nil, http.StatusBadRequest, fmt.Errorf("read body: %w", err)
	}
	return body, 0, nil
}

// respond sends an empty ExportTraceServiceResponse (full success) in the request's encoding.
func respond(w http.ResponseWriter, isJSON bool) {
	resp := ptraceotlp.NewExportResponse()
	var b []byte
	var err error
	if isJSON {
		w.Header().Set("Content-Type", "application/json")
		b, err = resp.MarshalJSON()
	} else {
		w.Header().Set("Content-Type", "application/x-protobuf")
		b, err = resp.MarshalProto()
	}
	if err != nil { // an empty message always marshals; this is only a guard
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(b)
}

// count adds to the running totals and logs them about once a minute, so the log shows spans
// are flowing without one line per request.
func (h *handler) count(received, skipped int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.received += received
	h.skipped += skipped
	if elapsed := time.Since(h.since); elapsed >= logEvery {
		h.logf("received %d spans in the last %v (%d skipped: no trace or span ID)",
			h.received, elapsed.Round(time.Second), h.skipped)
		h.received, h.skipped, h.since = 0, 0, time.Now()
	}
}

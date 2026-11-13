package docker

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestEventsStream(t *testing.T) {
	var gotPath string
	var gotFilters map[string][]string
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		json.Unmarshal([]byte(r.URL.Query().Get("filters")), &gotFilters)
		w.Write([]byte(`{"Type":"container","Action":"start","Actor":{"ID":"aaa","Attributes":{"name":"x"}}}` + "\n"))
		w.(http.Flusher).Flush()
		w.Write([]byte(`{"Type":"container","Action":"die","Actor":{"ID":"bbb"}}` + "\n"))
		// handler returns: the stream ends
	})

	var got []Event
	err := c.Events(context.Background(), func(e Event) { got = append(got, e) })
	if err == nil {
		t.Error("want an error when the stream ends")
	}
	want := []Event{{Action: "start", ContainerID: "aaa"}, {Action: "die", ContainerID: "bbb"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("events = %+v, want %+v", got, want)
	}
	if gotPath != "/events" {
		t.Errorf("path = %q, want /events", gotPath)
	}
	wantFilters := map[string][]string{"type": {"container"}, "event": {"start", "die"}}
	if !reflect.DeepEqual(gotFilters, wantFilters) {
		t.Errorf("filters = %v, want %v", gotFilters, wantFilters)
	}
}

func TestEventsReturnsOnCancel(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select { // hold the stream open
		case <-release:
		case <-r.Context().Done():
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Events(ctx, func(Event) {}) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Events did not return after cancel")
	}
}

func TestEventsHTTPError(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	if err := c.Events(context.Background(), func(Event) {}); err == nil {
		t.Error("want error for HTTP 500")
	}
}

func TestEventsBadJSON(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"Action": `)) })
	var n int
	if err := c.Events(context.Background(), func(Event) { n++ }); err == nil {
		t.Error("want error for malformed JSON")
	}
	if n != 0 {
		t.Errorf("got %d events from malformed JSON", n)
	}
}

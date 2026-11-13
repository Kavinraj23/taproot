package docker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

// Trimmed from a real `GET /containers/json` response.
const containersJSON = `[
  {
    "Id": "3f4e5d6c7b8a99887766554433221100ffeeddccbbaa00112233445566778899",
    "Names": ["/astronomy-db"],
    "Image": "postgres:18.4",
    "State": "running",
    "Labels": {
      "com.docker.compose.project": "opentelemetry-demo",
      "com.docker.compose.service": "postgresql"
    }
  },
  {
    "Id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "Names": ["/loose-container"],
    "Image": "alpine:3.20",
    "Labels": {}
  }
]`

func serve(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &Client{BaseURL: srv.URL, HTTP: srv.Client()}
}

func TestContainers(t *testing.T) {
	var gotPath string
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Write([]byte(containersJSON))
	})

	got, err := c.Containers(context.Background())
	if err != nil {
		t.Fatalf("Containers: %v", err)
	}
	if gotPath != "/containers/json" {
		t.Errorf("request path = %q, want /containers/json", gotPath)
	}
	want := []Container{
		{ID: "3f4e5d6c7b8a99887766554433221100ffeeddccbbaa00112233445566778899", Name: "astronomy-db",
			Image: "postgres:18.4", Project: "opentelemetry-demo", ComposeService: "postgresql"},
		{ID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Name: "loose-container", Image: "alpine:3.20"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Containers =\n  %+v\nwant\n  %+v", got, want)
	}
}

func TestContainersHTTPError(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	if _, err := c.Containers(context.Background()); err == nil {
		t.Error("want error for HTTP 500")
	}
}

func TestContainersBadJSON(t *testing.T) {
	c := serve(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`[{"Id": `)) })
	if _, err := c.Containers(context.Background()); err == nil {
		t.Error("want error for malformed JSON")
	}
}

func TestContainersUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens there now

	c := &Client{BaseURL: url, HTTP: &http.Client{}}
	done := make(chan error, 1)
	go func() { _, err := c.Containers(context.Background()); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Error("want error when the server is unreachable")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Containers hung on an unreachable server")
	}
}

//go:build linux

package docker

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestUnixClient(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "docker.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(containersJSON))
	}))
	srv.Listener = ln
	srv.Start()
	defer srv.Close()

	got, err := NewUnixClient(sock).Containers(context.Background())
	if err != nil {
		t.Fatalf("Containers over unix socket: %v", err)
	}
	if len(got) != 2 || got[0].Name != "astronomy-db" {
		t.Errorf("Containers = %+v, want astronomy-db first of 2", got)
	}
}

//go:build linux

package discovery

import (
	"net"
	"os"
	"slices"
	"testing"
)

// Checks the real /proc layout and that os.DirFS can read its fd and exe links.
func TestScanRealProc(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	procs, err := Scan(os.DirFS("/proc"))
	if err != nil {
		t.Fatalf("Scan(/proc): %v", err)
	}
	for _, p := range procs {
		if p.PID != os.Getpid() {
			continue
		}
		if p.Exe == "" {
			t.Errorf("own process has empty exe")
		}
		if !slices.Contains(p.ListenPorts, port) {
			t.Errorf("own process ports = %v, want to contain %d", p.ListenPorts, port)
		}
		return
	}
	t.Fatalf("own pid %d not found in scan of %d processes", os.Getpid(), len(procs))
}

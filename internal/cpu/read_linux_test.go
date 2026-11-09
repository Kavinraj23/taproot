//go:build linux

package cpu

import "testing"

func TestReadProcStat(t *testing.T) {
	got, err := Read("/proc/stat")
	if err != nil {
		t.Fatalf("Read(/proc/stat): %v", err)
	}
	if got.User+got.System+got.Idle == 0 {
		t.Errorf("Read returned all-zero counters: %+v", got)
	}
}

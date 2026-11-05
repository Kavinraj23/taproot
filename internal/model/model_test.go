package model

import "testing"

func TestName(t *testing.T) {
	if Name != "taproot" {
		t.Fatalf("Name = %q, want %q", Name, "taproot")
	}
}

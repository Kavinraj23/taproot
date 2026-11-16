//go:build integration

package sensor

import (
	"context"
	"os"
	"testing"
	"time"

	"taproot/internal/discovery"
)

// Run with a throwaway Valkey:
//
//	docker run -d --name vk-it -p 56379:6379 valkey/valkey:9.0.4-alpine
//	TAPROOT_VALKEY_TEST_ADDR=localhost:56379 go test -tags integration ./internal/sensor/
func TestValkeyCollectAgainstRealServer(t *testing.T) {
	addr := os.Getenv("TAPROOT_VALKEY_TEST_ADDR")
	if addr == "" {
		t.Skip("TAPROOT_VALKEY_TEST_ADDR not set")
	}
	c, err := NewValkey()(discovery.Service{Kind: "valkey"}, addr)
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rs, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	got := byMetric(rs)
	for _, m := range []string{
		"valkey_connected_clients", "valkey_blocked_clients", "valkey_rejected_connections",
		"valkey_used_memory_bytes", "valkey_maxmemory_bytes", "valkey_mem_fragmentation_ratio",
		"valkey_commands", "valkey_keyspace_hits", "valkey_keyspace_misses",
		"valkey_evicted_keys", "valkey_expired_keys",
	} {
		if len(got[m]) != 1 {
			t.Errorf("%s: got %d readings, want 1", m, len(got[m]))
		}
	}
	if rs := got["valkey_connected_clients"]; len(rs) == 1 && rs[0].Value < 1 {
		t.Errorf("connected_clients = %v, want >= 1 (our own connection)", rs[0].Value)
	}
}

func TestValkeyNothingListening(t *testing.T) {
	if os.Getenv("TAPROOT_VALKEY_TEST_ADDR") == "" {
		t.Skip("TAPROOT_VALKEY_TEST_ADDR not set")
	}
	c, err := NewValkey()(discovery.Service{Kind: "valkey"}, "localhost:1")
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := c.Collect(ctx); err == nil {
		t.Error("Collect with nothing listening succeeded")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("failed Collect took %v", d)
	}
}

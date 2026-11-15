//go:build integration

package sensor

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"taproot/internal/discovery"
)

// Run with a throwaway Postgres:
//
//	docker run -d --name pg-it -e POSTGRES_PASSWORD=pw -p 55432:5432 postgres:18.4
//	TAPROOT_PG_TEST_ADDR=localhost:55432 go test -tags integration ./internal/sensor/
func pgTestAddr(t *testing.T) (host string, port int) {
	addr := os.Getenv("TAPROOT_PG_TEST_ADDR")
	if addr == "" {
		t.Skip("TAPROOT_PG_TEST_ADDR not set")
	}
	h, p, _ := strings.Cut(addr, ":")
	n, err := strconv.Atoi(p)
	if err != nil {
		t.Fatalf("bad TAPROOT_PG_TEST_ADDR %q", addr)
	}
	return h, n
}

func TestPostgresCollectAgainstRealServer(t *testing.T) {
	host, port := pgTestAddr(t)
	c, err := NewPostgres("postgres", "pw")(discovery.Service{Kind: "postgres", Port: port}, host+":"+strconv.Itoa(port))
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
		"postgres_connections_max", "postgres_xact_commit", "postgres_xact_rollback", "postgres_blocks_hit",
		"postgres_blocks_read", "postgres_deadlocks", "postgres_locks_waiting", "postgres_longest_query_seconds",
	} {
		if len(got[m]) != 1 {
			t.Errorf("%s: got %d readings, want 1", m, len(got[m]))
		}
	}
	// Our own connection is a client backend, so at least one connection is counted. It's
	// "active" while it runs the activity query itself.
	var total float64
	for _, r := range got["postgres_connections"] {
		total += r.Value
	}
	if total < 1 {
		t.Errorf("postgres_connections total = %v, want >= 1 (our own connection)", total)
	}

	// A second Collect reuses the connection.
	if _, err := c.Collect(ctx); err != nil {
		t.Errorf("second Collect: %v", err)
	}
}

func TestPostgresWrongPassword(t *testing.T) {
	host, port := pgTestAddr(t)
	c, err := NewPostgres("postgres", "wrong")(discovery.Service{Kind: "postgres", Port: port}, host+":"+strconv.Itoa(port))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := c.Collect(ctx); err == nil {
		t.Error("Collect with a wrong password succeeded")
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("failed Collect took %v", time.Since(start))
	}
}

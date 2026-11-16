package sensor

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"taproot/internal/discovery"
)

// An excerpt of real INFO output from the demo's valkey-cart, trimmed.
const sampleInfo = "# Server\r\n" +
	"redis_version:7.2.4\r\n" +
	"valkey_version:9.0.4\r\n" +
	"executable:/usr/local/bin/valkey-server\r\n" +
	"\r\n" +
	"# Clients\r\n" +
	"connected_clients:3\r\n" +
	"blocked_clients:1\r\n" +
	"\r\n" +
	"# Memory\r\n" +
	"used_memory:1282512\r\n" +
	"maxmemory:0\r\n" +
	"mem_fragmentation_ratio:7.59\r\n" +
	"\r\n" +
	"# Stats\r\n" +
	"total_commands_processed:98\r\n" +
	"rejected_connections:0\r\n" +
	"expired_keys:4\r\n" +
	"evicted_keys:2\r\n" +
	"keyspace_hits:21\r\n" +
	"keyspace_misses:20\r\n" +
	"\r\n" +
	"# Keyspace\r\n" +
	"db0:keys=54,expires=53,avg_ttl=2616161,keys_with_volatile_items=0\r\n" +
	"db3:keys=2,expires=0,avg_ttl=0\r\n"

func TestParseInfo(t *testing.T) {
	got := parseInfo(sampleInfo)
	if got["connected_clients"] != "3" || got["valkey_version"] != "9.0.4" {
		t.Errorf("fields = %v", got)
	}
	if got["executable"] != "/usr/local/bin/valkey-server" {
		t.Errorf("executable = %q, want the full path", got["executable"])
	}
	for k := range got {
		if k == "" || strings.HasPrefix(k, "#") || strings.ContainsAny(k, "\r\n") {
			t.Errorf("unexpected key %q", k)
		}
	}
}

func TestValkeyReadings(t *testing.T) {
	got := byMetric(valkeyReadings(parseInfo(sampleInfo)))
	want := []struct {
		metric string
		kind   Kind
		value  float64
	}{
		{"valkey_connected_clients", Gauge, 3},
		{"valkey_blocked_clients", Gauge, 1},
		{"valkey_rejected_connections", Counter, 0},
		{"valkey_used_memory_bytes", Gauge, 1282512},
		{"valkey_maxmemory_bytes", Gauge, 0},
		{"valkey_mem_fragmentation_ratio", Gauge, 7.59},
		{"valkey_commands", Counter, 98},
		{"valkey_keyspace_hits", Counter, 21},
		{"valkey_keyspace_misses", Counter, 20},
		{"valkey_evicted_keys", Counter, 2},
		{"valkey_expired_keys", Counter, 4},
	}
	for _, w := range want {
		rs := got[w.metric]
		if len(rs) != 1 || rs[0].Kind != w.kind || rs[0].Value != w.value {
			t.Errorf("%s = %+v, want one %v of %v", w.metric, rs, w.kind, w.value)
		}
	}

	keys := map[string]float64{}
	for _, r := range got["valkey_keys"] {
		if r.Kind != Gauge {
			t.Errorf("valkey_keys kind = %v, want Gauge", r.Kind)
		}
		keys[r.Labels["db"]] = r.Value
	}
	if len(keys) != 2 || keys["db0"] != 54 || keys["db3"] != 2 {
		t.Errorf("valkey_keys by db = %v, want db0 54, db3 2", keys)
	}
}

func TestValkeyReadingsEmptyKeyspace(t *testing.T) {
	got := byMetric(valkeyReadings(parseInfo("# Clients\r\nconnected_clients:1\r\n# Keyspace\r\n")))
	if rs := got["valkey_keys"]; len(rs) != 0 {
		t.Errorf("valkey_keys = %+v, want none for an empty cache", rs)
	}
}

func TestValkeyReadingsSkipsMissingAndBadFields(t *testing.T) {
	got := byMetric(valkeyReadings(map[string]string{
		"connected_clients": "5",
		"used_memory":       "lots", // not a number
		"db0":               "keys=oops,expires=0",
	}))
	if rs := got["valkey_connected_clients"]; len(rs) != 1 || rs[0].Value != 5 {
		t.Errorf("connected_clients = %+v, want 5", rs)
	}
	for _, m := range []string{"valkey_used_memory_bytes", "valkey_commands", "valkey_keys"} {
		if len(got[m]) != 0 {
			t.Errorf("%s = %+v, want skipped", m, got[m])
		}
	}
}

func TestReadReply(t *testing.T) {
	read := func(s string) (string, error) { return readReply(bufio.NewReader(strings.NewReader(s))) }

	if got, err := read("$5\r\nhello\r\n"); err != nil || got != "hello" {
		t.Errorf("bulk string = %q, %v; want hello", got, err)
	}
	if _, err := read("-NOAUTH Authentication required.\r\n"); err == nil || !strings.Contains(err.Error(), "NOAUTH Authentication required.") {
		t.Errorf("error reply = %v, want it to carry the server's message", err)
	}
	for _, bad := range []string{"$10\r\nhello\r\n", "+OK\r\n", "$x\r\n", "$-1\r\n", ""} {
		if _, err := read(bad); err == nil {
			t.Errorf("readReply(%q) succeeded, want an error", bad)
		}
	}
}

// fakeValkey answers INFO with sampleInfo. It counts accepted connections and can drop them all.
type fakeValkey struct {
	ln       net.Listener
	accepted atomic.Int32
	conns    chan net.Conn
}

func newFakeValkey(t *testing.T) *fakeValkey {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeValkey{ln: ln, conns: make(chan net.Conn, 10)}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.accepted.Add(1)
			f.conns <- c
			go f.serve(c)
		}
	}()
	return f
}

func (f *fakeValkey) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	want := "*1\r\n$4\r\nINFO\r\n"
	buf := make([]byte, len(want))
	for {
		if _, err := io.ReadFull(r, buf); err != nil {
			return
		}
		if string(buf) != want {
			fmt.Fprintf(c, "-ERR unexpected %q\r\n", buf)
			continue
		}
		fmt.Fprintf(c, "$%d\r\n%s\r\n", len(sampleInfo), sampleInfo)
	}
}

// dropAll closes every connection accepted so far.
func (f *fakeValkey) dropAll() {
	for {
		select {
		case c := <-f.conns:
			c.Close()
		default:
			return
		}
	}
}

func newValkeyCollector(t *testing.T, addr string) Collector {
	c, err := NewValkey()(discovery.Service{Kind: "valkey"}, addr)
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestValkeyCollectorReusesAndReconnects(t *testing.T) {
	f := newFakeValkey(t)
	c := newValkeyCollector(t, f.ln.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := range 2 {
		rs, err := c.Collect(ctx)
		if err != nil {
			t.Fatalf("Collect %d: %v", i, err)
		}
		if got := byMetric(rs)["valkey_connected_clients"]; len(got) != 1 || got[0].Value != 3 {
			t.Fatalf("Collect %d: connected_clients = %+v", i, got)
		}
	}
	if n := f.accepted.Load(); n != 1 {
		t.Errorf("two Collects opened %d connections, want 1", n)
	}

	f.dropAll()
	if _, err := c.Collect(ctx); err == nil {
		t.Error("Collect on a dropped connection succeeded")
	}
	if _, err := c.Collect(ctx); err != nil {
		t.Errorf("Collect after reconnecting: %v", err)
	}
	if n := f.accepted.Load(); n != 2 {
		t.Errorf("connections = %d, want 2 (one reconnect)", n)
	}
}

func TestValkeyCollectExpiredContext(t *testing.T) {
	// A server that accepts but never answers.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		var held []net.Conn // kept open, never answered
		defer func() {
			for _, c := range held {
				c.Close()
			}
		}()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			held = append(held, c)
		}
	}()

	c := newValkeyCollector(t, ln.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := c.Collect(ctx); err == nil {
		t.Error("Collect against a silent server succeeded")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("Collect took %v, want it to give up when the context ends", d)
	}
}

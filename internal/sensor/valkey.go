package sensor

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"taproot/internal/discovery"
)

// The INFO command in RESP, Valkey's wire format: an array of one bulk string.
const valkeyInfoCmd = "*1\r\n$4\r\nINFO\r\n"

// valkeyFields maps INFO fields to metrics. Counters are totals since Valkey started; the
// Manager turns them into per-second rates. Valkey's own instantaneous_ops_per_sec isn't used:
// it's averaged over Valkey's own window, so it wouldn't line up with our timestamps.
var valkeyFields = []struct {
	field, metric string
	kind          Kind
}{
	{"connected_clients", "valkey_connected_clients", Gauge},
	{"blocked_clients", "valkey_blocked_clients", Gauge},
	{"rejected_connections", "valkey_rejected_connections", Counter},
	{"used_memory", "valkey_used_memory_bytes", Gauge},
	{"maxmemory", "valkey_maxmemory_bytes", Gauge}, // 0 means no limit
	{"mem_fragmentation_ratio", "valkey_mem_fragmentation_ratio", Gauge},
	{"total_commands_processed", "valkey_commands", Counter},
	{"keyspace_hits", "valkey_keyspace_hits", Counter},
	{"keyspace_misses", "valkey_keyspace_misses", Counter},
	{"evicted_keys", "valkey_evicted_keys", Counter},
	{"expired_keys", "valkey_expired_keys", Counter},
}

// parseInfo splits INFO output into fields. Lines look like "name:value"; "# Section" headers
// and blank lines are skipped. A value may itself contain ':' (paths), so only the first splits.
func parseInfo(text string) map[string]string {
	out := map[string]string{}
	for line := range strings.Lines(text) {
		line = strings.TrimRight(line, "\r\n")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if k, v, ok := strings.Cut(line, ":"); ok && k != "" {
			out[k] = v
		}
	}
	return out
}

// valkeyReadings maps INFO fields to readings. A missing or non-numeric field is skipped, not an
// error: older Redis versions lack some fields, and the rest are still worth sending.
func valkeyReadings(info map[string]string) []Reading {
	rs := make([]Reading, 0, len(valkeyFields)+1)
	for _, f := range valkeyFields {
		v, err := strconv.ParseFloat(info[f.field], 64)
		if err != nil {
			continue
		}
		rs = append(rs, Reading{Metric: f.metric, Kind: f.kind, Value: v})
	}
	// Keyspace lines: "db0:keys=54,expires=53,avg_ttl=...". Only databases holding keys appear.
	for k, v := range info {
		if !isDBName(k) {
			continue
		}
		for part := range strings.SplitSeq(v, ",") {
			name, n, _ := strings.Cut(part, "=")
			if name != "keys" {
				continue
			}
			if keys, err := strconv.ParseFloat(n, 64); err == nil {
				rs = append(rs, Reading{Metric: "valkey_keys", Kind: Gauge, Value: keys, Labels: map[string]string{"db": k}})
			}
		}
	}
	return rs
}

// isDBName reports whether k is "db" followed by digits.
func isDBName(k string) bool {
	n, ok := strings.CutPrefix(k, "db")
	if !ok || n == "" {
		return false
	}
	_, err := strconv.Atoi(n)
	return err == nil
}

// readReply reads one RESP reply that should be a bulk string ("$<len>\r\n<bytes>\r\n"). An
// error reply ("-ERR ...") comes back as an error carrying the server's message.
func readReply(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return "", errors.New("valkey: empty reply")
	}
	switch line[0] {
	case '-':
		return "", fmt.Errorf("valkey: %s", line[1:])
	case '$':
	default:
		return "", fmt.Errorf("valkey: unexpected reply %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 0 {
		return "", fmt.Errorf("valkey: bad reply length %q", line)
	}
	buf := make([]byte, n+2) // payload plus its trailing \r\n
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", fmt.Errorf("valkey: reply cut short: %w", err)
	}
	return string(buf[:n]), nil
}

// NewValkey returns a Factory for Valkey (or Redis) collectors. No login: the demo's Valkey has
// no password.
func NewValkey() Factory {
	return func(_ discovery.Service, addr string) (Collector, error) {
		return &valkeyCollector{addr: addr}, nil
	}
}

// valkeyCollector keeps one connection open, reconnecting after any error.
type valkeyCollector struct {
	addr string
	conn net.Conn
	r    *bufio.Reader
}

func (c *valkeyCollector) Collect(ctx context.Context) ([]Reading, error) {
	if c.conn == nil {
		d := net.Dialer{Timeout: 2 * time.Second}
		conn, err := d.DialContext(ctx, "tcp", c.addr)
		if err != nil {
			return nil, err
		}
		c.conn, c.r = conn, bufio.NewReader(conn)
	}
	text, err := c.info(ctx)
	if err != nil {
		c.Close() // a broken or half-read connection is replaced on the next tick
		return nil, err
	}
	return valkeyReadings(parseInfo(text)), nil
}

func (c *valkeyCollector) info(ctx context.Context) (string, error) {
	// A plain connection doesn't watch ctx, so its deadline stands in for it.
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(2 * time.Second)
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return "", err
	}
	if _, err := io.WriteString(c.conn, valkeyInfoCmd); err != nil {
		return "", err
	}
	return readReply(c.r)
}

func (c *valkeyCollector) Close() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn, c.r = nil, nil
	return err
}

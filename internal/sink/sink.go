// Package sink sends metric points to Redpanda.
//
// Sending is asynchronous so the collection loop never waits on the network. Undelivered
// points sit in the client's bounded in-memory buffer while it retries with no time limit;
// once the buffer is full, new points are dropped and counted. The disk-backed buffer (M3)
// replaces this drop behavior.
//
// There is deliberately no delivery timeout: franz-go fails every buffered record in a
// partition once the oldest one times out, and only checks after the broker responds, so a
// timeout would drop the whole outage, late, rather than just the oldest points.
package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"taproot/internal/model"
)

// maxBuffered is about 10 minutes of points at one per second: an outage shorter than that
// loses nothing.
const maxBuffered = 600

// producer is the part of *kgo.Client the sink uses, so tests can swap in a fake.
type producer interface {
	TryProduce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Flush(ctx context.Context) error
	Close()
}

// NewClient builds a Redpanda client that sends to topic by default.
// Acks from all replicas, the idempotent producer, and unlimited retries are franz-go defaults.
func NewClient(brokers []string, topic string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.MaxBufferedRecords(maxBuffered),
	)
}

// Key identifies a series: the metric name plus its labels sorted by name, such as
// host_cpu_usage_percent{host=h1}. Records with the same key go to the same partition, so
// one series stays in order. It decides placement only; it is not a dedup key.
func Key(p model.Point) []byte {
	names := make([]string, 0, len(p.Labels))
	for n := range p.Labels {
		names = append(names, n)
	}
	sort.Strings(names)

	var b strings.Builder
	b.WriteString(p.Metric)
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n)
		b.WriteByte('=')
		b.WriteString(p.Labels[n])
	}
	b.WriteByte('}')
	return []byte(b.String())
}

// Encode turns a point into a record keyed by series with a JSON value. The topic is left
// empty for the client's default topic to fill in.
func Encode(p model.Point) (*kgo.Record, error) {
	v, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("encode point: %w", err)
	}
	return &kgo.Record{Key: Key(p), Value: v}, nil
}

// Sink sends points and reports delivery trouble only when it starts and ends, so an
// outage produces two log lines, not one per point.
type Sink struct {
	p    producer
	logf func(format string, args ...any)

	mu      sync.Mutex // promises run on the client's goroutines
	failing bool
	dropped int
}

// New wraps a producer; *kgo.Client from NewClient is the real one.
func New(p producer, logf func(format string, args ...any)) *Sink {
	return &Sink{p: p, logf: logf}
}

// Emit sends one point without waiting for it to be delivered.
func (s *Sink) Emit(pt model.Point) {
	r, err := Encode(pt)
	if err != nil {
		s.result(err)
		return
	}
	s.p.TryProduce(context.Background(), r, func(_ *kgo.Record, err error) { s.result(err) })
}

// result records one point's outcome: nil means delivered, anything else means dropped.
func (s *Sink) result(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		if !s.failing {
			s.failing = true
			s.logf("redpanda delivery failing: %v; dropping points", err)
		}
		s.dropped++
		return
	}
	if s.failing {
		s.logf("redpanda delivery recovered; dropped %d points", s.dropped)
		s.failing = false
		s.dropped = 0
	}
}

// Close waits up to timeout for buffered points to be delivered, then closes the client.
// Points still undelivered at the deadline are lost.
func (s *Sink) Close(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := s.p.Flush(ctx); err != nil {
		s.logf("flush on shutdown: %v", err)
	}
	s.p.Close()
}

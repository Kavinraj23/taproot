// Package sink sends metric points to Redpanda through the agent's disk buffer.
//
// Emit writes a point to the spool on disk and returns; it never waits on the network. Run
// reads the spool in order and produces each point, and crosses it off (Ack) once Redpanda
// confirms it. While Redpanda is down or slow, the client's in-memory window fills, Produce
// blocks, and points wait on disk: up to the spool's cap (hours), and across agent restarts.
//
// There is deliberately no delivery timeout (decision 0001): retries are unlimited, so a
// failed promise means a non-retriable error, or shutdown.
package sink

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"taproot/internal/model"
	"taproot/internal/spool"
)

// inFlight is how many records the client holds in memory before Produce blocks. It is the
// send window, not a loss limit: everything beyond it waits on disk.
const inFlight = 600

// Backlog thresholds for logging: "behind" when this many points wait on disk (about 15s at
// ~40 points/s), "caught up" once it is back under caughtUpAt.
const (
	behindAt   = 600
	caughtUpAt = 100
)

// producer is the part of *kgo.Client the sink uses, so tests can swap in a fake.
type producer interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Flush(ctx context.Context) error
	Close()
}

// NewClient builds a Redpanda client that sends to topic by default.
// Acks from all replicas, the idempotent producer, and unlimited retries are franz-go defaults.
func NewClient(brokers []string, topic string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.MaxBufferedRecords(inFlight),
	)
}

// Key identifies a series: the metric name plus its labels sorted by name, such as
// host_cpu_usage_percent{host=h1}. Records with the same key go to the same partition, so
// one series stays in order. It decides placement only; dedup uses model.Point.Key.
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

// frame packs a record's key and value into one spool entry: uint32 key length | key | value.
func frame(key, value []byte) []byte {
	b := make([]byte, 4+len(key)+len(value))
	binary.BigEndian.PutUint32(b, uint32(len(key)))
	copy(b[4:], key)
	copy(b[4+len(key):], value)
	return b
}

func unframe(b []byte) (key, value []byte, err error) {
	if len(b) < 4 {
		return nil, nil, errors.New("spool entry too short")
	}
	n := binary.BigEndian.Uint32(b)
	if uint64(n) > uint64(len(b)-4) {
		return nil, nil, fmt.Errorf("spool entry key length %d exceeds entry", n)
	}
	return b[4 : 4+n], b[4+n:], nil
}

// Sink sends points via the spool and reports trouble when it starts and when it ends, so an
// outage produces two log lines, not one per point.
type Sink struct {
	p    producer
	sp   *spool.Spool
	logf func(format string, args ...any)

	mu            sync.Mutex
	behind        bool // backlog over behindAt, logged once
	failing       bool // permanent delivery errors, logged once per episode
	failed        int
	appendFailing bool
	running       bool
	done          chan struct{} // closed when Run returns
}

// New wraps a producer (*kgo.Client from NewClient is the real one) and an open spool.
func New(p producer, sp *spool.Spool, logf func(format string, args ...any)) *Sink {
	return &Sink{p: p, sp: sp, logf: logf, done: make(chan struct{})}
}

// Emit writes one point to the disk buffer. It returns straight away; a failed write is
// logged (once per episode) and the point dropped, since the sensor can't do anything better.
func (s *Sink) Emit(pt model.Point) {
	r, err := Encode(pt)
	if err != nil {
		s.logf("drop point %s: %v", pt.Metric, err)
		return
	}
	s.Append(r.Key, r.Value)
}

// Append writes one record (Redpanda key and value) to the disk buffer and says whether it
// got there. The OTLP receiver uses the error to tell the collector to retry (M4).
func (s *Sink) Append(key, value []byte) error {
	err := s.sp.Append(frame(key, value))
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err != nil && !s.appendFailing:
		s.appendFailing = true
		s.logf("disk buffer write failing (points dropped, spans refused for the collector to retry): %v", err)
	case err == nil && s.appendFailing:
		s.appendFailing = false
		s.logf("disk buffer write recovered")
	}
	return err
}

// Run sends buffered points to Redpanda until ctx is done, oldest first, and saves progress
// to disk about once a second.
func (s *Sink) Run(ctx context.Context) {
	s.mu.Lock()
	s.running = true
	s.mu.Unlock()
	defer close(s.done)

	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := s.sp.Sync(); err != nil && !errors.Is(err, spool.ErrClosed) {
					s.logf("disk buffer sync: %v", err)
				}
				s.checkBacklog()
			}
		}
	}()

	for {
		pos, b, err := s.sp.Next(ctx)
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, spool.ErrClosed) {
				s.logf("disk buffer read: %v", err)
			}
			return
		}
		key, value, err := unframe(b)
		if err != nil {
			s.logf("disk buffer: skipping damaged entry: %v", err)
			s.sp.Ack(pos)
			continue
		}
		// Blocks while the client's window is full: that is the backpressure that keeps
		// points on disk during an outage.
		s.p.Produce(ctx, &kgo.Record{Key: key, Value: value}, func(_ *kgo.Record, err error) {
			s.delivered(pos, err)
		})
	}
}

// delivered handles one promise. A point cut off by shutdown stays on disk to be sent on the
// next start; any other error is permanent (retries are unlimited), so the point is logged
// and crossed off rather than blocking everything behind it.
func (s *Sink) delivered(pos spool.Pos, err error) {
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, kgo.ErrClientClosed)) {
		return
	}
	s.mu.Lock()
	switch {
	case err != nil:
		if !s.failing {
			s.failing = true
			s.logf("redpanda rejected a point, skipping it: %v", err)
		}
		s.failed++
	case s.failing:
		s.logf("redpanda accepting points again; skipped %d", s.failed)
		s.failing, s.failed = false, 0
	}
	s.mu.Unlock()
	s.sp.Ack(pos)
}

// checkBacklog logs once when points start piling up on disk and once when they clear.
func (s *Sink) checkBacklog() {
	n := s.sp.Pending()
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case !s.behind && n >= behindAt:
		s.behind = true
		s.logf("redpanda behind: %d points waiting on disk", n)
	case s.behind && n < caughtUpAt:
		s.behind = false
		s.logf("redpanda caught up (%d points dropped by the buffer cap since start)", s.sp.Dropped())
	}
}

// Close is called after Run's context is done. It waits for Run to stop, gives records already
// handed to the client up to timeout to be confirmed, then saves progress and closes. Anything
// unconfirmed stays on disk and is sent on the next start.
func (s *Sink) Close(timeout time.Duration) {
	s.mu.Lock()
	running := s.running
	s.mu.Unlock()
	if running {
		select {
		case <-s.done:
		case <-time.After(timeout):
			s.logf("sink: sender still busy after %v; closing anyway", timeout)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := s.p.Flush(ctx); err != nil {
		s.logf("flush on shutdown: %v", err)
	}
	if err := s.sp.Close(); err != nil {
		s.logf("disk buffer close: %v", err)
	}
	s.p.Close()
}

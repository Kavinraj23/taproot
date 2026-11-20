// Package processor moves metric points from Redpanda into storage: poll a batch, decode it,
// write it, mark it done in Redpanda, repeat.
package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"taproot/internal/model"
)

const maxBackoff = 30 * time.Second

// Loop holds everything Run depends on, so tests can swap in fakes. T is what one record
// decodes to: a metric point or (M4) a span. The rules are the same for both.
type Loop[T any] struct {
	Poll   func(ctx context.Context) ([]*kgo.Record, error)    // one fetch from Redpanda
	Decode func(r *kgo.Record) (T, error)                      // one record; an error skips it
	Write  func(ctx context.Context, items []T) error          // one batch insert
	Commit func(ctx context.Context, recs []*kgo.Record) error // mark records done in Redpanda
	Wait   func(ctx context.Context, d time.Duration) error    // sleep d, or return ctx's error
	Logf   func(format string, args ...any)
}

// Config is the metrics loop, the processor's original job.
type Config = Loop[model.Point]

// DecodeSpan turns a record into a span, rejecting records that couldn't be stored or
// deduplicated: the agent writes IDs as lowercase hex (internal/otlp), so anything else is
// corruption.
func DecodeSpan(r *kgo.Record) (model.Span, error) {
	var s model.Span
	if err := json.Unmarshal(r.Value, &s); err != nil {
		return s, fmt.Errorf("decode span: %w", err)
	}
	switch {
	case !hexID(s.TraceID, 32):
		return s, fmt.Errorf("decode span: bad trace_id %q", s.TraceID)
	case !hexID(s.SpanID, 16):
		return s, fmt.Errorf("decode span: bad span_id %q", s.SpanID)
	case s.Service == "":
		return s, errors.New("decode span: empty service")
	case s.Start.IsZero():
		return s, errors.New("decode span: missing start")
	}
	return s, nil
}

// hexID reports whether id is n lowercase hex digits and not all zeros (W3C's invalid ID).
func hexID(id string, n int) bool {
	if len(id) != n {
		return false
	}
	nonzero := false
	for _, c := range id {
		switch {
		case c >= '1' && c <= '9', c >= 'a' && c <= 'f':
			nonzero = true
		case c == '0':
		default:
			return false
		}
	}
	return nonzero
}

// Decode turns a record into a point, rejecting records that would make a meaningless row.
func Decode(r *kgo.Record) (model.Point, error) {
	var p model.Point
	if err := json.Unmarshal(r.Value, &p); err != nil {
		return p, fmt.Errorf("decode: %w", err)
	}
	if p.Metric == "" {
		return p, errors.New("decode: empty metric")
	}
	if p.Timestamp.IsZero() {
		return p, errors.New("decode: missing ts")
	}
	return p, nil
}

// backoff is the wait before retry number attempt (1-based): 1s, 2s, 4s, ... capped at 30s.
func backoff(attempt int) time.Duration {
	if attempt > 6 { // 2^5 s already exceeds the cap; stop shifting before it overflows
		return maxBackoff
	}
	return min(time.Second<<(attempt-1), maxBackoff)
}

// Run polls, decodes, writes and commits until ctx is done. A batch that fails to write is
// retried with backoff before anything new is polled, so a storage outage pauses consumption
// and Redpanda holds the backlog.
//
// Commit-after-write: a batch is marked done in Redpanda only once it is stored, so a crash at
// any point re-reads it rather than losing it (at-least-once). A crash between the write and
// the commit stores it again; the write path drops points that are already stored.
func Run[T any](ctx context.Context, cfg Loop[T]) {
	for ctx.Err() == nil {
		recs, err := cfg.Poll(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			cfg.Logf("poll: %v", err)
			if cfg.Wait(ctx, time.Second) != nil {
				return
			}
			continue
		}

		items := make([]T, 0, len(recs))
		for _, r := range recs {
			it, err := cfg.Decode(r)
			if err != nil {
				// A poison record is skipped so one bad message can't stall the pipeline.
				cfg.Logf("skip record at partition %d offset %d: %v", r.Partition, r.Offset, err)
				continue
			}
			items = append(items, it)
		}
		if len(recs) == 0 {
			continue
		}

		if len(items) > 0 && !write(ctx, cfg, items) {
			return
		}

		// Every polled record, skipped ones included, so the offsets move past bad records.
		// A failed commit only means a replay later, which dedup absorbs; the next commit
		// covers these offsets anyway, so it isn't retried.
		if err := cfg.Commit(ctx, recs); err != nil && ctx.Err() == nil {
			cfg.Logf("commit %d records: %v", len(recs), err)
		}
	}
}

// write retries one batch until it is stored. It returns false if ctx ended first.
func write[T any](ctx context.Context, cfg Loop[T], items []T) bool {
	for attempt := 1; ; attempt++ {
		err := cfg.Write(ctx, items)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		d := backoff(attempt)
		cfg.Logf("write %d records failed (attempt %d), retrying in %v: %v", len(items), attempt, d, err)
		if cfg.Wait(ctx, d) != nil {
			return false
		}
	}
}

// Sleep is the real Config.Wait.
func Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// NewConsumer builds a client in consumer group group for topic. Each topic has its own group,
// so metrics and spans keep separate progress. Auto-commit is off: Run commits each batch
// itself, right after it is written (see CommitFrom).
func NewConsumer(brokers []string, topic, group string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // first run reads what's already there
		kgo.DisableAutoCommit(),
	)
}

// CommitFrom adapts a consumer client to Config.Commit. CommitRecords commits, per partition,
// the offset after the highest record given.
func CommitFrom(cl *kgo.Client) func(ctx context.Context, recs []*kgo.Record) error {
	return func(ctx context.Context, recs []*kgo.Record) error {
		return cl.CommitRecords(ctx, recs...)
	}
}

// PollFrom adapts a consumer client to Config.Poll.
func PollFrom(cl *kgo.Client) func(ctx context.Context) ([]*kgo.Record, error) {
	return func(ctx context.Context) ([]*kgo.Record, error) {
		fetches := cl.PollFetches(ctx)
		var errs []error
		fetches.EachError(func(topic string, partition int32, err error) {
			if !errors.Is(err, context.Canceled) {
				errs = append(errs, fmt.Errorf("%s/%d: %w", topic, partition, err))
			}
		})
		return fetches.Records(), errors.Join(errs...)
	}
}

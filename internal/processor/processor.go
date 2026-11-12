// Package processor moves metric points from Redpanda into storage: poll a batch, decode it,
// write it, repeat.
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

// Config holds everything Run depends on, so tests can swap in fakes.
type Config struct {
	Poll  func(ctx context.Context) ([]*kgo.Record, error)  // one fetch from Redpanda
	Write func(ctx context.Context, pts []model.Point) error // one batch insert
	Wait  func(ctx context.Context, d time.Duration) error   // sleep d, or return ctx's error
	Logf  func(format string, args ...any)
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

// Run polls, decodes and writes until ctx is done. A batch that fails to write is retried
// with backoff before anything new is polled, so a storage outage pauses consumption and
// Redpanda holds the backlog.
func Run(ctx context.Context, cfg Config) {
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

		pts := make([]model.Point, 0, len(recs))
		for _, r := range recs {
			p, err := Decode(r)
			if err != nil {
				// A poison record is skipped so one bad message can't stall the pipeline.
				cfg.Logf("skip record at partition %d offset %d: %v", r.Partition, r.Offset, err)
				continue
			}
			pts = append(pts, p)
		}
		if len(pts) == 0 {
			continue
		}

		if !write(ctx, cfg, pts) {
			return
		}

		// M3 (hand-written): commit-after-write goes here. Mark these records done in
		// Redpanda only now that they're stored. Until then the consumer auto-commits on a
		// timer, which can commit records that were polled but not yet written: a crash in
		// that window loses them.
	}
}

// write retries one batch until it is stored. It returns false if ctx ended first.
func write(ctx context.Context, cfg Config, pts []model.Point) bool {
	for attempt := 1; ; attempt++ {
		err := cfg.Write(ctx, pts)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		d := backoff(attempt)
		cfg.Logf("write %d points failed (attempt %d), retrying in %v: %v", len(pts), attempt, d, err)
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

// NewConsumer builds a consumer-group client for topic. Offsets are auto-committed on a
// timer for now (see the M3 note in Run).
func NewConsumer(brokers []string, topic string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("taproot-processor"),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // first run reads what's already there
	)
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

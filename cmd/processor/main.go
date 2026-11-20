package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"taproot/internal/model"
	"taproot/internal/processor"
	"taproot/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	openCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := store.Open(openCtx, store.Config{
		Addr:     envOr("TAPROOT_CLICKHOUSE_ADDR", "localhost:9000"),
		User:     envOr("TAPROOT_CLICKHOUSE_USER", "taproot"),
		Password: envOr("TAPROOT_CLICKHOUSE_PASSWORD", "taproot"),
		Database: "taproot",
		Table:    "metrics",
	})
	if err != nil {
		log.Fatalf("%v", err)
	}
	defer db.Close()
	if err := db.EnsureSchema(openCtx); err != nil {
		log.Fatalf("%v", err)
	}
	if err := db.EnsureSpanSchema(openCtx); err != nil {
		log.Fatalf("%v", err)
	}

	brokers := strings.Split(envOr("TAPROOT_BROKERS", "localhost:9092"), ",")
	topic := envOr("TAPROOT_TOPIC", "taproot.metrics")
	consumer, err := processor.NewConsumer(brokers, topic, "taproot-processor")
	if err != nil {
		log.Fatalf("redpanda consumer: %v", err)
	}
	defer consumer.Close() // leaves the group; every written batch is already committed

	// Spans (M4) run on their own loop and consumer group, so a stall on one kind (say,
	// ClickHouse rejecting a span batch) doesn't hold up the other.
	spanTopic := envOr("TAPROOT_SPAN_TOPIC", "taproot.spans")
	spanConsumer, err := processor.NewConsumer(brokers, spanTopic, "taproot-processor-spans")
	if err != nil {
		log.Fatalf("redpanda consumer (spans): %v", err)
	}
	defer spanConsumer.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		log.Printf("processor reading %s at %v into taproot.metrics", topic, brokers)
		processor.Run(ctx, processor.Config{
			Poll:   processor.PollFrom(consumer),
			Decode: processor.Decode,
			Write:  writeNew(db),
			Commit: processor.CommitFrom(consumer),
			Wait:   processor.Sleep,
			Logf:   log.Printf,
		})
	}()
	go func() {
		defer wg.Done()
		log.Printf("processor reading %s at %v into taproot.spans", spanTopic, brokers)
		processor.Run(ctx, processor.Loop[model.Span]{
			Poll:   processor.PollFrom(spanConsumer),
			Decode: processor.DecodeSpan,
			Write:  writeNewSpans(db),
			Commit: processor.CommitFrom(spanConsumer),
			Wait:   processor.Sleep,
			Logf:   func(format string, args ...any) { log.Printf("spans: "+format, args...) },
		})
	}()
	wg.Wait()
	log.Printf("processor stopped")
}

// writeNewSpans stores only spans not already stored, like writeNew does for points.
func writeNewSpans(db *store.Store) func(ctx context.Context, spans []model.Span) error {
	return func(ctx context.Context, spans []model.Span) error {
		n, err := db.InsertNewSpans(ctx, spans)
		if err != nil {
			return err
		}
		if skipped := len(spans) - n; skipped > 0 {
			log.Printf("skipped %d of %d spans: already stored", skipped, len(spans))
		}
		return nil
	}
}

// writeNew stores only points not already stored (a replay after a crash, or a resend from the
// agent), and says when it skipped any, so dedup at work is visible in the logs.
func writeNew(db *store.Store) func(ctx context.Context, pts []model.Point) error {
	return func(ctx context.Context, pts []model.Point) error {
		n, err := db.InsertNew(ctx, pts)
		if err != nil {
			return err
		}
		if skipped := len(pts) - n; skipped > 0 {
			log.Printf("skipped %d of %d points: already stored", skipped, len(pts))
		}
		return nil
	}
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

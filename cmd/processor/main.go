package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
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

	brokers := strings.Split(envOr("TAPROOT_BROKERS", "localhost:9092"), ",")
	topic := envOr("TAPROOT_TOPIC", "taproot.metrics")
	consumer, err := processor.NewConsumer(brokers, topic)
	if err != nil {
		log.Fatalf("redpanda consumer: %v", err)
	}
	defer consumer.Close() // leaves the group; every written batch is already committed

	log.Printf("processor reading %s at %v into taproot.metrics", topic, brokers)
	processor.Run(ctx, processor.Config{
		Poll:   processor.PollFrom(consumer),
		Write:  writeNew(db),
		Commit: processor.CommitFrom(consumer),
		Wait:   processor.Sleep,
		Logf:   log.Printf,
	})
	log.Printf("processor stopped")
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

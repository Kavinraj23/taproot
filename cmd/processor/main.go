package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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
	defer consumer.Close() // leaves the group; auto-commit makes its final commit here

	log.Printf("processor reading %s at %v into taproot.metrics", topic, brokers)
	processor.Run(ctx, processor.Config{
		Poll:  processor.PollFrom(consumer),
		Write: db.Insert,
		Wait:  processor.Sleep,
		Logf:  log.Printf,
	})
	log.Printf("processor stopped")
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

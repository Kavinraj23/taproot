package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"taproot/internal/agent"
	"taproot/internal/cpu"
	"taproot/internal/sink"
)

func main() {
	host, err := os.Hostname()
	if err != nil {
		log.Fatalf("hostname: %v", err)
	}

	brokers := strings.Split(envOr("TAPROOT_BROKERS", "localhost:9092"), ",")
	topic := envOr("TAPROOT_TOPIC", "taproot.metrics")
	client, err := sink.NewClient(brokers, topic)
	if err != nil {
		log.Fatalf("redpanda client: %v", err)
	}
	out := sink.New(client, log.Printf)
	defer out.Close(5 * time.Second)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	log.Printf("agent on %s sending to %s at %v", host, topic, brokers)
	agent.Run(ctx, ticker.C, agent.Config{
		Read: func() (cpu.Times, error) { return cpu.Read("/proc/stat") },
		Now:  time.Now,
		Host: host,
		Emit: out.Emit,
		Logf: log.Printf,
	})
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"taproot/internal/agent"
	"taproot/internal/cpu"
	"taproot/internal/model"
)

func main() {
	host, err := os.Hostname()
	if err != nil {
		log.Fatalf("hostname: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	// Points go to stdout as JSON lines for now; a Redpanda producer replaces this later.
	enc := json.NewEncoder(os.Stdout)
	agent.Run(ctx, ticker.C, agent.Config{
		Read: func() (cpu.Times, error) { return cpu.Read("/proc/stat") },
		Now:  time.Now,
		Host: host,
		Emit: func(p model.Point) {
			if err := enc.Encode(p); err != nil {
				log.Printf("emit point: %v", err)
			}
		},
		Logf: log.Printf,
	})
}

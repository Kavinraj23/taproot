package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"taproot/internal/agent"
	"taproot/internal/cpu"
	"taproot/internal/discovery"
	"taproot/internal/docker"
	"taproot/internal/sink"
)

func main() {
	scan := flag.Bool("scan", false, "print one /proc process scan as JSON and exit")
	flag.Parse()
	if *scan {
		if err := printScan(envOr("TAPROOT_PROC_ROOT", "/proc")); err != nil {
			log.Fatalf("scan: %v", err)
		}
		return
	}

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

// printScan is a debug hook: it shows what discovery sees from where the agent runs.
func printScan(procRoot string) error {
	procs, err := discovery.Scan(os.DirFS(procRoot))
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	svcs := discovery.Identify(procs)
	// Docker only adds names; without it the scan still works.
	sock := envOr("TAPROOT_DOCKER_SOCKET", "/var/run/docker.sock")
	if cs, err := docker.NewUnixClient(sock).Containers(context.Background()); err != nil {
		log.Printf("docker unavailable, services not labelled with container names: %v", err)
	} else {
		svcs = discovery.Enrich(svcs, cs)
	}
	return enc.Encode(struct {
		Services  []discovery.Service `json:"services"`
		Processes []discovery.Process `json:"processes"`
	}{svcs, procs})
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

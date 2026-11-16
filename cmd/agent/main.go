package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
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
	"taproot/internal/sensor"
	"taproot/internal/sink"
)

func main() {
	scan := flag.Bool("scan", false, "print one discovery round as JSON and exit")
	flag.Parse()

	procFS := os.DirFS(envOr("TAPROOT_PROC_ROOT", "/proc"))
	dockerClient := docker.NewUnixClient(envOr("TAPROOT_DOCKER_SOCKET", "/var/run/docker.sock"))

	if *scan {
		if err := printScan(procFS, dockerClient); err != nil {
			log.Fatalf("scan: %v", err)
		}
		return
	}

	host, err := os.Hostname()
	if err != nil {
		log.Fatalf("hostname: %v", err)
	}
	interval, err := time.ParseDuration(envOr("TAPROOT_DISCOVERY_INTERVAL", "30s"))
	if err != nil {
		log.Fatalf("TAPROOT_DISCOVERY_INTERVAL: %v", err)
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

	sensors := sensor.NewManager(sensor.ManagerConfig{
		Factories: map[string]sensor.Factory{
			"postgres": sensor.NewPostgres(
				envOr("TAPROOT_POSTGRES_USER", "monitoring_user"),
				envOr("TAPROOT_POSTGRES_PASSWORD", "monitoring_password")),
			"valkey": sensor.NewValkey(),
			"kafka":  sensor.NewKafka(),
		},
		Emit: out.Emit,
		Now:  time.Now,
		Host: host,
		Logf: log.Printf,
	})
	defer sensors.Stop()

	go runDiscovery(ctx, procFS, dockerClient, interval, func(c discovery.Changes) {
		logChanges(c)
		sensors.Apply(c)
	})

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

// runDiscovery keeps the service list current: a full rescan every interval, plus an early one
// shortly after Docker reports a container starting or stopping. onChange gets every change.
func runDiscovery(ctx context.Context, procFS fs.FS, dc *docker.Client, interval time.Duration, onChange func(discovery.Changes)) {
	if cs, err := dc.Containers(ctx); err == nil {
		if own := discovery.OwnProject(procFS, cs); own != "" {
			log.Printf("skipping services in own compose project %q", own)
		}
	}

	triggers := make(chan struct{}, 1)
	go discovery.Ring(ctx, dc.Events, triggers, 2*time.Second, log.Printf, time.Second)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	dockerDown := false // log Docker trouble once, not every round
	discovery.Watch(ctx, discovery.WatchConfig{
		Discover: func(ctx context.Context) ([]discovery.Service, error) {
			svcs, _, dockerErr, err := discovery.Discover(ctx, procFS, dc.Containers)
			if dockerErr != nil && !dockerDown {
				log.Printf("docker unavailable, services not labelled with container names: %v", dockerErr)
			}
			dockerDown = dockerErr != nil
			return svcs, err
		},
		Ticks:    ticker.C,
		Triggers: triggers,
		OnChange: onChange,
		Logf:     log.Printf,
	})
}

func logChanges(c discovery.Changes) {
	for _, s := range c.Removed {
		log.Printf("lost    %s", describe(s))
	}
	for _, s := range c.Added {
		log.Printf("found   %s", describe(s))
	}
	for _, s := range c.Updated {
		log.Printf("updated %s", describe(s))
	}
}

func describe(s discovery.Service) string {
	where := s.Container
	if where == "" {
		where = "(no container name)"
	}
	return fmt.Sprintf("%-8s %-14s pid %-6d port %d", s.Kind, where, s.PID, s.Port)
}

// printScan is a debug hook: it shows what discovery sees from where the agent runs.
func printScan(procFS fs.FS, dc *docker.Client) error {
	svcs, procs, dockerErr, err := discovery.Discover(context.Background(), procFS, dc.Containers)
	if err != nil {
		return err
	}
	if dockerErr != nil {
		log.Printf("docker unavailable, services not labelled with container names: %v", dockerErr)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"taproot/internal/agent"
	"taproot/internal/cpu"
	"taproot/internal/discovery"
	"taproot/internal/docker"
	"taproot/internal/model"
	"taproot/internal/otlp"
	"taproot/internal/sensor"
	"taproot/internal/sink"
	"taproot/internal/spool"
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
	bufDir := envOr("TAPROOT_BUFFER_DIR", "/var/lib/taproot/buffer")
	maxMB, err := strconv.Atoi(envOr("TAPROOT_BUFFER_MAX_MB", "256"))
	if err != nil || maxMB <= 0 {
		log.Fatalf("TAPROOT_BUFFER_MAX_MB: want a positive number of megabytes, got %q", os.Getenv("TAPROOT_BUFFER_MAX_MB"))
	}
	buf, err := spool.Open(bufDir, spool.Options{MaxBytes: int64(maxMB) << 20, Logf: log.Printf})
	if err != nil {
		log.Fatalf("disk buffer: %v", err)
	}
	if n := buf.Pending(); n > 0 {
		log.Printf("disk buffer: %d points from before the restart still to send", n)
	}
	out := sink.New(client, buf, log.Printf)
	// Runs after stop(), once Run has seen ctx end. A short wait is enough: anything Redpanda
	// hasn't confirmed stays on disk and is sent on the next start, and a long wait only
	// lengthens the stretch with no measurements while the agent restarts.
	defer out.Close(time.Second)

	// Spans (M4) get their own topic and their own disk buffer, so a flood of one can't push
	// the other out of the buffer. The folder sits inside the metrics buffer's volume; the
	// metrics spool only looks at *.seg files, so it ignores it.
	spanClient, err := sink.NewClient(brokers, spanTopic)
	if err != nil {
		log.Fatalf("redpanda client (spans): %v", err)
	}
	spanDir := envOr("TAPROOT_SPAN_BUFFER_DIR", "/var/lib/taproot/buffer/spans")
	spanMaxMB, err := strconv.Atoi(envOr("TAPROOT_SPAN_BUFFER_MAX_MB", "256"))
	if err != nil || spanMaxMB <= 0 {
		log.Fatalf("TAPROOT_SPAN_BUFFER_MAX_MB: want a positive number of megabytes, got %q", os.Getenv("TAPROOT_SPAN_BUFFER_MAX_MB"))
	}
	spanBuf, err := spool.Open(spanDir, spool.Options{MaxBytes: int64(spanMaxMB) << 20, Logf: log.Printf})
	if err != nil {
		log.Fatalf("span disk buffer: %v", err)
	}
	if n := spanBuf.Pending(); n > 0 {
		log.Printf("span disk buffer: %d spans from before the restart still to send", n)
	}
	spanOut := sink.New(spanClient, spanBuf, log.Printf)
	defer spanOut.Close(time.Second)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go out.Run(ctx)
	go spanOut.Run(ctx)

	otlpAddr := envOr("TAPROOT_OTLP_ADDR", ":4318")
	otlpServer := &http.Server{
		Addr:              otlpAddr,
		Handler:           otlp.Handler(appendSpans(spanOut), log.Printf),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := otlpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("otlp receiver: %v", err)
		}
	}()
	log.Printf("receiving OTLP spans on %s, sending to %s, buffering in %s (cap %d MB)", otlpAddr, spanTopic, spanDir, spanMaxMB)
	// Stop taking spans before the span buffer closes (deferred above, so it runs after this).
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		otlpServer.Shutdown(shutCtx)
	}()

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

	log.Printf("agent on %s sending to %s at %v, buffering in %s (cap %d MB)", host, topic, brokers, bufDir, maxMB)
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

// spanTopic is where the agent sends spans; the processor reads it in task 3.
const spanTopic = "taproot.spans"

// appendSpans puts spans on disk, keyed by trace ID so one trace's spans share a partition.
// It stops at the first failure: the receiver then answers 503, the collector resends the
// whole export, and the spans already written are stored twice and dropped by dedup.
func appendSpans(out *sink.Sink) func([]model.Span) error {
	return func(spans []model.Span) error {
		for _, sp := range spans {
			v, err := json.Marshal(sp)
			if err != nil {
				return fmt.Errorf("encode span %s: %w", sp.Key(), err)
			}
			if err := out.Append([]byte(sp.TraceID), v); err != nil {
				return err
			}
		}
		return nil
	}
}

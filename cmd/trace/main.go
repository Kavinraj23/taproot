// Command trace prints one trace as a tree of who called whom, or lists recent traces of a
// service so there's an ID to look up (M4 task 4, docs/decisions/0008).
//
//	go run ./cmd/trace 2b98f06f4b6ba8546967222aa20acffe
//	go run ./cmd/trace -service checkout [-since 15m] [-n 10]
//
// Exit codes: 0 found, 1 not found or a ClickHouse error, 2 bad usage.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"taproot/internal/model"
	"taproot/internal/store"
	"taproot/internal/trace"
)

func main() {
	addr := flag.String("clickhouse", envOr("TAPROOT_CLICKHOUSE_ADDR", "localhost:9000"), "ClickHouse native address")
	service := flag.String("service", "", "list recent traces that include this service instead of printing one")
	since := flag.Duration("since", 15*time.Minute, "with -service: how far back to look")
	n := flag.Int("n", 10, "with -service: how many traces to list")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: trace [-clickhouse addr] <trace-id>\n       trace -service <name> [-since 15m] [-n 10]")
		flag.PrintDefaults()
	}
	flag.Parse()

	var traceID string
	switch {
	case *service != "" && flag.NArg() == 0:
	case *service == "" && flag.NArg() == 1:
		// Pasted IDs may come uppercase (some UIs show them that way); stored ones are lowercase.
		traceID = strings.ToLower(strings.TrimSpace(flag.Arg(0)))
		if !model.ValidTraceID(traceID) {
			fmt.Fprintf(os.Stderr, "%q is not a trace ID: want 32 hex digits, not all zeros\n", flag.Arg(0))
			os.Exit(2)
		}
	default:
		flag.Usage()
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := store.Open(ctx, store.Config{
		Addr:     *addr,
		User:     envOr("TAPROOT_CLICKHOUSE_USER", "taproot"),
		Password: envOr("TAPROOT_CLICKHOUSE_PASSWORD", "taproot"),
		Database: "taproot",
		Table:    "metrics",
	})
	if err != nil {
		fail(err)
	}
	defer db.Close()

	if *service != "" {
		list(ctx, db, *service, *since, *n)
		return
	}
	spans, err := db.Trace(ctx, traceID)
	if errors.Is(err, store.ErrTraceNotFound) {
		fmt.Fprintf(os.Stderr, "trace %s not found (spans are kept 48 h, and can take ~10 s to arrive)\n", traceID)
		os.Exit(1)
	}
	if err != nil {
		fail(err)
	}
	trace.Render(os.Stdout, traceID, spans, trace.Build(spans))
}

func list(ctx context.Context, db *store.Store, service string, since time.Duration, n int) {
	traces, err := db.RecentTraces(ctx, service, since, n)
	if err != nil {
		fail(err)
	}
	if len(traces) == 0 {
		fmt.Fprintf(os.Stderr, "no traces with a %s span in the last %v\n", service, since)
		os.Exit(1)
	}
	fmt.Printf("newest traces with a %s span (last %v):\n", service, since)
	for _, t := range traces {
		fmt.Printf("  %s  %s  %s\n", t.Start.UTC().Format("15:04:05.000"), t.TraceID, t.Name)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

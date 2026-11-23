// Command losscheck reports whether every point in Redpanda reached ClickHouse exactly once
// over a recent window, and whether any every-tick series went silent; and the same "exactly
// once" for spans that started in the window (M4). Exit 0 means all clear.
//
//	go run ./cmd/losscheck -window 10m
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"taproot/internal/losscheck"
	"taproot/internal/model"
	"taproot/internal/processor"
)

func main() {
	window := flag.Duration("window", 10*time.Minute, "how far back to check")
	settle := flag.Duration("settle", 30*time.Second, "skip the newest stretch, which may still be in flight")
	gap := flag.Duration("gap", 5*time.Second, "silence longer than this in an every-tick series is a gap (the VM clock jumps 2-4s)")
	brokers := flag.String("brokers", "localhost:9092", "Redpanda brokers, comma separated")
	topic := flag.String("topic", "taproot.metrics", "topic the agent produces to")
	chAddr := flag.String("clickhouse", "localhost:9000", "ClickHouse native address")
	table := flag.String("table", "taproot.metrics", "raw metrics table")
	spanTopic := flag.String("span-topic", "taproot.spans", "topic the agent produces spans to; empty skips the span check")
	spanTable := flag.String("span-table", "taproot.spans", "spans table")
	after := flag.String("after", "", "start no earlier than this RFC 3339 time (e.g. a minute after the agent started, to skip its startup)")
	flag.Parse()
	var notBefore time.Time
	if *after != "" {
		t, err := time.Parse(time.RFC3339Nano, *after)
		if err != nil {
			log.Fatalf("-after: %v", err)
		}
		notBefore = t
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ch, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{*chAddr},
		Auth: clickhouse.Auth{Username: "taproot", Password: "taproot"},
	})
	if err != nil {
		log.Fatalf("clickhouse: %v", err)
	}
	defer ch.Close()

	// The window ends at the newest stored point minus settle: ClickHouse's ts is the Docker VM
	// clock the agent stamps with, and the Windows clock can disagree with it.
	var newest time.Time
	if err := ch.QueryRow(ctx, "SELECT max(ts) FROM "+*table).Scan(&newest); err != nil {
		log.Fatalf("newest point: %v", err)
	}
	// Whole seconds: clickhouse-go binds a time.Time parameter at second precision, so a
	// fractional bound would make the two sides cut the window at slightly different places.
	to := newest.Add(-*settle).Truncate(time.Second)
	from := to.Add(-*window).Truncate(time.Second)
	if notBefore.After(from) {
		from = notBefore.Truncate(time.Second).Add(time.Second)
	}
	if !from.Before(to) {
		fmt.Printf("nothing to check yet: the window starts at %s UTC, after its end %s\n", from.Format("15:04:05"), to.Format("15:04:05"))
		return
	}

	sent, err := readTopic(ctx, strings.Split(*brokers, ","), *topic, from, to)
	if err != nil {
		log.Fatalf("redpanda: %v", err)
	}
	stored, err := readTable(ctx, ch, *table, from, to)
	if err != nil {
		log.Fatalf("clickhouse: %v", err)
	}

	r := losscheck.Report{From: from, To: to, Sent: distinct(sent), Stored: len(stored)}
	r.Missing, r.Duplicates, r.MissingSample = losscheck.Compare(sent, stored)
	r.Gaps = losscheck.Gaps(heartbeats(stored), *gap)

	checkSpans := *spanTopic != ""
	if checkSpans {
		sentSpans, err := readSpanTopic(ctx, strings.Split(*brokers, ","), *spanTopic, from, to)
		if err != nil {
			log.Fatalf("redpanda (spans): %v", err)
		}
		storedSpans, err := readSpanTable(ctx, ch, *spanTable, from, to)
		if err != nil {
			log.Fatalf("clickhouse (spans): %v", err)
		}
		r.SpansSent, r.SpansStored = distinctKeys(sentSpans), len(storedSpans)
		r.SpansMissing, r.SpanDuplicates, r.SpanMissingSample = losscheck.CompareKeys(sentSpans, storedSpans)
	}
	print(r, *gap, checkSpans)
	if !r.OK() {
		os.Exit(1)
	}
}

// readTopic returns the points in topic whose timestamps fall in [from, to].
func readTopic(ctx context.Context, brokers []string, topic string, from, to time.Time) ([]model.Point, error) {
	var pts []model.Point
	err := eachRecord(ctx, brokers, topic, from, func(rec *kgo.Record) {
		p, err := processor.Decode(rec)
		if err != nil {
			return // the processor skips these too
		}
		if !p.Timestamp.Before(from) && !p.Timestamp.After(to) {
			pts = append(pts, p)
		}
	})
	return pts, err
}

// readSpanTopic returns the keys of the spans in topic that started in [from, to]. A span is
// produced after it ends, so its record time is never before its start.
func readSpanTopic(ctx context.Context, brokers []string, topic string, from, to time.Time) ([]string, error) {
	var keys []string
	err := eachRecord(ctx, brokers, topic, from, func(rec *kgo.Record) {
		sp, err := processor.DecodeSpan(rec)
		if err != nil {
			return // the processor skips these too
		}
		if !sp.Start.Before(from) && !sp.Start.After(to) {
			keys = append(keys, sp.Key())
		}
	})
	return keys, err
}

// eachRecord calls fn for every record in topic from a minute before from by record time
// (records are produced just after what they carry happened) up to each partition's end
// offset as of now.
func eachRecord(ctx context.Context, brokers []string, topic string, from time.Time, fn func(*kgo.Record)) error {
	admCl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer admCl.Close()
	adm := kadm.NewClient(admCl)

	starts, err := adm.ListOffsetsAfterMilli(ctx, from.Add(-time.Minute).UnixMilli(), topic)
	if err == nil {
		err = starts.Error()
	}
	if err != nil {
		return fmt.Errorf("start offsets: %w", err)
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return fmt.Errorf("end offsets: %w", err)
	}

	begin := map[int32]kgo.Offset{}
	stop := map[int32]int64{} // partition -> first offset not to read
	starts.Each(func(o kadm.ListedOffset) {
		end, _ := ends.Lookup(o.Topic, o.Partition)
		if o.Offset < end.Offset {
			begin[o.Partition] = kgo.NewOffset().At(o.Offset)
			stop[o.Partition] = end.Offset
		}
	})
	if len(begin) == 0 {
		return nil
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: begin}))
	if err != nil {
		return err
	}
	defer cl.Close()

	for len(stop) > 0 {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("reading %d partitions: %w", len(stop), err)
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("fetch %s/%d: %w", errs[0].Topic, errs[0].Partition, errs[0].Err)
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			end, ok := stop[rec.Partition]
			if !ok || rec.Offset >= end {
				return
			}
			if rec.Offset == end-1 {
				delete(stop, rec.Partition)
			}
			fn(rec)
		})
	}
	return nil
}

// readSpanTable returns each stored span that started in [from, to] with its number of copies.
func readSpanTable(ctx context.Context, ch driver.Conn, table string, from, to time.Time) (map[string]uint64, error) {
	rows, err := ch.Query(ctx, `SELECT trace_id, span_id, count() FROM `+table+
		` WHERE start BETWEEN ? AND ? GROUP BY trace_id, span_id`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]uint64{}
	for rows.Next() {
		var sp model.Span
		var n uint64
		if err := rows.Scan(&sp.TraceID, &sp.SpanID, &n); err != nil {
			return nil, err
		}
		out[sp.Key()] = n
	}
	return out, rows.Err()
}

func distinctKeys(keys []string) int {
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		seen[k] = true
	}
	return len(seen)
}

// readTable returns one row per distinct point in [from, to], with its number of copies.
// Labels are sorted because a Map keeps insertion order and Go maps iterate randomly.
func readTable(ctx context.Context, ch driver.Conn, table string, from, to time.Time) ([]losscheck.Stored, error) {
	rows, err := ch.Query(ctx, `SELECT metric, mapSort(labels) AS l, ts, count()
FROM `+table+` WHERE ts BETWEEN ? AND ? GROUP BY metric, l, ts`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []losscheck.Stored
	for rows.Next() {
		var s losscheck.Stored
		if err := rows.Scan(&s.Metric, &s.Labels, &s.TS, &s.Copies); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// heartbeats picks the series emitted every tick no matter what (host CPU and every sensor's
// _up), so a hole in them means lost points, not a sensor with nothing to say.
func heartbeats(stored []losscheck.Stored) map[string][]time.Time {
	out := map[string][]time.Time{}
	for _, s := range stored {
		if s.Metric != "host_cpu_usage_percent" && !strings.HasSuffix(s.Metric, "_up") {
			continue
		}
		k := losscheck.Series(s.Metric, s.Labels)
		out[k] = append(out[k], s.TS)
	}
	return out
}

func distinct(pts []model.Point) int {
	seen := map[string]bool{}
	for _, p := range pts {
		seen[losscheck.ID(p.Metric, p.Labels, p.Timestamp)] = true
	}
	return len(seen)
}

func print(r losscheck.Report, gap time.Duration, spans bool) {
	verdict := func(ok bool) string {
		if ok {
			return "PASS"
		}
		return "FAIL"
	}
	fmt.Printf("window  %s -> %s UTC (%v)\n", r.From.Format("15:04:05"), r.To.Format("15:04:05"), r.To.Sub(r.From).Round(time.Second))
	fmt.Printf("points  %d in Redpanda, %d in ClickHouse\n", r.Sent, r.Stored)
	fmt.Printf("%s  L1 missing: %d in Redpanda but not ClickHouse\n", verdict(r.Missing == 0), r.Missing)
	for _, s := range r.MissingSample {
		fmt.Printf("        e.g. %s\n", s)
	}
	fmt.Printf("%s  L2 duplicates: %d extra copies in ClickHouse\n", verdict(r.Duplicates == 0), r.Duplicates)
	fmt.Printf("%s  L3 gaps over %v in heartbeat series: %d\n", verdict(len(r.Gaps) == 0), gap, len(r.Gaps))
	for _, g := range r.Gaps {
		fmt.Printf("        %s\n", g)
	}
	if !spans {
		return
	}
	fmt.Printf("spans   %d in Redpanda, %d in ClickHouse (by start time)\n", r.SpansSent, r.SpansStored)
	fmt.Printf("%s  S1 spans missing: %d in Redpanda but not ClickHouse\n", verdict(r.SpansMissing == 0), r.SpansMissing)
	for _, s := range r.SpanMissingSample {
		fmt.Printf("        e.g. %s\n", s)
	}
	fmt.Printf("%s  S2 span duplicates: %d extra copies in ClickHouse\n", verdict(r.SpanDuplicates == 0), r.SpanDuplicates)
}

//go:build integration

package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"taproot/internal/model"
)

// Run with: TAPROOT_BROKERS=localhost:9092 go test -tags integration ./internal/sink/
func TestProduceToRedpanda(t *testing.T) {
	brokers := os.Getenv("TAPROOT_BROKERS")
	if brokers == "" {
		t.Skip("TAPROOT_BROKERS not set")
	}
	seeds := strings.Split(brokers, ",")
	topic := fmt.Sprintf("taproot-it-%d", time.Now().UnixNano())

	// Auto-creation keeps the test self-contained; the agent itself relies on redpanda-init.
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.DefaultProduceTopic(topic), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	s := New(cl, t.Logf)

	base := time.Date(2026, 11, 11, 9, 30, 0, 0, time.UTC)
	var want []model.Point
	for i := 0; i < 3; i++ {
		p := model.Point{
			Metric:    "host_cpu_usage_percent",
			Labels:    map[string]string{"host": "it"},
			Timestamp: base.Add(time.Duration(i) * time.Second),
			Value:     float64(10 * i),
		}
		want = append(want, p)
		s.Emit(p)
	}
	s.Close(10 * time.Second)
	if s.dropped != 0 {
		t.Fatalf("dropped %d points", s.dropped)
	}

	consumer, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var got []*kgo.Record
	for len(got) < len(want) {
		fetches := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("got %d of %d records before timeout", len(got), len(want))
		}
		fetches.EachError(func(_ string, _ int32, err error) { t.Fatalf("fetch: %v", err) })
		got = append(got, fetches.Records()...)
	}

	// One series, one key, so all three share a partition and arrive in order.
	for i, r := range got {
		if string(r.Key) != "host_cpu_usage_percent{host=it}" {
			t.Errorf("record %d key = %q", i, r.Key)
		}
		var p model.Point
		if err := json.Unmarshal(r.Value, &p); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if !p.Timestamp.Equal(want[i].Timestamp) || p.Value != want[i].Value {
			t.Errorf("record %d = %+v, want %+v", i, p, want[i])
		}
	}
}

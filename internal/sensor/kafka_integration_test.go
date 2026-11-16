//go:build integration

package sensor

import (
	"context"
	"os"
	"testing"
	"time"

	"taproot/internal/discovery"
)

// Runs against Taproot's own Redpanda (Kafka-compatible), which already has the
// taproot.metrics topic and the taproot-processor group:
//
//	TAPROOT_KAFKA_TEST_ADDR=localhost:9092 go test -tags integration ./internal/sensor/
func kafkaTestAddr(t *testing.T) string {
	addr := os.Getenv("TAPROOT_KAFKA_TEST_ADDR")
	if addr == "" {
		t.Skip("TAPROOT_KAFKA_TEST_ADDR not set")
	}
	return addr
}

func TestKafkaCollectAgainstRealBroker(t *testing.T) {
	c, err := NewKafka()(discovery.Service{Kind: "kafka"}, kafkaTestAddr(t))
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rs, err := c.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if got := findReading(rs, "kafka_brokers", nil); len(got) != 1 || got[0].Value < 1 {
		t.Errorf("kafka_brokers = %+v, want >= 1", got)
	}
	if got := findReading(rs, "kafka_topic_messages", topicL("taproot.metrics")); len(got) != 1 || got[0].Value <= 0 {
		t.Errorf("kafka_topic_messages{taproot.metrics} = %+v, want > 0", got)
	}
	if got := findReading(rs, "kafka_consumergroup_lag", groupTopicL("taproot-processor", "taproot.metrics")); len(got) != 1 || got[0].Value < 0 {
		t.Errorf("lag{taproot-processor, taproot.metrics} = %+v, want one >= 0", got)
	}

	// The client is kept between ticks.
	if _, err := c.Collect(ctx); err != nil {
		t.Errorf("second Collect: %v", err)
	}
}

func TestKafkaNothingListening(t *testing.T) {
	kafkaTestAddr(t)
	c, err := NewKafka()(discovery.Service{Kind: "kafka"}, "localhost:1")
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if _, err := c.Collect(ctx); err == nil {
		t.Error("Collect with nothing listening succeeded")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("failed Collect took %v", d)
	}
}

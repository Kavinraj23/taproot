package sensor

import (
	"context"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"taproot/internal/discovery"
)

// kafkaStats is one round of numbers read from Kafka, before they become readings. Offsets are
// per topic, then per partition.
type kafkaStats struct {
	brokers   int
	topicEnds map[string]map[int32]int64 // end of each partition: messages ever written
	groups    []kafkaGroup
}

// kafkaGroup is one consumer group: how many consumers it has and where its bookmarks are.
type kafkaGroup struct {
	name      string
	members   int
	committed map[string]map[int32]int64 // only partitions the group has committed
}

// Kafka's own topics (__consumer_offsets, ...) start with "__".
func isInternalTopic(t string) bool { return strings.HasPrefix(t, "__") }

// kafkaReadings maps one round of stats to readings. Partitions are summed per topic. Lag counts
// only partitions the group has committed, so a new group doesn't show a fake backlog of the
// whole topic.
func kafkaReadings(s kafkaStats) []Reading {
	rs := []Reading{{Metric: "kafka_brokers", Kind: Gauge, Value: float64(s.brokers)}}
	for topic, parts := range s.topicEnds {
		if isInternalTopic(topic) {
			continue
		}
		var total int64
		for _, end := range parts {
			total += end
		}
		rs = append(rs, Reading{Metric: "kafka_topic_messages", Kind: Counter, Value: float64(total),
			Labels: map[string]string{"topic": topic}})
	}
	for _, g := range s.groups {
		rs = append(rs, Reading{Metric: "kafka_consumergroup_members", Kind: Gauge, Value: float64(g.members),
			Labels: map[string]string{"group": g.name}})
		for topic, parts := range g.committed {
			ends, ok := s.topicEnds[topic]
			if isInternalTopic(topic) || !ok || len(parts) == 0 {
				continue
			}
			var lag, consumed int64
			for p, at := range parts {
				consumed += at
				if end, ok := ends[p]; ok && end > at { // a commit seen ahead of the end is no lag
					lag += end - at
				}
			}
			labels := map[string]string{"group": g.name, "topic": topic}
			rs = append(rs,
				Reading{Metric: "kafka_consumergroup_lag", Kind: Gauge, Value: float64(lag), Labels: labels},
				Reading{Metric: "kafka_consumergroup_consumed", Kind: Counter, Value: float64(consumed), Labels: labels},
			)
		}
	}
	return rs
}

// NewKafka returns a Factory for Kafka collectors. No login: the demo's Kafka is plaintext.
func NewKafka() Factory {
	return func(_ discovery.Service, addr string) (Collector, error) {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(addr),
			kgo.DialTimeout(2*time.Second),
		)
		if err != nil {
			return nil, err
		}
		return &kafkaCollector{cl: cl, adm: kadm.NewClient(cl)}, nil
	}
}

// kafkaCollector keeps one client for its life. The client reconnects to brokers by itself, so
// unlike the Postgres and Valkey collectors nothing is torn down after an error.
type kafkaCollector struct {
	cl  *kgo.Client
	adm *kadm.Client
}

func (c *kafkaCollector) Collect(ctx context.Context) ([]Reading, error) {
	s, err := c.stats(ctx)
	if err != nil {
		return nil, err
	}
	return kafkaReadings(s), nil
}

// stats asks Kafka for brokers and topics, the end of every topic, and every group's members and
// committed offsets. A broker that can't be reached is an error; one group failing to be described
// or fetched only skips that group this tick.
func (c *kafkaCollector) stats(ctx context.Context) (kafkaStats, error) {
	s := kafkaStats{topicEnds: map[string]map[int32]int64{}}

	md, err := c.adm.Metadata(ctx)
	if err != nil {
		return s, err
	}
	s.brokers = len(md.Brokers)
	var topics []string
	for _, t := range md.Topics {
		if !t.IsInternal && !isInternalTopic(t.Topic) {
			topics = append(topics, t.Topic)
		}
	}

	if len(topics) > 0 {
		ends, err := c.adm.ListEndOffsets(ctx, topics...)
		if err != nil {
			return s, err
		}
		ends.Each(func(o kadm.ListedOffset) {
			if o.Err != nil || o.Offset < 0 {
				return
			}
			if s.topicEnds[o.Topic] == nil {
				s.topicEnds[o.Topic] = map[int32]int64{}
			}
			s.topicEnds[o.Topic][o.Partition] = o.Offset
		})
	}

	described, err := c.adm.DescribeGroups(ctx) // no names: every (classic) group
	if err != nil {
		return s, err
	}
	var names []string
	for name, g := range described {
		if g.Err == nil {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return s, nil
	}
	fetched := c.adm.FetchManyOffsets(ctx, names...)
	for _, name := range names {
		f, ok := fetched[name]
		if !ok || f.Err != nil {
			continue
		}
		g := kafkaGroup{name: name, members: len(described[name].Members), committed: map[string]map[int32]int64{}}
		f.Fetched.Each(func(o kadm.OffsetResponse) {
			if o.Err != nil || o.At < 0 {
				return
			}
			if g.committed[o.Topic] == nil {
				g.committed[o.Topic] = map[int32]int64{}
			}
			g.committed[o.Topic][o.Partition] = o.At
		})
		s.groups = append(s.groups, g)
	}
	return s, nil
}

func (c *kafkaCollector) Close() error {
	c.cl.Close()
	return nil
}

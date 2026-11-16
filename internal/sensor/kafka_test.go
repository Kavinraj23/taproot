package sensor

import (
	"maps"
	"testing"
)

// findReading returns the readings for metric whose labels are exactly labels.
func findReading(rs []Reading, metric string, labels map[string]string) []Reading {
	var out []Reading
	for _, r := range rs {
		if r.Metric == metric && maps.Equal(r.Labels, labels) {
			out = append(out, r)
		}
	}
	return out
}

// wantOne checks there is exactly one reading for metric+labels with the given kind and value.
func wantOne(t *testing.T, rs []Reading, metric string, labels map[string]string, kind Kind, value float64) {
	t.Helper()
	got := findReading(rs, metric, labels)
	if len(got) != 1 || got[0].Kind != kind || got[0].Value != value {
		t.Errorf("%s%v = %+v, want one %v of %v", metric, labels, got, kind, value)
	}
}

func topicL(topic string) map[string]string { return map[string]string{"topic": topic} }
func groupL(group string) map[string]string { return map[string]string{"group": group} }
func groupTopicL(group, topic string) map[string]string {
	return map[string]string{"group": group, "topic": topic}
}

func TestKafkaReadingsOneTopicOneGroup(t *testing.T) {
	rs := kafkaReadings(kafkaStats{
		brokers:   1,
		topicEnds: map[string]map[int32]int64{"orders": {0: 944}},
		groups: []kafkaGroup{{
			name: "fraud-detection", members: 1,
			committed: map[string]map[int32]int64{"orders": {0: 940}},
		}},
	})
	wantOne(t, rs, "kafka_brokers", nil, Gauge, 1)
	wantOne(t, rs, "kafka_topic_messages", topicL("orders"), Counter, 944)
	wantOne(t, rs, "kafka_consumergroup_lag", groupTopicL("fraud-detection", "orders"), Gauge, 4)
	wantOne(t, rs, "kafka_consumergroup_consumed", groupTopicL("fraud-detection", "orders"), Counter, 940)
	wantOne(t, rs, "kafka_consumergroup_members", groupL("fraud-detection"), Gauge, 1)
}

func TestKafkaReadingsSumsPartitions(t *testing.T) {
	rs := kafkaReadings(kafkaStats{
		brokers:   1,
		topicEnds: map[string]map[int32]int64{"orders": {0: 10, 1: 20, 2: 30}},
		groups: []kafkaGroup{{
			name: "g", members: 3,
			committed: map[string]map[int32]int64{"orders": {0: 8, 1: 20, 2: 25}},
		}},
	})
	wantOne(t, rs, "kafka_topic_messages", topicL("orders"), Counter, 60)
	wantOne(t, rs, "kafka_consumergroup_lag", groupTopicL("g", "orders"), Gauge, 7)
	wantOne(t, rs, "kafka_consumergroup_consumed", groupTopicL("g", "orders"), Counter, 53)
}

func TestKafkaReadingsTwoGroupsSameTopic(t *testing.T) {
	rs := kafkaReadings(kafkaStats{
		brokers:   1,
		topicEnds: map[string]map[int32]int64{"orders": {0: 100}},
		groups: []kafkaGroup{
			{name: "accounting", members: 1, committed: map[string]map[int32]int64{"orders": {0: 100}}},
			{name: "fraud-detection", members: 1, committed: map[string]map[int32]int64{"orders": {0: 70}}},
		},
	})
	wantOne(t, rs, "kafka_consumergroup_lag", groupTopicL("accounting", "orders"), Gauge, 0)
	wantOne(t, rs, "kafka_consumergroup_lag", groupTopicL("fraud-detection", "orders"), Gauge, 30)
}

func TestKafkaReadingsUncommittedPartitionsLeftOut(t *testing.T) {
	rs := kafkaReadings(kafkaStats{
		brokers:   1,
		topicEnds: map[string]map[int32]int64{"orders": {0: 10, 1: 5000}},
		groups: []kafkaGroup{
			// Partition 1 never committed: not counted as a 5000-message backlog.
			{name: "partial", members: 1, committed: map[string]map[int32]int64{"orders": {0: 9}}},
			// No commits at all: members only.
			{name: "fresh", members: 2},
		},
	})
	wantOne(t, rs, "kafka_consumergroup_lag", groupTopicL("partial", "orders"), Gauge, 1)
	wantOne(t, rs, "kafka_consumergroup_members", groupL("fresh"), Gauge, 2)
	for _, m := range []string{"kafka_consumergroup_lag", "kafka_consumergroup_consumed"} {
		if got := findReading(rs, m, groupTopicL("fresh", "orders")); len(got) != 0 {
			t.Errorf("%s for a group with no commits = %+v, want none", m, got)
		}
	}
}

func TestKafkaReadingsCommitAheadOfEnd(t *testing.T) {
	rs := kafkaReadings(kafkaStats{
		brokers:   1,
		topicEnds: map[string]map[int32]int64{"orders": {0: 100}},
		groups:    []kafkaGroup{{name: "g", members: 1, committed: map[string]map[int32]int64{"orders": {0: 103}}}},
	})
	wantOne(t, rs, "kafka_consumergroup_lag", groupTopicL("g", "orders"), Gauge, 0)
}

func TestKafkaReadingsSkipsInternalTopics(t *testing.T) {
	rs := kafkaReadings(kafkaStats{
		brokers:   1,
		topicEnds: map[string]map[int32]int64{"__consumer_offsets": {0: 500}, "orders": {0: 1}},
		groups: []kafkaGroup{{name: "g", members: 1,
			committed: map[string]map[int32]int64{"__consumer_offsets": {0: 400}}}},
	})
	for _, r := range rs {
		if r.Labels["topic"] == "__consumer_offsets" {
			t.Errorf("reading for an internal topic: %+v", r)
		}
	}
}

func TestKafkaReadingsGroupWithNoMembers(t *testing.T) {
	// The consumers are gone but their bookmarks remain: lag keeps being reported.
	rs := kafkaReadings(kafkaStats{
		brokers:   1,
		topicEnds: map[string]map[int32]int64{"orders": {0: 50}},
		groups:    []kafkaGroup{{name: "g", members: 0, committed: map[string]map[int32]int64{"orders": {0: 20}}}},
	})
	wantOne(t, rs, "kafka_consumergroup_members", groupL("g"), Gauge, 0)
	wantOne(t, rs, "kafka_consumergroup_lag", groupTopicL("g", "orders"), Gauge, 30)
}

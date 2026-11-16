package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestPointJSON(t *testing.T) {
	p := Point{
		Metric:    "host_cpu_usage_percent",
		Labels:    map[string]string{"host": "my-laptop"},
		Timestamp: time.Date(2026, 11, 8, 14, 3, 1, 250_000_000, time.UTC),
		Value:     22.39,
	}
	got, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"metric":"host_cpu_usage_percent","labels":{"host":"my-laptop"},"ts":"2026-11-08T14:03:01.25Z","value":22.39}`
	if string(got) != want {
		t.Errorf("JSON = %s\nwant   %s", got, want)
	}
}

// Key: one point's identity for dedup (M3, docs/decisions/0007).
func TestKey(t *testing.T) {
	ts := time.Date(2026, 11, 16, 12, 0, 0, 5, time.UTC)
	p := func(metric string, labels map[string]string, ts time.Time) Point {
		return Point{Metric: metric, Labels: labels, Timestamp: ts, Value: 1}
	}
	same := []struct {
		name string
		a, b Point
	}{
		{"label order", p("m", map[string]string{"x": "1", "y": "2"}, ts), p("m", map[string]string{"y": "2", "x": "1"}, ts)},
		{"nil vs empty labels", p("m", nil, ts), p("m", map[string]string{}, ts)},
		{"value ignored", p("m", nil, ts), Point{Metric: "m", Timestamp: ts, Value: 99}},
		{"time zone ignored", p("m", nil, ts), p("m", nil, ts.In(time.FixedZone("EST", -5*3600)))},
	}
	for _, c := range same {
		if c.a.Key() != c.b.Key() {
			t.Errorf("%s: %q != %q, want equal", c.name, c.a.Key(), c.b.Key())
		}
	}
	differ := []struct {
		name string
		a, b Point
	}{
		{"value looks like two labels", p("m", map[string]string{"a": "1,b=2"}, ts), p("m", map[string]string{"a": "1", "b": "2"}, ts)},
		{"label value with quote", p("m", map[string]string{"a": `1",b="2`}, ts), p("m", map[string]string{"a": "1", "b": "2"}, ts)},
		{"1ns apart", p("m", nil, ts), p("m", nil, ts.Add(time.Nanosecond))},
		{"different metric", p("m", nil, ts), p("n", nil, ts)},
		{"metric looks like labels", p(`m{a="1"}`, nil, ts), p("m", map[string]string{"a": "1"}, ts)},
	}
	for _, c := range differ {
		if c.a.Key() == c.b.Key() {
			t.Errorf("%s: both %q, want different", c.name, c.a.Key())
		}
	}
}

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

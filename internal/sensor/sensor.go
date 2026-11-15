// Package sensor measures discovered services once a second. A Manager runs one collector per
// service instance, started and stopped as discovery reports changes; each kind of service
// (Postgres, Valkey, Kafka) supplies a Factory that knows what to measure.
package sensor

import (
	"context"

	"taproot/internal/discovery"
)

// Kind says how a reading is turned into a point.
type Kind int

const (
	// Gauge is a value as it is right now (connections open). Sent as is.
	Gauge Kind = iota
	// Counter is a running total (commits since start). Sent as its per-second rate, under
	// the metric name plus "_per_sec".
	Counter
)

func (k Kind) String() string {
	if k == Counter {
		return "Counter"
	}
	return "Gauge"
}

// Reading is one measurement from a collector.
type Reading struct {
	Metric string
	Kind   Kind
	Value  float64
	Labels map[string]string // extra labels, e.g. {"state": "active"}; may be nil
}

// Collector measures one service instance.
type Collector interface {
	Collect(ctx context.Context) ([]Reading, error)
	Close() error
}

// Factory makes a Collector for a discovered service, reachable at addr ("host:port").
type Factory func(svc discovery.Service, addr string) (Collector, error)

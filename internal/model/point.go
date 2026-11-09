package model

import "time"

// Point is one metric measurement. Metric, Labels and Timestamp together identify it,
// so a resent copy of the same point can be recognised as a duplicate.
type Point struct {
	Metric    string            `json:"metric"`
	Labels    map[string]string `json:"labels"`
	Timestamp time.Time         `json:"ts"`
	Value     float64           `json:"value"`
}

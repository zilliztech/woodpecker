package wal

import (
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

type (
	Description = evidence.Description
	Measurement = evidence.Measurement
	TrendPoint  = evidence.TrendPoint
	Curve       = evidence.Curve
	Inspection  = evidence.Inspection
)

func Describe(d Definition, p Policy) Description     { return evidence.Describe(d, p) }
func Descriptions(p Policy) map[string]Description    { return evidence.Descriptions(p) }
func Inspect(r Report, id string) (Inspection, error) { return evidence.Inspect(r, id) }

func rawCurve(d Definition, s prometheus.Series, p Policy) Curve { return evidence.RawCurve(d, s, p) }
func resourceAnomalies(d Definition, s prometheus.Series, p Policy) []Measurement {
	return evidence.ResourceAnomalies(d, s, p)
}

package prometheus

import (
	"fmt"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

// CounterFact is an unextrapolated first/last delta over complete raw scrapes.
// It does not imply payload rows, committed writes, demand or reader identity.
type CounterFact struct {
	Increase  float64 `json:"increase"`
	PerSecond float64 `json:"per_second"`
	Samples   []Point `json:"retained_samples"`
}

func evaluateCounter(data *Dataset, sig signal, r d.Request, p Policy, id string) []d.Finding {
	out := []d.Finding{}
	for _, s := range data.Series {
		labels := map[string]string{}
		for k, v := range s.AllLabels {
			if k != "__name__" {
				labels[k] = v
			}
		}
		pop := fingerprint(labels)
		f := finding(sig, r, id, pop, s.Labels)
		f.RuleID = sig.ID + ".counter"
		f.Unknown = []string{"retained_counter_delta_not_request_specific", "events_between_scrapes_unobservable"}
		if code := quality(s.Points, data.From, data.To, p.MaxGapMS, true); code != "" {
			f.Summary = "Counter temporal/reset coverage is incomplete"
			f.Unknown = append(f.Unknown, code)
			out = append(out, f)
			continue
		}
		points := s.Points
		first, last := points[0], points[len(points)-1]
		increase := last.Value - first.Value
		rate := increase / (float64(last.TimeMS-first.TimeMS) / 1000)
		data.Facts = append(data.Facts, Fact{Population: pop, Labels: s.Labels, ObservedFromMS: first.TimeMS, ObservedToMS: last.TimeMS, Counter: &CounterFact{Increase: increase, PerSecond: rate, Samples: append([]Point{}, points...)}})
		f.Outcome = "counter_increase_observed"
		if increase == 0 {
			f.Outcome = "idle_or_no_events"
		}
		f.Strength = "observed_counter_delta"
		f.Severity = "unknown"
		f.Summary = fmt.Sprintf("%s: observed increase %.3f, %.3f/s over actual retained scrapes (%s); no causal inference", sig.Metric, increase, rate, sig.GaugeUnit)
		out = append(out, f)
	}
	return out
}

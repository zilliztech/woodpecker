package evidence

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

func Evaluate(d Definition, keys []string, data map[string]prometheus.Dataset, p Policy, from, to time.Time) Evidence {
	empty := unknown(d.ID, "no_matching_series", "No usable source series for this point; absence is not zero")
	if len(keys) == 0 {
		return empty
	}
	sets := []prometheus.Dataset{}
	for _, key := range keys {
		x, ok := data[key]
		if !ok {
			return unknown(d.ID, "query_unavailable", "A required source query was unavailable")
		}
		sets = append(sets, x)
	}
	if d.ID == "R10" {
		return readBranch(d, sets, p)
	}
	for _, x := range sets {
		if len(x.Series) == 0 {
			return empty
		}
	}
	if d.ID == "S06" {
		return diskState(d, sets, p)
	}
	var facts []prometheus.WALFact
	threshold := p.LatencyMS
	if d.Unit == "seconds" {
		threshold = p.LatencyMS / 1000
	}
	if v, ok := p.Thresholds[d.ID]; ok {
		threshold = v
	}
	switch d.Kind {
	case "p99":
		facts = prometheus.WALHistogramFactsInWindow(sets[0], d.Metrics[0], .99, threshold, p.MinObservations, p.MaxGapMS, p.CalculationWindowMS)
	case "position":
		if len(sets) != 2 {
			return empty
		}
		facts = prometheus.WALPositionFacts(sets[0], sets[1], p.MaxGapMS)
	case "rate", "increase", "errors", "trend", "gauge", "io_wait_rate":
		facts = prometheus.WALSimpleFacts(sets[0], d.Kind == "rate" || d.Kind == "increase" || d.Kind == "errors", p.MaxGapMS)
	default:
		return unknown(d.ID, "unsupported_kind", "This observation is not yet supported")
	}
	if len(facts) == 0 {
		return empty
	}
	rank := map[string]int{"not_applicable": 0, "clear": 1, "context": 2, "idle": 2, "unknown": 3, "abnormal": 4}
	result := Evidence{ID: d.ID, Status: "clear", ObservationIDs: []string{}, Facts: facts}
	for index, f := range facts {
		one := Evidence{Status: "context", Value: "Observed", Note: "Independent source observation; no causal explanation"}
		if f.Code != "" {
			one = unknown(d.ID, f.Code, qualityExplanation(f.Code))
			if f.Code == "insufficient_observations" {
				one.Value = fmt.Sprintf("%.0f completed", f.Count)
				one.Note = fmt.Sprintf("%.0f completed operations in this population; at least %d are required for a latency assessment. Low traffic or idle periods cannot establish slow-path health", f.Count, p.MinObservations)
			}
		} else {
			switch d.Kind {
			case "p99":
				one.Status = "clear"
				one.Value = prometheus.FormatWALBounds(f, d.Unit)
				one.Note = fmt.Sprintf("P99 interval compared with configured %.3g %s reference; not an end-to-end timer", threshold, d.Unit)
				if f.Lower == nil {
					one = unknown(d.ID, "no_quantile", "No completed population is available")
				} else if f.AboveForMS >= p.SustainMS {
					one.Status = "abnormal"
					one.Note = fmt.Sprintf("P99 lower bound exceeds %.3g %s for at least %.0f minutes in this exact population; cause unconfirmed", threshold, d.Unit, float64(f.AboveForMS)/60000)
				} else if f.Upper == nil || *f.Upper > threshold {
					one.Status = "unknown"
					one.Code = "sustained_exceedance_or_bucket_resolution_unestablished"
					one.Note = "A high or unresolved P99 interval exists, but the configured sustained rule is not established"
				}
			case "position":
				one.Value = "No observed advancement"
				if f.Delta > 0 {
					one.Value = "Position advancing"
				}
				one.Note = "Paired positions are context; readable backlog and expected workload are not inferred"
			case "rate":
				v := f.Rate
				unit := "/s"
				if strings.Contains(d.Unit, "bytes") {
					v /= 1 << 20
					unit = " MiB/s"
				}
				one.Value = fmt.Sprintf("%.3g%s", v, unit)
				one.Note = "Observed rate for an exact exporter population; offered-load target is not assumed"
			case "increase", "errors":
				one.Status = "clear"
				one.Value = fmt.Sprintf("%.0f observed events", f.Delta)
				one.Note = fmt.Sprintf("Observed event increase compared with configured reference %g; no-match does not cover uninstrumented paths", p.Thresholds[d.ID])
				if d.ID == "B01" || d.ID == "R09" || d.ID == "OR01" || d.ID == "OW01" {
					one.Status = "context"
				} else if f.Delta > p.Thresholds[d.ID] {
					one.Status = "abnormal"
					one.Note = fmt.Sprintf("Instrumented failures exceeded configured reference %g in this population; final request outcome and cause require corroboration", p.Thresholds[d.ID])
				}
			case "io_wait_rate":
				one.Value = fmt.Sprintf("%.3g cumulative s/s", f.Rate)
				one.Note = "Host I/O-wait seconds per wall second; context only. This metric is not a percentage, and a matching host CPU-time denominator is unavailable"
				for i := 1; i < len(f.Samples); i++ {
					if f.Samples[i].Value < f.Samples[i-1].Value {
						one = unknown(d.ID, "io_wait_reset", "Cumulative I/O-wait reset during this window; its rate is withheld")
						break
					}
				}
			case "trend":
				one.Value = fmt.Sprintf("%.0f → %.0f", f.First, f.Last)
				one.Note = "Pending operations observed; sustained growth needs same-population submission evidence"
			case "gauge":
				one.Value = fmt.Sprintf("%.3g", f.Last)
				one.Note = "Gauge samples retained; no causal or stall verdict is implied"
				resourceThreshold := math.Inf(1)
				factor := 1.0
				if d.ID == "H01" {
					resourceThreshold = 90
				}

				if d.ID == "H03" {
					resourceThreshold = .90
					factor = 100
				}
				if v, ok := p.Thresholds[d.ID]; ok {
					resourceThreshold = v
				}
				if d.ID == "MV03" {
					one.Value = fmt.Sprintf("%.3g MiB", f.Last/(1<<20))
				} else if strings.HasPrefix(d.ID, "H") {
					one.Value = fmt.Sprintf("%.3g%%", f.Last*factor)
					one.Status = "clear"
					one.Note = "System-level resource observation, not per-process root-cause evidence"
					maximum := 100.0
					if d.ID == "H03" {
						maximum = 1
					}
					if f.Minimum < 0 || f.Peak > maximum {
						one = unknown(d.ID, "resource_range_invalid", "Resource samples exceed the source unit range; no utilization verdict")
						break
					}
					if sustainedAbove(f.Samples, resourceThreshold) >= p.SustainMS {
						one.Status = "abnormal"
						one.Note = "System resource usage exceeded its reference for the sustained interval; WP attribution is unestablished"
					}
				}
			}
		}
		facts[index].Assessment = one.Status
		facts[index].Reading = one.Value
		if one.Status == "abnormal" && d.Kind == "p99" {
			start := 0
			for j := 0; j <= len(facts[index].Windows); j++ {
				if j < len(facts[index].Windows) && facts[index].Windows[j].Above {
					continue
				}
				calculationMS := p.CalculationWindowMS
				if calculationMS <= 0 {
					calculationMS = 60000
				}
				if int64(j-start)*calculationMS >= p.SustainMS {
					for k := start; k < j; k++ {
						facts[index].Windows[k].Matched = true
					}
				}
				start = j + 1
			}
		}
		if result.Value == "" || rank[one.Status] > rank[result.Status] {
			result.Status = one.Status
			result.Value = one.Value
			result.Note = one.Note
			result.Code = one.Code
		}
	}
	if len(facts) > 1 {
		result.Note += fmt.Sprintf(". %d separate exporter populations are retained; values are not merged", len(facts))
	}
	return result
}

func readBranch(d Definition, sets []prometheus.Dataset, p Policy) Evidence {
	e := Evidence{ID: d.ID, Status: "context", Value: "Read branch observations", Note: "Per-candidate branch activity retained; no Reader health verdict", ObservationIDs: []string{}}
	any := false
	for _, x := range sets {
		for _, f := range prometheus.WALSimpleFacts(x, true, p.MaxGapMS) {
			e.Facts = append(e.Facts, f)
			if f.Code == "" && f.Delta > 0 {
				any = true
			}
		}
	}
	if !any {
		e.Status = "unknown"
		e.Value = "Branch not established"
		e.Note = "No qualified positive branch activity; missing or lazy series are not zero"
	}
	return e
}

func diskState(d Definition, sets []prometheus.Dataset, p Policy) Evidence {
	e := unknown(d.ID, "watermark_binding_missing", "Require the WAL path usage and both watermarks on the same node")
	if d.ID != "S06" || len(sets) != 3 {
		return e
	}
	usage := prometheus.WALSimpleFacts(sets[0], false, p.MaxGapMS)
	soft := prometheus.WALSimpleFacts(sets[1], false, p.MaxGapMS)
	hard := prometheus.WALSimpleFacts(sets[2], false, p.MaxGapMS)
	for _, u := range usage {
		for _, s := range soft {
			for _, h := range hard {
				if u.Labels["instance"] == "" || u.Labels["instance"] != s.Labels["instance"] || u.Labels["instance"] != h.Labels["instance"] || u.Labels["node_id"] == "" || u.Labels["node_id"] != s.Labels["node_id"] || u.Labels["node_id"] != h.Labels["node_id"] || u.Code != "" || s.Code != "" || h.Code != "" || s.Minimum != s.Peak || h.Minimum != h.Peak || u.Minimum < 0 || u.Peak > 1 || s.Last <= 0 || s.Last >= h.Last || h.Last > 1 {
					continue
				}
				candidate := Evidence{ID: d.ID, Status: "clear", Value: fmt.Sprintf("%.1f%% (hard %.1f%%)", u.Peak*100, h.Last*100), Note: "Mapped WAL filesystem peak compared with same-node stable watermark samples; cleanup correctness is not established", Facts: []prometheus.WALFact{u, s, h}, ObservationIDs: []string{}}
				if u.Peak >= s.Last {
					candidate.Status = "abnormal"
					candidate.Note = "Mapped WAL filesystem reached a configured protection watermark; why it filled is unconfirmed"
				}
				candidate.Facts[0].Assessment = candidate.Status
				candidate.Facts[0].Reading = candidate.Value
				for i := 1; i < len(candidate.Facts); i++ {
					candidate.Facts[i].Assessment = "context"
				}
				if e.Status != "abnormal" || candidate.Status == "abnormal" {
					e = candidate
				}
			}
		}
	}
	return e
}

func sustainedAbove(xs []prometheus.Point, threshold float64) int64 {
	best, start := int64(0), int64(-1)
	for _, p := range xs {
		if p.Value > threshold {
			if start < 0 {
				start = p.TimeMS
			}
			best = max(best, p.TimeMS-start)
		} else {
			start = -1
		}
	}
	return best
}

func qualityExplanation(code string) string {
	switch code {
	case "insufficient_scrapes":
		return "Fewer than two source samples; a historical change cannot be measured"
	case "scrape_gap":
		return "A gap between source samples exceeds the configured allowance; this interval cannot support a reliable assessment"
	case "stale_or_uncovered_window_boundary":
		return "Source samples do not cover both ends of the selected incident window"
	case "counter_reset_or_restart":
		return "A counter reset or process restart crosses this window; select a continuous interval"
	case "histogram_empty_or_no_completions":
		return "No completed operations were observed; latency cannot be measured and offered load is not established"
	default:
		return "The source samples cannot support this assessment (" + code + "). Open the original observation for details"
	}
}

func metricUnit(d Definition) string {
	if d.ID == "H02" {
		return "cumulative_seconds"
	}
	return d.Unit
}

func QualityExplanation(code string) string { return qualityExplanation(code) }

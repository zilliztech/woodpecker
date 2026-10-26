package prometheus

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

type Fact struct {
	Population     string            `json:"population_id"`
	Labels         map[string]string `json:"labels"`
	ObservedFromMS int64             `json:"observed_from_ms,omitempty"`
	ObservedToMS   int64             `json:"observed_to_ms,omitempty"`
	Completions    float64           `json:"observed_completions"`
	Rate           float64           `json:"completions_per_second"`
	MeanMS         *float64          `json:"mean_ms,omitempty"`
	P95LowerMS     *float64          `json:"p95_lower_ms,omitempty"`
	P95UpperMS     *float64          `json:"p95_upper_ms,omitempty"`
	ThresholdMS    float64           `json:"threshold_ms,omitempty"`
	ExporterKey    string            `json:"exporter_identity,omitempty"`
	Gauge          *GaugeFact        `json:"gauge,omitempty"`
}

type GaugeFact struct {
	// Samples are retained only for persistence profiles for exact-time comparisons.
	Samples     []Point `json:"retained_samples,omitempty"`
	First       float64 `json:"first"`
	Last        float64 `json:"last"`
	Peak        float64 `json:"peak"`
	Minimum     float64 `json:"minimum"`
	LastTwoZero bool    `json:"last_two_zero"`
}

func quality(points []Point, from, to time.Time, gap int64, counter bool) string {
	if len(points) < 2 {
		return "insufficient_scrapes"
	}
	if points[0].TimeMS-from.UnixMilli() > gap || to.UnixMilli()-points[len(points)-1].TimeMS > gap {
		return "stale_or_uncovered_window_boundary"
	}
	for i := 1; i < len(points); i++ {
		if points[i].TimeMS-points[i-1].TimeMS > gap {
			return "scrape_gap"
		}
		if counter && points[i].Value < points[i-1].Value {
			return "counter_reset_or_restart"
		}
	}
	return ""
}

func finding(sig signal, r d.Request, id, pop string, labels map[string]string) d.Finding {
	target := d.Target{Deployment: r.Selection.Target.Deployment, Instance: labels["instance"], NodeID: labels["node_id"], TenantAssociation: "unresolved: exporter labels do not establish client metadata authority"}
	if n, err := strconv.ParseInt(labels["log_id"], 10, 64); err == nil && n >= 0 {
		target.LogID = strconv.FormatInt(n, 10)
	}
	return d.Finding{ID: id + ":" + pop, RuleID: sig.ID + ".latency", RuleVersion: 1, Flow: r.Selection.Flow, Stage: sig.Stage, Target: target, Outcome: "inconclusive", Severity: "unknown", Strength: "inconclusive", Supporting: []string{id}, Contradicting: []string{}, Unknown: []string{"completed_operations_only", "pending_work_and_error_coverage_not_established", "events_between_scrapes_unobservable"}, NextChecks: []string{"Compare the exact exported population and observation window; do not add/subtract unrelated stage quantiles", "Inspect workload demand and the relevant E2E dashboard before assigning a cause"}}
}

func evaluate(data *Dataset, sig signal, r d.Request, p Policy, id string) []d.Finding {
	if d.Has(data.Notes, "backend_annotations_withheld") {
		f := finding(sig, r, id, "coverage", nil)
		f.Summary = "Backend annotations qualify this query; conclusions withheld"
		f.Unknown = append(f.Unknown, "backend_partial_or_qualified_response")
		return []d.Finding{f}
	}
	if len(data.Series) == 0 {
		f := finding(sig, r, id, "empty", nil)
		f.Summary = "No matching series; missing/lazy/unsupported metrics are not healthy zeros"
		f.Unknown = append(f.Unknown, "no_matching_series")
		return []d.Finding{f}
	}
	if sig.Gauge {
		if sig.GaugeUnit != "" {
			return evaluateGauge(data, sig, r, p, id)
		}
		return evaluateAdmission(data, sig, r, p, id)
	}
	type population struct {
		labels map[string]string
		series []Series
	}
	groups := map[string]*population{}
	for _, s := range data.Series {
		labels := map[string]string{}
		for k, v := range s.AllLabels {
			if k != "__name__" && k != "le" {
				labels[k] = v
			}
		}
		key := fingerprint(labels)
		if groups[key] == nil {
			visible := map[string]string{}
			for k, v := range s.Labels {
				if k != "__name__" && k != "le" {
					visible[k] = v
				}
			}
			groups[key] = &population{labels: visible}
		}
		groups[key].series = append(groups[key].series, s)
	}
	keys := []string{}
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := []d.Finding{}
	for _, key := range keys {
		g := groups[key]
		f := finding(sig, r, id, key, g.labels)
		fact, code := histogram(g.series, sig, *data, p)
		fact.Population = key
		fact.Labels = g.labels
		data.Facts = append(data.Facts, fact)
		if code != "" {
			f.Summary = "Cannot assess " + sig.Metric + ": " + code
			f.Unknown = append(f.Unknown, code)
			result = append(result, f)
			continue
		}
		switch {
		case fact.Completions == 0:
			f.Outcome = "idle_or_no_completions"
			f.Summary = "Zero observed completion increase; without expected demand this is not a stall"
			f.Unknown = append(f.Unknown, "expected_work_unknown")
		case fact.Completions < float64(p.MinObservations):
			f.Summary = fmt.Sprintf("Only %.0f %s observations; need at least %d", fact.Completions, sig.Metric, p.MinObservations)
			f.Unknown = append(f.Unknown, "insufficient_observations")
		case *fact.P95LowerMS >= fact.ThresholdMS:
			f.Outcome = "suspected_high_latency"
			f.Severity = "warning"
			f.Strength = "suspected"
			f.Summary = fmt.Sprintf("%s P95 is at least %.3fms, above configured %.3fms; %.0f completions at %.3f/s", sig.Metric, *fact.P95LowerMS, fact.ThresholdMS, fact.Completions, fact.Rate)
		case fact.P95UpperMS != nil && *fact.P95UpperMS <= fact.ThresholdMS:
			f.Outcome = "within_configured_latency_for_observed_completions"
			f.Severity = "none"
			f.Summary = fmt.Sprintf("%s observed P95 upper bound %.3fms is within %.3fms; %.0f completions at %.3f/s. This does not establish end-to-end health", sig.Metric, *fact.P95UpperMS, fact.ThresholdMS, fact.Completions, fact.Rate)
		default:
			f.Summary = "Configured latency threshold falls within an unresolved histogram bucket"
			f.Unknown = append(f.Unknown, "histogram_resolution_insufficient")
		}
		if fact.P95UpperMS == nil && fact.Completions > 0 {
			f.Unknown = append(f.Unknown, "histogram_tail_unbounded")
		}
		result = append(result, f)
		for _, s := range g.series {
			if s.AllLabels["__name__"] == sig.Metric+"_count" && len(s.Points) >= 5 && fact.Completions >= float64(p.MinObservations) {
				points := s.Points
				mid := len(points) / 2
				first := (points[mid].Value - points[0].Value) / float64(points[mid].TimeMS-points[0].TimeMS) * 1000
				last := (points[len(points)-1].Value - points[mid].Value) / float64(points[len(points)-1].TimeMS-points[mid].TimeMS) * 1000
				if first > 0 && last < first*p.ThroughputDropRatio {
					drop := finding(sig, r, id, key+":throughput", g.labels)
					drop.RuleID = sig.ID + ".throughput_change"
					drop.Outcome = "observed_throughput_drop"
					drop.Unknown = append(drop.Unknown, "demand_and_arrival_rate_unknown")
					drop.Summary = fmt.Sprintf("%s completion rate fell from %.3f/s to %.3f/s (configured ratio %.3f). Reduced demand can explain this; not a no-progress verdict", sig.Metric, first, last, p.ThroughputDropRatio)
					result = append(result, drop)
				}
			}
		}
	}
	return result
}

func histogram(series []Series, sig signal, data Dataset, p Policy) (Fact, string) {
	fact := Fact{ThresholdMS: p.LatencyMS[sig.ID]}
	var count, sum []Point
	type bucket struct {
		upper  float64
		points []Point
	}
	buckets := []bucket{}
	var reference []Point
	for _, s := range series {
		if code := quality(s.Points, data.From, data.To, p.MaxGapMS, true); code != "" {
			return fact, code
		}
		if s.AllLabels["__name__"] != sig.Metric+"_sum" {
			for _, point := range s.Points {
				if point.Value > 1<<53 || point.Value != math.Trunc(point.Value) {
					return fact, "histogram_count_precision_or_integrality"
				}
			}
		}
		if reference == nil {
			reference = s.Points
		} else {
			if len(reference) != len(s.Points) {
				return fact, "histogram_sample_misalignment"
			}
			for i := range reference {
				if reference[i].TimeMS != s.Points[i].TimeMS {
					return fact, "histogram_sample_misalignment"
				}
			}
		}
		switch s.AllLabels["__name__"] {
		case sig.Metric + "_count":
			count = s.Points
		case sig.Metric + "_sum":
			sum = s.Points
		case sig.Metric + "_bucket":
			upper, err := strconv.ParseFloat(s.AllLabels["le"], 64)
			if err != nil || math.IsNaN(upper) || upper < 0 {
				return fact, "invalid_bucket_boundary"
			}
			buckets = append(buckets, bucket{upper, s.Points})
		}
	}
	if len(count) == 0 || len(sum) == 0 || len(buckets) < 2 {
		return fact, "missing_histogram_family"
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].upper < buckets[j].upper })
	if !math.IsInf(buckets[len(buckets)-1].upper, 1) {
		return fact, "missing_infinite_bucket"
	}
	for i, b := range buckets {
		if i > 0 && b.upper == buckets[i-1].upper {
			return fact, "duplicate_bucket_boundary"
		}
		for j, point := range b.points {
			if (i > 0 && point.Value < buckets[i-1].points[j].Value) || (i == len(buckets)-1 && point.Value != count[j].Value) {
				return fact, "inconsistent_cumulative_histogram"
			}
		}
	}
	first, last := count[0], count[len(count)-1]
	fact.ObservedFromMS = first.TimeMS
	fact.ObservedToMS = last.TimeMS
	fact.Completions = last.Value - first.Value
	fact.Rate = fact.Completions / float64(last.TimeMS-first.TimeMS) * 1000
	if fact.Completions == 0 {
		if sum[len(sum)-1].Value != sum[0].Value {
			return fact, "sum_increased_without_observations"
		}
		return fact, ""
	}
	mean := (sum[len(sum)-1].Value - sum[0].Value) / fact.Completions * sig.Factor
	fact.MeanMS = &mean
	quantile := fact.Completions * 0.95
	lower := 0.0
	previous := 0.0
	for _, b := range buckets {
		delta := b.points[len(b.points)-1].Value - b.points[0].Value
		if delta < previous || delta > fact.Completions {
			return fact, "inconsistent_bucket_increase"
		}
		previous = delta
	}
	for _, b := range buckets {
		delta := b.points[len(b.points)-1].Value - b.points[0].Value
		if delta >= quantile {
			lo := lower * sig.Factor
			fact.P95LowerMS = &lo
			if !math.IsInf(b.upper, 1) {
				hi := b.upper * sig.Factor
				fact.P95UpperMS = &hi
			}
			return fact, ""
		}
		lower = b.upper
	}
	return fact, "unresolved_quantile"
}

func evaluateAdmission(data *Dataset, sig signal, r d.Request, p Policy, id string) []d.Finding {
	result := []d.Finding{}
	for _, s := range data.Series {
		f := finding(sig, r, id, s.ID, s.Labels)
		f.RuleID = "wp.admission.exported_state"
		if code := quality(s.Points, data.From, data.To, p.MaxGapMS, false); code != "" {
			f.Unknown = append(f.Unknown, code)
			f.Summary = "Admission state has incomplete temporal coverage"
			result = append(result, f)
			continue
		}
		peak := 0.0
		valid := true
		for _, point := range s.Points {
			if point.Value != 0 && point.Value != 1 && point.Value != 2 {
				valid = false
			}
			peak = max(peak, point.Value)
		}
		if !valid {
			f.Unknown = append(f.Unknown, "unknown_admission_state")
			f.Summary = "Exported admission state is incompatible with the documented 0/1/2 contract"
		} else if peak > 0 {
			f.Outcome = "observed_admission_pressure"
			f.Strength = "suspected"
			f.Severity = "warning"
			f.Summary = fmt.Sprintf("%s reached %.0f (0=normal, 1=warn, 2=blocked) under the deployed disk policy; causal relationship to latency is unestablished", sig.Metric, peak)
		} else {
			f.Outcome = "no_admission_pressure_observed"
			f.Severity = "none"
			f.Summary = "No nonzero exported admission state in retained samples; unsampled intervals remain unknown"
		}
		f.Unknown = append(f.Unknown, "deployed_policy_indicator_not_latency_root_cause")
		result = append(result, f)
	}
	return result
}

func isBudget(code string) bool { return strings.HasSuffix(code, "budget_exceeded") }

func evaluateGauge(data *Dataset, sig signal, r d.Request, p Policy, id string) []d.Finding {
	findings := []d.Finding{}
	for _, series := range data.Series {
		labels := map[string]string{}
		for k, v := range series.AllLabels {
			if k != "__name__" {
				labels[k] = v
			}
		}
		pop := fingerprint(labels)
		f := finding(sig, r, id, pop, series.Labels)
		f.RuleID = sig.ID + ".gauge"
		if code := quality(series.Points, data.From, data.To, p.MaxGapMS, false); code != "" {
			f.Summary = "Gauge temporal coverage is incomplete"
			f.Unknown = append(f.Unknown, code)
			findings = append(findings, f)
			continue
		}
		points := series.Points
		if sig.Stage == "persistence" {
			valid := true
			for _, point := range points {
				if sig.GaugeUnit == "state_indicator" {
					valid = valid && (point.Value == 0 || point.Value == 1)
				}
				if sig.GaugeUnit == "state_0_normal_1_warn_2_blocked" {
					valid = valid && (point.Value == 0 || point.Value == 1 || point.Value == 2)
				}
			}
			if !valid {
				f.Summary = "Unsupported exported state indicator"
				f.Unknown = append(f.Unknown, "unknown_state_indicator")
				findings = append(findings, f)
				continue
			}
		}
		if sig.GaugeUnit == "state_0_normal_1_warn_2_blocked" {
			valid := true
			for _, p := range points {
				valid = valid && (p.Value == 0 || p.Value == 1 || p.Value == 2)
			}
			if !valid {
				f.Summary = "Unsupported exported disk admission state"
				f.Unknown = append(f.Unknown, "unknown_disk_admission_state")
				findings = append(findings, f)
				continue
			}
		}
		g := GaugeFact{First: points[0].Value, Last: points[len(points)-1].Value, Peak: points[0].Value, Minimum: points[0].Value, LastTwoZero: len(points) >= 2 && points[len(points)-1].Value == 0 && points[len(points)-2].Value == 0}
		if sig.Stage == "persistence" {
			g.Samples = append([]Point{}, points...)
		}
		for _, point := range points {
			g.Peak = max(g.Peak, point.Value)
			g.Minimum = min(g.Minimum, point.Value)
		}
		data.Facts = append(data.Facts, Fact{Population: pop, Labels: series.Labels, ObservedFromMS: points[0].TimeMS, ObservedToMS: points[len(points)-1].TimeMS, Gauge: &g})
		f.Outcome = "gauge_observations_only"
		f.Unknown = []string{"sampled_exporter_state_not_request_specific", "events_between_scrapes_unobservable"}
		f.Summary = fmt.Sprintf("%s: first=%.3f last=%.3f peak=%.3f %s; no causal inference", sig.Metric, g.First, g.Last, g.Peak, sig.GaugeUnit)
		findings = append(findings, f)
	}
	return findings
}

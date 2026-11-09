package evidence

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/zilliztech/woodpecker/diagnostics/prometheus"
)

// An inspection explains an existing result. It performs no source IO and never
// changes an assessment. Formulas and chart units are owned with the WAL rules.
type Description struct {
	Metrics   []string `json:"metrics"`
	Meaning   string   `json:"meaning"`
	Formula   string   `json:"formula"`
	Criterion string   `json:"criterion"`
}
type Measurement struct {
	Labels map[string]string `json:"labels"`
	Value  string            `json:"value"`
	Status string            `json:"status"`
	Detail string            `json:"detail,omitempty"`
	FromMS int64             `json:"from_ms,omitempty"`
	ToMS   int64             `json:"to_ms,omitempty"`
}
type TrendPoint struct {
	TimeMS  int64    `json:"time_ms"`
	Value   *float64 `json:"value"`
	Upper   *float64 `json:"upper,omitempty"`
	Count   *float64 `json:"count,omitempty"`
	Above   bool     `json:"above_reference,omitempty"`
	Matched bool     `json:"matched_rule,omitempty"`
	Code    string   `json:"code,omitempty"`
}
type Curve struct {
	Metric    string            `json:"metric"`
	Labels    map[string]string `json:"labels"`
	Unit      string            `json:"unit"`
	Kind      string            `json:"kind"`
	Reference *float64          `json:"reference,omitempty"`
	Points    []TrendPoint      `json:"points"`
}
type Inspection struct {
	ValueLabel string `json:"value_label"`
	Description
	Status         string        `json:"status"`
	DataState      string        `json:"data_state"`
	StatusLabel    string        `json:"status_label"`
	Value          string        `json:"value"`
	Conclusion     string        `json:"conclusion"`
	Measurements   []Measurement `json:"measurements"`
	Anomalies      []Measurement `json:"anomalies"`
	Curves         []Curve       `json:"curves"`
	ObservationIDs []string      `json:"observation_ids"`
}

var meanings = map[string]string{
	"W01":  "Time recorded for successful Writer write operations.",
	"W02":  "Recorded Writer write errors.",
	"W03":  "Encoded bytes successfully written by the Writer.",
	"W04":  "Append entries submitted by the client.",
	"W05":  "Append operations still pending in the client.",
	"W06":  "Recorded client append-completion time.",
	"W07":  "Paired segment and entry position acknowledged to the Writer.",
	"W08":  "Failed append outcomes for individual replicas.",
	"S01":  "Server AddEntry calls recorded with status=error.",
	"S02":  "Recorded server buffer-notification wait time.",
	"S03":  "Server flush execution time; not isolated fsync time.",
	"S04":  "Bytes written by completed server flushes.",
	"S05":  "Writes rejected by node disk-pressure protection.",
	"S06":  "WAL filesystem usage compared with the same node's protection watermarks.",
	"S07":  "Successful server get_batch_entries method time, including its internal processing.",
	"S08":  "Server get_batch_entries calls recorded with status=error; EOF is separate. error_get_processor is not included by this selector.",
	"R01":  "Encoded Entry bytes successfully delivered by the Reader.",
	"R02":  "Time of the successful Reader loop iteration. Earlier retry/tail waits are excluded; cached delivery is included.",
	"R03":  "Reader read_next observations recorded with status=error; explicitly labeled cancel observations are separate.",
	"R04":  "Paired segment and entry position delivered by the Reader.",
	"R05":  "Readable tail compared with Reader position at the incident time.",
	"R06":  "Successful direct-read method time for sealed segments in object storage.",
	"R07":  "Object-storage direct-read outcomes recorded as error; EOF is separate.",
	"R08":  "Failed candidate-replica read attempts by az_scope, not final Reader failures.",
	"R09":  "Successful reads served from a different scope than the preferred scope; same-scope node changes are not counted.",
	"R10":  "Recorded replica/direct-read outcomes; either or both branches may be active within a window.",
	"B01":  "Completed client segment rolls.",
	"B02":  "Recorded client segment-compaction failures.",
	"B03":  "Paired segment and entry position of the compaction frontier.",
	"H01":  "System CPU usage on the explicitly mapped node, not WP process CPU usage.",
	"H02":  "Cumulative host CPU I/O-wait seconds, despite the metric's percentage HELP text.",
	"H03":  "System used-memory / total-memory ratio, not WP process RSS.",
	"OR01": "Successful Reader-open operations.",
	"OR02": "Reader-open operations recorded as errors.",
	"OR03": "Successful Reader-open operation time.",
	"OR04": "Reader-open operation time recorded on error paths.",
	"OR05": "Application/log error associated with this Reader-open operation.",
	"OW01": "Successful Writer-open operations.",
	"OW02": "Writer-open operations with a lock-acquisition error.",
	"OW03": "Writer-open operations with a fencing error.",
	"OW04": "Writer-open time recorded with status=lock_error.",
	"OW05": "Application/log error associated with this Writer-open operation.",
	"M01":  "Current Log metadata returned by wpcli.",
	"M02":  "Current Segment metadata returned by wpcli.",
	"M03":  "Current namespace-scoped Quorum metadata returned by wpcli.",
	"M04":  "Constraint findings returned by the existing wpcli meta check command.",
	"T01":  "Write spans correlated by request/log identity and parent relationships.",
	"T02":  "Read spans correlated by request/log identity and parent relationships.",
	"T03":  "Open-operation spans correlated by request/log identity and parent relationships.",
	"MV01": "Milvus producer rate-reservation delay for an explicitly associated WAL channel.",
	"MV02": "Milvus WAL implementation append-message duration for an explicitly associated channel.",
	"MV03": "Bytes pending delivery in the associated Milvus WAL Scanner queue.",
}

func thresholdFor(d Definition, p Policy) float64 {
	threshold := p.LatencyMS
	if d.Unit == "seconds" {
		threshold /= 1000
	}
	if d.ID == "H01" {
		threshold = 90
	}
	if d.ID == "H03" {
		threshold = .9
	}
	if v, ok := p.Thresholds[d.ID]; ok {
		threshold = v
	}
	return threshold
}

func Describe(d Definition, p Policy) Description {
	out := Description{Metrics: []string{}, Meaning: meanings[d.ID]}
	keys := []string{}
	for k := range d.Match {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	match := []string{}
	for _, k := range keys {
		match = append(match, fmt.Sprintf("%s=%q", k, d.Match[k]))
	}
	for _, metric := range d.Metrics {
		if d.Kind == "errors" {
			metric += "_count"
		}
		if len(match) > 0 {
			metric += "{" + strings.Join(match, ",") + "}"
		}
		out.Metrics = append(out.Metrics, metric)
	}
	switch d.Kind {
	case "p99":
		out.Formula = "Per 1-minute block: P99 = bucket interval at rank 0.99 × Δcount, using Δbucket."
		out.Criterion = fmt.Sprintf("Abnormal: P99 lower bound ≥ %.3g %s in consecutive blocks covering ≥ %.3g min, each with ≥ %d completions.", thresholdFor(d, p), d.Unit, float64(p.SustainMS)/60000, p.MinObservations)
	case "rate":
		out.Formula = "Window average = (last counter − first counter) / actual sample span in seconds."
		out.Criterion = "Context only; no workload baseline or throughput threshold is assumed."
	case "errors", "increase":
		out.Formula = "Window increase = last counter − first counter."
		out.Criterion = fmt.Sprintf("Abnormal: recorded error increase > %g in an exact population.", p.Thresholds[d.ID])
	case "io_wait_rate":
		out.Formula = "(Last I/O-wait seconds − first I/O-wait seconds) / actual sample span in seconds."
		out.Criterion = "Context only, in cumulative seconds / wall second. No percentage threshold."
	case "position":
		out.Formula = "Compare paired (segment, entry) samples in time order. Do not subtract entry IDs across segments."
		out.Criterion = "Context only; position advancement does not measure readable backlog."
	case "trend":
		out.Formula = fmt.Sprintf("Pending samples stay nondecreasing and increase over ≥ %.3g min; submissions increase in the same population and interval.", float64(p.SustainMS)/60000)
		out.Criterion = "Abnormal only when both pending growth and submission evidence match."
	case "gauge":
		out.Formula = "Latest sample; retain the window minimum and peak."
		out.Criterion = "Context only; no independent threshold."
	}
	switch d.ID {
	case "R10":
		out.Formula = "Window increase of each replica/direct counter, separated by status and scope."
		out.Criterion = "Positive increase establishes recorded branch activity. Missing series do not establish an unused branch."
	case "B01", "R09", "OR01", "OW01":
		out.Criterion = "Context only; no error or slowness verdict from this count."
	case "H01", "H03":
		out.Formula = fmt.Sprintf("Longest consecutive sampled interval above %.3g %s; not a window average.", thresholdFor(d, p), d.Unit)
		out.Criterion = fmt.Sprintf("Abnormal: that interval lasts ≥ %.3g min.", float64(p.SustainMS)/60000)
	case "S06":
		out.Formula = "Compare filesystem peak with stable soft/hard watermarks from the same exporter and node."
		out.Criterion = "Abnormal: usage peak ≥ soft watermark."
	case "W07":
		out.Criterion = "Abnormal: unchanged acknowledgment position for ≥ 120 s of aligned samples, positive submissions and continuously nonzero pending work in the same population."
	case "M01", "M02", "M03":
		out.Formula = map[string]string{"M01": "wp meta log show", "M02": "wp meta segment list", "M03": "wp meta quorum list"}[d.ID]
		out.Criterion = "Current read-only view; no additional consistency rules."
	case "M04":
		out.Formula = "wp meta check"
		out.Criterion = "Use the command's original state and constraint findings."
	}
	if d.Disposition == "defer" {
		out.Formula = "Not implemented in this workbench."
		out.Criterion = "Unknown; no automatic assessment."
	}
	return out
}

func Descriptions(p Policy) map[string]Description {
	out := map[string]Description{}
	for _, d := range Definitions().Evidence {
		out[d.ID] = Describe(d, p)
	}
	return out
}

func Inspect(r Report, id string) (Inspection, error) {
	var d Definition
	for _, def := range Definitions().Evidence {
		if def.ID == id {
			d = def
			break
		}
	}
	var e Evidence
	found := false
	for _, item := range r.Evidence {
		if item.ID == id {
			e = item
			found = true
			break
		}
	}
	if !found || d.ID == "" || r.SchemaVersion != "wpdiag.wal/v1" {
		return Inspection{}, fmt.Errorf("evidence point is not in this report")
	}
	out := Inspection{Description: Describe(d, r.Policy), Status: e.Status, DataState: DataState(e), StatusLabel: DisplayState(e), Value: e.Value, Measurements: []Measurement{}, Anomalies: []Measurement{}, Curves: []Curve{}, ObservationIDs: e.ObservationIDs}
	out.ValueLabel = "Window result"
	switch d.Kind {
	case "p99":
		out.ValueLabel = "Window P99"
	case "rate":
		out.ValueLabel = "Window average"
	case "increase", "errors":
		out.ValueLabel = "Window increase"
	case "io_wait_rate":
		out.ValueLabel = "Window average"
	case "gauge":
		out.ValueLabel = "Latest sample"
	}
	if d.ID == "S06" {
		out.ValueLabel = "Window peak"
	}
	switch e.Status {
	case "abnormal":
		out.Conclusion = fmt.Sprintf("Recorded failures exceeded the configured event-count reference %g in at least one exact population.", r.Policy.Thresholds[d.ID])
		if d.Kind == "p99" {
			out.Conclusion = fmt.Sprintf("P99 exceeded %.3g %s in qualified blocks lasting at least %.3g min.", thresholdFor(d, r.Policy), d.Unit, float64(r.Policy.SustainMS)/60000)
		}
		switch d.ID {
		case "H01":
			out.Conclusion = "Sustained system CPU pressure occurred on the mapped node."
		case "H03":
			out.Conclusion = "Sustained system memory pressure occurred on the mapped node."
		case "S06":
			out.Conclusion = "The WAL filesystem reached a configured protection watermark."
		case "W05":
			out.Conclusion = "Pending work grew over a sustained interval with matching submissions."
		case "W07":
			out.Conclusion = "Acknowledgment position did not advance while matching work remained pending."
		case "M04":
			out.Conclusion = "Existing wpcli metadata constraints returned findings."
		}
	case "clear":
		out.Conclusion = fmt.Sprintf("No exact population exceeded the configured event-count reference %g.", r.Policy.Thresholds[d.ID])
		if d.Kind == "p99" {
			out.Conclusion = "No sustained P99 exceedance was detected."
		}
		if d.ID == "H01" || d.ID == "H03" {
			out.Conclusion = "No sustained resource-threshold exceedance was detected."
		}
		if d.ID == "S06" {
			out.Conclusion = "The qualified filesystem samples stayed below the soft watermark."
		}
		if d.ID == "M04" {
			out.Conclusion = "Existing wpcli metadata constraints passed."
		}
	case "context":
		out.Conclusion = "Measured context; no independent abnormality threshold."
		switch d.ID {
		case "R01":
			out.Conclusion = "Reader delivery throughput measured; workload demand is not assessed."
		case "H02":
			out.Conclusion = "I/O-wait accumulation rate measured; percentage utilization is not established."
		case "R09":
			out.Conclusion = "Cross-scope successful-read count measured."
		case "M01", "M02", "M03":
			out.Conclusion = "Current metadata returned by the existing read-only wpcli command."
		}
	case "not_applicable":
		out.Conclusion = "This branch was classified as unused by the retained branch evidence."
	default:
		out.Conclusion = e.Note
		switch e.Code {
		case "deferred_evidence":
			out.Conclusion = "Automatic collection / correlation is not implemented."
		case "insufficient_observations":
			out.Conclusion = "Too few completed operations to assess P99."
		case "no_matching_series":
			out.Conclusion = "No matching source series."
		case "sustained_exceedance_or_bucket_resolution_unestablished":
			out.Conclusion = "Sustained P99 exceedance is not established."
		}
	}
	if d.ID == "R10" && e.Status == "context" {
		branches := map[string]bool{}
		for _, fact := range e.Facts {
			if fact.Code != "" || fact.Delta <= 0 {
				continue
			}
			metric := fact.Labels["__name__"]
			if strings.Contains(metric, "quorum_read") {
				branches["Replica"] = true
			}
			if strings.Contains(metric, "direct_read") {
				branches["Direct"] = true
			}
		}
		names := []string{}
		for _, name := range []string{"Replica", "Direct"} {
			if branches[name] {
				names = append(names, name)
			}
		}
		out.ValueLabel = "Observed branch activity"
		out.Value = strings.Join(names, " + ")
		out.Conclusion = "Recorded outcomes increased on these branches."
	}
	for _, f := range e.Facts {
		status := f.Assessment
		if status == "" {
			status = "context"
			if f.Code != "" {
				status = "unknown"
			}
		}
		reading := f.Reading
		if reading == "" {
			reading = fmt.Sprintf("last %.3g; peak %.3g", f.Last, f.Peak)
			if d.Kind == "increase" || d.Kind == "errors" {
				reading = fmt.Sprintf("%.0f recorded events", f.Delta)
			}
		}
		row := Measurement{Labels: f.Labels, Value: reading, Status: status, FromMS: f.ObservedFromMS, ToMS: f.ObservedToMS, Detail: f.Code}
		if d.Kind == "p99" {
			row.Detail = fmt.Sprintf("%.0f completions; longest qualified exceedance %.3g min", f.Count, float64(f.AboveForMS)/60000)
		}
		out.Measurements = append(out.Measurements, row)
		if status == "abnormal" && d.Kind != "p99" && d.ID != "H01" && d.ID != "H03" {
			out.Anomalies = append(out.Anomalies, row)
		}
		if d.Kind == "p99" {
			reference := thresholdFor(d, r.Policy)
			curve := Curve{Metric: d.Metrics[0], Labels: f.Labels, Unit: d.Unit, Kind: "interval", Reference: &reference, Points: []TrendPoint{}}
			for _, w := range f.Windows {
				curve.Points = append(curve.Points, TrendPoint{TimeMS: w.ToMS, Value: w.Lower, Upper: w.Upper, Count: &w.Count, Above: w.Above, Matched: w.Matched, Code: w.Code})
				if w.Matched {
					out.Anomalies = append(out.Anomalies, Measurement{Labels: f.Labels, Value: prometheus.FormatWALBounds(prometheus.WALFact{Lower: w.Lower, Upper: w.Upper}, d.Unit), Status: "abnormal", FromMS: w.FromMS, ToMS: w.ToMS, Detail: fmt.Sprintf("%.0f completions; reference %.3g %s", w.Count, reference, d.Unit)})
				}
			}
			out.Curves = append(out.Curves, curve)
		}
	}
	refs := map[string]bool{}
	for _, id := range e.ObservationIDs {
		refs[id] = true
	}
	if d.Kind != "p99" {
		for _, o := range r.Observations {
			if !refs[o.ID] || o.Status != "observed" {
				continue
			}
			var data prometheus.Dataset
			if json.Unmarshal(o.Data, &data) != nil {
				continue
			}
			for _, series := range data.Series {
				metric := series.Labels["__name__"]
				owned := false
				for _, m := range d.Metrics {
					if d.Kind == "errors" {
						m += "_count"
					}
					if metric == m {
						owned = true
						break
					}
				}
				if owned {
					if d.ID == "H01" || d.ID == "H03" {
						for _, fact := range e.Facts {
							if fact.Population == series.ID && fact.Assessment == "abnormal" {
								out.Anomalies = append(out.Anomalies, resourceAnomalies(d, series, r.Policy)...)
								break
							}
						}
					}
					curve := rawCurve(d, series, r.Policy)
					if d.ID == "S06" && len(e.Facts) == 3 && series.Labels["instance"] == e.Facts[0].Labels["instance"] && series.Labels["node_id"] == e.Facts[0].Labels["node_id"] {
						reference := e.Facts[1].Last * 100
						curve.Reference = &reference
					}
					out.Curves = append(out.Curves, curve)
				}
			}
		}
	}
	if d.ID == "M04" && e.Status == "abnormal" {
		for _, o := range r.Observations {
			if !refs[o.ID] {
				continue
			}
			var check struct {
				Issues []string `json:"issues"`
			}
			if json.Unmarshal(o.Data, &check) == nil {
				for _, issue := range check.Issues {
					out.Anomalies = append(out.Anomalies, Measurement{Value: issue, Status: "abnormal"})
				}
			}
		}
	}
	return out, nil
}

func rawCurve(d Definition, s prometheus.Series, p Policy) Curve {
	out := Curve{Metric: s.Labels["__name__"], Labels: s.Labels, Kind: "line", Unit: d.Unit, Points: []TrendPoint{}}
	if d.Kind == "position" {
		out.Unit = "entry ID"
		if strings.HasSuffix(out.Metric, "_segment") {
			out.Unit = "segment ID"
		}
	}
	delta := d.Kind == "errors" || d.Kind == "increase" || d.Kind == "rate" || d.Kind == "io_wait_rate"
	if d.Kind == "errors" || d.Kind == "increase" {
		out.Unit = "events / scrape"
	}
	if d.Kind == "rate" && strings.Contains(d.Unit, "bytes") {
		out.Unit = "MiB/s"
	}
	if d.ID == "H03" || d.ID == "S06" {
		out.Unit = "percent"
	}
	if d.ID == "H01" || d.ID == "H03" {
		v := thresholdFor(d, p)
		if d.ID == "H03" {
			v *= 100
		}
		out.Reference = &v
	}
	for i, x := range s.Points {
		value := x.Value
		point := TrendPoint{TimeMS: x.TimeMS}
		if !delta && i > 0 && x.TimeMS-s.Points[i-1].TimeMS > p.MaxGapMS {
			point.Code = "scrape_gap"
		}
		if delta {
			if i == 0 {
				continue
			}
			previous := s.Points[i-1]
			gap := x.TimeMS - previous.TimeMS
			if gap <= 0 || gap > p.MaxGapMS || value < previous.Value {
				point.Code = "gap_or_reset"
				out.Points = append(out.Points, point)
				continue
			}
			value -= previous.Value
			if d.Kind == "rate" || d.Kind == "io_wait_rate" {
				value = value / float64(gap) * 1000
			}
			if d.Kind == "rate" && strings.Contains(d.Unit, "bytes") {
				value /= 1 << 20
			}
		}
		if d.ID == "H03" || d.ID == "S06" {
			value *= 100
		}
		point.Value = &value
		if out.Reference != nil {
			point.Above = value > *out.Reference
		}
		out.Points = append(out.Points, point)
	}
	return out
}

func resourceAnomalies(d Definition, s prometheus.Series, p Policy) []Measurement {
	rows := []Measurement{}
	start := -1
	threshold := thresholdFor(d, p)
	factor := 1.0
	if d.ID == "H03" {
		factor = 100
	}
	flush := func(end int) {
		if start < 0 || end < start || s.Points[end].TimeMS-s.Points[start].TimeMS < p.SustainMS {
			return
		}
		peak := s.Points[start].Value
		for i := start; i <= end; i++ {
			if s.Points[i].Value > peak {
				peak = s.Points[i].Value
			}
		}
		rows = append(rows, Measurement{Labels: s.Labels, Status: "abnormal", Value: fmt.Sprintf("peak %.3g%%", peak*factor), Detail: fmt.Sprintf("reference %.3g%%; %.3g min above reference", threshold*factor, float64(s.Points[end].TimeMS-s.Points[start].TimeMS)/60000), FromMS: s.Points[start].TimeMS, ToMS: s.Points[end].TimeMS})
	}
	for i, x := range s.Points {
		if i > 0 && x.TimeMS-s.Points[i-1].TimeMS > p.MaxGapMS {
			flush(i - 1)
			start = -1
		}
		if x.Value > threshold {
			if start < 0 {
				start = i
			}
		} else {
			flush(i - 1)
			start = -1
		}
	}
	flush(len(s.Points) - 1)
	return rows
}

func RawCurve(d Definition, s prometheus.Series, p Policy) Curve { return rawCurve(d, s, p) }
func ResourceAnomalies(d Definition, s prometheus.Series, p Policy) []Measurement {
	return resourceAnomalies(d, s, p)
}

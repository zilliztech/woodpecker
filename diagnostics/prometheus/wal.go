package prometheus

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

// WALQuery is a catalog-owned query, never an arbitrary browser-supplied expression.
// The existing client still enforces deployment labels, response identity and budgets.
type WALQuery struct {
	ID        string
	Metric    string
	Kind      string // histogram, counter or gauge
	Unit      string
	Match     map[string]string
	Regex     map[string]string
	Component string
}
type WALCollection struct {
	Datasets     map[string]Dataset
	Observations []d.Observation
	Problems     []d.Problem
	Status       string
	Policy       Policy
}

// CollectWAL reuses the source-owned HTTP client/authentication/scope/parser.
// The callback runs serially and may publish advisory collection progress.
func CollectWAL(ctx context.Context, connection d.Connection, target d.Target, from, to time.Time, queries []WALQuery, limits d.Limits, progress func(string)) WALCollection {
	return collectWAL(ctx, connection, target, from, to, queries, limits, progress, appendPlan)
}

// CollectChecks applies one shared source query/input budget to the deduplicated
// atomic-check plan. It does not run legacy source/stage diagnostic rules.
func CollectChecks(ctx context.Context, connection d.Connection, target d.Target, from, to time.Time, queries []WALQuery, limits d.Limits, progress func(string)) WALCollection {
	return collectWAL(ctx, connection, target, from, to, queries, limits, progress, atomicPlan)
}

func collectWAL(ctx context.Context, connection d.Connection, target d.Target, from, to time.Time, queries []WALQuery, limits d.Limits, progress func(string), kind queryPlanKind) WALCollection {
	out := WALCollection{Datasets: map[string]Dataset{}, Observations: []d.Observation{}, Problems: []d.Problem{}, Status: "completed"}
	s, err := settingsFor(connection, kind)
	if err != nil {
		out.Status = "invalid"
		out.Problems = append(out.Problems, d.Problem{Code: "invalid_connection_settings", Message: "Check the source-owned Prometheus connection"})
		return out
	}
	out.Policy = s.Policy
	if target.Deployment != "" && target.Deployment != s.Deployment {
		out.Status = "invalid"
		out.Problems = append(out.Problems, d.Problem{Code: "deployment_binding_mismatch", Message: "Target and connection deployment differ"})
		return out
	}
	if from.IsZero() || !from.Before(to) || to.After(time.Now()) || to.Sub(from).Milliseconds() > s.Limits.MaxWindowMS || len(queries) > s.Limits.MaxQueries {
		out.Status = "invalid"
		out.Problems = append(out.Problems, d.Problem{Code: "query_budget_invalid", Message: "Window or query count exceeds the configured Prometheus bounds"})
		return out
	}
	token, user, password, err := auth(s)
	if err != nil {
		out.Status = "failed"
		out.Problems = append(out.Problems, d.Problem{Code: "authentication_unavailable", Message: "Configured authentication environment reference is unavailable"})
		return out
	}
	transport, err := d.HTTPTransport(s.TLS, connection.BaseDir)
	if err != nil {
		out.Status = "failed"
		out.Problems = append(out.Problems, d.Problem{Code: "tls_configuration_unavailable", Message: err.Error()})
		return out
	}
	defer transport.CloseIdleConnections()
	c := client{http: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, settings: s, token: token, user: user, password: password}
	budget := queryBudget{targets: map[string]bool{}}
	runID := uuid.NewString()
	for i, q := range queries {
		if ctx.Err() != nil {
			out.Status = "cancelled"
			break
		}
		if budget.bytes >= limits.MaxInputBytes {
			out.Status = "budget_exceeded"
			out.Problems = append(out.Problems, d.Problem{Code: "input_budget_exceeded", Message: "Remaining queries were not executed"})
			break
		}
		if !regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`).MatchString(q.Metric) || !d.Has([]string{"histogram", "counter", "gauge"}, q.Kind) {
			out.Status = "invalid"
			out.Problems = append(out.Problems, d.Problem{Code: "invalid_catalog_query", Message: "Invalid metric catalog entry"})
			break
		}
		validRegex := len(q.Regex) <= 8
		for key, value := range q.Regex {
			_, err := regexp.Compile(value)
			validRegex = validRegex && labelName.MatchString(key) && !strings.HasPrefix(key, "__") && len(value) <= 512 && err == nil
		}
		if !validRegex {
			out.Status = "invalid"
			out.Problems = append(out.Problems, d.Problem{Code: "invalid_catalog_regex", Message: "Invalid catalog matcher"})
			break
		}
		sig := signal{ID: q.ID, Metric: q.Metric, Component: q.Component, Match: q.Match, Regex: q.Regex, Gauge: q.Kind != "histogram", Factor: 1}
		expr := expression(s, sig, target, from, to)
		started := time.Now().UTC()
		if progress != nil {
			progress(q.ID)
		}
		data, code := c.query(ctx, expr, sig, target, from, to, &budget, limits)
		if q.Unit != "" {
			data.Unit = q.Unit
		}
		o := d.Observation{ID: runID + ":q" + strconv.Itoa(i), SourceID: "prometheus:" + q.ID, Kind: "prometheus_matrix", Target: target, StartedAt: started, FinishedAt: time.Now().UTC(), Status: "observed", Query: &d.SourceQuery{Language: "promql", Expression: expr, Executed: true}}
		o.Target.Deployment = s.Deployment
		if code != "" {
			o.Status = "unavailable"
			o.Code = code
			out.Status = "partial"
		} else {
			out.Datasets[q.ID] = data
		}
		o.Data, _ = json.Marshal(data)
		out.Observations = append(out.Observations, o)
		if isBudget(code) {
			out.Status = "budget_exceeded"
			break
		}
	}
	if ctx.Err() != nil {
		out.Status = "cancelled"
	}
	return out
}

// WALFact is a source-derived fact for one exact exporter population. Quantile
// bounds are explicitly named rather than reusing the legacy P95 JSON fields.
type WALWindow struct {
	FromMS  int64    `json:"from_ms"`
	ToMS    int64    `json:"to_ms"`
	Count   float64  `json:"count"`
	Lower   *float64 `json:"lower,omitempty"`
	Upper   *float64 `json:"upper,omitempty"`
	Code    string   `json:"code,omitempty"`
	Above   bool     `json:"above_reference"`
	Matched bool     `json:"matched_rule,omitempty"`
}

type WALFact struct {
	Assessment     string            `json:"assessment,omitempty"`
	Reading        string            `json:"reading,omitempty"`
	Windows        []WALWindow       `json:"windows,omitempty"`
	Population     string            `json:"population"`
	JoinKey        string            `json:"join_key"`
	ObservedFromMS int64             `json:"observed_from_ms,omitempty"`
	ObservedToMS   int64             `json:"observed_to_ms,omitempty"`
	Labels         map[string]string `json:"labels"`
	Code           string            `json:"code,omitempty"`
	Count          float64           `json:"count,omitempty"`
	First          float64           `json:"first,omitempty"`
	Last           float64           `json:"last,omitempty"`
	Minimum        float64           `json:"minimum,omitempty"`
	Peak           float64           `json:"peak,omitempty"`
	Delta          float64           `json:"delta,omitempty"`
	Rate           float64           `json:"rate,omitempty"`
	Quantile       float64           `json:"quantile,omitempty"`
	Lower          *float64          `json:"lower,omitempty"`
	Upper          *float64          `json:"upper,omitempty"`
	AboveForMS     int64             `json:"above_for_ms,omitempty"`
	Samples        []Point           `json:"samples,omitempty"`
}

func WALSimpleFacts(data Dataset, counter bool, maxGapMS int64) []WALFact {
	out := []WALFact{}
	for _, s := range data.Series {
		f := WALFact{Population: s.ID, JoinKey: walJoinKey(s.AllLabels), Labels: s.Labels, Code: quality(s.Points, data.From, data.To, maxGapMS, counter)}
		if len(data.Notes) > 0 && d.Has(data.Notes, "backend_annotations_withheld") {
			f.Code = "backend_partial_or_qualified_response"
		}
		if f.Code == "" {
			for _, p := range s.Points {
				if p.Value > 1<<53 || (counter && p.Value != math.Trunc(p.Value)) {
					f.Code = "counter_or_position_precision_unverifiable"
					break
				}
			}
		}
		if f.Code == "" {
			xs := s.Points
			f.ObservedFromMS = xs[0].TimeMS
			f.ObservedToMS = xs[len(xs)-1].TimeMS
			f.First = xs[0].Value
			f.Last = xs[len(xs)-1].Value
			f.Minimum = f.First
			f.Peak = f.First
			for _, p := range xs {
				f.Minimum = math.Min(f.Minimum, p.Value)
				f.Peak = math.Max(f.Peak, p.Value)
			}
			f.Delta = f.Last - f.First
			f.Rate = f.Delta / float64(xs[len(xs)-1].TimeMS-xs[0].TimeMS) * 1000
			f.Samples = xs
		}
		out = append(out, f)
	}
	return out
}

func WALHistogramFacts(data Dataset, metric string, quantile, threshold float64, minSamples int, maxGapMS int64) []WALFact {
	return WALHistogramFactsInWindow(data, metric, quantile, threshold, minSamples, maxGapMS, 60000)
}

func WALHistogramFactsInWindow(data Dataset, metric string, quantile, threshold float64, minSamples int, maxGapMS, calculationMS int64) []WALFact {
	if calculationMS <= 0 {
		calculationMS = 60000
	}
	calculation := time.Duration(calculationMS) * time.Millisecond
	groups := map[string][]Series{}
	labels := map[string]map[string]string{}
	for _, s := range data.Series {
		keyLabels := map[string]string{}
		for k, v := range s.AllLabels {
			if k != "__name__" && k != "le" {
				keyLabels[k] = v
			}
		}
		key := fingerprint(keyLabels)
		groups[key] = append(groups[key], s)
		pub := map[string]string{}
		for k, v := range s.Labels {
			if k != "__name__" && k != "le" {
				pub[k] = v
			}
		}
		labels[key] = pub
	}
	keys := []string{}
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := []WALFact{}
	for _, key := range keys {
		sig := signal{ID: metric, Metric: metric, Factor: 1}
		policy := Policy{MaxGapMS: maxGapMS, LatencyMS: map[string]float64{metric: threshold}}
		f, code := histogramAtQuantile(groups[key], sig, data, policy, quantile)
		fact := WALFact{Population: key, Labels: labels[key], Code: code, Count: f.Completions, Quantile: quantile, Lower: f.P95LowerMS, Upper: f.P95UpperMS}
		if d.Has(data.Notes, "backend_annotations_withheld") {
			fact.Code = "backend_partial_or_qualified_response"
		}
		if fact.Code == "" && f.Completions < float64(minSamples) {
			fact.Code = "insufficient_observations"
		}
		if fact.Code == "" || fact.Code == "insufficient_observations" {
			consecutive := int64(0)
			for start := data.From; !start.Add(calculation).After(data.To); start = start.Add(calculation) {
				end := start.Add(calculation)
				subset := []Series{}
				for _, s := range groups[key] {
					cp := s
					cp.Points = nil
					for _, p := range s.Points {
						if p.TimeMS >= start.UnixMilli() && p.TimeMS <= end.UnixMilli() {
							cp.Points = append(cp.Points, p)
						}
					}
					subset = append(subset, cp)
				}
				block := data
				block.From = start
				block.To = end
				one, c := histogramAtQuantile(subset, sig, block, policy, quantile)
				window := WALWindow{FromMS: start.UnixMilli(), ToMS: end.UnixMilli(), Count: one.Completions, Lower: one.P95LowerMS, Upper: one.P95UpperMS, Code: c}
				if window.Code == "" && window.Count < float64(minSamples) {
					window.Code = "insufficient_observations"
				}
				window.Above = window.Code == "" && window.Lower != nil && *window.Lower >= threshold
				fact.Windows = append(fact.Windows, window)
				if window.Above {
					consecutive += calculationMS
					fact.AboveForMS = max(fact.AboveForMS, consecutive)
				} else {
					consecutive = 0
				}
			}
		}
		out = append(out, fact)
	}
	return out
}

func WALConnectionInfo(c d.Connection) (string, error) {
	s, e := settingsFor(c, appendPlan)
	return s.Deployment, e
}

// FormatWALBounds never turns an unbounded final bucket into an exact latency.
func FormatWALBounds(f WALFact, unit string) string {
	if f.Lower == nil {
		return "No quantile"
	}
	if f.Upper == nil {
		return fmt.Sprintf(">= %s %s", strconv.FormatFloat(*f.Lower, 'f', -1, 64), unit)
	}
	return fmt.Sprintf("%s–%s %s", strconv.FormatFloat(*f.Lower, 'f', -1, 64), strconv.FormatFloat(*f.Upper, 'f', -1, 64), unit)
}

func walLabelsKey(labels map[string]string) string {
	cp := map[string]string{}
	for k, v := range labels {
		if k != "__name__" {
			cp[k] = v
		}
	}
	return fingerprint(cp)
}

// WALPositionFacts pairs samples only with exactly matching exporter labels and
// scrape timestamps. Segment transitions are retained, not subtracted.
func WALPositionFacts(segment, entry Dataset, maxGapMS int64) []WALFact {
	out := []WALFact{}
	entries := map[string]Series{}
	for _, s := range entry.Series {
		entries[walLabelsKey(s.AllLabels)] = s
	}
	for _, s := range segment.Series {
		e, ok := entries[walLabelsKey(s.AllLabels)]
		f := WALFact{Population: s.ID, JoinKey: walJoinKey(s.AllLabels), Labels: s.Labels}
		if !ok {
			f.Code = "missing_position_pair"
		} else if c := quality(s.Points, segment.From, segment.To, maxGapMS, false); c != "" {
			f.Code = c
		} else if c := quality(e.Points, entry.From, entry.To, maxGapMS, false); c != "" {
			f.Code = c
		} else if len(s.Points) != len(e.Points) {
			f.Code = "position_sample_misalignment"
		}
		if f.Code == "" {
			advanced := false
			f.ObservedFromMS = s.Points[0].TimeMS
			f.ObservedToMS = s.Points[len(s.Points)-1].TimeMS
			for i, p := range s.Points {
				if p.TimeMS != e.Points[i].TimeMS || p.Value != math.Trunc(p.Value) || e.Points[i].Value != math.Trunc(e.Points[i].Value) || p.Value > 1<<53 || e.Points[i].Value > 1<<53 {
					f.Code = "position_precision_or_alignment"
					break
				}
				if i > 0 && (p.Value < s.Points[i-1].Value || (p.Value == s.Points[i-1].Value && e.Points[i].Value < e.Points[i-1].Value)) {
					f.Code = "position_regressed_or_restarted"
					break
				}
				if i > 0 && (p.Value > s.Points[i-1].Value || e.Points[i].Value > e.Points[i-1].Value) {
					advanced = true
				}
			}
			if advanced {
				f.Delta = 1
			}
			f.First = s.Points[0].Value
			f.Last = s.Points[len(s.Points)-1].Value
			f.Count = e.Points[len(e.Points)-1].Value
		}
		out = append(out, f)
	}
	return out
}

// ValidateWALMatch prevents reserved name matchers from changing a catalog query.
func ValidateWALMatch(m map[string]string) bool {
	for k, v := range m {
		if !labelName.MatchString(k) || strings.HasPrefix(k, "__") || len(v) > 512 || strings.ContainsAny(v, "\x00\r\n") {
			return false
		}
	}
	return true
}

// Preserve unexposed exporter dimensions in joins without publishing their values.
func walJoinKey(labels map[string]string) string {
	cp := map[string]string{}
	for k, v := range labels {
		if k != "__name__" && k != "status" {
			cp[k] = v
		}
	}
	return fingerprint(cp)
}

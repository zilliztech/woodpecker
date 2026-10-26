package prometheus

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

type Point struct {
	TimeMS int64   `json:"time_ms"`
	Value  float64 `json:"value"`
}
type Series struct {
	ID        string            `json:"series_id"`
	Labels    map[string]string `json:"labels"`
	Points    []Point           `json:"samples"`
	AllLabels map[string]string `json:"-"`
}
type Dataset struct {
	Signal string    `json:"signal"`
	From   time.Time `json:"from"`
	To     time.Time `json:"to"`
	Unit   string    `json:"unit"`
	Series []Series  `json:"series"`
	Notes  []string  `json:"notes"`
	Facts  []Fact    `json:"facts"`
}
type queryBudget struct {
	bytes           int64
	series, samples int
	targets         map[string]bool
}
type client struct {
	http                  *http.Client
	settings              Settings
	token, user, password string
}

func fingerprint(value any) string {
	b, _ := json.Marshal(value)
	hash := sha256.Sum256(b)
	return hex.EncodeToString(hash[:])
}

func expression(s Settings, sig signal, target d.Target, from, to time.Time) string {
	labels := map[string]string{}
	for k, v := range s.MatchLabels {
		labels[k] = v
	}
	if sig.Component != "" {
		labels["component"] = sig.Component
	}
	if target.Instance != "" {
		labels["instance"] = target.Instance
	}
	if target.NodeID != "" {
		labels["node_id"] = target.NodeID
	}
	for k, v := range sig.Match {
		labels[k] = v
	}
	parts := []string{}
	for k, v := range labels {
		parts = append(parts, k+"="+strconv.Quote(v))
	}
	for k, v := range sig.Regex {
		parts = append(parts, k+"=~"+strconv.Quote(v))
	}
	name := sig.Metric
	if sig.Gauge {
		parts = append(parts, "__name__="+strconv.Quote(name))
	} else {
		parts = append(parts, "__name__=~"+strconv.Quote(regexp.QuoteMeta(name)+"_(bucket|count|sum)"))
	}
	sort.Strings(parts)
	return "{" + strings.Join(parts, ",") + "}[" + strconv.FormatInt(to.Sub(from).Milliseconds(), 10) + "ms] @ " + strconv.FormatFloat(float64(to.UnixMilli())/1000, 'f', 3, 64)
}

func (c client) query(ctx context.Context, expr string, sig signal, target d.Target, from, to time.Time, budget *queryBudget, limits d.Limits) (Dataset, string) {
	data := Dataset{Signal: sig.ID, From: from, To: to, Unit: "milliseconds", Series: []Series{}, Notes: []string{}, Facts: []Fact{}}
	if sig.Gauge {
		data.Unit = "admission_state_0_normal_1_warn_2_blocked"
		if sig.GaugeUnit != "" {
			data.Unit = sig.GaugeUnit
		}
	} else if sig.Factor == 1000 {
		data.Unit = "seconds (histogram); milliseconds (derived latency)"
	}
	u, _ := url.Parse(c.settings.Endpoint)
	u.Path = strings.TrimSuffix(u.Path, "/") + "/api/v1/query"
	q := u.Query()
	q.Set("query", expr)
	q.Set("time", strconv.FormatFloat(float64(to.UnixMilli())/1000, 'f', 3, 64))
	q.Set("timeout", (time.Duration(c.settings.Limits.TimeoutMS) * time.Millisecond).String())
	q.Set("limit", strconv.Itoa(c.settings.Limits.MaxSeries-budget.series+1))
	u.RawQuery = q.Encode()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(c.settings.Limits.TimeoutMS)*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return data, "invalid_request"
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}
	response, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return data, "query_timeout_or_cancelled"
		}
		return data, "transport_failed"
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return data, fmt.Sprintf("http_%d", response.StatusCode)
	}
	capBytes := min(c.settings.Limits.MaxResponseBytes, limits.MaxInputBytes-budget.bytes)
	if capBytes < 1 {
		return data, "input_budget_exceeded"
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, capBytes+1))
	budget.bytes += int64(len(body))
	if int64(len(body)) > capBytes {
		return data, "input_budget_exceeded"
	}
	if err != nil {
		return data, "response_read_failed"
	}
	var wire struct {
		Status   string            `json:"status"`
		Warnings []json.RawMessage `json:"warnings"`
		Infos    []json.RawMessage `json:"infos"`
		Data     struct {
			Type   string `json:"resultType"`
			Result []struct {
				Metric     map[string]string   `json:"metric"`
				Values     [][]json.RawMessage `json:"values"`
				Histograms json.RawMessage     `json:"histograms"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &wire) != nil || wire.Status != "success" || wire.Data.Type != "matrix" {
		return data, "invalid_prometheus_response"
	}
	if len(wire.Warnings) > 0 || len(wire.Infos) > 0 {
		data.Notes = append(data.Notes, "backend_annotations_withheld")
	}
	budget.series += len(wire.Data.Result)
	if budget.series > c.settings.Limits.MaxSeries {
		return data, "series_budget_exceeded"
	}
	seen := map[string]string{}
	for _, item := range wire.Data.Result {
		budget.samples += len(item.Values)
		if budget.samples > c.settings.Limits.MaxSamples {
			return data, "sample_budget_exceeded"
		}
		if len(item.Histograms) > 0 && string(item.Histograms) != "null" && string(item.Histograms) != "[]" {
			return data, "native_histogram_not_supported"
		}
		if !matches(c.settings, sig, target, item.Metric) {
			return data, "series_scope_mismatch"
		}
		instance := item.Metric["instance"]
		if instance == "" || (sig.Component == "woodpecker" && item.Metric["node_id"] == "") {
			return data, "missing_exporter_identity"
		}
		budget.targets[instance] = true
		if len(budget.targets) > limits.MaxTargets {
			return data, "target_budget_exceeded"
		}
		series := Series{ID: fingerprint(item.Metric), Labels: publicLabels(item.Metric, c.settings), AllLabels: item.Metric, Points: []Point{}}
		for _, pair := range item.Values {
			if len(pair) != 2 {
				return data, "invalid_sample"
			}
			var ts float64
			var value string
			if json.Unmarshal(pair[0], &ts) != nil || json.Unmarshal(pair[1], &value) != nil || math.IsNaN(ts) || math.IsInf(ts, 0) || ts < 0 || ts > float64(to.UnixMilli())/1000 {
				return data, "invalid_sample"
			}
			v, err := strconv.ParseFloat(value, 64)
			if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 1e300 {
				return data, "invalid_sample"
			}
			point := Point{TimeMS: int64(math.Round(ts * 1000)), Value: v}
			if point.TimeMS <= from.UnixMilli() || point.TimeMS > to.UnixMilli() {
				return data, "sample_outside_window"
			}
			if len(series.Points) > 0 {
				last := series.Points[len(series.Points)-1]
				if point.TimeMS < last.TimeMS || (point.TimeMS == last.TimeMS && point.Value != last.Value) {
					return data, "conflicting_sample_identity"
				}
				if point == last {
					data.Notes = append(data.Notes, "identical_sample_deduplicated")
					continue
				}
			}
			series.Points = append(series.Points, point)
		}
		if previous, ok := seen[series.ID]; ok {
			if previous != fingerprint(series.Points) {
				return data, "conflicting_series_identity"
			}
			data.Notes = append(data.Notes, "identical_series_deduplicated")
			continue
		}
		seen[series.ID] = fingerprint(series.Points)
		data.Series = append(data.Series, series)
	}
	return data, ""
}

func matches(s Settings, sig signal, target d.Target, labels map[string]string) bool {
	for k, v := range s.MatchLabels {
		if labels[k] != v {
			return false
		}
	}
	if (sig.Component != "" && labels["component"] != sig.Component) || (target.Instance != "" && labels["instance"] != target.Instance) || (target.NodeID != "" && labels["node_id"] != target.NodeID) {
		return false
	}
	for k, v := range sig.Match {
		if labels[k] != v {
			return false
		}
	}
	for k, v := range sig.Regex {
		if !regexp.MustCompile("^(?:" + v + ")$").MatchString(labels[k]) {
			return false
		}
	}
	if sig.Gauge {
		return labels["__name__"] == sig.Metric
	}
	return d.Has([]string{sig.Metric + "_count", sig.Metric + "_sum", sig.Metric + "_bucket"}, labels["__name__"])
}

func publicLabels(labels map[string]string, s Settings) map[string]string {
	out := map[string]string{}
	for k, v := range labels {
		if d.Has([]string{"__name__", "le", "instance", "node_id", "log_ns", "log_id", "status", "operation", "component", "pod", "namespace", "cluster", "woodpecker_id", "msg_type", "query_type", "channel_name", "job", "collection_id", "collection_name", "database", "resource_group", "db_name", "rg", "segment_state", "reduce_level", "reduce_type"}, k) || s.MatchLabels[k] != "" {
			out[k] = v
		}
	}
	return out
}

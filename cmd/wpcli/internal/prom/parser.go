// Package prom provides helpers for scraping and parsing Prometheus text
// exposition format from Woodpecker server /metrics endpoints.
package prom

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// MetricFamily is the parsed Prometheus metric family.
type MetricFamily = dto.MetricFamily

// Parse reads Prometheus text format from r and returns metric families keyed by name.
func Parse(r io.Reader) (map[string]*MetricFamily, error) {
	var parser expfmt.TextParser
	return parser.TextToMetricFamilies(r)
}

// ScrapeNode fetches GET /metrics from the given base URL and parses it.
func ScrapeNode(baseURL string, timeout time.Duration) (map[string]*MetricFamily, error) {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(baseURL + "/metrics")
	if err != nil {
		return nil, fmt.Errorf("scrape %s: %w", baseURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrape %s: status %d", baseURL, resp.StatusCode)
	}
	return Parse(resp.Body)
}

// MetricInfo is a summary of a metric family for listing.
type MetricInfo struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Help        string `json:"help"`
	SeriesCount int    `json:"series_count"`
}

// ListMetrics returns a sorted summary of all metric families.
func ListMetrics(families map[string]*MetricFamily) []MetricInfo {
	var result []MetricInfo
	for name, mf := range families {
		info := MetricInfo{
			Name:        name,
			Help:        mf.GetHelp(),
			Type:        strings.ToLower(mf.GetType().String()),
			SeriesCount: len(mf.GetMetric()),
		}
		result = append(result, info)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// LabelMap converts a Prometheus metric's label pairs to a map.
func LabelMap(m *dto.Metric) map[string]string {
	result := make(map[string]string, len(m.GetLabel()))
	for _, lp := range m.GetLabel() {
		result[lp.GetName()] = lp.GetValue()
	}
	return result
}

// LabelsMatch returns true if the metric has all the specified label key=value pairs.
func LabelsMatch(m *dto.Metric, filter map[string]string) bool {
	labels := LabelMap(m)
	for k, v := range filter {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// GetGaugeValue extracts the gauge value from metrics matching the given labels.
// Returns the value and true if found, or 0 and false if not.
func GetGaugeValue(mf *MetricFamily, labels map[string]string) (float64, bool) {
	for _, m := range mf.GetMetric() {
		if LabelsMatch(m, labels) && m.GetGauge() != nil {
			return m.GetGauge().GetValue(), true
		}
	}
	return 0, false
}

// GetCounterValue extracts the counter value from metrics matching the given labels.
func GetCounterValue(mf *MetricFamily, labels map[string]string) (float64, bool) {
	for _, m := range mf.GetMetric() {
		if LabelsMatch(m, labels) && m.GetCounter() != nil {
			return m.GetCounter().GetValue(), true
		}
	}
	return 0, false
}

// GetHistogramCount extracts the histogram sample count from metrics matching labels.
func GetHistogramCount(mf *MetricFamily, labels map[string]string) (uint64, bool) {
	for _, m := range mf.GetMetric() {
		if LabelsMatch(m, labels) && m.GetHistogram() != nil {
			return m.GetHistogram().GetSampleCount(), true
		}
	}
	return 0, false
}

// MetricValue extracts a single numeric value from a metric, regardless of type.
func MetricValue(m *dto.Metric) float64 {
	if m.GetGauge() != nil {
		return m.GetGauge().GetValue()
	}
	if m.GetCounter() != nil {
		return m.GetCounter().GetValue()
	}
	if m.GetUntyped() != nil {
		return m.GetUntyped().GetValue()
	}
	if m.GetHistogram() != nil {
		return m.GetHistogram().GetSampleSum()
	}
	if m.GetSummary() != nil {
		return m.GetSummary().GetSampleSum()
	}
	return 0
}

// FormatLabels renders label pairs as a compact string like {k1="v1",k2="v2"}.
func FormatLabels(m *dto.Metric) string {
	labels := m.GetLabel()
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, len(labels))
	for i, lp := range labels {
		parts[i] = fmt.Sprintf("%s=%q", lp.GetName(), lp.GetValue())
	}
	return "{" + strings.Join(parts, ",") + "}"
}

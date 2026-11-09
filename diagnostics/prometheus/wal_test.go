package prometheus

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func walHistogram(t *testing.T, end time.Time) Dataset {
	t.Helper()
	out := Dataset{From: end.Add(-5 * time.Minute), To: end}
	counts := map[string]float64{"_count": 100, "_sum": 50000, "_bucket:10": 0, "_bucket:100": 0, "_bucket:1000": 100, "_bucket:+Inf": 100}
	for key, step := range counts {
		metric, bound, _ := strings.Cut(key, ":")
		labels := map[string]string{"__name__": "latency" + metric, "instance": "wp:9091", "node_id": "node-1", "log_ns": "tenant", "log_id": "1"}
		if bound != "" {
			labels["le"] = bound
		}
		s := Series{ID: key, Labels: labels, AllLabels: labels}
		for i := 0; i <= 30; i++ {
			s.Points = append(s.Points, Point{TimeMS: out.From.Add(time.Duration(i) * 10 * time.Second).UnixMilli(), Value: float64(i) * step})
		}
		out.Series = append(out.Series, s)
	}
	return out
}

func TestWALP99KeepsBoundsAndSustainedCoverage(t *testing.T) {
	data := walHistogram(t, time.Unix(1800000000, 0))
	fs := WALHistogramFacts(data, "latency", .99, 100, 100, 30000)
	require.Len(t, fs, 1)
	f := fs[0]
	require.Empty(t, f.Code)
	require.Equal(t, .99, f.Quantile)
	require.Equal(t, 100.0, *f.Lower)
	require.Equal(t, 1000.0, *f.Upper)
	require.EqualValues(t, 300000, f.AboveForMS)
	// A missing scrape in the full window withholds an otherwise high percentile.
	data.Series[0].Points = append(data.Series[0].Points[:10], data.Series[0].Points[15:]...)
	require.NotEmpty(t, WALHistogramFacts(data, "latency", .99, 100, 100, 30000)[0].Code)
	data = walHistogram(t, time.Unix(1800000000, 0))
	require.Equal(t, "insufficient_observations", WALHistogramFacts(data, "latency", .99, 100, 10000, 30000)[0].Code)
}

func TestWALPositionRequiresExactPairAndPreservesSegmentTransition(t *testing.T) {
	from := time.Unix(1800000000, 0)
	labels := map[string]string{"instance": "x", "log_id": "1", "log_ns": "t"}
	mk := func(name string, values ...float64) Dataset {
		all := map[string]string{"__name__": name, "instance": "x", "log_id": "1", "log_ns": "t"}
		s := Series{ID: name, Labels: labels, AllLabels: all}
		for i, v := range values {
			s.Points = append(s.Points, Point{from.Add(time.Duration(i) * 10 * time.Second).UnixMilli(), v})
		}
		return Dataset{From: from, To: from.Add(20 * time.Second), Series: []Series{s}}
	}
	seg, entry := mk("segment", 1, 1, 2), mk("entry", 100, 120, 0)
	f := WALPositionFacts(seg, entry, 30000)[0]
	require.Empty(t, f.Code)
	require.Equal(t, 1.0, f.Delta)
	entry.Series[0].Points[2].TimeMS++
	require.Equal(t, "position_precision_or_alignment", WALPositionFacts(seg, entry, 30000)[0].Code)
	require.Equal(t, "missing_position_pair", WALPositionFacts(seg, Dataset{}, 30000)[0].Code)
}

func TestWALCollectorScopesQueriesAndRejectsBudgetBeforeIO(t *testing.T) {
	calls := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		q := r.URL.Query().Get("query")
		require.Contains(t, q, `cluster="test"`)
		require.Contains(t, q, `log_id="1"`)
		json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": []any{}}})
	}))
	defer backend.Close()
	c := connection(t, backend.URL, nil)
	to := time.Now().Add(-time.Second)
	from := to.Add(-5 * time.Minute)
	queries := []WALQuery{{ID: "W04", Metric: "woodpecker_client_append_requests_total", Kind: "counter", Match: map[string]string{"log_id": "1"}}}
	got := CollectWAL(context.Background(), c, d.Target{}, from, to, queries, d.DefaultLimits(), nil)
	require.Equal(t, "completed", got.Status)
	require.Equal(t, 1, calls)
	require.Len(t, got.Observations, 1)
	require.Contains(t, string(got.Observations[0].Data), `"series":[]`)
	queries = make([]WALQuery, 33)
	got = CollectWAL(context.Background(), c, d.Target{}, from, to, queries, d.DefaultLimits(), nil)
	require.Equal(t, "invalid", got.Status)
	require.Equal(t, 1, calls)
	require.Contains(t, fmt.Sprint(got.Problems), "query_budget_invalid")
}

func TestWALP99DoesNotRelabelLegacyP95(t *testing.T) {
	data := walHistogram(t, time.Unix(1800000000, 0))
	for i := range data.Series {
		s := &data.Series[i]
		step := -1.0
		if s.AllLabels["__name__"] == "latency_sum" {
			step = 2975
		}
		if s.AllLabels["le"] == "10" || s.AllLabels["le"] == "100" {
			step = 95
		}
		if step >= 0 {
			for j := range s.Points {
				s.Points[j].Value = float64(j) * step
			}
		}
	}
	p95 := WALHistogramFacts(data, "latency", .95, 100, 100, 30000)[0]
	p99 := WALHistogramFacts(data, "latency", .99, 100, 100, 30000)[0]
	require.Empty(t, p95.Code)
	require.Empty(t, p99.Code)
	require.Equal(t, 10.0, *p95.Upper)
	require.Equal(t, 100.0, *p99.Lower)
	require.Equal(t, 1000.0, *p99.Upper)
}

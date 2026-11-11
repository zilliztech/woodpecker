package prometheus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func TestCounterSourceFactsAndResetCoverage(t *testing.T) {
	for _, tc := range []struct {
		name      string
		values    []float64
		want, gap string
	}{
		{"increase", []float64{0, 20, 40, 60, 80, 100}, "counter_increase_observed", ""},
		{"idle", []float64{10, 10, 10, 10, 10, 10}, "idle_or_no_events", ""},
		{"reset", []float64{0, 20, 40, 0, 20, 40}, "inconclusive", "counter_reset_or_restart"},
		{"short", []float64{1}, "inconclusive", "insufficient_scrapes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			points := []Point{}
			for i, v := range tc.values {
				points = append(points, Point{TimeMS: 1005000 + int64(i)*10000, Value: v})
			}
			labels := map[string]string{"__name__": "counter_total", "cluster": "test", "instance": "sn:9091", "node_id": "sn"}
			data := Dataset{From: time.Unix(1000, 0), To: time.Unix(1060, 0), Series: []Series{{Labels: labels, AllLabels: labels, Points: points}}}
			sig := signal{ID: "milvus.consume.scanned", Metric: "counter_total", Stage: "consume", Gauge: true, GaugeUnit: "counter_messages"}
			r := request("milvus", "read")
			findings := evaluate(&data, sig, r, Policy{MaxGapMS: 30000}, "fixture:q0")
			require.Len(t, findings, 1)
			require.Equal(t, tc.want, findings[0].Outcome)
			require.Equal(t, "milvus.consume.scanned.counter", findings[0].RuleID)
			if tc.gap != "" {
				require.Contains(t, findings[0].Unknown, tc.gap)
				require.Empty(t, data.Facts)
			} else {
				require.Len(t, data.Facts, 1)
				require.EqualValues(t, 1005000, data.Facts[0].ObservedFromMS)
				require.Equal(t, tc.values[len(tc.values)-1]-tc.values[0], data.Facts[0].Counter.Increase)
			}
		})
	}
}

func TestConsumeCatalogConnectionAndBoundedMissingSignals(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": []any{}}})
	}))
	defer server.Close()
	engine, e := d.NewEngine(ConsumeModule{})
	require.NoError(t, e)
	for _, scope := range []string{"milvus", "e2e"} {
		cfg := d.Config{Sources: map[string]d.SourceConfig{"prometheus": {Connections: map[string]d.Connection{"fixture": connection(t, server.URL, func(s *Settings) { s.Limits.MaxQueries = 24 })}}}}
		r := request(scope, "read")
		report := engine.Run(context.Background(), cfg, r)
		require.Equal(t, "completed", report.Status)
		expected := 15
		if scope == "e2e" {
			expected = 20
		}
		require.Len(t, report.Results[0].Observations, expected)
		require.Len(t, report.Results[0].Findings, expected)
		for _, f := range report.Results[0].Findings {
			require.Equal(t, "inconclusive", f.Outcome)
		}
	}
	require.Equal(t, 35, calls)
	cfg := d.Config{Sources: map[string]d.SourceConfig{"prometheus": {Connections: map[string]d.Connection{"fixture": connection(t, server.URL, func(s *Settings) { s.Limits.MaxQueries = 1 })}}}}
	report := engine.Run(context.Background(), cfg, request("e2e", "read"))
	require.Equal(t, "invalid", report.Status)
	require.Equal(t, 35, calls)
	for _, sig := range consumeSignals("e2e") {
		expr := expression(Settings{MatchLabels: map[string]string{"cluster": "test"}}, sig, d.Target{}, time.Unix(1000, 0), time.Unix(1060, 0))
		require.Contains(t, expr, sig.Metric)
	}
}

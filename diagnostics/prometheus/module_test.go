package prometheus

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

var updateFixtures = flag.Bool("update-prometheus-fixtures", false, "Update the sanitized Prometheus report fixture")

func TestVersionedReportFixture(t *testing.T) {
	server := serve(t, nil)
	report := run(t, server.URL, "wp", "read", nil)
	report.RunID = "fixture-460-wrapper"
	report.StartedAt = time.Unix(2000, 0).UTC()
	report.FinishedAt = time.Unix(2010, 0).UTC()
	for i := range report.Results[0].Observations {
		report.Results[0].Observations[i].StartedAt = time.Unix(2001+int64(i), 0).UTC()
		report.Results[0].Observations[i].FinishedAt = time.Unix(2002+int64(i), 0).UTC()
	}
	var metadata struct {
		RunID string `json:"source_run_id"`
	}
	require.NoError(t, json.Unmarshal(report.Results[0].Metadata, &metadata))
	body, _, err := d.EncodeReport(report)
	require.NoError(t, err)
	body = []byte(strings.ReplaceAll(string(body), metadata.RunID, "fixture-460-source"))
	path := filepath.Join("testdata", "wp-read-slow-v1.json")
	if *updateFixtures {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, body, 0o644))
	}
	expected, err := os.ReadFile(path)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(body))
}

func request(scope, flow string) d.Request {
	from, to := time.Unix(1000, 0).UTC(), time.Unix(1060, 0).UTC()
	return d.Request{Selection: d.Selection{Sources: []string{"prometheus"}, Scope: scope, Flow: flow, Window: d.Window{Mode: "historical", From: &from, To: &to}}, Connection: "fixture", Limits: d.DefaultLimits()}
}

func connection(t *testing.T, endpoint string, modify func(*Settings)) d.Connection {
	t.Helper()
	var c d.Connection
	var node yaml.Node
	require.NoError(t, yaml.Unmarshal([]byte("endpoint: "+strconv.Quote(endpoint)+"\ndeployment: fixture\nmatch_labels: {cluster: test}\n"), &node))
	c.Settings = *node.Content[0]
	s, err := settings(c)
	require.NoError(t, err)
	if modify != nil {
		modify(&s)
	}
	body, err := yaml.Marshal(s)
	require.NoError(t, err)
	require.NoError(t, yaml.Unmarshal(body, &node))
	c.Settings = *node.Content[0]
	return c
}

func run(t *testing.T, endpoint, scope, flow string, modify func(*Settings)) d.Report {
	t.Helper()
	engine, err := d.NewEngine(Module{})
	require.NoError(t, err)
	cfg := d.Config{SchemaVersion: d.ConfigSchema, Sources: map[string]d.SourceConfig{"prometheus": {Connections: map[string]d.Connection{"fixture": connection(t, endpoint, modify)}}}}
	return engine.Run(context.Background(), cfg, request(scope, flow))
}

type wireSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][]any           `json:"values"`
}

func responseSeries(t *testing.T, query string, slow bool) []wireSeries {
	t.Helper()
	parts := regexp.MustCompile(`\[(\d+)ms\] @ ([0-9.]+)$`).FindStringSubmatch(query)
	require.Len(t, parts, 3)
	duration, err := strconv.ParseInt(parts[1], 10, 64)
	require.NoError(t, err)
	end, err := strconv.ParseFloat(parts[2], 64)
	require.NoError(t, err)
	from := int64(end*1000) - duration
	var sig signal
	candidates := append(signals("e2e", "write"), signals("e2e", "read")...)
	candidates = append(candidates, querySignals()...)
	for _, candidate := range candidates {
		if strings.Contains(query, candidate.Metric) && !(candidate.ID == "wp.append" && strings.Contains(query, "get_batch_entries")) {
			sig = candidate
			break
		}
	}
	require.NotEmpty(t, sig.Metric)
	component := sig.Component
	if component == "" {
		component = "querynode"
	}
	labels := map[string]string{"cluster": "test", "instance": component + ":9091", "component": component, "password": "sentinel-secret"}
	if sig.Component == "woodpecker" {
		labels["node_id"] = "node-1"
		if !sig.Gauge {
			labels["log_ns"] = "tenant-a"
			labels["log_id"] = "2"
		}
	}
	for k, v := range sig.Match {
		labels[k] = v
	}
	for k, v := range sig.Regex {
		labels[k] = strings.Split(v, "|")[0]
	}
	makeSeries := func(name, le string, increment float64) wireSeries {
		m := map[string]string{}
		for k, v := range labels {
			m[k] = v
		}
		m["__name__"] = name
		if le != "" {
			m["le"] = le
		}
		values := [][]any{}
		for i := 0; i < 6; i++ {
			ts := from + int64(i+1)*duration/7
			values = append(values, []any{float64(ts) / 1000, strconv.FormatFloat(float64(i)*increment, 'f', -1, 64)})
		}
		return wireSeries{m, values}
	}
	if sig.Gauge {
		series := makeSeries(sig.Metric, "", 0)
		if slow {
			for i := range series.Values {
				series.Values[i][1] = "2"
			}
		}
		return []wireSeries{series}
	}
	latency := 50.0
	if slow {
		latency = 3000
	}
	latency /= sig.Factor
	result := []wireSeries{makeSeries(sig.Metric+"_count", "", 20), makeSeries(sig.Metric+"_sum", "", 20*latency)}
	for _, upper := range []float64{100, 256, 2048, 4096} {
		increment := 0.0
		if upper >= latency*sig.Factor {
			increment = 20
		}
		result = append(result, makeSeries(sig.Metric+"_bucket", strconv.FormatFloat(upper/sig.Factor, 'f', -1, 64), increment))
	}
	return append(result, makeSeries(sig.Metric+"_bucket", "+Inf", 20))
}

func serve(t *testing.T, modify func(string, []wireSeries) ([]wireSeries, int, []string)) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/query", r.URL.Path)
		query := r.URL.Query().Get("query")
		series := responseSeries(t, query, true)
		status := 200
		warnings := []string{}
		if modify != nil {
			series, status, warnings = modify(query, series)
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": series}, "warnings": warnings})
	}))
	t.Cleanup(server.Close)
	return server
}

func TestAllProfilesPreserveSourceOnlyEvidence(t *testing.T) {
	server := serve(t, nil)
	for _, scope := range []string{"wp", "milvus", "e2e"} {
		for _, flow := range []string{"write", "read"} {
			t.Run(scope+"/"+flow, func(t *testing.T) {
				report := run(t, server.URL, scope, flow, nil)
				require.Equal(t, "completed", report.Status)
				result := report.Results[0]
				require.NotEmpty(t, result.Findings)
				ids := map[string]bool{}
				for _, o := range result.Observations {
					ids[o.ID] = true
					require.True(t, o.Query.Executed)
					require.Contains(t, o.Query.Expression, `cluster="test"`)
					require.Contains(t, o.Query.Expression, " @ 1060.000")
					require.NotContains(t, string(o.Data), "sentinel-secret")
				}
				for _, f := range result.Findings {
					require.Contains(t, []string{"suspected_high_latency", "observed_admission_pressure"}, f.Outcome)
					require.Equal(t, "fixture", f.Target.Deployment)
					for _, id := range f.Supporting {
						require.True(t, ids[id])
					}
					require.Contains(t, f.Unknown, "events_between_scrapes_unobservable")
				}
				body, _, err := d.EncodeReport(report)
				require.NoError(t, err)
				require.NotContains(t, string(body), server.URL)
				require.NotContains(t, string(body), "sentinel-secret")
				if scope == "e2e" {
					require.Equal(t, "source_only_partial_stage_coverage", result.Gaps[len(result.Gaps)-1].Code)
				}
			})
		}
	}
}

func TestHealthyIdleAndInsufficientEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, want, unknown string
		edit                func([]wireSeries) []wireSeries
	}{
		{"healthy", "within_configured_latency_for_observed_completions", "", func(s []wireSeries) []wireSeries {
			for i := range s {
				if strings.HasSuffix(s[i].Metric["__name__"], "_bucket") {
					s[i].Values = s[0].Values
				}
			}
			return s
		}},
		{"idle", "idle_or_no_completions", "expected_work_unknown", func(s []wireSeries) []wireSeries {
			for i := range s {
				for j := range s[i].Values {
					s[i].Values[j][1] = "0"
				}
			}
			return s
		}},
		{"empty", "inconclusive", "no_matching_series", func(_ []wireSeries) []wireSeries { return []wireSeries{} }},
		{"reset", "inconclusive", "counter_reset_or_restart", func(s []wireSeries) []wireSeries { s[0].Values[3][1] = "0"; return s }},
		{"sparse", "inconclusive", "scrape_gap", func(s []wireSeries) []wireSeries {
			for i := range s {
				s[i].Values = append(s[i].Values[:1], s[i].Values[5:]...)
			}
			return s
		}},
		{"missing sum", "inconclusive", "missing_histogram_family", func(s []wireSeries) []wireSeries { return append(s[:1], s[2:]...) }},
		{"misaligned", "inconclusive", "histogram_sample_misalignment", func(s []wireSeries) []wireSeries { s[1].Values[1][0] = 1020.0; return s }},
		{"insufficient samples", "inconclusive", "insufficient_scrapes", func(s []wireSeries) []wireSeries {
			for i := range s {
				s[i].Values = s[i].Values[:1]
			}
			return s
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := serve(t, func(_ string, s []wireSeries) ([]wireSeries, int, []string) { return tc.edit(s), 200, nil })
			report := run(t, server.URL, "wp", "read", nil)
			require.Equal(t, "completed", report.Status)
			f := report.Results[0].Findings[0]
			require.Equal(t, tc.want, f.Outcome)
			if tc.unknown != "" {
				require.Contains(t, f.Unknown, tc.unknown)
			}
		})
	}
}

func TestTransportScopeAndDataBudgets(t *testing.T) {
	for _, tc := range []struct {
		name, code, status string
		edit               func(string, []wireSeries) ([]wireSeries, int, []string)
		settings           func(*Settings)
	}{
		{"auth failure", "http_401", "failed", func(_ string, s []wireSeries) ([]wireSeries, int, []string) { return s, 401, nil }, nil},
		{"wrong labels", "series_scope_mismatch", "failed", func(_ string, s []wireSeries) ([]wireSeries, int, []string) {
			s[0].Metric["cluster"] = "other"
			return s, 200, nil
		}, nil},
		{"missing identity", "missing_exporter_identity", "failed", func(_ string, s []wireSeries) ([]wireSeries, int, []string) {
			delete(s[0].Metric, "instance")
			return s, 200, nil
		}, nil},
		{"conflicting point", "conflicting_sample_identity", "failed", func(_ string, s []wireSeries) ([]wireSeries, int, []string) {
			s[0].Values[1][0] = s[0].Values[0][0]
			return s, 200, nil
		}, nil},
		{"NaN", "invalid_sample", "failed", func(_ string, s []wireSeries) ([]wireSeries, int, []string) {
			s[0].Values[1][1] = "NaN"
			return s, 200, nil
		}, nil},
		{"sample outside window", "sample_outside_window", "failed", func(_ string, s []wireSeries) ([]wireSeries, int, []string) {
			s[0].Values[0][0] = 1000.0
			return s, 200, nil
		}, nil},
		{"series limit", "series_budget_exceeded", "budget_exceeded", nil, func(s *Settings) { s.Limits.MaxSeries = 1 }},
		{"sample limit", "sample_budget_exceeded", "budget_exceeded", nil, func(s *Settings) { s.Limits.MaxSamples = 2 }},
		{"bytes limit", "input_budget_exceeded", "budget_exceeded", nil, func(s *Settings) { s.Limits.MaxResponseBytes = 1024 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := serve(t, tc.edit)
			report := run(t, server.URL, "wp", "read", tc.settings)
			require.Equal(t, tc.status, report.Status)
			require.Equal(t, tc.code, report.Results[0].Observations[0].Code)
			require.Empty(t, report.Results[0].Findings)
		})
	}
}

func TestDuplicateAndQualifiedResponses(t *testing.T) {
	server := serve(t, func(_ string, s []wireSeries) ([]wireSeries, int, []string) { return append(s, s[0]), 200, nil })
	report := run(t, server.URL, "wp", "read", nil)
	require.Equal(t, "completed", report.Status)
	require.Len(t, report.Results[0].Findings, 1)
	require.Contains(t, string(report.Results[0].Observations[0].Data), "identical_series_deduplicated")
	server2 := serve(t, func(_ string, s []wireSeries) ([]wireSeries, int, []string) {
		return s, 200, []string{"sentinel-secret"}
	})
	report = run(t, server2.URL, "wp", "read", nil)
	require.Equal(t, "partial", report.Status)
	require.Equal(t, "inconclusive", report.Results[0].Findings[0].Outcome)
	body, _, err := d.EncodeReport(report)
	require.NoError(t, err)
	require.NotContains(t, string(body), "sentinel-secret")
}

func TestAuthenticationAndCancellation(t *testing.T) {
	t.Setenv("WPDIAG_TOKEN", "sentinel-secret")
	observed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed = true
		require.Equal(t, "Bearer sentinel-secret", r.Header.Get("Authorization"))
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	}))
	defer server.Close()
	report := run(t, server.URL, "wp", "read", func(s *Settings) { s.BearerTokenEnv = "WPDIAG_TOKEN" })
	require.True(t, observed)
	require.Equal(t, "completed", report.Status)
	t.Setenv("WPDIAG_PASSWORD", "sentinel-secret")
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		require.True(t, ok)
		require.Equal(t, "private-user", u)
		require.Equal(t, "sentinel-secret", p)
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	}))
	defer server2.Close()
	report = run(t, server2.URL, "wp", "read", func(s *Settings) { s.BasicAuth = &BasicAuth{Username: "private-user", PasswordEnv: "WPDIAG_PASSWORD"} })
	body, _, err := d.EncodeReport(report)
	require.NoError(t, err)
	require.NotContains(t, string(body), "sentinel-secret")
	require.NotContains(t, string(body), "private-user")
	_ = os.Unsetenv("WPDIAG_TOKEN")
	report = run(t, server.URL, "wp", "read", func(s *Settings) { s.BearerTokenEnv = "WPDIAG_TOKEN" })
	require.Equal(t, "authentication_unavailable", report.Results[0].Problems[0].Code)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := request("wp", "read")
	r.Selection.Window = d.Window{Mode: "live", DurationMS: 100, IntervalMS: 20}
	result := (Module{}).Run(ctx, r, connection(t, server.URL, nil))
	require.Equal(t, "cancelled", result.Status)
}

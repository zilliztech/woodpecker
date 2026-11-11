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

func TestSettingsAndCapabilityBoundaries(t *testing.T) {
	for _, modify := range []func(*Settings){
		func(s *Settings) { s.Endpoint = "http://user:sentinel-secret@host" },
		func(s *Settings) { s.Endpoint = "http://host?password=sentinel-secret" },
		func(s *Settings) { s.Deployment = "" }, func(s *Settings) { s.MatchLabels = nil },
		func(s *Settings) { s.MatchLabels["instance"] = "reserved" },
		func(s *Settings) { s.Limits.MaxSamples = 1 }, func(s *Settings) { s.Limits.TimeoutMS = 0 },
		func(s *Settings) { s.Policy.LatencyMS["unknown"] = 1 }, func(s *Settings) { s.Policy.LatencyMS["wp.append"] = 0 },
		func(s *Settings) { s.BearerTokenEnv = "bad-env" },
		func(s *Settings) { s.BasicAuth = &BasicAuth{Username: "user", PasswordEnv: "bad-env"} },
		func(s *Settings) {
			s.BasicAuth = &BasicAuth{Username: "user", PasswordEnv: "PASSWORD"}
			s.BearerTokenEnv = "TOKEN"
		},
	} {
		c := connection(t, "http://127.0.0.1:1", modify)
		err := (Module{}).ValidateConnection(c)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "sentinel-secret")
		result := (Module{}).Run(context.Background(), request("wp", "read"), c)
		require.Equal(t, "invalid", result.Status)
	}
	engine, err := d.NewEngine(Module{})
	require.NoError(t, err)
	for _, scope := range []string{"wp", "milvus", "e2e"} {
		for _, flow := range []string{"write", "read"} {
			r := request(scope, flow)
			r.Selection.Window = d.Window{Mode: "snapshot"}
			require.Equal(t, "unsupported", engine.Run(context.Background(), d.Config{}, r).Status)
		}
	}
	r := request("wp", "read")
	r.Selection.Target.Collection = "not-supported"
	require.Equal(t, "target_filter_not_supported", engine.Run(context.Background(), d.Config{}, r).Results[0].Problems[0].Code)
	for _, tc := range []struct {
		code   string
		edit   func(*d.Request)
		modify func(*Settings)
	}{
		{"deployment_binding_mismatch", func(r *d.Request) { r.Selection.Target.Deployment = "other" }, nil},
		{"window_budget_invalid", nil, func(s *Settings) { s.Limits.MaxWindowMS = 1000 }},
		{"query_budget_invalid", func(r *d.Request) { r.Selection.Flow = "write" }, func(s *Settings) { s.Limits.MaxQueries = 1 }},
		{"future_historical_window", func(r *d.Request) {
			from, to := time.Now().Add(time.Hour), time.Now().Add(2*time.Hour)
			r.Selection.Window.From = &from
			r.Selection.Window.To = &to
		}, nil},
		{"live_budget_too_short", func(r *d.Request) { r.Selection.Window = d.Window{Mode: "live", DurationMS: 1000, IntervalMS: 100} }, nil},
	} {
		r := request("wp", "read")
		if tc.edit != nil {
			tc.edit(&r)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second/10)
		result := (Module{}).Run(ctx, r, connection(t, "http://127.0.0.1:1", tc.modify))
		cancel()
		require.Equal(t, tc.code, result.Problems[0].Code)
	}
}

func TestLiveTransportTimeoutRedirectAndInvalidResponse(t *testing.T) {
	server := serve(t, nil)
	r := request("wp", "read")
	r.Selection.Window = d.Window{Mode: "live", DurationMS: 70, IntervalMS: 20}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := (Module{}).Run(ctx, r, connection(t, server.URL, nil))
	require.Equal(t, "completed", result.Status)
	require.NotEmpty(t, result.Findings)
	for _, tc := range []struct {
		name, code string
		handler    http.HandlerFunc
	}{
		{"timeout", "query_timeout_or_cancelled", func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }},
		{"redirect", "http_302", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "http://sentinel-secret.invalid", 302)
		}},
		{"invalid JSON", "invalid_prometheus_response", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "sentinel-secret") }},
		{"backend error", "invalid_prometheus_response", func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"status":"error","error":"sentinel-secret"}`)
		}},
		{"native histogram", "native_histogram_not_supported", func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[{"histograms":[[1001,{}]]}]}}`)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()
			report := run(t, server.URL, "wp", "read", func(s *Settings) { s.Limits.TimeoutMS = 20 })
			require.Equal(t, "failed", report.Status)
			require.Equal(t, tc.code, report.Results[0].Observations[0].Code)
			body, _, err := d.EncodeReport(report)
			require.NoError(t, err)
			require.NotContains(t, string(body), "sentinel-secret")
		})
	}
}

func TestHistogramResolutionThroughputAndContrastingStages(t *testing.T) {
	for _, tc := range []struct {
		name, outcome, unknown string
		edit                   func([]wireSeries) []wireSeries
		policy                 func(*Settings)
	}{
		{"coarse bucket", "inconclusive", "histogram_resolution_insufficient", func(s []wireSeries) []wireSeries {
			for i := range s {
				if s[i].Metric["le"] == "2048" {
					s[i].Values = s[0].Values
				}
			}
			return s
		}, func(s *Settings) { s.Policy.LatencyMS["wp.wal_read"] = 1000 }},
		{"saturated tail", "suspected_high_latency", "histogram_tail_unbounded", func(s []wireSeries) []wireSeries {
			for i := range s {
				if s[i].Metric["le"] == "4096" {
					for j := range s[i].Values {
						s[i].Values[j][1] = "0"
					}
				}
			}
			return s
		}, nil},
		{"small sample count", "inconclusive", "insufficient_observations", nil, func(s *Settings) { s.Policy.MinObservations = 101 }},
		{"invalid high bucket increase", "inconclusive", "inconsistent_cumulative_histogram", func(s []wireSeries) []wireSeries { s[len(s)-1].Values[5][1] = "99"; return s }, nil},
		{"separate unknown dimensions", "inconclusive", "missing_histogram_family", func(s []wireSeries) []wireSeries {
			s[0].Metric["unrecognized_tenant_dimension"] = "different"
			return s
		}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := serve(t, func(_ string, s []wireSeries) ([]wireSeries, int, []string) {
				if tc.edit != nil {
					s = tc.edit(s)
				}
				return s, 200, nil
			})
			report := run(t, server.URL, "wp", "read", tc.policy)
			require.Equal(t, tc.outcome, report.Results[0].Findings[0].Outcome)
			require.Contains(t, report.Results[0].Findings[0].Unknown, tc.unknown)
		})
	}
	server := serve(t, func(_ string, s []wireSeries) ([]wireSeries, int, []string) {
		for i := range s {
			for j := 3; j < len(s[i].Values); j++ {
				s[i].Values[j][1] = s[i].Values[2][1]
			}
		}
		return s, 200, nil
	})
	report := run(t, server.URL, "wp", "read", nil)
	findings := report.Results[0].Findings
	require.Len(t, findings, 2)
	require.Equal(t, "observed_throughput_drop", findings[1].Outcome)
	require.Contains(t, findings[1].Unknown, "demand_and_arrival_rate_unknown")
	server2 := serve(t, func(query string, s []wireSeries) ([]wireSeries, int, []string) {
		if !strings.Contains(query, "woodpecker_server") {
			s = responseSeries(t, query, false)
		}
		return s, 200, nil
	})
	report = run(t, server2.URL, "e2e", "write", nil)
	results := map[string]string{}
	for _, f := range report.Results[0].Findings {
		results[f.RuleID] = f.Outcome
	}
	require.Equal(t, "suspected_high_latency", results["wp.append.latency"])
	require.Equal(t, "within_configured_latency_for_observed_completions", results["milvus.append.latency"])
}

func TestAdmissionZeroAndUnknownState(t *testing.T) {
	for _, value := range []string{"0", "3"} {
		server := serve(t, func(query string, s []wireSeries) ([]wireSeries, int, []string) {
			if strings.Contains(query, "backpressure_state") {
				for j := range s[0].Values {
					s[0].Values[j][1] = value
				}
			}
			return s, 200, nil
		})
		report := run(t, server.URL, "wp", "write", nil)
		f := report.Results[0].Findings[len(report.Results[0].Findings)-1]
		if value == "0" {
			require.Equal(t, "no_admission_pressure_observed", f.Outcome)
		} else {
			require.Contains(t, f.Unknown, "unknown_admission_state")
		}
	}
}

func TestConflictingDuplicateSeries(t *testing.T) {
	server := serve(t, func(_ string, s []wireSeries) ([]wireSeries, int, []string) {
		body, err := json.Marshal(s[0])
		require.NoError(t, err)
		var other wireSeries
		require.NoError(t, json.Unmarshal(body, &other))
		other.Values[5][1] = "101"
		return append(s, other), 200, nil
	})
	report := run(t, server.URL, "wp", "read", nil)
	require.Equal(t, "failed", report.Status)
	require.Equal(t, "conflicting_series_identity", report.Results[0].Observations[0].Code)
}

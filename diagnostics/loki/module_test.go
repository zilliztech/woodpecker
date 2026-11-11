package loki

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func request(scope, flow string) d.Request {
	from, to := time.Unix(1000, 0).UTC(), time.Unix(1060, 0).UTC()
	return d.Request{Selection: d.Selection{Sources: []string{"loki"}, Scope: scope, Flow: flow, Window: d.Window{Mode: "historical", From: &from, To: &to}}, Connection: "test", Limits: d.DefaultLimits()}
}

func connection(t *testing.T, endpoint string, modify func(*Settings)) d.Connection {
	t.Helper()
	s := Settings{Endpoint: endpoint, Deployment: "fixture", MatchLabels: map[string]string{"namespace": "fixture", "cluster": "test"}, Limits: Limits{MaxQueries: 8, PageSize: 500, MaxLines: 2000, MaxResponseBytes: 2 << 20, MaxWindowMS: 3600000, TimeoutMS: 1000}}
	if modify != nil {
		modify(&s)
	}
	b, e := yaml.Marshal(s)
	require.NoError(t, e)
	var n yaml.Node
	require.NoError(t, yaml.Unmarshal(b, &n))
	return d.Connection{Settings: *n.Content[0]}
}

func run(t *testing.T, server *httptest.Server, r d.Request, modify func(*Settings)) d.Report {
	t.Helper()
	engine, e := d.NewEngine(Module{})
	require.NoError(t, e)
	return engine.Run(context.Background(), d.Config{Sources: map[string]d.SourceConfig{"loki": {Connections: map[string]d.Connection{"test": connection(t, server.URL, modify)}}}}, r)
}

func labels(component, pod string) map[string]string {
	return map[string]string{"namespace": "fixture", "cluster": "test", "component": component, "pod": pod, "container": "main", "private": "sentinel-secret"}
}

func payload(w http.ResponseWriter, streams ...any) {
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "streams", "result": streams}})
}

func stream(component, pod string, values ...[]any) any {
	return map[string]any{"stream": labels(component, pod), "values": values}
}

func line(msg string) string {
	b, _ := json.Marshal(map[string]string{"msg": msg, "error": "password=sentinel-secret", "requestID": "request-1", "traceID": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	return string(b)
}

func TestProfilesExactPatternsAndSafeEvidence(t *testing.T) {
	for _, p := range Catalog() {
		for _, scope := range []string{"wp", "milvus", "e2e"} {
			if !applicable(p, scope, p.Flow) {
				continue
			}
			t.Run(scope+"/"+p.ID, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					require.Equal(t, "GET", r.Method)
					require.Equal(t, "/loki/api/v1/query_range", r.URL.Path)
					require.Contains(t, r.URL.Query().Get("query"), `namespace="fixture"`)
					payload(w, stream(p.Component, "pod-1", []any{"1001000000000", line(p.Message), map[string]string{"secret": "sentinel-secret"}}))
				}))
				defer server.Close()
				report := run(t, server, request(scope, p.Flow), nil)
				require.Equal(t, "completed", report.Status)
				require.Equal(t, "loki."+p.ID, report.Results[0].Findings[0].RuleID)
				require.Equal(t, "observed_log_event", report.Results[0].Findings[0].Strength)
				b, _, e := d.EncodeReport(report)
				require.NoError(t, e)
				require.NotContains(t, string(b), "sentinel-secret")
				require.NotContains(t, string(b), server.URL)
				require.Contains(t, string(b), "matched_excerpt")
				require.True(t, report.Results[0].Observations[0].Query.Executed)
			})
		}
	}
}

func TestControlsNoMatchIsNeverHealthy(t *testing.T) {
	for _, value := range []string{"", `{"msg":"normal progress"}`, `{"msg":"ERROR read entries error unsupported version"}`, `{"msg":"new format","error":"add entry failed"}`, "add entry failed", `[2026-01-01] [WARN] [file.go:1] ["add entry failed"] [requestID=request-1]`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if value == "" {
				payload(w)
				return
			}
			payload(w, stream("woodpecker", "p", []any{"1001000000000", value}))
		}))
		report := run(t, server, request("wp", "write"), nil)
		server.Close()
		require.Equal(t, "completed", report.Status)
		expected := "inconclusive"
		if strings.HasPrefix(value, "[2026") {
			expected = "suspected_operation_problem"
		}
		require.Equal(t, expected, report.Results[0].Findings[0].Outcome)
		require.NotEmpty(t, report.Results[0].Gaps)
	}
}

func TestIdentityAssociationRecoveryAndDedup(t *testing.T) {
	for _, same := range []bool{true, false} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			other := line(Catalog()[6].Message)
			if !same {
				other = strings.ReplaceAll(strings.ReplaceAll(other, "request-1", "request-2"), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
			}
			payload(w, stream("woodpecker", "wp", []any{"1001000000000", line(Catalog()[2].Message)}, []any{"1001000000000", line(Catalog()[2].Message)}, []any{"1002000000000", line(Catalog()[3].Message)}), stream("streamingnode", "sn", []any{"1003000000000", other}))
		}))
		report := run(t, server, request("e2e", "write"), nil)
		server.Close()
		found := false
		recovered := false
		for _, f := range report.Results[0].Findings {
			found = found || f.RuleID == "loki.e2e.observed_identity"
			recovered = recovered || f.RuleID == "loki.wp.disk_pressure" && len(f.Contradicting) > 0
		}
		require.Equal(t, same, found)
		require.True(t, recovered)
		require.Contains(t, string(report.Results[0].Metadata), `"duplicate_occurrences":1`)
	}
}

func TestInclusivePaginationAndSaturatedBoundary(t *testing.T) {
	for _, saturated := range []bool{false, true} {
		calls := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			cursor := r.URL.Query().Get("start")
			if calls == 1 {
				payload(w, stream("woodpecker", "p", []any{"1001000000000", line(Catalog()[0].Message)}, []any{"1002000000000", line(Catalog()[1].Message)}))
				return
			}
			require.Equal(t, "1002000000000", cursor)
			values := [][]any{{"1002000000000", line(Catalog()[1].Message)}}
			if saturated {
				values = append(values, values[0])
			}
			payload(w, stream("woodpecker", "p", values...))
		}))
		report := run(t, server, request("wp", "write"), func(s *Settings) { s.Limits.PageSize = 2 })
		server.Close()
		require.Equal(t, 2, calls)
		expected := "completed"
		if saturated {
			expected = "partial"
		}
		require.Equal(t, expected, report.Status)
		require.Len(t, report.Results[0].Findings, 2)
	}
}

func TestBackendFailuresAndWrongEvidence(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
		code    string
	}{
		{"http", func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "sentinel-secret", 401) }, "backend_http_error"},
		{"redirect", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "http://sentinel-secret.invalid", 302)
		}, "backend_http_error"},
		{"wrong namespace", func(w http.ResponseWriter, _ *http.Request) {
			v := stream("woodpecker", "p", []any{"1001000000000", line(Catalog()[0].Message)}).(map[string]any)
			v["stream"].(map[string]string)["namespace"] = "wrong"
			payload(w, v)
		}, "response_scope_mismatch"},
		{"wrong component", func(w http.ResponseWriter, _ *http.Request) {
			payload(w, stream("proxy", "p", []any{"1001000000000", line(Catalog()[0].Message)}))
		}, "response_scope_mismatch"},
		{"wrong time", func(w http.ResponseWriter, _ *http.Request) {
			payload(w, stream("woodpecker", "p", []any{"1060000000000", line(Catalog()[0].Message)}))
		}, "response_time_mismatch"},
		{"timestamp number", func(w http.ResponseWriter, _ *http.Request) {
			payload(w, stream("woodpecker", "p", []any{1001000000000, line(Catalog()[0].Message)}))
		}, "unsupported_response"},
		{"unsupported", func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix"}}`)
		}, "unsupported_response"},
		{"warning", func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"status":"success","warnings":["sentinel-secret"],"data":{"resultType":"streams","result":[]}}`)
		}, "backend_warnings_withheld"},
		{"bytes", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, strings.Repeat("sentinel-secret", 1000)) }, "input_budget_exceeded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()
			report := run(t, server, request("wp", "write"), func(s *Settings) { s.Limits.MaxResponseBytes = 1024 })
			require.NotEqual(t, "completed", report.Status)
			require.Equal(t, tc.code, report.Results[0].Observations[0].Code)
			b, _, e := d.EncodeReport(report)
			require.NoError(t, e)
			require.NotContains(t, string(b), "sentinel-secret")
		})
	}
}

func TestSelectorsBudgetsAndAuthentication(t *testing.T) {
	t.Setenv("LOKI_TEST_TOKEN", "sentinel-secret")
	t.Setenv("LOKI_TEST_TENANT", "tenant-fixture")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer sentinel-secret", r.Header.Get("Authorization"))
		require.Equal(t, "tenant-fixture", r.Header.Get("X-Scope-OrgID"))
		require.Contains(t, r.URL.Query().Get("query"), `pod="p"`)
		payload(w, stream("woodpecker", "p", []any{"1001000000000", line(Catalog()[0].Message)}))
	}))
	defer server.Close()
	r := request("wp", "write")
	r.Selection.Target.Instance = "p"
	r.Selection.Target.OperationID = "unrelated"
	report := run(t, server, r, func(s *Settings) { s.BearerTokenEnv = "LOKI_TEST_TOKEN"; s.TenantEnv = "LOKI_TEST_TENANT" })
	require.Equal(t, "inconclusive", report.Results[0].Findings[0].Outcome)
	report = run(t, server, r, func(s *Settings) { s.BearerTokenEnv = "LOKI_MISSING_TEST_TOKEN" })
	require.Equal(t, "failed", report.Status)
	require.Equal(t, "authentication_unavailable", report.Results[0].Problems[0].Code)
	for _, modify := range []func(*Settings){func(s *Settings) { s.Endpoint = "https://user:secret@host" }, func(s *Settings) { s.MatchLabels = nil }, func(s *Settings) { s.MatchLabels["pod"] = "x" }, func(s *Settings) { s.Limits.PageSize = 0 }, func(s *Settings) { s.TenantEnv = "bad env" }, func(s *Settings) { s.BasicAuth = &BasicAuth{Username: "a:b", PasswordEnv: "PW"} }, func(s *Settings) {
		s.BasicAuth = &BasicAuth{Username: "a", PasswordEnv: "PW"}
		s.BearerTokenEnv = "TOKEN"
	}} {
		require.Error(t, (Module{}).ValidateConnection(connection(t, server.URL, modify)))
	}
	r = request("wp", "write")
	r.Selection.Target.Deployment = "other"
	require.Equal(t, "invalid", run(t, server, r, nil).Status)
	r = request("wp", "write")
	r.Selection.Target.LogID = "1"
	require.Equal(t, "unsupported", run(t, server, r, nil).Status)
	r = request("wp", "write")
	r.Selection.Window = d.Window{Mode: "snapshot"}
	require.Equal(t, "unsupported", run(t, server, r, nil).Status)
}

func TestBoundedWorkCancellationAndLive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		payload(w, stream("woodpecker", "p", []any{"1001000000000", line(Catalog()[0].Message)}), stream("woodpecker", "p2", []any{"1002000000000", line(Catalog()[0].Message)}))
	}))
	defer server.Close()
	r := request("wp", "write")
	r.Limits.MaxTargets = 1
	require.Equal(t, "budget_exceeded", run(t, server, r, nil).Status)
	r = request("wp", "write")
	require.Equal(t, "budget_exceeded", run(t, server, r, func(s *Settings) { s.Limits.MaxQueries = 1; s.Limits.PageSize = 2 }).Status)
	r = request("wp", "write")
	r.Selection.Window = d.Window{Mode: "live", DurationMS: 10, IntervalMS: 1}
	r.Limits.BudgetMS = 1
	require.Equal(t, "invalid", run(t, server, r, nil).Status)
	engine, e := d.NewEngine(Module{})
	require.NoError(t, e)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report := engine.Run(ctx, d.Config{Sources: map[string]d.SourceConfig{"loki": {Connections: map[string]d.Connection{"test": connection(t, server.URL, nil)}}}}, request("wp", "write"))
	require.NotEqual(t, "completed", report.Status)
	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { payload(w) }))
	defer empty.Close()
	r.Limits = d.DefaultLimits()
	require.Equal(t, "completed", run(t, empty, r, nil).Status)
	for _, ts := range []string{"0", "01", "not-a-number", strconv.FormatUint(^uint64(0), 10)} {
		_, ok := validNS(ts)
		require.False(t, ok)
	}
}

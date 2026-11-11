package jaeger

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

func TestConnectionAndSelectionBoundaries(t *testing.T) {
	for _, modify := range []func(*Settings){
		func(s *Settings) { s.Endpoint = "http://user:sentinel-secret@host" }, func(s *Settings) { s.Endpoint = "http://host?token=sentinel-secret" },
		func(s *Settings) { s.Deployment = "" }, func(s *Settings) { s.Services = nil }, func(s *Settings) { s.Services["proxy"] = "unknown" }, func(s *Settings) { s.MatchTags = map[string]string{"authorization": "sentinel-secret"} },
		func(s *Settings) { s.Search.Service = "unbound" }, func(s *Settings) { s.RequestIDTag = "not a tag" }, func(s *Settings) { s.Limits.MaxSpans = 0 }, func(s *Settings) { s.Limits.MaxTraces = 101 }, func(s *Settings) { s.Limits.MaxQueries = 0 }, func(s *Settings) { s.Limits.MaxResponseBytes = 1 }, func(s *Settings) { s.Policy.SlowSpanMS = 0 },
		func(s *Settings) { s.BearerTokenEnv = "bad-env" }, func(s *Settings) {
			s.BasicAuth = &BasicAuth{Username: "user", PasswordEnv: "PASSWORD"}
			s.BearerTokenEnv = "TOKEN"
		},
	} {
		c := connection(t, "http://localhost:1", modify)
		require.Error(t, (Module{}).ValidateConnection(c))
		report := run(t, request("wp", "write"), c)
		require.Equal(t, "invalid", report.Status)
		raw, err := json.Marshal(report)
		require.NoError(t, err)
		require.NotContains(t, string(raw), "sentinel-secret")
	}
	for _, tc := range []struct {
		name, code string
		edit       func(*d.Request)
		modify     func(*Settings)
	}{
		{name: "wrong deployment", code: "deployment_binding_mismatch", edit: func(r *d.Request) { r.Selection.Target.Deployment = "other" }},
		{name: "snapshot needs exact ID", code: "snapshot_requires_trace_id", edit: func(r *d.Request) { r.Selection.Target.TraceID = "" }},
		{name: "unmapped collection filter", code: "target_filter_not_supported", edit: func(r *d.Request) { r.Selection.Target.Collection = "7" }},
		{name: "request tag unconfigured", code: "request_id_tag_unconfigured", edit: func(r *d.Request) { r.Selection.Target.OperationID = "request" }, modify: func(s *Settings) { s.RequestIDTag = "" }},
		{name: "unsupported consume stage", code: "profile_not_supported", edit: func(r *d.Request) {
			r.Selection.Scope = "e2e"
			r.Selection.Flow = "read"
			r.Selection.Stage = "consume"
		}},
		{name: "search must bind service", code: "search_service_required", edit: func(r *d.Request) {
			r.Selection.Target.TraceID = ""
			r.Selection.Window = d.Window{Mode: "live", DurationMS: 20, IntervalMS: 10}
		}, modify: func(s *Settings) { s.Search.Service = "" }},
		{name: "budget must include window", code: "live_budget_too_short", edit: func(r *d.Request) {
			r.Selection.Window = d.Window{Mode: "live", DurationMS: 20, IntervalMS: 10}
			r.Limits.BudgetMS = 20
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := request("wp", "write")
			tc.edit(&r)
			report := run(t, r, connection(t, "http://localhost:1", tc.modify))
			require.Equal(t, tc.code, report.Results[0].Problems[0].Code)
		})
	}
}

func TestMalformedAndBoundedTraceResponses(t *testing.T) {
	for _, tc := range []struct {
		name, code, status string
		edit               func(*wireTrace)
		modify             func(*Settings)
		changeRequest      func(*d.Request)
	}{
		{name: "trace identity mismatch", code: "trace_id_scope_mismatch", edit: func(tr *wireTrace) { tr.ID = "22222222222222222222222222222222" }},
		{name: "invalid span identity", code: "invalid_span_identity", edit: func(tr *wireTrace) { tr.Spans[0].ID = "invalid" }},
		{name: "missing span time", code: "invalid_span_time", edit: func(tr *wireTrace) { tr.Spans[0].Duration = nil }},
		{name: "negative duration", code: "invalid_span_time", edit: func(tr *wireTrace) { tr.Spans[0].Duration = intPointer(-1) }},
		{name: "missing process", code: "missing_span_process", edit: func(tr *wireTrace) { delete(tr.Processes, "p1") }},
		{name: "unknown reference type", code: "invalid_span_reference", edit: func(tr *wireTrace) { tr.Spans[1].References[0].Type = "MAYBE" }},
		{name: "reference work bound", code: "reference_budget_exceeded", status: "budget_exceeded", edit: func(tr *wireTrace) {
			for i := 0; i < 17; i++ {
				tr.Spans[0].References = append(tr.Spans[0].References, reference{Type: "CHILD_OF", TraceID: testID, SpanID: "0000000000000002"})
			}
		}},
		{name: "span work bound", code: "span_budget_exceeded", status: "budget_exceeded", modify: func(s *Settings) { s.Limits.MaxSpans = 1 }},
		{name: "source response bound", code: "input_budget_exceeded", status: "budget_exceeded", modify: func(s *Settings) { s.Limits.MaxResponseBytes = 1024 }},
		{name: "whole input bound", code: "input_budget_exceeded", status: "budget_exceeded", changeRequest: func(r *d.Request) { r.Limits.MaxInputBytes = 1024 }},
		{name: "target bound", code: "target_budget_exceeded", status: "budget_exceeded", changeRequest: func(r *d.Request) { r.Limits.MaxTargets = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := fixture("write")
			if tc.edit != nil {
				tc.edit(&tr)
			}
			server := serve(t, []wireTrace{tr})
			r := request("e2e", "write")
			if tc.changeRequest != nil {
				tc.changeRequest(&r)
			}
			report := run(t, r, connection(t, server.URL, tc.modify))
			status := tc.status
			if status == "" {
				status = "failed"
			}
			require.Equal(t, status, report.Status)
			require.True(t, hasGap(report, tc.code))
			require.Empty(t, report.Results[0].Findings)
		})
	}
}

func TestBackendErrorsNoMatchAndUntrustedAnnotations(t *testing.T) {
	for _, tc := range []struct {
		name, body, code string
		status           int
	}{
		{name: "invalid JSON", body: "sentinel-secret", code: "invalid_jaeger_response"},
		{name: "null data", body: `{"data":null}`, code: "invalid_jaeger_response"},
		{name: "wrong data shape", body: `{"data":{}}`, code: "invalid_jaeger_response"},
		{name: "backend error", body: `{"data":[],"errors":[{"message":"sentinel-secret"}]}`, code: "backend_errors_withheld"},
		{name: "http auth error", status: 401, body: "sentinel-secret", code: "http_401"},
		{name: "redirect not followed", status: 302, code: "http_302"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.status != 0 {
					w.Header().Set("Location", "http://sentinel-secret.invalid")
					w.WriteHeader(tc.status)
				}
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			report := run(t, request("wp", "write"), connection(t, server.URL, nil))
			require.True(t, hasGap(report, tc.code))
			raw, err := json.Marshal(report)
			require.NoError(t, err)
			require.NotContains(t, string(raw), "sentinel-secret")
		})
	}
	server := serve(t, []wireTrace{})
	report := run(t, request("wp", "write"), connection(t, server.URL, nil))
	require.Equal(t, "completed", report.Status)
	require.True(t, hasOutcome(report, "inconclusive"))
	require.True(t, hasGap(report, "no_retained_trace_evidence"))
}

func TestRetainedWindowAndIdentityAreActuallyChecked(t *testing.T) {
	for _, tc := range []struct {
		name, gap string
		edit      func(*wireTrace)
		modify    func(*Settings)
	}{
		{name: "wrong deployment tag", gap: "trace_target_or_window_unverified", modify: func(s *Settings) { s.MatchTags = map[string]string{"namespace": "expected"} }},
		{name: "future span clocks", gap: "trace_target_or_window_unverified", edit: func(tr *wireTrace) {
			for i := range tr.Spans {
				tr.Spans[i].Start = intPointer(time.Now().Add(time.Hour).UnixMicro())
			}
		}},
		{name: "request tag not seen", gap: "request_id_not_observed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := fixture("write")
			if tc.edit != nil {
				tc.edit(&tr)
			}
			server := serve(t, []wireTrace{tr})
			r := request("e2e", "write")
			if tc.name == "request tag not seen" {
				r.Selection.Target.OperationID = "missing"
			}
			report := run(t, r, connection(t, server.URL, tc.modify))
			require.True(t, hasGap(report, tc.gap))
			require.False(t, hasOutcome(report, "observed_slow_span"))
			require.False(t, hasOutcome(report, "observed_stages_connected"))
		})
	}
	server := serve(t, []wireTrace{fixture("write")})
	r := request("e2e", "write")
	from, to := time.Now().Add(-10*time.Second), time.Now()
	r.Selection.Window = d.Window{Mode: "historical", From: &from, To: &to}
	report := run(t, r, connection(t, server.URL, nil))
	require.True(t, hasGap(report, "trace_target_or_window_unverified"))
	require.False(t, hasOutcome(report, "observed_slow_span"))
}

func TestErrorFlagSemanticsAndAuthentication(t *testing.T) {
	tr := fixture("write")
	for i := range tr.Spans {
		tr.Spans[i].Duration = intPointer(1000)
	}
	tr.Spans[3].Tags = []tag{testTag("otel.status_code", "sentinel-secret")}
	server := serve(t, []wireTrace{tr})
	report := run(t, request("e2e", "write"), connection(t, server.URL, nil))
	require.True(t, hasGap(report, "error_tag_semantics_unverified"))
	require.True(t, hasOutcome(report, "inconclusive"))
	raw, err := json.Marshal(report)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "sentinel-secret")
	t.Setenv("WPDIAG_TRACE_TOKEN", "sentinel-secret")
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer sentinel-secret", r.Header.Get("Authorization"))
		json.NewEncoder(w).Encode(map[string]any{"data": []wireTrace{fixture("write")}})
	}))
	defer server.Close()
	report = run(t, request("wp", "write"), connection(t, server.URL, func(s *Settings) { s.BearerTokenEnv = "WPDIAG_TRACE_TOKEN" }))
	require.Equal(t, "completed", report.Status)
	raw, err = json.Marshal(report)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "sentinel-secret")
	c := connection(t, server.URL, func(s *Settings) { s.BearerTokenEnv = "WPDIAG_TRACE_MISSING_TOKEN" })
	report = run(t, request("wp", "write"), c)
	require.Equal(t, "authentication_secret_unavailable", report.Results[0].Problems[0].Code)
}

func TestLiveCancellationAndHTTPTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	report := run(t, request("wp", "write"), connection(t, server.URL, func(s *Settings) { s.Limits.TimeoutMS = 10 }))
	require.True(t, hasGap(report, "query_timeout_or_cancelled"))
	r := request("wp", "write")
	r.Selection.Window = d.Window{Mode: "live", DurationMS: 1000, IntervalMS: 100}
	engine, err := d.NewEngine(Module{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	defer cancel()
	started := time.Now()
	report = engine.Run(ctx, d.Config{Sources: map[string]d.SourceConfig{"trace": {Connections: map[string]d.Connection{"local": connection(t, server.URL, nil)}}}}, r)
	require.Equal(t, "cancelled", report.Status)
	require.Less(t, time.Since(started), time.Second)
}

func TestTraceSelectorsAreSourceOwnedAndCanonical(t *testing.T) {
	r := request("wp", "write")
	r.Selection.Target.TraceID = strings.ToUpper("abcdefabcdefabcd")
	normalized, err := d.Normalize(r.Selection)
	require.NoError(t, err)
	require.Equal(t, "0000000000000000abcdefabcdefabcd", normalized.Target.TraceID)
	r.Selection.Sources = []string{"wpcli"}
	_, err = d.Normalize(r.Selection)
	require.Error(t, err)
}

func TestQueuedAppendScopeDoesNotSupplyMilvusStage(t *testing.T) {
	for _, scope := range []string{"AppendOp", "BatchAppendOp"} {
		t.Run(scope, func(t *testing.T) {
			tr := fixture("write")
			tr.Spans[1].Operation = "Execute"
			tr.Spans[1].Tags = []tag{testTag("otel.scope.name", scope)}
			server := serve(t, []wireTrace{tr})
			report := run(t, request("e2e", "write"), connection(t, server.URL, nil))
			require.Equal(t, "completed", report.Status)
			for _, f := range report.Results[0].Findings {
				if f.RuleID == "trace.relationship_coverage" {
					require.Equal(t, "partial_observed_path", f.Outcome)
					require.Contains(t, f.Unknown, "expected_connected_stages_not_established")
				}
			}
			var normalized Trace
			require.NoError(t, json.Unmarshal(report.Results[0].Observations[1].Data, &normalized))
			require.Equal(t, scope, normalized.Spans[1].Scope)
			require.True(t, selectedSpan(normalized.Spans[1], "wp"))
			require.False(t, selectedSpan(normalized.Spans[1], "milvus"))
		})
	}
	known, _ := knownOperation("wp_client", "", "Execute", "write")
	require.False(t, known, "a generic Execute name is not enough to identify a WP append")
	known, _ = knownOperation("wp_client", "AppendOp", "Execute", "read")
	require.False(t, known, "write execution is not a read path")
}

func TestMilvusWALScopeRecognizesUpstreamAppendBoundaries(t *testing.T) {
	for _, op := range []string{"wal.autocommit", "wal.txn", "wal.dist_append", "wal.append", "wal.appendimpl"} {
		t.Run(op, func(t *testing.T) {
			tr := fixture("write")
			tr.Spans[1].Operation = op
			// Jaeger 1.53 stores the actual exporter scope on process rows.
			tr.Processes["p2"] = wireProcess{Service: "streamingnode", Tags: []tag{testTag("otel.library.name", "milvus.streaming.wal")}}
			server := serve(t, []wireTrace{tr})
			report := run(t, request("e2e", "write"), connection(t, server.URL, nil))
			require.Equal(t, "completed", report.Status)
			for _, f := range report.Results[0].Findings {
				if f.RuleID == "trace.relationship_coverage" {
					require.Equal(t, "observed_stages_connected", f.Outcome)
				}
			}
			var normalized Trace
			require.NoError(t, json.Unmarshal(report.Results[0].Observations[1].Data, &normalized))
			require.Equal(t, op, normalized.Spans[1].Operation)
			require.Equal(t, "milvus.streaming.wal", normalized.Spans[1].Scope)
			known, _ := knownOperation("streamingnode", "", op, "write")
			require.False(t, known, "a matching name without the emitter's scope is not sufficient")
			known, _ = knownOperation("wp_client", "milvus.streaming.wal", op, "write")
			require.False(t, known, "a WP client cannot supply a Milvus WAL stage")
			known, _ = knownOperation("streamingnode", "milvus.streaming.wal", op, "read")
			require.False(t, known, "append timers are not user query or WAL consume evidence")
		})
	}
}

func TestConflictingWALScopeAliasesCannotEstablishStreamingNode(t *testing.T) {
	tr := fixture("write")
	tr.Spans[1].Operation = "wal.appendimpl"
	tr.Processes["p2"] = wireProcess{Service: "streamingnode", Tags: []tag{testTag("otel.library.name", "milvus.streaming.wal")}}
	tr.Spans[1].Tags = []tag{testTag("otel.scope.name", "AppendOp")}
	server := serve(t, []wireTrace{tr})
	report := run(t, request("e2e", "write"), connection(t, server.URL, nil))
	require.Equal(t, "completed", report.Status)
	for _, f := range report.Results[0].Findings {
		if f.RuleID == "trace.relationship_coverage" {
			require.Equal(t, "partial_observed_path", f.Outcome)
		}
	}
}

func TestAppendRPCCancellationDoesNotAssertApplicationFailure(t *testing.T) {
	for _, tc := range []struct{ name, kind, code, outcome string }{
		{"client cancellation", "client", "1", "observed_rpc_cancellation"},
		{"server cancellation", "server", "1", "observed_span_error"},
		{"client unavailable", "client", "14", "observed_span_error"},
		{"conflicting status", "client", "1", "observed_span_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := fixture("write")
			tr.Spans[2].Operation = "woodpecker.proto.logstore.LogStore/AddEntry"
			tr.Spans[2].Tags = []tag{testTag("span.kind", tc.kind), testTag("rpc.grpc.status_code", tc.code), testTag("error", "true")}
			if tc.name == "conflicting status" {
				tr.Spans[2].Tags = append(tr.Spans[2].Tags, testTag("rpc.grpc.status_code", "14"))
			}
			server := serve(t, []wireTrace{tr})
			report := run(t, request("wp", "write"), connection(t, server.URL, nil))
			require.Equal(t, "completed", report.Status)
			found := false
			for _, f := range report.Results[0].Findings {
				if f.Outcome == tc.outcome {
					found = true
					if tc.outcome == "observed_rpc_cancellation" {
						require.Equal(t, "unknown", f.Severity)
					}
				}
			}
			require.True(t, found)
		})
	}
}

package jaeger

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

const testID = "11111111111111111111111111111111"

func testTag(key, value string) tag {
	raw, _ := json.Marshal(value)
	return tag{Key: key, Type: "string", Value: raw}
}
func intPointer(v int64) *int64 { return &v }

func fixture(flow string) wireTrace {
	now := time.Now().Add(-time.Minute).UnixMicro()
	trace := wireTrace{ID: testID, Processes: map[string]wireProcess{
		"p1": {Service: "proxy"}, "p2": {Service: "streamingnode"}, "p3": {Service: "wp-client", Tags: []tag{testTag("otel.library.name", "LogWriter")}}, "p4": {Service: "wp-server", Tags: []tag{testTag("otel.library.name", "LogStore")}},
	}}
	for i, entry := range []struct{ process, op string }{{"p1", "Proxy-Insert"}, {"p2", "AppendMessages"}, {"p3", "Write"}, {"p4", "AddEntry"}} {
		span := wireSpan{TraceID: testID, ID: fmt.Sprintf("%016x", i+1), Operation: entry.op, ProcessID: entry.process, Start: intPointer(now + int64(i)*1000), Duration: intPointer(2000000 - int64(i)*10000)}
		if i > 0 {
			span.References = []reference{{Type: "CHILD_OF", TraceID: testID, SpanID: fmt.Sprintf("%016x", i)}}
		}
		trace.Spans = append(trace.Spans, span)
	}
	if flow == "read" {
		trace.Spans[0].Operation = "Proxy-Search"
		trace.Spans[1].Operation = "milvus.proto.query.QueryNode/Search"
		trace.Spans[2].Operation = "ReadNext"
		trace.Spans[3].Operation = "GetBatchEntriesAdv"
		p := trace.Processes["p3"]
		p.Tags = []tag{testTag("otel.library.name", "LogReader")}
		trace.Processes["p3"] = p
	}
	return trace
}

func connection(t *testing.T, endpoint string, modify func(*Settings)) d.Connection {
	t.Helper()
	var node yaml.Node
	text := fmt.Sprintf("endpoint: %q\ndeployment: fixture\nservices: {proxy: proxy, streamingnode: streamingnode, wp-client: wp_client, wp-server: wp_server}\nsearch: {service: proxy}\nrequest_id_tag: request_id\n", endpoint)
	require.NoError(t, yaml.Unmarshal([]byte(text), &node))
	c := d.Connection{Settings: *node.Content[0]}
	s, err := settings(c)
	require.NoError(t, err)
	if modify != nil {
		modify(&s)
	}
	raw, err := yaml.Marshal(s)
	require.NoError(t, err)
	require.NoError(t, yaml.Unmarshal(raw, &node))
	c.Settings = *node.Content[0]
	return c
}

func request(scope, flow string) d.Request {
	return d.Request{Connection: "local", Limits: d.DefaultLimits(), Selection: d.Selection{Sources: []string{"trace"}, Scope: scope, Flow: flow, Target: d.Target{TraceID: testID}, Window: d.Window{Mode: "snapshot"}}}
}

func run(t *testing.T, r d.Request, c d.Connection) d.Report {
	t.Helper()
	engine, err := d.NewEngine(Module{})
	require.NoError(t, err)
	return engine.Run(context.Background(), d.Config{Sources: map[string]d.SourceConfig{"trace": {Connections: map[string]d.Connection{"local": c}}}}, r)
}

func serve(t *testing.T, traces []wireTrace) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.True(t, strings.HasPrefix(r.URL.Path, "/api/traces"))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"data": traces, "errors": nil})
	}))
	t.Cleanup(server.Close)
	return server
}

func hasOutcome(report d.Report, outcome string) bool {
	for _, r := range report.Results {
		for _, f := range r.Findings {
			if f.Outcome == outcome {
				return true
			}
		}
	}
	return false
}

func hasGap(report d.Report, code string) bool {
	for _, r := range report.Results {
		for _, g := range r.Gaps {
			if g.Code == code {
				return true
			}
		}
	}
	return false
}

func TestScopeFlowMatrixAndSuspiciousSpanEvidence(t *testing.T) {
	for _, scope := range []string{"wp", "milvus", "e2e"} {
		for _, flow := range []string{"write", "read"} {
			t.Run(scope+flow, func(t *testing.T) {
				server := serve(t, []wireTrace{fixture(flow)})
				report := run(t, request(scope, flow), connection(t, server.URL, nil))
				require.Equal(t, "completed", report.Status)
				require.True(t, hasOutcome(report, "observed_slow_span"))
				require.True(t, hasOutcome(report, "observed_stages_connected"))
				require.False(t, hasOutcome(report, "healthy"))
				ids := map[string]bool{}
				for _, o := range report.Results[0].Observations {
					ids[o.ID] = true
				}
				for _, f := range report.Results[0].Findings {
					for _, id := range f.Supporting {
						require.True(t, ids[id])
					}
					if f.Severity == "warning" {
						require.Equal(t, "suspected", f.Strength)
						require.Equal(t, testID, f.Target.TraceID)
						require.Contains(t, f.Summary, "duration")
					}
				}
			})
		}
	}
}

func TestRelationshipAndTimingExclusions(t *testing.T) {
	for _, tc := range []struct {
		name, gap, outcome string
		edit               func(*wireTrace)
	}{
		{name: "missing parent", gap: "parent_not_retained", outcome: "partial_observed_path", edit: func(tr *wireTrace) { tr.Spans[2].References[0].SpanID = "9999999999999999" }},
		{name: "async link is not a child", gap: "async_links_not_serial_children", outcome: "partial_observed_path", edit: func(tr *wireTrace) { tr.Spans[2].References[0].Type = "FOLLOWS_FROM" }},
		{name: "cross trace parent", gap: "external_trace_parent_not_fetched", outcome: "partial_observed_path", edit: func(tr *wireTrace) { tr.Spans[2].References[0].TraceID = "22222222222222222222222222222222" }},
		{name: "cycle", gap: "cyclic_span_references", outcome: "inconclusive", edit: func(tr *wireTrace) {
			tr.Spans[0].References = []reference{{Type: "CHILD_OF", TraceID: testID, SpanID: tr.Spans[3].ID}}
		}},
		{name: "clock or async lifetime", gap: "clock_or_async_lifetime_unverified", outcome: "observed_stages_connected", edit: func(tr *wireTrace) { tr.Spans[2].Start = intPointer(*tr.Spans[0].Start - 2000000) }},
		{name: "overlapping siblings are not summed", gap: "durations_are_inclusive_not_additive", outcome: "observed_stages_connected"},
		{name: "partial insert only", gap: "expected_connected_stages_not_established", outcome: "partial_observed_path", edit: func(tr *wireTrace) { tr.Spans = tr.Spans[:1] }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := fixture("write")
			if tc.edit != nil {
				tc.edit(&tr)
			}
			server := serve(t, []wireTrace{tr})
			report := run(t, request("e2e", "write"), connection(t, server.URL, nil))
			require.True(t, hasGap(report, tc.gap))
			require.True(t, hasOutcome(report, tc.outcome))
			raw, err := json.Marshal(report)
			require.NoError(t, err)
			require.NotContains(t, string(raw), "critical_path_ms")
		})
	}
}

func TestLongReadNextIsNotAutomaticallyReadLatency(t *testing.T) {
	tr := fixture("read")
	tr.Spans = tr.Spans[2:3]
	server := serve(t, []wireTrace{tr})
	report := run(t, request("wp", "read"), connection(t, server.URL, nil))
	require.False(t, hasOutcome(report, "observed_slow_span"))
	require.True(t, hasGap(report, "long_readnext_may_be_normal_tailing"))
	tr.Spans[0].Tags = []tag{{Key: "error", Type: "bool", Value: json.RawMessage("true")}}
	server = serve(t, []wireTrace{tr})
	report = run(t, request("wp", "read"), connection(t, server.URL, nil))
	require.True(t, hasOutcome(report, "observed_span_error"))
}

func TestDuplicateViewsAndConflicts(t *testing.T) {
	tr := fixture("write")
	tr.Spans = append(tr.Spans, tr.Spans[3])
	server := serve(t, []wireTrace{tr, tr})
	report := run(t, request("e2e", "write"), connection(t, server.URL, nil))
	require.Equal(t, "completed", report.Status)
	require.Len(t, report.Results[0].Observations, 2)
	warnings := 0
	for _, f := range report.Results[0].Findings {
		if f.Severity == "warning" {
			warnings++
		}
	}
	require.Equal(t, 4, warnings)
	tr.Spans[len(tr.Spans)-1].Duration = intPointer(999)
	server = serve(t, []wireTrace{tr})
	report = run(t, request("e2e", "write"), connection(t, server.URL, nil))
	require.Equal(t, "failed", report.Status)
	require.True(t, hasGap(report, "conflicting_duplicate_span"))
}

func TestBoundedSearchAndRequestSubtree(t *testing.T) {
	tr := fixture("write")
	tr.Spans[0].Tags = []tag{testTag("request_id", "selected")}
	other := tr.Spans[0]
	other.ID = "9999999999999999"
	other.Duration = intPointer(10000000)
	other.Tags = []tag{testTag("request_id", "unrelated")}
	tr.Spans = append(tr.Spans, other)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "proxy", r.URL.Query().Get("service"))
		require.NotEmpty(t, r.URL.Query().Get("start"))
		require.NotEmpty(t, r.URL.Query().Get("end"))
		require.Equal(t, "21", r.URL.Query().Get("limit"))
		var tags map[string]string
		require.NoError(t, json.Unmarshal([]byte(r.URL.Query().Get("tags")), &tags))
		require.Equal(t, "selected", tags["request_id"])
		json.NewEncoder(w).Encode(map[string]any{"data": []wireTrace{tr}})
	}))
	defer server.Close()
	r := request("e2e", "write")
	r.Selection.Target.TraceID = ""
	r.Selection.Target.OperationID = "selected"
	from, to := time.Now().Add(-2*time.Minute), time.Now()
	r.Selection.Window = d.Window{Mode: "historical", From: &from, To: &to}
	report := run(t, r, connection(t, server.URL, nil))
	require.Equal(t, "completed", report.Status)
	require.True(t, hasOutcome(report, "observed_stages_connected"))
	for _, f := range report.Results[0].Findings {
		if f.RuleID == "trace.slow_span" {
			require.NotContains(t, f.Summary, other.ID)
		}
	}
}

func TestPrivacyAndPartialRealInsertFixture(t *testing.T) {
	tr := fixture("write")
	tr.Spans[3].Tags = []tag{testTag("authorization", "sentinel-secret"), {Key: "error", Type: "bool", Value: json.RawMessage("true")}}
	tr.Warnings = []json.RawMessage{json.RawMessage(`"sentinel-secret"`)}
	server := serve(t, []wireTrace{tr})
	report := run(t, request("e2e", "write"), connection(t, server.URL, nil))
	raw, err := json.Marshal(report)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "sentinel-secret")
	require.NotContains(t, string(raw), server.URL)
	require.True(t, hasOutcome(report, "observed_span_error"))
	data, err := os.ReadFile("testdata/partial-insert-v1.json")
	require.NoError(t, err)
	var real wireTrace
	require.NoError(t, json.Unmarshal(data, &real))
	server = serve(t, []wireTrace{real})
	r := request("e2e", "write")
	r.Selection.Target.TraceID = real.ID
	report = run(t, r, connection(t, server.URL, nil))
	require.Equal(t, "completed", report.Status)
	require.True(t, hasOutcome(report, "partial_observed_path"))
	require.True(t, hasGap(report, "insert_propagation_can_be_partial"))
	require.False(t, hasOutcome(report, "observed_stages_connected"))
}

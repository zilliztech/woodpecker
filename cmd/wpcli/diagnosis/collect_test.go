package diagnosis

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
)

func testOptions() Options {
	return Options{
		Scenario: "stuck-write", Mode: "live", Window: 20 * time.Millisecond, Interval: 10 * time.Millisecond,
		Budget: time.Second, RequestTimeout: time.Second, Concurrency: 1,
		Limits: Limits{MaxNodes: 1, MaxSamples: 3, MaxOps: 10, MaxSeries: 20, ResponseBytes: 4096},
	}
}

const testMetrics = `# TYPE process_start_time_seconds gauge
process_start_time_seconds 100
# TYPE woodpecker_server_logstore_operations_total counter
woodpecker_server_logstore_operations_total{node_id="node-1",log_id="2",log_ns="tenant-a",operation="add_entry",status="success"} 5
`

func TestCollectionCoverageAndRedaction(t *testing.T) {
	for _, tc := range []struct {
		name, path, body, code, status string
		httpStatus                     int
		maxOps, maxSeries              int
	}{
		{name: "working", status: "completed"},
		{name: "missing metrics", path: "/metrics", httpStatus: 404, status: "partial", code: "http_404"},
		{name: "missing ops", path: "/admin/runtime/ops", httpStatus: 503, body: "secret-password", status: "partial", code: "http_503"},
		{name: "malformed metrics", path: "/metrics", body: "a{unclosed", status: "partial", code: "invalid_metrics_schema"},
		{name: "NaN counter", path: "/metrics", body: strings.ReplaceAll(testMetrics, "} 5", "} NaN"), status: "partial", code: "invalid_completion_counter"},
		{name: "stale timestamp not current evidence", path: "/metrics", body: strings.ReplaceAll(testMetrics, "} 5", "} 5 123"), status: "completed", code: "explicit_metric_timestamp_unverified"},
		{name: "null ops", path: "/admin/runtime/ops", body: "null", status: "partial", code: "invalid_ops_schema"},
		{name: "missing op start", path: "/admin/runtime/ops", body: `[{"op_id":"op","op_type":"logstore.add_entry","log_id":2}]`, status: "partial", code: "incomplete_ops_identity"},
		{name: "conflicting op ID", path: "/admin/runtime/ops", body: `[{"op_id":"op","op_type":"logstore.add_entry","log_id":2,"started_at":"2026-10-01T00:00:00Z"},{"op_id":"op","op_type":"logstore.get_batch_entries","log_id":2,"started_at":"2026-10-01T00:00:00Z"}]`, status: "partial", code: "conflicting_op_identity"},
		{name: "missing registry", path: "/admin/runtime/ops/stats", httpStatus: 404, status: "completed", code: "http_404"},
		{name: "incomplete registry", path: "/admin/runtime/ops/stats", body: `{}`, status: "completed", code: "invalid_registry_schema"},
		{name: "unknown version", path: "/admin/env", body: `{"build":{}}`, status: "completed", code: "build_version_unknown"},
		{name: "version mismatch", path: "/admin/env", body: `{"build":{"version":"v2.0.0"}}`, status: "completed", code: "unsupported_version"},
		{name: "response limit", path: "/metrics", body: strings.Repeat("x", 4097), status: "partial", code: "response_byte_limit"},
		{name: "ops limit", path: "/admin/runtime/ops", body: `[{"op_id":"a","op_type":"file.flush","started_at":"2026-10-01T00:00:00Z"},{"op_id":"b","op_type":"file.flush","started_at":"2026-10-01T00:00:00Z"}]`, maxOps: 1, status: "partial", code: "ops_limit_exceeded"},
		{name: "series limit", path: "/metrics", body: testMetrics + `woodpecker_server_logstore_operations_total{node_id="node-1",log_id="3",operation="add_entry",status="success"} 1
`, maxSeries: 1, status: "partial", code: "metric_series_limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodGet {
					t.Errorf("unexpected mutation: %s", req.Method)
				}
				if req.URL.Path == tc.path {
					if tc.httpStatus != 0 {
						w.WriteHeader(tc.httpStatus)
					}
					_, _ = fmt.Fprint(w, tc.body)
					return
				}
				switch req.URL.Path {
				case "/metrics":
					_, _ = fmt.Fprint(w, testMetrics)
				case "/admin/runtime/ops":
					if req.URL.Query().Get("limit") == "" {
						t.Error("ops request has no limit")
					}
					_, _ = fmt.Fprint(w, `[{"op_id":"op","op_type":"logstore.add_entry","log_id":2,"segment_id":7,"started_at":"2026-10-01T00:00:00Z"}]`)
				case "/admin/runtime/ops/stats":
					_, _ = fmt.Fprint(w, `{"capacity":1024,"in_use":1,"evicted_total":0}`)
				case "/admin/env":
					_, _ = fmt.Fprint(w, `{"env":{"PASSWORD":"secret-password"},"build":{"version":"v0.1.46"}}`)
				default:
					t.Errorf("unexpected endpoint: %s", req.URL.Path)
				}
			}))
			defer server.Close()
			o := testOptions()
			if tc.maxOps > 0 {
				o.Limits.MaxOps = tc.maxOps
			}
			if tc.maxSeries > 0 {
				o.Limits.MaxSeries = tc.maxSeries
			}
			require.NoError(t, o.Validate())
			report := NewReport(o, "local")
			Collect(context.Background(), &report, []Node{{Target: Target{NodeID: "node-1", Identity: "discovered"}, URL: server.URL}}, o)
			require.Equal(t, tc.status, report.Status)
			if tc.code != "" {
				found := false
				for _, gap := range report.Gaps {
					found = found || gap.Code == tc.code
				}
				require.True(t, found, "missing code %s in %v", tc.code, report.Gaps)
			}
			body, err := json.Marshal(report)
			require.NoError(t, err)
			require.NotContains(t, string(body), "secret-password")
			require.NotContains(t, string(body), server.URL)
			require.NotContains(t, string(body), "PASSWORD")
			if tc.name == "working" {
				require.Equal(t, "suspected_stall", report.Findings[0].Outcome)
			}
			if tc.name == "version mismatch" {
				require.Equal(t, "inconclusive", report.Findings[0].Outcome)
			}
		})
	}
}

func TestCollectorCancellationAndBudget(t *testing.T) {
	for _, cancelNow := range []bool{false, true} {
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
			<-req.Context().Done()
		}))
		o := testOptions()
		var ctx context.Context
		var cancel context.CancelFunc
		if cancelNow {
			ctx, cancel = context.WithCancel(context.Background())
			cancel()
		} else {
			ctx, cancel = context.WithTimeout(context.Background(), 15*time.Millisecond)
		}
		r := NewReport(o, "")
		started := time.Now()
		Collect(ctx, &r, []Node{{Target: Target{NodeID: "node-1"}, URL: server.URL}}, o)
		cancel()
		server.Close()
		require.Less(t, time.Since(started), time.Second)
		want := "budget_exceeded"
		if cancelNow {
			want = "cancelled"
		}
		require.Equal(t, want, r.Status)
		require.Len(t, r.Findings, 1)
		for _, f := range r.Findings {
			require.Equal(t, "inconclusive", f.Outcome)
		}
	}
}

func TestOptionsRejectUnboundedOrHistoricalRequests(t *testing.T) {
	for _, update := range []func(*Options){
		func(o *Options) { o.Mode = "historical" },
		func(o *Options) { o.Scenario = "slow-compact" },
		func(o *Options) { o.Window = 0 },
		func(o *Options) { o.Interval = 0 },
		func(o *Options) { o.Interval = time.Nanosecond },
		func(o *Options) { o.Budget = 0 },
		func(o *Options) { o.Budget = time.Nanosecond },
		func(o *Options) { o.Window = time.Hour },
		func(o *Options) { o.Limits.MaxNodes = 33 },
		func(o *Options) { o.Limits.MaxOps = 1001 },
		func(o *Options) { o.Limits.MaxSeries = 5001 },
		func(o *Options) { o.Limits.MaxSamples = 2 },
		func(o *Options) { o.Limits.ResponseBytes = 1 },
		func(o *Options) { o.RequestTimeout = 0 },
		func(o *Options) { o.Concurrency = 0 },
	} {
		o := testOptions()
		update(&o)
		require.Error(t, o.Validate())
	}
}

func TestSlowBaselineDoesNotShortenLiveWindow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/admin/env" {
			time.Sleep(30 * time.Millisecond)
			_, _ = fmt.Fprint(w, `{"build":{"version":"dev"}}`)
		} else if req.URL.Path == "/metrics" {
			_, _ = fmt.Fprint(w, testMetrics)
		} else if req.URL.Path == "/admin/runtime/ops" {
			_, _ = fmt.Fprint(w, `[]`)
		} else {
			_, _ = fmt.Fprint(w, `{"capacity":1024,"in_use":0,"evicted_total":0}`)
		}
	}))
	defer server.Close()
	o := testOptions()
	r := NewReport(o, "")
	Collect(context.Background(), &r, []Node{{Target: Target{NodeID: "node-1"}, URL: server.URL}}, o)
	var first, last Observation
	for _, obs := range r.Observations {
		if obs.Kind == "ops" {
			if first.ID == "" {
				first = obs
			}
			last = obs
		}
	}
	require.GreaterOrEqual(t, last.StartedAt.Sub(first.FinishedAt), o.Window)
	require.Equal(t, "completed", r.Status)
}

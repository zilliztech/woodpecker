package webui

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/integrated"
)

type fixtureModule struct {
	calls   atomic.Int64
	entered chan struct{}
}

func (m *fixtureModule) Descriptor() d.Descriptor {
	profiles := []d.Profile{}
	for _, scope := range []string{"wp", "milvus", "e2e"} {
		for _, flow := range []string{"read", "write"} {
			profiles = append(profiles, d.Profile{ID: scope + "." + flow, Version: 1, Scope: scope, Flow: flow, Stage: map[string]string{"read": "query", "write": "append"}[flow], Modes: []string{"snapshot", "live", "historical"}, TargetFields: []string{"deployment", "instance"}})
		}
	}
	return d.Descriptor{Source: "prometheus", Profiles: profiles}
}
func (m *fixtureModule) ValidateConnection(d.Connection) error { return nil }
func (m *fixtureModule) Run(ctx context.Context, q d.Request, _ d.Connection) d.Result {
	m.calls.Add(1)
	if m.entered != nil {
		select {
		case m.entered <- struct{}{}:
		default:
		}
	}
	r := d.NewResult("prometheus")
	r.Status = "completed"
	r.Observations = []d.Observation{{ID: "original-observation", SourceID: "fixture-source", Kind: "fixture", Status: "observed", Data: json.RawMessage(`{"exact_int64":9007199254740993,"excerpt":"<img src=x onerror=alert(1)>"}`), Query: &d.SourceQuery{Language: "fixture", Expression: "bounded read-only evidence", Executed: true}, StartedAt: time.Unix(1000, 0), FinishedAt: time.Unix(1060, 0)}}
	r.Findings = []d.Finding{{ID: "original-finding", RuleID: "fixture.rule", RuleVersion: 1, Flow: q.Selection.Flow, Stage: q.Selection.Stage, Severity: "warning", Outcome: "fixture_suspicion", Strength: "fixture_observation", Summary: "Observed fixture symptom", Supporting: []string{"original-observation"}, Contradicting: []string{"original-observation"}, Unknown: []string{"Contradicting evidence stays visible"}, NextChecks: []string{"Inspect the original evidence"}}}
	if q.Selection.Window.Mode == "live" {
		select {
		case <-time.After(time.Duration(q.Selection.Window.DurationMS) * time.Millisecond):
		case <-ctx.Done():
		}
	}
	return r
}

func fixture(t *testing.T) (*Server, *fixtureModule) {
	t.Helper()
	m := &fixtureModule{entered: make(chan struct{}, 8)}
	runner, err := integrated.New(integrated.Definition{ID: "prometheus", Source: "prometheus", Module: m})
	require.NoError(t, err)
	config, err := d.ParseConfig([]byte(`schema_version: wpdiag.config/v1
sources:
  prometheus:
    connections:
      local:
        settings: {endpoint: 'http://private.invalid?token=sentinel-secret', password: sentinel-secret}
      disabled:
        enabled: false
        settings: {}
`), t.TempDir())
	require.NoError(t, err)
	s := New(context.Background(), config, runner)
	t.Cleanup(s.Close)
	return s, m
}

func requestFixture() integrated.Request {
	return integrated.Request{Selection: d.Selection{Scope: "e2e", Flow: "write", Window: d.Window{Mode: "snapshot"}}, Modules: []string{"prometheus"}, Connection: "local", Limits: d.DefaultLimits()}
}

func call(t *testing.T, s *Server, method, path string, q any) (int, []byte) {
	t.Helper()
	var body io.Reader
	if q != nil {
		b, err := json.Marshal(q)
		require.NoError(t, err)
		body = bytes.NewReader(b)
	}
	r := httptest.NewRequest(method, "http://127.0.0.1"+path, body)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-WPDiag-Session", s.token)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w.Code, w.Body.Bytes()
}

func startFixture(t *testing.T, s *Server, q integrated.Request) jobView {
	t.Helper()
	code, body := call(t, s, "POST", "/api/runs", q)
	require.Equal(t, 202, code, string(body))
	var j jobView
	require.NoError(t, json.Unmarshal(body, &j))
	return j
}

func finished(t *testing.T, s *Server, id string) jobView {
	t.Helper()
	var view jobView
	require.Eventually(t, func() bool {
		code, body := call(t, s, "GET", "/api/runs/"+id, nil)
		if code != 200 {
			return false
		}
		if json.Unmarshal(body, &view) != nil {
			return false
		}
		return view.ReportReady
	}, 3*time.Second, 5*time.Millisecond)
	return view
}

func TestCatalogPlanAndOriginBoundary(t *testing.T) {
	s, m := fixture(t)
	for _, path := range []string{"/", "/app.js", "/styles.css", "/api/catalog"} {
		code, body := call(t, s, "GET", path, nil)
		require.Equal(t, 200, code)
		require.NotContains(t, string(body), "sentinel-secret")
		require.NotContains(t, string(body), "private.invalid")
	}
	code, body := call(t, s, "GET", "/api/catalog", nil)
	require.Equal(t, 200, code)
	require.Contains(t, string(body), `"disabled"`)
	require.Contains(t, string(body), `"enabled":false`)
	code, body = call(t, s, "POST", "/api/plan", requestFixture())
	require.Equal(t, 200, code)
	require.Contains(t, string(body), `"ready"`)
	require.Zero(t, m.calls.Load())
	for _, tc := range []struct{ host, origin, token string }{
		{"rebinding.invalid", "", s.token}, {"127.0.0.1", "http://evil.invalid", s.token}, {"127.0.0.1", "http://127.0.0.1:9000", s.token}, {"127.0.0.1", "null", s.token}, {"127.0.0.1", "", ""},
	} {
		r := httptest.NewRequest("POST", "http://127.0.0.1/api/runs", strings.NewReader(`{}`))
		r.Host = tc.host
		r.Header.Set("Origin", tc.origin)
		r.Header.Set("X-WPDiag-Session", tc.token)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		require.Equal(t, 403, w.Code)
	}
	require.Zero(t, m.calls.Load())
	r := httptest.NewRequest("GET", "http://127.0.0.1/api/catalog", nil)
	r.Header.Set("Sec-Fetch-Site", "cross-site")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	require.Equal(t, 403, w.Code)
	require.Contains(t, w.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'")
	// A normal link navigation may open the UI; cross-site API calls stay blocked.
	r = httptest.NewRequest("GET", "http://127.0.0.1/", nil)
	r.Header.Set("Sec-Fetch-Site", "cross-site")
	w = httptest.NewRecorder()
	s.ServeHTTP(w, r)
	require.Equal(t, 200, w.Code)

	code, _ = call(t, s, "GET", "/does-not-exist", nil)
	require.Equal(t, 404, code)
}

func TestRunPreservesOriginalReportAndExactEvidence(t *testing.T) {
	s, _ := fixture(t)
	q := requestFixture()
	j := startFixture(t, s, q)
	done := finished(t, s, j.ID)
	require.Equal(t, "completed", done.Status)
	require.NotEmpty(t, done.Events)
	code, body := call(t, s, "GET", "/api/runs/"+j.ID+"/report", nil)
	require.Equal(t, 200, code)
	var report integrated.Report
	require.NoError(t, json.Unmarshal(body, &report))
	require.Equal(t, "original-finding", report.Summary.Findings[0].ID)
	f, ok := integrated.Resolve(report, report.Summary.Findings[0].Reference)
	require.True(t, ok)
	require.Equal(t, []string{"original-observation"}, f.Contradicting)
	require.Contains(t, string(body), "9007199254740993")
	require.NotContains(t, string(body), "sentinel-secret")
	code, evidence := call(t, s, "GET", "/api/runs/"+j.ID+"/evidence?module_id=prometheus&observation_id=original-observation", nil)
	require.Equal(t, 200, code)
	require.Contains(t, string(evidence), "9007199254740993")
	require.Contains(t, string(evidence), `"executed": true`)
	code, again := call(t, s, "GET", "/api/runs/"+j.ID+"/report?download=1", nil)
	require.Equal(t, 200, code)
	require.Equal(t, body, again)
	_, _ = call(t, s, "POST", "/api/runs/"+j.ID+"/cancel", map[string]any{})
	code, again = call(t, s, "GET", "/api/runs/"+j.ID+"/report", nil)
	require.Equal(t, 200, code)
	require.Equal(t, body, again, "Cancelling a finished job must not replace its report")
	code, _ = call(t, s, "GET", "/api/runs/"+j.ID+"/evidence?module_id=loki&observation_id=original-observation", nil)
	require.Equal(t, 404, code)
	code, _ = call(t, s, "GET", "/api/runs/unknown/report", nil)
	require.Equal(t, 404, code)
	code, _ = call(t, s, "POST", "/api/runs/unknown/cancel", map[string]any{})
	require.Equal(t, 404, code)
}

func TestRunLimitsCancellationAndShutdown(t *testing.T) {
	s, m := fixture(t)
	q := requestFixture()
	q.Selection.Window = d.Window{Mode: "live", DurationMS: 30000, IntervalMS: 1000}
	a := startFixture(t, s, q)
	b := startFixture(t, s, q)
	code, _ := call(t, s, "POST", "/api/runs", q)
	require.Equal(t, 429, code)
	code, _ = call(t, s, "GET", "/api/runs/"+a.ID+"/report", nil)
	require.Equal(t, 409, code)
	select {
	case <-m.entered:
	case <-time.After(time.Second):
		t.Fatal("No collection started")
	}
	code, _ = call(t, s, "POST", "/api/runs/"+a.ID+"/cancel", map[string]any{})
	require.Equal(t, 200, code)
	require.Equal(t, "cancelled", finished(t, s, a.ID).Status)
	code, body := call(t, s, "GET", "/api/runs/"+a.ID+"/report", nil)
	require.Equal(t, 200, code)
	require.Contains(t, string(body), "original-observation")
	s.Close()
	require.Equal(t, "cancelled", finished(t, s, b.ID).Status)
	code, _ = call(t, s, "POST", "/api/runs", q)
	require.Equal(t, 503, code)
}

func TestBudgetErrorsAndHistory(t *testing.T) {
	s, m := fixture(t)
	q := requestFixture()
	q.Selection.Flow = "invalid"
	code, body := call(t, s, "POST", "/api/runs", q)
	require.Equal(t, 422, code)
	require.Contains(t, string(body), "invalid_selection")
	require.Zero(t, m.calls.Load())
	q = requestFixture()
	q.Connection = "disabled"
	code, _ = call(t, s, "POST", "/api/runs", q)
	require.Equal(t, 422, code)
	require.Zero(t, m.calls.Load())
	q = requestFixture()
	q.Selection.Window = d.Window{Mode: "live", DurationMS: 30000, IntervalMS: 1000}
	q.Limits.BudgetMS = 10
	j := startFixture(t, s, q)
	require.Equal(t, "budget_exceeded", finished(t, s, j.ID).Status)
	first := j.ID
	// The encoder can compact a completed run into a budget failure. Job status
	// must reflect the exact report returned to the browser, not the prior status.
	q = requestFixture()
	q.Limits.MaxOutputBytes = 4096
	compact := startFixture(t, s, q)
	require.Equal(t, "budget_exceeded", finished(t, s, compact.ID).Status)
	code, body = call(t, s, "GET", "/api/runs/"+compact.ID+"/report", nil)
	require.Equal(t, 200, code)
	require.LessOrEqual(t, len(body), 4096)
	require.Contains(t, string(body), `"status": "budget_exceeded"`)
	for i := 0; i < maxJobs; i++ {
		j = startFixture(t, s, requestFixture())
		finished(t, s, j.ID)
	}
	code, _ = call(t, s, "GET", "/api/runs/"+first, nil)
	require.Equal(t, 404, code)
	code, body = call(t, s, "GET", "/api/runs", nil)
	require.Equal(t, 200, code)
	var rows []map[string]any
	require.NoError(t, json.Unmarshal(body, &rows))
	require.Len(t, rows, maxJobs)
	// Force the retention threshold without allocating a 64MiB fixture.
	s.mu.Lock()
	s.retained = maxHistoryBytes + 1
	s.evictLocked(maxJobs, maxHistoryBytes, "")
	require.Less(t, len(s.order), maxJobs)
	s.mu.Unlock()
}

func TestRequestBoundsAndLoopbackListen(t *testing.T) {
	s, _ := fixture(t)
	for _, body := range []string{`{"unknown":"sentinel-secret"}`, `{} {}`, `{"connection":"` + strings.Repeat("a", maxRequestBytes) + `"}`} {
		r := httptest.NewRequest("POST", "http://127.0.0.1/api/plan", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-WPDiag-Session", s.token)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		require.Equal(t, 400, w.Code)
		require.NotContains(t, w.Body.String(), "sentinel-secret")
	}
	r := httptest.NewRequest("POST", "http://127.0.0.1/api/plan", strings.NewReader(`{}`))
	r.Header.Set("X-WPDiag-Session", s.token)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	require.Equal(t, 415, w.Code)
	for _, addr := range []string{":8787", "0.0.0.0:8787", "remote.invalid:8787", "127.0.0.1:99999", "127.0.0.1:bad"} {
		require.Error(t, Serve(context.Background(), addr, d.Config{}, io.Discard))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, Serve(ctx, "127.0.0.1:0", d.Config{}, io.Discard))
}

func TestHTTPRunOutlivesRequestAndUsesSameRunner(t *testing.T) {
	s, _ := fixture(t)
	httpServer := httptest.NewServer(s)
	defer httpServer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	q := requestFixture()
	q.Selection.Window = d.Window{Mode: "live", DurationMS: 50, IntervalMS: 10}
	body, err := json.Marshal(q)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, "POST", httpServer.URL+"/api/runs", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-WPDiag-Session", s.token)
	req.Header.Set("Origin", httpServer.URL)
	resp, err := httpServer.Client().Do(req)
	require.NoError(t, err)
	var j jobView
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&j))
	require.NoError(t, resp.Body.Close())
	cancel()
	require.Equal(t, "completed", finished(t, s, j.ID).Status)
	code, data := call(t, s, "GET", "/api/runs/"+j.ID+"/report", nil)
	require.Equal(t, 200, code)
	var actual integrated.Report
	require.NoError(t, json.Unmarshal(data, &actual))
	direct := s.runner.Run(context.Background(), s.config, q, nil)
	require.Equal(t, direct.Runs[0].Report.Results[0].Findings, actual.Runs[0].Report.Results[0].Findings)
	require.Equal(t, direct.Summary.Findings, actual.Summary.Findings)
	require.Equal(t, fmt.Sprint(direct.Summary.Gaps), fmt.Sprint(actual.Summary.Gaps))
}

package webui

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/diagnostics/checks"
	"github.com/zilliztech/woodpecker/diagnostics/evidence"
	"github.com/zilliztech/woodpecker/diagnostics/reports"
	"github.com/zilliztech/woodpecker/diagnostics/wal"
)

func TestWorkspaceAnalysisHistoryPreservesEvidenceAndPriorContext(t *testing.T) {
	app, srv, _, calls := workspaceFixture(t)
	dir := t.TempDir()
	packetPath := filepath.Join(dir, "last-packet.json")
	executable := filepath.Join(dir, "fake-codex")
	script := fmt.Sprintf("#!/bin/sh\nif [ \"$1 $2\" = 'login status' ]; then exit 0; fi\ncat > %q\nprintf '%%s\\n' '{\"type\":\"turn.started\"}' '{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"## Finding\\nHigh filesystem usage; symptom impact unconfirmed. [S06]\"}}' '{\"type\":\"turn.completed\"}'\n", packetPath)
	require.NoError(t, os.WriteFile(executable, []byte(script), 0o700))
	require.NoError(t, app.workspace.file.Agent.Encode(map[string]any{"enabled": true, "provider": "codex", "auth": "cli", "executable": executable, "timeout_ms": 2000}))
	ctx := app.workspace.file.Contexts["a"]
	ctx.Agent = app.workspace.file.Agent
	app.workspace.file.Contexts["a"] = ctx
	a, b := browser(t, srv.URL), browser(t, srv.URL)
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	owner := ""
	for _, cookie := range a.Jar.Cookies(u) {
		if cookie.Name == "wp-session" {
			owner = cookie.Value
		}
	}
	require.NotEmpty(t, owner)
	report := checks.Report{SchemaVersion: checks.ReportSchema, RunID: "source-run", Request: checks.Request{Target: "a", Scenario: "slow-writes", From: time.Now().Add(-5 * time.Minute), To: time.Now()}, Checks: []checks.Result{{Evidence: evidence.Evidence{ID: "S06", Status: "abnormal", Value: "92%"}}}}
	report.Summary.Metrics.Abnormal = 1
	original := checks.Encode(report)
	app.mu.Lock()
	app.jobs["diagnosis"] = &job{owner: owner, cancel: func() {}, body: original, view: jobView{ID: "diagnosis", Kind: "diag", Status: "completed", ReportReady: true, DiagRequest: &report.Request, DiagSummary: &report.Summary, CreatedAt: time.Now()}}
	app.jobs["other-diagnosis"] = &job{owner: owner, cancel: func() {}, body: original, view: jobView{ID: "other-diagnosis", Kind: "diag", Status: "completed", ReportReady: true, CreatedAt: time.Now()}}
	app.order = append(app.order, "diagnosis", "other-diagnosis")
	app.retained += 2 * len(original)
	app.mu.Unlock()
	before := calls.Load()
	previous := ""
	for n := 0; n < maxJobs+2; n++ {
		question := ""
		if n > 0 {
			question = "Which evidence confirms the symptom impact?"
		}
		code, data := wsCall(t, a, srv.URL, app.token, "POST", "/api/diag/runs/diagnosis/analyze", map[string]string{"previous_analysis_id": previous, "question": question})
		require.Equal(t, 202, code, string(data))
		var started jobView
		require.NoError(t, json.Unmarshal(data, &started))
		require.Eventually(t, func() bool {
			_, data := wsCall(t, a, srv.URL, app.token, "GET", "/api/runs/"+started.ID, nil)
			var j jobView
			return json.Unmarshal(data, &j) == nil && j.ReportReady
		}, 3*time.Second, 10*time.Millisecond)
		code, data = wsCall(t, a, srv.URL, app.token, "GET", "/api/runs/"+started.ID+"/report", nil)
		require.Equal(t, 200, code)
		var result wal.Analysis
		require.NoError(t, json.Unmarshal(data, &result))
		require.Equal(t, "completed", result.Status)
		expected, err := reports.Packet(original)
		require.NoError(t, err)
		require.Equal(t, expected.ReportSHA256, result.ReportSHA256)
		packetJSON, err := os.ReadFile(packetPath)
		require.NoError(t, err)
		var input wal.Packet
		require.NoError(t, json.Unmarshal(packetJSON, &input))
		require.Equal(t, question, input.Question)
		if n == 0 {
			require.Nil(t, input.PreviousAnalysis)
		} else {
			require.Equal(t, previous, input.PreviousAnalysis.ID)
			require.Equal(t, "codex", result.PreviousAnalysis.Provider)
			require.Contains(t, input.PreviousAnalysis.Text, "High filesystem usage")
		}
		previous = started.ID
		code, data = wsCall(t, a, srv.URL, app.token, "GET", "/api/runs/diagnosis/report", nil)
		require.Equal(t, 200, code)
		require.Equal(t, original, data)
		_, data = wsCall(t, a, srv.URL, app.token, "GET", "/api/runs", nil)
		var jobs []jobView
		require.NoError(t, json.Unmarshal(data, &jobs))
		require.LessOrEqual(t, len(jobs), maxJobs)
	}
	// Nothing in report analysis should query Admin, Prometheus or Trace again.
	require.Equal(t, before, calls.Load())
	code, _ := wsCall(t, b, srv.URL, app.token, "POST", "/api/diag/runs/diagnosis/analyze", map[string]string{"previous_analysis_id": previous})
	require.Equal(t, 404, code)
	// The retained answer cannot be attached to a different run, even with identical JSON.
	app.mu.Lock()
	app.jobs["different"] = &job{owner: owner, cancel: func() {}, body: original, view: jobView{ID: "different", Kind: "diag", ReportReady: true, CreatedAt: time.Now()}}
	app.order = append(app.order, "different")
	app.retained += len(original)
	app.mu.Unlock()
	code, _ = wsCall(t, a, srv.URL, app.token, "POST", "/api/diag/runs/different/analyze", map[string]string{"previous_analysis_id": previous})
	require.Equal(t, 404, code)
	code, data := wsCall(t, a, srv.URL, app.token, "GET", "/api/runs/"+previous, nil)
	require.Equal(t, 200, code)
	require.Contains(t, string(data), "previous_analysis_id")
}

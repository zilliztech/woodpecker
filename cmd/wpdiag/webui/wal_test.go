package webui

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/diagnostics/wal"
)

func TestWALJobsKeepUnknownReportsAndShareHistory(t *testing.T) {
	s, m := fixture(t)
	for _, path := range []string{"/", "/advanced", "/wal.js", "/wal.css", "/api/wal/catalog"} {
		code, body := call(t, s, "GET", path, nil)
		require.Equal(t, 200, code, string(body))
		require.NotContains(t, string(body), "sentinel-secret")
		require.NotContains(t, string(body), "private.invalid")
	}
	q := wal.Request{Problem: "write_slow", Target: "local", From: time.Now().Add(-5 * time.Minute), To: time.Now().Add(-time.Second)}
	code, body := call(t, s, "POST", "/api/wal/runs", q)
	require.Equal(t, 202, code, string(body))
	var j jobView
	require.NoError(t, json.Unmarshal(body, &j))
	require.Equal(t, "wal", j.Kind)
	completed := finished(t, s, j.ID)
	require.NotEmpty(t, completed.WALEvents)
	code, body = call(t, s, "GET", "/api/runs/"+j.ID+"/report", nil)
	require.Equal(t, 200, code)
	var r wal.Report
	require.NoError(t, json.Unmarshal(body, &r))
	require.NotEmpty(t, r.Evidence)
	for _, e := range r.Evidence {
		require.Equal(t, "unknown", e.Status)
	}
	code, body = call(t, s, "GET", "/api/wal/runs/"+j.ID+"/packet", nil)
	require.Equal(t, 200, code)
	require.Contains(t, string(body), "report_sha256")
	code, _ = call(t, s, "POST", "/api/wal/runs/"+j.ID+"/analyze", map[string]any{"findings": "forged"})
	require.Equal(t, 400, code)
	code, _ = call(t, s, "POST", "/api/wal/runs/"+j.ID+"/analyze", struct{}{})
	require.Equal(t, 422, code)
	code, inspected := call(t, s, "GET", "/api/wal/runs/"+j.ID+"/inspect/W01", nil)
	require.Equal(t, 200, code, string(inspected))
	require.Contains(t, string(inspected), "0.99 × Δcount")
	require.NotContains(t, string(inspected), "sentinel-secret")
	code, _ = call(t, s, "GET", "/api/wal/runs/"+j.ID+"/inspect/not-collected", nil)
	require.Equal(t, 404, code)
	require.Zero(t, m.calls.Load(), "WAL jobs must not silently invoke old broad Milvus diagnosis")
}

func TestWALRejectsInvalidSelectionBeforeStarting(t *testing.T) {
	s, _ := fixture(t)
	for _, q := range []wal.Request{{Problem: "query", Target: "local"}, {Problem: "write_slow", Target: "unconfigured"}, {Problem: "write_slow", Target: "local", From: time.Now(), To: time.Now().Add(time.Hour)}} {
		code, _ := call(t, s, "POST", "/api/wal/runs", q)
		require.Equal(t, 422, code)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Empty(t, s.jobs)
}

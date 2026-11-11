package cmd

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testPromMetrics = `# HELP woodpecker_server_logstore_active_logs Number of active logs
# TYPE woodpecker_server_logstore_active_logs gauge
woodpecker_server_logstore_active_logs{node_id="node-1",namespace="bucket/root"} 5
# HELP woodpecker_server_logstore_operations_total Total operations
# TYPE woodpecker_server_logstore_operations_total counter
woodpecker_server_logstore_operations_total{node_id="node-1",namespace="bucket/root",log_id="42",operation="add_entry",status="success"} 1234
# HELP woodpecker_server_op_registry_evicted_total Total evictions
# TYPE woodpecker_server_op_registry_evicted_total counter
woodpecker_server_op_registry_evicted_total{signal="young"} 95
woodpecker_server_op_registry_evicted_total{signal="old"} 5
`

func metricsTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"members": []map[string]any{
				{"id": "node-1", "gossip_addr": "127.0.0.1:17946", "service_addr": "127.0.0.1:18080"},
			},
		})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(testPromMetrics))
	})
	return httptest.NewServer(mux)
}

func TestMetricsList(t *testing.T) {
	srv := metricsTestServer(t)
	defer srv.Close()
	out, err := runLogstoreCmd(t, srv, "metrics", "list", "node-1")
	require.NoError(t, err)
	assert.Contains(t, out, "logstore_active_logs")
	assert.Contains(t, out, "gauge")
}

func TestMetricsList_WithFilter(t *testing.T) {
	srv := metricsTestServer(t)
	defer srv.Close()
	out, err := runLogstoreCmd(t, srv, "metrics", "list", "node-1", "--filter", "evicted")
	require.NoError(t, err)
	assert.Contains(t, out, "evicted")
	assert.NotContains(t, out, "active_logs")
}

func TestMetricsSnapshot(t *testing.T) {
	srv := metricsTestServer(t)
	defer srv.Close()
	out, err := runLogstoreCmd(t, srv, "metrics", "snapshot", "node-1", "--metric", "woodpecker_server_logstore_active_logs")
	require.NoError(t, err)
	assert.Contains(t, out, "node-1")
	assert.Contains(t, out, "5")
}

func TestMetricsTop(t *testing.T) {
	srv := metricsTestServer(t)
	defer srv.Close()
	out, err := runLogstoreCmd(t, srv, "metrics", "top", "--by", "woodpecker_server_logstore_active_logs")
	require.NoError(t, err)
	assert.Contains(t, out, "node-1")
	assert.Contains(t, out, "5")
}

func TestMetricsTop_MissingBy(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"--endpoint", "http://localhost:9091", "metrics", "top"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--by is required")
}

func TestMetricsReport_ListScenarios(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"metrics", "report", "--list"})
	err := root.Execute()
	require.NoError(t, err)
	assert.Contains(t, buf.String(), "stuck-flush")
	assert.Contains(t, buf.String(), "hot-write")
}

func TestMetricsReport_MissingScenario(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"--endpoint", "http://localhost:9091", "metrics", "report"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--scenario is required")
}

func TestMetricsReport_UnknownScenario(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"--endpoint", "http://localhost:9091", "metrics", "report", "--scenario", "nonexistent"})
	err := root.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown scenario")
}

func TestMetricsReport_StuckFlush(t *testing.T) {
	srv := metricsTestServer(t)
	defer srv.Close()
	// Use very short window to avoid long test
	out, err := runLogstoreCmd(t, srv, "metrics", "report", "node-1", "--scenario", "stuck-flush", "--window", "1s", "--interval", "500ms")
	// Should find the old eviction signal (evicted_old=5 > 0)
	// This returns a red finding error (exit code 9)
	require.Error(t, err)
	assert.Contains(t, out, "stuck-flush")
	assert.Contains(t, out, "old eviction counter")
}

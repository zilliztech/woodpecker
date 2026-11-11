package cmd

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// healthTestServer answers the memberlist plus whatever health endpoints a case needs.
func healthTestServer(t *testing.T, routes map[string]func(http.ResponseWriter, *http.Request)) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"members":[{"id":"node-1","gossip_addr":"10.244.1.5:17946","service_addr":"10.244.1.5:18080"}]}`))
	})
	for path, h := range routes {
		mux.HandleFunc(path, h)
	}
	return httptest.NewServer(mux)
}

// TestNodeLogHealth_503KeepsTheReport pins the case the command exists for. /admin/log-health
// answers 503 only when every tracked log is Stalled or Failed, and it encodes the full report
// with that status, so the body carries exactly the per-log detail the operator needs. Reporting
// the status alone turns a data-plane failure into an unreachable node.
func TestNodeLogHealth_503KeepsTheReport(t *testing.T) {
	report := `{"state":"Failed","reason":"all logs stalled","node_id":"node-1",
		"tracked_logs":2,"healthy_logs":0,"stalled_logs":1,"failed_logs":1,"idle_logs":0,
		"logs":[{"log_id":7,"bucket_name":"b","root_path":"r","write_state":"Stalled","read_state":"Ok","last_failure_reason":"quorum unreachable"},
		        {"log_id":8,"bucket_name":"b","root_path":"r","write_state":"Failed","read_state":"Failed","last_failure_reason":"fenced"}]}`
	srv := healthTestServer(t, map[string]func(http.ResponseWriter, *http.Request){
		"/admin/log-health": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(report))
		},
	})
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"node", "log-health", "127.0.0.1:" + extractPort(t, srv.URL), "--timeout", "3s"})

	err := root.Execute()

	require.Error(t, err, "all logs stalled or failed is not a successful command")
	require.NotContains(t, err.Error(), "returned status 503",
		"a 503 carrying the report is a data-plane finding, not a transport failure")
	out := buf.String()
	require.Contains(t, out, "stalled: 1", "the counts must survive a 503")
	require.Contains(t, out, "quorum unreachable", "the per-log table must survive a 503")
}

// TestNodeHealthz_JSONOutputIsParseable covers `-o json`: /healthz already answers JSON, so the
// raw body passes through for piping into jq.
func TestNodeHealthz_JSONOutputIsParseable(t *testing.T) {
	srv := healthTestServer(t, map[string]func(http.ResponseWriter, *http.Request){
		"/healthz": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"state":"Healthy","detail":[{"name":"disk","code":"OK"}]}`))
		},
	})
	defer srv.Close()
	withCliYAML(t, srv.URL)

	root := NewRootCommand()
	out := new(bytes.Buffer)
	root.SetOut(out)
	root.SetErr(new(bytes.Buffer))
	root.SetArgs([]string{"node", "healthz", "127.0.0.1:" + extractPort(t, srv.URL), "-o", "json", "--timeout", "3s"})

	require.NoError(t, root.Execute())

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &parsed), "-o json must emit JSON, got: %s", out.String())
	require.Equal(t, "Healthy", parsed["state"])
}

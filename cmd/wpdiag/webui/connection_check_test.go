package webui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/cmd/wpcli/candidates"
	d "github.com/zilliztech/woodpecker/diagnostics"
)

func TestConnectionCheckUsesCurrentFormWithoutRunningDiagnostics(t *testing.T) {
	app, server, path, originalCalls := workspaceFixture(t)
	client := browser(t, server.URL)
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		require.Equal(t, "/api/v1/query", r.URL.Path)
		require.Equal(t, "vector(1)", r.URL.Query().Get("query"))
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"1"]}]}}`)
	}))
	defer api.Close()
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	// Bad assessment rules do not prevent testing the independent source connection.
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(path), "diag-metrics.yaml"), []byte("invalid: true\n"), 0o600))
	request := map[string]any{"source": "metrics", "bindings": map[string]any{"server": map[string]any{"metrics_endpoint": api.URL, "deployment": "fixture", "metric_labels": map[string]string{"namespace": "fixture"}, "log_id": "invalid-but-unused", "metadata": map[string]string{"etcd": "not-used", "prefix": "not-used"}}}}
	code, body := wsCall(t, client, server.URL, app.token, "POST", "/api/workspace/connections/check", request)
	require.Equal(t, 200, code, string(body))
	var result struct {
		Results []d.ConnectionCheck `json:"results"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	require.Len(t, result.Results, 1)
	require.Equal(t, "ready", result.Results[0].Status, result)
	require.EqualValues(t, 1, calls.Load())
	require.Zero(t, originalCalls.Load())
	require.Empty(t, app.jobs, "connection testing does not create or run diagnoses")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	history, err := candidates.List()
	require.NoError(t, err)
	require.Len(t, history["metrics_endpoint"], 1)
	require.Empty(t, history["log_id"])
	require.Empty(t, history["metadata.etcd"])
	code, _ = wsCall(t, client, server.URL, app.token, "POST", "/api/workspace/connections/check", map[string]any{"source": "loki"})
	require.Equal(t, 400, code)
}

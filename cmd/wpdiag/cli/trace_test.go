package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func TestTraceCapabilitiesAndRealFixtureThroughCLI(t *testing.T) {
	var out, errout bytes.Buffer
	require.Zero(t, Execute(context.Background(), []string{"capabilities", "-o", "json"}, &out, &errout))
	var catalog d.Catalog
	require.NoError(t, json.Unmarshal(out.Bytes(), &catalog))
	var profiles []d.Profile
	for _, source := range catalog.Sources {
		if source.Source == "trace" {
			require.Equal(t, "available", source.Status)
			profiles = source.Profiles
		}
	}
	require.Len(t, profiles, 6)
	for _, p := range profiles {
		require.Contains(t, p.TargetFields, "trace_id")
		require.Contains(t, p.Modes, "snapshot")
	}
	raw, err := os.ReadFile("../../../diagnostics/jaeger/testdata/partial-insert-v1.json")
	require.NoError(t, err)
	var trace map[string]any
	require.NoError(t, json.Unmarshal(raw, &trace))
	id := trace["traceID"].(string)
	requests := 0
	fail := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if fail {
			w.WriteHeader(401)
			_, _ = w.Write([]byte("sentinel-secret"))
			return
		}
		require.Equal(t, "/api/traces/"+id, r.URL.Path)
		json.NewEncoder(w).Encode(map[string]any{"data": []any{trace}})
	}))
	defer server.Close()
	config := map[string]any{"schema_version": d.ConfigSchema, "sources": map[string]any{"trace": map[string]any{"connections": map[string]any{"local": map[string]any{"settings": map[string]any{"endpoint": server.URL, "deployment": "fixture", "services": map[string]string{"proxy": "proxy", "mixture": "other"}}}}}}}
	encoded, err := json.Marshal(config)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "trace.yaml")
	require.NoError(t, os.WriteFile(path, encoded, 0o600))
	for _, format := range []string{"json", "text"} {
		out.Reset()
		errout.Reset()
		code := Execute(context.Background(), []string{"jaeger", "diagnose", "--scope", "e2e", "--flow", "write", "--mode", "snapshot", "--trace-id", id, "--config", path, "--connection", "local", "-o", format}, &out, &errout)
		require.Zero(t, code, errout.String())
		require.Contains(t, out.String(), "partial_observed_path")
		require.NotContains(t, out.String(), server.URL)
		if format == "text" {
			require.Contains(t, out.String(), "/trace/"+id)
		} else {
			var report d.Report
			require.NoError(t, json.Unmarshal(out.Bytes(), &report))
			require.Equal(t, "e2e.write.trace", report.Results[0].Profile)
		}
	}
	before := requests
	out.Reset()
	errout.Reset()
	require.Equal(t, 2, Execute(context.Background(), []string{"trace", "diagnose", "--flow", "write", "--mode", "snapshot", "--trace-id", "invalid/trace", "-o", "json"}, &out, &errout))
	require.Equal(t, before, requests)
	require.Contains(t, out.String(), "invalid_selection")
	fail = true
	out.Reset()
	errout.Reset()
	require.Equal(t, 1, Execute(context.Background(), []string{"trace", "diagnose", "--scope", "e2e", "--flow", "write", "--mode", "snapshot", "--trace-id", id, "--config", path, "--connection", "local"}, &out, &errout))
	require.Contains(t, out.String(), "http_401")
	require.NotContains(t, out.String()+errout.String(), "sentinel-secret")
}

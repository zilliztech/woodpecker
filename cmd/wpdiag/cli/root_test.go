package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func TestCLIIndependentCapabilitiesAndFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		args     []string
		exit     int
		contains string
	}{
		{"help", []string{"--help"}, 0, "capabilities"},
		{"capabilities", []string{"capabilities", "-o", "json"}, 0, d.CatalogSchema},
		{"text capabilities", []string{"capabilities"}, 0, "wp.write_stall"},
		{"query catalog", []string{"milvus", "query", "--list", "-o", "json"}, 0, "milvus.query.tsafe"},
		{"query text catalog", []string{"milvus", "query", "--list"}, 0, "milvus_querynode_sq_core_latency"},
		{"query unsupported source", []string{"milvus", "query", "--source", "trace", "-o", "json"}, 3, "stage_source_not_supported"},
		{"query wrong flow", []string{"milvus", "query", "--flow", "write", "-o", "json"}, 2, "invalid_selection"},
		{"query wrong scope", []string{"milvus", "query", "--scope", "wp", "-o", "json"}, 2, "invalid_selection"},
		{"query missing mapping filter", []string{"milvus", "query", "--collection", "7", "-o", "json"}, 3, "target_filter_not_supported"},
		{"bad catalog output", []string{"capabilities", "-o", "yaml"}, 2, "Invalid command"},
		{"bad report output", []string{"wpcli", "diagnose", "-o", "yaml"}, 2, "Invalid command"},
		{"bad timestamp", []string{"wpcli", "diagnose", "--from", "invalid"}, 2, "Invalid command"},
		{"submillisecond duration", []string{"wpcli", "diagnose", "--window", "1ns"}, 2, "Invalid command"},
		{"text unavailable source", []string{"loki", "diagnose", "--flow", "write"}, 3, "module_not_implemented"},
		{"planned source", []string{"trace", "diagnose", "--scope", "e2e", "--flow", "read", "--stage", "query", "-o", "json"}, 3, "module_not_implemented"},
		{"invalid stage", []string{"wpcli", "diagnose", "--flow", "write", "--stage", "wal_read", "-o", "json"}, 2, "invalid_selection"},
		{"historical is unsupported", []string{"wpcli", "diagnose", "--flow", "write", "--mode", "historical", "--from", "2026-10-01T00:00:00Z", "--to", "2026-10-01T01:00:00Z", "-o", "json"}, 3, "profile_not_supported"},
		{"missing config", []string{"wpcli", "diagnose", "--flow", "write", "--mode", "snapshot", "--connection", "local", "--config", filepath.Join(t.TempDir(), "missing.yaml"), "-o", "json"}, 2, "configuration_unavailable"},
		{"missing flow", []string{"wpcli", "diagnose", "--mode", "snapshot", "-o", "json"}, 2, "invalid_selection"},
		{"bad duration", []string{"wpcli", "diagnose", "--flow", "read", "--window", "0s", "-o", "json"}, 2, "invalid_selection"},
		{"unknown option", []string{"--sentinel-secret"}, 2, "Invalid command"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out, errout bytes.Buffer
			code := Execute(context.Background(), tc.args, &out, &errout)
			require.Equal(t, tc.exit, code)
			require.Contains(t, out.String()+errout.String(), tc.contains)
			require.NotContains(t, out.String()+errout.String(), "sentinel-secret")
			if json.Valid(out.Bytes()) && tc.name != "capabilities" && tc.name != "query catalog" {
				var report d.Report
				require.NoError(t, json.Unmarshal(out.Bytes(), &report))
				require.Equal(t, d.ReportSchema, report.SchemaVersion)
				require.NotEqual(t, "completed", report.Status)
			}
		})
	}
}

func TestCLIDisabledConnectionTextReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wpdiag.yaml")
	require.NoError(t, os.WriteFile(path, []byte("schema_version: wpdiag.config/v1\nsources:\n  wpcli:\n    connections:\n      local:\n        enabled: false\n        settings: {}\n"), 0o600))
	var out, errout bytes.Buffer
	code := Execute(context.Background(), []string{"wpcli", "diagnose", "--flow", "read", "--mode", "snapshot", "--connection", "local", "--config", path}, &out, &errout)
	require.Equal(t, 3, code)
	require.Contains(t, out.String(), "connection_disabled")
	require.Contains(t, out.String(), "stage=wal_read")
	require.Empty(t, errout.String())
}

func TestQueryTextAssessmentSourceErrorsAndOutputBudget(t *testing.T) {
	for _, deny := range []bool{false, true} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if deny {
				http.Error(w, "sentinel-secret", 401)
				return
			}
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
		}))
		path := filepath.Join(t.TempDir(), "wpdiag.yaml")
		require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf("schema_version: wpdiag.config/v1\nsources:\n  prometheus:\n    connections:\n      test:\n        settings:\n          endpoint: %q\n          deployment: fixture\n          match_labels: {cluster: test}\n", server.URL)), 0o600))
		args := []string{"milvus", "query", "--config", path, "--connection", "test", "--scope", "e2e", "--mode", "historical", "--from", "1970-01-01T00:16:40Z", "--to", "1970-01-01T00:17:40Z"}
		var out, errout bytes.Buffer
		code := Execute(context.Background(), args, &out, &errout)
		if deny {
			require.Equal(t, 1, code)
			require.Contains(t, out.String(), "Source gap http_401")
			require.Contains(t, out.String(), "Additional source gaps")
		} else {
			require.Zero(t, code)
			require.Contains(t, out.String(), "5 Result return")
			require.Contains(t, out.String(), "wp_visibility_mapping_unavailable")
		}
		require.NotContains(t, out.String(), "sentinel-secret")
		require.NotContains(t, out.String(), "milvus.query.core.latency node=")
		out.Reset()
		errout.Reset()
		code = Execute(context.Background(), append(args, "--max-output-bytes", "4096", "-o", "json"), &out, &errout)
		require.Equal(t, 5, code)
		require.True(t, json.Valid(out.Bytes()))
		require.LessOrEqual(t, out.Len(), 4096)
		require.NotContains(t, out.String(), "assessments")
		server.Close()
	}
}

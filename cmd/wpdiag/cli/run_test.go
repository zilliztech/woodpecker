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
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/integrated"
	"github.com/zilliztech/woodpecker/diagnostics/milvusappend"
	"github.com/zilliztech/woodpecker/diagnostics/milvusconsume"
	"github.com/zilliztech/woodpecker/diagnostics/milvuspersistence"
	"github.com/zilliztech/woodpecker/diagnostics/milvusquery"
)

func TestRunCLIPlanAndSelection(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "wpdiag.yaml")
	require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf(`schema_version: wpdiag.config/v1
sources:
  prometheus:
    connections:
      metrics:
        settings:
          endpoint: %q
          deployment: fixture
          match_labels: {cluster: fixture}
  trace:
    connections:
      logs:
        enabled: false
        settings: {}
`, server.URL)), 0o600))
	for _, tc := range []struct {
		args     []string
		code     int
		contains string
	}{
		{[]string{"--list", "-o", "json"}, 0, "wpdiag.modules/v1"},
		{[]string{"--list"}, 0, "milvus.consume"},
		{[]string{"--flow", "write", "--plan", "-o", "json"}, 0, "milvus.persistence"},
		{[]string{"--flow", "read", "--plan"}, 0, "milvus.consume"},
		{[]string{"--flow", "write", "--stages", "query", "--plan"}, 2, "invalid_stages"},
		{[]string{"--flow", "write", "--modules", "trace", "--plan"}, 3, "disabled"},
		{[]string{"--flow", "write", "--connections", "trace=logs", "--plan"}, 2, "unused_connection_override"},
		{[]string{"--flow", "write", "--target", "prometheus.unknown=invalid"}, 2, "Invalid command"},
		{[]string{"--flow", "write", "--import", "not-a-file", "-o", "json"}, 3, "report_import_not_supported"},
		{[]string{"--flow", "write", "--modules", "prometheus", "--target", "prometheus.channel=a", "--plan"}, 3, "target_filter_not_supported"},
	} {
		var out, errout bytes.Buffer
		code := Execute(context.Background(), append([]string{"run", "--config", path}, tc.args...), &out, &errout)
		require.Equal(t, tc.code, code, "%v: %s %s", tc.args, out.String(), errout.String())
		require.Contains(t, out.String()+errout.String(), tc.contains)
		require.NotContains(t, out.String(), server.URL)
	}
	require.Zero(t, calls.Load(), "Plan/catalog/unsupported requests must not query sources")
}

func TestRunCLIStageReuseAndPartialSources(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if strings.HasPrefix(r.URL.Path, "/denied/") {
			http.Error(w, "sentinel-secret", 401)
			return
		}
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "wpdiag.yaml")
	require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf(`schema_version: wpdiag.config/v1
sources:
  prometheus:
    connections:
      metrics:
        settings: {endpoint: %q, deployment: fixture, match_labels: {cluster: fixture}}
      denied:
        settings: {endpoint: %q, deployment: fixture, match_labels: {cluster: fixture}}
  trace:
    connections:
      disabled:
        enabled: false
        settings: {}
`, server.URL, server.URL+"/denied")), 0o600))
	for _, flow := range []string{"write", "read"} {
		for _, scope := range []string{"wp", "milvus", "e2e"} {
			args := []string{"run", "--config", path, "--sources", "prometheus", "--connections", "prometheus=metrics", "--scope", scope, "--flow", flow, "--mode", "historical", "--from", "1970-01-01T00:16:40Z", "--to", "1970-01-01T00:17:40Z", "--progress", "-o", "json"}
			var out, errout bytes.Buffer
			code := Execute(context.Background(), args, &out, &errout)
			require.Zero(t, code, "%s %s: %s %s", scope, flow, out.String(), errout.String())
			var report integrated.Report
			require.NoError(t, json.Unmarshal(out.Bytes(), &report))
			require.Equal(t, integrated.Schema, report.SchemaVersion)
			require.Contains(t, errout.String(), `"status":"planned"`)
			require.Contains(t, errout.String(), `"status":"completed"`)
			for _, run := range report.Runs {
				var expected d.Assessment
				switch run.ModuleID {
				case "milvus.append":
					expected = milvusappend.Assess(context.Background(), run.Report)
				case "milvus.persistence":
					expected = milvuspersistence.Assess(context.Background(), run.Report)
				case "milvus.consume":
					expected = milvusconsume.Assess(context.Background(), run.Report)
				case "milvus.query":
					expected = milvusquery.Assess(context.Background(), run.Report)
				default:
					continue
				}
				require.Equal(t, expected, run.Report.Assessments[0], "must reuse the exact standalone assessor")
			}
			for _, view := range report.Summary.Findings {
				_, ok := integrated.Resolve(report, view.Reference)
				require.True(t, ok, "summary must resolve to an original finding")
			}
			require.NotContains(t, out.String(), server.URL)
			require.NotContains(t, out.String(), "invalid_report_lineage")
			if scope == "e2e" && flow == "write" {
				out.Reset()
				errout.Reset()
				code = Execute(context.Background(), append(args, "--sources", "prometheus,trace", "--connections", "prometheus=metrics,trace=disabled"), &out, &errout)
				require.Equal(t, 6, code)
				require.NoError(t, json.Unmarshal(out.Bytes(), &report))
				require.Equal(t, "partial", report.Status)
				require.Len(t, report.Runs, 3)
				require.NotEmpty(t, report.Summary.Findings)
			}
		}
	}
	require.Positive(t, calls.Load())
	for _, tc := range []struct {
		extra    []string
		code     int
		contains string
	}{
		{[]string{"--connection", "denied"}, 1, "http_401"},
		{[]string{"--max-output-bytes", "4096"}, 5, "report_output_limit"},
	} {
		var out, errout bytes.Buffer
		args := []string{"run", "--config", path, "--modules", "milvus.append", "--connection", "metrics", "--flow", "write", "--mode", "historical", "--from", "1970-01-01T00:16:40Z", "--to", "1970-01-01T00:17:40Z", "-o", "json"}
		code := Execute(context.Background(), append(args, tc.extra...), &out, &errout)
		require.Equal(t, tc.code, code, "%s", errout.String())
		require.True(t, json.Valid(out.Bytes()))
		require.Contains(t, out.String(), tc.contains)
		require.NotContains(t, out.String()+errout.String(), "sentinel-secret")
		if tc.code == 5 {
			require.LessOrEqual(t, out.Len(), 4096)
		}
	}
	var out, errout bytes.Buffer
	code := Execute(context.Background(), []string{"run", "--config", path, "--modules", "milvus.query", "--connection", "metrics", "--flow", "read", "--mode", "historical", "--from", "1970-01-01T00:16:40Z", "--to", "1970-01-01T00:17:40Z", "--all-findings"}, &out, &errout)
	require.Zero(t, code)
	require.Contains(t, out.String(), "completion is execution status")
	require.Contains(t, out.String(), "inconclusive")
}

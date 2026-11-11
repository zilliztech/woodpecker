package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/diagnosis"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/common/http/management"
	"github.com/zilliztech/woodpecker/common/runtime/opregistry"
)

func diagnoseTestServer(t *testing.T, unavailable string) *httptest.Server {
	t.Helper()
	return healthTestServer(t, map[string]func(http.ResponseWriter, *http.Request){
		"/metrics": func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodGet, req.Method)
			_, _ = fmt.Fprint(w, `# TYPE process_start_time_seconds gauge
process_start_time_seconds 100
# TYPE woodpecker_server_logstore_operations_total counter
woodpecker_server_logstore_operations_total{node_id="node-1",log_id="2",log_ns="tenant-a",operation="add_entry",status="success"} 5
`)
		},
		"/admin/runtime/ops": func(w http.ResponseWriter, req *http.Request) {
			require.Equal(t, http.MethodGet, req.Method)
			require.Equal(t, "100", req.URL.Query().Get("limit"))
			if unavailable == "ops" {
				w.WriteHeader(503)
				_, _ = fmt.Fprint(w, "server-secret")
				return
			}
			_, _ = fmt.Fprint(w, `[{"op_id":"pending-write","op_type":"logstore.add_entry","log_id":2,"segment_id":7,"started_at":"2026-10-01T00:00:00Z"}]`)
		},
		"/admin/runtime/ops/stats": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprint(w, `{"capacity":1024,"in_use":1,"evicted_total":0}`)
		},
		"/admin/env": func(w http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprint(w, `{"env":{"SECRET":"server-secret"},"build":{"version":"v0.1.46"}}`)
		},
	})
}

func runDiagnose(t *testing.T, server *httptest.Server, ctx context.Context, extra ...string) (diagnosis.Report, string, error) {
	t.Helper()
	withCliYAML(t, server.URL)
	root := NewRootCommand()
	root.SetContext(ctx)
	out, stderr := new(bytes.Buffer), new(bytes.Buffer)
	root.SetOut(out)
	root.SetErr(stderr)
	args := []string{"diagnose", "node-1", "--scenario", "stuck-write", "--window", "20ms", "--interval", "10ms", "--max-samples", "3", "--budget", "1s", "--timeout", "1s", "--node-admin-url", "node-1=" + server.URL, "-o", "json"}
	root.SetArgs(append(args, extra...))
	err := root.Execute()
	var report diagnosis.Report
	require.NoError(t, json.Unmarshal(out.Bytes(), &report), "output=%s stderr=%s error=%v", out.String(), stderr.String(), err)
	return report, out.String(), err
}

func TestDiagnoseCommandJSONAndFindingExitContract(t *testing.T) {
	for _, fail := range []bool{false, true} {
		server := diagnoseTestServer(t, "")
		defer server.Close()
		extra := []string{}
		if fail {
			extra = append(extra, "--fail-on-findings")
		}
		r, out, err := runDiagnose(t, server, context.Background(), extra...)
		code := 0
		if fail {
			code = 8
		}
		require.Equal(t, code, wperrors.ExitCodeFor(err))
		require.Equal(t, diagnosis.SchemaVersion, r.SchemaVersion)
		require.Equal(t, "completed", r.Status)
		require.Equal(t, "prod", r.Context)
		require.Equal(t, "suspected_stall", r.Findings[0].Outcome)
		require.Equal(t, "pending-write", r.Findings[0].Scope.Operation)
		require.Contains(t, r.Findings[0].Unknown, "tenant_association_unknown")
		require.NotContains(t, out, server.URL)
		require.NotContains(t, out, "server-secret")
	}
}

func TestDiagnoseSnapshotCannotClaimStall(t *testing.T) {
	server := diagnoseTestServer(t, "")
	defer server.Close()
	r, _, err := runDiagnose(t, server, context.Background(), "--mode", "snapshot", "--fail-on-findings")
	require.NoError(t, err)
	require.Equal(t, "snapshot", r.Mode)
	require.Zero(t, r.WindowMS)
	require.Len(t, r.Observations, 4)
	require.Equal(t, "inconclusive", r.Findings[0].Outcome)
}

func TestDiagnoseRetainsSourceFailuresAndCancellation(t *testing.T) {
	server := diagnoseTestServer(t, "ops")
	defer server.Close()
	r, out, err := runDiagnose(t, server, context.Background())
	require.Equal(t, 6, wperrors.ExitCodeFor(err))
	require.Equal(t, "partial", r.Status)
	require.Equal(t, "inconclusive", r.Findings[0].Outcome)
	require.NotContains(t, out, "server-secret")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, _, err = runDiagnose(t, server, ctx)
	require.Equal(t, 7, wperrors.ExitCodeFor(err))
	require.Equal(t, "cancelled", r.Status)
	require.Empty(t, r.Observations)
}

func TestDiagnoseDiscoverySharesWallClockBudget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) { <-req.Context().Done() }))
	defer server.Close()
	started := time.Now()
	r, out, err := runDiagnose(t, server, context.Background(), "--budget", "15ms")
	require.Equal(t, 5, wperrors.ExitCodeFor(err))
	require.Equal(t, "budget_exceeded", r.Status)
	require.NotContains(t, out, server.URL)
	require.Less(t, time.Since(started), time.Second)
}

func TestDiagnoseValidationAndCatalogNeedNoNetwork(t *testing.T) {
	for _, args := range [][]string{
		{"--mode", "historical"},
		{"--scenario", "slow-read"},
		{"--window", "0s"},
		{"--interval", "0s"},
		{"--window", "10m"},
		{"--max-nodes", "0"},
		{"--max-response-bytes", "0"},
		{"--budget", "0s"},
		{"--scope", "milvus"},
		{"--from", "2026-10-25T00:00:00Z"},
	} {
		root := NewRootCommand()
		root.SetOut(new(bytes.Buffer))
		root.SetErr(new(bytes.Buffer))
		root.SetArgs(append([]string{"diagnose"}, args...))
		require.Equal(t, 2, wperrors.ExitCodeFor(root.Execute()), "%v", args)
	}
	for _, format := range []string{"json", "yaml", "table"} {
		root := NewRootCommand()
		out := new(bytes.Buffer)
		root.SetOut(out)
		root.SetErr(new(bytes.Buffer))
		root.SetArgs([]string{"diagnose", "--list", "-o", format})
		require.NoError(t, root.Execute())
		require.Contains(t, out.String(), "write_stall")
		if format != "table" {
			var value map[string]any
			var err error
			if format == "json" {
				err = json.Unmarshal(out.Bytes(), &value)
			} else {
				err = yaml.Unmarshal(out.Bytes(), &value)
			}
			require.NoError(t, err)
			require.Equal(t, diagnosis.SchemaVersion, value["schema_version"])
		}
	}
}

func TestDiagnoseYAMLAndHumanOutput(t *testing.T) {
	for _, format := range []string{"yaml", "table"} {
		server := diagnoseTestServer(t, "")
		defer server.Close()
		withCliYAML(t, server.URL)
		root := NewRootCommand()
		out := new(bytes.Buffer)
		root.SetOut(out)
		root.SetErr(new(bytes.Buffer))
		root.SetArgs([]string{"diagnose", "node-1", "--scenario", "stuck-write", "--mode", "snapshot", "--node-admin-url", "node-1=" + server.URL, "-o", format})
		require.NoError(t, root.Execute())
		if format == "yaml" {
			var report map[string]any
			require.NoError(t, yaml.Unmarshal(out.Bytes(), &report))
			require.Equal(t, diagnosis.SchemaVersion, report["schema_version"])
			require.Equal(t, "snapshot", report["observation_mode"])
		} else {
			require.Contains(t, out.String(), "not a health verdict")
			require.Contains(t, out.String(), "inconclusive")
		}
		require.False(t, strings.Contains(out.String(), "server-secret"))
	}
}

func TestDiagnoseTargetsAndMapping(t *testing.T) {
	c := clientForDiagnoseTest()
	members := &client.Memberlist{Members: []client.Member{{ID: "node-1", ServiceAddr: "internal:8080"}, {ID: "node-2", ServiceAddr: "other:8080"}}}
	_, err := diagnoseNodes(c, members, nil, 1)
	require.Equal(t, 2, wperrors.ExitCodeFor(err))
	nodes, err := diagnoseNodes(c, members, []string{"node-1"}, 1)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "http://127.0.0.1:19091", nodes[0].URL)
	_, err = diagnoseNodes(c, members, []string{"missing"}, 1)
	require.Equal(t, 3, wperrors.ExitCodeFor(err))
	_, err = diagnoseNodes(c, &client.Memberlist{}, nil, 1)
	require.Equal(t, 1, wperrors.ExitCodeFor(err))
	_, err = diagnoseNodes(c, &client.Memberlist{Members: []client.Member{{ID: "node-1"}, {ID: "node-1"}}}, nil, 2)
	require.Equal(t, 1, wperrors.ExitCodeFor(err))
}

func clientForDiagnoseTest() *client.Client {
	return client.New("http://127.0.0.1:19091", client.ClientOpts{NodeAdminURLs: map[string]string{"node-1": "http://127.0.0.1:19091"}})
}

func TestDiagnoseAgainstExistingAdminHandlersAndRegistry(t *testing.T) {
	registry := opregistry.New(1024, time.Second)
	handle := registry.Register(opregistry.OpRecord{
		OpID: "live-pending", OpType: opregistry.OpTypeLogstoreAddEntry,
		LogID: 2, SegmentID: 7, StartedAt: time.Now().Add(-time.Minute),
	})
	counters := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "woodpecker_server_logstore_operations_total", Help: "Test completions"}, []string{"node_id", "log_id", "log_ns", "operation", "status"})
	counters.WithLabelValues("node-1", "2", "tenant-a", "add_entry", "success").Add(0)
	process := prometheus.NewGauge(prometheus.GaugeOpts{Name: "process_start_time_seconds", Help: "Test process"})
	process.Set(float64(time.Now().Add(-time.Hour).Unix()))
	metrics := prometheus.NewRegistry()
	metrics.MustRegister(counters, process)
	server := healthTestServer(t, map[string]func(http.ResponseWriter, *http.Request){
		"/metrics": promhttp.HandlerFor(metrics, promhttp.HandlerOpts{}).ServeHTTP,
		"/admin/runtime/ops": management.NewOpsListHandler(func(params map[string]string) any {
			limit, _ := strconv.Atoi(params["limit"])
			return registry.List(opregistry.Filter{Limit: limit})
		}),
		"/admin/runtime/ops/stats": management.NewOpsStatsHandler(func() any { return registry.Stats() }),
		"/admin/env":               management.NewEnvHandler(),
	})
	defer server.Close()
	r, _, err := runDiagnose(t, server, context.Background())
	require.NoError(t, err)
	require.Equal(t, "suspected_stall", r.Findings[0].Outcome)
	require.Equal(t, "live-pending", r.Findings[0].Scope.Operation)
	registry.Deregister(handle)
	counters.WithLabelValues("node-1", "2", "tenant-a", "add_entry", "success").Inc()
	r, _, err = runDiagnose(t, server, context.Background())
	require.NoError(t, err)
	require.Equal(t, "no_pending_work_observed", r.Findings[0].Outcome)
}

func TestDiagnoseYAMLPreservesLargeLogIDs(t *testing.T) {
	root := NewRootCommand()
	out := new(bytes.Buffer)
	root.SetOut(out)
	large := int64(469986968496707423)
	r := diagnosis.Report{SchemaVersion: diagnosis.SchemaVersion, Findings: []diagnosis.Finding{{Scope: diagnosis.Scope{LogID: &large}}}}
	require.NoError(t, renderDiagnosisYAML(root, r))
	var value map[string]any
	require.NoError(t, yaml.Unmarshal(out.Bytes(), &value))
	ids := value["findings"].([]any)[0].(map[string]any)["target"].(map[string]any)
	require.EqualValues(t, large, ids["log_id"])
}

func TestDiagnoseHumanOutputNamesUnavailableSource(t *testing.T) {
	server := diagnoseTestServer(t, "ops")
	defer server.Close()
	withCliYAML(t, server.URL)
	root := NewRootCommand()
	out := new(bytes.Buffer)
	root.SetOut(out)
	root.SetErr(new(bytes.Buffer))
	root.SetArgs([]string{"diagnose", "node-1", "--mode", "snapshot", "--node-admin-url", "node-1=" + server.URL})
	require.Equal(t, 6, wperrors.ExitCodeFor(root.Execute()))
	require.Contains(t, out.String(), "http_503")
	require.Contains(t, out.String(), "ops evidence")
	require.NotContains(t, out.String(), "server-secret")
}

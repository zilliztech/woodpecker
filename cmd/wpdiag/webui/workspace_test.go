package webui

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/zilliztech/woodpecker/diagnostics/checks"
	diagcommand "github.com/zilliztech/woodpecker/diagnostics/command"
)

func workspaceFixture(t *testing.T) (*Server, *httptest.Server, string, *atomic.Int32) {
	t.Helper()
	t.Setenv("WOODPECKER_RUN_CACHE_DIR", t.TempDir())
	calls := &atomic.Int32{}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/admin/memberlist" {
			fmt.Fprint(w, `{"members":[{"id":"node-a","state":0,"service_addr":"node-a:18080"}]}`)
		} else {
			fmt.Fprint(w, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
		}
	}))
	t.Cleanup(backend.Close)
	root := map[string]any{"current-context": "a", "contexts": map[string]any{"a": map[string]any{"endpoint": backend.URL, "diagnostic_target": "a"}, "b": map[string]any{"endpoint": backend.URL, "diagnostic_target": "b"}}, "agent": map[string]any{"provider": "not-installed", "enabled": true}, "diagnostics": map[string]any{"sources": map[string]any{"metrics": map[string]any{"connections": map[string]any{"local": map[string]any{"settings": map[string]any{"endpoint": backend.URL, "deployment": "fixture", "match_labels": map[string]string{"namespace": "fixture"}}}}}}, "targets": map[string]any{}}}
	targets := root["diagnostics"].(map[string]any)["targets"].(map[string]any)
	for _, name := range []string{"a", "b"} {
		targets[name] = map[string]any{"context": name, "metrics": "local", "label": "Target " + name, "log_id": "1", "node_id": "n", "log_name": "log-" + name, "metadata": map[string]string{"etcd": "http://client-etcd.invalid:2379", "prefix": "client-" + name + "/woodpecker"}}
	}
	for _, value := range root["contexts"].(map[string]any) {
		value.(map[string]any)["observability"] = map[string]string{"prometheus": "local"}
	}
	body, err := yaml.Marshal(root)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "cli.yaml")
	require.NoError(t, os.WriteFile(path, body, 0o600))
	cfg, _, err := diagcommand.Load(path, "")
	require.NoError(t, err)
	app := New(context.Background(), cfg, nil)
	require.NoError(t, app.setupWorkspace(path, ""))
	t.Cleanup(app.Close)
	srv := httptest.NewServer(app)
	t.Cleanup(srv.Close)
	return app, srv, path, calls
}

func browser(t *testing.T, base string) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	c := &http.Client{Jar: jar}
	res, err := c.Get(base)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, 200, res.StatusCode)
	body, _ := io.ReadAll(res.Body)
	require.Contains(t, string(body), "Operations workspace")
	return c
}

func wsCall(t *testing.T, c *http.Client, base, token, method, path string, value any) (int, []byte) {
	t.Helper()
	var body []byte
	var err error
	if value != nil {
		body, err = json.Marshal(value)
		require.NoError(t, err)
	}
	req, err := http.NewRequest(method, base+path, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Origin", base)
	if method == "POST" {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-WPDiag-Session", token)
	}
	res, err := c.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	return res.StatusCode, data
}

func TestWorkspaceSessionsSeparateContextsMetadataAndRunOwnership(t *testing.T) {
	app, srv, path, _ := workspaceFixture(t)
	a, b := browser(t, srv.URL), browser(t, srv.URL)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	code, data := wsCall(t, a, srv.URL, app.token, "POST", "/api/workspace/session", map[string]string{"action": "connect-meta", "etcd": "http://client-etcd.invalid:2379", "prefix": "client-a/woodpecker", "log_name": "log-a"})
	require.Equal(t, 200, code, string(data))
	require.Contains(t, string(data), "client-a/woodpecker")
	code, data = wsCall(t, b, srv.URL, app.token, "GET", "/api/workspace", nil)
	require.Equal(t, 200, code)
	var detached struct {
		Session browserSession `json:"session"`
	}
	require.NoError(t, json.Unmarshal(data, &detached))
	require.Nil(t, detached.Session.Metadata)
	code, data = wsCall(t, a, srv.URL, app.token, "POST", "/api/workspace/session", map[string]string{"action": "use-context", "context": "b"})
	require.Equal(t, 200, code, string(data))
	require.Contains(t, string(data), `"metadata":`)
	var changed struct {
		Session browserSession `json:"session"`
	}
	require.NoError(t, json.Unmarshal(data, &changed))
	require.Empty(t, changed.Session.Target)
	code, data = wsCall(t, b, srv.URL, app.token, "GET", "/api/workspace", nil)
	require.Equal(t, 200, code)
	var info struct {
		Session browserSession `json:"session"`
	}
	require.NoError(t, json.Unmarshal(data, &info))
	require.Equal(t, "a", info.Session.Context)
	q := checks.Request{Source: "metrics", Check: "writer-throughput", Bindings: &checks.Bindings{Client: &checks.Scope{Metrics: app.workspace.file.Contexts["b"].Observability.MetricsReference()}}, From: time.Now().Add(-5 * time.Minute), To: time.Now().Add(-time.Second), Details: "all"}
	code, data = wsCall(t, a, srv.URL, app.token, "POST", "/api/diag/runs", q)
	require.Equal(t, 202, code, string(data))
	var job struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(data, &job))
	require.Eventually(t, func() bool {
		code, data = wsCall(t, a, srv.URL, app.token, "GET", "/api/runs/"+job.ID, nil)
		var view jobView
		return code == 200 && json.Unmarshal(data, &view) == nil && view.ReportReady
	}, 2*time.Second, 10*time.Millisecond)
	for _, suffix := range []string{"", "/report", "/cancel"} {
		method := "GET"
		var body any
		if suffix == "/cancel" {
			method = "POST"
			body = struct{}{}
		}
		code, _ = wsCall(t, b, srv.URL, app.token, method, "/api/runs/"+job.ID+suffix, body)
		require.Equal(t, 404, code)
	}
	code, data = wsCall(t, b, srv.URL, app.token, "GET", "/api/runs", nil)
	require.Equal(t, 200, code)
	require.NotContains(t, string(data), job.ID)
	code, data = wsCall(t, a, srv.URL, app.token, "GET", "/api/diag/runs/"+job.ID+"/inspect/W03", nil)
	require.Equal(t, 200, code, string(data))
	require.Contains(t, string(data), "No data")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestWorkspaceUsesSameResultContractAndReadOnlyOperations(t *testing.T) {
	app, srv, _, calls := workspaceFixture(t)
	browser := browser(t, srv.URL)
	code, data := wsCall(t, browser, srv.URL, app.token, "GET", "/api/diag/catalog", nil)
	require.Equal(t, 200, code, string(data))
	var catalog checks.Catalog
	require.NoError(t, json.Unmarshal(data, &catalog))
	require.Len(t, catalog.Checks, 55)
	// Invalid optional Agent configuration does not disable checks or Ops.
	code, data = wsCall(t, browser, srv.URL, app.token, "GET", "/api/workspace", nil)
	require.Equal(t, 200, code)
	require.Contains(t, string(data), "agent_configuration_invalid")
	before := calls.Load()
	code, data = wsCall(t, browser, srv.URL, app.token, "GET", "/api/ops/cluster", nil)
	require.Equal(t, 200, code, string(data))
	require.Contains(t, string(data), "node-a")
	require.Equal(t, before+1, calls.Load())
	code, _ = wsCall(t, browser, srv.URL, app.token, "GET", "/api/ops/metadata/M01", nil)
	require.Equal(t, 422, code)
	code, _ = wsCall(t, browser, srv.URL, app.token, "POST", "/api/workspace/session", map[string]string{"action": "repair"})
	require.Equal(t, 400, code)
	code, _ = wsCall(t, browser, srv.URL, app.token, "POST", "/api/runs", map[string]string{})
	require.Equal(t, 404, code, "Legacy composite runner is not exposed through the new workspace")
}

func TestWorkspaceDiagnosticTargetIsIndependentOfAdminContext(t *testing.T) {
	app, srv, _, _ := workspaceFixture(t)
	c := browser(t, srv.URL)
	code, _ := wsCall(t, c, srv.URL, app.token, "POST", "/api/workspace/session", map[string]string{"action": "use-target", "target": "b"})
	require.Equal(t, 422, code)
	q := checks.Request{Scenario: "all", Bindings: &checks.Bindings{Client: &checks.Scope{Metrics: app.workspace.file.Contexts["b"].Observability.MetricsReference()}}, From: time.Now().Add(-5 * time.Minute), To: time.Now().Add(-time.Second)}
	code, _ = wsCall(t, c, srv.URL, app.token, "POST", "/api/diag/runs", q)
	require.Equal(t, 202, code)
}

func TestWorkspaceTelemetryWithoutServiceContextAndMetadataWithoutLog(t *testing.T) {
	app, srv, _, _ := workspaceFixture(t)
	app.workspace.file.Contexts = nil
	app.workspace.file.CurrentContext = ""
	c := browser(t, srv.URL)
	code, data := wsCall(t, c, srv.URL, app.token, "GET", "/api/workspace", nil)
	require.Equal(t, 200, code, string(data))
	var info struct {
		Session browserSession `json:"session"`
		Targets []any          `json:"targets"`
	}
	require.NoError(t, json.Unmarshal(data, &info))
	require.Empty(t, info.Session.Context)
	require.Empty(t, info.Targets)
	code, _ = wsCall(t, c, srv.URL, app.token, "GET", "/api/ops/cluster", nil)
	require.Equal(t, 422, code)
	code, data = wsCall(t, c, srv.URL, app.token, "POST", "/api/workspace/session", map[string]string{"action": "connect-meta", "etcd": "http://client-etcd.invalid:2379", "prefix": "client/woodpecker"})
	require.Equal(t, 200, code, string(data))
	code, data = wsCall(t, c, srv.URL, app.token, "GET", "/api/ops/metadata/M01", nil)
	require.Equal(t, 422, code, string(data))
	require.Contains(t, string(data), "log_name_required")
	code, data = wsCall(t, c, srv.URL, app.token, "POST", "/api/diag/runs", checks.Request{Source: "metrics", Check: "writer-throughput", Bindings: &checks.Bindings{Client: &checks.Scope{Metrics: app.workspace.file.Contexts["b"].Observability.MetricsReference()}}, From: time.Now().Add(-5 * time.Minute), To: time.Now().Add(-time.Second)})
	require.Equal(t, 202, code, string(data))
}

func TestWorkspaceBrokenTelemetryDoesNotDisableAdminOrExplicitMetadata(t *testing.T) {
	app, srv, _, _ := workspaceFixture(t)
	require.NoError(t, app.workspace.file.Diagnostics.Encode(map[string]any{"unknown_field": true}))
	c := browser(t, srv.URL)
	code, data := wsCall(t, c, srv.URL, app.token, "GET", "/api/workspace", nil)
	require.Equal(t, 200, code, string(data))
	require.Contains(t, string(data), "diagnostics_error")
	code, data = wsCall(t, c, srv.URL, app.token, "GET", "/api/ops/cluster", nil)
	require.Equal(t, 200, code, string(data))
	code, data = wsCall(t, c, srv.URL, app.token, "POST", "/api/workspace/session", map[string]string{"action": "connect-meta", "etcd": "http://client-etcd.invalid:2379", "prefix": "other-client/woodpecker"})
	require.Equal(t, 200, code, string(data))
	code, data = wsCall(t, c, srv.URL, app.token, "GET", "/api/diag/catalog", nil)
	require.Equal(t, 200, code, string(data))
}

func TestWorkspaceDeduplicatesMetadataAuthoritiesAcrossDiagnosticTargets(t *testing.T) {
	app, srv, _, _ := workspaceFixture(t)
	var raw map[string]any
	require.NoError(t, app.workspace.file.Diagnostics.Decode(&raw))
	targets := raw["targets"].(map[string]any)
	targets["another-replica"] = targets["a"]
	require.NoError(t, app.workspace.file.Diagnostics.Encode(raw))
	c := browser(t, srv.URL)
	code, data := wsCall(t, c, srv.URL, app.token, "GET", "/api/workspace", nil)
	require.Equal(t, 200, code, string(data))
	var info struct {
		Targets     []any             `json:"targets"`
		Authorities []metadataSession `json:"metadata_authorities"`
	}
	require.NoError(t, json.Unmarshal(data, &info))
	require.Len(t, info.Targets, 2, "only environment source populations, never legacy tenant targets")
	require.Empty(t, info.Authorities)
}

func TestContextDefaultsExposeConnectionsWithoutInferringMetadataLabels(t *testing.T) {
	app, _, path, _ := workspaceFixture(t)
	f := app.workspace.file
	ctx := f.Contexts["a"]
	ctx.DiagnosticTarget = "a"
	ctx.Etcd.Endpoints = []string{"http://context-etcd:2379"}
	ctx.Etcd.MetaPrefix = "operations/client/wp"
	ctx.Etcd.Password = "private-default-password"
	f.Contexts["a"] = ctx
	cfg, err := diagcommand.DiagnosticsConfig(f, path)
	require.NoError(t, err)
	settings, err := checks.LoadSettings(cfg)
	require.NoError(t, err)
	for id, v := range settings.Targets {
		v.NodeID = ""
		settings.Targets[id] = v
	}
	group := cfg.Sources["prometheus"]
	ref := f.Contexts["a"].Observability.MetricsReference()
	conn := group.Connections[ref]
	var source map[string]any
	require.NoError(t, conn.Settings.Decode(&source))
	source["match_labels"] = map[string]string{"namespace": "fixture", "node_id": "metric-node"}
	require.NoError(t, conn.Settings.Encode(source))
	group.Connections[ref] = conn
	cfg.Sources["prometheus"] = group
	values := contextConnectionDefaults(settings, f, cfg)
	body, err := json.Marshal(values)
	require.NoError(t, err)
	require.NotContains(t, string(body), "private-default-password")
	defaults := values["a"].(map[string]any)
	client := defaults["client"].(checks.Scope)
	server := defaults["server"].(checks.Scope)
	require.Empty(t, client.Metadata.Etcd)
	require.Empty(t, client.Metadata.Prefix)
	require.Empty(t, client.LogNamespace)
	require.Empty(t, server.NodeID)
}

func TestPublicServerAndClientProfilesKeepCredentialsSeparate(t *testing.T) {
	app, _, _, _ := workspaceFixture(t)
	file := app.workspace.file
	ctx := file.Contexts["a"]
	ctx.Observability.Grafana = "http://grafana:3000"
	ctx.NodeAdminURLs = map[string]string{"node-x": "http://node-x:19091"}
	visible := publicServerContext(ctx)
	require.Equal(t, "http://grafana:3000", visible["grafana"])
	require.Equal(t, "http://node-x:19091", visible["node_admin_urls"].(map[string]string)["node-x"])
	profile, err := file.ResolveContext("a")
	require.NoError(t, err)
	profile.Etcd.Endpoints = []string{"http://tenant-etcd:2379"}
	profile.Etcd.MetaPrefix = "tenant/wp"
	profile.Etcd.Password = "private-tenant-password"
	file.Contexts["a"] = profile
	visibleClients := publicClientMetas(file)
	require.Empty(t, visibleClients)
	body, err := json.Marshal(visibleClients)
	require.NoError(t, err)
	require.NotContains(t, string(body), "private-tenant-password")
}

func TestWorkspaceKeepsSharedAssetsAndRetiresLegacyUI(t *testing.T) {
	app, srv, _, _ := workspaceFixture(t)
	c := browser(t, srv.URL)
	for _, path := range []string{"/wal.css", "/workflow.css", "/workspace.js", "/curves.js", "/commands.json"} {
		code, body := wsCall(t, c, srv.URL, app.token, "GET", path, nil)
		require.Equal(t, 200, code, path)
		require.NotEmpty(t, body, path)
	}
	for _, path := range []string{"/advanced", "/advanced/", "/wal.js", "/api/wal/catalog"} {
		code, _ := wsCall(t, c, srv.URL, app.token, "GET", path, nil)
		require.Equal(t, 404, code, path)
	}
}

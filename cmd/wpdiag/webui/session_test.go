package webui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	d "github.com/zilliztech/woodpecker/diagnostics"
)

func sessionWorkspaceFixture(t *testing.T) (*Server, *httptest.Server, string) {
	t.Helper()
	t.Setenv("WOODPECKER_RUN_CACHE_DIR", t.TempDir())
	path := filepath.Join(t.TempDir(), "session.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`service_context:
  endpoint: http://service:9091
  observability:
    prometheus:
      endpoint: http://metrics:9090
client_meta:
  endpoints: [http://etcd:2379]
  meta_prefix: tenant/wp
  username: alice
  password: private-session-secret
client_storage:
  bucket: tenant-bucket
  root_path: tenant/root
`), 0o600))
	app := New(context.Background(), d.Config{}, nil)
	require.NoError(t, app.setupWorkspaceWithSession("", "", path))
	app.workspace.file.Contexts["other"] = config.Context{Endpoint: "http://other:9091"}
	t.Cleanup(app.Close)
	server := httptest.NewServer(app)
	t.Cleanup(server.Close)
	return app, server, path
}

func TestWorkspaceSessionConfigurationIsolationAndPreview(t *testing.T) {
	app, server, path := sessionWorkspaceFixture(t)
	a, b := browser(t, server.URL), browser(t, server.URL)
	for _, endpoint := range []string{"/api/workspace", "/api/workspace/config"} {
		code, body := wsCall(t, a, server.URL, app.token, "GET", endpoint, nil)
		require.Equal(t, 200, code, string(body))
		require.NotContains(t, string(body), "private-session-secret")
		require.NotContains(t, string(body), "alice")
	}
	code, body := wsCall(t, a, server.URL, app.token, "POST", "/api/workspace/session", map[string]string{"action": "disconnect-meta"})
	require.Equal(t, 200, code, string(body))
	var state struct {
		Session browserSession        `json:"session"`
		Storage *config.StorageConfig `json:"client_storage"`
	}
	require.NoError(t, json.Unmarshal(body, &state))
	require.Nil(t, state.Session.Metadata)
	require.Nil(t, state.Storage)
	code, body = wsCall(t, b, server.URL, app.token, "GET", "/api/workspace", nil)
	require.Equal(t, 200, code)
	require.NoError(t, json.Unmarshal(body, &state))
	require.Equal(t, "tenant/wp", state.Session.Metadata.Prefix)
	code, body = wsCall(t, b, server.URL, app.token, "POST", "/api/workspace/session", map[string]string{"action": "use-context", "context": "other"})
	require.Equal(t, 200, code, string(body))
	state.Session.Metadata = nil
	require.NoError(t, json.Unmarshal(body, &state))
	require.Nil(t, state.Session.Metadata)
	for _, v := range app.workspace.sessions {
		require.Nil(t, v.Configuration.ClientMeta)
		require.Nil(t, v.Configuration.ClientStorage)
	}
	original, err := config.LoadSession(path)
	require.NoError(t, err)
	require.Equal(t, "private-session-secret", original.ClientMeta.Password)
}

func TestWorkspaceSessionStartupValidationAndContextSelection(t *testing.T) {
	t.Setenv("WOODPECKER_RUN_CACHE_DIR", t.TempDir())
	dir := t.TempDir()
	saved := filepath.Join(dir, "cli.yaml")
	require.NoError(t, os.WriteFile(saved, []byte("current-context: saved\ncontexts:\n  saved:\n    endpoint: http://saved:9091\n"), 0o600))
	t.Setenv("WOODPECKER_CLI_CONFIG", saved)
	for _, tc := range []struct {
		name, body, configPath, context, endpoint string
		metadata, invalid                         bool
	}{
		{name: "service only", body: `{"service_context":{"endpoint":"http://session:9091"}}`, endpoint: "http://session:9091"},
		{name: "metadata only", body: `{"client_meta":{"endpoints":["http://etcd:2379"],"meta_prefix":"tenant/wp"}}`, metadata: true},
		{name: "explicit saved context", body: `{"service_context":{"endpoint":"http://session:9091"},"client_meta":{"endpoints":["http://etcd:2379"],"meta_prefix":"tenant/wp"}}`, context: "saved", endpoint: "http://saved:9091", metadata: true},
		{name: "explicit saved file", body: `{"service_context":{"endpoint":"http://session:9091"}}`, configPath: saved, endpoint: "http://saved:9091"},
		{name: "malformed", body: "client_meta: [private-value", invalid: true},
		{name: "partial storage", body: `{"client_storage":{"bucket":"private-value"}}`, invalid: true},
		{name: "missing metadata endpoints", body: `{"client_meta":{"meta_prefix":"tenant/wp"}}`, invalid: true},
		{name: "root-only prefix", body: `{"client_meta":{"endpoints":["http://etcd:2379"],"meta_prefix":"///"}}`, invalid: true},
		{name: "password without username", body: `{"client_meta":{"endpoints":["http://etcd:2379"],"meta_prefix":"tenant/wp","password":"private-value"}}`, invalid: true},
		{name: "unpaired certificate", body: `{"client_meta":{"endpoints":["http://etcd:2379"],"meta_prefix":"tenant/wp","cert":"private-value"}}`, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "session.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.body), 0o600))
			app := New(context.Background(), d.Config{}, nil)
			t.Cleanup(app.Close)
			err := app.setupWorkspaceWithSession(tc.configPath, tc.context, path)
			if tc.invalid {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "private-value")
				return
			}
			require.NoError(t, err)
			server := httptest.NewServer(app)
			t.Cleanup(server.Close)
			user := browser(t, server.URL)
			code, body := wsCall(t, user, server.URL, app.token, "GET", "/api/workspace", nil)
			require.Equal(t, 200, code, string(body))
			var state struct{ Session browserSession }
			require.NoError(t, json.Unmarshal(body, &state))
			require.Equal(t, tc.metadata, state.Session.Metadata != nil)
			service, err := app.workspace.file.ResolveContext(state.Session.Context)
			require.NoError(t, err)
			require.Equal(t, tc.endpoint, service.Endpoint)
		})
	}
}

func TestWorkspaceSessionReconnectAndSourceProtection(t *testing.T) {
	app, server, source := sessionWorkspaceFixture(t)
	user := browser(t, server.URL)
	before, err := os.ReadFile(source)
	require.NoError(t, err)
	code, body := wsCall(t, user, server.URL, app.token, "POST", "/api/ops/commands", commandRequest{Name: "ctx use", Arguments: []string{"other"}})
	require.Equal(t, 422, code, string(body))
	code, body = wsCall(t, user, server.URL, app.token, "POST", "/api/workspace/session", map[string]string{
		"action": "connect-meta", "etcd": "http://new:2379", "prefix": "new/wp", "bucket": "new-bucket", "root_path": "new-root",
	})
	require.Equal(t, 200, code, string(body))
	for _, session := range app.workspace.sessions {
		require.Equal(t, "new/wp", session.Configuration.ClientMeta.MetaPrefix)
		require.Empty(t, session.Configuration.ClientMeta.Password)
		require.Equal(t, &config.StorageConfig{Bucket: "new-bucket", RootPath: "new-root"}, session.Configuration.ClientStorage)
	}
	code, body = wsCall(t, user, server.URL, app.token, "POST", "/api/workspace/session", map[string]string{
		"action": "connect-meta", "etcd": "http://wrong:2379", "prefix": "wrong/wp", "bucket": "partial",
	})
	require.Equal(t, 422, code, string(body))
	for _, session := range app.workspace.sessions {
		require.Equal(t, "new/wp", session.Configuration.ClientMeta.MetaPrefix, "invalid connection must retain the current selection")
	}
	after, err := os.ReadFile(source)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestWorkspaceSessionSnapshotFailureDoesNotStartCommand(t *testing.T) {
	app, server, source := sessionWorkspaceFixture(t)
	user := browser(t, server.URL)
	require.NoError(t, os.Rename(filepath.Dir(source), filepath.Join(t.TempDir(), "moved")))
	code, body := wsCall(t, user, server.URL, app.token, "POST", "/api/ops/commands", commandRequest{Name: "meta log list"})
	require.Equal(t, 202, code, string(body))
	var view jobView
	require.NoError(t, json.Unmarshal(body, &view))
	result := awaitCommand(t, app, view.ID)
	require.Equal(t, "failed", result.Status)
	require.Equal(t, -1, *result.ExitCode)
	require.Empty(t, result.Stdout)
	require.Contains(t, result.Stderr, "Cannot create private command configuration")
	require.NotContains(t, result.Stderr, "private-session-secret")
}

func TestWorkspaceSessionChild(t *testing.T) {
	if os.Getenv("WP_WORKSPACE_SESSION_HELPER") != "1" {
		return
	}
	path := os.Getenv(config.SessionConfigEnv)
	session, err := config.LoadSession(path)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	info, err := os.Stat(path)
	if err != nil {
		os.Exit(3)
	}
	result := map[string]any{"path": path, "private": info.Mode().Perm() == 0o600, "args": os.Args}
	if session.ClientMeta != nil {
		result["prefix"] = session.ClientMeta.MetaPrefix
		result["authenticated"] = session.ClientMeta.Username == "alice" && session.ClientMeta.Password == "private-session-secret"
	}
	if session.ClientStorage != nil {
		result["bucket"] = session.ClientStorage.Bucket
	}
	_ = json.NewEncoder(os.Stdout).Encode(result)
	os.Exit(0)
}

func TestWorkspaceSessionCommandsUsePrivateSnapshots(t *testing.T) {
	app, server, source := sessionWorkspaceFixture(t)
	t.Setenv("WP_WORKSPACE_SESSION_HELPER", "1")
	t.Setenv(config.SessionConfigEnv, "/ambient/should-not-be-used.yaml")
	executable := filepath.Join(t.TempDir(), "wp")
	quoted := "'" + strings.ReplaceAll(os.Args[0], "'", "'\"'\"'") + "'"
	require.NoError(t, os.WriteFile(executable, []byte("#!/bin/sh\nexec "+quoted+" -test.run=^TestWorkspaceSessionChild$ -- \"$@\"\n"), 0o700))
	app.workspace.executable = executable
	user := browser(t, server.URL)
	// Changing the source after startup must not change a running browser's target.
	require.NoError(t, os.WriteFile(source, []byte("client_meta: {endpoints: [http://wrong:2379], meta_prefix: wrong/wp}"), 0o600))
	for _, tc := range []struct {
		name    string
		request commandRequest
		bound   bool
	}{
		{"metadata", commandRequest{Name: "meta log list"}, true},
		{"physical", commandRequest{Name: "segment inspect", Arguments: []string{"log", "0"}}, true},
		{"explicit target", commandRequest{Name: "meta log list", Metadata: &metadataSession{Etcd: "http://other:2379", Prefix: "other/wp"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := wsCall(t, user, server.URL, app.token, "POST", "/api/ops/commands", tc.request)
			require.Equal(t, 202, code, string(body))
			require.NotContains(t, string(body), "private-session-secret")
			var view jobView
			require.NoError(t, json.Unmarshal(body, &view))
			result := awaitCommand(t, app, view.ID)
			require.Equal(t, "completed", result.Status, result.Stderr)
			var output struct {
				Path          string
				Private       bool
				Prefix        string
				Bucket        string
				Authenticated bool
				Args          []string
			}
			require.NoError(t, json.Unmarshal([]byte(result.Stdout), &output))
			require.True(t, output.Private)
			require.Equal(t, tc.bound, output.Authenticated)
			if tc.bound {
				require.Equal(t, "tenant/wp", output.Prefix)
				require.Equal(t, "tenant-bucket", output.Bucket)
				require.NotContains(t, strings.Join(output.Args, " "), "--etcd=")
			} else {
				require.Empty(t, output.Prefix)
				require.Empty(t, output.Bucket)
			}
			require.NotContains(t, result.Stdout, "private-session-secret")
			_, err := os.Stat(output.Path)
			require.True(t, os.IsNotExist(err), "child configuration must be removed")
		})
	}
	code, body := wsCall(t, user, server.URL, app.token, "POST", "/api/workspace/session", map[string]string{"action": "disconnect-meta"})
	require.Equal(t, 200, code, string(body))
	code, body = wsCall(t, user, server.URL, app.token, "POST", "/api/ops/commands", commandRequest{Name: "meta log list", UseMetadata: true})
	require.Equal(t, 422, code, string(body))
}

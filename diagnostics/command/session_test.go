package command

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
)

func TestSessionObservabilitySelection(t *testing.T) {
	dir := t.TempDir()
	session := filepath.Join(dir, "session.json")
	require.NoError(t, os.WriteFile(session, []byte(`{"schema_version":1,"service_context":{"endpoint":"http://service:9091","observability":{"prometheus":{"endpoint":"http://session-metrics:9090"}}},"client_meta":{"endpoints":["etcd:2379"],"meta_prefix":"tenant/wp","password":"private-value"}}`), 0o600))
	t.Setenv(config.SessionConfigEnv, session)
	saved := filepath.Join(dir, "cli.yaml")
	require.NoError(t, os.WriteFile(saved, []byte("current-context: saved\ncontexts:\n  saved:\n    endpoint: http://saved:9091\n    observability:\n      prometheus:\n        endpoint: http://saved-metrics:9090\n"), 0o600))
	t.Setenv("WOODPECKER_CLI_CONFIG", saved)
	for _, tc := range []struct {
		name, path, context, endpoint string
	}{
		{"session", "", "", "http://session-metrics:9090"},
		{"explicit config", saved, "", "http://saved-metrics:9090"},
		{"explicit context", "", "saved", "http://saved-metrics:9090"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _, err := Load(tc.path, tc.context)
			require.NoError(t, err)
			require.Len(t, cfg.Sources["prometheus"].Connections, 1)
			for _, conn := range cfg.Sources["prometheus"].Connections {
				var settings map[string]any
				require.NoError(t, conn.Settings.Decode(&settings))
				require.Equal(t, tc.endpoint, settings["endpoint"])
				require.Equal(t, dir, conn.BaseDir)
				require.NotContains(t, settings, "password")
			}
		})
	}
	t.Setenv(config.SessionConfigEnv, filepath.Join(dir, "missing.json"))
	_, _, err := Load("", "")
	require.ErrorContains(t, err, config.SessionConfigEnv)
	_, _, err = Load(saved, "")
	require.NoError(t, err)
}

func TestSessionProviderSettingsReachDiagnostics(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"schema_version":1,"service_context":{"observability":{
		"loki":{"endpoint":"http://logs:3100","match_labels":{"namespace":"wp"},"tenant_env":"LOG_TENANT"},
		"future_source":{"settings":{"endpoint":"https://future","nested":{"values":[1,2],"enabled":true}}}
	}}}`), 0o600))
	t.Setenv(config.SessionConfigEnv, path)
	cfg, _, err := Load("", "")
	require.NoError(t, err)
	for source, endpoint := range map[string]string{"loki": "http://logs:3100", "future_source": "https://future"} {
		conn := cfg.Sources[source].Connections[config.SourceReference("session", source)]
		require.Equal(t, dir, conn.BaseDir)
		var settings map[string]any
		require.NoError(t, conn.Settings.Decode(&settings))
		require.Equal(t, endpoint, settings["endpoint"])
		if source == "loki" {
			require.Equal(t, "LOG_TENANT", settings["tenant_env"])
		} else {
			require.Equal(t, map[string]any{"values": []any{1, 2}, "enabled": true}, settings["nested"])
		}
	}
}

func TestSessionMetricsAndTraceUseProviderConnections(t *testing.T) {
	for _, tc := range []struct{ source, command, check, response string }{
		{"prometheus", "metrics", "writer-throughput", `{"status":"success","data":{"resultType":"matrix","result":[]}}`},
		{"trace", "trace", "server-append-duration", `{"data":[]}`},
	} {
		t.Run(tc.source, func(t *testing.T) {
			auth := make(chan string, 16)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				auth <- r.Header.Get("Authorization")
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, tc.response)
			}))
			defer server.Close()
			path := filepath.Join(t.TempDir(), "session.json")
			settings := fmt.Sprintf(`"endpoint":%q,"bearer_token_env":"WP_SESSION_TEST_TOKEN"`, server.URL)
			if tc.source == "trace" {
				settings += `,"services":{"woodpecker":"wp_server"},"search":{"service":"woodpecker"}`
			} else {
				settings += `,"match_labels":{"namespace":"wp"}`
			}
			require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf(`{"schema_version":1,"service_context":{"observability":{%q:{%s}}}}`, tc.source, settings)), 0o600))
			t.Setenv(config.SessionConfigEnv, path)
			t.Setenv("WP_SESSION_TEST_TOKEN", "private-value")
			out, _, err := execute(t, "diag", tc.command, "check", tc.check, "--window=5m", "--output=json")
			require.NoError(t, err, string(out))
			require.NotContains(t, string(out), "private-value")
			require.NotEmpty(t, auth, "configured provider must be queried")
			close(auth)
			for header := range auth {
				require.Equal(t, "Bearer private-value", header)
			}
		})
	}
}

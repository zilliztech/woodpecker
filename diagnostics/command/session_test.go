package command

import (
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

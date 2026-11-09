package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestEnvironmentRoundTripExcludesLegacyClientState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cli.yaml")
	original := []byte(`current-context: local
contexts:
  local:
    endpoint: http://wp:9091
    etcd: {endpoints: [http://etcd:2379], meta_prefix: tenant/wp, password: old-secret}
    diagnostic_target: old-tenant
    observability:
      prometheus: old
      trace:
        endpoint: http://trace:16686
        deployment: local
        services: {wp: wp_server}
        search: {service: wp}
      grafana: http://grafana:3000
client_metas:
  tenant: {bucket: old-bucket, root_path: old-root}
diagnostics:
  sources:
    metrics:
      connections:
        old:
          settings: {endpoint: http://prom:9090, deployment: local, match_labels: {namespace: local}}
  targets:
    old-tenant: {log_namespace: private-namespace, log_id: '7'}
`)
	require.NoError(t, os.WriteFile(path, original, 0o600))
	f, err := Load(path)
	require.NoError(t, err)
	body, err := yaml.Marshal(f)
	require.NoError(t, err)
	for _, forbidden := range []string{"client_metas:", "diagnostics:", "etcd:", "diagnostic_target:", "old-secret", "old-bucket", "old-root", "private-namespace", "log_id:"} {
		require.NotContains(t, string(body), forbidden)
	}
	require.Contains(t, string(body), "http://prom:9090")
	require.Contains(t, string(body), "http://trace:16686")
	require.NoError(t, os.WriteFile(path, body, 0o600))
	again, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, f.Contexts["local"].Observability.MetricsReference(), again.Contexts["local"].Observability.MetricsReference())
	require.Len(t, again.Contexts["local"].Observability.Connections, 2)
}

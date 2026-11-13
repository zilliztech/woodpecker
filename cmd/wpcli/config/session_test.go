package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadSessionPreservesConfigurationTypes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
  "schema_version": 1,
  "scope": {"woodpecker_id": "wp-a", "milvus_instance_id": "client-a"},
  "service_context": {
    "endpoint": "http://node:9091", "admin_port": 19091, "timeout": "250ms",
    "concurrency": 3, "strict": true, "node_admin_urls": {"node:18080": "http://localhost:19091"},
    "k8s": {"namespace": "pool", "kube_context": "cluster-a"},
    "observability": {"prometheus": {"endpoint": "http://metrics:9090"}}
  },
  "client_meta": {
    "endpoints": ["https://etcd:2379"], "meta_prefix": "tenant/woodpecker",
    "username": "user", "password": "secret", "cert": "client.pem", "key": "client-key.pem",
    "cacert": "ca.pem", "tls_min_version": "1.3", "tls_enabled": false
  },
  "client_storage": {"bucket": "instance-bucket", "root_path": "instance/root"}
}`), 0o600))
	s, err := LoadSession(path)
	require.NoError(t, err)
	require.Equal(t, "client-a", s.Scope.MilvusInstanceID)
	require.Equal(t, 19091, s.ServiceContext.AdminPort)
	require.Equal(t, 250*time.Millisecond, s.ServiceContext.Timeout)
	require.Equal(t, 3, s.ServiceContext.Concurrency)
	require.True(t, s.ServiceContext.Strict)
	require.Equal(t, "http://localhost:19091", s.ServiceContext.NodeAdminURLs["node:18080"])
	require.Equal(t, "cluster-a", s.ServiceContext.K8s.KubeContext)
	require.Contains(t, s.ServiceContext.Observability.Connections, "prometheus")
	require.Equal(t, "tenant/woodpecker", s.ClientMeta.MetaPrefix)
	require.Equal(t, "secret", s.ClientMeta.Password)
	require.Equal(t, "client-key.pem", s.ClientMeta.Key)
	require.Equal(t, "1.3", s.ClientMeta.TLSMinVersion)
	require.NotNil(t, s.ClientMeta.TLSEnabled)
	require.False(t, *s.ClientMeta.TLSEnabled)
	require.Equal(t, &StorageConfig{Bucket: "instance-bucket", RootPath: "instance/root"}, s.ClientStorage)
}

func TestLoadSessionIndependentSections(t *testing.T) {
	for _, body := range []string{
		`{"schema_version":1,"service_context":{"endpoint":"http://node:9091"}}`,
		`{"schema_version":1,"client_meta":{"endpoints":["localhost:2379"],"meta_prefix":"tenant/wp"}}`,
	} {
		path := filepath.Join(t.TempDir(), "session.json")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		s, err := LoadSession(path)
		require.NoError(t, err)
		require.NotEqual(t, s.ServiceContext == nil, s.ClientMeta == nil)
	}
}

func TestLoadSessionRejectsInvalidInputWithoutLeakingValues(t *testing.T) {
	for name, body := range map[string]string{
		"yaml":                    "schema_version: 1\nclient_meta: {password: private-value}",
		"malformed":               `{"schema_version":1,"password":"private-value"`,
		"trailing document":       `{"schema_version":1} {"password":"private-value"}`,
		"unknown field":           `{"schema_version":1,"private-value":"private-value"}`,
		"unknown nested field":    `{"schema_version":1,"client_meta":{"etcd":{"password":"private-value"}}}`,
		"unknown storage field":   `{"schema_version":1,"client_storage":{"bucket_name":"private-value"}}`,
		"provider must be object": `{"schema_version":1,"service_context":{"observability":{"loki":"private-value"}}}`,
		"duration type":           `{"schema_version":1,"service_context":{"timeout":"private-value"}}`,
		"unsupported version":     `{"schema_version":2}`,
		"missing version":         `{}`,
		"null":                    `null`,
		"array":                   `[]`,
		"duplicate key":           `{"schema_version":1,"schema_version":1}`,
		"too large":               `{"schema_version":1,"scope":{"woodpecker_id":"` + strings.Repeat("x", 1<<20) + `"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "session.json")
			require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
			_, err := LoadSession(path)
			require.Error(t, err)
			require.Contains(t, err.Error(), SessionConfigEnv)
			require.NotContains(t, err.Error(), "private-value")
		})
	}
	_, err := LoadSession(filepath.Join(t.TempDir(), "missing.json"))
	require.ErrorContains(t, err, "cannot open")
	_, err = LoadSession(t.TempDir())
	require.ErrorContains(t, err, "regular file")
}

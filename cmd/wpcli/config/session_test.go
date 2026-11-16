package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestLoadSessionPreservesConfigurationTypes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
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
	for _, tc := range []struct {
		body                       string
		service, metadata, storage bool
	}{
		{`{"service_context":{"endpoint":"http://node:9091"}}`, true, false, false},
		{`{"client_meta":{"endpoints":["localhost:2379"],"meta_prefix":"tenant/wp"}}`, false, true, false},
		{`{"client_storage":{"bucket":"bucket","root_path":"root"}}`, false, false, true},
		{`{}`, false, false, false},
	} {
		path := filepath.Join(t.TempDir(), "session.json")
		require.NoError(t, os.WriteFile(path, []byte(tc.body), 0o600))
		s, err := LoadSession(path)
		require.NoError(t, err)
		require.Equal(t, tc.service, s.ServiceContext != nil)
		require.Equal(t, tc.metadata, s.ClientMeta != nil)
		require.Equal(t, tc.storage, s.ClientStorage != nil)
	}
}

func TestSessionServiceViewExcludesClientSettings(t *testing.T) {
	session := &SessionConfig{
		ServiceContext: &Context{Endpoint: "http://service:9091"},
		ClientMeta: &EtcdConfig{
			Endpoints: []string{"http://etcd:2379"}, MetaPrefix: "tenant/wp", Password: "private-value",
		},
		ClientStorage: &StorageConfig{Bucket: "bucket", RootPath: "tenant"},
	}
	file, err := session.ServiceFile()
	require.NoError(t, err)
	require.Equal(t, "session", file.CurrentContext)
	service, err := file.ResolveContext("")
	require.NoError(t, err)
	require.Equal(t, "http://service:9091", service.Endpoint)
	require.Equal(t, 30*time.Second, service.Timeout)
	body, err := yaml.Marshal(file)
	require.NoError(t, err)
	for _, omitted := range []string{"client_meta", "client_storage", "private-value", "http://etcd:2379"} {
		require.NotContains(t, string(body), omitted)
	}
	_, err = (&SessionConfig{ClientMeta: session.ClientMeta}).ServiceFile()
	require.ErrorContains(t, err, "requires service_context")
}

func TestSessionStorageRequiresCompleteLocation(t *testing.T) {
	for _, storage := range []StorageConfig{{}, {Bucket: "bucket"}, {RootPath: "root"}, {Bucket: " ", RootPath: "root"}} {
		require.Error(t, storage.Validate())
	}
	require.NoError(t, (StorageConfig{Bucket: "bucket", RootPath: "tenant/root"}).Validate())
}

func TestSessionPrivateFileFailureDoesNotExposeCredentials(t *testing.T) {
	session := SessionConfig{ClientMeta: &EtcdConfig{Password: "private-value"}}
	path, err := session.WriteTemp(filepath.Join(t.TempDir(), "missing"))
	require.Empty(t, path)
	require.ErrorContains(t, err, "cannot create private session configuration")
	require.NotContains(t, err.Error(), "private-value")
}

func TestLoadSessionRejectsInvalidInputWithoutLeakingValues(t *testing.T) {
	for name, body := range map[string]string{
		"misplaced client metadata": `{"service_context":{"etcd":{"endpoints":["private-value"]}}}`,
		"multiple YAML documents":   "client_meta: {meta_prefix: tenant/wp}\n---\nclient_storage: {bucket: private-value, root_path: root}\n",
		"malformed":                 `{"password":"private-value"`,
		"trailing document":         `{} {"password":"private-value"}`,
		"unknown field":             `{"private-value":"private-value"}`,
		"unrelated scope":           `{"scope":{"milvus_instance_id":"private-value"}}`,
		"unknown nested field":      `{"client_meta":{"etcd":{"password":"private-value"}}}`,
		"unknown storage field":     `{"client_storage":{"bucket_name":"private-value"}}`,
		"provider must be object":   `{"service_context":{"observability":{"loki":"private-value"}}}`,
		"duration type":             `{"service_context":{"timeout":"private-value"}}`,
		"section type":              `{"client_meta":[]}`,
		"scalar":                    `"private-value"`,
		"null":                      `null`,
		"array":                     `[]`,
		"duplicate key":             `{"client_meta":{},"client_meta":{}}`,
		"too large":                 `{"service_context":{"endpoint":"` + strings.Repeat("x", 1<<20) + `"}}`,
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

func TestLoadSessionJSONAndYAMLHaveTheSameMeaning(t *testing.T) {
	dir := t.TempDir()
	jsonPath, yamlPath := filepath.Join(dir, "session.json"), filepath.Join(dir, "session.yaml")
	require.NoError(t, os.WriteFile(jsonPath, []byte(`{"service_context":{"endpoint":"http://wp:9091","timeout":"2s","observability":{"trace":{"endpoint":"http://trace:16686"}}},"client_meta":{"endpoints":["http://etcd:2379"],"meta_prefix":"tenant/wp","tls_enabled":false},"client_storage":{"bucket":"bucket","root_path":"tenant"}}`), 0o600))
	require.NoError(t, os.WriteFile(yamlPath, []byte(`service_context:
  endpoint: http://wp:9091
  timeout: 2s
  observability:
    trace:
      endpoint: http://trace:16686
client_meta:
  endpoints: [http://etcd:2379]
  meta_prefix: tenant/wp
  tls_enabled: false
client_storage:
  bucket: bucket
  root_path: tenant
`), 0o600))
	a, err := LoadSession(jsonPath)
	require.NoError(t, err)
	b, err := LoadSession(yamlPath)
	require.NoError(t, err)
	// YAML node source locations/styles differ; compare their decoded values.
	var aj, by map[string]any
	traceA, traceB := a.ServiceContext.Observability.Connections["trace"], b.ServiceContext.Observability.Connections["trace"]
	require.NoError(t, traceA.Decode(&aj))
	require.NoError(t, traceB.Decode(&by))
	require.Equal(t, aj, by)
	a.ServiceContext.Observability, b.ServiceContext.Observability = ObservabilityConfig{}, ObservabilityConfig{}
	require.Equal(t, a, b)
	private, err := a.WriteTemp(dir)
	require.NoError(t, err)
	info, err := os.Stat(private)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	c, err := LoadSession(private)
	require.NoError(t, err)
	require.Equal(t, a, c)
}

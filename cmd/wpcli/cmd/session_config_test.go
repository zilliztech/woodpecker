package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	"github.com/zilliztech/woodpecker/cmd/wpcli/diagnosis"
)

func withSessionJSON(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "session.json")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	t.Setenv(config.SessionConfigEnv, path)
	old := Globals
	t.Cleanup(func() { Globals = old })
	return path
}

func TestSessionServicePrecedenceAndTransportDefaults(t *testing.T) {
	withCliYAML(t, "http://saved:9091")
	t.Setenv("WOODPECKER_ENDPOINT", "http://localhost:9091")
	path := withSessionJSON(t, `{"schema_version":1,"service_context":{"endpoint":"http://session:19091","admin_port":19091,"timeout":"250ms","concurrency":2,"strict":true,"node_admin_urls":{"node":"http://mapped:19091"}}}`)
	for _, tc := range []struct {
		name     string
		args     []string
		endpoint string
		mappings int
		timeout  time.Duration
		strict   bool
	}{
		{"session", nil, "http://session:19091", 1, 250 * time.Millisecond, true},
		{"transport flags", []string{"--timeout=2s", "--strict=false"}, "http://session:19091", 1, 2 * time.Second, false},
		{"explicit endpoint", []string{"--endpoint=http://explicit:9091"}, "http://explicit:9091", 0, 30 * time.Second, false},
		{"explicit context", []string{"--context=prod"}, "http://saved:9091", 0, 30 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := NewRootCommand()
			root.AddCommand(&cobra.Command{Use: "resolve-test", RunE: func(*cobra.Command, []string) error {
				r, err := resolveConfiguration()
				if err != nil {
					return err
				}
				require.Equal(t, tc.endpoint, r.Context.Endpoint)
				require.Len(t, r.Context.NodeAdminURLs, tc.mappings)
				require.Equal(t, tc.timeout, r.Context.Timeout)
				require.Equal(t, tc.strict, r.Context.Strict)
				if tc.name == "session" {
					require.Equal(t, path, r.ConfigPath)
					require.Equal(t, 19091, r.Context.AdminPort)
					require.Equal(t, 2, r.Context.Concurrency)
				}
				return nil
			}})
			root.SetArgs(append([]string{"resolve-test"}, tc.args...))
			require.NoError(t, root.Execute())
		})
	}
	Globals = GlobalFlags{Context: "missing"}
	_, err := resolveConfiguration()
	require.Error(t, err)
	for _, arg := range []string{"--endpoint=", "--context="} {
		root := NewRootCommand()
		root.AddCommand(&cobra.Command{Use: "empty-target", RunE: func(*cobra.Command, []string) error {
			_, err := resolveConfiguration()
			return err
		}})
		root.SetErr(io.Discard)
		root.SetArgs([]string{"empty-target", arg})
		require.ErrorContains(t, root.Execute(), "must not be empty")
	}
}

func TestSessionServiceDiscoveryAndMappedFanout(t *testing.T) {
	server := spinTestServer(t, `{"members":[{"id":"node-1","service_addr":"unresolvable.invalid:18080","state":0}]}`, nil)
	defer server.Close()
	withSessionJSON(t, fmt.Sprintf(`{"schema_version":1,"service_context":{"endpoint":%q,"node_admin_urls":{"unresolvable.invalid:18080":%q}}}`, server.URL, server.URL))
	t.Setenv("WOODPECKER_ENDPOINT", "http://127.0.0.1:1")
	root := NewRootCommand()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"node", "list", "-o", "json"})
	require.NoError(t, root.Execute())
	var rows []map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &rows))
	require.Len(t, rows, 1)
	require.Equal(t, "node-1", rows[0]["name"])
}

func TestSessionMetadataTargetSelection(t *testing.T) {
	withSessionJSON(t, `{"schema_version":1,"client_meta":{"endpoints":["http://session-etcd:2379"],"meta_prefix":"tenant-a/wp","username":"alice","password":"session-secret","cacert":"unused.pem","tls_enabled":false}}`)
	Globals = GlobalFlags{Context: "irrelevant"}
	conn, err := resolveMetaEtcd(&metaEtcdFlags{})
	require.NoError(t, err)
	require.Equal(t, "tenant-a/wp", conn.kb.Prefix())
	require.Equal(t, "session-secret", conn.password)
	require.False(t, conn.useSSL)
	for _, flags := range []metaEtcdFlags{
		{etcdEndpoints: "http://other:2379"}, {metaPrefix: "tenant-b/wp"}, {standalone: true},
	} {
		_, err := resolveMetaEtcd(&flags)
		require.Error(t, err, "partial overrides must not inherit session settings")
	}
	conn, err = resolveMetaEtcd(&metaEtcdFlags{etcdEndpoints: "http://other:2379", metaPrefix: "tenant-b/wp"})
	require.NoError(t, err)
	require.Empty(t, conn.password)
	require.Empty(t, conn.username)
	require.Empty(t, conn.tlsCACert)
	parameters := filepath.Join(t.TempDir(), "client.yaml")
	require.NoError(t, os.WriteFile(parameters, []byte("endpoints: [http://file:2379]\nmeta_prefix: file/wp\n"), 0o600))
	conn, err = resolveMetaEtcd(&metaEtcdFlags{parametersFile: parameters})
	require.NoError(t, err)
	require.Equal(t, []string{"http://file:2379"}, conn.endpoints)
	require.Empty(t, conn.password)
	conn, err = resolveEtcd(&metaEtcdFlags{etcdEndpoints: "http://raw:2379"}, false)
	require.NoError(t, err)
	require.Empty(t, conn.password)
	_, err = resolveConfiguration()
	require.Error(t, err, "metadata-only config must not silently select a service")
}

func TestSessionConfigurationFailuresAreClosedAndRedacted(t *testing.T) {
	withCliYAML(t, "http://saved:9091")
	t.Setenv("WOODPECKER_ENDPOINT", "http://localhost:9091")
	for _, body := range []string{
		`{"schema_version":2}`,
		`{"schema_version":1,"service_context":{"endpoint":"http://user:private-value@node:9091"}}`,
		`{"schema_version":1,"service_context":{"endpoint":"http://node:9091","node_admin_urls":{"node":"http://user:private-value@node:9091"}}}`,
		`{"schema_version":1,"service_context":{}}`,
	} {
		t.Run(body, func(t *testing.T) {
			withSessionJSON(t, body)
			Globals = GlobalFlags{}
			_, err := resolveConfiguration()
			require.Error(t, err)
			require.NotContains(t, err.Error(), "private-value")
		})
	}
	for _, fields := range []string{
		`"endpoints":["http://user:private-value@etcd:2379"]`,
		`"endpoints":["localhost:2379"],"password":"private-value"`,
		`"endpoints":["localhost:2379"],"cert":"private-value"`,
		`"endpoints":["localhost:2379"],"tls_enabled":true,"tls_min_version":"private-value"`,
		`"endpoints":[""]`,
	} {
		t.Run(fields, func(t *testing.T) {
			withSessionJSON(t, `{"schema_version":1,"client_meta":{"meta_prefix":"tenant/wp",`+fields+`}}`)
			_, err := resolveMetaEtcd(&metaEtcdFlags{})
			require.Error(t, err)
			require.NotContains(t, err.Error(), "private-value")
		})
	}
	t.Setenv(config.SessionConfigEnv, filepath.Join(t.TempDir(), "missing"))
	Globals = GlobalFlags{}
	_, err := resolveConfiguration()
	require.ErrorContains(t, err, "cannot open")
	_, err = resolveMetaEtcd(&metaEtcdFlags{})
	require.ErrorContains(t, err, "cannot open")
	Globals.Endpoint = "http://explicit:9091"
	_, err = resolveConfiguration()
	require.NoError(t, err, "explicit service target does not load the session")
	_, err = resolveMetaEtcd(&metaEtcdFlags{etcdEndpoints: "localhost:2379", metaPrefix: "explicit/wp"})
	require.NoError(t, err)
}

func TestSessionMetadataReadAndDiagnosisPrerequisites(t *testing.T) {
	etcd, endpoint := startShellTestEtcd(t)
	_, err := etcd.Put(context.Background(), "tenant/wp/example", "session-value")
	require.NoError(t, err)
	withSessionJSON(t, fmt.Sprintf(`{"schema_version":1,"client_meta":{"endpoints":[%q],"meta_prefix":"tenant/wp"}}`, endpoint))
	root := NewRootCommand()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"etcd", "get", "tenant/wp/example"})
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), "session-value")
	var read readDiagnosisFlags
	fixture := &cobra.Command{}
	read.metadata.register(fixture)
	fixture.Flags().StringVar(&read.logName, "log-name", "", "")
	fixture.Flags().Int64Var(&read.segment, "segment", 0, "")
	require.NoError(t, fixture.ParseFlags([]string{"--log-name", "log", "--segment", "0"}))
	require.NoError(t, read.validate("read"))
}

func TestSessionShellStartupAndDisconnect(t *testing.T) {
	for _, service := range []string{"", `,"service_context":{"endpoint":"http://node:9091"}`} {
		withSessionJSON(t, `{"schema_version":1,"scope":{"milvus_instance_id":"client-a"},"client_meta":{"endpoints":["localhost:2379"],"meta_prefix":"a/wp","username":"alice","password":"private-value"}`+service+`}`)
		root := NewRootCommand()
		var out bytes.Buffer
		root.SetIn(strings.NewReader("status\ndisconnect\nstatus\nexit\n"))
		root.SetOut(&out)
		root.SetErr(&out)
		root.SetArgs([]string{"shell"})
		require.NoError(t, root.Execute())
		require.Contains(t, out.String(), "client-a")
		require.Contains(t, out.String(), "Tenant metadata: not configured")
		require.NotContains(t, out.String(), "private-value")
		require.NotContains(t, strings.Join(shellChildEnvironment("selected.yaml"), "\n"), config.SessionConfigEnv+"=")
	}
}

func TestSessionShellExplicitMetadataDoesNotReuseCredentials(t *testing.T) {
	s, _ := newTestShell(t)
	s.sessionConfig = true
	s.tenant = &shellTenant{name: "old", bucket: "old-bucket", rootPath: "old/root", etcd: config.EtcdConfig{
		Endpoints: []string{"old:2379"}, MetaPrefix: "old/wp", Username: "old-user", Password: "old-secret",
	}}
	s.runChild = func(_ context.Context, args []string, _ string) error {
		joined := strings.Join(args, " ")
		require.NotContains(t, joined, "old")
		require.NotContains(t, joined, "--client-parameters-file")
		return nil
	}
	require.NoError(t, shellExecute(t, s, "log", "readers", "log", "--etcd=new:2379", "--meta-prefix=new/wp"))
	require.ErrorContains(t, shellExecute(t, s, "log", "readers", "log", "--etcd=new:2379"), "both --etcd and --meta-prefix")
	require.Equal(t, "old-secret", s.tenant.etcd.Password)
	s.tenant = nil
	require.NoError(t, shellExecute(t, s, "log", "readers", "log", "--etcd=new:2379", "--meta-prefix=new/wp"))
}

func TestSessionShellConnectUsesSharedMetadataResolver(t *testing.T) {
	withSessionJSON(t, `{"schema_version":1,"client_meta":{"endpoints":["localhost:2379"],"meta_prefix":"a/wp","username":"alice","password":"private-value","tls_enabled":false}}`)
	s, out := newTestShell(t)
	require.NoError(t, shellExecute(t, s, "connect", "client-a"))
	require.Equal(t, "private-value", s.tenant.etcd.Password)
	require.NotContains(t, out.String(), "private-value")
	s.runChild = func(_ context.Context, args []string, path string) error {
		for _, arg := range args {
			if filename, ok := strings.CutPrefix(arg, "--client-parameters-file="); ok {
				conn, err := resolveMetaEtcd(&metaEtcdFlags{parametersFile: filename})
				require.NoError(t, err)
				require.Equal(t, "a/wp", conn.kb.Prefix())
				require.Equal(t, "private-value", conn.password)
				return nil
			}
		}
		t.Fatal("missing private client parameters file")
		return nil
	}
	require.NoError(t, shellExecute(t, s, "meta", "log", "list"))
}

func TestSessionK8sAndNativeDiagnosis(t *testing.T) {
	server := diagnoseTestServer(t, "")
	defer server.Close()
	withSessionJSON(t, fmt.Sprintf(`{"schema_version":1,"service_context":{"endpoint":%q,"timeout":"1s","node_admin_urls":{"node-1":%q},"k8s":{"namespace":"session-pool","cluster":"session-cluster"}}}`, server.URL, server.URL))
	Globals = GlobalFlags{}
	k8s, err := resolveK8sConfig()
	require.NoError(t, err)
	require.Equal(t, "session-pool", k8s.Namespace)
	require.Equal(t, "session-cluster", k8s.Cluster)
	root := NewRootCommand()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(io.Discard)
	root.SetArgs([]string{"diagnose", "node-1", "--mode=snapshot", "--scenario=read", "-o=json"})
	require.NoError(t, root.Execute())
	var report diagnosis.Report
	require.NoError(t, json.Unmarshal(out.Bytes(), &report))
	require.Equal(t, "session", report.Context)
	require.Equal(t, "completed", report.Status)

	s, _ := newTestShell(t)
	s.sessionConfig = true
	s.runChild = func(context.Context, []string, string) error { return nil }
	require.NoError(t, shellExecute(t, s, "diagnose", "--scenario=read", "--mode=snapshot"))
}

func TestSessionMetadataOnlyShellChecks(t *testing.T) {
	_, endpoint := startShellTestEtcd(t)
	withSessionJSON(t, fmt.Sprintf(`{"schema_version":1,"client_meta":{"endpoints":[%q],"meta_prefix":"tenant/wp"}}`, endpoint))
	root := NewRootCommand()
	var out bytes.Buffer
	root.SetIn(strings.NewReader("status --check\nexit\n"))
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"shell"})
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), "Admin check: skipped")
	require.Contains(t, out.String(), "Etcd check: OK")
}

func TestSessionMetadataRejectsImplicitPrefixDefaults(t *testing.T) {
	for _, prefix := range []string{"", ".", "/", "   "} {
		withSessionJSON(t, fmt.Sprintf(`{"schema_version":1,"client_meta":{"endpoints":["localhost:2379"],"meta_prefix":%q}}`, prefix))
		_, err := resolveMetaEtcd(&metaEtcdFlags{})
		require.Error(t, err)
		_, err = resolveEtcd(&metaEtcdFlags{}, false)
		require.NoError(t, err, "raw etcd commands do not require a metadata prefix")
	}
}

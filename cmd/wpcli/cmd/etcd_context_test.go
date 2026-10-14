package cmd

import (
	"bytes"
	"crypto/tls"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestMetaEtcdContextResolution(t *testing.T) {
	original := Globals
	t.Cleanup(func() { Globals = original })
	Globals.Context = ""
	configPath := filepath.Join(t.TempDir(), "cli.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(`current-context: remote
contexts:
  remote:
    etcd:
      endpoints: ["127.0.0.1:32379"]
      meta_prefix: lab/woodpecker
      username: context-user
      password: context-password
      cacert: /local/ca.pem
      tls_min_version: "1.3"
`), 0o600))
	t.Setenv("WOODPECKER_CLI_CONFIG", configPath)
	f := metaEtcdFlags{}
	cmd := &cobra.Command{}
	f.register(cmd)
	conn, err := resolveMetaEtcd(&f)
	require.NoError(t, err)
	require.Equal(t, []string{"127.0.0.1:32379"}, conn.endpoints)
	require.True(t, conn.useSSL)
	require.Equal(t, "/local/ca.pem", conn.tlsCACert)
	require.Equal(t, "1.3", conn.tlsMinVer)
	require.Equal(t, "context-user", conn.username)
	require.NoError(t, cmd.Flags().Parse([]string{"--etcd", " localhost:1234 , localhost:5678 ", "--etcd-username", "flag-user", "--etcd-password=", "--etcd-cacert", "/flag/ca.pem", "--meta-prefix", "override/woodpecker"}))
	conn, err = resolveMetaEtcd(&f)
	require.NoError(t, err)
	require.Equal(t, []string{"localhost:1234", "localhost:5678"}, conn.endpoints)
	require.Equal(t, "flag-user", conn.username)
	require.Empty(t, conn.password)
	require.Equal(t, "/flag/ca.pem", conn.tlsCACert)
	expected, err := resolveMetaEtcd(&metaEtcdFlags{etcdEndpoints: "localhost:1234", metaPrefix: "override/woodpecker"})
	require.NoError(t, err)
	require.Equal(t, expected.kb, conn.kb)
	Globals.Context = "missing"
	_, err = resolveMetaEtcd(&f)
	require.ErrorContains(t, err, "context")
}

func TestEtcdTLSConfig_ServerAuthentication(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("verified"))
	}))
	defer srv.Close()
	caPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600))
	cfg, err := metaEtcdTLSConfig(&metaEtcdConn{tlsCACert: caPath})
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
	transport := &http.Transport{TLSClientConfig: cfg}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	response, err := client.Get(srv.URL)
	require.NoError(t, err, "CA-only server-auth TLS must not require a client cert/key")
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, "verified", string(body))
}

func TestEtcdTLSConfig_InvalidLocalSettings(t *testing.T) {
	badCA := filepath.Join(t.TempDir(), "invalid-ca.pem")
	require.NoError(t, os.WriteFile(badCA, []byte("not a PEM certificate"), 0o600))
	for _, tc := range []struct {
		conn metaEtcdConn
		want string
	}{
		{metaEtcdConn{tlsCert: "cert-only.pem"}, "cert and key must be provided together"},
		{metaEtcdConn{tlsKey: "key-only.pem"}, "cert and key must be provided together"},
		{metaEtcdConn{tlsCACert: badCA}, "no valid PEM certificates"},
		{metaEtcdConn{tlsMinVer: "invalid"}, "unknown etcd TLS minimum version"},
	} {
		_, err := metaEtcdTLSConfig(&tc.conn)
		require.ErrorContains(t, err, tc.want)
	}
	cfg, err := metaEtcdTLSConfig(&metaEtcdConn{tlsMinVer: "1.3"})
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS13), cfg.MinVersion)
}

func TestCtxViewEtcdDoesNotPrintPassword(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "cli.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(`current-context: remote
contexts:
  remote:
    etcd:
      endpoints: ["127.0.0.1:32379"]
      meta_prefix: lab/woodpecker
      username: context-user
      password: do-not-print-this-test-password
      tls_enabled: false
`), 0o600))
	t.Setenv("WOODPECKER_CLI_CONFIG", configPath)
	old := Globals
	t.Cleanup(func() { Globals = old })
	root := NewRootCommand()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"ctx", "view"})
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), "127.0.0.1:32379")
	require.Contains(t, out.String(), "lab/woodpecker")
	require.Contains(t, out.String(), "context-user")
	require.NotContains(t, out.String(), "do-not-print-this-test-password")
}

func TestMetaEtcdContextExplicitTLSFalse(t *testing.T) {
	original := Globals
	t.Cleanup(func() { Globals = original })
	Globals.Context = ""
	configPath := filepath.Join(t.TempDir(), "cli.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(`current-context: remote
contexts:
  remote:
    etcd:
      endpoints: ["127.0.0.1:32379"]
      meta_prefix: lab/woodpecker
      tls_enabled: false
      cacert: /unused/context-ca.pem
`), 0o600))
	t.Setenv("WOODPECKER_CLI_CONFIG", configPath)
	f := metaEtcdFlags{}
	command := &cobra.Command{}
	f.register(command)
	conn, err := resolveMetaEtcd(&f)
	require.NoError(t, err)
	require.False(t, conn.useSSL)
	require.NoError(t, command.Flags().Parse([]string{"--etcd-cacert", "/flag/ca.pem"}))
	conn, err = resolveMetaEtcd(&f)
	require.NoError(t, err)
	require.True(t, conn.useSSL)
	require.Equal(t, "/flag/ca.pem", conn.tlsCACert)
}

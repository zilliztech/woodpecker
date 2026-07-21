package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMarkingDiscovery_FromAdminConfig verifies the zero-config path: etcd endpoints and
// the meta prefix are discovered from a node's /admin/config (etcd.rootPath joined with
// woodpecker.meta.prefix), and the key builder lands on the marking keyspace.
func TestMarkingDiscovery_FromAdminConfig(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	cfg := `{"Etcd":{"Endpoints":["etcd-a:2379","etcd-b:2379"],"RootPath":"by-dev",` +
		`"Ssl":{"Enabled":true,"TlsCert":"/certs/client.crt","TlsKey":"/certs/client.key","TlsCACert":"/certs/ca.crt","TlsMinVersion":"1.3"},` +
		`"Auth":{"Enabled":true,"UserName":"wp","Password":"secret"}},` +
		`"Woodpecker":{"Meta":{"Prefix":"woodpecker"}}}`
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/memberlist", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(ml)) })
	mux.HandleFunc("/admin/config", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(cfg)) })
	srv := httptest.NewServer(mux)
	defer srv.Close()
	withCliYAML(t, srv.URL)

	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	port, err := strconv.Atoi(extractPort(t, srv.URL))
	require.NoError(t, err)
	Globals = GlobalFlags{AdminPort: port, Timeout: 5 * time.Second}

	conn, err := resolveMarkingEtcd(&markingEtcdFlags{})
	require.NoError(t, err)
	require.Equal(t, []string{"etcd-a:2379", "etcd-b:2379"}, conn.endpoints)
	require.Equal(t, "by-dev/woodpecker/marking", conn.kb.SegmentCompactedNotifyStatusPrefix())
	require.Equal(t, "by-dev/woodpecker/marking/7/3", conn.kb.BuildSegmentCompactedNotifyStatusKey(7, 3))
	// The cluster's own TLS/auth settings are carried into the connection spec.
	assert.True(t, conn.useSSL)
	assert.Equal(t, "/certs/client.crt", conn.tlsCert)
	assert.Equal(t, "/certs/client.key", conn.tlsKey)
	assert.Equal(t, "/certs/ca.crt", conn.tlsCACert)
	assert.Equal(t, "1.3", conn.tlsMinVer)
	assert.Equal(t, "wp", conn.username)
	assert.Equal(t, "secret", conn.password)
}

// TestMarkingDiscovery_FlagOverridesSkipDiscovery verifies --etcd/--meta-prefix work fully
// offline: with both set, no admin endpoint is contacted at all — and the TLS/auth flags
// carry into the connection spec (enabling SSL when cert flags are present).
func TestMarkingDiscovery_FlagOverridesSkipDiscovery(t *testing.T) {
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 5 * time.Second}

	conn, err := resolveMarkingEtcd(&markingEtcdFlags{
		etcdEndpoints: "e1:2379,e2:2379",
		metaPrefix:    "custom/wp",
		etcdCACert:    "/local/ca.crt",
		etcdUsername:  "op",
		etcdPassword:  "pw",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"e1:2379", "e2:2379"}, conn.endpoints)
	require.Equal(t, "custom/wp/marking", conn.kb.SegmentCompactedNotifyStatusPrefix())
	assert.True(t, conn.useSSL, "a cert flag enables SSL even without discovery")
	assert.Equal(t, "/local/ca.crt", conn.tlsCACert)
	assert.Equal(t, "op", conn.username)
	assert.Equal(t, "pw", conn.password)
}

// TestMarkingConfirm_InvalidArgs verifies argument validation fires before any etcd dial.
func TestMarkingConfirm_InvalidArgs(t *testing.T) {
	root := NewRootCommand()
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"marking", "confirm", "abc", "2", "--etcd", "127.0.0.1:1", "--meta-prefix", "wp"})
	err := root.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid logId")
}

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

// TestMarkingDiscovery_LoopbackEndpointIsRefused covers what every real deployment actually
// returns. The LogStore server never connects to etcd — there is no clientv3 reference anywhere
// in server/ or cmd/main.go — so nothing ever overrides the default in its config, and discovery
// hands back a loopback address that belongs to whatever host happens to answer.
//
// Dialing it wastes the full timeout and ends in a raw etcd client dump. Since the value cannot
// be right, the useful outcome is to say so at once and name the flag that fixes it.
func TestMarkingDiscovery_LoopbackEndpointIsRefused(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	// Exactly what a UAT node reports.
	cfg := `{"Etcd":{"Endpoints":["localhost:2379"],"RootPath":"woodpecker",` +
		`"Ssl":{"Enabled":false},"Auth":{"Enabled":false}},` +
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

	_, err = resolveMarkingEtcd(&markingEtcdFlags{})

	require.Error(t, err, "a loopback endpoint discovered from a server that does not use etcd is not usable")
	require.Contains(t, err.Error(), "--etcd", "the message must name the flag that resolves it")
}

// TestMarkingDiscovery_ExplicitFlagBeatsLoopback keeps the refusal from becoming a wall: an
// operator who already knows the address must be able to say so and proceed.
func TestMarkingDiscovery_ExplicitFlagBeatsLoopback(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	cfg := `{"Etcd":{"Endpoints":["localhost:2379"],"RootPath":"woodpecker",` +
		`"Ssl":{"Enabled":false},"Auth":{"Enabled":false}},` +
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

	conn, err := resolveMarkingEtcd(&markingEtcdFlags{etcdEndpoints: "etcd-a:2379"})

	require.NoError(t, err)
	require.Equal(t, []string{"etcd-a:2379"}, conn.endpoints)
}

// TestMarkingDiscovery_NoEndpointDiscoveredIsRefused covers a node that reports no etcd endpoint
// at all. Falling through with an empty list would dial nothing and fail somewhere further in
// with a message about the keyspace rather than about the connection, so the refusal has to
// happen here and has to name the flag.
func TestMarkingDiscovery_NoEndpointDiscoveredIsRefused(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	cfg := `{"Etcd":{"Endpoints":[],"RootPath":"woodpecker",` +
		`"Ssl":{"Enabled":false},"Auth":{"Enabled":false}},` +
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

	_, err = resolveMarkingEtcd(&markingEtcdFlags{})

	require.Error(t, err, "an empty discovered endpoint list is not something to proceed on")
	require.Contains(t, err.Error(), "--etcd", "the message must name the flag that resolves it")
	// Assert the empty-list wording, not just the flag: without the length guard the empty list
	// falls through the loop into the loopback branch, which also names --etcd and would let a
	// broken guard pass this test while reporting "endpoint [] is loopback".
	require.Contains(t, err.Error(), "no etcd endpoint could be discovered")
}

// TestMarkingDiscovery_LoopbackIPIsRefused pins the address form rather than the spelling. The
// default is written as "localhost", but a deployment that resolves or rewrites it hands back
// 127.0.0.1, which is the same unusable address and has to be refused the same way -- matching
// on the literal string alone would let it through.
func TestMarkingDiscovery_LoopbackIPIsRefused(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	cfg := `{"Etcd":{"Endpoints":["127.0.0.1:2379"],"RootPath":"woodpecker",` +
		`"Ssl":{"Enabled":false},"Auth":{"Enabled":false}},` +
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

	_, err = resolveMarkingEtcd(&markingEtcdFlags{})

	require.Error(t, err, "127.0.0.1 is the same unusable address as localhost")
	require.Contains(t, err.Error(), "--etcd", "the message must name the flag that resolves it")
}

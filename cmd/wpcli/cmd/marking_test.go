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

	conn, err := resolveMetaEtcd(&metaEtcdFlags{})
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

	conn, err := resolveMetaEtcd(&metaEtcdFlags{
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

// TestMarkingDiscovery_LoopbackEndpointIsRefused covers what a real deployment reports: the
// server does not connect to etcd, so its config carries the built-in loopback default. That
// address belongs to whichever host answers, so it is refused with the flag that replaces it.
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

	_, err = resolveMetaEtcd(&metaEtcdFlags{})

	require.Error(t, err, "a loopback endpoint discovered from a server that does not use etcd is not usable")
	require.Contains(t, err.Error(), "--etcd", "the message must name the flag that resolves it")
}

// TestMarkingDiscovery_ExplicitFlagBeatsLoopback keeps the refusal from becoming a wall: an
// operator who knows the address states it and proceeds.
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

	conn, err := resolveMetaEtcd(&metaEtcdFlags{etcdEndpoints: "etcd-a:2379"})

	require.NoError(t, err)
	require.Equal(t, []string{"etcd-a:2379"}, conn.endpoints)
}

// TestMarkingDiscovery_NoEndpointDiscoveredIsRefused covers a node that reports no etcd endpoint
// at all. An empty list is refused here, where the message can name the connection and the flag,
// rather than further in where the failure reads as a keyspace problem.
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

	_, err = resolveMetaEtcd(&metaEtcdFlags{})

	require.Error(t, err, "an empty discovered endpoint list is not something to proceed on")
	require.Contains(t, err.Error(), "--etcd", "the message must name the flag that resolves it")
	// Assert the empty-list wording, not just the flag: the loopback branch also names --etcd,
	// so a broken length guard would otherwise pass here while reporting "endpoint [] is loopback".
	require.Contains(t, err.Error(), "no etcd endpoint could be discovered")
}

// TestMarkingDiscovery_LoopbackIPIsRefused pins the address form, not the spelling: 127.0.0.1
// is as unusable as "localhost", and matching the literal string alone would let it through.
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

	_, err = resolveMetaEtcd(&metaEtcdFlags{})

	require.Error(t, err, "127.0.0.1 is the same unusable address as localhost")
	require.Contains(t, err.Error(), "--etcd", "the message must name the flag that resolves it")
}

// TestIsLoopbackEndpoint covers the address forms etcd clientv3 accepts. The scheme-prefixed
// form matters because net.SplitHostPort rejects it ("too many colons"), leaving the whole
// string as the host, which matches neither the name nor an IP.
func TestIsLoopbackEndpoint(t *testing.T) {
	for _, tc := range []struct {
		endpoint string
		want     bool
	}{
		{"localhost:2379", true},
		{"127.0.0.1:2379", true},
		{"127.0.0.1", true},
		{"[::1]:2379", true},
		{"http://localhost:2379", true},
		{"https://127.0.0.1:2379", true},
		{"etcd-a:2379", false},
		{"10.0.0.5:2379", false},
		{"http://etcd-a:2379", false},
	} {
		assert.Equal(t, tc.want, isLoopbackEndpoint(tc.endpoint), "endpoint %q", tc.endpoint)
	}
}

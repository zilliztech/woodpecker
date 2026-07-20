package cmd

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMarkingDiscovery_FromAdminConfig verifies the zero-config path: etcd endpoints and
// the meta prefix are discovered from a node's /admin/config (etcd.rootPath joined with
// woodpecker.meta.prefix), and the key builder lands on the marking keyspace.
func TestMarkingDiscovery_FromAdminConfig(t *testing.T) {
	ml := `{"members":[{"id":"node-1","gossip_addr":"127.0.0.1:17946"}]}`
	cfg := `{"Etcd":{"Endpoints":["etcd-a:2379","etcd-b:2379"],"RootPath":"by-dev"},"Woodpecker":{"Meta":{"Prefix":"woodpecker"}}}`
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

	endpoints, kb, err := resolveMarkingEtcd(&markingEtcdFlags{})
	require.NoError(t, err)
	require.Equal(t, []string{"etcd-a:2379", "etcd-b:2379"}, endpoints)
	require.Equal(t, "by-dev/woodpecker/marking", kb.SegmentCompactedNotifyStatusPrefix())
	require.Equal(t, "by-dev/woodpecker/marking/7/3", kb.BuildSegmentCompactedNotifyStatusKey(7, 3))
}

// TestMarkingDiscovery_FlagOverridesSkipDiscovery verifies --etcd/--meta-prefix work fully
// offline: with both set, no admin endpoint is contacted at all.
func TestMarkingDiscovery_FlagOverridesSkipDiscovery(t *testing.T) {
	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 5 * time.Second}

	endpoints, kb, err := resolveMarkingEtcd(&markingEtcdFlags{
		etcdEndpoints: "e1:2379,e2:2379",
		metaPrefix:    "custom/wp",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"e1:2379", "e2:2379"}, endpoints)
	require.Equal(t, "custom/wp/marking", kb.SegmentCompactedNotifyStatusPrefix())
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

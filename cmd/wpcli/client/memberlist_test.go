package client

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

const memberlistJSONBody = `{
  "members": [
    {
      "id": "node-1",
      "gossip_addr": "10.0.1.1:17946",
      "service_addr": "10.0.1.1:18080",
      "cluster_name": "cluster-a",
      "region": "us-east-1",
      "az": "us-east-1a",
      "rg": "default",
      "state": 0,
      "incarnation": 42,
      "last_seen_ms": 1712553600000,
      "tags": {"role": "logstore"}
    },
    {
      "id": "node-2",
      "gossip_addr": "10.0.1.2:17946",
      "service_addr": "10.0.1.2:18080",
      "az": "us-east-1b",
      "rg": "default",
      "state": 0,
      "incarnation": 17,
      "last_seen_ms": 1712553601000,
      "tags": {"role": "logstore"}
    }
  ]
}`

func TestMemberlistJSON_Parse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/admin/memberlist", r.URL.Path)
		require.Equal(t, "application/json", r.Header.Get("Accept"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(memberlistJSONBody))
	}))
	defer srv.Close()

	c := New(srv.URL, ClientOpts{})
	ml, err := c.GetMemberlist()
	require.NoError(t, err)
	require.Len(t, ml.Members, 2)
	require.Equal(t, "node-1", ml.Members[0].ID)
	require.Equal(t, "cluster-a", ml.Members[0].ClusterName)
	require.Equal(t, "us-east-1", ml.Members[0].Region)
	require.Equal(t, "us-east-1a", ml.Members[0].AZ)
}

func TestResolveNode_ByID(t *testing.T) {
	ml := &Memberlist{
		Members: []Member{
			{ID: "node-1", GossipAddr: "10.0.1.1:17946"},
			{ID: "node-2", GossipAddr: "10.0.1.2:17946"},
		},
	}
	m, ok := ml.Resolve("node-2")
	require.True(t, ok)
	require.Equal(t, "node-2", m.ID)
}

func TestResolveNode_ByHostPrefix(t *testing.T) {
	ml := &Memberlist{
		Members: []Member{
			{ID: "node-1", GossipAddr: "10.0.1.1:17946"},
			{ID: "node-2", GossipAddr: "10.0.1.2:17946"},
		},
	}
	m, ok := ml.Resolve("10.0.1.2")
	require.True(t, ok)
	require.Equal(t, "node-2", m.ID)
}

func TestResolveNode_NotFound(t *testing.T) {
	ml := &Memberlist{Members: []Member{{ID: "node-1"}}}
	_, ok := ml.Resolve("node-99")
	require.False(t, ok)
}

// TestResolve_ExplicitAddressRequiresAPort keeps the escape hatch from swallowing typos. Without
// a port there is nothing to distinguish an address from a mistyped node name, and dialing a
// guess is worse than reporting the target as not found.
func TestResolve_ExplicitAddressRequiresAPort(t *testing.T) {
	ml := &Memberlist{Members: []Member{{ID: "node-1", ServiceAddr: "10.244.1.5:18080"}}}

	m, ok := ml.Resolve("127.0.0.1:9091")
	require.True(t, ok, "an explicit host:port should resolve even when no member carries it")
	require.Equal(t, "127.0.0.1:9091", m.ServiceAddr)
	require.Equal(t, "9091", m.Tags["admin_port"])

	_, ok = ml.Resolve("noed-1")
	require.False(t, ok, "a mistyped node name must stay unresolved rather than being dialed")

	_, ok = ml.Resolve("127.0.0.1:not-a-port")
	require.False(t, ok, "a non-numeric port is not an address")
}

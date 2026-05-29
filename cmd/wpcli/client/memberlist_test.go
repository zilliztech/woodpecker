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

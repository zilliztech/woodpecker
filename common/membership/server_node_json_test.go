package membership

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetMemberlistJSON_Shape(t *testing.T) {
	// Bring up a single-node memberlist (mirrors cluster_test.go pattern).
	node, err := NewServerNode(&ServerConfig{
		NodeID:        "node-test",
		BindPort:      0, // let OS pick
		ServicePort:   18080,
		ResourceGroup: "default",
		AZ:            "us-east-1a",
		Tags:          map[string]string{"role": "logstore"},
	})
	require.NoError(t, err)
	defer node.Shutdown()

	data := node.GetMemberlistJSON()
	require.NotEmpty(t, data)

	// Verify it's valid JSON with the expected top-level shape
	// (matches cmd/wpcli/client/memberlist.go Member struct).
	var parsed struct {
		Members []struct {
			ID          string            `json:"id"`
			GossipAddr  string            `json:"gossip_addr"`
			ServiceAddr string            `json:"service_addr"`
			AZ          string            `json:"az"`
			RG          string            `json:"rg"`
			State       int               `json:"state"`
			Incarnation uint32            `json:"incarnation"`
			LastSeenMS  int64             `json:"last_seen_ms"`
			Tags        map[string]string `json:"tags"`
		} `json:"members"`
	}
	require.NoError(t, json.Unmarshal(data, &parsed))
	require.GreaterOrEqual(t, len(parsed.Members), 1)

	found := false
	for _, m := range parsed.Members {
		if m.ID == "node-test" {
			found = true
			require.Equal(t, "us-east-1a", m.AZ)
			require.Equal(t, "default", m.RG)
			require.NotEmpty(t, m.GossipAddr)
			require.NotEmpty(t, m.ServiceAddr)
			require.NotZero(t, m.LastSeenMS)
			require.Equal(t, "logstore", m.Tags["role"])
			break
		}
	}
	require.True(t, found, "expected node-test in memberlist")
}

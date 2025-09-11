package quorum

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/membership"
	"github.com/zilliztech/woodpecker/proto"
)

// Helper function to create test nodes
func createTestNode(nodeID, rg, az string) *proto.NodeMeta {
	return &proto.NodeMeta{
		NodeId:        nodeID,
		ResourceGroup: rg,
		Az:            az,
		Endpoint:      nodeID + ":8080",
		Tags:          map[string]string{"env": "test"},
		Version:       1,
		LastUpdate:    time.Now().UnixMilli(),
	}
}

// Helper function to create client node with test data
func createTestClientNode(nodeID string, testNodes []*proto.NodeMeta) (*membership.ClientNode, error) {
	config := &membership.ClientConfig{
		NodeID:   nodeID,
		BindAddr: "127.0.0.1",
		BindPort: 0, // Auto-assign port
	}

	clientNode, err := membership.NewClientNode(config)
	if err != nil {
		return nil, err
	}

	// Add test nodes to the discovery
	discovery := clientNode.GetDiscovery()
	for _, node := range testNodes {
		discovery.UpdateServer(node.NodeId, node)
	}

	return clientNode, nil
}

func TestPassiveQuorumDiscovery_SelectQuorumNodes_SingleRegion_Success(t *testing.T) {
	ctx := context.Background()

	// Create test nodes for single region
	testNodes := []*proto.NodeMeta{
		createTestNode("node1", "prod-us", "us-east-1a"),
		createTestNode("node2", "prod-us", "us-east-1a"),
		createTestNode("node3", "prod-us", "us-east-1a"),
	}

	// Create client node with test data
	clientNode, err := createTestClientNode("client1", testNodes)
	assert.NoError(t, err)
	defer clientNode.Leave()

	cfg := &config.QuorumConfig{
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	discovery := &passiveQuorumDiscovery{
		cfg:         cfg,
		clientNodes: []*membership.ClientNode{clientNode},
	}

	result, err := discovery.SelectQuorumNodes(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result.Nodes))
	assert.Equal(t, int32(3), result.Wq) // WriteQuorum = EnsembleSize
	assert.Equal(t, int32(2), result.Aq) // AckQuorum = (WriteQuorum/2) + 1
	assert.Equal(t, int32(3), result.Es)

	// Verify all nodes are from our test data
	for _, nodeEndpoint := range result.Nodes {
		found := false
		for _, testNode := range testNodes {
			if testNode.Endpoint == nodeEndpoint {
				found = true
				break
			}
		}
		assert.True(t, found, "Selected node %s should be from test data", nodeEndpoint)
	}
}

func TestPassiveQuorumDiscovery_SelectQuorumNodes_CrossRegion_Success(t *testing.T) {
	ctx := context.Background()

	// Create test nodes for multiple regions
	testNodes1 := []*proto.NodeMeta{
		createTestNode("node1", "prod-us", "us-east-1a"),
		createTestNode("node2", "prod-us", "us-east-1a"),
		createTestNode("node3", "prod-us", "us-east-1a"),
	}
	testNodes2 := []*proto.NodeMeta{
		createTestNode("node4", "prod-eu", "eu-west-1a"),
		createTestNode("node5", "prod-eu", "eu-west-1a"),
		createTestNode("node6", "prod-eu", "eu-west-1a"),
	}

	// Create client nodes with test data
	clientNode1, err := createTestClientNode("client1", testNodes1)
	assert.NoError(t, err)
	defer clientNode1.Leave()

	clientNode2, err := createTestClientNode("client2", testNodes2)
	assert.NoError(t, err)
	defer clientNode2.Leave()

	cfg := &config.QuorumConfig{
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "cross-region",
			AffinityMode: "soft",
			Replicas:     5, // This will result in EnsembleSize=5
		},
	}

	discovery := &passiveQuorumDiscovery{
		cfg:         cfg,
		clientNodes: []*membership.ClientNode{clientNode1, clientNode2},
	}

	result, err := discovery.SelectQuorumNodes(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 5, len(result.Nodes))
	assert.Equal(t, int32(5), result.Wq)
	assert.Equal(t, int32(3), result.Aq) // (5/2) + 1 = 3
	assert.Equal(t, int32(5), result.Es)
}

func TestPassiveQuorumDiscovery_SelectQuorumNodes_CustomPlacement_Success(t *testing.T) {
	ctx := context.Background()

	// Create test nodes for custom placement
	testNodes1 := []*proto.NodeMeta{
		createTestNode("node1", "rg-1", "az-1"),
		createTestNode("node2", "rg-1", "az-1"),
	}
	testNodes2 := []*proto.NodeMeta{
		createTestNode("node3", "rg-2", "az-2"),
		createTestNode("node4", "rg-2", "az-2"),
	}
	testNodes3 := []*proto.NodeMeta{
		createTestNode("node5", "rg-3", "az-3"),
		createTestNode("node6", "rg-3", "az-3"),
	}

	// Create client nodes with test data
	clientNode1, err := createTestClientNode("client1", testNodes1)
	assert.NoError(t, err)
	defer clientNode1.Leave()

	clientNode2, err := createTestClientNode("client2", testNodes2)
	assert.NoError(t, err)
	defer clientNode2.Leave()

	clientNode3, err := createTestClientNode("client3", testNodes3)
	assert.NoError(t, err)
	defer clientNode3.Leave()

	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed-a:8080"}},
			{Name: "region-b", Seeds: []string{"seed-b:8080"}},
			{Name: "region-c", Seeds: []string{"seed-c:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "custom",
			AffinityMode: "hard",
			Replicas:     3, // This will result in EnsembleSize=3
			CustomPlacement: []config.CustomPlacement{
				{Region: "region-a", Az: "az-1", ResourceGroup: "rg-1"},
				{Region: "region-b", Az: "az-2", ResourceGroup: "rg-2"},
				{Region: "region-c", Az: "az-3", ResourceGroup: "rg-3"}, // Add third placement
			},
		},
	}

	discovery := &passiveQuorumDiscovery{
		cfg:         cfg,
		clientNodes: []*membership.ClientNode{clientNode1, clientNode2, clientNode3},
	}

	result, err := discovery.SelectQuorumNodes(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result.Nodes))
	assert.Equal(t, int32(3), result.Wq)
	assert.Equal(t, int32(2), result.Aq)
	assert.Equal(t, int32(3), result.Es)
}

func TestPassiveQuorumDiscovery_SelectQuorumNodes_CustomPlacement_ValidationError(t *testing.T) {
	ctx := context.Background()

	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed-a:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "custom",
			AffinityMode: "hard",
			Replicas:     3, // Mismatch: 3 required but only 1 custom placement rule
			CustomPlacement: []config.CustomPlacement{
				{Region: "region-a", Az: "az-1", ResourceGroup: "rg-1"},
			},
		},
	}

	discovery := &passiveQuorumDiscovery{
		cfg:         cfg,
		clientNodes: []*membership.ClientNode{},
	}

	result, err := discovery.SelectQuorumNodes(ctx)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "custom placement rules count (1) must equal required nodes count (3)")
}

func TestPassiveQuorumDiscovery_SelectQuorumNodes_NoClientNodes(t *testing.T) {
	ctx := context.Background()

	cfg := &config.QuorumConfig{
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	discovery := &passiveQuorumDiscovery{
		cfg:         cfg,
		clientNodes: []*membership.ClientNode{}, // No client nodes
	}

	result, err := discovery.SelectQuorumNodes(ctx)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "no client nodes available")
}

func TestPassiveQuorumDiscovery_SelectQuorumNodes_ServiceDiscoveryTimeout(t *testing.T) {
	// Test timeout behavior when service discovery continuously fails
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Create test node with empty discovery (no nodes added)
	clientNode, err := createTestClientNode("client1", []*proto.NodeMeta{})
	assert.NoError(t, err)
	defer clientNode.Leave()

	cfg := &config.QuorumConfig{
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	discovery := &passiveQuorumDiscovery{
		cfg:         cfg,
		clientNodes: []*membership.ClientNode{clientNode},
	}

	result, err := discovery.SelectQuorumNodes(ctx)

	assert.Error(t, err)
	assert.Nil(t, result)
	// Should eventually timeout due to retry mechanism when no nodes are available
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "context deadline exceeded") || strings.Contains(err.Error(), "no nodes returned"))
}

func TestPassiveQuorumDiscovery_Close_Success(t *testing.T) {
	ctx := context.Background()

	// Create test client nodes
	clientNode1, err := createTestClientNode("client1", []*proto.NodeMeta{
		createTestNode("node1", "prod-us", "us-east-1a"),
	})
	assert.NoError(t, err)

	clientNode2, err := createTestClientNode("client2", []*proto.NodeMeta{
		createTestNode("node2", "prod-eu", "eu-west-1a"),
	})
	assert.NoError(t, err)

	discovery := &passiveQuorumDiscovery{
		cfg:         &config.QuorumConfig{},
		clientNodes: []*membership.ClientNode{clientNode1, clientNode2},
	}

	err = discovery.Close(ctx)
	assert.NoError(t, err)
}

func TestPassiveQuorumDiscovery_Close_NilNodes(t *testing.T) {
	ctx := context.Background()

	discovery := &passiveQuorumDiscovery{
		cfg:         &config.QuorumConfig{},
		clientNodes: []*membership.ClientNode{nil, nil}, // nil nodes
	}

	err := discovery.Close(ctx)
	assert.NoError(t, err) // Should handle nil nodes gracefully
}

func TestPassiveQuorumDiscovery_StrategyTypeMapping(t *testing.T) {
	ctx := context.Background()

	// Create test nodes
	testNodes := []*proto.NodeMeta{
		createTestNode("node1", "prod-us", "us-east-1a"),
		createTestNode("node2", "prod-us", "us-east-1b"),
		createTestNode("node3", "prod-us", "us-east-1a"),
	}

	// Create client node with test data
	clientNode, err := createTestClientNode("client1", testNodes)
	assert.NoError(t, err)
	defer clientNode.Leave()

	tests := []struct {
		name     string
		strategy string
	}{
		{"single-az-single-rg", "single-az-single-rg"},
		{"single-az-multi-rg", "single-az-multi-rg"},
		{"multi-az-single-rg", "multi-az-single-rg"},
		{"multi-az-multi-rg", "multi-az-multi-rg"},
		{"random", "random"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.QuorumConfig{
				SelectStrategy: config.QuorumSelectStrategy{
					Strategy:     tt.strategy,
					AffinityMode: "soft",
					Replicas:     1, // Request only 1 node for simplicity
				},
			}

			discovery := &passiveQuorumDiscovery{
				cfg:         cfg,
				clientNodes: []*membership.ClientNode{clientNode},
			}

			result, err := discovery.SelectQuorumNodes(ctx)

			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, 1, len(result.Nodes))
		})
	}
}

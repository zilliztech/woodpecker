// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integration

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker/client"
	"github.com/zilliztech/woodpecker/woodpecker/quorum"
)

func TestQuorumDiscoveryIntegration_RandomStrategy(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestQuorumDiscoveryIntegration_RandomStrategy")

	// Start a 5-node cluster
	const nodeCount = 5
	cluster, _, _, serviceSeeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	defer func() {
		cluster.StopMultiNodeCluster(t)
	}()

	// Wait for cluster to be ready
	time.Sleep(3 * time.Second)

	ctx := context.Background()

	// Create QuorumDiscovery with random strategy
	quorumConfig := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{
				Name:  "region-a",
				Seeds: serviceSeeds,
			},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     3, // Select 3 nodes out of 5
		},
	}

	clientPool := client.NewLogStoreClientPool(268435456, 536870912)
	defer clientPool.Close(ctx)

	discovery, err := quorum.NewQuorumDiscovery(ctx, quorumConfig, clientPool)
	assert.NoError(t, err)
	assert.NotNil(t, discovery)
	defer discovery.Close(ctx)

	// Test multiple selections to verify randomness and consistency
	for i := 0; i < 3; i++ {
		t.Logf("=== Selection Round %d ===", i+1)

		result, err := discovery.SelectQuorum(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Verify basic properties
		assert.Equal(t, 3, len(result.Nodes), "Should select exactly 3 nodes")
		assert.Equal(t, int32(3), result.Es, "Ensemble size should be 3")
		assert.Equal(t, int32(3), result.Wq, "Write quorum should be 3")
		assert.Equal(t, int32(2), result.Aq, "Ack quorum should be 2")

		// Verify all selected nodes have valid addresses (port should match one of the service ports)
		servicePorts := extractPorts(serviceSeeds)
		for j, node := range result.Nodes {
			selectedPort := extractPort(node)
			assert.Contains(t, servicePorts, selectedPort, "Selected node %d port should match one of the service ports", j)
		}

		// Verify no duplicates
		uniqueNodes := make(map[string]bool)
		for _, node := range result.Nodes {
			assert.False(t, uniqueNodes[node], "Node %s should not be selected twice", node)
			uniqueNodes[node] = true
		}

		t.Logf("Selected nodes: %v", result.Nodes)
	}
}

func TestQuorumDiscoveryIntegration_SingleAZSingleRGStrategy(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestQuorumDiscoveryIntegration_SingleAZSingleRGStrategy")

	// Start a 4-node cluster
	const nodeCount = 4
	cluster, _, _, serviceSeeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	defer func() {
		cluster.StopMultiNodeCluster(t)
	}()

	// Wait for cluster to be ready
	time.Sleep(3 * time.Second)

	ctx := context.Background()

	// Create QuorumDiscovery with single-az-single-rg strategy
	quorumConfig := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{
				Name:  "region-a",
				Seeds: serviceSeeds,
			},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "single-az-single-rg",
			AffinityMode: "soft",
			Replicas:     3, // Select 3 nodes out of 4
		},
	}

	clientPool := client.NewLogStoreClientPool(268435456, 536870912)
	defer clientPool.Close(ctx)

	discovery, err := quorum.NewQuorumDiscovery(ctx, quorumConfig, clientPool)
	assert.NoError(t, err)
	assert.NotNil(t, discovery)
	defer discovery.Close(ctx)

	// Test the selection
	result, err := discovery.SelectQuorum(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify basic properties
	assert.Equal(t, 3, len(result.Nodes), "Should select exactly 3 nodes")
	assert.Equal(t, int32(3), result.Es, "Ensemble size should be 3")
	assert.Equal(t, int32(3), result.Wq, "Write quorum should be 3")
	assert.Equal(t, int32(2), result.Aq, "Ack quorum should be 2")

	// Verify all selected nodes have valid addresses (port should match one of the service ports)
	servicePorts := extractPorts(serviceSeeds)
	for j, node := range result.Nodes {
		selectedPort := extractPort(node)
		assert.Contains(t, servicePorts, selectedPort, "Selected node %d port should match one of the service ports", j)
	}

	t.Logf("Selected nodes for single-az-single-rg: %v", result.Nodes)
}

func TestQuorumDiscoveryIntegration_CrossRegionStrategy(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestQuorumDiscoveryIntegration_CrossRegionStrategy")

	// Start two 3-node clusters representing different regions
	const nodesPerRegion = 3

	// Start first region cluster
	cluster1, _, _, serviceSeeds1 := utils.StartMiniCluster(t, nodesPerRegion, rootPath+"_region1")
	defer func() {
		cluster1.StopMultiNodeCluster(t)
	}()

	// Start second region cluster
	cluster2, _, _, serviceSeeds2 := utils.StartMiniCluster(t, nodesPerRegion, rootPath+"_region2")
	defer func() {
		cluster2.StopMultiNodeCluster(t)
	}()

	// Wait for clusters to be ready
	time.Sleep(3 * time.Second)

	ctx := context.Background()

	// Create QuorumDiscovery with cross-region strategy
	quorumConfig := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{
				Name:  "region-a",
				Seeds: serviceSeeds1,
			},
			{
				Name:  "region-b",
				Seeds: serviceSeeds2,
			},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "cross-region",
			AffinityMode: "soft",
			Replicas:     3, // Select 3 nodes total across regions (only 3 and 5 are supported)
		},
	}

	clientPool := client.NewLogStoreClientPool(268435456, 536870912)
	defer clientPool.Close(ctx)

	discovery, err := quorum.NewQuorumDiscovery(ctx, quorumConfig, clientPool)
	assert.NoError(t, err)
	assert.NotNil(t, discovery)
	defer discovery.Close(ctx)

	// Test the selection
	result, err := discovery.SelectQuorum(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify basic properties - in real cross-region scenario, we might get fewer nodes if regions are isolated
	assert.GreaterOrEqual(t, len(result.Nodes), 2, "Should select at least 2 nodes")
	assert.LessOrEqual(t, len(result.Nodes), 3, "Should not select more than 3 nodes")

	// The quorum values should match what's configured (Replicas: 3 means Es=3, Wq=3, Aq=2)
	expectedEs := int32(3)
	expectedWq := int32(3)
	expectedAq := int32(2)
	assert.Equal(t, expectedEs, result.Es, "Ensemble size should match configured value")
	assert.Equal(t, expectedWq, result.Wq, "Write quorum should match configured value")
	assert.Equal(t, expectedAq, result.Aq, "Ack quorum should match configured value")

	// Verify nodes are distributed across regions by checking ports
	servicePorts1 := extractPorts(serviceSeeds1)
	servicePorts2 := extractPorts(serviceSeeds2)
	region1Nodes := 0
	region2Nodes := 0
	for _, node := range result.Nodes {
		selectedPort := extractPort(node)
		if contains(servicePorts1, selectedPort) {
			region1Nodes++
		} else if contains(servicePorts2, selectedPort) {
			region2Nodes++
		} else {
			t.Errorf("Selected node %s port %s is not from either region", node, selectedPort)
		}
	}

	// Should have nodes from at least one region (preferably both if network allows)
	assert.Greater(t, region1Nodes+region2Nodes, 0, "Should have nodes from at least one region")
	assert.Equal(t, len(result.Nodes), region1Nodes+region2Nodes, "Total nodes should equal selected nodes")

	// Log the distribution for manual verification
	if region1Nodes > 0 && region2Nodes > 0 {
		t.Logf("SUCCESS: Nodes distributed across both regions")
	} else {
		t.Logf("NOTE: Nodes only from one region (expected in isolated clusters)")
	}

	t.Logf("Selected nodes for cross-region: %v", result.Nodes)
	t.Logf("Region 1 nodes: %d, Region 2 nodes: %d", region1Nodes, region2Nodes)
}

func TestQuorumDiscoveryIntegration_CustomPlacementStrategy(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestQuorumDiscoveryIntegration_CustomPlacementStrategy")

	// Start a single cluster with nodes having different AZ/RG configurations
	nodeConfigs := []utils.NodeConfig{
		{Index: 0, ResourceGroup: "rg-1", AZ: "az-1"}, // For region-a
		{Index: 1, ResourceGroup: "rg-2", AZ: "az-2"}, // For region-b
		{Index: 2, ResourceGroup: "rg-3", AZ: "az-3"}, // For region-c
		{Index: 3, ResourceGroup: "rg-1", AZ: "az-1"}, // Additional node for region-a
		{Index: 4, ResourceGroup: "rg-2", AZ: "az-2"}, // Additional node for region-b
	}

	cluster, _, _, serviceSeeds := utils.StartMiniClusterWithCustomNodes(t, nodeConfigs, rootPath)
	defer func() {
		cluster.StopMultiNodeCluster(t)
	}()

	// Wait for cluster to be ready
	time.Sleep(3 * time.Second)

	ctx := context.Background()

	// Create QuorumDiscovery with custom placement strategy
	quorumConfig := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{
				Name:  "region-a", // All nodes are in the same region for simplicity
				Seeds: serviceSeeds,
			},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "custom",
			AffinityMode: "hard",
			Replicas:     3, // Must match number of custom placement rules
			CustomPlacement: []config.CustomPlacement{
				{Region: "region-a", Az: "az-1", ResourceGroup: "rg-1"},
				{Region: "region-a", Az: "az-2", ResourceGroup: "rg-2"},
				{Region: "region-a", Az: "az-3", ResourceGroup: "rg-3"},
			},
		},
	}

	clientPool := client.NewLogStoreClientPool(268435456, 536870912)
	defer clientPool.Close(ctx)

	discovery, err := quorum.NewQuorumDiscovery(ctx, quorumConfig, clientPool)
	assert.NoError(t, err)
	assert.NotNil(t, discovery)
	defer discovery.Close(ctx)

	// Test the selection
	result, err := discovery.SelectQuorum(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify basic properties
	assert.Equal(t, 3, len(result.Nodes), "Should select exactly 3 nodes")
	assert.Equal(t, int32(3), result.Es, "Ensemble size should be 3")
	assert.Equal(t, int32(3), result.Wq, "Write quorum should be 3")
	assert.Equal(t, int32(2), result.Aq, "Ack quorum should be 2")

	// Verify exactly one node from each AZ/RG combination
	servicePorts := extractPorts(serviceSeeds)

	for _, node := range result.Nodes {
		selectedPort := extractPort(node)
		assert.Contains(t, servicePorts, selectedPort, "Selected node port should be from our cluster")

		// We can't directly verify AZ from the port, but we can verify we have 3 unique nodes
		// and that custom placement strategy worked (returning exactly 3 nodes from different placements)
	}

	// The custom placement strategy should return exactly one node per placement rule
	// Since we have 3 placement rules, we should get exactly 3 nodes
	assert.Equal(t, 3, len(result.Nodes), "Custom placement should select exactly 3 nodes (one per rule)")

	// Verify all nodes are unique
	uniqueNodes := make(map[string]bool)
	for _, node := range result.Nodes {
		assert.False(t, uniqueNodes[node], "Each selected node should be unique")
		uniqueNodes[node] = true
	}

	t.Logf("Selected nodes for custom placement: %v", result.Nodes)
}

func TestQuorumDiscoveryIntegration_InsufficientNodes(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestQuorumDiscoveryIntegration_InsufficientNodes")

	// Start a 2-node cluster but try to select 5 nodes
	const nodeCount = 2
	cluster, _, _, serviceSeeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	defer func() {
		cluster.StopMultiNodeCluster(t)
	}()

	// Wait for cluster to be ready
	time.Sleep(3 * time.Second)

	ctx := context.Background()

	// Create QuorumDiscovery with soft affinity mode
	quorumConfig := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{
				Name:  "region-a",
				Seeds: serviceSeeds,
			},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft", // Should still err with fewer nodes in soft mode, soft mode only used to restrict location, not for replicas
			Replicas:     5,      // Request more nodes than available
		},
	}

	clientPool := client.NewLogStoreClientPool(268435456, 536870912)
	defer clientPool.Close(ctx)

	discovery, err := quorum.NewQuorumDiscovery(ctx, quorumConfig, clientPool)
	assert.NoError(t, err)
	assert.NotNil(t, discovery)
	defer discovery.Close(ctx)

	// Test the selection - should succeed in soft mode
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := discovery.SelectQuorum(timeoutCtx)
	assert.Error(t, err, "Should fail in soft mode with insufficient nodes")
	assert.True(t, strings.Contains(err.Error(), "insufficient nodes"), "should return last retry error when timeout, which msg contains 'insufficient nodes'")
	assert.Nil(t, result)
}

func TestQuorumDiscoveryIntegration_NodeFailureRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestQuorumDiscoveryIntegration_NodeFailureRecovery")

	// Start a 5-node cluster
	const nodeCount = 5
	cluster, _, gossipSeeds, serviceSeeds := utils.StartMiniCluster(t, nodeCount, rootPath)
	defer func() {
		cluster.StopMultiNodeCluster(t)
	}()

	// Wait for cluster to be ready
	time.Sleep(3 * time.Second)

	ctx := context.Background()

	// Create QuorumDiscovery
	quorumConfig := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{
				Name:  "region-a",
				Seeds: serviceSeeds,
			},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	clientPool := client.NewLogStoreClientPool(268435456, 536870912)
	defer clientPool.Close(ctx)

	discovery, err := quorum.NewQuorumDiscovery(ctx, quorumConfig, clientPool)
	assert.NoError(t, err)
	assert.NotNil(t, discovery)
	defer discovery.Close(ctx)

	// Test selection with all nodes available
	result1, err := discovery.SelectQuorum(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result1)
	assert.Equal(t, 3, len(result1.Nodes))

	t.Logf("Initial selection: %v", result1.Nodes)

	// Stop one node
	stoppedNodeIndex, err := cluster.LeaveRandomNode(t)
	assert.NoError(t, err)
	t.Logf("Stopped node %d", stoppedNodeIndex)

	// Wait for failure detection
	time.Sleep(2 * time.Second)

	// Test selection after node failure - should still work
	result2, err := discovery.SelectQuorum(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result2)
	assert.Equal(t, 3, len(result2.Nodes))

	t.Logf("Selection after node failure: %v", result2.Nodes)

	// Restart the stopped node
	_, err = cluster.RestartNode(t, stoppedNodeIndex, gossipSeeds)
	assert.NoError(t, err)
	t.Logf("Restarted node %d", stoppedNodeIndex)

	// Wait for node to rejoin
	time.Sleep(2 * time.Second)

	// Test selection after node recovery
	result3, err := discovery.SelectQuorum(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result3)
	assert.Equal(t, 3, len(result3.Nodes))

	t.Logf("Selection after node recovery: %v", result3.Nodes)
}

// Helper function to check if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// Helper function to extract port from address string (format: "ip:port")
func extractPort(address string) string {
	parts := strings.Split(address, ":")
	if len(parts) >= 2 {
		return parts[len(parts)-1] // Return the last part as port
	}
	return ""
}

// Helper function to extract all ports from a slice of addresses
func extractPorts(addresses []string) []string {
	ports := make([]string, len(addresses))
	for i, addr := range addresses {
		ports[i] = extractPort(addr)
	}
	return ports
}

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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/membership"
	"github.com/zilliztech/woodpecker/tests/utils"
)

func TestMiniCluster_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMiniCluster_Basic")

	// Start a 3-node cluster
	const nodeCount = 3
	cluster, _, seeds, _ := utils.StartMiniCluster(t, nodeCount, rootPath)

	t.Logf("Started cluster with %d nodes", len(cluster.Servers))
	t.Logf("Seeds: %v", seeds)

	// Wait for cluster to stabilize
	time.Sleep(3 * time.Second)

	// Create a client node to observe the cluster
	clientConfig := &membership.ClientConfig{
		NodeID:   "test-client-basic",
		BindAddr: "127.0.0.1",
		BindPort: 0, // Use random port
	}

	clientNode, err := membership.NewClientNode(clientConfig)
	assert.NoError(t, err)
	defer func() {
		_ = clientNode.Leave()
		_ = clientNode.Shutdown()
	}()

	// Join the cluster to observe
	err = clientNode.Join(seeds)
	assert.NoError(t, err)

	// Wait for membership information to propagate
	time.Sleep(2 * time.Second)

	// Verify all nodes are discovered
	discovery := clientNode.GetDiscovery()
	allServers := discovery.GetAllServers()

	t.Logf("Discovered %d servers", len(allServers))
	for _, server := range allServers {
		t.Logf("  Server: %s -> %s (AZ: %s)", server.NodeId, server.Endpoint, server.Az)
	}

	// Should discover all 3 nodes
	assert.Equal(t, nodeCount, len(allServers), "Should discover all nodes in the cluster")

	// Print client view
	clientNode.PrintStatus()

	// Test cluster shutdown
	t.Logf("Stopping cluster...")
	cluster.StopMultiNodeCluster(t)

	// Wait for nodes to shutdown
	time.Sleep(2 * time.Second)

	// Verify active nodes count
	activeNodes := clientNode.GetDiscovery().GetAllServers()
	assert.Equal(t, 0, len(activeNodes), "All nodes should be stopped")

	t.Logf("TestMiniCluster_Basic completed successfully")
}

func TestMiniCluster_LastScale(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMiniCluster_Basic")

	// Start a 50-node cluster
	const nodeCount = 50
	cluster, _, seeds, _ := utils.StartMiniCluster(t, nodeCount, rootPath)

	t.Logf("Started cluster with %d nodes", len(cluster.Servers))
	t.Logf("Seeds: %v", seeds)

	// Wait for cluster to stabilize
	time.Sleep(3 * time.Second)

	// Create a client node to observe the cluster
	clientConfig := &membership.ClientConfig{
		NodeID:   "test-client-basic",
		BindAddr: "127.0.0.1",
		BindPort: 0, // Use random port
	}

	clientNode, err := membership.NewClientNode(clientConfig)
	assert.NoError(t, err)
	defer func() {
		_ = clientNode.Leave()
		_ = clientNode.Shutdown()
	}()

	// Join the cluster to observe
	err = clientNode.Join(seeds)
	assert.NoError(t, err)

	// Wait for membership information to propagate
	time.Sleep(2 * time.Second)

	// Verify all nodes are discovered
	discovery := clientNode.GetDiscovery()
	allServers := discovery.GetAllServers()

	t.Logf("Discovered %d servers", len(allServers))
	for _, server := range allServers {
		t.Logf("  Server: %s -> %s (AZ: %s)", server.NodeId, server.Endpoint, server.Az)
	}

	// Should discover all 3 nodes
	assert.Equal(t, nodeCount, len(allServers), "Should discover all nodes in the cluster")

	// Print client view
	clientNode.PrintStatus()

	// Test cluster shutdown
	t.Logf("Stopping cluster...")
	cluster.StopMultiNodeCluster(t)

	// Wait for nodes to shutdown
	time.Sleep(2 * time.Second)

	// Verify active nodes count
	activeNodes := clientNode.GetDiscovery().GetAllServers()
	assert.Equal(t, 0, len(activeNodes), "All nodes should be stopped", activeNodes)

	t.Logf("TestMiniCluster_Basic completed successfully")
}

func TestMiniCluster_Join(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMiniCluster_Join")

	// Start a 2-node cluster initially
	const initialNodeCount = 2
	cluster, _, seeds, _ := utils.StartMiniCluster(t, initialNodeCount, rootPath)
	defer cluster.StopMultiNodeCluster(t)

	t.Logf("Started initial cluster with %d nodes", len(cluster.Servers))

	// Wait for cluster to stabilize
	time.Sleep(3 * time.Second)

	// Create a client node to observe the cluster
	clientConfig := &membership.ClientConfig{
		NodeID:   "test-client-join",
		BindAddr: "127.0.0.1",
		BindPort: 0,
	}

	clientNode, err := membership.NewClientNode(clientConfig)
	assert.NoError(t, err)
	defer func() {
		_ = clientNode.Leave()
		_ = clientNode.Shutdown()
	}()

	// Join the cluster to observe
	err = clientNode.Join(seeds)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)

	// Verify initial node count
	discovery := clientNode.GetDiscovery()
	initialServers := discovery.GetAllServers()
	assert.Equal(t, initialNodeCount, len(initialServers), "Should discover initial nodes")
	t.Logf("Initial cluster has %d servers", len(initialServers))

	// Now join a new node to the cluster
	t.Logf("Joining new node to the cluster...")
	currentSeeds := cluster.GetSeedList() // Get current active seeds
	newNodeIndex, newNodeAddr, err := cluster.JoinNewNode(t, currentSeeds)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, newNodeIndex, 0, "New node index should be valid")
	assert.NotEmpty(t, newNodeAddr, "New node address should not be empty")

	// Wait for the new node to join and propagate
	time.Sleep(3 * time.Second)

	// Verify the new node is discovered
	updatedServers := discovery.GetAllServers()
	expectedCount := initialNodeCount + 1
	assert.Equal(t, expectedCount, len(updatedServers), "Should discover the new joined node")
	t.Logf("After join: cluster has %d servers", len(updatedServers))

	// Verify active nodes count
	activeNodes := clientNode.GetDiscovery().GetAllServers()
	assert.Equal(t, expectedCount, len(activeNodes), "Should have correct number of active nodes")

	// Print updated client view
	clientNode.PrintStatus()

	t.Logf("TestMiniCluster_Join completed successfully - joined node %d at %s", newNodeIndex, newNodeAddr)
}

func TestMiniCluster_Leave(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMiniCluster_Leave")

	// Start a 3-node cluster
	const nodeCount = 3
	cluster, _, seeds, _ := utils.StartMiniCluster(t, nodeCount, rootPath)
	defer cluster.StopMultiNodeCluster(t)

	t.Logf("Started cluster with %d nodes", len(cluster.Servers))

	// Wait for cluster to stabilize
	time.Sleep(3 * time.Second)

	// Create a client node to observe the cluster
	clientConfig := &membership.ClientConfig{
		NodeID:   "test-client-leave",
		BindAddr: "127.0.0.1",
		BindPort: 0,
	}

	clientNode, err := membership.NewClientNode(clientConfig)
	assert.NoError(t, err)
	defer func() {
		_ = clientNode.Leave()
		_ = clientNode.Shutdown()
	}()

	// Join the cluster to observe
	err = clientNode.Join(seeds)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)

	// Verify initial node count
	discovery := clientNode.GetDiscovery()
	initialServers := discovery.GetAllServers()
	assert.Equal(t, nodeCount, len(initialServers), "Should discover all initial nodes")
	t.Logf("Initial cluster has %d servers", len(initialServers))

	// Print initial status
	clientNode.PrintStatus()

	// Make one node leave the cluster
	t.Logf("Making a node leave the cluster...")
	leftNodeIndex, err := cluster.LeaveNode(t)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, leftNodeIndex, 0, "Left node index should be valid")

	// Wait for leave to propagate
	time.Sleep(3 * time.Second)

	// Verify one less node is active
	activeNodes := clientNode.GetDiscovery().GetAllServers()
	expectedActiveNodes := nodeCount - 1
	assert.Equal(t, expectedActiveNodes, len(activeNodes), "Should have one less active node")

	// Note: The gossip protocol might take time to detect the left node
	// So we check if the discovery eventually reflects the change
	t.Logf("Active nodes after leave: %d (expected: %d)", len(activeNodes), expectedActiveNodes)

	// Print updated status
	clientNode.PrintStatus()

	t.Logf("TestMiniCluster_Leave completed successfully - node %d left", leftNodeIndex)
}

func TestMiniCluster_Restart(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMiniCluster_Restart")

	// Start a 3-node cluster
	const nodeCount = 3
	cluster, _, seeds, _ := utils.StartMiniCluster(t, nodeCount, rootPath)
	defer cluster.StopMultiNodeCluster(t)

	t.Logf("Started cluster with %d nodes", len(cluster.Servers))

	// Wait for cluster to stabilize
	time.Sleep(3 * time.Second)

	// Create a client node to observe the cluster
	clientConfig := &membership.ClientConfig{
		NodeID:   "test-client-restart",
		BindAddr: "127.0.0.1",
		BindPort: 0,
	}

	clientNode, err := membership.NewClientNode(clientConfig)
	assert.NoError(t, err)
	defer func() {
		_ = clientNode.Leave()
		_ = clientNode.Shutdown()
	}()

	// Join the cluster to observe
	err = clientNode.Join(seeds)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)

	// Verify initial node count
	discovery := clientNode.GetDiscovery()
	initialServers := discovery.GetAllServers()
	assert.Equal(t, nodeCount, len(initialServers), "Should discover all initial nodes")
	t.Logf("Initial cluster has %d servers", len(initialServers))

	// Print initial status
	clientNode.PrintStatus()

	// Stop one node (simulate crash)
	t.Logf("Stopping a node to simulate restart...")
	stoppedNodeIndex, err := cluster.LeaveNode(t)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, stoppedNodeIndex, 0, "Stopped node index should be valid")

	// Wait for stop to propagate
	time.Sleep(2 * time.Second)

	// Verify one less node is active
	activeNodes := clientNode.GetDiscovery().GetAllServers()
	expectedActiveNodes := nodeCount - 1
	assert.Equal(t, expectedActiveNodes, len(activeNodes), "Should have one less active node after stop")
	t.Logf("Active nodes after stop: %d", len(activeNodes))

	// Now restart the stopped node
	t.Logf("Restarting node %d...", stoppedNodeIndex)
	currentSeeds := cluster.GetSeedList() // Get current active seeds for restart
	restartedNodeAddr, err := cluster.RestartNode(t, stoppedNodeIndex, currentSeeds)
	assert.NoError(t, err)
	assert.NotEmpty(t, restartedNodeAddr, "Restarted node address should not be empty")

	// Wait for restart to complete and propagate
	time.Sleep(3 * time.Second)

	// Verify all nodes are active again
	finalActiveNodes := clientNode.GetDiscovery().GetAllServers()
	assert.Equal(t, nodeCount, len(finalActiveNodes), "Should have all nodes active after restart")
	t.Logf("Active nodes after restart: %d", len(finalActiveNodes))

	// Note: The gossip protocol might take time to detect the restarted node
	// In a real deployment, this would be more reliable

	// Print final status
	clientNode.PrintStatus()

	t.Logf("TestMiniCluster_Restart completed successfully - node %d restarted at %s", stoppedNodeIndex, restartedNodeAddr)
}

func TestMiniCluster_NodeIndexManagement(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMiniCluster_NodeIndexManagement")

	// Start a 3-node cluster (should have nodeIndex 0, 1, 2)
	const initialNodeCount = 3
	cluster, _, seeds, _ := utils.StartMiniCluster(t, initialNodeCount, rootPath)
	defer cluster.StopMultiNodeCluster(t)

	t.Logf("Started initial cluster with %d nodes", len(cluster.Servers))

	// Wait for cluster to stabilize
	time.Sleep(3 * time.Second)

	// Create a client node to observe the cluster
	clientConfig := &membership.ClientConfig{
		NodeID:   "test-client-node-mgmt",
		BindAddr: "127.0.0.1",
		BindPort: 0,
	}

	clientNode, err := membership.NewClientNode(clientConfig)
	assert.NoError(t, err)
	defer func() {
		_ = clientNode.Leave()
		_ = clientNode.Shutdown()
	}()

	// Join the cluster to observe
	err = clientNode.Join(seeds)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)

	// Verify initial state - should have nodes 0, 1, 2
	activeIndexes := cluster.GetActiveNodeIndexes()
	assert.Equal(t, []int{0, 1, 2}, activeIndexes, "Should have nodes 0, 1, 2 initially")
	t.Logf("Initial active nodes: %v", activeIndexes)

	// Leave node 1 specifically
	t.Logf("Leaving node 1...")
	leftNodeIndex, err := cluster.LeaveNodeWithIndex(t, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, leftNodeIndex, "Should have left node 1")
	time.Sleep(2 * time.Second)

	// Verify node 1 is gone - should have nodes 0, 2
	activeIndexes = cluster.GetActiveNodeIndexes()
	expectedActive := []int{0, 2}
	assert.ElementsMatch(t, expectedActive, activeIndexes, "Should have nodes 0, 2 after leaving node 1")
	t.Logf("Active nodes after leaving node 1: %v", activeIndexes)

	// Join a new node with auto-increment (should get nodeIndex 3)
	t.Logf("Joining new node with auto-increment...")
	currentSeeds := cluster.GetSeedList()
	newNodeIndex, newNodeAddr, err := cluster.JoinNewNode(t, currentSeeds)
	assert.NoError(t, err)
	assert.Equal(t, 3, newNodeIndex, "New node should get nodeIndex 3")
	assert.NotEmpty(t, newNodeAddr, "New node address should not be empty")
	time.Sleep(2 * time.Second)

	// Verify we now have nodes 0, 2, 3
	activeIndexes = cluster.GetActiveNodeIndexes()
	expectedActive = []int{0, 2, 3}
	assert.ElementsMatch(t, expectedActive, activeIndexes, "Should have nodes 0, 2, 3")
	t.Logf("Active nodes after joining auto-increment node: %v", activeIndexes)

	// Join a node with specific index 100
	t.Logf("Joining new node with nodeIndex 100...")
	specificNodeIndex, specificNodeAddr, err := cluster.JoinNodeWithIndex(t, 100, currentSeeds)
	assert.NoError(t, err)
	assert.Equal(t, 100, specificNodeIndex, "Should have joined node with index 100")
	assert.NotEmpty(t, specificNodeAddr, "Specific node address should not be empty")
	time.Sleep(2 * time.Second)

	// Verify we now have nodes 0, 2, 3, 100
	activeIndexes = cluster.GetActiveNodeIndexes()
	expectedActive = []int{0, 2, 3, 100}
	assert.ElementsMatch(t, expectedActive, activeIndexes, "Should have nodes 0, 2, 3, 100")
	t.Logf("Active nodes after joining specific node 100: %v", activeIndexes)

	// Try to join a node with index 100 again (should fail)
	t.Logf("Trying to join another node with nodeIndex 100 (should fail)...")
	_, _, err = cluster.JoinNodeWithIndex(t, 100, currentSeeds)
	assert.Error(t, err, "Should fail to join node with existing index 100")

	// Restart node 1 (which was previously stopped)
	t.Logf("Restarting node 1...")
	restartedAddr, err := cluster.RestartNode(t, 1, currentSeeds)
	assert.NoError(t, err)
	assert.NotEmpty(t, restartedAddr, "Restarted node address should not be empty")
	time.Sleep(2 * time.Second)

	// Verify we now have nodes 0, 1, 2, 3, 100
	activeIndexes = cluster.GetActiveNodeIndexes()
	expectedActive = []int{0, 1, 2, 3, 100}
	assert.ElementsMatch(t, expectedActive, activeIndexes, "Should have nodes 0, 1, 2, 3, 100 after restart")
	t.Logf("Active nodes after restarting node 1: %v", activeIndexes)

	// Final verification
	assert.Equal(t, 5, cluster.GetActiveNodes(), "Should have 5 active nodes total")

	t.Logf("TestMiniCluster_NodeIndexManagement completed successfully")
}

// TestMiniCluster_EdgeCases tests edge cases and error handling
func TestMiniCluster_EdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMiniCluster_EdgeCases")

	// Start a 2-node cluster
	const initialNodeCount = 2
	cluster, _, _, _ := utils.StartMiniCluster(t, initialNodeCount, rootPath)
	defer cluster.StopMultiNodeCluster(t)

	t.Logf("Started initial cluster with %d nodes", len(cluster.Servers))
	time.Sleep(2 * time.Second)

	// Test 1: Try to leave a non-existent node
	t.Logf("Test 1: Trying to leave non-existent node 999...")
	_, err := cluster.LeaveNodeWithIndex(t, 999)
	assert.Error(t, err, "Should fail to leave non-existent node")
	t.Logf("✓ Correctly failed to leave non-existent node: %v", err)

	// Test 2: Try to restart a node that's still running
	t.Logf("Test 2: Trying to restart running node 0...")
	currentSeeds := cluster.GetSeedList()
	_, err = cluster.RestartNode(t, 0, currentSeeds)
	assert.Error(t, err, "Should fail to restart running node")
	t.Logf("✓ Correctly failed to restart running node: %v", err)

	// Test 3: Try to join node with negative index
	t.Logf("Test 3: Trying to join node with negative index -1...")
	_, _, err = cluster.JoinNodeWithIndex(t, -1, currentSeeds)
	assert.Error(t, err, "Should fail to join node with negative index")
	t.Logf("✓ Correctly failed to join node with negative index: %v", err)

	// Test 4: Leave all nodes then try operations
	t.Logf("Test 4: Leaving all nodes...")
	for _, nodeIndex := range cluster.GetActiveNodeIndexes() {
		_, err := cluster.LeaveNodeWithIndex(t, nodeIndex)
		assert.NoError(t, err, "Should be able to leave node %d", nodeIndex)
	}
	time.Sleep(1 * time.Second)

	// Verify no active nodes
	assert.Equal(t, 0, cluster.GetActiveNodes(), "Should have no active nodes")
	assert.Empty(t, cluster.GetActiveNodeIndexes(), "Should have no active node indexes")

	// Try to leave when no nodes are active
	t.Logf("Test 5: Trying to leave when no nodes are active...")
	_, err = cluster.LeaveRandomNode(t)
	assert.Error(t, err, "Should fail to leave when no nodes are active")
	t.Logf("✓ Correctly failed to leave when no nodes active: %v", err)

	// Try to get seed list when no nodes are active
	t.Logf("Test 6: Getting seed list when no nodes are active...")
	seedList := cluster.GetSeedList()
	assert.Empty(t, seedList, "Should have empty seed list when no nodes are active")
	t.Logf("✓ Correctly got empty seed list: %v", seedList)

	t.Logf("TestMiniCluster_EdgeCases completed successfully")
}

// TestMiniCluster_MultipleOperations tests multiple leave/join operations
func TestMiniCluster_MultipleOperations(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMiniCluster_MultipleOperations")

	// Start a 5-node cluster
	const initialNodeCount = 5
	cluster, _, _, _ := utils.StartMiniCluster(t, initialNodeCount, rootPath)
	defer cluster.StopMultiNodeCluster(t)

	t.Logf("Started initial cluster with %d nodes", len(cluster.Servers))
	time.Sleep(3 * time.Second)

	// Initial state: nodes 0, 1, 2, 3, 4
	activeIndexes := cluster.GetActiveNodeIndexes()
	assert.Equal(t, []int{0, 1, 2, 3, 4}, activeIndexes, "Should have nodes 0-4 initially")
	t.Logf("Initial active nodes: %v", activeIndexes)

	// Phase 1: Leave nodes 1, 3 (leave some gaps)
	t.Logf("Phase 1: Leaving nodes 1 and 3...")
	_, err := cluster.LeaveNodeWithIndex(t, 1)
	assert.NoError(t, err)
	_, err = cluster.LeaveNodeWithIndex(t, 3)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)

	// Should have nodes 0, 2, 4
	activeIndexes = cluster.GetActiveNodeIndexes()
	expectedActive := []int{0, 2, 4}
	assert.ElementsMatch(t, expectedActive, activeIndexes, "Should have nodes 0, 2, 4")
	t.Logf("After leaving 1,3: %v", activeIndexes)

	// Phase 2: Join new nodes (should get indexes 5, 6)
	t.Logf("Phase 2: Joining 2 new nodes...")
	currentSeeds := cluster.GetSeedList()
	for i := 0; i < 2; i++ {
		newNodeIndex, _, err := cluster.JoinNewNode(t, currentSeeds)
		assert.NoError(t, err)
		assert.Equal(t, 5+i, newNodeIndex, "New node should get index %d", 5+i)
		time.Sleep(1 * time.Second)
	}

	// Should have nodes 0, 2, 4, 5, 6
	activeIndexes = cluster.GetActiveNodeIndexes()
	expectedActive = []int{0, 2, 4, 5, 6}
	assert.ElementsMatch(t, expectedActive, activeIndexes, "Should have nodes 0, 2, 4, 5, 6")
	t.Logf("After joining new nodes: %v", activeIndexes)

	// Phase 3: Restart the stopped nodes to fill gaps
	t.Logf("Phase 3: Restarting nodes 1 and 3 to fill gaps...")
	_, err = cluster.RestartNode(t, 1, currentSeeds)
	assert.NoError(t, err)
	_, err = cluster.RestartNode(t, 3, currentSeeds)
	assert.NoError(t, err)
	time.Sleep(2 * time.Second)

	// Should have nodes 0, 1, 2, 3, 4, 5, 6
	activeIndexes = cluster.GetActiveNodeIndexes()
	expectedActive = []int{0, 1, 2, 3, 4, 5, 6}
	assert.ElementsMatch(t, expectedActive, activeIndexes, "Should have nodes 0-6")
	t.Logf("After restarting gaps: %v", activeIndexes)

	// Phase 4: Leave random nodes multiple times
	t.Logf("Phase 4: Leaving 3 random nodes...")
	leftNodes := make([]int, 0, 3)
	for i := 0; i < 3; i++ {
		leftNodeIndex, err := cluster.LeaveRandomNode(t)
		assert.NoError(t, err)
		leftNodes = append(leftNodes, leftNodeIndex)
		time.Sleep(1 * time.Second)
	}
	t.Logf("Left nodes: %v", leftNodes)

	// Should have 4 nodes remaining
	activeIndexes = cluster.GetActiveNodeIndexes()
	assert.Equal(t, 4, len(activeIndexes), "Should have 4 nodes remaining")
	t.Logf("Remaining nodes: %v", activeIndexes)

	// Phase 5: Restart some of the left nodes
	t.Logf("Phase 5: Restarting 2 of the left nodes...")
	for i := 0; i < 2 && i < len(leftNodes); i++ {
		nodeIndex := leftNodes[i]
		_, err := cluster.RestartNode(t, nodeIndex, cluster.GetSeedList())
		assert.NoError(t, err)
		time.Sleep(1 * time.Second)
	}

	// Should have 6 nodes active now
	finalActiveIndexes := cluster.GetActiveNodeIndexes()
	assert.Equal(t, 6, len(finalActiveIndexes), "Should have 6 nodes after restarts")
	t.Logf("Final active nodes: %v", finalActiveIndexes)

	t.Logf("TestMiniCluster_MultipleOperations completed successfully")
}

// TestMiniCluster_RapidOperations tests rapid leave/join operations
func TestMiniCluster_RapidOperations(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMiniCluster_RapidOperations")

	// Start a 3-node cluster
	const initialNodeCount = 3
	cluster, _, _, _ := utils.StartMiniCluster(t, initialNodeCount, rootPath)
	defer cluster.StopMultiNodeCluster(t)

	t.Logf("Started initial cluster with %d nodes", len(cluster.Servers))
	time.Sleep(2 * time.Second)

	// Rapidly join 5 new nodes
	t.Logf("Rapidly joining 5 new nodes...")
	joinedNodes := make([]int, 0, 5)
	currentSeeds := cluster.GetSeedList()

	for i := 0; i < 5; i++ {
		newNodeIndex, _, err := cluster.JoinNewNode(t, currentSeeds)
		assert.NoError(t, err)
		joinedNodes = append(joinedNodes, newNodeIndex)
		// Short delay to allow node to start
		time.Sleep(100 * time.Millisecond)
	}

	t.Logf("Joined nodes: %v", joinedNodes)

	// Wait for stabilization
	time.Sleep(3 * time.Second)

	// Should have 8 nodes total (3 initial + 5 joined)
	assert.Equal(t, 8, cluster.GetActiveNodes(), "Should have 8 active nodes")
	activeIndexes := cluster.GetActiveNodeIndexes()
	expectedActive := []int{0, 1, 2, 3, 4, 5, 6, 7}
	assert.ElementsMatch(t, expectedActive, activeIndexes, "Should have nodes 0-7")
	t.Logf("Active nodes after rapid join: %v", activeIndexes)

	// Rapidly leave some nodes
	t.Logf("Rapidly leaving nodes 1, 3, 5...")
	nodesToLeave := []int{1, 3, 5}
	for _, nodeIndex := range nodesToLeave {
		_, err := cluster.LeaveNodeWithIndex(t, nodeIndex)
		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for stabilization
	time.Sleep(2 * time.Second)

	// Should have 5 nodes remaining (8 - 3)
	assert.Equal(t, 5, cluster.GetActiveNodes(), "Should have 5 active nodes")
	activeIndexes = cluster.GetActiveNodeIndexes()
	expectedActive = []int{0, 2, 4, 6, 7}
	assert.ElementsMatch(t, expectedActive, activeIndexes, "Should have nodes 0, 2, 4, 6, 7")
	t.Logf("Active nodes after rapid leave: %v", activeIndexes)

	t.Logf("TestMiniCluster_RapidOperations completed successfully")
}

// TestMiniCluster_LargeIndexNumbers tests using large node index numbers
func TestMiniCluster_LargeIndexNumbers(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMiniCluster_LargeIndexNumbers")

	// Start a minimal 1-node cluster
	cluster, _, seeds, _ := utils.StartMiniCluster(t, 1, rootPath)
	defer cluster.StopMultiNodeCluster(t)

	t.Logf("Started minimal cluster")
	time.Sleep(2 * time.Second)

	// Test joining nodes with very large indexes
	largeIndexes := []int{1000, 9999, 100000}

	for _, largeIndex := range largeIndexes {
		t.Logf("Joining node with large index %d...", largeIndex)
		nodeIndex, _, err := cluster.JoinNodeWithIndex(t, largeIndex, seeds)
		assert.NoError(t, err)
		assert.Equal(t, largeIndex, nodeIndex, "Should join with specified large index")
		time.Sleep(500 * time.Millisecond)
	}

	// Verify all nodes are active
	activeIndexes := cluster.GetActiveNodeIndexes()
	expectedActive := []int{0, 1000, 9999, 100000}
	assert.ElementsMatch(t, expectedActive, activeIndexes, "Should have all large index nodes")
	t.Logf("Active nodes with large indexes: %v", activeIndexes)

	// Test max node index tracking
	assert.Equal(t, 100000, cluster.MaxNodeIndex, "MaxNodeIndex should be 100000")

	// Join a new auto-increment node (should get 100001)
	t.Logf("Joining auto-increment node (should get 100001)...")
	newNodeIndex, _, err := cluster.JoinNewNode(t, cluster.GetSeedList())
	assert.NoError(t, err)
	assert.Equal(t, 100001, newNodeIndex, "Auto-increment should give 100001")

	// Verify final state
	activeIndexes = cluster.GetActiveNodeIndexes()
	expectedActive = []int{0, 1000, 9999, 100000, 100001}
	assert.ElementsMatch(t, expectedActive, activeIndexes, "Should have all nodes including auto-increment")
	t.Logf("Final active nodes: %v", activeIndexes)

	t.Logf("TestMiniCluster_LargeIndexNumbers completed successfully")
}

// TestMiniCluster_AddressBasedOperations tests leave and restart operations by address
func TestMiniCluster_AddressBasedOperations(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMiniCluster_AddressBasedOperations")

	// Start a 3-node cluster
	const initialNodeCount = 3
	cluster, _, _, _ := utils.StartMiniCluster(t, initialNodeCount, rootPath)
	defer cluster.StopMultiNodeCluster(t)

	t.Logf("Started initial cluster with %d nodes", len(cluster.Servers))
	time.Sleep(2 * time.Second)

	// Get initial addresses
	gossipSeedList := cluster.GetSeedList()
	assert.Equal(t, 3, len(gossipSeedList), "Should have 3 seed addresses")
	t.Logf("Initial seed list: %v", gossipSeedList)

	// Pick one address to leave
	targetAddress := gossipSeedList[1] // Choose the second node
	t.Logf("Target address to leave: %s", targetAddress)

	// Get nodeIndex for this address (for verification)
	nodeIndex, err := cluster.GetNodeIndexByAddress(targetAddress)
	assert.NoError(t, err)
	t.Logf("Target address corresponds to nodeIndex: %d", nodeIndex)

	// Leave node by address
	t.Logf("Leaving node by address...")
	leftNodeIndex, err := cluster.LeaveNodeByAddress(t, targetAddress)
	assert.NoError(t, err)
	assert.Equal(t, nodeIndex, leftNodeIndex, "Left node index should match")
	time.Sleep(2 * time.Second)

	// Verify node is gone
	newSeedList := cluster.GetSeedList()
	assert.Equal(t, 2, len(newSeedList), "Should have 2 seed addresses after leave")
	t.Logf("Seed list after leave: %v", newSeedList)

	// Verify the specific address is no longer in the list
	for _, addr := range newSeedList {
		assert.NotEqual(t, targetAddress, addr, "Target address should not be in active seed list")
	}

	// Try to leave the same address again (should fail)
	t.Logf("Trying to leave the same address again (should fail)...")
	_, err = cluster.LeaveNodeByAddress(t, targetAddress)
	assert.Error(t, err, "Should fail to leave an inactive node")
	t.Logf("✓ Correctly failed to leave inactive node: %v", err)

	// Restart node by address
	t.Logf("Restarting node by address...")
	restartedNodeIndex, restartedAddress, err := cluster.RestartNodeByAddress(t, targetAddress, cluster.GetSeedList())
	assert.NoError(t, err)
	assert.Equal(t, nodeIndex, restartedNodeIndex, "Restarted node index should match original")
	assert.NotEmpty(t, restartedAddress, "Restarted address should not be empty")
	time.Sleep(2 * time.Second)

	// Verify node is back
	finalSeedList := cluster.GetSeedList()
	assert.Equal(t, 3, len(finalSeedList), "Should have 3 seed addresses after restart")
	t.Logf("Final seed list: %v", finalSeedList)

	// The restarted address might be different due to dynamic port allocation
	// But we should be able to find the nodeIndex again
	finalNodeIndex, err := cluster.GetNodeIndexByAddress(restartedAddress)
	assert.NoError(t, err)
	assert.Equal(t, nodeIndex, finalNodeIndex, "Final node index should match original")

	// Try to restart the same address again (should fail since it's running)
	t.Logf("Trying to restart running node (should fail)...")
	_, _, err = cluster.RestartNodeByAddress(t, restartedAddress, cluster.GetSeedList())
	assert.Error(t, err, "Should fail to restart a running node")
	t.Logf("✓ Correctly failed to restart running node: %v", err)

	// Test with non-existent address
	t.Logf("Testing with non-existent address...")
	_, err = cluster.LeaveNodeByAddress(t, "192.168.1.1:9999")
	assert.Error(t, err, "Should fail to leave non-existent address")
	t.Logf("✓ Correctly failed to leave non-existent address: %v", err)

	_, _, err = cluster.RestartNodeByAddress(t, "192.168.1.1:9999", cluster.GetSeedList())
	assert.Error(t, err, "Should fail to restart non-existent address")
	t.Logf("✓ Correctly failed to restart non-existent address: %v", err)

	t.Logf("TestMiniCluster_AddressBasedOperations completed successfully")
}

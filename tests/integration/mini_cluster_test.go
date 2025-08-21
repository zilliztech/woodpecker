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
	cluster, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)

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
		t.Logf("  Server: %s -> %s (AZ: %s)", server.NodeID, server.Endpoint, server.AZ)
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

	// Start a 100-node cluster
	const nodeCount = 100
	cluster, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)

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
		t.Logf("  Server: %s -> %s (AZ: %s)", server.NodeID, server.Endpoint, server.AZ)
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

func TestMiniCluster_Join(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMiniCluster_Join")

	// Start a 2-node cluster initially
	const initialNodeCount = 2
	cluster, _, seeds := utils.StartMiniCluster(t, initialNodeCount, rootPath)
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
	newNodeAddr, err := cluster.JoinNewNode(t, currentSeeds)
	assert.NoError(t, err)
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

	t.Logf("TestMiniCluster_Join completed successfully")
}

func TestMiniCluster_Leave(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMiniCluster_Leave")

	// Start a 3-node cluster
	const nodeCount = 3
	cluster, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
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
	cluster, _, seeds := utils.StartMiniCluster(t, nodeCount, rootPath)
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

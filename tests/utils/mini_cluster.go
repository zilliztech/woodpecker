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

package utils

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/membership"
	"github.com/zilliztech/woodpecker/server"
)

// MiniCluster represents a cluster of woodpecker servers
type MiniCluster struct {
	Servers        map[int]*server.Server // Map of nodeIndex -> server
	Config         *config.Configuration
	BaseDir        string
	UsedPorts      map[int]int        // Map of nodeIndex -> port for consistent restart, service gRPC ports
	UsedAddresses  map[int]string     // Map of nodeIndex -> last known address, gossip advertiseAddr
	NodeConfigs    map[int]NodeConfig // Map of nodeIndex -> node configuration (AZ, ResourceGroup, etc.)
	MaxNodeIndex   int                // Track the highest nodeIndex used
	allocatedPorts map[int]bool       // Track all allocated ports in this test to avoid competitive reuse
}

// NodeConfig represents configuration for a single node in the cluster
type NodeConfig struct {
	Index         int
	ResourceGroup string
	AZ            string
}

// allocateUniquePort allocates a free port and ensures it's not already allocated in this cluster
func (cluster *MiniCluster) allocateUniquePort() (int, error) {
	maxRetries := 50 // Try up to 50 times to find a unique port
	for i := 0; i < maxRetries; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			continue
		}
		port := listener.Addr().(*net.TCPAddr).Port
		listener.Close()

		// Check if this port is already allocated in this cluster
		if !cluster.allocatedPorts[port] {
			cluster.allocatedPorts[port] = true
			return port, nil
		}
		// Port already used in this cluster, try again
	}
	return 0, fmt.Errorf("failed to allocate unique port after %d retries", maxRetries)
}

// StartMiniCluster starts a test mini cluster of woodpecker servers
func StartMiniCluster(t *testing.T, nodeCount int, baseDir string) (*MiniCluster, *config.Configuration, []string, []string) {
	// Load base configuration
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err, "Failed to load configuration")
	return StartMiniClusterWithCfg(t, nodeCount, baseDir, cfg)
}

func StartMiniClusterWithCfg(t *testing.T, nodeCount int, baseDir string, cfg *config.Configuration) (*MiniCluster, *config.Configuration, []string, []string) {
	// Generate default node configs with consecutive indices
	nodeConfigs := make([]NodeConfig, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeConfigs[i] = NodeConfig{
			Index:         i,
			ResourceGroup: "default",
			AZ:            "default",
		}
	}

	return StartMiniClusterWithCustomNodesAndCfg(t, nodeConfigs, baseDir, cfg)
}

// StartMiniClusterWithCustomNodes starts a test mini cluster with custom node configurations
func StartMiniClusterWithCustomNodes(t *testing.T, nodeConfigs []NodeConfig, baseDir string) (*MiniCluster, *config.Configuration, []string, []string) {
	// Load base configuration
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err, "Failed to load configuration")
	return StartMiniClusterWithCustomNodesAndCfg(t, nodeConfigs, baseDir, cfg)
}

// StartMiniClusterWithCustomNodesAndCfg starts a test mini cluster with custom node configurations and config
func StartMiniClusterWithCustomNodesAndCfg(t *testing.T, nodeConfigs []NodeConfig, baseDir string, cfg *config.Configuration) (*MiniCluster, *config.Configuration, []string, []string) {
	// Set up staged storage type for quorum testing
	cfg.Woodpecker.Storage.Type = "service"
	cfg.Woodpecker.Storage.RootPath = baseDir
	cfg.Minio.RootPath = baseDir
	cfg.Log.Level = "debug"

	cluster := &MiniCluster{
		Servers:        make(map[int]*server.Server),
		Config:         cfg,
		BaseDir:        baseDir,
		UsedPorts:      make(map[int]int),
		UsedAddresses:  make(map[int]string),
		NodeConfigs:    make(map[int]NodeConfig),
		MaxNodeIndex:   -1,
		allocatedPorts: make(map[int]bool),
	}

	ctx := context.Background()

	// Phase 1: Allocate ports for all nodes
	type portAllocation struct {
		nodeIndex   int
		nodeConfig  NodeConfig
		servicePort int
		gossipPort  int
	}
	portAllocations := make([]portAllocation, len(nodeConfigs))

	for i, nodeConfig := range nodeConfigs {
		// Allocate unique service port
		servicePort, err := cluster.allocateUniquePort()
		require.NoError(t, err, "Failed to allocate service port for node %d", nodeConfig.Index)

		// Allocate unique gossip port
		gossipPort, err := cluster.allocateUniquePort()
		require.NoError(t, err, "Failed to allocate gossip port for node %d", nodeConfig.Index)

		portAllocations[i] = portAllocation{
			nodeIndex:   nodeConfig.Index,
			nodeConfig:  nodeConfig,
			servicePort: servicePort,
			gossipPort:  gossipPort,
		}
	}

	// Build complete gossip seeds and service seeds based on allocated ports
	gossipSeeds := make([]string, 0, len(portAllocations))
	serviceSeeds := make([]string, 0, len(portAllocations))
	for _, allocation := range portAllocations {
		gossipSeeds = append(gossipSeeds, fmt.Sprintf("127.0.0.1:%d", allocation.gossipPort))
		serviceSeeds = append(serviceSeeds, fmt.Sprintf("127.0.0.1:%d", allocation.servicePort))
	}

	// Phase 2: Create, prepare and start all servers with complete seeds
	for _, allocation := range portAllocations {
		// Create server configuration for this node
		nodeCfg := *cfg // Copy the base config
		nodeCfg.Woodpecker.Storage.RootPath = filepath.Join(baseDir, fmt.Sprintf("node%d", allocation.nodeIndex))

		// Create server with custom AZ/RG
		nodeServer, err := server.NewServerWithConfig(ctx, &nodeCfg, &membership.ServerConfig{
			NodeID:               fmt.Sprintf("node%d", allocation.nodeIndex),
			BindPort:             allocation.gossipPort,
			AdvertisePort:        allocation.gossipPort,
			AdvertiseAddr:        "127.0.0.1",
			ServicePort:          allocation.servicePort,
			AdvertiseServicePort: allocation.servicePort,
			AdvertiseServiceAddr: "127.0.0.1",
			ResourceGroup:        allocation.nodeConfig.ResourceGroup,
			AZ:                   allocation.nodeConfig.AZ,
			Tags:                 map[string]string{"role": "test"},
		}, gossipSeeds) // Pass complete gossip seeds
		require.NoError(t, err, "Failed to create node %d server", allocation.nodeIndex)

		// Prepare server (sets up listener and gossip)
		err := nodeServer.Prepare()
		require.NoError(t, err, "Failed to prepare node %d", allocation.nodeIndex)

		// Run server (starts grpc server and log store)
		go func(srv *server.Server, nodeID int) {
			if runErr := srv.Run(); runErr != nil {
				// Use fmt instead of t.Logf to avoid panic if test has already finished
				fmt.Printf("Node %d server run error: %v\n", nodeID, runErr)
			}
		}(nodeServer, allocation.nodeIndex)

		// Track in cluster
		cluster.Servers[allocation.nodeIndex] = nodeServer
		cluster.UsedPorts[allocation.nodeIndex] = allocation.servicePort
		cluster.UsedAddresses[allocation.nodeIndex] = fmt.Sprintf("127.0.0.1:%d", allocation.gossipPort)
		cluster.NodeConfigs[allocation.nodeIndex] = allocation.nodeConfig
		if allocation.nodeIndex > cluster.MaxNodeIndex {
			cluster.MaxNodeIndex = allocation.nodeIndex
		}

		t.Logf("Started node %d on service port %d, gossip port %d, AZ=%s, RG=%s",
			allocation.nodeIndex, allocation.servicePort, allocation.gossipPort,
			allocation.nodeConfig.AZ, allocation.nodeConfig.ResourceGroup)
	}

	// Wait for all nodes to start
	time.Sleep(2 * time.Second)

	return cluster, cfg, gossipSeeds, serviceSeeds
}

// StopMultiNodeCluster stops all nodes in the cluster
func (cluster *MiniCluster) StopMultiNodeCluster(t *testing.T) {
	// Stop all server nodes
	for nodeIndex, srv := range cluster.Servers {
		if srv != nil {
			err := srv.Stop()
			if err != nil {
				t.Logf("Error stopping node %d: %v", nodeIndex, err)
			}
			cluster.Servers[nodeIndex] = nil
			t.Logf("Stopped node %d", nodeIndex)
		}
	}
}

// JoinNewNode adds a NEW node to the cluster with the next available nodeIndex
func (cluster *MiniCluster) JoinNewNode(t *testing.T, seeds []string) (int, string, error) {
	// Get next available nodeIndex
	nodeIndex := cluster.MaxNodeIndex + 1
	return cluster.JoinNodeWithIndex(t, nodeIndex, seeds)
}

// JoinNodeWithIndex adds a NEW node to the cluster with specified nodeIndex
func (cluster *MiniCluster) JoinNodeWithIndex(t *testing.T, nodeIndex int, gossipSeeds []string) (int, string, error) {
	// Validate nodeIndex is non-negative
	if nodeIndex < 0 {
		return -1, "", fmt.Errorf("node index cannot be negative: %d", nodeIndex)
	}

	// Check if nodeIndex is already in use
	if _, exists := cluster.Servers[nodeIndex]; exists {
		return -1, "", fmt.Errorf("node %d already exists", nodeIndex)
	}

	// Create server configuration for the new node
	nodeCfg := *cluster.Config // Copy the base config
	nodeCfg.Woodpecker.Storage.RootPath = filepath.Join(cluster.BaseDir, fmt.Sprintf("node%d", nodeIndex))

	// Allocate unique service port
	servicePort, err := cluster.allocateUniquePort()
	if err != nil {
		return -1, "", fmt.Errorf("failed to allocate service port: %w", err)
	}

	// Allocate unique gossip port
	gossipPort, err := cluster.allocateUniquePort()
	if err != nil {
		return -1, "", fmt.Errorf("failed to allocate gossip port: %w", err)
	}

	ctx := context.Background()
	// Create server with default AZ/RG
	nodeServer, err := server.NewServerWithConfig(ctx, &nodeCfg, &membership.ServerConfig{
		NodeID:               fmt.Sprintf("node%d", nodeIndex),
		BindPort:             gossipPort,
		AdvertisePort:        gossipPort,
		AdvertiseAddr:        "127.0.0.1",
		ServicePort:          servicePort,
		AdvertiseServicePort: servicePort,
		AdvertiseServiceAddr: "127.0.0.1",
		ResourceGroup:        "default",
		AZ:                   "default",
		Tags:                 map[string]string{"role": "test"},
	}, gossipSeeds) // Pass complete gossip seeds
	require.NoError(t, err, "Failed to create node %d server", nodeIndex)

	// Prepare server (sets up listener and gossip)
	err = nodeServer.Prepare()
	if err != nil {
		return -1, "", fmt.Errorf("failed to prepare node: %w", err)
	}

	// Run server (starts grpc server and log store)
	go func(srv *server.Server, nodeID int) {
		if runErr := srv.Run(); runErr != nil {
			// Use fmt instead of t.Logf to avoid panic if test has already finished
			fmt.Printf("Node %d server run error: %v\n", nodeID, runErr)
		}
	}(nodeServer, nodeIndex)

	// Wait a bit for the node to fully start
	time.Sleep(1 * time.Second)

	// Add to cluster
	cluster.Servers[nodeIndex] = nodeServer
	cluster.UsedPorts[nodeIndex] = servicePort
	cluster.NodeConfigs[nodeIndex] = NodeConfig{
		Index:         nodeIndex,
		ResourceGroup: "default",
		AZ:            "default",
	}

	// Update MaxNodeIndex if necessary
	if nodeIndex > cluster.MaxNodeIndex {
		cluster.MaxNodeIndex = nodeIndex
	}

	// Get advertise address
	advertiseAddr := fmt.Sprintf("127.0.0.1:%d", gossipPort)
	cluster.UsedAddresses[nodeIndex] = advertiseAddr // Record the address

	t.Logf("Joined new node %d on port %d with address %s", nodeIndex, gossipPort, advertiseAddr)

	return nodeIndex, advertiseAddr, nil
}

// LeaveRandomNode stops and removes a random active node from the cluster
func (cluster *MiniCluster) LeaveRandomNode(t *testing.T) (int, error) {
	if len(cluster.Servers) == 0 {
		return -1, fmt.Errorf("no nodes to leave")
	}

	// Find all active nodes
	activeNodes := make([]int, 0)
	for nodeIndex, srv := range cluster.Servers {
		if srv != nil {
			activeNodes = append(activeNodes, nodeIndex)
		}
	}

	if len(activeNodes) == 0 {
		return -1, fmt.Errorf("no active nodes to leave")
	}

	// Sort to ensure deterministic behavior: always pick the highest-indexed active node
	sort.Ints(activeNodes)
	nodeIndex := activeNodes[len(activeNodes)-1]
	return cluster.LeaveNodeWithIndex(t, nodeIndex)
}

// LeaveNode is an alias for LeaveRandomNode for backward compatibility
func (cluster *MiniCluster) LeaveNode(t *testing.T) (int, error) {
	return cluster.LeaveRandomNode(t)
}

// LeaveNodeWithIndex stops and removes a specific node from the cluster
func (cluster *MiniCluster) LeaveNodeWithIndex(t *testing.T, nodeIndex int) (int, error) {
	srv, exists := cluster.Servers[nodeIndex]
	if !exists || srv == nil {
		return -1, fmt.Errorf("node %d is not active or does not exist", nodeIndex)
	}

	// Stop the node
	err := srv.Stop()
	if err != nil {
		t.Logf("Error stopping node %d: %v", nodeIndex, err)
	}

	// Mark as stopped (but keep the slot for restart)
	cluster.Servers[nodeIndex] = nil

	// Give a short grace period for cleanup
	// With explicit listener.Close() and Shutdown(), ports should be released quickly
	time.Sleep(500 * time.Millisecond)

	t.Logf("Node %d left the cluster", nodeIndex)
	return nodeIndex, nil
}

// LeaveNodeByAddress stops and removes a node by its advertise address
func (cluster *MiniCluster) LeaveNodeByAddress(t *testing.T, address string) (int, error) {
	ctx := context.Background()

	// Find the node with the matching address
	for nodeIndex, srv := range cluster.Servers {
		if srv != nil {
			advertiseAddr := srv.GetAdvertiseAddrPort(ctx)
			if advertiseAddr == address {
				return cluster.LeaveNodeWithIndex(t, nodeIndex)
			}
		}
	}

	return -1, fmt.Errorf("no active node found with address: %s", address)
}

// RestartNode restarts a stopped node with the same servicePort, but a new random gossip port
func (cluster *MiniCluster) RestartNode(t *testing.T, nodeIndex int, gossipSeeds []string) (string, error) {
	// Check if the nodeIndex has a recorded port (was previously started)
	servicePort, portExists := cluster.UsedPorts[nodeIndex]
	if !portExists {
		return "", fmt.Errorf("node %d was never started before", nodeIndex)
	}

	// Check if node is currently running
	if srv, exists := cluster.Servers[nodeIndex]; exists && srv != nil {
		return "", fmt.Errorf("node %d is still running", nodeIndex)
	}

	// Allocate unique gossip port for restart (we'll use the same service port)
	gossipPort, err := cluster.allocateUniquePort()
	require.NoError(t, err, "Failed to allocate gossip port for node %d", nodeIndex)

	// Create server configuration for this node
	nodeCfg := *cluster.Config // Copy the base config
	nodeCfg.Woodpecker.Storage.RootPath = filepath.Join(cluster.BaseDir, fmt.Sprintf("node%d", nodeIndex))

	ctx := context.Background()

	// Retrieve stored node config (AZ, ResourceGroup) for this node
	nodeMeta, hasMeta := cluster.NodeConfigs[nodeIndex]
	rg := "default"
	az := "default"
	if hasMeta {
		rg = nodeMeta.ResourceGroup
		az = nodeMeta.AZ
	}

	// Create server with original AZ/RG
	nodeServer, err := server.NewServerWithConfig(ctx, &nodeCfg, &membership.ServerConfig{
		NodeID:               fmt.Sprintf("node%d", nodeIndex),
		BindPort:             gossipPort,
		AdvertisePort:        gossipPort,
		AdvertiseAddr:        "127.0.0.1",
		ServicePort:          servicePort,
		AdvertiseServicePort: servicePort,
		AdvertiseServiceAddr: "127.0.0.1",
		ResourceGroup:        rg,
		AZ:                   az,
		Tags:                 map[string]string{"role": "test"},
	}, gossipSeeds) // Pass complete gossip seeds
	require.NoError(t, err, "Failed to create server for node %d", nodeIndex)

	// Prepare server (sets up listener and gossip)
	err = nodeServer.Prepare()
	if err != nil {
		return "", fmt.Errorf("failed to prepare node: %w", err)
	}

	// Run server (starts grpc server and log store)
	go func(srv *server.Server, nodeID int) {
		if runErr := srv.Run(); runErr != nil {
			// Use fmt instead of t.Logf to avoid panic if test has already finished
			fmt.Printf("Node %d server run error: %v\n", nodeID, runErr)
		}
	}(nodeServer, nodeIndex)

	// Update cluster
	cluster.Servers[nodeIndex] = nodeServer

	// Get advertise address
	advertiseAddr := fmt.Sprintf("127.0.0.1:%d", gossipPort)
	cluster.UsedAddresses[nodeIndex] = advertiseAddr // Update the address record

	t.Logf("Restarted node %d on service port %d, gossip port %d, AZ=%s, RG=%s", nodeIndex, servicePort, gossipPort, az, rg)

	return advertiseAddr, nil
}

// RestartNodeByAddress restarts a stopped node by finding it through the address it previously used
func (cluster *MiniCluster) RestartNodeByAddress(t *testing.T, address string, seeds []string) (int, string, error) {
	// Look through the stored addresses to find the matching nodeIndex
	for nodeIndex, storedAddr := range cluster.UsedAddresses {
		if storedAddr == address {
			// Check if this node is currently stopped
			if srv, exists := cluster.Servers[nodeIndex]; !exists || srv == nil {
				// Found the stopped node, restart it
				restartedAddr, err := cluster.RestartNode(t, nodeIndex, seeds)
				return nodeIndex, restartedAddr, err
			} else {
				// Node is still running
				return -1, "", fmt.Errorf("node with address %s (nodeIndex %d) is still running", address, nodeIndex)
			}
		}
	}

	return -1, "", fmt.Errorf("no node found that previously used address: %s", address)
}

// GetNodeIndexByAddress returns the nodeIndex for a given address (for active nodes only)
func (cluster *MiniCluster) GetNodeIndexByAddress(address string) (int, error) {
	ctx := context.Background()

	for nodeIndex, srv := range cluster.Servers {
		if srv != nil {
			advertiseAddr := srv.GetAdvertiseAddrPort(ctx)
			if advertiseAddr == address {
				return nodeIndex, nil
			}
		}
	}

	return -1, fmt.Errorf("no active node found with address: %s", address)
}

// GetActiveNodes returns the count of active nodes
func (cluster *MiniCluster) GetActiveNodes() int {
	count := 0
	for _, srv := range cluster.Servers {
		if srv != nil {
			count++
		}
	}
	return count
}

// GetActiveNodeIndexes returns a slice of active node indexes (sorted)
func (cluster *MiniCluster) GetActiveNodeIndexes() []int {
	activeNodes := make([]int, 0)
	for nodeIndex, srv := range cluster.Servers {
		if srv != nil {
			activeNodes = append(activeNodes, nodeIndex)
		}
	}
	// Sort the slice to ensure consistent ordering
	sort.Ints(activeNodes)
	return activeNodes
}

// GetSeedList returns current active seed addresses
func (cluster *MiniCluster) GetSeedList() []string {
	ctx := context.Background()
	seeds := make([]string, 0)
	for _, srv := range cluster.Servers {
		if srv != nil {
			addr := srv.GetAdvertiseAddrPort(ctx)
			if addr != "" {
				seeds = append(seeds, addr)
			}
		}
	}
	return seeds
}

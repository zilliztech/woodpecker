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

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/server"
)

// MiniCluster represents a cluster of woodpecker servers
type MiniCluster struct {
	Servers       map[int]*server.Server // Map of nodeIndex -> server
	Config        *config.Configuration
	BaseDir       string
	UsedPorts     map[int]int    // Map of nodeIndex -> port for consistent restart, gRPC ports
	UsedAddresses map[int]string // Map of nodeIndex -> last known address, gossip advertiseAddr
	MaxNodeIndex  int            // Track the highest nodeIndex used
}

func StartMiniClusterWithCfg(t *testing.T, nodeCount int, baseDir string, cfg *config.Configuration) (*MiniCluster, *config.Configuration, []string, []string) {
	// Set up staged storage type for quorum testing
	cfg.Woodpecker.Storage.Type = "service"
	cfg.Woodpecker.Storage.RootPath = baseDir
	cfg.Minio.RootPath = baseDir
	cfg.Log.Level = "debug"

	cluster := &MiniCluster{
		Servers:       make(map[int]*server.Server),
		Config:        cfg,
		BaseDir:       baseDir,
		UsedPorts:     make(map[int]int),
		UsedAddresses: make(map[int]string),
		MaxNodeIndex:  -1,
	}

	ctx := context.Background()

	// Start each node with consecutive nodeIndex starting from 0
	gossipSeeds := make([]string, 0)
	serviceSeeds := make([]string, 0)
	for i := 0; i < nodeCount; i++ {
		nodeIndex := i

		// Create server configuration for this node
		nodeCfg := *cfg // Copy the base config
		nodeCfg.Woodpecker.Storage.RootPath = filepath.Join(baseDir, fmt.Sprintf("node%d", nodeIndex))

		// Find a free port for this node
		listener, err := net.Listen("tcp", "localhost:0")
		assert.NoError(t, err)
		port := listener.Addr().(*net.TCPAddr).Port
		cluster.UsedPorts[nodeIndex] = port // Track the port for this node
		listener.Close()                    // Close it so server can use it

		// Create and start server
		nodeServer := server.NewServer(ctx, &nodeCfg, 0, port, gossipSeeds)

		// Prepare server (sets up listener and gossip)
		err = nodeServer.Prepare()
		assert.NoError(t, err)

		// add node to seed list
		advertiseAddr := nodeServer.GetAdvertiseAddrPort(ctx)
		gossipSeeds = append(gossipSeeds, advertiseAddr)
		serviceAddr := nodeServer.GetServiceAddrPort(ctx)
		serviceSeeds = append(serviceSeeds, serviceAddr)

		// Run server (starts grpc server and log store)
		go func(srv *server.Server, nodeID int) {
			err := srv.Run()
			if err != nil {
				t.Logf("Node %d server run error: %v", nodeID, err)
			}
		}(nodeServer, nodeIndex)

		cluster.Servers[nodeIndex] = nodeServer
		cluster.MaxNodeIndex = nodeIndex
		cluster.UsedAddresses[nodeIndex] = advertiseAddr // Record the address
		t.Logf("Started node %d on port %d", nodeIndex, port)
	}

	// Wait for all nodes to start
	time.Sleep(2 * time.Second)

	return cluster, cfg, gossipSeeds, serviceSeeds
}

// NodeConfig represents configuration for a single node in the cluster
type NodeConfig struct {
	Index         int
	ResourceGroup string
	AZ            string
}

// StartMiniCluster starts a test mini cluster of woodpecker servers
func StartMiniCluster(t *testing.T, nodeCount int, baseDir string) (*MiniCluster, *config.Configuration, []string, []string) {
	// Load base configuration
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)
	return StartMiniClusterWithCfg(t, nodeCount, baseDir, cfg)
}

// StartMiniClusterWithCustomNodes starts a test mini cluster with custom node configurations
func StartMiniClusterWithCustomNodes(t *testing.T, nodeConfigs []NodeConfig, baseDir string) (*MiniCluster, *config.Configuration, []string, []string) {
	// Load base configuration
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)
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
		Servers:       make(map[int]*server.Server),
		Config:        cfg,
		BaseDir:       baseDir,
		UsedPorts:     make(map[int]int),
		UsedAddresses: make(map[int]string),
		MaxNodeIndex:  -1,
	}

	ctx := context.Background()

	// Start each node with specified configurations
	gossipSeeds := make([]string, 0)
	serviceSeeds := make([]string, 0)
	for _, nodeConfig := range nodeConfigs {
		nodeIndex := nodeConfig.Index

		// Create server configuration for this node
		nodeCfg := *cfg // Copy the base config
		nodeCfg.Woodpecker.Storage.RootPath = filepath.Join(baseDir, fmt.Sprintf("node%d", nodeIndex))

		// Find a free port for this node
		listener, err := net.Listen("tcp", "localhost:0")
		assert.NoError(t, err)
		port := listener.Addr().(*net.TCPAddr).Port
		cluster.UsedPorts[nodeIndex] = port // Track the port for this node
		listener.Close()                    // Close it so server can use it

		// Find a free port for gossip
		gossipListener, err := net.Listen("tcp", "localhost:0")
		assert.NoError(t, err)
		gossipPort := gossipListener.Addr().(*net.TCPAddr).Port
		gossipListener.Close()

		// Create and start server with custom AZ/RG
		nodeServer := server.NewServerWithConfig(ctx, &nodeCfg, &server.Config{
			BindPort:            gossipPort,
			ServicePort:         port,
			SeedNodes:           gossipSeeds,
			AdvertiseGrpcPort:   port,
			AdvertiseGossipPort: gossipPort,
			ResourceGroup:       nodeConfig.ResourceGroup,
			AZ:                  nodeConfig.AZ,
		})

		// Prepare server (sets up listener and gossip)
		err = nodeServer.Prepare()
		assert.NoError(t, err)

		// add node to seed list
		advertiseAddr := nodeServer.GetAdvertiseAddrPort(ctx)
		gossipSeeds = append(gossipSeeds, advertiseAddr)
		serviceAddr := nodeServer.GetServiceAddrPort(ctx)
		serviceSeeds = append(serviceSeeds, serviceAddr)

		// Run server (starts grpc server and log store)
		go func(srv *server.Server, nodeID int) {
			err := srv.Run()
			if err != nil {
				t.Logf("Node %d server run error: %v", nodeID, err)
			}
		}(nodeServer, nodeIndex)

		cluster.Servers[nodeIndex] = nodeServer
		if nodeIndex > cluster.MaxNodeIndex {
			cluster.MaxNodeIndex = nodeIndex
		}
		cluster.UsedAddresses[nodeIndex] = advertiseAddr
		t.Logf("Started node %d on port %d with AZ=%s, RG=%s", nodeIndex, port, nodeConfig.AZ, nodeConfig.ResourceGroup)
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
			t.Logf("Stopped node %d", nodeIndex)
		}
	}
}

// JoinNewNode adds a new node to the cluster with the next available nodeIndex
func (cluster *MiniCluster) JoinNewNode(t *testing.T, seeds []string) (int, string, error) {
	// Get next available nodeIndex
	nodeIndex := cluster.MaxNodeIndex + 1
	return cluster.JoinNodeWithIndex(t, nodeIndex, seeds)
}

// JoinNodeWithIndex adds a new node to the cluster with specified nodeIndex
func (cluster *MiniCluster) JoinNodeWithIndex(t *testing.T, nodeIndex int, seeds []string) (int, string, error) {
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

	// Find a free port for this node
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return -1, "", fmt.Errorf("failed to find free port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	ctx := context.Background()

	// Create and start server
	nodeServer := server.NewServer(ctx, &nodeCfg, 0, port, seeds)

	// Prepare server (sets up listener and gossip)
	err = nodeServer.Prepare()
	if err != nil {
		return -1, "", fmt.Errorf("failed to prepare node: %w", err)
	}

	// Run server (starts grpc server and log store)
	go func(srv *server.Server, nodeID int) {
		err := srv.Run()
		if err != nil {
			t.Logf("Node %d server run error: %v", nodeID, err)
		}
	}(nodeServer, nodeIndex)

	// Add to cluster
	cluster.Servers[nodeIndex] = nodeServer
	cluster.UsedPorts[nodeIndex] = port

	// Update MaxNodeIndex if necessary
	if nodeIndex > cluster.MaxNodeIndex {
		cluster.MaxNodeIndex = nodeIndex
	}

	// Get advertise address
	advertiseAddr := nodeServer.GetAdvertiseAddrPort(ctx)
	cluster.UsedAddresses[nodeIndex] = advertiseAddr // Record the address

	t.Logf("Joined new node %d on port %d with address %s", nodeIndex, port, advertiseAddr)

	// Wait a bit for the node to fully start
	time.Sleep(1 * time.Second)

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

	// Choose the last active node for simplicity (could be randomized)
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

// RestartNode restarts a stopped node with the same port
func (cluster *MiniCluster) RestartNode(t *testing.T, nodeIndex int, seeds []string) (string, error) {
	// Check if the nodeIndex has a recorded port (was previously started)
	port, portExists := cluster.UsedPorts[nodeIndex]
	if !portExists {
		return "", fmt.Errorf("node %d was never started before", nodeIndex)
	}

	// Check if node is currently running
	if srv, exists := cluster.Servers[nodeIndex]; exists && srv != nil {
		return "", fmt.Errorf("node %d is still running", nodeIndex)
	}

	// Create server configuration for this node
	nodeCfg := *cluster.Config // Copy the base config
	nodeCfg.Woodpecker.Storage.RootPath = filepath.Join(cluster.BaseDir, fmt.Sprintf("node%d", nodeIndex))

	ctx := context.Background()

	// Create and start server
	nodeServer := server.NewServer(ctx, &nodeCfg, 0, port, seeds)

	// Prepare server (sets up listener and gossip)
	err := nodeServer.Prepare()
	if err != nil {
		return "", fmt.Errorf("failed to prepare node: %w", err)
	}

	// Run server (starts grpc server and log store)
	go func(srv *server.Server, nodeID int) {
		err := srv.Run()
		if err != nil {
			t.Logf("Node %d server run error: %v", nodeID, err)
		}
	}(nodeServer, nodeIndex)

	// Update cluster
	cluster.Servers[nodeIndex] = nodeServer

	// Get advertise address
	advertiseAddr := nodeServer.GetAdvertiseAddrPort(ctx)
	cluster.UsedAddresses[nodeIndex] = advertiseAddr // Update the address record

	t.Logf("Restarted node %d on port %d with address %s", nodeIndex, port, advertiseAddr)

	// Wait a bit for the node to fully start
	time.Sleep(1 * time.Second)

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

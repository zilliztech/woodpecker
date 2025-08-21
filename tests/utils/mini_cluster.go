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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/server"
)

// MiniCluster represents a cluster of woodpecker servers
type MiniCluster struct {
	Servers   []*server.Server
	Config    *config.Configuration
	BaseDir   string
	UsedPorts []int // Track used ports for consistent restart
}

func StartMiniClusterWithCfg(t *testing.T, nodeCount int, baseDir string, cfg *config.Configuration) (*MiniCluster, *config.Configuration, []string) {
	// Set up staged storage type for quorum testing
	cfg.Woodpecker.Storage.Type = "service"
	cfg.Woodpecker.Storage.RootPath = baseDir
	cfg.Minio.RootPath = baseDir
	cfg.Log.Level = "debug"

	cluster := &MiniCluster{
		Servers:   make([]*server.Server, nodeCount),
		Config:    cfg,
		BaseDir:   baseDir,
		UsedPorts: make([]int, nodeCount),
	}

	ctx := context.Background()

	// Start each node
	seeds := make([]string, 0)
	for i := 0; i < nodeCount; i++ {
		// Create server configuration for this node
		nodeCfg := *cfg // Copy the base config
		nodeCfg.Woodpecker.Storage.RootPath = filepath.Join(baseDir, fmt.Sprintf("node%d", i))

		// Find a free port for this node
		listener, err := net.Listen("tcp", "localhost:0")
		assert.NoError(t, err)
		port := listener.Addr().(*net.TCPAddr).Port
		cluster.UsedPorts[i] = port // Track the port for this node
		listener.Close()            // Close it so server can use it

		// Create and start server
		nodeServer := server.NewServer(ctx, &nodeCfg, 0, port, []string{})

		// Prepare server (sets up listener and gossip)
		err = nodeServer.Prepare()
		assert.NoError(t, err)

		// add node to seed list
		advertiseAddr := nodeServer.GetAdvertiseAddr(ctx)
		seeds = append(seeds, advertiseAddr)

		// Run server (starts grpc server and log store)
		go func(srv *server.Server, nodeID int) {
			err := srv.Run()
			if err != nil {
				t.Logf("Node %d server run error: %v", nodeID, err)
			}
		}(nodeServer, i)

		cluster.Servers[i] = nodeServer
		t.Logf("Started node %d on port %d", i, port)
	}

	// Wait for all nodes to start
	time.Sleep(2 * time.Second)

	return cluster, cfg, seeds
}

// StartMiniCluster starts a test mini cluster of woodpecker servers
func StartMiniCluster(t *testing.T, nodeCount int, baseDir string) (*MiniCluster, *config.Configuration, []string) {
	// Load base configuration
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)
	return StartMiniClusterWithCfg(t, nodeCount, baseDir, cfg)
}

// StopMultiNodeCluster stops all nodes in the cluster
func (cluster *MiniCluster) StopMultiNodeCluster(t *testing.T) {
	// Stop all server nodes
	for i, srv := range cluster.Servers {
		if srv != nil {
			err := srv.Stop()
			if err != nil {
				t.Logf("Error stopping node %d: %v", i, err)
			}
			t.Logf("Stopped node %d", i)
		}
	}
}

// JoinNewNode adds a new node to the cluster
func (cluster *MiniCluster) JoinNewNode(t *testing.T, seeds []string) (string, error) {
	nodeIndex := len(cluster.Servers)

	// Create server configuration for the new node
	nodeCfg := *cluster.Config // Copy the base config
	nodeCfg.Woodpecker.Storage.RootPath = filepath.Join(cluster.BaseDir, fmt.Sprintf("node%d", nodeIndex))

	// Find a free port for this node
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return "", fmt.Errorf("failed to find free port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	ctx := context.Background()

	// Create and start server
	nodeServer := server.NewServer(ctx, &nodeCfg, 0, port, seeds)

	// Prepare server (sets up listener and gossip)
	err = nodeServer.Prepare()
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

	// Add to cluster
	cluster.Servers = append(cluster.Servers, nodeServer)
	cluster.UsedPorts = append(cluster.UsedPorts, port)

	// Get advertise address
	advertiseAddr := nodeServer.GetAdvertiseAddr(ctx)

	t.Logf("Joined new node %d on port %d with address %s", nodeIndex, port, advertiseAddr)

	// Wait a bit for the node to fully start
	time.Sleep(1 * time.Second)

	return advertiseAddr, nil
}

// LeaveNode stops and removes a random node from the cluster
func (cluster *MiniCluster) LeaveNode(t *testing.T) (int, error) {
	if len(cluster.Servers) == 0 {
		return -1, fmt.Errorf("no nodes to leave")
	}

	// Find the last active node (simple strategy)
	nodeIndex := len(cluster.Servers) - 1
	for nodeIndex >= 0 && cluster.Servers[nodeIndex] == nil {
		nodeIndex--
	}

	if nodeIndex < 0 {
		return -1, fmt.Errorf("no active nodes to leave")
	}

	// Stop the node
	srv := cluster.Servers[nodeIndex]
	err := srv.Stop()
	if err != nil {
		t.Logf("Error stopping node %d: %v", nodeIndex, err)
	}

	// Mark as stopped (but keep the slot for restart)
	cluster.Servers[nodeIndex] = nil

	t.Logf("Node %d left the cluster", nodeIndex)
	return nodeIndex, nil
}

// RestartNode restarts a stopped node with the same port
func (cluster *MiniCluster) RestartNode(t *testing.T, nodeIndex int, seeds []string) (string, error) {
	if nodeIndex >= len(cluster.Servers) {
		return "", fmt.Errorf("invalid node index: %d", nodeIndex)
	}

	if cluster.Servers[nodeIndex] != nil {
		return "", fmt.Errorf("node %d is still running", nodeIndex)
	}

	// Create server configuration for this node
	nodeCfg := *cluster.Config // Copy the base config
	nodeCfg.Woodpecker.Storage.RootPath = filepath.Join(cluster.BaseDir, fmt.Sprintf("node%d", nodeIndex))

	// Use the same port as before
	port := cluster.UsedPorts[nodeIndex]

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
	advertiseAddr := nodeServer.GetAdvertiseAddr(ctx)

	t.Logf("Restarted node %d on port %d with address %s", nodeIndex, port, advertiseAddr)

	// Wait a bit for the node to fully start
	time.Sleep(1 * time.Second)

	return advertiseAddr, nil
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

// GetSeedList returns current active seed addresses
func (cluster *MiniCluster) GetSeedList() []string {
	ctx := context.Background()
	seeds := make([]string, 0)
	for _, srv := range cluster.Servers {
		if srv != nil {
			addr := srv.GetAdvertiseAddr(ctx)
			if addr != "" {
				seeds = append(seeds, addr)
			}
		}
	}
	return seeds
}

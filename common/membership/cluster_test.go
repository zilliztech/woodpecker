// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package membership

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitForConvergence waits until the discovery on the given node sees the expected number of servers,
// or returns false after timeout.
func waitForConvergence(discovery *ServiceDiscovery, expectedCount int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(discovery.GetAllServers()) == expectedCount {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return len(discovery.GetAllServers()) == expectedCount
}

// waitForNodeGone waits until the specified node is no longer in discovery, or returns false after timeout.
func waitForNodeGone(discovery *ServiceDiscovery, nodeID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		servers := discovery.GetAllServers()
		if _, exists := servers[nodeID]; !exists {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	_, exists := discovery.GetAllServers()[nodeID]
	return !exists
}

// waitForNodePresent waits until the specified node appears in discovery, or returns false after timeout.
func waitForNodePresent(discovery *ServiceDiscovery, nodeID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		servers := discovery.GetAllServers()
		if _, exists := servers[nodeID]; exists {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	_, exists := discovery.GetAllServers()[nodeID]
	return exists
}

// printDiscoveryStatus prints the current discovery state for debugging.
func printDiscoveryStatus(t *testing.T, label string, discovery *ServiceDiscovery) {
	servers := discovery.GetAllServers()
	t.Logf("[%s] Discovered servers: %d", label, len(servers))
	for id, meta := range servers {
		t.Logf("  - %s: RG=%s, AZ=%s, Endpoint=%s", id, meta.ResourceGroup, meta.Az, meta.Endpoint)
	}
	rgs := discovery.GetResourceGroups()
	t.Logf("[%s] Resource groups: %v", label, rgs)
	for _, rg := range rgs {
		dist := discovery.GetAZDistribution(rg)
		t.Logf("[%s]   RG=%s AZ distribution: %v", label, rg, dist)
	}
}

// TestCluster_TwoServers_JoinAndLeave tests two server nodes forming a cluster,
// verifying they discover each other, then one leaves and the other sees the change.
func TestCluster_TwoServers_JoinAndLeave(t *testing.T) {
	// --- Create server 1 ---
	server1, err := NewServerNode(&ServerConfig{
		NodeID:        "cluster-s1",
		ResourceGroup: "rg-1",
		AZ:            "az-1",
		BindPort:      17960,
		ServicePort:   18090,
		Tags:          map[string]string{"role": "leader"},
	})
	require.NoError(t, err)
	defer server1.Shutdown()

	t.Log("=== Server1 created ===")
	printDiscoveryStatus(t, "Server1", server1.GetDiscovery())

	// --- Create server 2 and join server 1 ---
	server2, err := NewServerNode(&ServerConfig{
		NodeID:        "cluster-s2",
		ResourceGroup: "rg-1",
		AZ:            "az-2",
		BindPort:      17961,
		ServicePort:   18091,
		Tags:          map[string]string{"role": "follower"},
	})
	require.NoError(t, err)
	defer server2.Shutdown()

	err = server2.Join([]string{"127.0.0.1:17960"})
	require.NoError(t, err)

	// Wait for gossip convergence: both servers should see 2 nodes
	t.Log("=== Waiting for gossip convergence (2 servers) ===")
	assert.True(t, waitForConvergence(server1.GetDiscovery(), 2, 5*time.Second),
		"server1 should discover 2 servers")
	assert.True(t, waitForConvergence(server2.GetDiscovery(), 2, 5*time.Second),
		"server2 should discover 2 servers")

	printDiscoveryStatus(t, "Server1", server1.GetDiscovery())
	printDiscoveryStatus(t, "Server2", server2.GetDiscovery())

	// Verify both servers see each other
	s1Servers := server1.GetDiscovery().GetAllServers()
	assert.Contains(t, s1Servers, "cluster-s1")
	assert.Contains(t, s1Servers, "cluster-s2")
	assert.Equal(t, "az-1", s1Servers["cluster-s1"].Az)
	assert.Equal(t, "az-2", s1Servers["cluster-s2"].Az)

	s2Servers := server2.GetDiscovery().GetAllServers()
	assert.Contains(t, s2Servers, "cluster-s1")
	assert.Contains(t, s2Servers, "cluster-s2")

	// Verify AZ distribution
	dist := server1.GetDiscovery().GetAZDistribution("rg-1")
	assert.Equal(t, 1, dist["az-1"])
	assert.Equal(t, 1, dist["az-2"])
	t.Logf("AZ distribution before leave: %v", dist)

	// --- Server 2 leaves ---
	t.Log("=== Server2 leaving cluster ===")
	err = server2.Leave()
	assert.NoError(t, err)

	// Wait for server1 to see server2 gone
	assert.True(t, waitForNodeGone(server1.GetDiscovery(), "cluster-s2", 10*time.Second),
		"server1 should see server2 gone after it leaves")

	printDiscoveryStatus(t, "Server1 after leave", server1.GetDiscovery())

	s1ServersAfter := server1.GetDiscovery().GetAllServers()
	assert.Contains(t, s1ServersAfter, "cluster-s1")
	assert.NotContains(t, s1ServersAfter, "cluster-s2")

	// AZ distribution should only have az-1
	distAfter := server1.GetDiscovery().GetAZDistribution("rg-1")
	assert.Equal(t, 1, distAfter["az-1"])
	assert.Equal(t, 0, distAfter["az-2"])
	t.Logf("AZ distribution after leave: %v", distAfter)

	err = server1.Leave()
	assert.NoError(t, err)
}

// TestCluster_TwoServers_ClientDiscovery tests a 2-server cluster with a client observer.
// The client discovers both servers, then servers leave one by one and the client sees each departure.
func TestCluster_TwoServers_ClientDiscovery(t *testing.T) {
	// --- Create 2 servers in different AZs ---
	server1, err := NewServerNode(&ServerConfig{
		NodeID:        "disc-s1",
		ResourceGroup: "rg-data",
		AZ:            "az-east",
		BindPort:      17970,
		ServicePort:   18100,
	})
	require.NoError(t, err)
	defer server1.Shutdown()

	server2, err := NewServerNode(&ServerConfig{
		NodeID:        "disc-s2",
		ResourceGroup: "rg-data",
		AZ:            "az-west",
		BindPort:      17971,
		ServicePort:   18101,
	})
	require.NoError(t, err)
	defer server2.Shutdown()

	// Join server2 to server1
	err = server2.Join([]string{"127.0.0.1:17970"})
	require.NoError(t, err)

	// Wait for servers to converge
	t.Log("=== Waiting for 2-server convergence ===")
	assert.True(t, waitForConvergence(server1.GetDiscovery(), 2, 5*time.Second))
	assert.True(t, waitForConvergence(server2.GetDiscovery(), 2, 5*time.Second))

	printDiscoveryStatus(t, "Server1", server1.GetDiscovery())

	// --- Create a client and join the cluster ---
	client, err := NewClientNode(&ClientConfig{
		NodeID:   "disc-c1",
		BindAddr: "127.0.0.1",
		BindPort: 17972,
	})
	require.NoError(t, err)
	defer client.Shutdown()

	err = client.Join([]string{"127.0.0.1:17970"})
	require.NoError(t, err)

	// Wait for client to discover both servers
	t.Log("=== Waiting for client to discover all servers ===")
	assert.True(t, waitForConvergence(client.GetDiscovery(), 2, 5*time.Second),
		"client should discover 2 servers")

	printDiscoveryStatus(t, "Client", client.GetDiscovery())

	// Verify client sees both servers
	clientServers := client.GetDiscovery().GetAllServers()
	assert.Len(t, clientServers, 2)
	assert.Contains(t, clientServers, "disc-s1")
	assert.Contains(t, clientServers, "disc-s2")

	// Verify AZ distribution: 2 AZs each with 1 server
	dist := client.GetDiscovery().GetAZDistribution("rg-data")
	assert.Equal(t, 1, dist["az-east"])
	assert.Equal(t, 1, dist["az-west"])
	t.Logf("Client view - AZ distribution: %v", dist)

	// 3-replica selection fails (only 2 AZs)
	_, replicaErr := client.SelectReplicas("rg-data")
	assert.Error(t, replicaErr)
	t.Logf("3-replica with 2 AZs: %v (expected error)", replicaErr)

	// --- Server2 leaves ---
	t.Log("=== Server2 leaving ===")
	err = server2.Leave()
	assert.NoError(t, err)

	assert.True(t, waitForNodeGone(client.GetDiscovery(), "disc-s2", 10*time.Second),
		"client should see server2 gone")
	printDiscoveryStatus(t, "Client after s2 leave", client.GetDiscovery())

	clientServers = client.GetDiscovery().GetAllServers()
	assert.Contains(t, clientServers, "disc-s1")
	assert.NotContains(t, clientServers, "disc-s2")

	// --- Server1 leaves ---
	t.Log("=== Server1 leaving ===")
	err = server1.Leave()
	assert.NoError(t, err)

	assert.True(t, waitForNodeGone(client.GetDiscovery(), "disc-s1", 10*time.Second),
		"client should see server1 gone")
	printDiscoveryStatus(t, "Client after all leave", client.GetDiscovery())

	assert.Empty(t, client.GetDiscovery().GetAllServers())

	err = client.Leave()
	assert.NoError(t, err)
}

// TestCluster_ServerRejoin tests a server leaving and rejoining with a different AZ.
func TestCluster_ServerRejoin(t *testing.T) {
	// --- Create server 1 (anchor) ---
	server1, err := NewServerNode(&ServerConfig{
		NodeID:        "rejoin-s1",
		ResourceGroup: "rg-1",
		AZ:            "az-1",
		BindPort:      17980,
		ServicePort:   18110,
	})
	require.NoError(t, err)
	defer server1.Shutdown()

	// --- Create server 2 and join ---
	server2, err := NewServerNode(&ServerConfig{
		NodeID:        "rejoin-s2",
		ResourceGroup: "rg-1",
		AZ:            "az-2",
		BindPort:      17981,
		ServicePort:   18111,
	})
	require.NoError(t, err)

	err = server2.Join([]string{"127.0.0.1:17980"})
	require.NoError(t, err)

	// --- Create client to observe ---
	client, err := NewClientNode(&ClientConfig{
		NodeID:   "rejoin-c1",
		BindAddr: "127.0.0.1",
		BindPort: 17982,
	})
	require.NoError(t, err)
	defer client.Shutdown()

	err = client.Join([]string{"127.0.0.1:17980"})
	require.NoError(t, err)

	// Wait for convergence
	assert.True(t, waitForConvergence(client.GetDiscovery(), 2, 5*time.Second))
	t.Log("=== Initial cluster: 2 servers ===")
	printDiscoveryStatus(t, "Client", client.GetDiscovery())

	// --- Server 2 leaves and shuts down ---
	t.Log("=== Server2 leaving ===")
	err = server2.Leave()
	assert.NoError(t, err)
	err = server2.Shutdown()
	assert.NoError(t, err)

	assert.True(t, waitForNodeGone(client.GetDiscovery(), "rejoin-s2", 10*time.Second))
	printDiscoveryStatus(t, "Client after s2 leave", client.GetDiscovery())
	assert.NotContains(t, client.GetDiscovery().GetAllServers(), "rejoin-s2")

	// --- Server 2 rejoins with a DIFFERENT AZ ---
	t.Log("=== Server2 rejoining with az-3 ===")
	server2New, err := NewServerNode(&ServerConfig{
		NodeID:        "rejoin-s2",
		ResourceGroup: "rg-1",
		AZ:            "az-3", // different AZ
		BindPort:      17983,  // different port (old one may still be in TIME_WAIT)
		ServicePort:   18112,
	})
	require.NoError(t, err)
	defer server2New.Shutdown()

	err = server2New.Join([]string{"127.0.0.1:17980"})
	require.NoError(t, err)

	assert.True(t, waitForNodePresent(client.GetDiscovery(), "rejoin-s2", 10*time.Second))
	t.Log("=== After rejoin ===")
	printDiscoveryStatus(t, "Client after rejoin", client.GetDiscovery())

	// Verify client sees the rejoined server with the new AZ
	clientServers := client.GetDiscovery().GetAllServers()
	assert.Contains(t, clientServers, "rejoin-s2")
	assert.Equal(t, "az-3", clientServers["rejoin-s2"].Az, "rejoined server should have new AZ")

	// Verify AZ distribution reflects the change
	dist := client.GetDiscovery().GetAZDistribution("rg-1")
	assert.Equal(t, 1, dist["az-1"])
	assert.Equal(t, 0, dist["az-2"], "old AZ should be gone")
	assert.Equal(t, 1, dist["az-3"], "new AZ should be present")
	t.Logf("AZ distribution after rejoin: %v", dist)

	server2New.Leave()
	server1.Leave()
	client.Leave()
}

// TestCluster_MultiResourceGroup tests servers in different resource groups
// and verifies a client can discover and differentiate between them.
func TestCluster_MultiResourceGroup(t *testing.T) {
	// --- Server in rg-storage ---
	server1, err := NewServerNode(&ServerConfig{
		NodeID:        "multi-s1",
		ResourceGroup: "rg-storage",
		AZ:            "az-1",
		BindPort:      17990,
		ServicePort:   18120,
	})
	require.NoError(t, err)
	defer server1.Shutdown()

	// --- Server in rg-compute ---
	server2, err := NewServerNode(&ServerConfig{
		NodeID:        "multi-s2",
		ResourceGroup: "rg-compute",
		AZ:            "az-2",
		BindPort:      17991,
		ServicePort:   18121,
	})
	require.NoError(t, err)
	defer server2.Shutdown()

	// Join server2 to server1
	err = server2.Join([]string{"127.0.0.1:17990"})
	require.NoError(t, err)

	// --- Client joins ---
	client, err := NewClientNode(&ClientConfig{
		NodeID:   "multi-c1",
		BindAddr: "127.0.0.1",
		BindPort: 17992,
	})
	require.NoError(t, err)
	defer client.Shutdown()

	err = client.Join([]string{"127.0.0.1:17990"})
	require.NoError(t, err)

	// Wait for convergence
	assert.True(t, waitForConvergence(client.GetDiscovery(), 2, 5*time.Second))

	t.Log("=== Multi-RG cluster ===")
	printDiscoveryStatus(t, "Client", client.GetDiscovery())

	// Verify resource groups
	rgs := client.GetDiscovery().GetResourceGroups()
	assert.Len(t, rgs, 2)
	assert.Contains(t, rgs, "rg-storage")
	assert.Contains(t, rgs, "rg-compute")

	// Verify per-RG counts
	storageNodes := client.GetDiscovery().GetServersByResourceGroup("rg-storage")
	assert.Len(t, storageNodes, 1)
	assert.Equal(t, "multi-s1", storageNodes[0].NodeId)
	t.Log("rg-storage servers:")
	for _, n := range storageNodes {
		t.Logf("  - %s (AZ=%s)", n.NodeId, n.Az)
	}

	computeNodes := client.GetDiscovery().GetServersByResourceGroup("rg-compute")
	assert.Len(t, computeNodes, 1)
	assert.Equal(t, "multi-s2", computeNodes[0].NodeId)
	t.Log("rg-compute servers:")
	for _, n := range computeNodes {
		t.Logf("  - %s (AZ=%s)", n.NodeId, n.Az)
	}

	// Verify AZ distribution per RG
	t.Logf("rg-storage AZ distribution: %v", client.GetDiscovery().GetAZDistribution("rg-storage"))
	t.Logf("rg-compute AZ distribution: %v", client.GetDiscovery().GetAZDistribution("rg-compute"))

	// --- Remove the rg-compute server ---
	t.Log("=== rg-compute server leaving ===")
	err = server2.Leave()
	assert.NoError(t, err)

	assert.True(t, waitForNodeGone(client.GetDiscovery(), "multi-s2", 10*time.Second))
	printDiscoveryStatus(t, "Client after compute leave", client.GetDiscovery())

	// rg-compute should be gone
	computeAfter := client.GetDiscovery().GetServersByResourceGroup("rg-compute")
	assert.Empty(t, computeAfter)

	// rg-storage server should still be present
	assert.Contains(t, client.GetDiscovery().GetAllServers(), "multi-s1")

	server1.Leave()
	client.Leave()
}

// TestCluster_ServerUpdateMeta tests that when a server updates its metadata,
// other nodes in the cluster eventually see the updated metadata via gossip.
func TestCluster_ServerUpdateMeta(t *testing.T) {
	server1, err := NewServerNode(&ServerConfig{
		NodeID:        "update-s1",
		ResourceGroup: "rg-1",
		AZ:            "az-1",
		BindPort:      18000,
		ServicePort:   18130,
		Tags:          map[string]string{"version": "1"},
	})
	require.NoError(t, err)
	defer server1.Shutdown()

	client, err := NewClientNode(&ClientConfig{
		NodeID:   "update-c1",
		BindAddr: "127.0.0.1",
		BindPort: 18001,
	})
	require.NoError(t, err)
	defer client.Shutdown()

	err = client.Join([]string{"127.0.0.1:18000"})
	require.NoError(t, err)

	assert.True(t, waitForConvergence(client.GetDiscovery(), 1, 5*time.Second))
	t.Log("=== Before meta update ===")
	printDiscoveryStatus(t, "Client", client.GetDiscovery())

	// Verify initial tags
	initialMeta := client.GetDiscovery().GetAllServers()["update-s1"]
	require.NotNil(t, initialMeta)
	assert.Equal(t, "1", initialMeta.Tags["version"])

	// --- Server updates its metadata ---
	t.Log("=== Server updating metadata (version=2, new tags) ===")
	server1.UpdateMeta(map[string]interface{}{
		"tags": map[string]string{"version": "2", "env": "prod"},
	})

	// Wait for gossip to propagate the update
	deadline := time.Now().Add(5 * time.Second)
	updatedMeta := initialMeta
	for time.Now().Before(deadline) {
		m := client.GetDiscovery().GetAllServers()["update-s1"]
		if m != nil && m.Tags["version"] == "2" {
			updatedMeta = m
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Log("=== After meta update ===")
	printDiscoveryStatus(t, "Client", client.GetDiscovery())

	assert.Equal(t, "2", updatedMeta.Tags["version"], "client should see updated version tag")
	assert.Equal(t, "prod", updatedMeta.Tags["env"], "client should see new env tag")

	server1.Leave()
	client.Leave()
}

// TestCluster_PrintStatus verifies that PrintStatus methods do not panic and produce output.
func TestCluster_PrintStatus(t *testing.T) {
	server, err := NewServerNode(&ServerConfig{
		NodeID:        "print-s1",
		ResourceGroup: "rg-1",
		AZ:            "az-1",
		BindPort:      18010,
		ServicePort:   18140,
	})
	require.NoError(t, err)
	defer server.Shutdown()

	client, err := NewClientNode(&ClientConfig{
		NodeID:   "print-c1",
		BindAddr: "127.0.0.1",
		BindPort: 18011,
	})
	require.NoError(t, err)
	defer client.Shutdown()

	err = client.Join([]string{fmt.Sprintf("127.0.0.1:%d", 18010)})
	require.NoError(t, err)

	assert.True(t, waitForConvergence(client.GetDiscovery(), 1, 5*time.Second))

	// These should not panic
	assert.NotPanics(t, func() { server.PrintStatus() })
	assert.NotPanics(t, func() { client.PrintStatus() })

	// GetMemberlistStatus should contain member info
	status := server.GetMemberlistStatus()
	assert.Contains(t, status, "Total Members")
	assert.Contains(t, status, "print-s1")
	t.Logf("Server memberlist status:\n%s", status)

	server.Leave()
	client.Leave()
}

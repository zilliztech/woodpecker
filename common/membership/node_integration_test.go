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

func TestServerNode_CreateAndShutdown(t *testing.T) {
	config := &ServerConfig{
		NodeID:        "test-server-1",
		ResourceGroup: "rg-1",
		AZ:            "az-1",
		BindPort:      17946,
		ServicePort:   18080,
		Tags:          map[string]string{"env": "test"},
	}
	node, err := NewServerNode(config)
	require.NoError(t, err)
	require.NotNil(t, node)
	defer node.Shutdown()

	// Test getters
	assert.NotNil(t, node.GetDiscovery())
	assert.NotNil(t, node.GetMemberlist())
	assert.NotNil(t, node.GetServerCfg())
	assert.NotNil(t, node.GetMeta())
	assert.Equal(t, "test-server-1", node.GetMeta().NodeId)
	assert.Equal(t, "rg-1", node.GetMeta().ResourceGroup)
	assert.Equal(t, "az-1", node.GetMeta().Az)

	// Test status output
	status := node.GetMemberlistStatus()
	assert.Contains(t, status, "Total Members")

	// Test Join with empty list (no-op)
	err = node.Join([]string{})
	assert.NoError(t, err)

	// Test UpdateMeta
	node.UpdateMeta(map[string]any{
		"resource_group": "rg-2",
		"az":             "az-2",
		"tags":           map[string]string{"env": "prod"},
	})

	// Leave
	err = node.Leave()
	assert.NoError(t, err)
}

func TestServerNode_WithAdvertiseAddr(t *testing.T) {
	config := &ServerConfig{
		NodeID:               "test-server-advertise",
		ResourceGroup:        "rg-1",
		AZ:                   "az-1",
		BindPort:             17947,
		AdvertiseAddr:        "127.0.0.1",
		AdvertisePort:        17947,
		ServicePort:          18081,
		AdvertiseServiceAddr: "10.0.0.1",
		AdvertiseServicePort: 9090,
		Tags:                 map[string]string{"env": "test"},
	}
	node, err := NewServerNode(config)
	require.NoError(t, err)
	require.NotNil(t, node)
	defer node.Shutdown()

	// Verify service endpoint includes advertise addresses
	assert.Contains(t, node.GetMeta().Endpoint, "10.0.0.1:9090")

	err = node.Leave()
	assert.NoError(t, err)
}

func TestClientNode_CreateAndShutdown(t *testing.T) {
	config := &ClientConfig{
		NodeID:   "test-client-1",
		BindAddr: "127.0.0.1",
		BindPort: 17948,
	}
	node, err := NewClientNode(config)
	require.NoError(t, err)
	require.NotNil(t, node)
	defer node.Shutdown()

	// Test getters
	assert.NotNil(t, node.GetDiscovery())

	// Test Join with empty list (no-op)
	err = node.Join([]string{})
	assert.NoError(t, err)

	// Leave
	err = node.Leave()
	assert.NoError(t, err)
}

func TestServerAndClientNode_Join(t *testing.T) {
	// Create server node
	serverConfig := &ServerConfig{
		NodeID:        "join-server-1",
		ResourceGroup: "rg-1",
		AZ:            "az-1",
		BindPort:      17950,
		ServicePort:   18083,
		Tags:          map[string]string{"env": "test"},
	}
	server, err := NewServerNode(serverConfig)
	require.NoError(t, err)
	defer server.Shutdown()

	// Create client node and join the server
	clientConfig := &ClientConfig{
		NodeID:   "join-client-1",
		BindAddr: "127.0.0.1",
		BindPort: 17951,
	}
	client, err := NewClientNode(clientConfig)
	require.NoError(t, err)
	defer client.Shutdown()

	err = client.Join([]string{fmt.Sprintf("127.0.0.1:%d", serverConfig.BindPort)})
	assert.NoError(t, err)

	// Wait for gossip propagation
	time.Sleep(500 * time.Millisecond)

	// Client should discover the server
	servers := client.GetDiscovery().GetAllServers()
	assert.Contains(t, servers, "join-server-1")

	client.Leave()
	server.Leave()
}

func TestClientNode_SelectReplicas_InsufficientAZs(t *testing.T) {
	// Create a single server
	serverConfig := &ServerConfig{
		NodeID:        "replica-server-1",
		ResourceGroup: "rg-1",
		AZ:            "az-1",
		BindPort:      17952,
		ServicePort:   18084,
	}
	server, err := NewServerNode(serverConfig)
	require.NoError(t, err)
	defer server.Shutdown()

	// Create a client that joins the server
	clientConfig := &ClientConfig{
		NodeID:   "replica-client-1",
		BindAddr: "127.0.0.1",
		BindPort: 17953,
	}
	client, err := NewClientNode(clientConfig)
	require.NoError(t, err)
	defer client.Shutdown()

	err = client.Join([]string{fmt.Sprintf("127.0.0.1:%d", serverConfig.BindPort)})
	assert.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// SelectReplicas needs 3 AZs but only 1 exists
	_, replicaErr := client.SelectReplicas("rg-1")
	assert.Error(t, replicaErr)

	client.Leave()
	server.Leave()
}

func TestClientNode_Join_InvalidAddr(t *testing.T) {
	clientConfig := &ClientConfig{
		NodeID:   "test-client-invalid",
		BindAddr: "127.0.0.1",
		BindPort: 17955,
	}
	client, err := NewClientNode(clientConfig)
	require.NoError(t, err)
	defer client.Shutdown()

	// Join with invalid address should fail
	err = client.Join([]string{"127.0.0.1:99999"})
	assert.Error(t, err)

	client.Leave()
}

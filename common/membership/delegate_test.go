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
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/assert"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/proto"
)

// === ClientDelegate Tests ===

func TestNewClientDelegate(t *testing.T) {
	d := NewClientDelegate()
	assert.NotNil(t, d)
}

func TestClientDelegate_NodeMeta(t *testing.T) {
	d := NewClientDelegate()
	meta := d.NodeMeta(512)
	assert.Empty(t, meta)
}

func TestClientDelegate_NotifyMsg(t *testing.T) {
	d := NewClientDelegate()
	// Should not panic
	assert.NotPanics(t, func() {
		d.NotifyMsg([]byte("test"))
		d.NotifyMsg(nil)
	})
}

func TestClientDelegate_GetBroadcasts(t *testing.T) {
	d := NewClientDelegate()
	broadcasts := d.GetBroadcasts(10, 100)
	assert.Nil(t, broadcasts)
}

func TestClientDelegate_LocalState(t *testing.T) {
	d := NewClientDelegate()
	state := d.LocalState(true)
	assert.Empty(t, state)
	state = d.LocalState(false)
	assert.Empty(t, state)
}

func TestClientDelegate_MergeRemoteState(t *testing.T) {
	d := NewClientDelegate()
	assert.NotPanics(t, func() {
		d.MergeRemoteState([]byte("data"), true)
		d.MergeRemoteState(nil, false)
	})
}

// === ServerDelegate Tests ===

func TestNewServerDelegate(t *testing.T) {
	meta := &proto.NodeMeta{
		NodeId:        "node-1",
		ResourceGroup: "rg-1",
		Az:            "az-1",
		Endpoint:      "localhost:8080",
	}
	d := NewServerDelegate(meta)
	assert.NotNil(t, d)
}

func TestServerDelegate_NodeMeta(t *testing.T) {
	meta := &proto.NodeMeta{
		NodeId:        "node-1",
		ResourceGroup: "rg-1",
		Az:            "az-1",
		Endpoint:      "localhost:8080",
		Tags:          map[string]string{"env": "test"},
	}
	d := NewServerDelegate(meta)
	data := d.NodeMeta(4096)
	assert.NotEmpty(t, data)

	// Verify we can unmarshal the data back
	var decoded proto.NodeMeta
	err := pb.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, "node-1", decoded.NodeId)
	assert.Equal(t, "rg-1", decoded.ResourceGroup)
	assert.Equal(t, "az-1", decoded.Az)
	assert.Equal(t, int64(1), decoded.Version)
	assert.NotZero(t, decoded.LastUpdate)
}

func TestServerDelegate_NodeMeta_SmallLimit(t *testing.T) {
	meta := &proto.NodeMeta{
		NodeId:        "node-1",
		ResourceGroup: "rg-1",
		Az:            "az-1",
		Endpoint:      "localhost:8080",
	}
	d := NewServerDelegate(meta)
	// Normal limit should work
	data := d.NodeMeta(4096)
	assert.NotEmpty(t, data)
}

func TestServerDelegate_LocalState(t *testing.T) {
	meta := &proto.NodeMeta{
		NodeId:        "node-1",
		ResourceGroup: "rg-1",
		Az:            "az-1",
		Endpoint:      "localhost:8080",
	}
	d := NewServerDelegate(meta)

	state := d.LocalState(true)
	assert.NotEmpty(t, state)

	var decoded proto.NodeMeta
	err := pb.Unmarshal(state, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, "node-1", decoded.NodeId)
}

func TestServerDelegate_NotifyMsg(t *testing.T) {
	meta := &proto.NodeMeta{NodeId: "n1"}
	d := NewServerDelegate(meta)
	assert.NotPanics(t, func() {
		d.NotifyMsg([]byte("test"))
	})
}

func TestServerDelegate_GetBroadcasts(t *testing.T) {
	meta := &proto.NodeMeta{NodeId: "n1"}
	d := NewServerDelegate(meta)
	assert.Nil(t, d.GetBroadcasts(10, 100))
}

func TestServerDelegate_MergeRemoteState(t *testing.T) {
	meta := &proto.NodeMeta{NodeId: "n1"}
	d := NewServerDelegate(meta)
	assert.NotPanics(t, func() {
		d.MergeRemoteState([]byte("data"), true)
	})
}

func TestServerDelegate_UpdateMeta(t *testing.T) {
	meta := &proto.NodeMeta{
		NodeId:        "node-1",
		ResourceGroup: "rg-1",
		Az:            "az-1",
		Endpoint:      "localhost:8080",
		Tags:          map[string]string{"env": "test"},
	}
	d := NewServerDelegate(meta)

	// Update resource group
	d.UpdateMeta(map[string]interface{}{
		"resource_group": "rg-2",
	})
	data := d.NodeMeta(4096)
	var decoded proto.NodeMeta
	err := pb.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, "rg-2", decoded.ResourceGroup)
	assert.Equal(t, int64(2), decoded.Version) // version should be incremented from initial 1 to 2

	// Update AZ
	d.UpdateMeta(map[string]interface{}{
		"az": "az-2",
	})
	data = d.NodeMeta(4096)
	err = pb.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, "az-2", decoded.Az)

	// Update tags
	d.UpdateMeta(map[string]interface{}{
		"tags": map[string]string{"env": "prod", "version": "2"},
	})
	data = d.NodeMeta(4096)
	err = pb.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, "prod", decoded.Tags["env"])
	assert.Equal(t, "2", decoded.Tags["version"])
}

// === EventDelegate Tests ===

func TestNewEventDelegate(t *testing.T) {
	sd := NewServiceDiscovery()
	ed := NewEventDelegate(sd, RoleServer, "localhost:8080")
	assert.NotNil(t, ed)
}

func TestEventDelegate_NotifyJoin_ServerWithMeta(t *testing.T) {
	sd := NewServiceDiscovery()
	ed := NewEventDelegate(sd, RoleClient, "localhost:8080")

	meta := &proto.NodeMeta{
		NodeId:        "server-1",
		ResourceGroup: "rg-1",
		Az:            "az-1",
		Endpoint:      "server-1:8080",
	}
	metaData, _ := pb.Marshal(meta)

	node := &memberlist.Node{
		Name: "server-1",
		Meta: metaData,
	}
	ed.NotifyJoin(node)

	// Verify server was added to discovery
	servers := sd.GetAllServers()
	assert.Contains(t, servers, "server-1")
	assert.Equal(t, "rg-1", servers["server-1"].ResourceGroup)
}

func TestEventDelegate_NotifyJoin_ClientWithoutMeta(t *testing.T) {
	sd := NewServiceDiscovery()
	ed := NewEventDelegate(sd, RoleServer, "localhost:8080")

	node := &memberlist.Node{
		Name: "client-1",
		Meta: nil,
	}
	ed.NotifyJoin(node)

	// Client should not be added to discovery
	servers := sd.GetAllServers()
	assert.NotContains(t, servers, "client-1")
}

func TestEventDelegate_NotifyJoin_InvalidMeta(t *testing.T) {
	sd := NewServiceDiscovery()
	ed := NewEventDelegate(sd, RoleServer, "localhost:8080")

	node := &memberlist.Node{
		Name: "bad-node",
		Meta: []byte("not valid protobuf"),
	}
	// Should not panic, just log error
	assert.NotPanics(t, func() {
		ed.NotifyJoin(node)
	})

	servers := sd.GetAllServers()
	assert.NotContains(t, servers, "bad-node")
}

func TestEventDelegate_NotifyLeave(t *testing.T) {
	sd := NewServiceDiscovery()
	ed := NewEventDelegate(sd, RoleClient, "localhost:8080")

	// Add a server first
	sd.UpdateServer("server-1", &proto.NodeMeta{
		NodeId:        "server-1",
		ResourceGroup: "rg-1",
		Az:            "az-1",
	})
	assert.Len(t, sd.GetAllServers(), 1)

	// Notify leave
	node := &memberlist.Node{Name: "server-1"}
	ed.NotifyLeave(node)

	assert.Len(t, sd.GetAllServers(), 0)
}

func TestEventDelegate_NotifyUpdate_ServerWithMeta(t *testing.T) {
	sd := NewServiceDiscovery()
	ed := NewEventDelegate(sd, RoleServer, "localhost:8080")

	// Add initial server
	sd.UpdateServer("server-1", &proto.NodeMeta{
		NodeId:        "server-1",
		ResourceGroup: "rg-1",
		Az:            "az-1",
		Endpoint:      "server-1:8080",
		Version:       1,
	})

	// Update with new metadata
	updatedMeta := &proto.NodeMeta{
		NodeId:        "server-1",
		ResourceGroup: "rg-2",
		Az:            "az-2",
		Endpoint:      "server-1:8080",
		Version:       2,
	}
	metaData, _ := pb.Marshal(updatedMeta)
	node := &memberlist.Node{
		Name: "server-1",
		Meta: metaData,
	}
	ed.NotifyUpdate(node)

	servers := sd.GetAllServers()
	assert.Equal(t, "rg-2", servers["server-1"].ResourceGroup)
	assert.Equal(t, "az-2", servers["server-1"].Az)
}

func TestEventDelegate_NotifyUpdate_NoMeta(t *testing.T) {
	sd := NewServiceDiscovery()
	ed := NewEventDelegate(sd, RoleClient, "localhost:8080")

	node := &memberlist.Node{
		Name: "client-1",
		Meta: nil,
	}
	// Should not panic
	assert.NotPanics(t, func() {
		ed.NotifyUpdate(node)
	})
}

// === NotifyLeave stale event guard Tests ===

func TestEventDelegate_NotifyLeave_StaleLeaveIgnored(t *testing.T) {
	sd := NewServiceDiscovery()
	ed := NewEventDelegate(sd, RoleClient, "localhost:9000")

	// Step 1: Old node joins
	oldMeta := &proto.NodeMeta{
		NodeId:        "s1",
		ResourceGroup: "rg-1",
		Az:            "az-1",
		Endpoint:      "10.0.0.1:8080",
		LastUpdate:    1000,
	}
	oldMetaData, _ := pb.Marshal(oldMeta)
	ed.NotifyJoin(&memberlist.Node{Name: "s1", Meta: oldMetaData})
	assert.Len(t, sd.GetAllServers(), 1)

	// Step 2: New incarnation joins (simulates rejoin with different endpoint)
	newMeta := &proto.NodeMeta{
		NodeId:        "s1",
		ResourceGroup: "rg-1",
		Az:            "az-2",
		Endpoint:      "10.0.0.1:8081",
		LastUpdate:    2000,
	}
	newMetaData, _ := pb.Marshal(newMeta)
	ed.NotifyJoin(&memberlist.Node{Name: "s1", Meta: newMetaData})
	assert.Len(t, sd.GetAllServers(), 1)
	assert.Equal(t, "az-2", sd.GetAllServers()["s1"].Az)

	// Step 3: Stale NotifyLeave arrives with OLD metadata
	ed.NotifyLeave(&memberlist.Node{Name: "s1", Meta: oldMetaData})

	// New incarnation should still be present
	servers := sd.GetAllServers()
	assert.Len(t, servers, 1, "stale leave should not remove the new incarnation")
	assert.Equal(t, "10.0.0.1:8081", servers["s1"].Endpoint)
	assert.Equal(t, "az-2", servers["s1"].Az)
}

func TestEventDelegate_NotifyLeave_NormalLeaveRemoves(t *testing.T) {
	sd := NewServiceDiscovery()
	ed := NewEventDelegate(sd, RoleServer, "localhost:9000")

	meta := &proto.NodeMeta{
		NodeId:        "s1",
		ResourceGroup: "rg-1",
		Az:            "az-1",
		Endpoint:      "10.0.0.1:8080",
		LastUpdate:    1000,
	}
	metaData, _ := pb.Marshal(meta)
	ed.NotifyJoin(&memberlist.Node{Name: "s1", Meta: metaData})
	assert.Len(t, sd.GetAllServers(), 1)

	// Normal leave with matching metadata — should remove
	ed.NotifyLeave(&memberlist.Node{Name: "s1", Meta: metaData})
	assert.Empty(t, sd.GetAllServers())
}

func TestEventDelegate_NotifyLeave_NoMetaFallback(t *testing.T) {
	sd := NewServiceDiscovery()
	ed := NewEventDelegate(sd, RoleClient, "localhost:9000")

	// Add server directly
	sd.UpdateServer("s1", &proto.NodeMeta{
		NodeId:        "s1",
		ResourceGroup: "rg-1",
		Az:            "az-1",
		Endpoint:      "10.0.0.1:8080",
	})
	assert.Len(t, sd.GetAllServers(), 1)

	// Leave with no metadata (e.g. client node) — should fall back to unconditional remove
	ed.NotifyLeave(&memberlist.Node{Name: "s1", Meta: nil})
	assert.Empty(t, sd.GetAllServers())
}

func TestEventDelegate_NotifyLeave_InvalidMetaFallback(t *testing.T) {
	sd := NewServiceDiscovery()
	ed := NewEventDelegate(sd, RoleClient, "localhost:9000")

	sd.UpdateServer("s1", &proto.NodeMeta{
		NodeId:        "s1",
		ResourceGroup: "rg-1",
		Az:            "az-1",
		Endpoint:      "10.0.0.1:8080",
	})
	assert.Len(t, sd.GetAllServers(), 1)

	// Leave with invalid protobuf — should fall back to unconditional remove
	ed.NotifyLeave(&memberlist.Node{Name: "s1", Meta: []byte("invalid protobuf")})
	assert.Empty(t, sd.GetAllServers())
}

func TestEventDelegate_NotifyLeave_FullRejoinSequence(t *testing.T) {
	sd := NewServiceDiscovery()
	ed := NewEventDelegate(sd, RoleClient, "localhost:9000")

	// Simulate the exact sequence that causes the bug:
	// 1. s1 and s2 join
	s1Meta := &proto.NodeMeta{NodeId: "s1", ResourceGroup: "rg-1", Az: "az-1", Endpoint: "10.0.0.1:8080", LastUpdate: 1000}
	s2OldMeta := &proto.NodeMeta{NodeId: "s2", ResourceGroup: "rg-1", Az: "az-2", Endpoint: "10.0.0.1:8081", LastUpdate: 1000}
	s1Data, _ := pb.Marshal(s1Meta)
	s2OldData, _ := pb.Marshal(s2OldMeta)

	ed.NotifyJoin(&memberlist.Node{Name: "s1", Meta: s1Data})
	ed.NotifyJoin(&memberlist.Node{Name: "s2", Meta: s2OldData})
	assert.Len(t, sd.GetAllServers(), 2)

	// 2. s2 leaves normally
	ed.NotifyLeave(&memberlist.Node{Name: "s2", Meta: s2OldData})
	assert.Len(t, sd.GetAllServers(), 1)
	assert.NotContains(t, sd.GetAllServers(), "s2")

	// 3. s2 rejoins with new AZ and endpoint
	s2NewMeta := &proto.NodeMeta{NodeId: "s2", ResourceGroup: "rg-1", Az: "az-3", Endpoint: "10.0.0.1:8082", LastUpdate: 2000}
	s2NewData, _ := pb.Marshal(s2NewMeta)
	ed.NotifyJoin(&memberlist.Node{Name: "s2", Meta: s2NewData})
	assert.Len(t, sd.GetAllServers(), 2)
	assert.Equal(t, "az-3", sd.GetAllServers()["s2"].Az)

	// 4. Stale leave for OLD s2 arrives (the suspect→dead timeout from memberlist)
	ed.NotifyLeave(&memberlist.Node{Name: "s2", Meta: s2OldData})

	// Both servers should still be present
	servers := sd.GetAllServers()
	assert.Len(t, servers, 2, "stale leave must not remove the rejoined node")
	assert.Equal(t, "az-1", servers["s1"].Az)
	assert.Equal(t, "az-3", servers["s2"].Az)
	assert.Equal(t, "10.0.0.1:8082", servers["s2"].Endpoint)

	// Verify indexes are consistent
	dist := sd.GetAZDistribution("rg-1")
	assert.Equal(t, 1, dist["az-1"])
	assert.Equal(t, 1, dist["az-3"])
}

// === NodeRole Tests ===

func TestNodeRole(t *testing.T) {
	assert.Equal(t, NodeRole("server"), RoleServer)
	assert.Equal(t, NodeRole("client"), RoleClient)
	assert.NotEqual(t, RoleServer, RoleClient)
}

// === ServiceDiscovery Compatibility Method Tests ===

func TestServiceDiscovery_GetAllServers(t *testing.T) {
	sd := NewServiceDiscovery()
	meta1 := &proto.NodeMeta{NodeId: "n1", ResourceGroup: "rg1", Az: "az1", Endpoint: "n1:8080", LastUpdate: time.Now().UnixMilli()}
	meta2 := &proto.NodeMeta{NodeId: "n2", ResourceGroup: "rg1", Az: "az2", Endpoint: "n2:8080", LastUpdate: time.Now().UnixMilli()}
	sd.UpdateServer("n1", meta1)
	sd.UpdateServer("n2", meta2)

	servers := sd.GetAllServers()
	assert.Len(t, servers, 2)
	assert.Contains(t, servers, "n1")
	assert.Contains(t, servers, "n2")

	// Verify it's a copy (modifying result should not affect original)
	delete(servers, "n1")
	assert.Len(t, sd.GetAllServers(), 2)
}

func TestServiceDiscovery_GetResourceGroups(t *testing.T) {
	sd := NewServiceDiscovery()
	sd.UpdateServer("n1", &proto.NodeMeta{NodeId: "n1", ResourceGroup: "rg1", Az: "az1"})
	sd.UpdateServer("n2", &proto.NodeMeta{NodeId: "n2", ResourceGroup: "rg2", Az: "az1"})
	sd.UpdateServer("n3", &proto.NodeMeta{NodeId: "n3", ResourceGroup: "rg1", Az: "az2"})

	rgs := sd.GetResourceGroups()
	assert.Len(t, rgs, 2)
	assert.Contains(t, rgs, "rg1")
	assert.Contains(t, rgs, "rg2")
}

func TestServiceDiscovery_GetServersByResourceGroup(t *testing.T) {
	sd := NewServiceDiscovery()
	sd.UpdateServer("n1", &proto.NodeMeta{NodeId: "n1", ResourceGroup: "rg1", Az: "az1"})
	sd.UpdateServer("n2", &proto.NodeMeta{NodeId: "n2", ResourceGroup: "rg1", Az: "az2"})
	sd.UpdateServer("n3", &proto.NodeMeta{NodeId: "n3", ResourceGroup: "rg2", Az: "az1"})

	nodes := sd.GetServersByResourceGroup("rg1")
	assert.Len(t, nodes, 2)

	nodes = sd.GetServersByResourceGroup("rg2")
	assert.Len(t, nodes, 1)
	assert.Equal(t, "n3", nodes[0].NodeId)

	nodes = sd.GetServersByResourceGroup("nonexistent")
	assert.Empty(t, nodes)
}

func TestServiceDiscovery_GetAZDistribution(t *testing.T) {
	sd := NewServiceDiscovery()
	sd.UpdateServer("n1", &proto.NodeMeta{NodeId: "n1", ResourceGroup: "rg1", Az: "az1"})
	sd.UpdateServer("n2", &proto.NodeMeta{NodeId: "n2", ResourceGroup: "rg1", Az: "az1"})
	sd.UpdateServer("n3", &proto.NodeMeta{NodeId: "n3", ResourceGroup: "rg1", Az: "az2"})

	dist := sd.GetAZDistribution("rg1")
	assert.Equal(t, 2, dist["az1"])
	assert.Equal(t, 1, dist["az2"])

	dist = sd.GetAZDistribution("nonexistent")
	assert.Empty(t, dist)
}

func TestServiceDiscovery_SelectServersAcrossAZ(t *testing.T) {
	sd := NewServiceDiscovery()
	sd.UpdateServer("n1", &proto.NodeMeta{NodeId: "n1", ResourceGroup: "rg1", Az: "az1", Endpoint: "n1:8080"})
	sd.UpdateServer("n2", &proto.NodeMeta{NodeId: "n2", ResourceGroup: "rg1", Az: "az2", Endpoint: "n2:8080"})
	sd.UpdateServer("n3", &proto.NodeMeta{NodeId: "n3", ResourceGroup: "rg1", Az: "az3", Endpoint: "n3:8080"})

	// Select 3 servers across different AZs
	servers, azs, err := sd.SelectServersAcrossAZ("rg1", 3)
	assert.NoError(t, err)
	assert.Len(t, servers, 3)
	assert.Len(t, azs, 3)

	// Verify all AZs are different
	azSet := make(map[string]bool)
	for _, az := range azs {
		azSet[az] = true
	}
	assert.Len(t, azSet, 3)
}

func TestServiceDiscovery_SelectServersAcrossAZ_InsufficientAZs(t *testing.T) {
	sd := NewServiceDiscovery()
	sd.UpdateServer("n1", &proto.NodeMeta{NodeId: "n1", ResourceGroup: "rg1", Az: "az1"})
	sd.UpdateServer("n2", &proto.NodeMeta{NodeId: "n2", ResourceGroup: "rg1", Az: "az2"})

	_, _, err := sd.SelectServersAcrossAZ("rg1", 3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient AZs")
}

func TestServiceDiscovery_SelectServersAcrossAZ_NonexistentRG(t *testing.T) {
	sd := NewServiceDiscovery()
	_, _, err := sd.SelectServersAcrossAZ("nonexistent", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// === getCandidateRGs with regex Tests ===

func TestServiceDiscovery_GetCandidateRGs_Regex(t *testing.T) {
	sd := NewServiceDiscovery()
	sd.UpdateServer("n1", &proto.NodeMeta{NodeId: "n1", ResourceGroup: "prod-rg1", Az: "az1"})
	sd.UpdateServer("n2", &proto.NodeMeta{NodeId: "n2", ResourceGroup: "prod-rg2", Az: "az1"})
	sd.UpdateServer("n3", &proto.NodeMeta{NodeId: "n3", ResourceGroup: "staging-rg1", Az: "az1"})

	filter := &proto.NodeFilter{ResourceGroup: "prod-.*"}
	rgs := sd.getCandidateRGs(filter)
	assert.Len(t, rgs, 2)
	assert.Contains(t, rgs, "prod-rg1")
	assert.Contains(t, rgs, "prod-rg2")
}

func TestServiceDiscovery_GetCandidateRGs_ExactMatch(t *testing.T) {
	sd := NewServiceDiscovery()
	sd.UpdateServer("n1", &proto.NodeMeta{NodeId: "n1", ResourceGroup: "rg1", Az: "az1"})
	sd.UpdateServer("n2", &proto.NodeMeta{NodeId: "n2", ResourceGroup: "rg2", Az: "az1"})

	filter := &proto.NodeFilter{ResourceGroup: "rg1"}
	rgs := sd.getCandidateRGs(filter)
	assert.Len(t, rgs, 1)
	assert.Equal(t, "rg1", rgs[0])

	// Non-matching exact
	filter = &proto.NodeFilter{ResourceGroup: "rg3"}
	rgs = sd.getCandidateRGs(filter)
	assert.Empty(t, rgs)
}

func TestServiceDiscovery_GetCandidateRGs_Empty(t *testing.T) {
	sd := NewServiceDiscovery()
	sd.UpdateServer("n1", &proto.NodeMeta{NodeId: "n1", ResourceGroup: "rg1", Az: "az1"})

	filter := &proto.NodeFilter{ResourceGroup: ""}
	rgs := sd.getCandidateRGs(filter)
	assert.Len(t, rgs, 1) // returns all RGs
}

// === getCandidateAZsInRG Tests ===

func TestServiceDiscovery_GetCandidateAZsInRG_Regex(t *testing.T) {
	sd := NewServiceDiscovery()
	sd.UpdateServer("n1", &proto.NodeMeta{NodeId: "n1", ResourceGroup: "rg1", Az: "us-east-1a"})
	sd.UpdateServer("n2", &proto.NodeMeta{NodeId: "n2", ResourceGroup: "rg1", Az: "us-east-1b"})
	sd.UpdateServer("n3", &proto.NodeMeta{NodeId: "n3", ResourceGroup: "rg1", Az: "eu-west-1a"})

	filter := &proto.NodeFilter{Az: "us-east-.*"}
	azs := sd.getCandidateAZsInRG("rg1", filter)
	assert.Len(t, azs, 2)
	assert.Contains(t, azs, "us-east-1a")
	assert.Contains(t, azs, "us-east-1b")
}

func TestServiceDiscovery_GetCandidateAZsInRG_ExactMatch(t *testing.T) {
	sd := NewServiceDiscovery()
	sd.UpdateServer("n1", &proto.NodeMeta{NodeId: "n1", ResourceGroup: "rg1", Az: "az1"})
	sd.UpdateServer("n2", &proto.NodeMeta{NodeId: "n2", ResourceGroup: "rg1", Az: "az2"})

	filter := &proto.NodeFilter{Az: "az1"}
	azs := sd.getCandidateAZsInRG("rg1", filter)
	assert.Len(t, azs, 1)
	assert.Equal(t, "az1", azs[0])
}

func TestServiceDiscovery_GetCandidateAZsInRG_NonexistentRG(t *testing.T) {
	sd := NewServiceDiscovery()
	filter := &proto.NodeFilter{Az: "az1"}
	azs := sd.getCandidateAZsInRG("nonexistent", filter)
	assert.Empty(t, azs)
}

func TestServiceDiscovery_GetCandidateAZsInRG_Empty(t *testing.T) {
	sd := NewServiceDiscovery()
	sd.UpdateServer("n1", &proto.NodeMeta{NodeId: "n1", ResourceGroup: "rg1", Az: "az1"})

	filter := &proto.NodeFilter{Az: ""}
	azs := sd.getCandidateAZsInRG("rg1", filter)
	assert.Len(t, azs, 1) // returns all AZs in this RG
}

// === removeNodeFromSlice Tests ===

func TestServiceDiscovery_RemoveNodeFromSlice(t *testing.T) {
	sd := NewServiceDiscovery()

	nodes := []*proto.NodeMeta{
		{NodeId: "n1"},
		{NodeId: "n2"},
		{NodeId: "n3"},
	}

	// Remove from middle
	result := sd.removeNodeFromSlice(nodes, "n2")
	assert.Len(t, result, 2)

	// Remove non-existent
	result = sd.removeNodeFromSlice(nodes, "n99")
	assert.Len(t, result, 3) // unchanged
}

// === isRegexLike Tests ===

func TestServiceDiscovery_IsRegexLike(t *testing.T) {
	sd := NewServiceDiscovery()

	assert.True(t, sd.isRegexLike("az-[0-9]+"))
	assert.True(t, sd.isRegexLike("prod-.*"))
	assert.True(t, sd.isRegexLike("rg1|rg2"))
	assert.True(t, sd.isRegexLike("rg?"))
	assert.True(t, sd.isRegexLike("rg(1)"))
	assert.True(t, sd.isRegexLike("^rg1$"))
	assert.True(t, sd.isRegexLike("rg{1,3}"))
	assert.True(t, sd.isRegexLike("rg\\d"))

	assert.False(t, sd.isRegexLike("rg1"))
	assert.False(t, sd.isRegexLike("simple-name"))
	assert.False(t, sd.isRegexLike("us-east-1a"))
	assert.False(t, sd.isRegexLike(""))
}

// === RemoveServer Tests ===

func TestServiceDiscovery_RemoveServer_CleanupIndexes(t *testing.T) {
	sd := NewServiceDiscovery()
	sd.UpdateServer("n1", &proto.NodeMeta{NodeId: "n1", ResourceGroup: "rg1", Az: "az1"})

	assert.Len(t, sd.GetResourceGroups(), 1)
	assert.Len(t, sd.GetAllServers(), 1)

	sd.RemoveServer("n1")

	assert.Len(t, sd.GetResourceGroups(), 0)
	assert.Len(t, sd.GetAllServers(), 0)
}

func TestServiceDiscovery_RemoveServer_Nonexistent(t *testing.T) {
	sd := NewServiceDiscovery()
	// Should not panic
	assert.NotPanics(t, func() {
		sd.RemoveServer("nonexistent")
	})
}

// === UpdateServer overwrite Tests ===

func TestServiceDiscovery_UpdateServer_Overwrite(t *testing.T) {
	sd := NewServiceDiscovery()
	sd.UpdateServer("n1", &proto.NodeMeta{NodeId: "n1", ResourceGroup: "rg1", Az: "az1"})
	assert.Equal(t, 1, sd.GetAZDistribution("rg1")["az1"])

	// Update the same node to a different RG/AZ
	sd.UpdateServer("n1", &proto.NodeMeta{NodeId: "n1", ResourceGroup: "rg2", Az: "az2"})

	// Old RG should be cleaned up
	assert.Empty(t, sd.GetAZDistribution("rg1"))
	assert.Equal(t, 1, sd.GetAZDistribution("rg2")["az2"])
}

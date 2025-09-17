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
	"log"
	"time"

	ml "github.com/hashicorp/memberlist"

	"github.com/zilliztech/woodpecker/common/net"
	"github.com/zilliztech/woodpecker/proto"
)

const (
	NodeMetaVersion = 1
)

// ServerNode Server node
type ServerNode struct {
	// main p2p memberlist
	memberlist *ml.Memberlist
	// node meta update hook
	delegate *ServerDelegate
	// node join/leave event hook
	eventDel *EventDelegate
	// service discovery for node list
	discovery *ServiceDiscovery
	// node metadata
	meta *proto.NodeMeta
	// server cfg
	serverConfig *ServerConfig
}

// ServerConfig Server configuration
type ServerConfig struct {
	// node info
	NodeID        string
	ResourceGroup string
	AZ            string
	Tags          map[string]string

	// gossip related - for internal cluster communication
	BindPort      int
	AdvertiseAddr string
	AdvertisePort int

	// service related - for client-server communication
	ServicePort          int
	AdvertiseServiceAddr string
	AdvertiseServicePort int
}

func NewServerNode(config *ServerConfig) (*ServerNode, error) {
	// Auto-detect local IP for advertise addresses if not specified
	localIP := net.GetLocalIP()

	// Build service endpoint for client connections
	// Use service advertise address if configured, otherwise use detected local IP
	endpointAddr := localIP
	if config.AdvertiseServiceAddr != "" {
		endpointAddr = config.AdvertiseServiceAddr
	}

	endpointPort := config.ServicePort
	if config.AdvertiseServicePort > 0 {
		endpointPort = config.AdvertiseServicePort
	}

	meta := &proto.NodeMeta{
		NodeId:        config.NodeID,
		ResourceGroup: config.ResourceGroup,
		Az:            config.AZ,
		Endpoint:      fmt.Sprintf("%s:%d", endpointAddr, endpointPort),
		Tags:          config.Tags,
		LastUpdate:    time.Now().UnixMilli(), // Convert to Unix timestamp in milliseconds
		Version:       NodeMetaVersion,
	}
	discovery := NewServiceDiscovery()
	delegate := NewServerDelegate(meta)
	eventDel := NewEventDelegate(discovery, RoleServer)

	mlConfig := ml.DefaultLocalConfig()
	mlConfig.Name = config.NodeID // unique identifier name for a node
	mlConfig.BindAddr = "0.0.0.0" // always bind to 0.0.0.0 for all interfaces
	mlConfig.BindPort = config.BindPort

	// Configure advertise addresses for gossip communication (Docker bridge networking)
	gossipAdvertiseAddr := localIP
	if config.AdvertiseAddr != "" {
		gossipAdvertiseAddr = config.AdvertiseAddr
	}
	mlConfig.AdvertiseAddr = gossipAdvertiseAddr

	if config.AdvertisePort > 0 {
		mlConfig.AdvertisePort = config.AdvertisePort
	}

	mlConfig.Delegate = delegate                     // behavior delegate for node, different for each instance
	mlConfig.Events = eventDel                       // event listener for node join/leave
	mlConfig.LogOutput = log.Writer()                // set log output, TODO can refer to this, we should also provide custom logger setting for wp later, one global logger is enough
	mlConfig.GossipInterval = 200 * time.Millisecond // gossip interval time
	mlConfig.GossipNodes = 3                         // number of randomly selected chat nodes for one gossip round

	list, err := ml.Create(mlConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}
	discovery.UpdateServer(config.NodeID, meta) // cache for known node meta information list

	return &ServerNode{
		memberlist:   list,
		delegate:     delegate,
		eventDel:     eventDel,
		discovery:    discovery,
		meta:         meta,
		serverConfig: config,
	}, nil
}

func (n *ServerNode) Join(existing []string) error {
	if len(existing) > 0 {
		count, err := n.memberlist.Join(existing)
		if err != nil {
			return fmt.Errorf("failed to join cluster: %w", err)
		}
		log.Printf("[SERVER] Successfully joined %d nodes", count)
	}
	return nil
}

func (n *ServerNode) UpdateMeta(updates map[string]interface{}) {
	n.delegate.UpdateMeta(updates)
	_ = n.memberlist.UpdateNode(5 * time.Second)
}

func (n *ServerNode) GetDiscovery() *ServiceDiscovery { return n.discovery }

func (n *ServerNode) PrintStatus() {
	fmt.Println("\n╔════════════════════════════════════════╗")
	fmt.Println("║          SERVER STATUS                 ║")
	fmt.Println("╚════════════════════════════════════════╝")
	members := n.memberlist.Members()
	fmt.Printf("\n📊 Cluster Members: %d\n", len(members))
	groups := n.discovery.GetResourceGroups()
	fmt.Printf("📁 Resource Groups: %d\n", len(groups))
	for _, rg := range groups {
		servers := n.discovery.GetServersByResourceGroup(rg)
		azDist := n.discovery.GetAZDistribution(rg)
		fmt.Printf("\n[%s] - %d servers\n", rg, len(servers))
		fmt.Println("  AZ Distribution:")
		for az, count := range azDist {
			fmt.Printf("    • %s: %d servers\n", az, count)
		}
		replicas, azs, err := n.discovery.SelectServersAcrossAZ(rg, 3)
		if err == nil {
			fmt.Printf("  ✅ 3-Replica Available: ")
			for i, replica := range replicas {
				fmt.Printf("%s(%s) ", replica.NodeId, azs[i])
			}
			fmt.Println()
		} else {
			fmt.Printf("  ❌ 3-Replica NOT Available: %v\n", err)
		}
	}
}

func (n *ServerNode) Leave() error    { return n.memberlist.Leave(5 * time.Second) }
func (n *ServerNode) Shutdown() error { return n.memberlist.Shutdown() }

func (n *ServerNode) GetMemberlist() *ml.Memberlist {
	return n.memberlist
}

func (n *ServerNode) GetServerCfg() *ServerConfig {
	return n.serverConfig
}

func (n *ServerNode) GetMeta() *proto.NodeMeta {
	return n.meta
}

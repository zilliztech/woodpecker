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
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
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
	// load reporter lifecycle
	loadCtx    context.Context
	loadCancel context.CancelFunc
	loadWG     sync.WaitGroup
	sampler    LoadSampler
}

// ServerConfig Server configuration
type ServerConfig struct {
	// node info
	NodeID        string
	ClusterName   string
	Region        string
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

	// Load-aware selection (issue #114). LoadReportInterval == 0 disables reporting.
	LoadReportInterval time.Duration
	MemSoftThreshold   float64 // memory ratio above which memory escalates load; default 0.85
	EWMAAlpha          float64 // EWMA weight on newest sample; default 0.5
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

	tags := make(map[string]string, len(config.Tags))
	for k, v := range config.Tags {
		tags[k] = v
	}

	meta := &proto.NodeMeta{
		NodeId:        config.NodeID,
		ResourceGroup: config.ResourceGroup,
		Az:            config.AZ,
		Endpoint:      fmt.Sprintf("%s:%d", endpointAddr, endpointPort),
		Tags:          tags,
		LastUpdate:    time.Now().UnixMilli(),
		Version:       NodeMetaVersion,
		ClusterName:   config.ClusterName,
		Region:        config.Region,
	}
	discovery := NewServiceDiscovery()
	delegate := NewServerDelegate(meta)
	eventDel := NewEventDelegate(discovery, RoleServer, fmt.Sprintf("%s:%d", endpointAddr, endpointPort))

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
	discovery.UpdateServer(config.NodeID, delegate.SnapshotMeta()) // store a snapshot, not the live meta pointer

	loadCtx, loadCancel := context.WithCancel(context.Background())
	node := &ServerNode{
		memberlist:   list,
		delegate:     delegate,
		eventDel:     eventDel,
		discovery:    discovery,
		meta:         meta,
		serverConfig: config,
		loadCtx:      loadCtx,
		loadCancel:   loadCancel,
	}
	if config.LoadReportInterval > 0 {
		node.sampler = NewSystemLoadSampler(config.MemSoftThreshold, config.EWMAAlpha)
		node.startLoadReporter(config.LoadReportInterval)
	}
	return node, nil
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

// startLoadReporter periodically samples local load, publishes it into the
// gossip meta, and refreshes the local discovery copy so this node's own
// load is visible to its own selections too.
func (n *ServerNode) startLoadReporter(interval time.Duration) {
	n.loadWG.Add(1)
	go func() {
		defer n.loadWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		n.reportLoadOnce()
		for {
			select {
			case <-n.loadCtx.Done():
				return
			case <-ticker.C:
				n.reportLoadOnce()
			}
		}
	}()
}

func (n *ServerNode) reportLoadOnce() {
	if n.sampler == nil {
		return
	}
	n.publishLoad()
	// Trigger gossip of the refreshed meta. Best-effort; ignore timeout errors.
	_ = n.memberlist.UpdateNode(2 * time.Second)
	// Keep our own discovery copy current for local selections. Store a snapshot,
	// not the live meta pointer, so the reporter's writes don't race with selectors.
	snap := n.delegate.SnapshotMeta()
	n.discovery.UpdateServer(snap.GetNodeId(), snap)
}

// publishLoad samples load and writes it into the gossip meta (no I/O).
func (n *ServerNode) publishLoad() {
	if n.sampler == nil {
		return
	}
	n.delegate.SetLoadFactor(n.sampler.Sample())
}

func (n *ServerNode) Leave() error {
	if n.loadCancel != nil {
		n.loadCancel()
	}
	return n.memberlist.Leave(5 * time.Second)
}

func (n *ServerNode) Shutdown() error {
	if n.loadCancel != nil {
		n.loadCancel()
	}
	n.loadWG.Wait()
	return n.memberlist.Shutdown()
}

func (n *ServerNode) GetMemberlist() *ml.Memberlist {
	return n.memberlist
}

func (n *ServerNode) GetServerCfg() *ServerConfig {
	return n.serverConfig
}

func (n *ServerNode) GetMeta() *proto.NodeMeta {
	return n.meta
}

// GetMemberlistStatus returns a formatted string of all memberlist members and their addresses
func (n *ServerNode) GetMemberlistStatus() string {
	members := n.memberlist.Members()
	if len(members) == 0 {
		return "No members in memberlist"
	}

	result := fmt.Sprintf("Total Members: %d\n\n", len(members))
	for i, member := range members {
		result += fmt.Sprintf("[%d] Name: %s\n", i+1, member.Name)
		result += fmt.Sprintf("    Addr: %s:%d\n", member.Addr.String(), member.Port)
		result += fmt.Sprintf("    State: %d\n", member.State)
		if i < len(members)-1 {
			result += "\n"
		}
	}
	return result
}

// GetMemberlistJSON returns the memberlist as a JSON blob matching the
// schema consumed by the wp CLI's client/memberlist.go. This is the
// structured counterpart of GetMemberlistStatus (which returns text).
func (n *ServerNode) GetMemberlistJSON() []byte {
	type memberJSON struct {
		ID          string            `json:"id"`
		GossipAddr  string            `json:"gossip_addr"`
		ServiceAddr string            `json:"service_addr"`
		ClusterName string            `json:"cluster_name"`
		Region      string            `json:"region"`
		AZ          string            `json:"az"`
		RG          string            `json:"rg"`
		State       int               `json:"state"`
		Incarnation uint32            `json:"incarnation"`
		LastSeenMS  int64             `json:"last_seen_ms"`
		Tags        map[string]string `json:"tags,omitempty"`
	}
	type listJSON struct {
		Members []memberJSON `json:"members"`
	}

	members := n.memberlist.Members()
	allMeta := n.discovery.GetAllServers()
	out := listJSON{Members: make([]memberJSON, 0, len(members))}
	for _, m := range members {
		mj := memberJSON{
			ID:         m.Name,
			GossipAddr: fmt.Sprintf("%s:%d", m.Addr.String(), m.Port),
			State:      int(m.State),
			LastSeenMS: time.Now().UnixMilli(),
		}
		if meta, ok := allMeta[m.Name]; ok {
			mj.ClusterName = meta.ClusterName
			mj.Region = meta.Region
			mj.AZ = meta.Az
			mj.RG = meta.ResourceGroup
			mj.ServiceAddr = meta.Endpoint
			mj.Tags = meta.Tags
		}
		out.Members = append(out.Members, mj)
	}
	b, err := json.Marshal(out)
	if err != nil {
		return []byte(`{"members":[]}`)
	}
	return b
}

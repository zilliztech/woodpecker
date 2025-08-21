package membership

import (
	"fmt"
	"log"
	"time"

	ml "github.com/hashicorp/memberlist"
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
	meta *ServerMeta
}

// ServerConfig Server configuration
type ServerConfig struct {
	NodeID        string
	ResourceGroup string
	AZ            string
	BindAddr      string
	BindPort      int
	ServicePort   int
	Tags          map[string]string
}

func NewServerNode(config *ServerConfig) (*ServerNode, error) {
	meta := &ServerMeta{
		NodeID:        config.NodeID,
		ResourceGroup: config.ResourceGroup,
		AZ:            config.AZ,
		Endpoint:      fmt.Sprintf("%s:%d", config.BindAddr, config.ServicePort),
		Tags:          config.Tags,
		LastUpdate:    time.Now(),
	}
	discovery := NewServiceDiscovery()
	delegate := NewServerDelegate(meta)
	eventDel := NewEventDelegate(discovery, RoleServer)

	mlConfig := ml.DefaultLocalConfig()
	mlConfig.Name = config.NodeID       // unique identifier name for a node
	mlConfig.BindAddr = config.BindAddr // address for running gossip protocol
	mlConfig.BindPort = config.BindPort
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
		memberlist: list,
		delegate:   delegate,
		eventDel:   eventDel,
		discovery:  discovery,
		meta:       meta,
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
				fmt.Printf("%s(%s) ", replica.NodeID, azs[i])
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

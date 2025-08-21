package membership

import (
	"fmt"
	"log"
	"time"

	ml "github.com/hashicorp/memberlist"
)

// ClientNode represents a client node (observer only)
type ClientNode struct {
	memberlist *ml.Memberlist    // gossip member manager
	eventDel   *EventDelegate    // gossip event delegate
	discovery  *ServiceDiscovery // mainly for sensing server node list
	nodeID     string
}

// ClientConfig represents client configuration
type ClientConfig struct {
	NodeID   string
	BindAddr string
	BindPort int
}

func NewClientNode(config *ClientConfig) (*ClientNode, error) {
	discovery := NewServiceDiscovery()
	delegate := NewClientDelegate()
	eventDel := NewEventDelegate(discovery, RoleClient)

	mlConfig := ml.DefaultLocalConfig()
	mlConfig.Name = config.NodeID       // client as the unique name identifier for gossip protocol node
	mlConfig.BindAddr = config.BindAddr // address where client runs gossip protocol
	mlConfig.BindPort = config.BindPort
	mlConfig.Delegate = delegate      // empty delegate, equivalent to only receiving and processing requests with empty processing; mainly join and leave, making this client passively receive some member list updates
	mlConfig.Events = eventDel        // mainly listens to node addition/removal events, synchronously updates member list cache information
	mlConfig.LogOutput = log.Writer() // set the log output for the node
	mlConfig.GossipNodes = 0
	mlConfig.PushPullInterval = 0

	list, err := ml.Create(mlConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}

	return &ClientNode{memberlist: list, eventDel: eventDel, discovery: discovery, nodeID: config.NodeID}, nil
}

func (n *ClientNode) Join(existing []string) error {
	if len(existing) > 0 {
		count, err := n.memberlist.Join(existing)
		if err != nil {
			return fmt.Errorf("failed to join cluster: %w", err)
		}
		log.Printf("[CLIENT] Successfully connected to %d nodes", count)
	}
	return nil
}

func (n *ClientNode) GetDiscovery() *ServiceDiscovery { return n.discovery }

func (n *ClientNode) SelectReplicas(resourceGroup string) ([]*ServerMeta, error) {
	servers, azs, err := n.discovery.SelectServersAcrossAZ(resourceGroup, 3)
	if err != nil {
		return nil, err
	}
	log.Printf("[CLIENT] Selected replicas for write:")
	for i, server := range servers {
		log.Printf("  • %s (AZ: %s, Endpoint: %s)", server.NodeID, azs[i], server.Endpoint)
	}
	return servers, nil
}

func (n *ClientNode) PrintStatus() {
	fmt.Println("\n╔════════════════════════════════════════╗")
	fmt.Println("║          CLIENT VIEW                   ║")
	fmt.Println("╚════════════════════════════════════════╝")
	fmt.Printf("\n🔍 Client: %s\n", n.nodeID)
	servers := n.discovery.GetAllServers()
	fmt.Printf("📡 Discovered Servers: %d\n", len(servers))
	groups := n.discovery.GetResourceGroups()
	for _, rg := range groups {
		rgServers := n.discovery.GetServersByResourceGroup(rg)
		azDist := n.discovery.GetAZDistribution(rg)
		fmt.Printf("\n[Resource Group: %s]\n", rg)
		fmt.Printf("  Servers: %d\n", len(rgServers))
		fmt.Println("  AZ Coverage:")
		for az, count := range azDist {
			fmt.Printf("    • %s: %d servers\n", az, count)
		}
		if _, _, err := n.discovery.SelectServersAcrossAZ(rg, 3); err == nil {
			fmt.Println("Can perform 3-replica writes")
		} else {
			fmt.Printf("Cannot perform 3-replica writes: %v\n", err)
		}
		fmt.Println("  Available Servers:")
		for _, server := range rgServers {
			fmt.Printf("    • %s -> %s (AZ: %s)\n", server.NodeID, server.Endpoint, server.AZ)
		}
	}
}

func (n *ClientNode) Leave() error    { return n.memberlist.Leave(5 * time.Second) }
func (n *ClientNode) Shutdown() error { return n.memberlist.Shutdown() }

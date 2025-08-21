package membership

import (
	"encoding/json"
	"log"

	"github.com/hashicorp/memberlist"
)

var _ memberlist.EventDelegate = (*EventDelegate)(nil)

// EventDelegate handles member change events
type EventDelegate struct {
	discovery *ServiceDiscovery
	role      NodeRole
}

func NewEventDelegate(discovery *ServiceDiscovery, role NodeRole) *EventDelegate {
	return &EventDelegate{discovery: discovery, role: role}
}

// NotifyJoin node joins
func (e *EventDelegate) NotifyJoin(node *memberlist.Node) {
	if len(node.Meta) > 0 { // has meta, indicating a server role joining
		var meta ServerMeta
		if err := json.Unmarshal(node.Meta, &meta); err == nil {
			e.discovery.UpdateServer(node.Name, &meta)
			if e.role == RoleClient {
				log.Printf("[CLIENT-WATCH] Server joined: %s (RG: %s, AZ: %s, Endpoint: %s)", node.Name, meta.ResourceGroup, meta.AZ, meta.Endpoint)
			} else {
				log.Printf("[SERVER-EVENT] Server joined: %s (RG: %s, AZ: %s)", node.Name, meta.ResourceGroup, meta.AZ)
			}
		}
	} else { // no meta, indicating a client role joining
		if e.role == RoleServer {
			log.Printf("[SERVER-EVENT] Client joined: %s", node.Name)
		}
	}
}

// NotifyLeave node leaves
func (e *EventDelegate) NotifyLeave(node *memberlist.Node) {
	e.discovery.RemoveServer(node.Name)
	if e.role == RoleClient {
		log.Printf("[CLIENT-WATCH] Node left: %s", node.Name)
	} else {
		log.Printf("[SERVER-EVENT] Node left: %s", node.Name)
	}
}

// NotifyUpdate node updates
func (e *EventDelegate) NotifyUpdate(node *memberlist.Node) {
	if len(node.Meta) > 0 { // server role node updates meta
		var meta ServerMeta
		if err := json.Unmarshal(node.Meta, &meta); err == nil {
			e.discovery.UpdateServer(node.Name, &meta)
			if e.role == RoleClient {
				log.Printf("[CLIENT-WATCH] Server updated: %s (Version: %d)", node.Name, meta.Version)
			} else {
				log.Printf("[SERVER-EVENT] Server updated: %s (Version: %d)", node.Name, meta.Version)
			}
		}
	}
}

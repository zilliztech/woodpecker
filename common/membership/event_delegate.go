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
	"log"

	"github.com/hashicorp/memberlist"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/proto"
)

var _ memberlist.EventDelegate = (*EventDelegate)(nil)

// EventDelegate handles member change events
type EventDelegate struct {
	discovery *ServiceDiscovery
	role      NodeRole
	addrPort  string // only used for logging
}

func NewEventDelegate(discovery *ServiceDiscovery, role NodeRole, addrPort string) *EventDelegate {
	return &EventDelegate{discovery: discovery, role: role, addrPort: addrPort}
}

// NotifyJoin node joins
func (e *EventDelegate) NotifyJoin(node *memberlist.Node) {
	if len(node.Meta) > 0 { // has meta, indicating a server role joining
		var meta proto.NodeMeta
		if err := pb.Unmarshal(node.Meta, &meta); err == nil {
			e.discovery.UpdateServer(node.Name, &meta)
			if e.role == RoleClient {
				log.Printf("[CLIENT-WATCH] Server[%s] joined: %s (RG: %s, AZ: %s, Endpoint: %s)", e.addrPort, node.Name, meta.ResourceGroup, meta.Az, meta.Endpoint)
			} else {
				log.Printf("[SERVER-EVENT] Server[%s] joined: %s (RG: %s, AZ: %s)", e.addrPort, node.Name, meta.ResourceGroup, meta.Az)
			}
		} else {
			log.Printf("[%s-EVENT] Error: failed to unmarshal metadata for joined node %s (metaLen=%d): %v", string(e.role), node.Name, len(node.Meta), err)
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
		var meta proto.NodeMeta
		if err := pb.Unmarshal(node.Meta, &meta); err == nil {
			e.discovery.UpdateServer(node.Name, &meta)
			if e.role == RoleClient {
				log.Printf("[CLIENT-WATCH] Server updated: %s (Version: %d)", node.Name, meta.Version)
			} else {
				log.Printf("[SERVER-EVENT] Server updated: %s (Version: %d)", node.Name, meta.Version)
			}
		} else {
			log.Printf("[%s-EVENT] Error: failed to unmarshal metadata for updated node %s (metaLen=%d): %v", string(e.role), node.Name, len(node.Meta), err)
		}
	}
}

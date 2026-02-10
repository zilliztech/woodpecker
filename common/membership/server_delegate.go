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
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/proto"
)

var _ memberlist.Delegate = (*ServerDelegate)(nil)

// ServerDelegate memberlist delegate for server nodes
type ServerDelegate struct {
	mu          sync.RWMutex
	meta        *proto.NodeMeta
	metaVersion int64 // metadata version, corresponds to version in ServerMeta for compatibility between nodes of different versions
}

func NewServerDelegate(meta *proto.NodeMeta) *ServerDelegate {
	return &ServerDelegate{meta: meta, metaVersion: 1}
}

// NodeMeta returns node metadata for gossip propagation
func (d *ServerDelegate) NodeMeta(limit int) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.meta.Version = d.metaVersion
	d.meta.LastUpdate = time.Now().UnixMilli() // Convert to Unix timestamp in milliseconds

	data, err := pb.Marshal(d.meta)
	if err != nil {
		log.Printf("Error marshaling meta: %v", err)
		return nil
	}
	if len(data) > limit {
		log.Fatalf("FATAL: node metadata size %d exceeds memberlist limit %d bytes. Reduce tags or other metadata fields. NodeId=%s, ResourceGroup=%s, AZ=%s",
			len(data), limit, d.meta.NodeId, d.meta.ResourceGroup, d.meta.Az)
	}
	return data
}

// NotifyMsg handles received messages
func (d *ServerDelegate) NotifyMsg(buf []byte) {}

// GetBroadcasts returns messages to be broadcast
func (d *ServerDelegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

// LocalState returns local state
func (d *ServerDelegate) LocalState(join bool) []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()
	data, err := pb.Marshal(d.meta)
	if err != nil {
		log.Printf("Error marshaling local state: %v", err)
		return nil
	}
	return data
}

// MergeRemoteState merges remote state
func (d *ServerDelegate) MergeRemoteState(buf []byte, join bool) {}

// UpdateMeta updates metadata
func (d *ServerDelegate) UpdateMeta(updates map[string]interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if rg, ok := updates["resource_group"].(string); ok {
		d.meta.ResourceGroup = rg
	}
	if az, ok := updates["az"].(string); ok {
		d.meta.Az = az
	}
	if tags, ok := updates["tags"].(map[string]string); ok {
		d.meta.Tags = tags
	}
	d.metaVersion++
	d.meta.Version = d.metaVersion
	d.meta.LastUpdate = time.Now().UnixMilli() // Convert to Unix timestamp in milliseconds
}

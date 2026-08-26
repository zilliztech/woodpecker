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
	"sync/atomic"
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
	// discovery receives peer metas merged in via push/pull anti-entropy
	// (MergeRemoteState) and load readings merged in via NotifyMsg.
	// Optional; nil disables ingestion. Issues #114 and #271.
	discovery *ServiceDiscovery

	// loadQueue holds this node's pending load reading for memberlist's
	// user-level broadcast channel. Load moves every report interval, which is
	// the wrong shape for NodeMeta: memberlist only republishes meta through
	// UpdateNode(), and that bumps the node's incarnation and rewrites its entry
	// in every peer's node table. This channel gets the same epidemic spread
	// without touching membership state. Issue #271.
	loadQueue *memberlist.TransmitLimitedQueue
	// ml is stored once memberlist.Create returns, so loadQueue can size its
	// retransmit count from the cluster size. Held atomically because
	// memberlist's gossip loop is already running by the time it is set.
	ml atomic.Pointer[memberlist.Memberlist]
}

func NewServerDelegate(meta *proto.NodeMeta) *ServerDelegate {
	d := &ServerDelegate{meta: meta, metaVersion: 1}
	d.loadQueue = &memberlist.TransmitLimitedQueue{
		NumNodes:       d.numMembers,
		RetransmitMult: loadRetransmitMult,
	}
	return d
}

// AttachMemberlist wires the delegate to the memberlist it belongs to. Call it
// once, right after memberlist.Create; until then the node counts as a cluster
// of one for retransmit sizing.
func (d *ServerDelegate) AttachMemberlist(m *memberlist.Memberlist) { d.ml.Store(m) }

// numMembers reports cluster size to the broadcast queue, which derives the
// retransmit count from log(N+1).
func (d *ServerDelegate) numMembers() int {
	if m := d.ml.Load(); m != nil {
		return m.NumMembers()
	}
	return 1
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

// NotifyMsg handles a user-level gossip message from a peer. The first byte
// tags the payload kind; unknown tags are ignored so nodes on either side of an
// upgrade can gossip together.
//
// A load reading that is new to this node is relayed onward, which is what makes
// the announce epidemic rather than a single sender fanning out to a bounded
// number of peers. Without it the originator's retransmit budget —
// RetransmitMult * ceil(log10(N+1)) deliveries, ~8 for a 16-node cluster — is
// the whole reach, and most peers never hear a given reading. Relaying only on
// first sight is what keeps it terminating; see applyLoadUpdate.
func (d *ServerDelegate) NotifyMsg(buf []byte) {
	if len(buf) == 0 {
		return
	}
	switch buf[0] {
	case msgTypeLoadUpdate:
		if nodeID, applied := applyLoadUpdate(d.discovery, buf[1:]); applied {
			d.relayLoad(nodeID, buf)
		}
	}
}

// relayLoad re-queues a peer's reading verbatim so this node gossips it onward.
// Queuing by the originating node's name means a newer reading for that node —
// whether relayed or received later — replaces this one rather than both going
// out.
func (d *ServerDelegate) relayLoad(nodeID string, msg []byte) {
	if nodeID == "" {
		return
	}
	relayed := make([]byte, len(msg))
	copy(relayed, msg) // memberlist reuses the receive buffer
	d.loadQueue.QueueBroadcast(&loadBroadcast{name: loadBroadcastName(nodeID), msg: relayed})
}

// GetBroadcasts hands memberlist the user-level messages waiting to ride this
// gossip round. memberlist frames each one and the receiver gets it in NotifyMsg.
func (d *ServerDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.loadQueue.GetBroadcasts(overhead, limit)
}

// BroadcastLoad queues this node's current load reading for gossip. Queuing is
// non-blocking: the message goes out on the next gossip round, is retransmitted
// a few rounds for reach, and a newer reading replaces it in place rather than
// queueing behind it.
func (d *ServerDelegate) BroadcastLoad(nodeID string, load float64, updatedAt int64) {
	msg, err := encodeLoadUpdate(nodeID, load, updatedAt)
	if err != nil {
		log.Printf("[SERVER] failed to encode load update for %s: %v", nodeID, err)
		return
	}
	d.loadQueue.QueueBroadcast(&loadBroadcast{name: loadBroadcastName(nodeID), msg: msg})
}

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

// MergeRemoteState ingests a peer's meta delivered by memberlist's push/pull
// anti-entropy (the counterpart of LocalState). This is how a node's best-effort
// load hint propagates: each node stamps its load onto its own meta, memberlist
// gossips it via its existing push/pull cadence, and the receiver merges it into
// discovery here. Best-effort: bad/empty payloads are ignored. Issue #114.
func (d *ServerDelegate) MergeRemoteState(buf []byte, join bool) {
	if d.discovery == nil || len(buf) == 0 {
		return
	}
	var meta proto.NodeMeta
	if err := pb.Unmarshal(buf, &meta); err != nil {
		return
	}
	if meta.GetNodeId() == "" {
		return
	}
	d.discovery.UpdateServer(meta.GetNodeId(), &meta)
}

// SetLoadFactor updates the node's published load factor (clamped to [0,1])
// and stamps load_updated_at. Writing it here is local only. Two paths carry it
// to peers: the load gossip message queued by ServerNode.reportLoadOnce, which
// reaches the whole cluster within a gossip round or two and is ingested in
// NotifyMsg; and memberlist's push/pull anti-entropy via LocalState, which
// reaches one random peer per PushPullInterval and is ingested in
// MergeRemoteState. The second is the slow fallback that keeps a peer running
// an older build — one that ignores the load message — from going blind. See
// issue #271 for why that path alone is not sufficient beyond a handful of
// nodes, and #273 for retiring the meta copy once that fallback is unnecessary.
func (d *ServerDelegate) SetLoadFactor(load float64) {
	if load < 0 {
		load = 0
	}
	if load > 1 {
		load = 1
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.meta.LoadFactor = load
	d.meta.LoadUpdatedAt = time.Now().UnixMilli()
}

// SnapshotMeta returns a deep copy of the current meta, safe to hand to other
// components (e.g. ServiceDiscovery) without exposing the live, mutated object.
func (d *ServerDelegate) SnapshotMeta() *proto.NodeMeta {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.meta.CloneVT()
}

// UpdateMeta updates metadata
func (d *ServerDelegate) UpdateMeta(updates map[string]interface{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if rg, ok := updates["resource_group"].(string); ok {
		d.meta.ResourceGroup = rg
	}
	if clusterName, ok := updates["cluster_name"].(string); ok {
		d.meta.ClusterName = clusterName
	}
	if region, ok := updates["region"].(string); ok {
		d.meta.Region = region
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

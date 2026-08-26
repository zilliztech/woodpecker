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
	"errors"

	"github.com/hashicorp/memberlist"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/proto"
)

// User-level gossip message tags. Every payload memberlist delivers to
// Delegate.NotifyMsg starts with one of these, so a receiver can route it and
// silently drop kinds it does not know — which is what lets a node running an
// older build (whose NotifyMsg ignored everything) gossip alongside a newer one.
const (
	msgTypeLoadUpdate byte = 1
)

// loadRetransmitMult mirrors memberlist's own RetransmitMult (4 in
// DefaultLocalConfig, which NewServerNode uses). The broadcast queue multiplies
// it by log(N+1) to decide how many gossip rounds a message rides for, so
// keeping it aligned gives a load reading the same spread as an alive message.
const loadRetransmitMult = 4

// loadBroadcastName namespaces a node's load message inside the broadcast queue,
// so adding another user-level message kind later cannot collide with it.
func loadBroadcastName(nodeID string) string { return "load:" + nodeID }

// encodeLoadUpdate builds the tagged wire payload for one node's load reading.
func encodeLoadUpdate(nodeID string, load float64, updatedAt int64) ([]byte, error) {
	data, err := pb.Marshal(&proto.LoadUpdate{
		NodeId:        nodeID,
		LoadFactor:    load,
		LoadUpdatedAt: updatedAt,
	})
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(data)+1)
	out = append(out, msgTypeLoadUpdate)
	return append(out, data...), nil
}

// decodeLoadUpdate parses a payload produced by encodeLoadUpdate, with the tag
// byte already stripped by the caller.
func decodeLoadUpdate(payload []byte) (*proto.LoadUpdate, error) {
	var upd proto.LoadUpdate
	if err := pb.Unmarshal(payload, &upd); err != nil {
		return nil, err
	}
	if upd.GetNodeId() == "" {
		return nil, errors.New("load update carries no node id")
	}
	return &upd, nil
}

// applyLoadUpdate decodes a load payload and merges it into discovery, reporting
// whether the reading was new. Shared by the server and client delegates: both
// keep a ServiceDiscovery and both select on load, so both have to ingest the
// announce. Best-effort — a malformed payload is dropped rather than logged,
// since this runs on every gossip round.
//
// The bool is what makes relaying safe: a node forwards a reading only the first
// time it learns it, so a message dies out instead of echoing around the cluster.
// This is the same trick memberlist plays with incarnation numbers on alive
// messages.
func applyLoadUpdate(discovery *ServiceDiscovery, payload []byte) (nodeID string, applied bool) {
	if discovery == nil {
		return "", false
	}
	upd, err := decodeLoadUpdate(payload)
	if err != nil {
		return "", false
	}
	return upd.GetNodeId(), discovery.ApplyLoad(upd.GetNodeId(), upd.GetLoadFactor(), upd.GetLoadUpdatedAt())
}

// loadBroadcast is one node's load reading waiting to be gossiped.
//
// It implements memberlist.NamedBroadcast so that queuing a newer reading for a
// node replaces the pending one in O(1) instead of letting stale samples queue
// up behind it — the reporter enqueues on every tick, and only the newest
// reading is worth the bytes.
type loadBroadcast struct {
	name string
	msg  []byte
}

var (
	_ memberlist.Broadcast      = (*loadBroadcast)(nil)
	_ memberlist.NamedBroadcast = (*loadBroadcast)(nil)
)

func (b *loadBroadcast) Name() string    { return b.name }
func (b *loadBroadcast) Message() []byte { return b.msg }
func (b *loadBroadcast) Finished()       {}

// Invalidates is not consulted while Name() is non-empty (the queue dedupes by
// name), but Broadcast requires it and the two must agree: a node's reading
// supersedes its own previous one.
func (b *loadBroadcast) Invalidates(other memberlist.Broadcast) bool {
	o, ok := other.(*loadBroadcast)
	return ok && o.name == b.name
}

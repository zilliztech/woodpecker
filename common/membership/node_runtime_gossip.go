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

// This file is the transport for NodeRuntimeInfo: encoding, queueing, spreading
// and relaying a node's runtime snapshot over memberlist's user-level broadcast
// channel. It is deliberately blind to what the snapshot contains — adding a
// runtime signal to the proto changes nothing here, only what a consumer
// (ServiceDiscovery) chooses to read out of it.

// User-level gossip message tags. Every payload memberlist delivers to
// Delegate.NotifyMsg starts with one of these, so a receiver can route it and
// silently drop kinds it does not know — which is what lets a node running an
// older build (whose NotifyMsg ignored everything) gossip alongside a newer one.
const (
	msgTypeNodeRuntimeInfo byte = 1
)

// runtimeRetransmitMult mirrors memberlist's own RetransmitMult (4 in
// DefaultLocalConfig, which NewServerNode uses). The broadcast queue multiplies
// it by log(N+1) to decide how many transmissions a message gets, so keeping it
// aligned gives a runtime snapshot the same reach as an alive message.
const runtimeRetransmitMult = 4

// runtimeBroadcastName namespaces a node's runtime snapshot inside the broadcast
// queue, so adding another user-level message kind later cannot collide with it.
func runtimeBroadcastName(nodeID string) string { return "runtime:" + nodeID }

// encodeRuntimeInfo builds the tagged wire payload for one node's snapshot.
func encodeRuntimeInfo(info *proto.NodeRuntimeInfo) ([]byte, error) {
	if info.GetNodeId() == "" {
		return nil, errors.New("runtime info carries no node id")
	}
	data, err := pb.Marshal(info)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(data)+1)
	out = append(out, msgTypeNodeRuntimeInfo)
	return append(out, data...), nil
}

// decodeRuntimeInfo parses a payload produced by encodeRuntimeInfo, with the tag
// byte already stripped by the caller.
func decodeRuntimeInfo(payload []byte) (*proto.NodeRuntimeInfo, error) {
	var info proto.NodeRuntimeInfo
	if err := pb.Unmarshal(payload, &info); err != nil {
		return nil, err
	}
	if info.GetNodeId() == "" {
		return nil, errors.New("runtime info carries no node id")
	}
	return &info, nil
}

// applyRuntimeInfo decodes a snapshot and hands it to discovery, reporting the
// node it describes and whether the snapshot was new. Shared by the server and
// client delegates: both keep a ServiceDiscovery and both select on runtime
// signals, so both have to ingest. Best-effort — a malformed payload is dropped
// rather than logged, since this runs on every gossip round.
//
// The bool is the relay contract. Dedup lives with the consumer, which already
// holds the version of what it has; the transport does not keep a second copy of
// that state, it just forwards whatever the consumer calls news. A node relays a
// snapshot only the first time it learns it, so a message dies out instead of
// echoing around the cluster — the same role incarnation numbers play for alive.
func applyRuntimeInfo(discovery *ServiceDiscovery, payload []byte) (nodeID string, applied bool) {
	if discovery == nil {
		return "", false
	}
	info, err := decodeRuntimeInfo(payload)
	if err != nil {
		return "", false
	}
	return info.GetNodeId(), discovery.ApplyRuntimeInfo(info)
}

// runtimeBroadcast is one node's runtime snapshot waiting to be gossiped.
//
// It implements memberlist.NamedBroadcast so that queuing a newer snapshot for a
// node replaces the pending one in O(1) instead of letting stale samples queue
// up behind it — the reporter enqueues on every tick, and only the newest
// snapshot is worth the bytes.
type runtimeBroadcast struct {
	name string
	msg  []byte
}

var (
	_ memberlist.Broadcast      = (*runtimeBroadcast)(nil)
	_ memberlist.NamedBroadcast = (*runtimeBroadcast)(nil)
)

func (b *runtimeBroadcast) Name() string    { return b.name }
func (b *runtimeBroadcast) Message() []byte { return b.msg }
func (b *runtimeBroadcast) Finished()       {}

// Invalidates is not consulted while Name() is non-empty (the queue dedupes by
// name), but Broadcast requires it and the two must agree: a node's snapshot
// supersedes its own previous one.
func (b *runtimeBroadcast) Invalidates(other memberlist.Broadcast) bool {
	o, ok := other.(*runtimeBroadcast)
	return ok && o.name == b.name
}

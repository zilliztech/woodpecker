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

import "github.com/hashicorp/memberlist"

var _ memberlist.Delegate = (*ClientDelegate)(nil)

// ClientDelegate is the memberlist delegate for client nodes. A client announces
// nothing of its own, but it does keep a ServiceDiscovery and select on runtime
// signals, so it has to ingest the snapshots servers gossip.
type ClientDelegate struct {
	// discovery receives peer runtime snapshots via NotifyMsg. Optional; nil
	// disables ingestion. Issue #271.
	discovery *ServiceDiscovery
}

func NewClientDelegate() *ClientDelegate { return &ClientDelegate{} }

// NodeMeta client does not provide metadata for gossip
func (d *ClientDelegate) NodeMeta(limit int) []byte { return []byte{} }

// NotifyMsg ingests a user-level gossip message. A client sets PushPullInterval
// to 0 and its MergeRemoteState is a no-op, so this is its only path to a peer's
// current runtime state — without it a client would select on whatever each
// server happened to publish when the client joined. Unknown tags are ignored.
func (d *ClientDelegate) NotifyMsg(buf []byte) {
	if len(buf) == 0 {
		return
	}
	switch buf[0] {
	case msgTypeNodeRuntimeInfo:
		// Ingest only, no relay: a client runs with GossipNodes 0 and returns
		// nothing from GetBroadcasts, so it is a leaf in the gossip graph.
		applyRuntimeInfo(d.discovery, buf[1:])
	}
}

// GetBroadcasts client does not broadcast messages
func (d *ClientDelegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

// LocalState client does not provide local state
func (d *ClientDelegate) LocalState(join bool) []byte { return []byte{} }

// MergeRemoteState client does not merge state
func (d *ClientDelegate) MergeRemoteState(buf []byte, join bool) {}

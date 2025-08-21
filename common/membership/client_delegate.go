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

// ClientDelegate is the memberlist delegate for client nodes (does not participate in gossip)
type ClientDelegate struct{}

func NewClientDelegate() *ClientDelegate { return &ClientDelegate{} }

// NodeMeta client does not provide metadata for gossip
func (d *ClientDelegate) NodeMeta(limit int) []byte { return []byte{} }

// NotifyMsg client does not handle gossip messages
func (d *ClientDelegate) NotifyMsg(buf []byte) {}

// GetBroadcasts client does not broadcast messages
func (d *ClientDelegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

// LocalState client does not provide local state
func (d *ClientDelegate) LocalState(join bool) []byte { return []byte{} }

// MergeRemoteState client does not merge state
func (d *ClientDelegate) MergeRemoteState(buf []byte, join bool) {}

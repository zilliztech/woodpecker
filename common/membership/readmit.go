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

	ml "github.com/hashicorp/memberlist"
)

// deadNodeReclaimTime lets a node that was declared dead be taken over at a
// new address at once. Node names are unique per server (a pod name, a
// configured node ID), so a new address under a dead node's name is that
// server back, restarted or rescheduled, never a second server. memberlist's
// default (0) keeps the name for the dead address until the entry is reaped
// (GossipToTheDeadTime), which kept a restarted server out for 30-50s (#395).
const deadNodeReclaimTime = time.Nanosecond

// suspicionMaxTimeoutMult caps how long a suspected node may go unconfirmed
// before it is declared dead, as a multiple of the suspicion timeout (3s in
// memberlist's local config): 6s instead of the default 18s. It matters when a
// server restarts at a new address before it is declared dead: a peer that
// has already taken it back no longer confirms the suspicion of the old
// entry, so the others wait out the cap before they can take it back too.
// A server that is alive and reachable refutes a suspicion as soon as it hears
// of it, well inside the cap; one that cannot be reached for 6s is cut off
// either way.
const suspicionMaxTimeoutMult = 2

// readmitPendingTTL bounds how long a refused address is remembered. A server
// that died without leaving is declared dead within seconds; one that is still
// alive after this long was not the same server coming back.
const readmitPendingTTL = time.Minute

// readmitter readmits a server that came back at a new address while its old
// entry was still alive or suspect.
//
// memberlist refuses such an announcement ("Conflicting address"), and the
// server does not announce itself again: peers would learn of it only at their
// next full-state sync, after its old entry is declared dead. The readmitter
// remembers the refused address, and when the old entry is declared dead it
// joins that address, so the full-state exchange brings the server back at
// once; deadNodeReclaimTime lets the dead entry be replaced.
type readmitter struct {
	join func(addrs []string) (int, error)

	mu      sync.Mutex
	pending map[string]pendingAddr // node name -> address it was refused at
}

type pendingAddr struct {
	addr string
	at   time.Time
}

func newReadmitter(join func(addrs []string) (int, error)) *readmitter {
	return &readmitter{join: join, pending: make(map[string]pendingAddr)}
}

// NotifyConflict implements memberlist.ConflictDelegate.
func (r *readmitter) NotifyConflict(existing, other *ml.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending[other.Name] = pendingAddr{addr: other.Address(), at: time.Now()}
}

// nodeLeft is called when a node leaves or is declared dead. memberlist holds
// its node lock while it notifies, so the join runs on its own goroutine.
func (r *readmitter) nodeLeft(name string) {
	r.mu.Lock()
	p, ok := r.pending[name]
	delete(r.pending, name)
	for n, q := range r.pending {
		if time.Since(q.at) > readmitPendingTTL {
			delete(r.pending, n)
		}
	}
	r.mu.Unlock()
	if !ok || time.Since(p.at) > readmitPendingTTL {
		return
	}
	go func() {
		if _, err := r.join([]string{p.addr}); err != nil {
			log.Printf("[SERVER-EVENT] Failed to readmit %s at %s: %v", name, p.addr, err)
			return
		}
		log.Printf("[SERVER-EVENT] Readmitted %s at %s", name, p.addr)
	}()
}

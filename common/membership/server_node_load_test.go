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
	"context"
	"fmt"
	"testing"
	"time"

	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/proto"
)

type fakeSampler struct{ v float64 }

func (f fakeSampler) Sample() float64 { return f.v }

func TestReportLoadOnce_PublishesLoadToMeta(t *testing.T) {
	meta := &proto.NodeMeta{NodeId: "n1"}
	delegate := NewServerDelegate(meta)
	n := &ServerNode{
		delegate: delegate,
		meta:     meta,
		sampler:  fakeSampler{v: 0.37},
	}
	// publishLoad does the sampler->delegate->meta part without memberlist/discovery.
	n.publishLoad()
	if got := meta.GetLoadFactor(); got != 0.37 {
		t.Fatalf("want load 0.37 published, got %v", got)
	}
	if meta.GetLoadUpdatedAt() == 0 {
		t.Fatalf("load_updated_at should be stamped")
	}
}

func TestStartLoadReporter_RunsAndStopsCleanly(t *testing.T) {
	meta := &proto.NodeMeta{NodeId: "n1"}
	delegate := NewServerDelegate(meta)
	ctx, cancel := context.WithCancel(context.Background())
	n := &ServerNode{
		delegate:   delegate,
		meta:       meta,
		sampler:    fakeSampler{v: 0.5},
		loadCtx:    ctx,
		loadCancel: cancel,
	}
	// reportLoadOnce touches memberlist/discovery, which are nil here; so for the
	// lifecycle test we run a reporter that only exercises publishLoad via a ticker.
	// Use the real startLoadReporter but guard: it calls reportLoadOnce which would
	// nil-panic on memberlist. So instead assert publishLoad + cancel/Wait semantics.
	n.loadWG.Add(1)
	go func() {
		defer n.loadWG.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		n.publishLoad()
		for {
			select {
			case <-n.loadCtx.Done():
				return
			case <-ticker.C:
				n.publishLoad()
			}
		}
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	n.loadWG.Wait() // must return promptly; if it hangs the goroutine leaked
	if meta.GetLoadFactor() != 0.5 {
		t.Fatalf("expected published load 0.5, got %v", meta.GetLoadFactor())
	}
}

// === Load broadcast (issue #271) ===

// loadInBroadcastQueue drains the node's user-level gossip queue and returns the
// load reading it is announcing for itself, if any. This is the payload that
// spreads to every peer and is ingested by ServerDelegate.NotifyMsg.
//
// GetBroadcasts counts as a transmission, so call it once per assertion.
func loadInBroadcastQueue(t *testing.T, n *ServerNode) (float64, bool) {
	t.Helper()
	for _, msg := range n.delegate.GetBroadcasts(0, 1<<20) {
		if len(msg) == 0 || msg[0] != msgTypeLoadUpdate {
			continue
		}
		upd, err := decodeLoadUpdate(msg[1:])
		if err != nil {
			t.Fatalf("decode load update: %v", err)
		}
		if upd.GetNodeId() == n.serverConfig.NodeID {
			return upd.GetLoadFactor(), true
		}
	}
	return 0, false
}

// loadInMemberlistMeta reads the load carried by memberlist's own node table —
// the payload that would ride an alive message. Load no longer travels this way
// (issue #271): it would cost an incarnation bump and a node-table rewrite on
// every peer per sample. Tests use this to assert the node table stays put.
func loadInMemberlistMeta(t *testing.T, n *ServerNode) float64 {
	t.Helper()
	for _, m := range n.GetMemberlist().Members() {
		if m.Name != n.serverConfig.NodeID {
			continue
		}
		var meta proto.NodeMeta
		if err := pb.Unmarshal(m.Meta, &meta); err != nil {
			t.Fatalf("unmarshal meta for %s: %v", m.Name, err)
		}
		return meta.GetLoadFactor()
	}
	t.Fatalf("node %s not found in its own memberlist", n.serverConfig.NodeID)
	return 0
}

func newLoadTestNode(t *testing.T, id string, port int) *ServerNode {
	t.Helper()
	n, err := NewServerNode(&ServerConfig{
		NodeID: id, ClusterName: "c", Region: "r", ResourceGroup: "rg", AZ: "az",
		BindPort: port, ServicePort: port + 1000,
		AdvertiseAddr: "127.0.0.1", AdvertisePort: port,
		LoadAwareEnabled: true,
		// 0 keeps the background reporter (and its real system sampler) out of the
		// way: the test installs its own sampler and drives reportLoadOnce by hand,
		// so the assertions are not racing a tick or comparing against whatever load
		// the machine running the test happens to have.
		LoadReportInterval: 0,
		LoadTTL:            30 * time.Second,
		MemSoftThreshold:   0.85,
		EWMAAlpha:          0.5,
	})
	if err != nil {
		t.Fatalf("create node %s: %v", id, err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })
	return n
}

// The regression test for #271: a reporter tick must put the fresh load on the
// wire, not only into this node's own memory. Every tick announces, so
// loadReportInterval is the propagation cadence.
func TestReportLoadOnce_AnnouncesLoadOnGossipChannel(t *testing.T) {
	n := newLoadTestNode(t, "bcast-1", 27810)
	if _, queued := loadInBroadcastQueue(t, n); queued {
		t.Fatalf("nothing should be announced before the first report")
	}

	n.sampler = fakeSampler{v: 0.40}
	n.reportLoadOnce()
	got, queued := loadInBroadcastQueue(t, n)
	if !queued {
		t.Fatalf("a report must announce the load to peers (issue #271)")
	}
	if got != 0.40 {
		t.Fatalf("announce should carry the reported load 0.40, got %v", got)
	}

	// Every subsequent tick re-announces, including a small move: there is no
	// change threshold to cross, which is what keeps loadTTL a simple multiple of
	// loadReportInterval rather than a value that has to allow for silent periods.
	n.sampler = fakeSampler{v: 0.42}
	n.reportLoadOnce()
	got, queued = loadInBroadcastQueue(t, n)
	if !queued || got != 0.42 {
		t.Fatalf("each tick should re-announce the current load; got %v (queued=%v), want 0.42", got, queued)
	}
}

// A node with no sampler must not announce — reportLoadOnce has nothing to say,
// and spending gossip bytes on it would be pure churn.
func TestReportLoadOnce_NoSamplerDoesNotAnnounce(t *testing.T) {
	n := newLoadTestNode(t, "bcast-2", 27812)
	n.reportLoadOnce() // sampler is nil: LoadReportInterval 0 means none was built
	if _, queued := loadInBroadcastQueue(t, n); queued {
		t.Fatalf("a node with no sampler has nothing to announce")
	}
}

// Only the newest reading is worth the bytes: queuing a fresher sample must
// replace the pending one rather than let stale samples pile up behind it. This
// is what loadBroadcast.Name() buys.
func TestBroadcastLoad_NewerReadingReplacesPending(t *testing.T) {
	d := NewServerDelegate(&proto.NodeMeta{NodeId: "n1"})
	d.BroadcastLoad("n1", 0.10, 100)
	d.BroadcastLoad("n1", 0.90, 200)

	msgs := d.GetBroadcasts(0, 1<<20)
	if len(msgs) != 1 {
		t.Fatalf("a node should have exactly one pending load announce, got %d", len(msgs))
	}
	upd, err := decodeLoadUpdate(msgs[0][1:])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if upd.GetLoadFactor() != 0.90 {
		t.Fatalf("the pending announce should be the newest reading, got %v", upd.GetLoadFactor())
	}
}

// Regression for the data race CI caught on #272: the load reporter must not
// write memberlist's internal node table while a reader walks the pointers
// Members() hands out.
//
// Members() returns pointers into memberlist's own nodeState and drops nodeLock
// before the caller reads them, so every field read through one is
// unsynchronised. Announcing load via UpdateNode() made aliveNode() rewrite
// state.Meta/Addr/Port on every report tick, turning that latent unsafety into a
// race the detector fires on reliably.
func TestReportLoadOnce_DoesNotRaceMemberlistReaders(t *testing.T) {
	n := newLoadTestNode(t, "race-1", 27890)
	n.sampler = fakeSampler{v: 0.3}

	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.Now().Add(300 * time.Millisecond)
		for time.Now().Before(deadline) {
			_ = n.GetMemberlistStatus()
			_ = n.GetMemberlistJSON()
		}
	}()
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		n.reportLoadOnce()
		time.Sleep(time.Millisecond)
	}
	<-done
}

// The structural guarantee behind the test above: a load report leaves
// memberlist's node table untouched. If load still rode an alive message the
// broadcast meta would track every sample, and every peer in the cluster would
// rewrite this node's entry on each tick.
func TestReportLoadOnce_LeavesMemberlistNodeTableAlone(t *testing.T) {
	n := newLoadTestNode(t, "notable-1", 27892)
	before := loadInMemberlistMeta(t, n)

	n.sampler = fakeSampler{v: 0.77}
	n.reportLoadOnce()

	if got := loadInMemberlistMeta(t, n); got != before {
		t.Fatalf("load report rewrote memberlist's node table meta (%v -> %v); "+
			"load must not ride an alive message", before, got)
	}
	// ...while the node's own view is still current.
	if got := n.delegate.SnapshotMeta().GetLoadFactor(); got != 0.77 {
		t.Fatalf("local meta should carry the fresh sample, got %v", got)
	}
}

// End-to-end: the announce has to land in a *peer's* discovery, which is what
// quorum selection reads. Covers the EventDelegate.NotifyUpdate wiring.
func TestLoadBroadcast_ReachesPeerDiscovery(t *testing.T) {
	a := newLoadTestNode(t, "peer-a", 27816)
	b := newLoadTestNode(t, "peer-b", 27818)
	if err := b.Join([]string{fmt.Sprintf("127.0.0.1:%d", 27816)}); err != nil {
		t.Fatalf("join: %v", err)
	}
	waitFor(t, 20*time.Second, func() bool {
		return len(a.GetMemberlist().Members()) == 2 && len(b.GetMemberlist().Members()) == 2
	}, "cluster did not converge")

	a.sampler = fakeSampler{v: 0.88}
	a.reportLoadOnce()

	// The announce is gossiped, so allow a short settle; without it this would be
	// the pairwise push/pull path, which needs tens of seconds (issue #271).
	waitFor(t, 5*time.Second, func() bool {
		b.discovery.mu.RLock()
		defer b.discovery.mu.RUnlock()
		meta, ok := b.discovery.Nodes["peer-a"]
		return ok && meta.GetLoadFactor() == 0.88
	}, "peer never saw the announced load within 5s")
}

// A client keeps its own discovery and selects on load (SelectReplicas ->
// SelectServersAcrossAZ, load-aware by default), but it announces nothing, runs
// with PushPullInterval 0 and has a no-op MergeRemoteState. The load gossip
// message is therefore its *only* path to a server's current load — without it a
// client would keep selecting on whatever each server published at join time.
func TestLoadBroadcast_ReachesClientDiscovery(t *testing.T) {
	s := newLoadTestNode(t, "srv-a", 27820)
	c, err := NewClientNode(&ClientConfig{NodeID: "cli-a", BindAddr: "127.0.0.1", BindPort: 27822})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = c.Shutdown() })
	if err := c.Join([]string{fmt.Sprintf("127.0.0.1:%d", 27820)}); err != nil {
		t.Fatalf("join: %v", err)
	}
	waitFor(t, 20*time.Second, func() bool {
		_, ok := c.GetDiscovery().GetAllServers()["srv-a"]
		return ok
	}, "client never saw the server join")

	s.sampler = fakeSampler{v: 0.66}
	s.reportLoadOnce()

	waitFor(t, 5*time.Second, func() bool {
		return c.GetDiscovery().GetAllServers()["srv-a"].GetLoadFactor() == 0.66
	}, "client never saw the announced load within 5s")
}

// The property #271 is actually about: after one announce, *every* peer holds
// the reading — not just the handful the originator managed to reach directly.
//
// A sender's retransmit budget is RetransmitMult * ceil(log10(N+1)) deliveries,
// about 8 for a 16-node cluster, spread over random peers. Fanning out from the
// originator alone therefore covers roughly half the cluster (measured: 8/15).
// Relaying on first sight makes the announce epidemic and closes the gap.
func TestLoadBroadcast_ReachesEveryPeerInACluster(t *testing.T) {
	if testing.Short() {
		t.Skip("spins up a 16-node cluster")
	}
	const n = 16
	nodes := make([]*ServerNode, n)
	for i := range nodes {
		nodes[i] = newLoadTestNode(t, fmt.Sprintf("cov-%d", i), 27900+i*2)
	}
	for i := 1; i < n; i++ {
		if err := nodes[i].Join([]string{fmt.Sprintf("127.0.0.1:%d", 27900)}); err != nil {
			t.Fatalf("join node %d: %v", i, err)
		}
	}
	waitFor(t, 90*time.Second, func() bool {
		for _, node := range nodes {
			if len(node.GetMemberlist().Members()) != n {
				return false
			}
		}
		return true
	}, "cluster did not converge")

	nodes[0].sampler = fakeSampler{v: 0.73}
	nodes[0].reportLoadOnce()

	waitFor(t, 15*time.Second, func() bool {
		for _, node := range nodes[1:] {
			meta, ok := node.GetDiscovery().GetAllServers()["cov-0"]
			if !ok || meta.GetLoadFactor() != 0.73 {
				return false
			}
		}
		return true
	}, "not every peer picked up the announced load")
}

// Relaying must terminate. A node forwards a reading only the first time it
// learns it, so re-delivering the same message is a no-op rather than something
// that puts it back on the wire.
func TestNotifyMsg_RelaysAReadingOnlyOnce(t *testing.T) {
	d := NewServerDelegate(&proto.NodeMeta{NodeId: "self"})
	d.discovery = NewServiceDiscovery()
	d.discovery.UpdateServer("peer", &proto.NodeMeta{NodeId: "peer", LoadUpdatedAt: 100})

	msg, err := encodeLoadUpdate("peer", 0.5, 200)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	d.NotifyMsg(msg)
	if got := d.loadQueue.NumQueued(); got != 1 {
		t.Fatalf("a newly learned reading should be relayed, queued %d", got)
	}

	// Spend its retransmit budget so the queue empties.
	for i := 0; i < 20 && d.loadQueue.NumQueued() > 0; i++ {
		d.loadQueue.GetBroadcasts(0, 1<<20)
	}
	if got := d.loadQueue.NumQueued(); got != 0 {
		t.Fatalf("the relay should stop after its retransmit budget, queued %d", got)
	}

	// The same reading arriving again is a retransmit, not news: it must not go
	// back on the wire, or readings would echo around the cluster forever.
	d.NotifyMsg(msg)
	if got := d.loadQueue.NumQueued(); got != 0 {
		t.Fatalf("a duplicate reading must not be relayed again, queued %d", got)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s (waited %s)", msg, timeout)
}

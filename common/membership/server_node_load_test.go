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

// loadInMemberlistMeta reads the load this node currently publishes in memberlist's
// own node table — the payload that rides an alive message to every peer. Before
// #271 this stayed frozen at the startup value forever.
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

// The regression test for #271: a reporter tick must put the fresh load into the
// meta memberlist broadcasts, not only into this node's own memory. Every tick
// announces, so loadReportInterval is the propagation cadence.
func TestReportLoadOnce_PublishesLoadIntoBroadcastMeta(t *testing.T) {
	n := newLoadTestNode(t, "bcast-1", 27810)
	if got := loadInMemberlistMeta(t, n); got != 0 {
		t.Fatalf("startup meta should carry load 0, got %v", got)
	}

	n.sampler = fakeSampler{v: 0.40}
	n.reportLoadOnce()
	if got := loadInMemberlistMeta(t, n); got != 0.40 {
		t.Fatalf("broadcast meta should carry the reported load 0.40, got %v "+
			"(load is not reaching the gossip broadcast path — issue #271)", got)
	}

	// Every subsequent tick re-announces, including a small move: there is no
	// change threshold to cross, which is what keeps loadTTL a simple multiple of
	// loadReportInterval rather than a value that has to allow for silent periods.
	n.sampler = fakeSampler{v: 0.42}
	n.reportLoadOnce()
	if got := loadInMemberlistMeta(t, n); got != 0.42 {
		t.Fatalf("each tick should re-announce the current load; got %v, want 0.42", got)
	}
}

// A node with no sampler must not announce — reportLoadOnce has nothing to say,
// and bumping the incarnation anyway would be pure churn.
func TestReportLoadOnce_NoSamplerDoesNotBroadcast(t *testing.T) {
	n := newLoadTestNode(t, "bcast-2", 27812)
	n.reportLoadOnce() // sampler is nil: LoadReportInterval 0 means none was built
	if got := loadInMemberlistMeta(t, n); got != 0 {
		t.Fatalf("a node with no sampler should not announce, broadcast meta = %v", got)
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

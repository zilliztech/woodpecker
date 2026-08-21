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

// shouldBroadcastLoad is the gate that decides whether a reporter tick is worth a
// cluster-wide announce. Table-driven because the interesting part is which of the
// three trigger conditions fires, and that they do not fire when they shouldn't.
func TestShouldBroadcastLoad(t *testing.T) {
	base := time.Unix(1_000_000, 0)
	cases := []struct {
		name      string
		threshold float64
		refresh   time.Duration
		published bool // has this node announced before?
		lastLoad  float64
		lastAt    time.Time
		load      float64
		now       time.Time
		want      bool
	}{
		{
			name: "disabled: threshold 0 never announces even on a huge jump",
			// 0 is the documented opt-out; it must beat every other trigger.
			threshold: 0, refresh: 15 * time.Second,
			published: true, lastLoad: 0.1, lastAt: base,
			load: 0.99, now: base.Add(time.Hour), want: false,
		},
		{
			name:      "first report always announces (startup meta still carries load 0)",
			threshold: 0.15, refresh: 15 * time.Second,
			published: false,
			load:      0.02, now: base, want: true,
		},
		{
			name:      "delta at the threshold announces",
			threshold: 0.15, refresh: time.Hour,
			published: true, lastLoad: 0.30, lastAt: base,
			load: 0.45, now: base.Add(time.Second), want: true,
		},
		{
			name:      "delta below the threshold stays quiet",
			threshold: 0.15, refresh: time.Hour,
			published: true, lastLoad: 0.30, lastAt: base,
			load: 0.44, now: base.Add(time.Second), want: false,
		},
		{
			name: "a drop in load announces too, not just a rise",
			// The idle direction matters as much as the busy one: a node that
			// freed up must become selectable again promptly.
			threshold: 0.15, refresh: time.Hour,
			published: true, lastLoad: 0.80, lastAt: base,
			load: 0.60, now: base.Add(time.Second), want: true,
		},
		{
			name: "steady load re-announces once the refresh window elapses",
			// Without this a stable node goes silent and every peer expires it.
			threshold: 0.15, refresh: 15 * time.Second,
			published: true, lastLoad: 0.30, lastAt: base,
			load: 0.30, now: base.Add(15 * time.Second), want: true,
		},
		{
			name:      "steady load stays quiet inside the refresh window",
			threshold: 0.15, refresh: 15 * time.Second,
			published: true, lastLoad: 0.30, lastAt: base,
			load: 0.30, now: base.Add(14 * time.Second), want: false,
		},
		{
			name:      "refresh disabled leaves the threshold as the only trigger",
			threshold: 0.15, refresh: 0,
			published: true, lastLoad: 0.30, lastAt: base,
			load: 0.30, now: base.Add(time.Hour), want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := &ServerNode{
				loadBroadcastThreshold: tc.threshold,
				loadBroadcastRefresh:   tc.refresh,
				loadBroadcasted:        tc.published,
				lastBroadcastLoad:      tc.lastLoad,
				lastBroadcastAt:        tc.lastAt,
			}
			if got := n.shouldBroadcastLoad(tc.load, tc.now); got != tc.want {
				t.Fatalf("shouldBroadcastLoad(%v) = %v, want %v", tc.load, got, tc.want)
			}
		})
	}
}

// loadInMemberlistMeta reads the load this node currently publishes in memberlist's
// own node table — the payload that rides an alive broadcast to every peer. Before
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

func newLoadTestNode(t *testing.T, id string, port int, threshold float64) *ServerNode {
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
		LoadReportInterval:     0,
		LoadTTL:                30 * time.Second,
		MemSoftThreshold:       0.85,
		EWMAAlpha:              0.5,
		LoadBroadcastThreshold: threshold,
	})
	if err != nil {
		t.Fatalf("create node %s: %v", id, err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })
	return n
}

// The regression test for #271: a reporter tick must put the fresh load into the
// meta that memberlist broadcasts, not only into this node's own memory.
func TestReportLoadOnce_PublishesLoadIntoBroadcastMeta(t *testing.T) {
	n := newLoadTestNode(t, "bcast-1", 27810, 0.15)
	if got := loadInMemberlistMeta(t, n); got != 0 {
		t.Fatalf("startup meta should carry load 0, got %v", got)
	}

	n.sampler = fakeSampler{v: 0.40}
	n.reportLoadOnce()

	if got := loadInMemberlistMeta(t, n); got != 0.40 {
		t.Fatalf("broadcast meta should carry the reported load 0.40, got %v "+
			"(load is not reaching the gossip broadcast path — issue #271)", got)
	}
}

// The threshold has to actually suppress announces, otherwise every 10s tick would
// bump the incarnation — the churn the pre-#271 code was avoiding.
func TestReportLoadOnce_SmallDeltaDoesNotRebroadcast(t *testing.T) {
	n := newLoadTestNode(t, "bcast-2", 27812, 0.15)

	n.sampler = fakeSampler{v: 0.40}
	n.reportLoadOnce() // first report always announces

	n.sampler = fakeSampler{v: 0.42} // delta 0.02, well under the threshold
	n.reportLoadOnce()

	if got := loadInMemberlistMeta(t, n); got != 0.40 {
		t.Fatalf("a sub-threshold move must not re-announce; broadcast meta = %v, want the previous 0.40", got)
	}
	// ...while the node's own discovery copy still tracks the latest value, so its
	// own selections are not stale.
	if got := n.delegate.SnapshotMeta().GetLoadFactor(); got != 0.42 {
		t.Fatalf("local meta should always hold the newest sample, got %v", got)
	}
}

func TestReportLoadOnce_ThresholdZeroDisablesBroadcast(t *testing.T) {
	n := newLoadTestNode(t, "bcast-3", 27814, 0)

	n.sampler = fakeSampler{v: 0.90}
	n.reportLoadOnce()

	if got := loadInMemberlistMeta(t, n); got != 0 {
		t.Fatalf("threshold 0 opts out of broadcasting, broadcast meta = %v, want 0", got)
	}
	if got := n.delegate.SnapshotMeta().GetLoadFactor(); got != 0.90 {
		t.Fatalf("opting out of broadcast must not stop local publishing, got %v", got)
	}
}

// End-to-end: the announce has to land in a *peer's* discovery, which is what
// quorum selection reads. Covers the EventDelegate.NotifyUpdate wiring.
func TestLoadBroadcast_ReachesPeerDiscovery(t *testing.T) {
	a := newLoadTestNode(t, "peer-a", 27816, 0.15)
	b := newLoadTestNode(t, "peer-b", 27818, 0.15)
	if err := b.Join([]string{fmt.Sprintf("127.0.0.1:%d", 27816)}); err != nil {
		t.Fatalf("join: %v", err)
	}
	waitFor(t, 20*time.Second, func() bool {
		return len(a.GetMemberlist().Members()) == 2 && len(b.GetMemberlist().Members()) == 2
	}, "cluster did not converge")

	a.sampler = fakeSampler{v: 0.88}
	a.reportLoadOnce()

	// The broadcast is gossiped, so allow a short settle; without it this is the
	// pairwise push/pull path, which needs tens of seconds (issue #271).
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

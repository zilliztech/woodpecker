// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integration

// Service-mode stability while one pod at a time is replaced.
//
// Each test runs a steady workload (an append every 20ms per log, several in
// flight, one tail reader per log) against a 5-node cluster whose nodes sit
// behind faultproxies, replaces or kills one pod, and asserts the workload
// never noticed in the way Milvus would notice: every append completed (a
// failed append is retried with backoff, as Milvus's WAL adaptor does), none
// took longer than stability.MaxStall end to end (2s by default,
// WP_STABILITY_MAX_STALL to override; measured from submission, so time queued
// inside the client and time spent retrying both count), and every acked entry
// reached the tail reader once, at its acked position, in log order, within the
// same budget. Read errors the reader reads on from are reported, not failed
// on: Milvus's scanner backs off and resumes, and what that costs is read lag.
//
// The proxies are what make this different from the failover tests. A plain
// localhost restart only ever refuses dials; a real pod replacement also has a
// window where the pod's name still resolves to an IP that no longer answers.
// See utils.PodPlan and faultproxy for the phases.

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/tests/utils/stability"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

const (
	podStabilityNodes    = 5
	podStabilityLogs     = 3
	podStabilityInterval = 20 * time.Millisecond
	podStabilityWarmup   = 3 * time.Second
	podStabilitySettle   = 5 * time.Second
)

type podLog struct {
	name   string
	handle log.LogHandle
	writer log.LogWriter
	reader log.LogReader
	w      *stability.Writer
	r      *stability.Reader
}

type podEnv struct {
	t       *testing.T
	cluster *utils.PodCluster
	client  woodpecker.Client
	logs    []*podLog
	// extraStall is added to the stall budget for faults that can only be
	// detected by a timeout; see vanishDetection.
	extraStall time.Duration
}

// vanishDetection is what a replica that vanishes costs before it is given up.
// A vanished replica answers nothing, not even a reset, so the client learns of
// it only when the bound on an append's first response expires (2s), after
// which the segment rolls. Faster detection would mean either a shorter bound,
// which would also fail replicas that are merely slow, or not waiting for the
// slowest replica at all, which is a change to the append pipeline this suite
// does not assume.
const vanishDetection = 2 * time.Second

// startPodEnv starts the cluster, a client that reaches it only through the
// pods' stable addresses, and a running workload on logCount logs.
func startPodEnv(t *testing.T, logCount int) *podEnv {
	return startPodEnvWith(t, logCount, false)
}

// startPodEnvWith is startPodEnv; with separateReader the tail readers use a
// client of their own, so they share no connections with the writers, the way
// a reader in another process would.
func startPodEnvWith(t *testing.T, logCount int, separateReader bool) *podEnv {
	rootPath := filepath.Join(t.TempDir(), t.Name())
	cluster, cfg := utils.StartPodCluster(t, podStabilityNodes, rootPath)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, cluster.ServiceSeeds())
	t.Cleanup(func() { bounded(t, "cluster stop", 60*time.Second, func() { cluster.Stop(t) }) })

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	t.Cleanup(func() { _ = etcdCli.Close() })

	ctx := context.Background()
	client, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	t.Cleanup(func() {
		bounded(t, "client close", 30*time.Second, func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = client.Close(closeCtx)
		})
	})

	readerClient := client
	if separateReader {
		readerClient, err = woodpecker.NewClient(ctx, cfg, etcdCli, true)
		require.NoError(t, err)
		t.Cleanup(func() {
			bounded(t, "reader client close", 30*time.Second, func() {
				closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = readerClient.Close(closeCtx)
			})
		})
	}

	env := &podEnv{t: t, cluster: cluster, client: client}
	for i := 0; i < logCount; i++ {
		name := fmt.Sprintf("pod_stability_%d_%d", time.Now().UnixNano(), i)
		require.NoError(t, client.CreateLog(ctx, name))
		handle, err := client.OpenLog(ctx, name)
		require.NoError(t, err)
		writer, err := handle.OpenLogWriter(ctx)
		require.NoError(t, err)
		readHandle := handle
		if separateReader {
			readHandle, err = readerClient.OpenLog(ctx, name)
			require.NoError(t, err)
		}
		reader, err := readHandle.OpenLogReader(ctx, &log.LogMessageId{SegmentId: 0, EntryId: 0}, "pod-stability-reader")
		require.NoError(t, err)
		env.logs = append(env.logs, &podLog{
			name: name, handle: handle, writer: writer, reader: reader,
			w: stability.StartWriter(name, writer, podStabilityInterval),
			r: stability.StartReader(name, reader),
		})
	}
	time.Sleep(podStabilityWarmup)
	return env
}

// quorumNodes returns the nodes holding each log's current writable segment.
func (e *podEnv) quorumNodes() [][]int {
	out := make([][]int, len(e.logs))
	for i, l := range e.logs {
		seg := l.handle.GetCurrentWritableSegmentHandle(context.Background())
		require.NotNil(e.t, seg, "log %s has no writable segment", l.name)
		qi, err := seg.GetQuorumInfo(context.Background())
		require.NoError(e.t, err)
		for _, addr := range qi.Nodes {
			idx, ok := e.cluster.NodeIndexByServiceAddr(addr)
			require.True(e.t, ok, "quorum node %s is not a pod address", addr)
			out[i] = append(out[i], idx)
		}
		e.t.Logf("log %s writable segment %d quorum nodes %v", l.name, seg.GetId(context.Background()), out[i])
	}
	return out
}

// busiestQuorumNode is the node in the most logs' writable quorums, so
// replacing it hits as many logs as possible.
func (e *podEnv) busiestQuorumNode() int {
	counts := make(map[int]int)
	for _, nodes := range e.quorumNodes() {
		for _, n := range nodes {
			counts[n]++
		}
	}
	best, bestCount := -1, 0
	for n, c := range counts {
		if c > bestCount || (c == bestCount && n < best) {
			best, bestCount = n, c
		}
	}
	return best
}

// busiestReadNode is the node the most tail readers are reading from. A reader
// starts at the first node of the segment's quorum and stays on it while it
// answers.
func (e *podEnv) busiestReadNode() int {
	counts := make(map[int]int)
	for _, nodes := range e.quorumNodes() {
		counts[nodes[0]]++
	}
	best, bestCount := -1, 0
	for n, c := range counts {
		if c > bestCount || (c == bestCount && n < best) {
			best, bestCount = n, c
		}
	}
	return best
}

// idleNode is a node in no log's writable quorum, or -1.
func (e *podEnv) idleNode() int {
	used := make(map[int]bool)
	for _, nodes := range e.quorumNodes() {
		for _, n := range nodes {
			used[n] = true
		}
	}
	for i := 0; i < podStabilityNodes; i++ {
		if !used[i] {
			return i
		}
	}
	return -1
}

// finish lets the cluster settle, stops the workload and asserts it was smooth.
func (e *podEnv) finish() {
	t := e.t
	time.Sleep(podStabilitySettle)

	maxStall := stability.MaxStall(t) + e.extraStall
	for _, l := range e.logs {
		writes := l.w.Stop(60 * time.Second)
		bounded(t, l.name+" writer close", 45*time.Second, func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := l.writer.Close(closeCtx); err != nil {
				t.Logf("[%s] writer close: %v", l.name, err)
			}
		})

		lastAcked := int64(-1)
		for _, w := range writes {
			if w.Err == nil && w.Seq > lastAcked {
				lastAcked = w.Seq
			}
		}
		reads, readErrs := l.r.WaitFor(lastAcked, 30*time.Second)
		bounded(t, l.name+" reader close", 30*time.Second, func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = l.reader.Close(closeCtx)
		})

		rep := stability.Analyze(l.name, writes, reads, readErrs)
		t.Log("\n" + rep.String())
		for _, sr := range l.r.SlowReads() {
			var at string
			if sr.Id != nil {
				at = fmt.Sprintf("%d/%d", sr.Id.SegmentId, sr.Id.EntryId)
			}
			t.Logf("[%s]   slow ReadNext %s -> %s took %v returned %s err=%v", l.name,
				sr.Start.Format("15:04:05.000"), sr.End.Format("15:04:05.000"), sr.End.Sub(sr.Start), at, sr.Err)
		}
		stability.AssertSmooth(t, rep, maxStall)
	}
	for i := 0; i < podStabilityNodes; i++ {
		t.Logf("proxy node%d mode=%s stats=%+v", i, e.cluster.Proxies[i].Mode(), e.cluster.Proxies[i].Stats())
	}
}

// bounded runs fn and reports an error if it has not returned within d, so a
// client call that ignores its context fails the test instead of hanging the
// whole run. The goroutine is abandoned.
func bounded(t *testing.T, what string, d time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(d):
		t.Errorf("%s did not return within %v (its context was not honoured)", what, d)
	}
}

// Baseline: no fault. If this is not smooth, nothing below means anything.
func TestServicePodStability_Baseline(t *testing.T) {
	env := startPodEnv(t, podStabilityLogs)
	time.Sleep(10 * time.Second)
	env.finish()
}

// A pod that holds no writable segment is replaced. Nothing should notice.
func TestServicePodStability_RestartIdlePod(t *testing.T) {
	env := startPodEnv(t, 1)
	idle := env.idleNode()
	require.GreaterOrEqual(t, idle, 0, "every node is in a quorum; cannot pick an idle one")
	env.cluster.PodRestart(t, idle, utils.RollingRestartPlan())
	env.finish()
}

// A quorum pod is restarted with no silent window: the old address refuses
// dials for a moment and the replacement serves right after. This is the only
// shape the existing localhost failover tests can produce.
func TestServicePodStability_RestartQuorumPod_NoSilence(t *testing.T) {
	env := startPodEnv(t, podStabilityLogs)
	env.cluster.PodRestart(t, env.busiestQuorumNode(), utils.PodPlan{RefuseFor: 500 * time.Millisecond})
	env.finish()
}

// A quorum pod gets a k8s rolling-restart replacement: graceful exit, the old
// IP refuses for ~0.5s, then goes silent for ~10s until the new pod serves.
// This is the production incident's shape.
func TestServicePodStability_RestartQuorumPod(t *testing.T) {
	env := startPodEnv(t, podStabilityLogs)
	env.cluster.PodRestart(t, env.busiestQuorumNode(), utils.RollingRestartPlan())
	env.finish()
}

// The same replacement, but the old IP's refusals arrive a round trip late, so
// the healthy replicas ack first and the client keeps the dead replica in the
// segment instead of rolling away from it. Production showed this ordering:
// the first failure against the replaced pod landed after its entry had
// already completed ("appendOp not found in queue").
func TestServicePodStability_RestartQuorumPod_SlowRefusal(t *testing.T) {
	env := startPodEnv(t, podStabilityLogs)
	plan := utils.RollingRestartPlan()
	plan.RefuseLatency = 200 * time.Millisecond
	env.cluster.PodRestart(t, env.busiestQuorumNode(), plan)
	env.finish()
}

// A quorum pod is killed (SIGKILL / OOM / crash): no gossip leave, connections
// reset, then the same refuse and silent windows before it comes back.
func TestServicePodStability_KillQuorumPod(t *testing.T) {
	env := startPodEnv(t, podStabilityLogs)
	plan := utils.RollingRestartPlan()
	plan.Kill = true
	env.cluster.PodRestart(t, env.busiestQuorumNode(), plan)
	env.finish()
}

// Kill with late refusals: no gossip leave, so every seed still offers the dead
// pod for new segments until probes catch up, and its failures lose the race
// against the healthy replicas' acks.
func TestServicePodStability_KillQuorumPod_SlowRefusal(t *testing.T) {
	env := startPodEnv(t, podStabilityLogs)
	plan := utils.RollingRestartPlan()
	plan.Kill = true
	plan.RefuseLatency = 200 * time.Millisecond
	env.cluster.PodRestart(t, env.busiestQuorumNode(), plan)
	env.finish()
}

// A quorum pod is killed and never comes back (node drained, pod unschedulable).
// Its address stays silent for the rest of the test.
func TestServicePodStability_KillQuorumPod_NeverReturns(t *testing.T) {
	env := startPodEnv(t, podStabilityLogs)
	env.cluster.PodRestart(t, env.busiestQuorumNode(), utils.PodPlan{
		Kill: true, RefuseFor: 500 * time.Millisecond, NoRestart: true,
	})
	time.Sleep(10 * time.Second)
	env.finish()
}

// A quorum pod vanishes from the network (its node dies or is partitioned):
// open connections go silent instead of resetting, and nothing answers until
// the replacement pod serves ~10s later.
func TestServicePodStability_VanishQuorumPod(t *testing.T) {
	env := startPodEnv(t, podStabilityLogs)
	env.extraStall = vanishDetection
	env.cluster.PodRestart(t, env.busiestQuorumNode(), utils.PodPlan{
		Vanish: true, BlackholeFor: 10 * time.Second,
	})
	env.finish()
}

// The pod the tail readers are reading from vanishes, and the readers run in a
// client of their own. Nothing on the write side can tear down the readers'
// connections for them, so only the read path's own bounds can move them to
// another replica.
func TestServicePodStability_VanishReadPod_SeparateReader(t *testing.T) {
	env := startPodEnvWith(t, podStabilityLogs, true)
	env.extraStall = vanishDetection
	env.cluster.PodRestart(t, env.busiestReadNode(), utils.PodPlan{
		Vanish: true, BlackholeFor: 10 * time.Second,
	})
	env.finish()
}

// Every pod is replaced in turn, the way an upgrade rolls a StatefulSet: the
// next pod goes only after the previous replacement has rejoined.
func TestServicePodStability_RollingRestartAllPods(t *testing.T) {
	env := startPodEnv(t, podStabilityLogs)
	order := make([]int, 0, podStabilityNodes)
	for i := 0; i < podStabilityNodes; i++ {
		order = append(order, i)
	}
	// StatefulSets roll from the highest ordinal down.
	sort.Sort(sort.Reverse(sort.IntSlice(order)))
	for _, idx := range order {
		env.cluster.PodRestart(t, idx, utils.RollingRestartPlan())
		time.Sleep(2 * time.Second)
	}
	env.finish()
}

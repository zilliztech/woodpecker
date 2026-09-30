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

// Service stability, process tier: one pod at a time is replaced while a
// steady workload runs. The same cases run against real containers in
// tests/docker/stability (Docker tier) and against a StatefulSet in
// tests/k8s/stability (K8s tier, nightly).
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
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/tests/utils/stability"
	"github.com/zilliztech/woodpecker/woodpecker"
)

const processStabilityNodes = 5

type processEnv struct {
	t       *testing.T
	cluster *utils.PodCluster
	*stability.Session
}

// startProcessEnv starts the cluster, a client that reaches it only through
// the pods' stable addresses, and a running workload on logCount logs. With
// separateReader the tail readers use a client of their own.
func startProcessEnv(t *testing.T, logCount int, separateReader bool) *processEnv {
	rootPath := filepath.Join(t.TempDir(), t.Name())
	cluster, cfg := utils.StartPodCluster(t, processStabilityNodes, rootPath)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, cluster.ServiceSeeds())
	t.Cleanup(func() { stability.Bounded(t, "cluster stop", 60*time.Second, func() { cluster.Stop(t) }) })

	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	t.Cleanup(func() { _ = etcdCli.Close() })

	newClient := func(what string) woodpecker.Client {
		c, err := woodpecker.NewClient(context.Background(), cfg, etcdCli, true)
		require.NoError(t, err)
		t.Cleanup(func() {
			stability.Bounded(t, what+" close", 30*time.Second, func() {
				closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = c.Close(closeCtx)
			})
		})
		return c
	}
	sc := stability.SessionConfig{Logs: logCount}
	client := newClient("client")
	if separateReader {
		sc.ReaderClient = newClient("reader client")
	}
	return &processEnv{t: t, cluster: cluster, Session: stability.StartSession(t, client, sc)}
}

// node maps a service address from a quorum to the pod behind it.
func (e *processEnv) node(addr string) int {
	idx, ok := e.cluster.NodeIndexByServiceAddr(addr)
	require.True(e.t, ok, "quorum node %q is not a pod address", addr)
	return idx
}

func (e *processEnv) busiestQuorumNode() int { return e.node(e.BusiestQuorumAddr()) }
func (e *processEnv) busiestReadNode() int   { return e.node(e.BusiestReadAddr()) }

// idleNode is a node in no log's writable quorum, or -1.
func (e *processEnv) idleNode() int {
	addr := e.IdleAddr(e.cluster.ServiceSeeds())
	if addr == "" {
		return -1
	}
	return e.node(addr)
}

// finish lets the cluster settle, stops the workload and asserts it was smooth.
func (e *processEnv) finish() {
	e.Finish(stability.DefaultSettle)
	for i := 0; i < processStabilityNodes; i++ {
		e.t.Logf("proxy node%d mode=%s stats=%+v", i, e.cluster.Proxies[i].Mode(), e.cluster.Proxies[i].Stats())
	}
}

// Baseline: no fault. If this is not smooth, nothing below means anything.
func TestServiceStabilityProcess_Baseline(t *testing.T) {
	env := startProcessEnv(t, stability.DefaultLogs, false)
	time.Sleep(10 * time.Second)
	env.finish()
}

// A pod that holds no writable segment is replaced. Nothing should notice.
func TestServiceStabilityProcess_RestartIdlePod(t *testing.T) {
	env := startProcessEnv(t, 1, false)
	idle := env.idleNode()
	require.GreaterOrEqual(t, idle, 0, "every node is in a quorum; cannot pick an idle one")
	env.cluster.PodRestart(t, idle, utils.RollingRestartPlan())
	env.finish()
}

// A quorum pod is restarted with no silent window: the old address refuses
// dials for a moment and the replacement serves right after. This is the only
// shape the existing localhost failover tests can produce.
func TestServiceStabilityProcess_RestartQuorumPod_NoSilence(t *testing.T) {
	env := startProcessEnv(t, stability.DefaultLogs, false)
	env.cluster.PodRestart(t, env.busiestQuorumNode(), utils.PodPlan{RefuseFor: 500 * time.Millisecond})
	env.finish()
}

// A quorum pod gets a k8s rolling-restart replacement: graceful exit, the old
// IP refuses for ~0.5s, then goes silent for ~10s until the new pod serves.
// This is the production incident's shape.
func TestServiceStabilityProcess_RestartQuorumPod(t *testing.T) {
	env := startProcessEnv(t, stability.DefaultLogs, false)
	env.cluster.PodRestart(t, env.busiestQuorumNode(), utils.RollingRestartPlan())
	env.finish()
}

// The same replacement, but the old IP's refusals arrive a round trip late, so
// the healthy replicas ack first and the client keeps the dead replica in the
// segment instead of rolling away from it. Production showed this ordering:
// the first failure against the replaced pod landed after its entry had
// already completed ("appendOp not found in queue").
func TestServiceStabilityProcess_RestartQuorumPod_SlowRefusal(t *testing.T) {
	env := startProcessEnv(t, stability.DefaultLogs, false)
	plan := utils.RollingRestartPlan()
	plan.RefuseLatency = 200 * time.Millisecond
	env.cluster.PodRestart(t, env.busiestQuorumNode(), plan)
	env.finish()
}

// A quorum pod is killed (SIGKILL / OOM / crash): no gossip leave, connections
// reset, then the same refuse and silent windows before it comes back.
func TestServiceStabilityProcess_KillQuorumPod(t *testing.T) {
	env := startProcessEnv(t, stability.DefaultLogs, false)
	plan := utils.RollingRestartPlan()
	plan.Kill = true
	env.cluster.PodRestart(t, env.busiestQuorumNode(), plan)
	env.finish()
}

// Kill with late refusals: no gossip leave, so every seed still offers the dead
// pod for new segments until probes catch up, and its failures lose the race
// against the healthy replicas' acks.
func TestServiceStabilityProcess_KillQuorumPod_SlowRefusal(t *testing.T) {
	env := startProcessEnv(t, stability.DefaultLogs, false)
	plan := utils.RollingRestartPlan()
	plan.Kill = true
	plan.RefuseLatency = 200 * time.Millisecond
	env.cluster.PodRestart(t, env.busiestQuorumNode(), plan)
	env.finish()
}

// A quorum pod is killed and never comes back (node drained, pod unschedulable).
// Its address stays silent for the rest of the test.
func TestServiceStabilityProcess_KillQuorumPod_NeverReturns(t *testing.T) {
	env := startProcessEnv(t, stability.DefaultLogs, false)
	env.cluster.PodRestart(t, env.busiestQuorumNode(), utils.PodPlan{
		Kill: true, RefuseFor: 500 * time.Millisecond, NoRestart: true,
	})
	time.Sleep(10 * time.Second)
	env.finish()
}

// A quorum pod vanishes from the network (its node dies or is partitioned):
// open connections go silent instead of resetting, and nothing answers until
// the replacement pod serves ~10s later.
func TestServiceStabilityProcess_VanishQuorumPod(t *testing.T) {
	env := startProcessEnv(t, stability.DefaultLogs, false)
	env.ExtraStall = stability.VanishDetection
	env.cluster.PodRestart(t, env.busiestQuorumNode(), utils.PodPlan{
		Vanish: true, BlackholeFor: 10 * time.Second,
	})
	env.finish()
}

// The pod the tail readers are reading from vanishes, and the readers run in a
// client of their own. Nothing on the write side can tear down the readers'
// connections for them, so only the read path's own bounds can move them to
// another replica.
func TestServiceStabilityProcess_VanishReadPod_SeparateReader(t *testing.T) {
	env := startProcessEnv(t, stability.DefaultLogs, true)
	env.ExtraStall = stability.VanishReadDetection
	env.cluster.PodRestart(t, env.busiestReadNode(), utils.PodPlan{
		Vanish: true, BlackholeFor: 10 * time.Second,
	})
	env.finish()
}

// Every pod is replaced in turn, the way an upgrade rolls a StatefulSet: the
// next pod goes only after the previous replacement has rejoined.
func TestServiceStabilityProcess_RollingRestartAllPods(t *testing.T) {
	env := startProcessEnv(t, stability.DefaultLogs, false)
	order := make([]int, 0, processStabilityNodes)
	for i := 0; i < processStabilityNodes; i++ {
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

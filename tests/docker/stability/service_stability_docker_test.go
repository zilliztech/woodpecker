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

package stability

// Service stability, Docker tier: the cases of the process tier
// (tests/integration/service_stability_process_test.go) against real
// woodpecker containers, faulted with docker itself: SIGKILL, a graceful stop,
// a frozen process. The workload and its assertions are the same
// (tests/utils/stability).
//
// What this tier cannot show: the client reaches every node through a host
// port mapping, so a restarted node keeps its address, and a dial never goes
// to an address nothing answers on. The process tier models that window with
// its proxies, and the K8s tier gets it for real from a pod's new IP.
//
// The cluster is started outside the tests (run_service_stability.sh or the
// workflow). Every test brings all nodes back before it returns.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/tests/docker/framework"
	harness "github.com/zilliztech/woodpecker/tests/utils/stability"
	"github.com/zilliztech/woodpecker/woodpecker"
)

// replacementDelay is how long a killed node stays down before it is started
// again, as a pod is rescheduled.
const replacementDelay = 10 * time.Second

type dockerEnv struct {
	t       *testing.T
	cluster *StabilityCluster
	*harness.Session
}

func startDockerEnv(t *testing.T, logCount int, separateReader bool) *dockerEnv {
	cluster := NewStabilityCluster(t)
	cluster.WaitConverged(t, 60*time.Second)
	t.Cleanup(func() { cluster.RestoreAll(t) })

	cfg := cluster.NewConfig(t)
	cfg.Log.Level = "info"
	etcdCli := cluster.NewEtcdClient(t, cfg)
	t.Cleanup(func() { _ = etcdCli.Close() })

	newClient := func(what string) woodpecker.Client {
		c, err := woodpecker.NewClient(context.Background(), cfg, etcdCli, true)
		require.NoError(t, err)
		t.Cleanup(func() {
			harness.Bounded(t, what+" close", 30*time.Second, func() {
				closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_ = c.Close(closeCtx)
			})
		})
		return c
	}
	sc := harness.SessionConfig{Logs: logCount}
	client := newClient("client")
	if separateReader {
		sc.ReaderClient = newClient("reader client")
	}
	return &dockerEnv{t: t, cluster: cluster, Session: harness.StartSession(t, client, sc)}
}

func (e *dockerEnv) busiestQuorumNode() framework.NodeInfo {
	return e.cluster.NodeByServiceAddr(e.t, e.BusiestQuorumAddr())
}

func (e *dockerEnv) busiestReadNode() framework.NodeInfo {
	return e.cluster.NodeByServiceAddr(e.t, e.BusiestReadAddr())
}

func (e *dockerEnv) finish() { e.Finish(harness.DefaultSettle) }

// Baseline: no fault. If this is not smooth, nothing below means anything.
func TestServiceStabilityDocker_Baseline(t *testing.T) {
	env := startDockerEnv(t, harness.DefaultLogs, false)
	time.Sleep(10 * time.Second)
	env.finish()
}

// A node that holds no writable segment is restarted. Nothing should notice.
func TestServiceStabilityDocker_RestartIdlePod(t *testing.T) {
	env := startDockerEnv(t, 1, false)
	addr := env.IdleAddr(env.cluster.ServiceAddrs())
	require.NotEmpty(t, addr, "every node is in a quorum; cannot pick an idle one")
	env.cluster.Restart(t, env.cluster.NodeByServiceAddr(t, addr))
	env.cluster.WaitConverged(t, 60*time.Second)
	env.finish()
}

// A quorum node is restarted gracefully: SIGTERM, a gossip leave, a drain,
// then the same container starts again.
func TestServiceStabilityDocker_RestartQuorumPod(t *testing.T) {
	env := startDockerEnv(t, harness.DefaultLogs, false)
	env.cluster.Restart(t, env.busiestQuorumNode())
	env.cluster.WaitConverged(t, 60*time.Second)
	env.finish()
}

// A quorum node is killed (SIGKILL: no gossip leave) and started again after
// the time a replacement takes.
func TestServiceStabilityDocker_KillQuorumPod(t *testing.T) {
	env := startDockerEnv(t, harness.DefaultLogs, false)
	n := env.busiestQuorumNode()
	env.cluster.Kill(t, n)
	time.Sleep(replacementDelay)
	env.cluster.StartNode(t, n.ContainerName)
	env.cluster.WaitConverged(t, 60*time.Second)
	env.finish()
}

// A quorum node is killed and does not come back while the workload runs.
func TestServiceStabilityDocker_KillQuorumPod_NeverReturns(t *testing.T) {
	env := startDockerEnv(t, harness.DefaultLogs, false)
	env.cluster.Kill(t, env.busiestQuorumNode())
	time.Sleep(replacementDelay)
	env.finish()
}

// A quorum node freezes for 10s: its connections stay open and nothing on
// them answers, and no reset tells the client so.
func TestServiceStabilityDocker_VanishQuorumPod(t *testing.T) {
	env := startDockerEnv(t, harness.DefaultLogs, false)
	env.ExtraStall = harness.VanishDetection
	n := env.busiestQuorumNode()
	env.cluster.Pause(t, n)
	time.Sleep(replacementDelay)
	env.cluster.Unpause(t, n)
	env.cluster.WaitConverged(t, 60*time.Second)
	env.finish()
}

// The node the tail readers read from freezes, and the readers run in a
// client of their own, so only the read path's own bounds can move them.
func TestServiceStabilityDocker_VanishReadPod_SeparateReader(t *testing.T) {
	env := startDockerEnv(t, harness.DefaultLogs, true)
	env.ExtraStall = harness.VanishReadDetection
	n := env.busiestReadNode()
	env.cluster.Pause(t, n)
	time.Sleep(replacementDelay)
	env.cluster.Unpause(t, n)
	env.cluster.WaitConverged(t, 60*time.Second)
	env.finish()
}

// Every node is restarted in turn, the next one only after the cluster has
// reconverged, as a StatefulSet rolls from the highest ordinal down.
func TestServiceStabilityDocker_RollingRestartAllPods(t *testing.T) {
	env := startDockerEnv(t, harness.DefaultLogs, false)
	for i := len(env.cluster.Nodes) - 1; i >= 0; i-- {
		env.cluster.Restart(t, env.cluster.Nodes[i])
		env.cluster.WaitConverged(t, 60*time.Second)
		time.Sleep(2 * time.Second)
	}
	env.finish()
}

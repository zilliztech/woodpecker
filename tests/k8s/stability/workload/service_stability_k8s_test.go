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

// Package workload is the K8s tier of the service stability suite: the cases
// of the process tier (tests/integration/service_stability_process_test.go)
// against a woodpecker StatefulSet, with the workload running in a client pod
// and the fault injected from outside by run_service_stability.sh.
//
// Here a replaced pod comes back with a new IP behind the same DNS name, the
// window the process tier can only model with its proxies.
//
// One test per case. Each starts the workload, writes the address of the node
// the fault should hit to <state-dir>/<case>.target, waits for the script to
// inject the fault and wait out the recovery (<state-dir>/<case>.done), then
// stops the workload and checks it. The checks are those of the other tiers;
// WP_STABILITY_LATENCY=report turns the stall budget into a report.
package workload

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	harness "github.com/zilliztech/woodpecker/tests/utils/stability"
	"github.com/zilliztech/woodpecker/woodpecker"
)

var (
	configFile   = flag.String("config-file", "/tmp/test-config.yaml", "woodpecker client config")
	stateDir     = flag.String("state-dir", "/tmp/wp-stability", "where the target and done markers are exchanged with the runner")
	faultTimeout = flag.Duration("fault-timeout", 10*time.Minute, "how long to wait for the runner to finish a fault")
)

type k8sEnv struct {
	t *testing.T
	*harness.Session
}

func startK8sEnv(t *testing.T, logCount int, separateReader bool) *k8sEnv {
	if _, err := os.Stat(*configFile); err != nil {
		t.Skipf("no client config at %s: these tests run in the client pod of run_service_stability.sh", *configFile)
	}
	cfg, err := config.NewConfiguration(*configFile)
	require.NoError(t, err)
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
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
	return &k8sEnv{t: t, Session: harness.StartSession(t, client, sc)}
}

// caseName is the part of the test name after TestServiceStabilityK8s_.
func (e *k8sEnv) caseName() string {
	const prefix = "TestServiceStabilityK8s_"
	return e.t.Name()[len(prefix):]
}

// fault hands the runner the node to fault ("" when the case needs none) and
// waits until it says the fault is over and the cluster has recovered.
func (e *k8sEnv) fault(target string) {
	t := e.t
	require.NoError(t, os.MkdirAll(*stateDir, 0o755))
	done := filepath.Join(*stateDir, e.caseName()+".done")
	_ = os.Remove(done)
	targetFile := filepath.Join(*stateDir, e.caseName()+".target")
	require.NoError(t, os.WriteFile(targetFile+".tmp", []byte(target), 0o644))
	require.NoError(t, os.Rename(targetFile+".tmp", targetFile))
	t.Logf("fault target %q written to %s; waiting for %s", target, targetFile, done)

	deadline := time.Now().Add(*faultTimeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(done); err == nil {
			_ = os.Remove(done)
			_ = os.Remove(targetFile)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("the runner did not finish the fault within %v", *faultTimeout)
}

func (e *k8sEnv) finish() { e.Finish(harness.DefaultSettle) }

// Baseline: no fault (the runner only waits).
func TestServiceStabilityK8s_Baseline(t *testing.T) {
	env := startK8sEnv(t, harness.DefaultLogs, false)
	env.fault("")
	env.finish()
}

// A pod that holds no writable segment is deleted gracefully and replaced.
func TestServiceStabilityK8s_RestartIdlePod(t *testing.T) {
	env := startK8sEnv(t, 1, false)
	target := env.IdleAddrOf()
	require.NotEmpty(t, target, "every pod is in a quorum; cannot pick an idle one")
	env.fault(target)
	env.finish()
}

// A quorum pod is deleted gracefully (SIGTERM, gossip leave, drain) and the
// StatefulSet replaces it: same DNS name, new IP.
func TestServiceStabilityK8s_RestartQuorumPod(t *testing.T) {
	env := startK8sEnv(t, harness.DefaultLogs, false)
	env.fault(env.BusiestQuorumAddr())
	env.finish()
}

// A quorum pod is force-deleted (SIGKILL, no gossip leave) and replaced.
func TestServiceStabilityK8s_KillQuorumPod(t *testing.T) {
	env := startK8sEnv(t, harness.DefaultLogs, false)
	env.fault(env.BusiestQuorumAddr())
	env.finish()
}

// A quorum pod's container is taken down (Chaos Mesh pod-failure) and does not
// run again while the workload does.
func TestServiceStabilityK8s_KillQuorumPod_NeverReturns(t *testing.T) {
	env := startK8sEnv(t, harness.DefaultLogs, false)
	env.fault(env.BusiestQuorumAddr())
	env.finish()
}

// A quorum pod is partitioned from everything (Chaos Mesh NetworkChaos): its
// connections go silent instead of resetting.
func TestServiceStabilityK8s_VanishQuorumPod(t *testing.T) {
	env := startK8sEnv(t, harness.DefaultLogs, false)
	env.ExtraStall = harness.VanishDetection
	env.fault(env.BusiestQuorumAddr())
	env.finish()
}

// The pod the tail readers read from is partitioned, and the readers run in a
// client of their own.
func TestServiceStabilityK8s_VanishReadPod_SeparateReader(t *testing.T) {
	env := startK8sEnv(t, harness.DefaultLogs, true)
	env.ExtraStall = harness.VanishReadDetection
	env.fault(env.BusiestReadAddr())
	env.finish()
}

// Every pod is deleted gracefully in turn, from the highest ordinal down, the
// next only after the previous replacement has rejoined.
func TestServiceStabilityK8s_RollingRestartAllPods(t *testing.T) {
	env := startK8sEnv(t, harness.DefaultLogs, false)
	env.fault("")
	env.finish()
}

// IdleAddrOf is a pod address in no log's writable quorum, taken from the
// seeds, which list every pod.
func (e *k8sEnv) IdleAddrOf() string {
	cfg, err := config.NewConfiguration(*configFile)
	require.NoError(e.t, err)
	var all []string
	for _, p := range cfg.Woodpecker.Client.Quorum.BufferPools.Get() {
		all = append(all, p.Seeds...)
	}
	return e.IdleAddr(all)
}

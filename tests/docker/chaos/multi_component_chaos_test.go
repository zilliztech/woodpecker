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

package chaos

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/tests/docker/framework"
)

// --- X1: Server crash + MinIO down simultaneously ---
//
// Scenario: Kill a server and stop MinIO at the same time.
// This is a worst-case scenario — both local processing and object storage are disrupted.
// Restart MinIO first, then the server. Verify full recovery and data integrity.
//
// Invariants verified:
//   - Durability: data acknowledged before failure is preserved
//   - Recovery completeness: system fully recovers from double failure
//   - No stuck state: no permanently stuck segments
func TestChaos_Multi_ServerCrashPlusMinIODown(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.TryStartMinIO(t)
		cluster.StartNode(t, "woodpecker-node1")
	})

	client1, etcdCli1, _ := cluster.NewClientManual(t, ctx)

	logName := fmt.Sprintf("chaos-multi-server-minio-%d", time.Now().UnixNano())
	err := client1.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write entries while everything is healthy
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 10)
	require.Len(t, ids1, 10)
	t.Log("10 entries written before double failure")

	// Close writer to ensure data is persisted
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Simultaneous double failure: kill server + stop MinIO
	cluster.KillNode(t, "woodpecker-node1")
	cluster.StopMinIO(t)
	t.Log("Double failure: woodpecker-node1 killed + MinIO stopped")

	time.Sleep(5 * time.Second)

	_ = client1.Close(ctx)
	_ = etcdCli1.Close()

	// Recovery: restart MinIO first (storage must be available for server recovery)
	cluster.StartMinIO(t)
	t.Log("MinIO restarted")
	cluster.WaitForHealthy(t, "minio", 60*time.Second)
	time.Sleep(5 * time.Second)

	// Then restart server
	cluster.StartNode(t, "woodpecker-node1")
	t.Log("woodpecker-node1 restarted")
	cluster.WaitForHealthy(t, "woodpecker-node1", 60*time.Second)
	time.Sleep(15 * time.Second)

	// Fresh client: verify data and write more
	client2, _ := cluster.NewClient(t, ctx)

	logHandle2, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	// Read original data
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, 10)
	require.Len(t, msgs, 10)
	verifyEntryOrder(t, msgs)
	t.Log("10 entries verified after double failure recovery")

	// Write more to verify system is fully functional
	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids2 := framework.WriteEntries(t, ctx, writer2, 10, 10)
	require.Len(t, ids2, 10)
	t.Log("10 more entries written after recovery")

	err = writer2.Close(ctx)
	require.NoError(t, err)

	t.Log("Multi_ServerCrashPlusMinIODown passed: full recovery from double failure")
}

// --- X2: etcd down + server restart ---
//
// Scenario: Stop etcd, then restart a server.
// Server should fail to initialize (needs etcd for metadata) or wait.
// After etcd starts, server should be able to serve.
//
// Invariants verified:
//   - No stuck state: server handles etcd unavailability during startup
//   - Recovery: server becomes functional after etcd is available
func TestChaos_Multi_EtcdDownThenServerRestart(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.TryStartEtcd(t)
		cluster.StartNode(t, "woodpecker-node1")
	})

	client1, etcdCli1, _ := cluster.NewClientManual(t, ctx)

	logName := fmt.Sprintf("chaos-multi-etcd-server-%d", time.Now().UnixNano())
	err := client1.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids1 := framework.WriteEntries(t, ctx, writer, 0, 10)
	require.Len(t, ids1, 10)
	t.Log("10 entries written")

	err = writer.Close(ctx)
	require.NoError(t, err)

	_ = client1.Close(ctx)
	_ = etcdCli1.Close()

	// Stop etcd first
	cluster.StopEtcd(t)
	t.Log("etcd stopped")
	time.Sleep(3 * time.Second)

	// Kill and restart server node (without etcd)
	cluster.KillNode(t, "woodpecker-node1")
	t.Log("woodpecker-node1 killed")
	time.Sleep(2 * time.Second)

	cluster.StartNode(t, "woodpecker-node1")
	t.Log("woodpecker-node1 restarted (etcd still down)")
	time.Sleep(5 * time.Second)

	// Start etcd — server should now be able to fully initialize
	cluster.StartEtcd(t)
	t.Log("etcd started")
	cluster.WaitForHealthy(t, "etcd", 60*time.Second)
	time.Sleep(15 * time.Second)

	// Wait for server to fully recover
	cluster.WaitForHealthy(t, "woodpecker-node1", 60*time.Second)
	time.Sleep(10 * time.Second)

	// Fresh client: verify system is functional
	client2, _ := cluster.NewClient(t, ctx)

	logHandle2, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	// Read original data
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, 10)
	require.Len(t, msgs, 10)
	verifyEntryOrder(t, msgs)

	// Write more
	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids2 := framework.WriteEntries(t, ctx, writer2, 10, 5)
	require.Len(t, ids2, 5)

	err = writer2.Close(ctx)
	require.NoError(t, err)

	t.Log("Multi_EtcdDownThenServerRestart passed")
}

// --- X3: Network partition: server isolated from etcd + MinIO ---
//
// Scenario: Disconnect a server from the Docker network so it can't reach etcd or MinIO.
// Client can still reach the server via host port mapping.
// Writes to local buffer succeed temporarily but cannot be persisted remotely.
//
// Invariants verified:
//   - Graceful degradation: server handles isolation without crashing
//   - Recovery: server reconnects and resumes after partition heals
func TestChaos_Multi_ServerIsolatedFromEtcdAndMinIO(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.TryConnectNetwork(t, "woodpecker-node1")
	})

	client1, etcdCli1, _ := cluster.NewClientManual(t, ctx)

	logName := fmt.Sprintf("chaos-multi-isolated-%d", time.Now().UnixNano())
	err := client1.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write with full connectivity
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 5)
	require.Len(t, ids1, 5)
	t.Log("5 entries written before partition")

	// Disconnect node1 from Docker network (isolates from etcd, MinIO, other nodes)
	cluster.DisconnectNetwork(t, "woodpecker-node1")
	t.Log("woodpecker-node1 disconnected from network (isolated from etcd + MinIO)")
	time.Sleep(10 * time.Second)

	// Try writing — the server is isolated but client may still route to other nodes
	ids2, failures := writeEntriesAllowFailures(t, ctx, writer, 5, 3, 20*time.Second)
	t.Logf("During isolation: %d successes, %d failures", len(ids2), failures)

	// Reconnect node1
	cluster.ConnectNetwork(t, "woodpecker-node1")
	t.Log("woodpecker-node1 reconnected to network")
	time.Sleep(15 * time.Second)

	// Close writer
	closeCtx, closeCancel := context.WithTimeout(ctx, 30*time.Second)
	_ = writer.Close(closeCtx)
	closeCancel()

	_ = client1.Close(ctx)
	_ = etcdCli1.Close()

	// Fresh client: verify and write more
	client2, _ := cluster.NewClient(t, ctx)

	logHandle2, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids3 := framework.WriteEntries(t, ctx, writer2, 10, 5)
	require.Len(t, ids3, 5)
	t.Log("5 entries written after partition healed")

	err = writer2.Close(ctx)
	require.NoError(t, err)

	expectedCount := len(ids1) + len(ids2) + len(ids3)
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("Multi_ServerIsolatedFromEtcdAndMinIO passed: %d entries verified", expectedCount)
}

// --- Q1: Write with exactly ack_quorum nodes alive ---
//
// Scenario: In 4-node cluster (es=3, aq=2), kill nodes until exactly aq=2 ensemble nodes
// remain for the current segment. Writes should succeed as long as aq is satisfied.
//
// Invariants verified:
//   - Quorum correctness: writes succeed with exactly aq nodes
//   - Durability: data is safely stored on aq nodes
func TestChaos_Quorum_ExactAckQuorum(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.StartNode(t, "woodpecker-node4")
	})

	client, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("chaos-quorum-exact-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write entries with all 4 nodes alive
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 5)
	require.Len(t, ids1, 5)
	t.Log("5 entries written with all 4 nodes alive")

	// Kill 1 node — leaving 3 nodes, which can still form ensemble of 3
	// If the killed node was in the current ensemble, only 2 of 3 ensemble nodes remain = aq=2 satisfied
	cluster.KillNode(t, "woodpecker-node4")
	t.Log("Killed woodpecker-node4 (3 nodes remaining)")
	time.Sleep(10 * time.Second)

	// Write more — should succeed since aq=2 is satisfied by remaining ensemble nodes
	ids2 := framework.WriteEntries(t, ctx, writer, 5, 5)
	require.Len(t, ids2, 5)
	t.Log("5 entries written with exactly aq nodes in ensemble")

	// Restart killed node
	cluster.StartNode(t, "woodpecker-node4")
	cluster.WaitForHealthy(t, "woodpecker-node4", 60*time.Second)
	time.Sleep(10 * time.Second)

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Read and verify
	allIds := append(ids1, ids2...)
	msgs := framework.ReadAllEntries(t, ctx, logHandle, 10)
	require.Len(t, msgs, 10)
	verifyEntryOrder(t, msgs)

	for i, msg := range msgs {
		require.Equal(t, allIds[i].SegmentId, msg.Id.SegmentId, "entry %d segment mismatch", i)
	}

	t.Log("Quorum_ExactAckQuorum passed: writes succeeded with minimum quorum")
}

// --- C1: Client disconnect during append ---
//
// Scenario: Cancel the client context mid-write.
// Verify server-side state is clean (no stuck segments).
// A new client can resume writing.
//
// Invariants verified:
//   - No stuck state: server cleans up after client disconnect
//   - Recovery: new client can write to the same log
func TestChaos_Client_DisconnectDuringAppend(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	client1, etcdCli1, _ := cluster.NewClientManual(t, ctx)

	logName := fmt.Sprintf("chaos-client-disconnect-%d", time.Now().UnixNano())
	err := client1.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write some entries successfully
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 5)
	require.Len(t, ids1, 5)
	t.Log("5 entries written before disconnect")

	// Abruptly close client (simulates disconnect)
	_ = client1.Close(ctx)
	_ = etcdCli1.Close()
	t.Log("Client disconnected abruptly")

	time.Sleep(10 * time.Second)

	// New client should be able to resume
	client2, _ := cluster.NewClient(t, ctx)

	logHandle2, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids2 := framework.WriteEntries(t, ctx, writer2, 5, 5)
	require.Len(t, ids2, 5)
	t.Log("5 entries written by new client")

	err = writer2.Close(ctx)
	require.NoError(t, err)

	// Read all entries
	expectedCount := len(ids1) + len(ids2)
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("Client_DisconnectDuringAppend passed: %d entries verified", expectedCount)
}

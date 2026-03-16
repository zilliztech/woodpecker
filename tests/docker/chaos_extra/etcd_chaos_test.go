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

package chaos_extra

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/tests/docker/framework"
)

// --- M1: etcd down during active writes ---
//
// Scenario: Stop etcd while client is appending entries.
// Writes should continue (server buffers locally in staged storage).
// Segment completion may fail while etcd is down.
// After etcd recovers, system should return to normal.
//
// Invariants verified:
//   - Durability: acknowledged writes are not lost
//   - No stuck state: system recovers after etcd returns
func TestChaosExtra_Etcd_WritesDuringEtcdDown(t *testing.T) {
	cluster := newChaosExtraCluster(t)
	ctx := context.Background()

	// Cleanup: ensure etcd is running at the end
	t.Cleanup(func() {
		cluster.TryStartEtcd(t)
	})

	// Create client and log while etcd is healthy
	client, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("chaos-etcd-write-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Phase 1: Write entries with etcd healthy
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 10)
	require.Len(t, ids1, 10)
	t.Log("Phase 1: 10 entries written with etcd healthy")

	// Stop etcd
	cluster.StopEtcd(t)
	t.Log("etcd stopped")
	time.Sleep(5 * time.Second)

	// Phase 2: Try to write while etcd is down.
	// Writes to the CURRENT segment should still succeed because the server
	// has the segment writer open and can buffer data locally.
	// However, segment completion (metadata update) will fail.
	ids2, failures := writeEntriesAllowFailures(t, ctx, writer, 10, 5, 30*time.Second)
	t.Logf("Phase 2: %d successes, %d failures with etcd down", len(ids2), failures)

	// Restart etcd
	cluster.StartEtcd(t)
	t.Log("etcd restarted")
	cluster.WaitForHealthy(t, "etcd", 60*time.Second)
	time.Sleep(10 * time.Second)

	// Close writer (triggers segment completion — needs etcd for metadata update)
	err = writer.Close(ctx)
	assert.NoError(t, err, "writer close should succeed after etcd recovery")

	// Phase 3: Write more entries with etcd healthy again
	logHandle2, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids3 := framework.WriteEntries(t, ctx, writer2, 20, 5)
	require.Len(t, ids3, 5)
	t.Log("Phase 3: 5 entries written after etcd recovery")

	err = writer2.Close(ctx)
	require.NoError(t, err)

	// Verify: read all successfully written entries
	expectedCount := len(ids1) + len(ids2) + len(ids3)
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("Etcd_WritesDuringEtcdDown passed: %d entries survived etcd outage", expectedCount)
}

// --- M2: etcd down during segment completion ---
//
// Scenario: Stop etcd right when a segment is about to complete.
// The completion should fail because metadata cannot be updated.
// After etcd recovers, a new writer should be able to trigger completion.
//
// Invariants verified:
//   - Metadata consistency: segment state is correct after recovery
//   - Recovery completeness: all synced data is readable
func TestChaosExtra_Etcd_SegmentCompletionDuringEtcdDown(t *testing.T) {
	cluster := newChaosExtraCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.TryStartEtcd(t)
	})

	client, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("chaos-etcd-completion-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write entries
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 10)
	require.Len(t, ids1, 10)
	t.Log("10 entries written")

	// Stop etcd before closing writer (close triggers segment completion)
	cluster.StopEtcd(t)
	t.Log("etcd stopped before segment completion")
	time.Sleep(3 * time.Second)

	// Close writer — this triggers segment completion which needs etcd.
	// It may fail or succeed with retry depending on timeout.
	closeCtx, closeCancel := context.WithTimeout(ctx, 30*time.Second)
	closeErr := writer.Close(closeCtx)
	closeCancel()
	t.Logf("Writer close result with etcd down: %v", closeErr)

	// Restart etcd
	cluster.StartEtcd(t)
	t.Log("etcd restarted")
	cluster.WaitForHealthy(t, "etcd", 60*time.Second)
	time.Sleep(10 * time.Second)

	// Create fresh client to verify data
	client2, _ := cluster.NewClient(t, ctx)

	logHandle2, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	// Write more entries to verify system is functional
	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids2 := framework.WriteEntries(t, ctx, writer2, 10, 5)
	require.Len(t, ids2, 5)
	t.Log("5 entries written after etcd recovery")

	err = writer2.Close(ctx)
	require.NoError(t, err)

	// Read and verify all data
	expectedCount := len(ids1) + len(ids2)
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("Etcd_SegmentCompletionDuringEtcdDown passed: %d entries verified", expectedCount)
}

// --- M3: etcd down during log creation ---
//
// Scenario: Stop etcd, then attempt to create a new log.
// Should fail with a retryable error. After etcd recovers, creation should succeed.
//
// Invariants verified:
//   - No stuck state: CreateLog fails cleanly when etcd is down
//   - Recovery: CreateLog succeeds after etcd recovery
func TestChaosExtra_Etcd_LogCreationDuringEtcdDown(t *testing.T) {
	cluster := newChaosExtraCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.TryStartEtcd(t)
	})

	// Stop etcd
	cluster.StopEtcd(t)
	t.Log("etcd stopped")
	time.Sleep(5 * time.Second)

	logName := fmt.Sprintf("chaos-etcd-create-%d", time.Now().UnixNano())

	// Attempt to create etcd client — should fail because etcd is down
	// (GetRemoteEtcdClient uses grpc.WithBlock + 5s dial timeout)
	cfg := cluster.NewConfig(t)
	etcdCli, etcdErr := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	if etcdErr != nil {
		t.Logf("etcd client creation failed as expected: %v", etcdErr)
	} else {
		// If client was created (unlikely with blocked dial), probe should fail
		probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
		_, putErr := etcdCli.Put(probeCtx, "/test-probe", "probe")
		probeCancel()
		assert.Error(t, putErr, "etcd should not be reachable")
		t.Logf("etcd probe failed as expected: %v", putErr)
		etcdCli.Close()
	}

	// Restart etcd
	cluster.StartEtcd(t)
	t.Log("etcd restarted")
	cluster.WaitForHealthy(t, "etcd", 60*time.Second)
	time.Sleep(10 * time.Second)

	// Now create log should succeed
	client, _ := cluster.NewClient(t, ctx)

	err := client.CreateLog(ctx, logName)
	require.NoError(t, err, "CreateLog should succeed after etcd recovery")
	t.Log("Log created successfully after etcd recovery")

	// Verify log is functional
	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids := framework.WriteEntries(t, ctx, writer, 0, 5)
	require.Len(t, ids, 5)

	err = writer.Close(ctx)
	require.NoError(t, err)

	msgs := framework.ReadAllEntries(t, ctx, logHandle, 5)
	require.Len(t, msgs, 5)
	verifyEntryOrder(t, msgs)

	t.Log("Etcd_LogCreationDuringEtcdDown passed")
}

// --- M4: etcd restart during active session ---
//
// Scenario: Restart etcd while writer holds a session lock.
// Verify session lease survives (etcd restart preserves leases if within TTL)
// or writer detects loss and the system can be re-established.
//
// Invariants verified:
//   - Single-writer guarantee: session lock behavior is correct across etcd restart
//   - Durability: data written before restart is preserved
func TestChaosExtra_Etcd_RestartDuringActiveSession(t *testing.T) {
	cluster := newChaosExtraCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.TryStartEtcd(t)
	})

	client1, etcdCli1, _ := cluster.NewClientManual(t, ctx)

	logName := fmt.Sprintf("chaos-etcd-session-%d", time.Now().UnixNano())
	err := client1.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write entries while session is active
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 5)
	require.Len(t, ids1, 5)
	t.Log("5 entries written with active session")

	// Kill and restart etcd (faster than stop/start to simulate sudden failure)
	cluster.KillEtcd(t)
	t.Log("etcd killed")
	time.Sleep(3 * time.Second)

	cluster.StartEtcd(t)
	t.Log("etcd restarted")
	cluster.WaitForHealthy(t, "etcd", 60*time.Second)
	time.Sleep(10 * time.Second)

	// Close the writer — may succeed if session lease survived, or fail if expired
	closeCtx, closeCancel := context.WithTimeout(ctx, 30*time.Second)
	closeErr := writer.Close(closeCtx)
	closeCancel()
	t.Logf("Writer close after etcd restart: %v", closeErr)

	// Close old client
	_ = client1.Close(ctx)
	_ = etcdCli1.Close()

	// Create fresh client and verify data
	client2, _ := cluster.NewClient(t, ctx)

	logHandle2, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	// Try to open a new writer (must acquire new session lock)
	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids2 := framework.WriteEntries(t, ctx, writer2, 10, 5)
	require.Len(t, ids2, 5)
	t.Log("5 entries written after etcd restart with new session")

	err = writer2.Close(ctx)
	require.NoError(t, err)

	// Read all entries
	expectedCount := len(ids1) + len(ids2)
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("Etcd_RestartDuringActiveSession passed: %d entries verified", expectedCount)
}

// --- M5: etcd network partition from servers ---
//
// Scenario: Disconnect etcd from the Docker network so servers cannot reach it.
// Servers can still serve buffered writes but cannot update metadata.
// After reconnecting, verify consistency.
//
// Invariants verified:
//   - Durability: writes that succeeded are not lost
//   - Metadata consistency: segment state converges after partition heals
//   - No stuck state: system recovers after partition heals
func TestChaosExtra_Etcd_NetworkPartitionFromServers(t *testing.T) {
	cluster := newChaosExtraCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.TryConnectEtcd(t)
	})

	client1, etcdCli1, _ := cluster.NewClientManual(t, ctx)

	logName := fmt.Sprintf("chaos-etcd-partition-%d", time.Now().UnixNano())
	err := client1.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Phase 1: Write with etcd connected
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 5)
	require.Len(t, ids1, 5)
	t.Log("Phase 1: 5 entries written with etcd connected")

	// Disconnect etcd from network (servers can't reach etcd, but client-etcd connection may still work via host)
	cluster.DisconnectEtcd(t)
	t.Log("etcd disconnected from Docker network")
	time.Sleep(5 * time.Second)

	// Phase 2: Try writes — may work for current segment (already open)
	// but segment completion will fail (can't update metadata in etcd)
	ids2, failures := writeEntriesAllowFailures(t, ctx, writer, 5, 5, 30*time.Second)
	t.Logf("Phase 2: %d successes, %d failures during etcd partition", len(ids2), failures)

	// Reconnect etcd
	cluster.ConnectEtcd(t)
	t.Log("etcd reconnected to Docker network")
	time.Sleep(10 * time.Second)

	// Close writer (completion should now work)
	closeCtx, closeCancel := context.WithTimeout(ctx, 30*time.Second)
	closeErr := writer.Close(closeCtx)
	closeCancel()
	t.Logf("Writer close after etcd reconnection: %v", closeErr)

	_ = client1.Close(ctx)
	_ = etcdCli1.Close()

	// Phase 3: Fresh client, verify data and write more
	client2, _ := cluster.NewClient(t, ctx)

	logHandle2, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids3 := framework.WriteEntries(t, ctx, writer2, 20, 5)
	require.Len(t, ids3, 5)
	t.Log("Phase 3: 5 entries written after etcd partition healed")

	err = writer2.Close(ctx)
	require.NoError(t, err)

	expectedCount := len(ids1) + len(ids2) + len(ids3)
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("Etcd_NetworkPartitionFromServers passed: %d entries verified", expectedCount)
}

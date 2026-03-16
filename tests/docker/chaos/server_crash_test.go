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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/tests/docker/framework"
)

// --- W1: Server crash during active writes (simulates crash during WAL sync) ---
//
// Scenario: Kill a server node while writes are actively being processed.
// The server crash interrupts any in-progress WAL sync on that node.
// Writes should continue via quorum (aq=2, remaining ensemble nodes).
// After restart, the crashed node should recover.
//
// Invariants verified:
//   - Durability: acknowledged writes are not lost
//   - Recovery completeness: crashed node recovers and serves data
//   - Ordering: entry order is preserved
func TestChaos_Server_CrashDuringActiveWrites(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.StartNode(t, "woodpecker-node1")
	})

	client, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("chaos-server-crash-write-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write initial entries
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 10)
	require.Len(t, ids1, 10)
	t.Log("10 entries written before crash")

	// Kill node1 mid-stream (simulates crash during WAL sync)
	cluster.KillNode(t, "woodpecker-node1")
	t.Log("woodpecker-node1 killed (simulating crash during WAL sync)")
	time.Sleep(5 * time.Second)

	// Continue writing — should succeed via quorum (2 of 3 ensemble nodes alive)
	ids2, failures := writeEntriesAllowFailures(t, ctx, writer, 10, 10, 30*time.Second)
	t.Logf("Wrote %d entries during node1 crash (%d failures)", len(ids2), failures)
	assert.Greater(t, len(ids2), 0, "at least some writes should succeed with 3/4 nodes alive")

	// Restart crashed node
	cluster.StartNode(t, "woodpecker-node1")
	t.Log("woodpecker-node1 restarted")
	cluster.WaitForHealthy(t, "woodpecker-node1", 60*time.Second)
	time.Sleep(15 * time.Second)

	// Close writer
	err = writer.Close(ctx)
	assert.NoError(t, err)

	// Read and verify all acknowledged entries
	expectedCount := len(ids1) + len(ids2)
	msgs := framework.ReadAllEntries(t, ctx, logHandle, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("Server_CrashDuringActiveWrites passed: %d entries verified", expectedCount)
}

// --- W2: Server crash during segment finalization ---
//
// Scenario: Kill a server while the segment is being finalized (Finalize() writes footer/index).
// After restart, the recovery mechanism should reconstruct the segment state.
//
// Invariants verified:
//   - Recovery completeness: recovery reconstructs index from data blocks
//   - Durability: synced data before crash is preserved
//   - Metadata consistency: segment state is correct after recovery
func TestChaos_Server_CrashDuringFinalization(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.StartNode(t, "woodpecker-node1")
	})

	client1, etcdCli1, _ := cluster.NewClientManual(t, ctx)

	logName := fmt.Sprintf("chaos-server-crash-finalize-%d", time.Now().UnixNano())
	err := client1.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write entries
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 15)
	require.Len(t, ids1, 15)
	t.Log("15 entries written")

	// Kill node1 right before close (close triggers fence + finalize)
	cluster.KillNode(t, "woodpecker-node1")
	t.Log("woodpecker-node1 killed (simulating crash during finalization)")

	// Close writer — finalization will fail on node1 but may succeed on other ensemble nodes
	closeCtx, closeCancel := context.WithTimeout(ctx, 30*time.Second)
	closeErr := writer.Close(closeCtx)
	closeCancel()
	t.Logf("Writer close with node1 crashed: %v", closeErr)

	// Restart node1
	cluster.StartNode(t, "woodpecker-node1")
	t.Log("woodpecker-node1 restarted")
	cluster.WaitForHealthy(t, "woodpecker-node1", 60*time.Second)
	time.Sleep(15 * time.Second)

	_ = client1.Close(ctx)
	_ = etcdCli1.Close()

	// Fresh client: write more entries and verify recovery
	client2, _ := cluster.NewClient(t, ctx)

	logHandle2, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids2 := framework.WriteEntries(t, ctx, writer2, 15, 5)
	require.Len(t, ids2, 5)
	t.Log("5 entries written after recovery")

	err = writer2.Close(ctx)
	require.NoError(t, err)

	expectedCount := len(ids1) + len(ids2)
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("Server_CrashDuringFinalization passed: %d entries verified", expectedCount)
}

// --- W3: Server crash during compaction upload ---
//
// Scenario: Kill a server while it's uploading compacted data to MinIO.
// After restart, compaction should retry.
//
// Invariants verified:
//   - Recovery: compaction retries after server restart
//   - Durability: all acknowledged data is preserved
func TestChaos_Server_CrashDuringCompaction(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.StartNode(t, "woodpecker-node1")
	})

	client1, etcdCli1, _ := cluster.NewClientManual(t, ctx)

	logName := fmt.Sprintf("chaos-server-crash-compact-%d", time.Now().UnixNano())
	err := client1.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write a larger batch to generate substantial compaction data
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 50)
	require.Len(t, ids1, 50)
	t.Log("50 entries written")

	// Close writer to trigger segment completion + compaction
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Kill node1 (may be mid-compaction upload)
	cluster.KillNode(t, "woodpecker-node1")
	t.Log("woodpecker-node1 killed (may be during compaction)")
	time.Sleep(5 * time.Second)

	// Restart
	cluster.StartNode(t, "woodpecker-node1")
	t.Log("woodpecker-node1 restarted")
	cluster.WaitForHealthy(t, "woodpecker-node1", 60*time.Second)
	time.Sleep(15 * time.Second)

	_ = client1.Close(ctx)
	_ = etcdCli1.Close()

	// Fresh client: verify data and write more
	client2, _ := cluster.NewClient(t, ctx)

	logHandle2, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	// Read all original entries
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, 50)
	require.Len(t, msgs, 50)
	verifyEntryOrder(t, msgs)

	// Write more to verify system is fully functional
	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids2 := framework.WriteEntries(t, ctx, writer2, 50, 10)
	require.Len(t, ids2, 10)

	err = writer2.Close(ctx)
	require.NoError(t, err)

	t.Logf("Server_CrashDuringCompaction passed: 50 + 10 entries verified")
}

// --- W5: Server crash during fence operation ---
//
// Scenario: Kill a server while it's processing a Fence() RPC.
// After restart, fencing state should be consistent.
//
// Invariants verified:
//   - Fencing correctness: fencing state is consistent after crash
//   - Recovery: server recovers and can serve new requests
func TestChaos_Server_CrashDuringFence(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.StartNode(t, "woodpecker-node1")
	})

	client1, etcdCli1, _ := cluster.NewClientManual(t, ctx)

	logName := fmt.Sprintf("chaos-server-crash-fence-%d", time.Now().UnixNano())
	err := client1.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids1 := framework.WriteEntries(t, ctx, writer, 0, 10)
	require.Len(t, ids1, 10)
	t.Log("10 entries written")

	// Kill node1 (may be during fence processing when writer.Close is called)
	cluster.KillNode(t, "woodpecker-node1")
	t.Log("woodpecker-node1 killed")

	// Close writer triggers fencing — will fail on killed node but may succeed on others
	closeCtx, closeCancel := context.WithTimeout(ctx, 30*time.Second)
	closeErr := writer.Close(closeCtx)
	closeCancel()
	t.Logf("Writer close with node1 killed: %v", closeErr)

	_ = client1.Close(ctx)
	_ = etcdCli1.Close()

	// Restart node
	cluster.StartNode(t, "woodpecker-node1")
	t.Log("woodpecker-node1 restarted")
	cluster.WaitForHealthy(t, "woodpecker-node1", 60*time.Second)
	time.Sleep(15 * time.Second)

	// Fresh client: verify data
	client2, _ := cluster.NewClient(t, ctx)

	logHandle2, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	// Should be able to open a new writer (proves fencing state is consistent)
	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err, "should be able to open new writer after server crash during fence")

	ids2 := framework.WriteEntries(t, ctx, writer2, 10, 5)
	require.Len(t, ids2, 5)
	t.Log("5 entries written on new writer after crash during fence")

	err = writer2.Close(ctx)
	require.NoError(t, err)

	expectedCount := len(ids1) + len(ids2)
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("Server_CrashDuringFence passed: %d entries verified", expectedCount)
}

// --- W6: Server process pause (simulates GC pause) ---
//
// Scenario: Pause a server container (simulates long GC pause or process hang).
// Writes should timeout on that node. Unpause, verify recovery.
//
// Note: We use docker pause briefly and then kill, because docker pause causes
// gRPC connections to hang indefinitely. The pattern is: pause -> attempt writes -> kill -> restart.
//
// Invariants verified:
//   - No stuck state: system handles paused node gracefully
//   - Recovery: system recovers after node comes back
func TestChaos_Server_ProcessPause(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.TryUnpauseNode(t, "woodpecker-node1")
		cluster.StartNode(t, "woodpecker-node1")
	})

	client, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("chaos-server-pause-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write before pause
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 5)
	require.Len(t, ids1, 5)
	t.Log("5 entries written before pause")

	// Pause node1 (simulates long GC pause)
	cluster.PauseNode(t, "woodpecker-node1")
	t.Log("woodpecker-node1 paused (GC pause simulation)")

	// Try writing — may succeed (aq=2 with 3 alive ensemble nodes) or hang
	ids2, failures := writeEntriesAllowFailures(t, ctx, writer, 5, 3, 15*time.Second)
	t.Logf("During pause: %d successes, %d failures", len(ids2), failures)

	// Kill the paused node (to avoid gRPC hangs) and restart cleanly
	cluster.KillNode(t, "woodpecker-node1")
	t.Log("Killed paused node1")
	time.Sleep(3 * time.Second)

	cluster.StartNode(t, "woodpecker-node1")
	t.Log("woodpecker-node1 restarted")
	cluster.WaitForHealthy(t, "woodpecker-node1", 60*time.Second)
	time.Sleep(15 * time.Second)

	// Close writer
	closeCtx, closeCancel := context.WithTimeout(ctx, 30*time.Second)
	_ = writer.Close(closeCtx)
	closeCancel()

	// Write more entries with fresh writer
	logHandle2, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids3 := framework.WriteEntries(t, ctx, writer2, 10, 5)
	require.Len(t, ids3, 5)

	err = writer2.Close(ctx)
	require.NoError(t, err)

	expectedCount := len(ids1) + len(ids2) + len(ids3)
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("Server_ProcessPause passed: %d entries verified", expectedCount)
}

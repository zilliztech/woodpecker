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

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/tests/docker/framework"
)

// --- S1: MinIO down during segment compaction ---
//
// Scenario: Stop MinIO while segment compaction (upload merged block) is in progress.
// Compaction should fail, but data written before compaction should be safe.
// After MinIO recovers, compaction should succeed on retry.
//
// Invariants verified:
//   - Durability: data written before compaction is not lost
//   - Recovery: compaction retries successfully after MinIO returns
func TestChaosExtra_MinIO_CompactionDuringMinIODown(t *testing.T) {
	cluster := newChaosExtraCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.TryStartMinIO(t)
	})

	client, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("chaos-minio-compact-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write a batch of entries to create segment data for compaction
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 20)
	require.Len(t, ids1, 20)
	t.Log("20 entries written")

	// Close writer to trigger segment completion (which includes compaction upload to MinIO)
	err = writer.Close(ctx)
	require.NoError(t, err)
	t.Log("First segment completed")

	// Write more to create a second segment
	logHandle2, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids2 := framework.WriteEntries(t, ctx, writer2, 20, 10)
	require.Len(t, ids2, 10)

	// Stop MinIO before closing (close triggers compaction upload)
	cluster.StopMinIO(t)
	t.Log("MinIO stopped before segment completion/compaction")
	time.Sleep(5 * time.Second)

	// Close writer — compaction upload will fail because MinIO is down
	closeCtx, closeCancel := context.WithTimeout(ctx, 30*time.Second)
	closeErr := writer2.Close(closeCtx)
	closeCancel()
	t.Logf("Writer close with MinIO down: %v", closeErr)

	// Restart MinIO
	cluster.StartMinIO(t)
	t.Log("MinIO restarted")
	cluster.WaitForHealthy(t, "minio", 60*time.Second)
	time.Sleep(15 * time.Second)

	// Verify: write more entries to confirm system is functional
	logHandle3, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer3, err := logHandle3.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids3 := framework.WriteEntries(t, ctx, writer3, 30, 5)
	require.Len(t, ids3, 5)
	t.Log("5 entries written after MinIO recovery")

	err = writer3.Close(ctx)
	require.NoError(t, err)

	// Read all data that was successfully acknowledged
	expectedCount := len(ids1) + len(ids2) + len(ids3)
	msgs := framework.ReadAllEntries(t, ctx, logHandle3, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("MinIO_CompactionDuringMinIODown passed: %d entries verified", expectedCount)
}

// --- S2: MinIO restart during active writes ---
//
// Scenario: Restart MinIO while writes are happening.
// Staged storage writes to local disk should continue.
// Compaction upload should retry after MinIO recovers.
//
// Invariants verified:
//   - Durability: staged storage local writes are not affected by MinIO restart
//   - Recovery: compaction resumes after MinIO is available
func TestChaosExtra_MinIO_RestartDuringWrites(t *testing.T) {
	cluster := newChaosExtraCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.TryStartMinIO(t)
	})

	client, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("chaos-minio-restart-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Phase 1: Write with MinIO healthy
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 10)
	require.Len(t, ids1, 10)
	t.Log("Phase 1: 10 entries written with MinIO healthy")

	// Kill MinIO (immediate crash)
	cluster.KillMinIO(t)
	t.Log("MinIO killed")
	time.Sleep(3 * time.Second)

	// Phase 2: Continue writing — staged storage local disk writes should work
	ids2, failures := writeEntriesAllowFailures(t, ctx, writer, 10, 5, 30*time.Second)
	t.Logf("Phase 2: %d successes, %d failures during MinIO outage", len(ids2), failures)

	// Restart MinIO
	cluster.StartMinIO(t)
	t.Log("MinIO restarted")
	cluster.WaitForHealthy(t, "minio", 60*time.Second)
	time.Sleep(10 * time.Second)

	// Close writer (compaction upload should work now)
	err = writer.Close(ctx)
	if err != nil {
		t.Logf("Writer close after MinIO restart: %v (may need retry)", err)
	}

	// Phase 3: Write more entries
	logHandle2, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids3 := framework.WriteEntries(t, ctx, writer2, 20, 5)
	require.Len(t, ids3, 5)
	t.Log("Phase 3: 5 entries written after MinIO recovery")

	err = writer2.Close(ctx)
	require.NoError(t, err)

	// Read and verify
	expectedCount := len(ids1) + len(ids2) + len(ids3)
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("MinIO_RestartDuringWrites passed: %d entries verified", expectedCount)
}

// --- S3: MinIO network partition ---
//
// Scenario: Disconnect MinIO from the Docker network.
// Servers can't reach MinIO for compaction upload, but local disk writes continue.
// After reconnecting, compaction should complete.
//
// Invariants verified:
//   - Durability: local staged writes are preserved during partition
//   - Recovery: compaction completes after partition heals
func TestChaosExtra_MinIO_NetworkPartition(t *testing.T) {
	cluster := newChaosExtraCluster(t)
	ctx := context.Background()

	t.Cleanup(func() {
		cluster.TryConnectMinIO(t)
	})

	client1, etcdCli1, _ := cluster.NewClientManual(t, ctx)

	logName := fmt.Sprintf("chaos-minio-partition-%d", time.Now().UnixNano())
	err := client1.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Phase 1: Write with MinIO connected
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 10)
	require.Len(t, ids1, 10)
	t.Log("Phase 1: 10 entries written with MinIO connected")

	// Disconnect MinIO from network
	cluster.DisconnectMinIO(t)
	t.Log("MinIO disconnected from network")
	time.Sleep(5 * time.Second)

	// Phase 2: Write during MinIO partition — local writes may succeed
	ids2, failures := writeEntriesAllowFailures(t, ctx, writer, 10, 5, 30*time.Second)
	t.Logf("Phase 2: %d successes, %d failures during MinIO partition", len(ids2), failures)

	// Reconnect MinIO
	cluster.ConnectMinIO(t)
	t.Log("MinIO reconnected to network")
	time.Sleep(10 * time.Second)

	// Close writer
	closeCtx, closeCancel := context.WithTimeout(ctx, 30*time.Second)
	closeErr := writer.Close(closeCtx)
	closeCancel()
	t.Logf("Writer close after MinIO reconnection: %v", closeErr)

	_ = client1.Close(ctx)
	_ = etcdCli1.Close()

	// Phase 3: Fresh client, write more and verify
	client2, _ := cluster.NewClient(t, ctx)

	logHandle2, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids3 := framework.WriteEntries(t, ctx, writer2, 20, 5)
	require.Len(t, ids3, 5)
	t.Log("Phase 3: 5 entries written after MinIO partition healed")

	err = writer2.Close(ctx)
	require.NoError(t, err)

	expectedCount := len(ids1) + len(ids2) + len(ids3)
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("MinIO_NetworkPartition passed: %d entries verified", expectedCount)
}

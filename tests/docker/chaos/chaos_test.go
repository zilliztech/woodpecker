package chaos

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/tests/docker/framework"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// writeEntriesAllowFailures writes n entries and returns successful IDs and failure count.
// Each entry has a timeout — timeouts count as failures.
//
// IMPORTANT: WriteAsync can block synchronously on SelectQuorum (e.g., when retrying
// to find enough nodes). We run each WriteAsync in a goroutine so the timeout works
// even when WriteAsync blocks before returning the result channel.
func writeEntriesAllowFailures(t *testing.T, ctx context.Context, writer log.LogWriter, offset, count int, timeout time.Duration) ([]*log.LogMessageId, int) {
	t.Helper()
	ids := make([]*log.LogMessageId, 0, count)
	failures := 0
	for i := 0; i < count; i++ {
		resultCh := make(chan *log.WriteResult, 1)
		go func(idx int) {
			ch := writer.WriteAsync(ctx, &log.WriteMessage{
				Payload: []byte(fmt.Sprintf("entry-%d", offset+idx)),
				Properties: map[string]string{
					"index": fmt.Sprintf("%d", offset+idx),
				},
			})
			if ch != nil {
				resultCh <- <-ch
			}
		}(i)

		select {
		case result := <-resultCh:
			if result.Err != nil {
				failures++
				t.Logf("write entry %d failed: %v", offset+i, result.Err)
			} else {
				ids = append(ids, result.LogMessageId)
			}
		case <-time.After(timeout):
			failures++
			t.Logf("write entry %d timed out after %v", offset+i, timeout)
		}
	}
	return ids, failures
}

// verifyEntryOrder checks that entries have monotonically increasing IDs.
func verifyEntryOrder(t *testing.T, msgs []*log.LogMessage) {
	t.Helper()
	for i := 1; i < len(msgs); i++ {
		prev := msgs[i-1].Id
		curr := msgs[i].Id
		if curr.SegmentId == prev.SegmentId {
			assert.Greater(t, curr.EntryId, prev.EntryId,
				"entry %d: entryId should increase within segment %d", i, curr.SegmentId)
		} else {
			assert.Greater(t, curr.SegmentId, prev.SegmentId,
				"entry %d: segmentId should increase across segments", i)
		}
	}
}

// newChaosCluster creates a ChaosCluster for use in chaos tests.
// It does NOT call Up/Down — that is managed by TestMain or run_chaos_tests.sh.
func newChaosCluster(t *testing.T) *ChaosCluster {
	t.Helper()
	return NewChaosCluster(t)
}

// --- Chaos Test Cases ---
//
// Cluster configuration: 4 Woodpecker nodes, ensemble_size(wq)=3, ack_quorum(aq)=2
// Each segment is replicated on an ensemble of 3 nodes.
// A write succeeds when at least aq=2 ensemble nodes acknowledge the write.
// New segment creation requires SelectQuorum to find ensemble_size=3 available nodes.

// TestChaos_BasicReadWrite verifies basic write and read functionality on the cluster.
// This is a smoke test to ensure the cluster is operational before running chaos scenarios.
//
// Expected behavior:
//   - 4 nodes alive, ensemble_size=3, ack_quorum=2
//   - Write 1000 entries → all succeed
//   - Read 1000 entries back → all match, monotonically increasing IDs
func TestChaos_BasicReadWrite(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	client, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("chaos-basic-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write 1000 entries
	ids := framework.WriteEntries(t, ctx, writer, 0, 1000)
	require.Len(t, ids, 1000)

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Read all entries back
	msgs := framework.ReadAllEntries(t, ctx, logHandle, 1000)
	require.Len(t, msgs, 1000)

	// Verify order
	verifyEntryOrder(t, msgs)

	// Verify data matches
	for i, msg := range msgs {
		assert.Equal(t, ids[i].SegmentId, msg.Id.SegmentId, "entry %d segment mismatch", i)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(msg.Payload), "entry %d payload mismatch", i)
	}

	t.Log("BasicReadWrite passed: 1000 entries written and verified")
}

// TestChaos_SingleNodeKill_WriteContinues verifies that writes continue after killing
// one Woodpecker node, without any client-side recovery actions.
//
// Expected behavior (4 nodes, es=3, aq=2):
//   - Kill 1 node → 3 remaining nodes
//   - Current segment's ensemble was selected from 4 nodes, may include the killed node
//   - Writes on the SAME writer continue successfully because aq=2:
//     even if the killed node is in the ensemble, 2 of 3 ensemble nodes are still alive,
//     and the write quorum (aq=2) is satisfied by those 2 acks
//   - If the segment rolls (fills up or error-triggered), new segment creation succeeds
//     because 3 alive nodes >= ensemble_size=3
//   - Reads work correctly since data exists on at least aq=2 nodes
func TestChaos_SingleNodeKill_WriteContinues(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	client, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("chaos-single-kill-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Cleanup: restart killed node at end
	t.Cleanup(func() {
		cluster.StartNode(t, "woodpecker-node1")
	})

	// Phase 1: Write 5 entries with all 4 nodes alive
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 5)
	require.Len(t, ids1, 5)
	t.Log("Phase 1: 5 entries written with all nodes alive")

	// Kill one Woodpecker node
	cluster.KillNode(t, "woodpecker-node1")
	t.Log("Killed woodpecker-node1")

	// Wait for gossip to detect the failure
	time.Sleep(15 * time.Second)

	// Phase 2: Continue writing on the SAME writer — should succeed because aq=2
	// Even if node1 was part of the current segment's ensemble, 2 of 3 ensemble nodes
	// are alive, satisfying the ack quorum requirement. No recovery actions needed.
	ids2 := framework.WriteEntries(t, ctx, writer, 5, 5)
	require.Len(t, ids2, 5)
	t.Log("Phase 2: 5 more entries written on SAME writer after node kill (aq=2 satisfied)")

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Read all 10 entries and verify correctness
	allIds := append(ids1, ids2...)
	msgs := framework.ReadAllEntries(t, ctx, logHandle, 10)
	require.Len(t, msgs, 10)
	verifyEntryOrder(t, msgs)

	for i, msg := range msgs {
		assert.Equal(t, allIds[i].SegmentId, msg.Id.SegmentId, "entry %d segment mismatch", i)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(msg.Payload), "entry %d payload mismatch", i)
	}

	t.Log("SingleNodeKill_WriteContinues passed: writes survived node failure without recovery actions")
}

// TestChaos_DoubleNodeKill_WriteBlocksAndRecovers verifies behavior when 2 out of 4 nodes
// are killed. After restarting nodes, writes recover and all data is readable.
//
// Expected behavior (4 nodes, es=3, aq=2):
//   - Phase 1: All 4 nodes alive → writes succeed normally
//   - Kill 2 nodes → only 2 remaining
//   - Phase 2: Writes may or may not succeed depending on gossip convergence timing:
//     * Before gossip detects failures: SelectQuorum may select an ensemble containing
//       1 dead + 2 alive nodes. Since aq=2, 2 alive acks satisfy the quorum → write succeeds.
//     * After gossip converges: SelectQuorum finds only 2 alive nodes, cannot form
//       ensemble of 3 → writes block indefinitely.
//     This phase uses a short timeout and does NOT assert failure, since the result
//     depends on gossip timing and ensemble selection.
//   - Phase 3: Restart at least 1 node (3 alive) → quorum can be formed again.
//     A fresh client creates new gRPC connections to discover the recovered node.
//     Writes and reads resume normally. All previously written data is readable.
func TestChaos_DoubleNodeKill_WriteBlocksAndRecovers(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	// Cleanup: ensure killed nodes are restarted at end
	t.Cleanup(func() {
		cluster.StartNode(t, "woodpecker-node1")
		cluster.StartNode(t, "woodpecker-node2")
	})

	// Use manual client so we control lifecycle explicitly (avoid double-close)
	client1, etcdCli1, _ := cluster.NewClientManual(t, ctx)

	logName := fmt.Sprintf("chaos-double-kill-%d", time.Now().UnixNano())
	err := client1.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Phase 1: Write 5 entries with all 4 nodes alive
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 5)
	require.Len(t, ids1, 5)
	t.Log("Phase 1: 5 entries written with all nodes alive")

	// Close writer to flush the current segment
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Kill 2 nodes → only 2 remain, cannot form ensemble of 3
	cluster.KillNode(t, "woodpecker-node1")
	cluster.KillNode(t, "woodpecker-node2")
	t.Log("Killed woodpecker-node1 and woodpecker-node2 (2 of 4 nodes down)")

	time.Sleep(10 * time.Second)

	// Phase 2: Try to write with only 2 nodes alive → should block/timeout
	// New segment creation requires SelectQuorum with ensemble_size=3,
	// but only 2 nodes available → SelectQuorum retries indefinitely.
	// Use cancellable context so stuck SelectQuorum goroutines can be stopped.
	phase2Ctx, phase2Cancel := context.WithCancel(ctx)

	logHandle2, err := client1.OpenLog(phase2Ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(phase2Ctx)
	require.NoError(t, err)

	// Try to write with a short timeout. Writes may succeed (if ensemble has 2 alive
	// nodes satisfying aq=2) or fail (if gossip converged and only 2 nodes discoverable).
	// We don't hard-assert failure because it depends on gossip timing.
	ids2, failures := writeEntriesAllowFailures(t, phase2Ctx, writer2, 5, 1, 15*time.Second)
	t.Logf("Phase 2: %d successes, %d failures with 2 nodes down (gossip-timing dependent)", len(ids2), failures)
	if failures > 0 {
		t.Log("Phase 2: writes failed as expected (gossip converged, only 2 nodes discoverable)")
	} else {
		t.Log("Phase 2: writes succeeded (ensemble had 2 alive nodes, aq=2 satisfied before gossip convergence)")
	}

	// Cancel phase2 context to stop any stuck SelectQuorum goroutines
	phase2Cancel()
	time.Sleep(2 * time.Second)

	// Close writer with a fresh timeout context (not the cancelled one)
	closeCtx, closeCancel := context.WithTimeout(ctx, 15*time.Second)
	_ = writer2.Close(closeCtx)
	closeCancel()

	// Close old client (stale gRPC connections to dead nodes)
	_ = client1.Close(ctx)
	_ = etcdCli1.Close()

	// Phase 3: Restart one node so 3 are alive → quorum can be formed again
	cluster.StartNode(t, "woodpecker-node1")
	t.Log("Restarted woodpecker-node1 (now 3 of 4 alive)")

	cluster.WaitForHealthy(t, "woodpecker-node1", 60*time.Second)
	time.Sleep(15 * time.Second) // gossip convergence

	// Create fresh client with new gRPC connections
	client2, _ := cluster.NewClient(t, ctx)

	logHandle3, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer3, err := logHandle3.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write 5 entries → should succeed now
	ids3 := framework.WriteEntries(t, ctx, writer3, 10, 5)
	require.Len(t, ids3, 5)
	t.Log("Phase 3: 5 entries written after restarting one node")

	err = writer3.Close(ctx)
	require.NoError(t, err)

	// Read all entries that succeeded (Phase 1 + any Phase 2 + Phase 3)
	expectedCount := len(ids1) + len(ids2) + len(ids3)
	msgs := framework.ReadAllEntries(t, ctx, logHandle3, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("DoubleNodeKill_WriteBlocksAndRecovers passed: %d entries verified", expectedCount)
}

// TestChaos_NodeRestart_RecoveryMode verifies that when a node is killed and restarted,
// the cluster can recover and continue accepting writes in a new segment.
//
// Expected behavior (4 nodes, es=3, aq=2):
//   - Phase 1: Write entries normally with all nodes alive
//   - Kill 1 node → writes still work (aq=2 satisfied by 2 of 3 ensemble nodes)
//   - Restart node → node rejoins cluster via gossip discovery
//   - Close writer and open new one → triggers segment rolling to a new segment
//   - New segment creation succeeds (4 alive nodes >= ensemble_size=3)
//   - Phase 2: Write more entries in new segment → success
//   - Read all data from both segments → all data is preserved and correctly ordered
func TestChaos_NodeRestart_RecoveryMode(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	client, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("chaos-restart-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Phase 1: Write 5 entries
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 5)
	require.Len(t, ids1, 5)
	t.Log("Phase 1: 5 entries written")

	// Kill node and restart it
	cluster.KillNode(t, "woodpecker-node1")
	t.Log("Killed woodpecker-node1")
	time.Sleep(5 * time.Second)

	cluster.StartNode(t, "woodpecker-node1")
	t.Log("Restarted woodpecker-node1")

	// Wait for the node to rejoin the cluster
	cluster.WaitForHealthy(t, "woodpecker-node1", 60*time.Second)
	time.Sleep(10 * time.Second) // Allow gossip convergence

	// Phase 2: Close old writer, create new one (triggers segment rolling for recovery)
	_ = writer.Close(ctx)

	logHandle2, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write 5 more entries in new segment
	ids2 := framework.WriteEntries(t, ctx, writer2, 5, 5)
	require.Len(t, ids2, 5)
	t.Log("Phase 2: 5 more entries written after node restart")

	err = writer2.Close(ctx)
	require.NoError(t, err)

	// Read all entries and verify
	allIds := append(ids1, ids2...)
	msgs := framework.ReadAllEntries(t, ctx, logHandle2, 10)
	require.Len(t, msgs, 10)
	verifyEntryOrder(t, msgs)

	for i, msg := range msgs {
		assert.Equal(t, allIds[i].SegmentId, msg.Id.SegmentId, "entry %d segment mismatch", i)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(msg.Payload), "entry %d payload mismatch", i)
	}

	t.Log("NodeRestart_RecoveryMode passed")
}

// TestChaos_RollingRestart verifies that the cluster survives a rolling restart of all nodes.
// Nodes are killed and restarted one at a time, with writes happening between each restart.
//
// Expected behavior (4 nodes, es=3, aq=2):
//   - At most 1 node is down at any point → 3 alive >= ensemble_size=3
//   - Each kill: writes may be slightly disrupted on current segment if the killed node
//     is in the ensemble, but aq=2 ensures writes succeed (2 of 3 alive)
//   - Each restart: node rejoins, cluster has full 4 nodes again
//   - After each restart, new writer creates a new segment (segment rolling)
//   - All data from all rounds is readable with correct ordering
func TestChaos_RollingRestart(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	client, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("chaos-rolling-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	entriesPerRound := 5
	totalWritten := 0
	allIds := make([]*log.LogMessageId, 0)

	// Initial write round
	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids := framework.WriteEntries(t, ctx, writer, totalWritten, entriesPerRound)
	allIds = append(allIds, ids...)
	totalWritten += entriesPerRound
	t.Logf("Initial round: wrote %d entries", entriesPerRound)

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Rolling restart: kill and restart each node, writing entries in between
	for _, node := range cluster.Nodes {
		cluster.KillNode(t, node.ContainerName)
		t.Logf("Killed %s", node.ContainerName)

		time.Sleep(10 * time.Second)

		cluster.StartNode(t, node.ContainerName)
		t.Logf("Restarted %s", node.ContainerName)

		// Wait for node to rejoin
		cluster.WaitForHealthy(t, node.ContainerName, 60*time.Second)
		time.Sleep(10 * time.Second) // gossip convergence

		// Open new writer and write more entries
		logHandle2, err := client.OpenLog(ctx, logName)
		require.NoError(t, err)

		writer2, err := logHandle2.OpenLogWriter(ctx)
		require.NoError(t, err)

		ids := framework.WriteEntries(t, ctx, writer2, totalWritten, entriesPerRound)
		allIds = append(allIds, ids...)
		totalWritten += entriesPerRound

		err = writer2.Close(ctx)
		require.NoError(t, err)

		t.Logf("Wrote %d entries after restarting %s (total: %d)", entriesPerRound, node.ContainerName, totalWritten)
	}

	// Read all entries and verify
	logHandleFinal, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	msgs := framework.ReadAllEntries(t, ctx, logHandleFinal, totalWritten)
	require.Len(t, msgs, totalWritten)
	verifyEntryOrder(t, msgs)

	for i, msg := range msgs {
		assert.Equal(t, allIds[i].SegmentId, msg.Id.SegmentId, "entry %d segment mismatch", i)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(msg.Payload), "entry %d payload mismatch", i)
	}

	t.Logf("RollingRestart passed: %d entries verified across %d rolling restarts", totalWritten, len(cluster.Nodes))
}

// TestChaos_FullClusterRestart_DataDurability verifies that all data is durable across
// a full cluster restart. All Woodpecker nodes are stopped and restarted, and previously
// written data must be fully readable.
//
// Expected behavior (4 nodes, es=3, aq=2):
//   - Write 10 entries → data replicated to aq=2 ensemble nodes and flushed to MinIO
//   - Close writer (flush all buffers)
//   - Stop all Woodpecker nodes → data exists in MinIO (object storage)
//   - Restart all nodes → nodes recover state from etcd metadata + MinIO storage
//   - Open new client and reader → all 10 entries readable with correct payloads
//   - Data durability is guaranteed by the WAL's object storage backend
func TestChaos_FullClusterRestart_DataDurability(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	// Use manual client creation so we can close it before stopping the cluster
	// without triggering a double-close from t.Cleanup.
	client, etcdCli, _ := cluster.NewClientManual(t, ctx)

	logName := fmt.Sprintf("chaos-durability-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write 10 entries
	ids := framework.WriteEntries(t, ctx, writer, 0, 10)
	require.Len(t, ids, 10)
	t.Log("Wrote 10 entries")

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Close the client before stopping the cluster
	_ = client.Close(ctx)
	_ = etcdCli.Close()

	// Stop all Woodpecker nodes
	cluster.StopAllWoodpeckerNodes(t)
	t.Log("All Woodpecker nodes stopped")

	time.Sleep(5 * time.Second)

	// Restart all nodes
	cluster.StartAllWoodpeckerNodes(t)
	t.Log("All Woodpecker nodes restarted")

	// Wait for cluster to recover
	cluster.WaitForClusterReady(t, 120*time.Second)

	// Create a new client (with auto-cleanup) and read the data
	client2, _ := cluster.NewClient(t, ctx)

	logHandle2, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	msgs := framework.ReadAllEntries(t, ctx, logHandle2, 10)
	require.Len(t, msgs, 10)
	verifyEntryOrder(t, msgs)

	for i, msg := range msgs {
		assert.Equal(t, ids[i].SegmentId, msg.Id.SegmentId, "entry %d segment mismatch", i)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(msg.Payload), "entry %d payload mismatch", i)
	}

	t.Log("FullClusterRestart_DataDurability passed: all 10 entries survived full restart")
}

// TestChaos_NetworkPartition_SingleNode verifies that writes continue on the same
// writer when one ensemble node is killed, and that the system recovers after the
// node is restarted.
//
// We use docker kill (not docker pause or docker network disconnect) because:
//   - docker pause: freezes processes but Docker port mapping keeps TCP alive, causing
//     gRPC calls to hang indefinitely (no keepalive configured). Segment fencing during
//     writer.Close() or OpenLogWriter() blocks forever.
//   - docker network disconnect: removes container from Docker network but host→container
//     port mapping still routes traffic. Node is reachable from host but can't reach
//     MinIO/etcd, causing RPCs to hang on the node side.
//   - docker kill: process crashes immediately, port becomes unreachable, gRPC calls
//     fail fast with connection errors. This cleanly simulates a node crash.
//
// Expected behavior (4 nodes, es=3, aq=2):
//   - Phase 1: Write 5 entries normally with all nodes alive, close writer
//   - Kill 1 node → 3 responsive nodes
//   - Phase 2: Open new writer (previous segment already completed, no fencing needed),
//     write 5 entries on new segment whose ensemble is selected from 3 alive nodes
//   - Restart killed node → node recovers and rejoins cluster
//   - Phase 3: Close writer2 (all nodes alive, fencing succeeds), write 5 more entries,
//     read all data → everything correct
func TestChaos_NetworkPartition_SingleNode(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	client1, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("chaos-partition1-%d", time.Now().UnixNano())
	err := client1.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Phase 1: Write 5 entries and close writer (segment completed while all nodes alive)
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 5)
	require.Len(t, ids1, 5)
	t.Log("Phase 1: 5 entries written with all nodes alive")

	err = writer.Close(ctx)
	require.NoError(t, err)
	t.Log("Phase 1: writer closed, segment completed")

	// Kill node1 (simulates crash — port becomes unreachable, gRPC fails fast)
	cluster.KillNode(t, "woodpecker-node1")
	t.Log("Partition: killed woodpecker-node1")
	time.Sleep(10 * time.Second) // allow gossip to detect the dead node

	// Phase 2: Open new writer and write entries with 3 alive nodes
	// The previous segment is already completed, so no fencing is needed.
	// SelectQuorum picks ensemble from alive nodes.
	logHandle2, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids2 := framework.WriteEntries(t, ctx, writer2, 5, 5)
	require.Len(t, ids2, 5)
	t.Log("Phase 2: 5 entries written with node1 down")

	// Restart the killed node
	cluster.StartNode(t, "woodpecker-node1")
	t.Log("Partition healed: restarted woodpecker-node1")
	cluster.WaitForHealthy(t, "woodpecker-node1", 60*time.Second)
	time.Sleep(15 * time.Second) // gossip convergence

	// Close writer2 now that all nodes are alive (fencing succeeds)
	err = writer2.Close(ctx)
	require.NoError(t, err)
	t.Log("Phase 2: writer2 closed after node recovery")

	// Phase 3: Fresh client, write more entries, read all
	_ = client1.Close(ctx)
	client2, _ := cluster.NewClient(t, ctx)

	logHandle3, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer3, err := logHandle3.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids3 := framework.WriteEntries(t, ctx, writer3, 10, 5)
	require.Len(t, ids3, 5)
	t.Log("Phase 3: 5 entries written after full recovery")

	err = writer3.Close(ctx)
	require.NoError(t, err)

	// Read all 15 entries and verify correctness
	allIds := append(append(ids1, ids2...), ids3...)
	msgs := framework.ReadAllEntries(t, ctx, logHandle3, 15)
	require.Len(t, msgs, 15)
	verifyEntryOrder(t, msgs)

	for i, msg := range msgs {
		assert.Equal(t, allIds[i].SegmentId, msg.Id.SegmentId, "entry %d segment mismatch", i)
		assert.Equal(t, fmt.Sprintf("entry-%d", i), string(msg.Payload), "entry %d payload mismatch", i)
	}

	t.Log("NetworkPartition_SingleNode passed: writes and reads survived node crash and recovery")
}

// TestChaos_NetworkPartition_TwoNodes verifies behavior when 2 nodes become
// unavailable (graceful stop). With only 2 of 4 nodes alive, new segments
// cannot form an ensemble of 3, so writes eventually block.
//
// Note on approach: We use docker stop (graceful SIGTERM) instead of docker pause
// because docker pause freezes the container process while Docker port mapping
// remains active, causing gRPC calls to hang indefinitely (no keepalive timeout).
// With 2/3 seeds paused, SelectQuorum almost always picks a paused seed and hangs.
// docker stop cleanly shuts down the node, making the port unreachable so gRPC
// fails fast — which is the behavior we need for reliable testing.
//
// Expected behavior (4 nodes, es=3, aq=2):
//   - Phase 1: Write entries normally with all nodes alive
//   - Stop 2 nodes → only 2 responsive, ports unreachable
//   - Phase 2: New segment creation may block (need ensemble_size=3 but only 2 responsive).
//     Writes may succeed if the ensemble still includes 2 alive nodes (aq=2),
//     or fail/timeout if gossip has converged and only 2 nodes are discoverable.
//   - Phase 3: Start both nodes → all 4 responsive again
//     New segments can be created, writes and reads resume.
//   - Read verification: the restarted nodes enter recovery mode for unfinalized segments.
//     The reader falls back to other quorum nodes that have the data.
func TestChaos_NetworkPartition_TwoNodes(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	// Cleanup: ensure nodes are started at end
	t.Cleanup(func() {
		_, _, _ = framework.RunCommandDirect("docker", "start", "woodpecker-node1")
		_, _, _ = framework.RunCommandDirect("docker", "start", "woodpecker-node2")
	})

	// Use manual client for Phase 1+2 to control lifecycle
	client1, etcdCli1, _ := cluster.NewClientManual(t, ctx)

	logName := fmt.Sprintf("chaos-partition2-%d", time.Now().UnixNano())
	err := client1.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Phase 1: Write 5 entries with all 4 nodes alive
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 5)
	require.Len(t, ids1, 5)
	t.Log("Phase 1: 5 entries written with all nodes alive")

	// Close writer to flush the segment
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Stop 2 nodes → only 2 responsive, ports become unreachable
	cluster.StopNode(t, "woodpecker-node1")
	cluster.StopNode(t, "woodpecker-node2")
	t.Log("Partition: stopped woodpecker-node1 and woodpecker-node2")

	time.Sleep(10 * time.Second)

	// Phase 2: Try to write → may block because new segment can't find 3 responsive nodes.
	// Use a cancellable context so that the SelectQuorum retry goroutine can be stopped
	// when Phase 2 is done — otherwise it retries forever and blocks Close().
	phase2Ctx, phase2Cancel := context.WithCancel(ctx)

	logHandle2, err := client1.OpenLog(phase2Ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(phase2Ctx)
	require.NoError(t, err)

	// Writes may succeed or fail depending on gossip timing and ensemble selection.
	ids2, failures := writeEntriesAllowFailures(t, phase2Ctx, writer2, 5, 1, 15*time.Second)
	t.Logf("Phase 2: %d successes, %d failures with 2 nodes stopped (gossip-timing dependent)", len(ids2), failures)
	if failures > 0 {
		t.Log("Phase 2: writes failed as expected")
	} else {
		t.Log("Phase 2: writes succeeded (ensemble had 2 alive nodes, aq=2 satisfied)")
	}

	// Cancel Phase 2 context to stop the stuck SelectQuorum goroutine
	phase2Cancel()
	time.Sleep(2 * time.Second) // Allow goroutines to clean up

	// Close writer and client with timeout
	closeCtx, closeCancel := context.WithTimeout(ctx, 15*time.Second)
	_ = writer2.Close(closeCtx)
	closeCancel()

	// Close old client (stale connections) with timeout
	closeCtx2, closeCancel2 := context.WithTimeout(ctx, 15*time.Second)
	_ = client1.Close(closeCtx2)
	closeCancel2()
	_ = etcdCli1.Close()

	// Phase 3: Start both nodes → all 4 responsive again
	cluster.StartNode(t, "woodpecker-node1")
	cluster.StartNode(t, "woodpecker-node2")
	t.Log("Partition healed: started both nodes")

	// Wait for nodes to be healthy and gossip to converge
	cluster.WaitForHealthy(t, "woodpecker-node1", 60*time.Second)
	cluster.WaitForHealthy(t, "woodpecker-node2", 60*time.Second)
	time.Sleep(15 * time.Second) // gossip convergence

	// Create fresh client for recovery
	client2, _ := cluster.NewClient(t, ctx)

	logHandle3, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer3, err := logHandle3.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write 5 entries → should succeed with all nodes back
	ids3 := framework.WriteEntries(t, ctx, writer3, 10, 5)
	require.Len(t, ids3, 5)
	t.Log("Phase 3: 5 entries written after partition healed")

	err = writer3.Close(ctx)
	require.NoError(t, err)

	// Read all entries that succeeded — tests reader fallback behavior.
	expectedCount := len(ids1) + len(ids2) + len(ids3)
	msgs := framework.ReadAllEntries(t, ctx, logHandle3, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("NetworkPartition_TwoNodes passed: %d entries verified after partition recovery", expectedCount)
}

// TestChaos_MinIOFailure_WriteBlocksAndRecovers verifies behavior when MinIO becomes
// unavailable: writes may block because data cannot be flushed to object storage.
// After MinIO returns, writes and reads should recover.
//
// Expected behavior (4 nodes, es=3, aq=2):
//   - Phase 1: Write entries with MinIO healthy → data replicated and flushed to MinIO
//   - Stop MinIO → data cannot be persisted to object storage
//   - Phase 2: Writes may initially succeed (buffered locally on nodes), but eventually
//     block when local buffers fill up and sync to MinIO fails. With a short timeout,
//     we expect writes to fail or timeout.
//   - Phase 3: Restart MinIO → object storage available again
//     Writes resume normally. Previously written data is fully readable.
func TestChaos_MinIOFailure_WriteBlocksAndRecovers(t *testing.T) {
	cluster := newChaosCluster(t)
	ctx := context.Background()

	client, _ := cluster.NewClient(t, ctx)

	logName := fmt.Sprintf("chaos-minio-%d", time.Now().UnixNano())
	err := client.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Phase 1: Write 5 entries with MinIO healthy
	ids1 := framework.WriteEntries(t, ctx, writer, 0, 5)
	require.Len(t, ids1, 5)
	t.Log("Phase 1: 5 entries written with MinIO healthy")

	// Ensure data is synced before stopping MinIO
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Stop MinIO
	cluster.StopNode(t, "minio")
	t.Log("MinIO stopped")

	time.Sleep(5 * time.Second)

	// Phase 2: Writes may fail because MinIO is down and buffers will eventually fill
	logHandle2, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write entries with a timeout — some or all may fail/timeout since MinIO is down.
	// Use cancellable context so stuck goroutines can be stopped.
	phase2Ctx, phase2Cancel := context.WithCancel(ctx)
	ids2, failures := writeEntriesAllowFailures(t, phase2Ctx, writer2, 5, 3, 30*time.Second)
	t.Logf("Phase 2: wrote %d entries, %d failures with MinIO down", len(ids2), failures)

	// Cancel to stop any stuck goroutines, then close with timeout
	phase2Cancel()
	time.Sleep(2 * time.Second)
	closeCtx, closeCancel := context.WithTimeout(ctx, 15*time.Second)
	_ = writer2.Close(closeCtx)
	closeCancel()

	// Restart MinIO
	cluster.StartNode(t, "minio")
	t.Log("MinIO restarted")

	// Wait for MinIO to become healthy
	cluster.WaitForHealthy(t, "minio", 60*time.Second)
	time.Sleep(10 * time.Second)

	// Phase 3: Writes should recover after MinIO is back
	logHandle3, err := client.OpenLog(ctx, logName)
	require.NoError(t, err)

	writer3, err := logHandle3.OpenLogWriter(ctx)
	require.NoError(t, err)

	ids3 := framework.WriteEntries(t, ctx, writer3, 10, 5)
	require.Len(t, ids3, 5)
	t.Log("Phase 3: 5 entries written after MinIO recovery")

	err = writer3.Close(ctx)
	require.NoError(t, err)

	// Read back everything we know succeeded
	expectedCount := len(ids1) + len(ids2) + len(ids3)
	msgs := framework.ReadAllEntries(t, ctx, logHandle3, expectedCount)
	require.Len(t, msgs, expectedCount)
	verifyEntryOrder(t, msgs)

	t.Logf("MinIOFailure_WriteBlocksAndRecovers passed: %d entries survived MinIO outage", expectedCount)
}

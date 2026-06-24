// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/server"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// TestServiceUpgrade_NodeStatus verifies that all nodes in a cluster report correct status
// through the node lifecycle API after startup.
func TestServiceUpgrade_NodeStatus(t *testing.T) {
	rootPath := t.TempDir()

	// Start a 3-node cluster
	cluster, _, _, _ := utils.StartMiniCluster(t, 3, rootPath)
	defer cluster.StopMultiNodeCluster(t)

	// All nodes should be in active state
	for nodeIndex, srv := range cluster.Servers {
		if srv == nil {
			continue
		}
		status := srv.GetNodeStatus()
		require.Equal(t, "active", status.State, "node %d should be active", nodeIndex)
		require.False(t, status.IsDecommissioning, "node %d should not be decommissioning", nodeIndex)
		require.Equal(t, 3, status.MemberCount, "node %d should see 3 members", nodeIndex)
		require.NotEmpty(t, status.Address, "node %d should have an address", nodeIndex)
		require.Equal(t, "default", status.ResourceGroup)
		require.Equal(t, "default", status.AZ)
		t.Logf("Node %d status: %+v", nodeIndex, status)
	}
}

// TestServiceUpgrade_DecommissionRejectsWrites verifies that a decommissioned node
// rejects new writes while the cluster remains functional through other nodes.
func TestServiceUpgrade_DecommissionRejectsWrites(t *testing.T) {
	rootPath := t.TempDir()

	// Start a 5-node cluster — enough nodes for quorum after 1 is decommissioned
	cluster, cfg, _, seeds := utils.StartMiniCluster(t, 5, rootPath)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	defer cluster.StopMultiNodeCluster(t)

	// Pick node 0 to decommission
	targetNode := cluster.Servers[0]
	require.NotNil(t, targetNode)

	// Verify node is active
	status := targetNode.GetNodeStatus()
	require.Equal(t, "active", status.State)

	// Decommission the node
	err := targetNode.Decommission()
	require.NoError(t, err)

	// Verify state changed
	status = targetNode.GetNodeStatus()
	require.Equal(t, "decommissioning", status.State)
	require.True(t, status.IsDecommissioning)

	// Idempotent call should succeed
	err = targetNode.Decommission()
	require.NoError(t, err)

	// Check progress
	progress := targetNode.GetDecommissionProgress()
	require.Equal(t, "decommissioning", progress.State)
	t.Logf("Decommission progress: %+v", progress)

	// Other nodes should still be active
	for nodeIndex, srv := range cluster.Servers {
		if srv == nil || nodeIndex == 0 {
			continue
		}
		otherStatus := srv.GetNodeStatus()
		require.Equal(t, "active", otherStatus.State, "node %d should still be active", nodeIndex)
	}

	// Wait for gossip to propagate decommission tag
	time.Sleep(2 * time.Second)

	// Verify writes still succeed — quorum selection filters out decommissioned node
	ctx := context.Background()
	etcdCli, etcdErr := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, etcdErr)
	defer etcdCli.Close()

	wpClient, wpErr := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, wpErr)
	defer func() { _ = wpClient.Close(ctx) }()

	logName := fmt.Sprintf("test-decommission-writes-%d", time.Now().UnixMilli())
	err = wpClient.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)

	logWriter, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		result := logWriter.Write(ctx, &log.WriteMessage{
			Payload: []byte("test-data-after-decommission"),
		})
		require.NoError(t, result.Err, "write %d should succeed via remaining nodes", i)
	}
	t.Log("Successfully wrote 10 entries with a decommissioning node in the cluster")
}

// TestServiceUpgrade_DecommissionPersistenceAcrossRestart verifies that decommission
// state survives a node restart.
func TestServiceUpgrade_DecommissionPersistenceAcrossRestart(t *testing.T) {
	rootPath := t.TempDir()

	// Start a 3-node cluster
	cluster, _, gossipSeeds, _ := utils.StartMiniCluster(t, 3, rootPath)
	defer cluster.StopMultiNodeCluster(t)

	// Decommission node 1
	targetNode := cluster.Servers[1]
	require.NotNil(t, targetNode)

	err := targetNode.Decommission()
	require.NoError(t, err)
	require.Equal(t, "decommissioning", targetNode.GetNodeStatus().State)

	// Verify state file exists on disk
	nodeDataDir := filepath.Join(rootPath, "node1")
	stateFilePath := filepath.Join(nodeDataDir, "node_state.json")
	_, statErr := os.Stat(stateFilePath)
	require.NoError(t, statErr, "node_state.json should exist after decommission")
	t.Logf("State file written at: %s", stateFilePath)

	// Stop node 1 (simulate crash/restart)
	_, err = cluster.LeaveNodeWithIndex(t, 1)
	require.NoError(t, err)
	t.Log("Node 1 stopped")

	// Restart node 1 — it should recover decommissioning state
	_, err = cluster.RestartNode(t, 1, gossipSeeds)
	require.NoError(t, err)
	t.Log("Node 1 restarted")

	// Wait for it to join the cluster
	time.Sleep(2 * time.Second)

	restartedNode := cluster.Servers[1]
	require.NotNil(t, restartedNode)

	// Should still be decommissioning after restart
	status := restartedNode.GetNodeStatus()
	require.Equal(t, "decommissioning", status.State,
		"node should resume decommissioning state after restart")
	require.True(t, status.IsDecommissioning)
	t.Logf("Restarted node status: %+v", status)
}

// TestServiceUpgrade_DecommissionAutoCompleteWhenNoData verifies that a node
// automatically transitions to decommissioned when it has no local segment data.
func TestServiceUpgrade_DecommissionAutoCompleteWhenNoData(t *testing.T) {
	rootPath := t.TempDir()

	// Start a 3-node cluster
	cluster, _, _, _ := utils.StartMiniCluster(t, 3, rootPath)
	defer cluster.StopMultiNodeCluster(t)

	// Pick a node that has never served any data (fresh node)
	targetNode := cluster.Servers[2]
	require.NotNil(t, targetNode)

	// Decommission it — since it has no local data, it should auto-complete
	err := targetNode.Decommission()
	require.NoError(t, err)

	// Wait for the decommission monitor to detect no data and mark complete
	require.Eventually(t, func() bool {
		return targetNode.GetNodeStatus().State == string(server.NodeStateDecommissioned)
	}, 15*time.Second, 500*time.Millisecond,
		"node with no data should auto-transition to decommissioned")

	// Progress must show safe to terminate
	progress := targetNode.GetDecommissionProgress()
	require.Equal(t, "decommissioned", progress.State)
	require.False(t, progress.HasLocalData)
	require.True(t, progress.SafeToTerminate)
	t.Logf("Auto-decommission complete: %+v", progress)

	// Further decommission calls should return error
	err = targetNode.Decommission()
	require.Error(t, err)
	require.Contains(t, err.Error(), "already decommissioned")
}

// TestServiceUpgrade_DecommissionWithActiveData verifies that a node with active
// segment data stays in decommissioning state until data is cleaned up.
func TestServiceUpgrade_DecommissionWithActiveData(t *testing.T) {
	rootPath := t.TempDir()

	// Use custom config with small segments for fast data creation
	cfg, cfgErr := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, cfgErr)
	cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize = 1024 * 2 // 2KB segments

	// Start a 3-node cluster
	cluster, cfg, _, seeds := utils.StartMiniClusterWithCfg(t, 3, rootPath, cfg)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	defer cluster.StopMultiNodeCluster(t)

	// Write some data to create segments on the nodes
	ctx := context.Background()
	etcdCli, etcdErr := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, etcdErr)
	defer etcdCli.Close()

	wpClient, wpErr := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, wpErr)
	defer func() { _ = wpClient.Close(ctx) }()

	logName := fmt.Sprintf("test-decommission-data-%d", time.Now().UnixMilli())
	err := wpClient.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)

	logWriter, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write enough data to create segment files on nodes
	const totalMsgs = 20
	const msgSize = 1024 // 1KB per message
	for i := 0; i < totalMsgs; i++ {
		payload := make([]byte, msgSize)
		for j := range payload {
			payload[j] = byte(i % 256)
		}
		result := logWriter.Write(ctx, &log.WriteMessage{
			Payload: payload,
		})
		require.NoError(t, result.Err, "write %d failed", i)
	}
	t.Logf("Wrote %d entries to create segment data on nodes", totalMsgs)

	// Find a node that has local segment data
	var targetNodeIndex int = -1
	for nodeIndex, srv := range cluster.Servers {
		if srv == nil {
			continue
		}
		progress := srv.GetDecommissionProgress()
		if progress.HasLocalData {
			targetNodeIndex = nodeIndex
			break
		}
	}

	if targetNodeIndex == -1 {
		t.Skip("No node has local segment data (may happen with object-only storage)")
	}

	targetNode := cluster.Servers[targetNodeIndex]
	t.Logf("Node %d has local segment data, decommissioning it", targetNodeIndex)

	// Decommission the node with data
	err = targetNode.Decommission()
	require.NoError(t, err)

	// The node should stay in decommissioning because it has local data
	// Give the monitor a couple of cycles to check
	time.Sleep(12 * time.Second)

	status := targetNode.GetNodeStatus()
	require.Equal(t, "decommissioning", status.State,
		"node with local data should remain decommissioning")

	progress := targetNode.GetDecommissionProgress()
	require.True(t, progress.HasLocalData, "node should still have local data")
	require.False(t, progress.SafeToTerminate, "should not be safe to terminate with local data")
	t.Logf("Node %d progress (with data): %+v", targetNodeIndex, progress)
}

// TestServiceUpgrade_DecommissionCompleteAfterDataCleanup verifies the full decommission
// lifecycle: write data → decommission node → truncate → retention expires → data cleaned
// → node auto-transitions to decommissioned.
//
// This test uses short retention (2s) and auditor interval (2s) to make the cleanup fast.
func TestServiceUpgrade_DecommissionCompleteAfterDataCleanup(t *testing.T) {
	rootPath := t.TempDir()

	// Load config with short retention and auditor for fast cleanup
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize = 1024 * 2 // 2KB segments for fast rolling
	cfg.Woodpecker.Client.Auditor.MaxInterval = config.DurationSeconds{
		Duration: config.NewDuration(2*time.Second, time.Second),
	}
	cfg.Woodpecker.Logstore.RetentionPolicy.TTL = 2 // 2 seconds retention

	// Start 5-node cluster — need enough nodes so quorum works after 1 decommissioned
	cluster, cfg, _, seeds := utils.StartMiniClusterWithCfg(t, 5, rootPath, cfg)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	defer cluster.StopMultiNodeCluster(t)

	// Create woodpecker client
	ctx := context.Background()
	etcdCli, etcdErr := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, etcdErr)
	defer etcdCli.Close()

	wpClient, wpErr := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, wpErr)
	defer func() { _ = wpClient.Close(ctx) }()

	logName := "test-decommission-full-lifecycle-" + fmt.Sprintf("%d", time.Now().UnixMilli())
	err = wpClient.CreateLog(ctx, logName)
	require.NoError(t, err)

	logHandle, err := wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)

	logWriter, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	// Write enough data to create multiple segments across nodes
	const totalMsgs = 30
	const msgSize = 1024 // 1KB per message → with 2KB max segment, forces frequent rolling
	writtenIds := make([]*log.LogMessageId, totalMsgs)
	segmentsSeen := make(map[int64]bool)

	for i := 0; i < totalMsgs; i++ {
		payload := make([]byte, msgSize)
		for j := range payload {
			payload[j] = byte(i % 256)
		}
		result := logWriter.Write(ctx, &log.WriteMessage{
			Payload: payload,
		})
		require.NoError(t, result.Err, "write %d failed", i)
		writtenIds[i] = result.LogMessageId
		segmentsSeen[result.LogMessageId.SegmentId] = true
	}
	t.Logf("Wrote %d messages across %d segments", totalMsgs, len(segmentsSeen))
	require.Greater(t, len(segmentsSeen), 3, "expected multiple segments to be created")

	// Find a node that has local segment data
	var targetNodeIndex int = -1
	for nodeIndex, srv := range cluster.Servers {
		if srv == nil {
			continue
		}
		if srv.GetDecommissionProgress().HasLocalData {
			targetNodeIndex = nodeIndex
			break
		}
	}
	require.NotEqual(t, -1, targetNodeIndex, "at least one node should have local data")
	targetNode := cluster.Servers[targetNodeIndex]
	t.Logf("Node %d has local segment data, decommissioning it", targetNodeIndex)

	// Decommission the target node
	err = targetNode.Decommission()
	require.NoError(t, err)
	require.Equal(t, "decommissioning", targetNode.GetNodeStatus().State)
	require.True(t, targetNode.GetDecommissionProgress().HasLocalData)

	// Wait for gossip to propagate the decommission tag so new segments avoid this node
	time.Sleep(2 * time.Second)

	// Continue writing MORE data to force segment rolling. New segments will be placed
	// on non-decommissioned nodes (quorum selection now filters them out). This causes
	// the old active segment on the decommissioned node to be completed/fenced.
	const extraMsgs = 20
	for i := 0; i < extraMsgs; i++ {
		payload := make([]byte, msgSize)
		for j := range payload {
			payload[j] = byte((totalMsgs + i) % 256)
		}
		result := logWriter.Write(ctx, &log.WriteMessage{
			Payload: payload,
		})
		require.NoError(t, result.Err, "post-decommission write %d failed", i)
		writtenIds = append(writtenIds, result.LogMessageId)
	}
	t.Logf("Wrote %d additional messages after decommission to trigger segment rolling", extraMsgs)

	// Write one final message to push data into a new segment beyond the decommissioned
	// node's data. The auditor skips the truncation-point segment (segId >= truncatedSegmentId),
	// so we need the truncation point to be in a segment AFTER all segments on the
	// decommissioned node. This extra write ensures the last segment containing node3's
	// data becomes eligible for cleanup.
	finalPayload := make([]byte, msgSize)
	finalResult := logWriter.Write(ctx, &log.WriteMessage{Payload: finalPayload})
	require.NoError(t, finalResult.Err, "final post-decommission write failed")

	// Truncate all data — mark everything as truncatable
	truncateAt := finalResult.LogMessageId
	t.Logf("Truncating at last entry: segmentId=%d, entryId=%d",
		truncateAt.SegmentId, truncateAt.EntryId)
	err = logHandle.Truncate(ctx, truncateAt)
	require.NoError(t, err)

	// Wait for: retention (2s) + auditor cycles (2s each) + cleanup execution
	// The auditor will detect truncated segments, check retention TTL, and call CleanSegment
	// on all quorum nodes including our decommissioning node.
	// The decommission monitor will then detect no local data and mark decommissioned.
	t.Log("Waiting for retention expiry + cleanup + decommission monitor...")

	require.Eventually(t, func() bool {
		state := targetNode.GetNodeStatus().State
		progress := targetNode.GetDecommissionProgress()
		t.Logf("  check: state=%s, hasLocalData=%v, remainingProcessors=%d, safeToTerminate=%v",
			state, progress.HasLocalData, progress.RemainingProcessors, progress.SafeToTerminate)
		return state == string(server.NodeStateDecommissioned)
	}, 120*time.Second, 3*time.Second,
		"node should auto-transition to decommissioned after data cleanup")

	// Verify final state
	progress := targetNode.GetDecommissionProgress()
	require.Equal(t, "decommissioned", progress.State)
	require.False(t, progress.HasLocalData)
	require.True(t, progress.SafeToTerminate)
	t.Logf("Full decommission lifecycle complete: %+v", progress)
}

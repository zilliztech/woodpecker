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
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server"
	"github.com/zilliztech/woodpecker/server/storage/stagedstorage"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func TestStagedStorageService_CompactedCleanup_MultiNodeReclaimAllowsDecommission(t *testing.T) {
	rootPath := t.TempDir()
	cfg := compactedCleanupE2EConfig(t)

	cluster, cfg, _, seeds := utils.StartMiniClusterWithCfg(t, 5, rootPath, cfg)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	defer cluster.StopMultiNodeCluster(t)

	ctx := context.Background()
	etcdCli, etcdErr := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, etcdErr)
	defer etcdCli.Close()

	wpClient, wpErr := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, wpErr)
	defer func() { _ = wpClient.Close(ctx) }()

	logName := fmt.Sprintf("test-compacted-cleanup-multinode-%d", time.Now().UnixMilli())
	require.NoError(t, wpClient.CreateLog(ctx, logName))

	logHandle, err := wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)

	logWriter, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)
	defer func() { _ = logWriter.Close(ctx) }()

	segID := writeCompactedCleanupSegment(t, ctx, logWriter, 8, 1024)
	segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, segHandle)
	quorumInfo, err := segHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	quorumNodeIndexes := compactedCleanupQuorumNodeIndexes(t, cluster, quorumInfo.Nodes)

	requireCompactedCleanupEventually(t, 10*time.Second, 200*time.Millisecond, func() (bool, string) {
		return compactedCleanupAllDataLogsPresent(cluster, cfg, quorumNodeIndexes, logHandle.GetId(), segID)
	})

	require.NoError(t, logHandle.CompleteAllActiveSegmentIfExists(ctx))

	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Sealed, 45*time.Second)
	requireCompactedCleanupEventually(t, 45*time.Second, 500*time.Millisecond, func() (bool, string) {
		return compactedCleanupAllQuorumReclaimed(cluster, cfg, quorumNodeIndexes, logHandle.GetId(), segID)
	})

	targetNodeIndex := quorumNodeIndexes[0]
	targetNode := cluster.Servers[targetNodeIndex]
	require.NotNil(t, targetNode)
	require.False(t, targetNode.GetDecommissionProgress().HasLocalData,
		"compacted cleanup should make the target node locally drained before decommission")

	require.NoError(t, targetNode.Decommission())
	requireCompactedCleanupEventually(t, 20*time.Second, 500*time.Millisecond, func() (bool, string) {
		progress := targetNode.GetDecommissionProgress()
		return progress.State == string(server.NodeStateDecommissioned),
			fmt.Sprintf("state=%s hasLocalData=%v safeToTerminate=%v", progress.State, progress.HasLocalData, progress.SafeToTerminate)
	})
}

func TestStagedStorageService_CompactedCleanup_ReconcileReclaimsUnnotifiedSegmentAfterRestart(t *testing.T) {
	rootPath := t.TempDir()
	cfg := compactedCleanupE2EConfig(t)

	cluster, cfg, gossipSeeds, seeds := utils.StartMiniClusterWithCfg(t, 3, rootPath, cfg)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	defer cluster.StopMultiNodeCluster(t)

	ctx := context.Background()
	etcdCli, etcdErr := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, etcdErr)
	defer etcdCli.Close()

	wpClient, wpErr := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, wpErr)
	defer func() { _ = wpClient.Close(ctx) }()

	logName := fmt.Sprintf("test-compacted-cleanup-reconcile-%d", time.Now().UnixMilli())
	require.NoError(t, wpClient.CreateLog(ctx, logName))

	logHandle, err := wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)

	logWriter, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	segID := writeCompactedCleanupSegment(t, ctx, logWriter, 8, 1024)
	segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, segHandle)
	quorumInfo, err := segHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	quorumNodeIndexes := compactedCleanupQuorumNodeIndexes(t, cluster, quorumInfo.Nodes)
	targetNodeIndex := quorumNodeIndexes[0]

	requireCompactedCleanupEventually(t, 10*time.Second, 200*time.Millisecond, func() (bool, string) {
		return compactedCleanupDataLogPresent(cluster, cfg, targetNodeIndex, logHandle.GetId(), segID)
	})

	require.NoError(t, logWriter.Close(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Completed, 20*time.Second)

	readonlySeg, err := logHandle.GetExistsReadonlySegmentHandle(ctx, segID)
	require.NoError(t, err)
	require.NoError(t, readonlySeg.Compact(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Sealed, 20*time.Second)

	ok, msg := compactedCleanupDataLogPresent(cluster, cfg, targetNodeIndex, logHandle.GetId(), segID)
	require.True(t, ok, msg)
	markPath := filepath.Join(compactedCleanupSegmentDir(cluster, cfg, targetNodeIndex, logHandle.GetId(), segID), stagedstorage.CompactedMarkFileName)
	require.NoFileExists(t, markPath, "manual Compact() must not write notify marks inline")

	stoppedNodeIndex, err := cluster.LeaveNodeWithIndex(t, targetNodeIndex)
	require.NoError(t, err)
	require.Equal(t, targetNodeIndex, stoppedNodeIndex)

	_, err = cluster.RestartNode(t, targetNodeIndex, gossipSeeds)
	require.NoError(t, err)
	targetNode := cluster.Servers[targetNodeIndex]
	require.NotNil(t, targetNode)

	requireCompactedCleanupEventually(t, 20*time.Second, 500*time.Millisecond, func() (bool, string) {
		return compactedCleanupSegmentReclaimed(cluster, cfg, targetNodeIndex, logHandle.GetId(), segID)
	})

	require.NoError(t, targetNode.Decommission())
	requireCompactedCleanupEventually(t, 20*time.Second, 500*time.Millisecond, func() (bool, string) {
		progress := targetNode.GetDecommissionProgress()
		return progress.State == string(server.NodeStateDecommissioned),
			fmt.Sprintf("state=%s hasLocalData=%v safeToTerminate=%v", progress.State, progress.HasLocalData, progress.SafeToTerminate)
	})
}

func compactedCleanupE2EConfig(t *testing.T) *config.Configuration {
	t.Helper()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Woodpecker.Client.Auditor.MaxInterval = config.NewDurationSecondsFromInt(1)
	cfg.Woodpecker.Logstore.MaintenanceStrategy.CompactedFileCleanupInterval = config.NewDurationSecondsFromInt(1)
	cfg.Woodpecker.Logstore.MaintenanceStrategy.ReconcileMinDataLogAge = config.NewDurationSecondsFromInt(0)
	return cfg
}

func writeCompactedCleanupSegment(t *testing.T, ctx context.Context, writer log.LogWriter, entries int, payloadSize int) int64 {
	t.Helper()
	var segID int64 = -1
	for i := 0; i < entries; i++ {
		payload := make([]byte, payloadSize)
		for j := range payload {
			payload[j] = byte((i + j) % 251)
		}
		result := writer.Write(ctx, &log.WriteMessage{Payload: payload})
		require.NoError(t, result.Err, "write %d failed", i)
		require.NotNil(t, result.LogMessageId)
		if segID == -1 {
			segID = result.LogMessageId.SegmentId
		}
		require.Equal(t, segID, result.LogMessageId.SegmentId, "test expects one explicit segment")
	}
	return segID
}

func requireCompactedCleanupSegmentState(t *testing.T, ctx context.Context, logHandle log.LogHandle, segID int64, state proto.SegmentState, timeout time.Duration) {
	t.Helper()
	requireCompactedCleanupEventually(t, timeout, 500*time.Millisecond, func() (bool, string) {
		segs, err := logHandle.GetSegments(ctx)
		if err != nil {
			return false, err.Error()
		}
		seg, ok := segs[segID]
		if !ok || seg == nil || seg.Metadata == nil {
			return false, fmt.Sprintf("segment %d missing", segID)
		}
		return seg.Metadata.State == state, fmt.Sprintf("segment %d state=%s", segID, seg.Metadata.State.String())
	})
}

func compactedCleanupQuorumNodeIndexes(t *testing.T, cluster *utils.MiniCluster, quorumNodes []string) []int {
	t.Helper()
	require.NotEmpty(t, quorumNodes)
	indexes := make([]int, 0, len(quorumNodes))
	for _, node := range quorumNodes {
		indexes = append(indexes, compactedCleanupNodeIndexByAddress(t, cluster, node))
	}
	return indexes
}

func compactedCleanupNodeIndexByAddress(t *testing.T, cluster *utils.MiniCluster, address string) int {
	t.Helper()
	_, portText, err := net.SplitHostPort(address)
	require.NoError(t, err, "invalid quorum node address %q", address)
	port, err := strconv.Atoi(portText)
	require.NoError(t, err)
	for nodeIndex, nodePort := range cluster.UsedPorts {
		if nodePort == port {
			return nodeIndex
		}
	}
	require.Failf(t, "node not found", "no mini-cluster node has service port %d from quorum address %s", port, address)
	return -1
}

func compactedCleanupSegmentDir(cluster *utils.MiniCluster, cfg *config.Configuration, nodeIndex int, logID int64, segID int64) string {
	return filepath.Join(
		cluster.BaseDir,
		fmt.Sprintf("node%d", nodeIndex),
		cfg.Minio.BucketName,
		cfg.Minio.RootPath,
		strconv.FormatInt(logID, 10),
		strconv.FormatInt(segID, 10),
	)
}

func compactedCleanupDataLogPresent(cluster *utils.MiniCluster, cfg *config.Configuration, nodeIndex int, logID int64, segID int64) (bool, string) {
	dataLogPath := filepath.Join(compactedCleanupSegmentDir(cluster, cfg, nodeIndex, logID, segID), "data.log")
	info, err := os.Stat(dataLogPath)
	if err != nil {
		return false, fmt.Sprintf("node %d data.log missing: %v", nodeIndex, err)
	}
	if info.Size() == 0 {
		return false, fmt.Sprintf("node %d data.log is empty", nodeIndex)
	}
	return true, ""
}

func compactedCleanupSegmentReclaimed(cluster *utils.MiniCluster, cfg *config.Configuration, nodeIndex int, logID int64, segID int64) (bool, string) {
	segDir := compactedCleanupSegmentDir(cluster, cfg, nodeIndex, logID, segID)
	markPath := filepath.Join(segDir, stagedstorage.CompactedMarkFileName)
	if _, err := os.Stat(markPath); err != nil {
		return false, fmt.Sprintf("node %d mark missing: %v", nodeIndex, err)
	}
	dataLogPath := filepath.Join(segDir, "data.log")
	if _, err := os.Stat(dataLogPath); err == nil {
		return false, fmt.Sprintf("node %d data.log still exists", nodeIndex)
	} else if !os.IsNotExist(err) {
		return false, fmt.Sprintf("node %d data.log stat failed: %v", nodeIndex, err)
	}
	return true, ""
}

func compactedCleanupAllDataLogsPresent(cluster *utils.MiniCluster, cfg *config.Configuration, nodeIndexes []int, logID int64, segID int64) (bool, string) {
	for _, nodeIndex := range nodeIndexes {
		if ok, msg := compactedCleanupDataLogPresent(cluster, cfg, nodeIndex, logID, segID); !ok {
			return false, msg
		}
	}
	return true, ""
}

func compactedCleanupAllQuorumReclaimed(cluster *utils.MiniCluster, cfg *config.Configuration, nodeIndexes []int, logID int64, segID int64) (bool, string) {
	for _, nodeIndex := range nodeIndexes {
		if ok, msg := compactedCleanupSegmentReclaimed(cluster, cfg, nodeIndex, logID, segID); !ok {
			return false, msg
		}
	}
	return true, ""
}

func requireCompactedCleanupEventually(t *testing.T, timeout time.Duration, interval time.Duration, check func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	lastMsg := ""
	for time.Now().Before(deadline) {
		ok, msg := check()
		if ok {
			return
		}
		lastMsg = msg
		time.Sleep(interval)
	}
	ok, msg := check()
	if ok {
		return
	}
	if msg != "" {
		lastMsg = msg
	}
	require.Failf(t, "condition not met", "timeout=%s last=%s", timeout, lastMsg)
}

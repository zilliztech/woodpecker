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

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// corruptionSegmentDir returns the on-disk directory holding a segment's local data.log for one
// mini-cluster node. It mirrors the staged writer's localBaseDir layout.
func corruptionSegmentDir(cluster *utils.MiniCluster, cfg *config.Configuration, nodeIndex int, logID, segID int64) string {
	return filepath.Join(
		cluster.BaseDir,
		fmt.Sprintf("node%d", nodeIndex),
		cfg.Minio.BucketName,
		cfg.Minio.RootPath,
		fmt.Sprintf("%d", logID),
		fmt.Sprintf("%d", segID),
	)
}

// corruptionDataLogPath returns the data.log path for a node/segment.
func corruptionDataLogPath(cluster *utils.MiniCluster, cfg *config.Configuration, nodeIndex int, logID, segID int64) string {
	return filepath.Join(corruptionSegmentDir(cluster, cfg, nodeIndex, logID, segID), "data.log")
}

// dataRecordPayloadOffsets scans the data.log records in order and returns the byte offset of each
// data record's payload. Entry ids are implicit: they start at the header's FirstEntryID and
// increase by one per data record. These test segments start at zero, so index N is entry N.
func dataRecordPayloadOffsets(t *testing.T, dataLogPath string) []int64 {
	t.Helper()
	data, err := os.ReadFile(dataLogPath)
	require.NoError(t, err)

	offsets := make([]int64, 0)
	offset := int64(0)
	for offset+codec.RecordHeaderSize <= int64(len(data)) {
		recordType := data[offset+4]
		payloadLength := int64(binary.LittleEndian.Uint32(data[offset+5 : offset+9]))
		total := int64(codec.RecordHeaderSize) + payloadLength
		if offset+total > int64(len(data)) {
			break
		}
		payloadStart := offset + int64(codec.RecordHeaderSize)
		switch recordType {
		case codec.HeaderRecordType:
			require.GreaterOrEqual(t, payloadLength, int64(12), "short header record")
			require.Zero(t, binary.LittleEndian.Uint64(data[payloadStart+4:payloadStart+12]), "test segments must start at entry zero")
		case codec.DataRecordType:
			offsets = append(offsets, payloadStart)
		}
		offset += total
	}
	require.NotEmpty(t, offsets, "no data records in %s", dataLogPath)
	return offsets
}

// corruptDataRecordAtEntry corrupts the payload of the data record holding the given entry id, so
// the block CRC no longer matches. This models a damaged block while keeping the record structure
// parseable -- the kind of damage a reader's CRC check and compaction's CRC check catch, rather
// than a truncation.
func corruptDataRecordAtEntry(t *testing.T, dataLogPath string, entryID int64) {
	t.Helper()
	offsets := dataRecordPayloadOffsets(t, dataLogPath)
	require.GreaterOrEqual(t, entryID, int64(0))
	require.Less(t, entryID, int64(len(offsets)), "entry %d out of range in %s", entryID, dataLogPath)

	data, err := os.ReadFile(dataLogPath)
	require.NoError(t, err)
	payloadStart := offsets[entryID]
	require.Greater(t, int64(len(data)), payloadStart, "data record too short to corrupt")
	data[payloadStart] ^= 0xFF
	require.NoError(t, os.WriteFile(dataLogPath, data, 0o644))
}

func corruptionE2EConfig(t *testing.T) *config.Configuration {
	t.Helper()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Woodpecker.Client.Auditor.MaxInterval = config.NewDurationSecondsFromInt(1)
	return cfg
}

func newCorruptionClient(t *testing.T, ctx context.Context, cfg *config.Configuration, seeds []string) woodpecker.Client {
	t.Helper()
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
	require.NoError(t, err)
	t.Cleanup(func() { _ = etcdCli.Close() })

	c, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

// writeCorruptionSegment writes entries through the writer and returns the single segment id used.
func writeCorruptionSegment(t *testing.T, ctx context.Context, writer log.LogWriter, entries int) int64 {
	t.Helper()
	var segID int64 = -1
	for i := 0; i < entries; i++ {
		payload := []byte(fmt.Sprintf("corruption-entry-%04d", i))
		result := writer.Write(ctx, &log.WriteMessage{Payload: payload})
		require.NoError(t, result.Err, "write %d failed", i)
		require.NotNil(t, result.LogMessageId)
		if segID == -1 {
			segID = result.LogMessageId.SegmentId
		}
		require.Equal(t, segID, result.LogMessageId.SegmentId)
	}
	return segID
}

// quorumNodeIndexes resolves quorum node addresses to mini-cluster node indexes.
func quorumNodeIndexes(t *testing.T, cluster *utils.MiniCluster, nodes []string) []int {
	t.Helper()
	require.NotEmpty(t, nodes)
	return compactedCleanupQuorumNodeIndexes(t, cluster, nodes)
}

// restartCorruptionNode waits for membership convergence before another node is
// stopped. Restarted nodes use new gossip ports, so rapid restarts against stale
// membership can otherwise leave each replica in an isolated cluster.
func restartCorruptionNode(t *testing.T, cluster *utils.MiniCluster, nodeIndex int) {
	t.Helper()
	_, err := cluster.RestartNode(t, nodeIndex, cluster.GetSeedList())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, srv := range cluster.Servers {
			if srv != nil && srv.GetMemberCount() < cluster.GetActiveNodes() {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "restarted nodes must rejoin the cluster")
}

// declareSkipRange writes the operator-declared skip range for a log/segment through the client's
// metadata provider, so a stalled reader can move past the damaged range.
func declareSkipRange(t *testing.T, ctx context.Context, c woodpecker.Client, logID, segID, from, to int64) {
	t.Helper()
	provider := c.GetMetadataProvider()
	rec, err := provider.GetAllSkipRanges(ctx)
	require.NoError(t, err)
	if rec.Metadata.GetByLogId() == nil {
		rec.Metadata.ByLogId = map[int64]*proto.LogSkipRanges{}
	}
	if rec.Metadata.ByLogId[logID] == nil {
		rec.Metadata.ByLogId[logID] = &proto.LogSkipRanges{BySegmentId: map[int64]*proto.SegmentSkipRanges{}}
	}
	rec.Metadata.ByLogId[logID].BySegmentId[segID] = &proto.SegmentSkipRanges{
		Ranges: []*proto.SkipRange{{
			FromEntryId: from, ToEntryId: to,
			CreationTimestamp: uint64(time.Now().Unix()), Reason: "e2e corruption",
		}},
	}
	require.NoError(t, provider.UpdateAllSkipRanges(ctx, rec))
}

// readEntry reads the next entry, or reports that it stalled (timed out) within the bound.
func readEntry(ctx context.Context, reader log.LogReader) (*log.LogMessage, error) {
	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return reader.ReadNext(readCtx)
}

// TestDataCorruptionService_Active_OneReplicaDamaged covers B1: an Active segment where
// one replica's local data is truncated before finalize. Restart + fence re-derives the LAC from
// the two healthy replicas, so no data is lost, no reader stalls, and compaction succeeds.
func TestDataCorruptionService_Active_OneReplicaDamaged(t *testing.T) {
	const (
		clusterSize = 3
		entries     = 100
	)
	rootPath := t.TempDir()
	cfg := corruptionE2EConfig(t)

	cluster, cfg, _, seeds := utils.StartMiniClusterWithCfg(t, clusterSize, rootPath, cfg)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	defer cluster.StopMultiNodeCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	wpClient := newCorruptionClient(t, ctx, cfg, seeds)
	logName := fmt.Sprintf("corruption-active-one-%d", time.Now().UnixMilli())
	require.NoError(t, wpClient.CreateLog(ctx, logName))

	logHandle, err := wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)
	defer logHandle.Close(context.Background())

	logWriter, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)
	defer logWriter.Close(ctx)

	segID := writeCorruptionSegment(t, ctx, logWriter, entries)
	segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, segHandle)
	quorumInfo, err := segHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	nodes := quorumNodeIndexes(t, cluster, quorumInfo.Nodes)

	// Damage one replica's local data.log (corrupt the first data entry's payload, breaking CRC).
	target := nodes[0]
	dataLogPath := corruptionDataLogPath(cluster, cfg, target, logHandle.GetId(), segID)
	require.FileExists(t, dataLogPath)
	corruptDataRecordAtEntry(t, dataLogPath, 0)

	// Restart the damaged node; recovery truncates it at the bad point.
	_, err = cluster.LeaveNodeWithIndex(t, target)
	require.NoError(t, err)
	restartCorruptionNode(t, cluster, target)

	// Re-open the writer to trigger fenceAllActiveSegments; LAC resolves from the healthy replicas.
	// Take over through a fresh handle before closing the original writer:
	// a normal close would finalize the segment and bypass recovery fencing.
	previousWriter := logWriter
	defer previousWriter.Close(context.Background())
	logHandle, err = wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)
	defer logHandle.Close(context.Background())
	logWriter, err = logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)
	defer logWriter.Close(ctx)

	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Completed, 45*time.Second)

	// The two healthy replicas cover the full range, so the reader still sees every entry.
	reader, err := logHandle.OpenLogReader(ctx, &log.LogMessageId{SegmentId: segID, EntryId: 0}, "b1-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)
	for i := 0; i < entries; i++ {
		msg, readErr := readEntry(ctx, reader)
		require.NoError(t, readErr, "read %d failed (must not stall with two healthy replicas)", i)
		require.Equal(t, int64(i), msg.Id.EntryId)
	}
	readonlySeg, err := logHandle.GetExistsReadonlySegmentHandle(ctx, segID)
	require.NoError(t, err)
	require.NoError(t, readonlySeg.Compact(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Sealed, 20*time.Second)
}

// TestDataCorruptionService_Completed_OneReplicaDamaged covers A1: a Completed segment
// with one damaged replica. The reader fails over to the two healthy replicas and reads every
// entry; compaction seals on a healthy replica.
func TestDataCorruptionService_Completed_OneReplicaDamaged(t *testing.T) {
	const (
		clusterSize = 3
		entries     = 100
	)
	rootPath := t.TempDir()
	cfg := corruptionE2EConfig(t)

	cluster, cfg, _, seeds := utils.StartMiniClusterWithCfg(t, clusterSize, rootPath, cfg)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	defer cluster.StopMultiNodeCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	wpClient := newCorruptionClient(t, ctx, cfg, seeds)
	logName := fmt.Sprintf("corruption-completed-one-%d", time.Now().UnixMilli())
	require.NoError(t, wpClient.CreateLog(ctx, logName))

	logHandle, err := wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)
	defer logHandle.Close(context.Background())

	logWriter, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	segID := writeCorruptionSegment(t, ctx, logWriter, entries)
	segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, segHandle)
	quorumInfo, err := segHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	nodes := quorumNodeIndexes(t, cluster, quorumInfo.Nodes)

	require.NoError(t, logHandle.CompleteAllActiveSegmentIfExists(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Completed, 45*time.Second)
	require.NoError(t, logWriter.Close(ctx))

	// Damage one replica's local data.log.
	target := nodes[0]
	dataLogPath := corruptionDataLogPath(cluster, cfg, target, logHandle.GetId(), segID)
	require.FileExists(t, dataLogPath)
	corruptDataRecordAtEntry(t, dataLogPath, 0)

	// Restart the damaged node.
	_, err = cluster.LeaveNodeWithIndex(t, target)
	require.NoError(t, err)
	restartCorruptionNode(t, cluster, target)

	// The two healthy replicas still cover the full range: the reader sees every entry.
	reader, err := logHandle.OpenLogReader(ctx, &log.LogMessageId{SegmentId: segID, EntryId: 0}, "a1-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)
	for i := 0; i < entries; i++ {
		msg, readErr := readEntry(ctx, reader)
		require.NoError(t, readErr, "read %d failed (must failover to healthy replicas)", i)
		require.Equal(t, int64(i), msg.Id.EntryId)
	}
	readonlySeg, err := logHandle.GetExistsReadonlySegmentHandle(ctx, segID)
	require.NoError(t, err)
	require.NoError(t, readonlySeg.Compact(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Sealed, 20*time.Second)
}

// TestDataCorruptionService_Completed_AllReplicasDamagedRange covers A3 + skip-range
// recovery: a range of entries is damaged on every replica, so the earliest reader stalls at the
// first damaged entry. Declaring a skip range over exactly that range lets the reader move past it
// and continue with the next healthy entries. Compaction cannot seal the damaged segment.
func TestDataCorruptionService_Completed_AllReplicasDamagedRange(t *testing.T) {
	const (
		clusterSize = 3
		entries     = 100
		fromEntry   = 10
		toEntry     = 19
	)
	rootPath := t.TempDir()
	cfg := corruptionE2EConfig(t)

	cluster, cfg, _, seeds := utils.StartMiniClusterWithCfg(t, clusterSize, rootPath, cfg)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	defer cluster.StopMultiNodeCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	wpClient := newCorruptionClient(t, ctx, cfg, seeds)
	logName := fmt.Sprintf("corruption-completed-range-%d", time.Now().UnixMilli())
	require.NoError(t, wpClient.CreateLog(ctx, logName))

	logHandle, err := wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)
	defer logHandle.Close(context.Background())

	logWriter, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	segID := writeCorruptionSegment(t, ctx, logWriter, entries)
	segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, segHandle)
	quorumInfo, err := segHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	nodes := quorumNodeIndexes(t, cluster, quorumInfo.Nodes)

	require.NoError(t, logHandle.CompleteAllActiveSegmentIfExists(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Completed, 45*time.Second)
	require.NoError(t, logWriter.Close(ctx))

	// Damage the same entry range on every replica.
	for _, nodeIndex := range nodes {
		dataLogPath := corruptionDataLogPath(cluster, cfg, nodeIndex, logHandle.GetId(), segID)
		require.FileExists(t, dataLogPath)
		for e := int64(fromEntry); e <= int64(toEntry); e++ {
			corruptDataRecordAtEntry(t, dataLogPath, e)
		}
	}

	// Restart every replica so the damaged data is actually picked up from disk.
	for _, nodeIndex := range nodes {
		_, err = cluster.LeaveNodeWithIndex(t, nodeIndex)
		require.NoError(t, err)
		restartCorruptionNode(t, cluster, nodeIndex)
	}

	readonlySeg, err := logHandle.GetExistsReadonlySegmentHandle(ctx, segID)
	require.NoError(t, err)
	require.Error(t, readonlySeg.Compact(ctx), "all damaged replicas must prevent compaction")
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Completed, 5*time.Second)

	reader, err := logHandle.OpenLogReader(ctx, &log.LogMessageId{SegmentId: segID, EntryId: 0}, "a3-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Read up to the damaged range without a problem.
	for i := int64(0); i < fromEntry; i++ {
		msg, readErr := readEntry(ctx, reader)
		require.NoError(t, readErr, "read %d before the damaged range failed", i)
		require.Equal(t, i, msg.Id.EntryId)
	}

	// The damaged entry stalls the reader.
	_, stallErr := readEntry(ctx, reader)
	require.Error(t, stallErr, "reader must stall on the damaged entry")
	require.True(t, isDeadline(stallErr), "stall must be a deadline, got %v", stallErr)

	// Keep ReadNext running across two report intervals: the first records the stalled
	// position, and the second refreshes skip ranges for that unchanged position.
	declareSkipRange(t, ctx, wpClient, logHandle.GetId(), segID, fromEntry, toEntry)
	skipCtx, cancelSkip := context.WithTimeout(ctx, 90*time.Second)
	defer cancelSkip()
	var firstAfter *log.LogMessage
	var readErr error
	for skipCtx.Err() == nil {
		firstAfter, readErr = readEntry(skipCtx, reader)
		if !isDeadline(readErr) {
			break
		}
	}
	require.NoError(t, readErr, "reader did not move past the damaged range after skip")
	require.Equal(t, int64(toEntry+1), firstAfter.Id.EntryId, "first entry after skip must be the one after the range")
	for i := int64(toEntry + 2); i < entries; i++ {
		msg, readErr := readEntry(ctx, reader)
		require.NoError(t, readErr, "read %d after skip failed", i)
		require.Equal(t, i, msg.Id.EntryId)
	}
}

func isDeadline(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded
}

// truncateDataLogAfterEntry truncates a data.log right after the data record holding the given
// entry, so a full-scan recovery stops at the following entry. This models a torn write where
// everything past a point is incomplete, rather than a CRC-damaged block.
func truncateDataLogAfterEntry(t *testing.T, dataLogPath string, lastGoodEntry int64) {
	t.Helper()
	data, err := os.ReadFile(dataLogPath)
	require.NoError(t, err)
	offsets := dataRecordPayloadOffsets(t, dataLogPath)
	require.Less(t, lastGoodEntry, int64(len(offsets)), "entry %d out of range", lastGoodEntry)

	require.GreaterOrEqual(t, lastGoodEntry, int64(0))
	// The length lives in the fixed header immediately before this payload.
	payloadStart := offsets[lastGoodEntry]
	headerStart := payloadStart - int64(codec.RecordHeaderSize)
	payloadLength := int64(binary.LittleEndian.Uint32(data[headerStart+5 : headerStart+9]))
	cutoff := payloadStart + payloadLength
	require.NoError(t, os.Truncate(dataLogPath, cutoff))
}

// TestDataCorruptionService_Active_AllReplicasTruncated covers B3: an Active segment
// whose three replicas are all truncated at the same point. Restart + fence re-derives the LAC at
// the truncation point, so the reader reads only up to that point and then reaches EOF cleanly --
// no stall, no skip range needed, and compaction seals at the shorter LAC.
func TestDataCorruptionService_Active_AllReplicasTruncated(t *testing.T) {
	const (
		clusterSize = 3
		entries     = 100
		lastGood    = 49
	)
	rootPath := t.TempDir()
	cfg := corruptionE2EConfig(t)

	cluster, cfg, _, seeds := utils.StartMiniClusterWithCfg(t, clusterSize, rootPath, cfg)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	defer cluster.StopMultiNodeCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	wpClient := newCorruptionClient(t, ctx, cfg, seeds)
	logName := fmt.Sprintf("corruption-active-all-%d", time.Now().UnixMilli())
	require.NoError(t, wpClient.CreateLog(ctx, logName))

	logHandle, err := wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)
	defer logHandle.Close(context.Background())

	logWriter, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	segID := writeCorruptionSegment(t, ctx, logWriter, entries)
	segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, segHandle)
	quorumInfo, err := segHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	nodes := quorumNodeIndexes(t, cluster, quorumInfo.Nodes)

	// Truncate every replica at the same point.
	for _, nodeIndex := range nodes {
		dataLogPath := corruptionDataLogPath(cluster, cfg, nodeIndex, logHandle.GetId(), segID)
		require.FileExists(t, dataLogPath)
		truncateDataLogAfterEntry(t, dataLogPath, lastGood)
	}

	// Restart all replicas.
	for _, nodeIndex := range nodes {
		_, err = cluster.LeaveNodeWithIndex(t, nodeIndex)
		require.NoError(t, err)
		restartCorruptionNode(t, cluster, nodeIndex)
	}

	// Re-open the writer to trigger fenceAllActiveSegments; LAC resolves to lastGood.
	// Take over through a fresh handle before closing the original writer:
	// a normal close would finalize the segment and bypass recovery fencing.
	previousWriter := logWriter
	defer previousWriter.Close(context.Background())
	logHandle, err = wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)
	defer logHandle.Close(context.Background())
	logWriter, err = logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)
	defer logWriter.Close(ctx)

	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Completed, 45*time.Second)

	// Reader sees exactly 0..lastGood, then reaches EOF (segment is complete at the shorter LAC).
	reader, err := logHandle.OpenLogReader(ctx, &log.LogMessageId{SegmentId: segID, EntryId: 0}, "b3-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)
	for i := int64(0); i <= lastGood; i++ {
		msg, readErr := readEntry(ctx, reader)
		require.NoError(t, readErr, "read %d failed", i)
		require.Equal(t, i, msg.Id.EntryId)
	}
	// A following segment proves the reader crosses the shortened segment's EOF
	// instead of exposing its discarded tail or stalling there.
	next := logWriter.Write(ctx, &log.WriteMessage{Payload: []byte("after-shortened-segment")})
	require.NoError(t, next.Err)
	require.Greater(t, next.LogMessageId.SegmentId, segID)
	msg, readErr := readEntry(ctx, reader)
	require.NoError(t, readErr)
	require.Equal(t, next.LogMessageId, msg.Id)
	require.Equal(t, []byte("after-shortened-segment"), msg.Payload)

	readonlySeg, err := logHandle.GetExistsReadonlySegmentHandle(ctx, segID)
	require.NoError(t, err)
	require.NoError(t, readonlySeg.Compact(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Sealed, 20*time.Second)
}

// TestDataCorruptionService_Active_TwoReplicasTruncated covers B2: two Active replicas
// truncated at the same point while the third is healthy. Fence re-derives the LAC at the
// truncation point (the next-smallest reported lastEntryId), so the reader sees only up to that
// point and then EOF -- no stall, no skip range.
func TestDataCorruptionService_Active_TwoReplicasTruncated(t *testing.T) {
	const (
		clusterSize = 3
		entries     = 100
		lastGood    = 49
	)
	rootPath := t.TempDir()
	cfg := corruptionE2EConfig(t)

	cluster, cfg, _, seeds := utils.StartMiniClusterWithCfg(t, clusterSize, rootPath, cfg)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	defer cluster.StopMultiNodeCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	wpClient := newCorruptionClient(t, ctx, cfg, seeds)
	logName := fmt.Sprintf("corruption-active-two-%d", time.Now().UnixMilli())
	require.NoError(t, wpClient.CreateLog(ctx, logName))

	logHandle, err := wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)
	defer logHandle.Close(context.Background())

	logWriter, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	segID := writeCorruptionSegment(t, ctx, logWriter, entries)
	segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, segHandle)
	quorumInfo, err := segHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	nodes := quorumNodeIndexes(t, cluster, quorumInfo.Nodes)

	// Truncate two of the three replicas at the same point.
	for _, nodeIndex := range nodes[:2] {
		dataLogPath := corruptionDataLogPath(cluster, cfg, nodeIndex, logHandle.GetId(), segID)
		require.FileExists(t, dataLogPath)
		truncateDataLogAfterEntry(t, dataLogPath, lastGood)
	}

	// Restart the two damaged replicas.
	for _, nodeIndex := range nodes[:2] {
		_, err = cluster.LeaveNodeWithIndex(t, nodeIndex)
		require.NoError(t, err)
		restartCorruptionNode(t, cluster, nodeIndex)
	}

	// Take over through a fresh handle before closing the original writer:
	// a normal close would finalize the segment and bypass recovery fencing.
	previousWriter := logWriter
	defer previousWriter.Close(context.Background())
	logHandle, err = wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)
	defer logHandle.Close(context.Background())
	logWriter, err = logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)
	defer logWriter.Close(ctx)

	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Completed, 45*time.Second)

	reader, err := logHandle.OpenLogReader(ctx, &log.LogMessageId{SegmentId: segID, EntryId: 0}, "b2-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)
	for i := int64(0); i <= lastGood; i++ {
		msg, readErr := readEntry(ctx, reader)
		require.NoError(t, readErr, "read %d failed", i)
		require.Equal(t, i, msg.Id.EntryId)
	}
	// A following segment proves the reader crosses the shortened segment's EOF
	// instead of exposing its discarded tail or stalling there.
	next := logWriter.Write(ctx, &log.WriteMessage{Payload: []byte("after-shortened-segment")})
	require.NoError(t, next.Err)
	require.Greater(t, next.LogMessageId.SegmentId, segID)
	msg, readErr := readEntry(ctx, reader)
	require.NoError(t, readErr)
	require.Equal(t, next.LogMessageId, msg.Id)
	require.Equal(t, []byte("after-shortened-segment"), msg.Payload)

	readonlySeg, err := logHandle.GetExistsReadonlySegmentHandle(ctx, segID)
	require.NoError(t, err)
	require.NoError(t, readonlySeg.Compact(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Sealed, 20*time.Second)
}

// TestDataCorruptionService_TruncateReclaimsDamagedSegment covers truncate as the
// lifecycle exit for a damaged Completed segment: truncating past the damaged range advances the
// log's truncation point, and the log remains usable afterward.
func TestDataCorruptionService_TruncateReclaimsDamagedSegment(t *testing.T) {
	const (
		clusterSize = 3
		entries     = 20
	)
	rootPath := t.TempDir()
	cfg := corruptionE2EConfig(t)

	cluster, cfg, _, seeds := utils.StartMiniClusterWithCfg(t, clusterSize, rootPath, cfg)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	defer cluster.StopMultiNodeCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	wpClient := newCorruptionClient(t, ctx, cfg, seeds)
	logName := fmt.Sprintf("corruption-truncate-%d", time.Now().UnixMilli())
	require.NoError(t, wpClient.CreateLog(ctx, logName))

	logHandle, err := wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)
	defer logHandle.Close(context.Background())

	logWriter, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	segID := writeCorruptionSegment(t, ctx, logWriter, entries)
	segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, segHandle)
	quorumInfo, err := segHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	nodes := quorumNodeIndexes(t, cluster, quorumInfo.Nodes)

	require.NoError(t, logHandle.CompleteAllActiveSegmentIfExists(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Completed, 45*time.Second)
	require.NoError(t, logWriter.Close(ctx))

	// Damage all replicas so the segment cannot be read or sealed.
	for _, nodeIndex := range nodes {
		dataLogPath := corruptionDataLogPath(cluster, cfg, nodeIndex, logHandle.GetId(), segID)
		require.FileExists(t, dataLogPath)
		corruptDataRecordAtEntry(t, dataLogPath, 0)
	}
	for _, nodeIndex := range nodes {
		_, err = cluster.LeaveNodeWithIndex(t, nodeIndex)
		require.NoError(t, err)
		restartCorruptionNode(t, cluster, nodeIndex)
	}

	// Truncate past the damaged range: Completed segments are valid truncate targets.
	truncatePoint := &log.LogMessageId{SegmentId: segID, EntryId: entries - 1}
	require.NoError(t, logHandle.Truncate(ctx, truncatePoint))

	truncated, err := logHandle.GetTruncatedRecordId(ctx)
	require.NoError(t, err)
	require.Equal(t, segID, truncated.SegmentId)
	require.Equal(t, int64(entries-1), truncated.EntryId)
	// After truncation, new writes and reads must still work in a new segment.
	logWriter, err = logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)
	defer logWriter.Close(ctx)
	next := logWriter.Write(ctx, &log.WriteMessage{Payload: []byte("after-truncate")})
	require.NoError(t, next.Err)
	require.Greater(t, next.LogMessageId.SegmentId, segID)
	reader, err := logHandle.OpenLogReader(ctx, next.LogMessageId, "truncate-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)
	msg, readErr := readEntry(ctx, reader)
	require.NoError(t, readErr)
	require.Equal(t, next.LogMessageId, msg.Id)
	require.Equal(t, []byte("after-truncate"), msg.Payload)
}

// TestDataCorruptionService_Completed_TwoReplicasDamaged covers A2a: a Completed segment
// with two damaged replicas and one healthy survivor that still covers the LAC. The quorum read
// only needs any replica to serve an entry, so a single healthy survivor still delivers every
// entry; compaction seals on that survivor.
func TestDataCorruptionService_Completed_TwoReplicasDamaged(t *testing.T) {
	const (
		clusterSize = 3
		entries     = 100
	)
	rootPath := t.TempDir()
	cfg := corruptionE2EConfig(t)

	cluster, cfg, _, seeds := utils.StartMiniClusterWithCfg(t, clusterSize, rootPath, cfg)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	defer cluster.StopMultiNodeCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	wpClient := newCorruptionClient(t, ctx, cfg, seeds)
	logName := fmt.Sprintf("corruption-completed-two-%d", time.Now().UnixMilli())
	require.NoError(t, wpClient.CreateLog(ctx, logName))

	logHandle, err := wpClient.OpenLog(ctx, logName)
	require.NoError(t, err)
	defer logHandle.Close(context.Background())

	logWriter, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err)

	segID := writeCorruptionSegment(t, ctx, logWriter, entries)
	segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, segHandle)
	quorumInfo, err := segHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	nodes := quorumNodeIndexes(t, cluster, quorumInfo.Nodes)

	require.NoError(t, logHandle.CompleteAllActiveSegmentIfExists(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Completed, 45*time.Second)
	require.NoError(t, logWriter.Close(ctx))

	// Damage two replicas; the third stays intact and still covers the full LAC.
	for _, nodeIndex := range nodes[:2] {
		dataLogPath := corruptionDataLogPath(cluster, cfg, nodeIndex, logHandle.GetId(), segID)
		require.FileExists(t, dataLogPath)
		corruptDataRecordAtEntry(t, dataLogPath, 0)
	}
	for _, nodeIndex := range nodes[:2] {
		_, err = cluster.LeaveNodeWithIndex(t, nodeIndex)
		require.NoError(t, err)
		restartCorruptionNode(t, cluster, nodeIndex)
	}

	// A single healthy survivor still serves every entry.
	reader, err := logHandle.OpenLogReader(ctx, &log.LogMessageId{SegmentId: segID, EntryId: 0}, "a2a-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)
	for i := 0; i < entries; i++ {
		msg, readErr := readEntry(ctx, reader)
		require.NoError(t, readErr, "read %d failed (one healthy survivor must serve it)", i)
		require.Equal(t, int64(i), msg.Id.EntryId)
	}
	readonlySeg, err := logHandle.GetExistsReadonlySegmentHandle(ctx, segID)
	require.NoError(t, err)
	require.NoError(t, readonlySeg.Compact(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Sealed, 20*time.Second)
}

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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

// corruptDataRecordAtEntry corrupts the payload of the data record holding the given entry id, so
// the block CRC no longer matches. This models a damaged block while keeping the record structure
// parseable -- the kind of damage a reader's CRC check and compaction's CRC check catch, rather
// than a truncation.
func corruptDataRecordAtEntry(t *testing.T, dataLogPath string, entryID int64) {
	t.Helper()
	positions := parseFileRecordPositions(t, dataLogPath)
	var target *RecordPosition
	for i := range positions {
		if positions[i].RecordType == "data" && positions[i].EntryID == entryID {
			p := positions[i]
			target = &p
			break
		}
	}
	require.NotNil(t, target, "entry %d not found in %s", entryID, dataLogPath)

	data, err := os.ReadFile(dataLogPath)
	require.NoError(t, err)
	// The payload follows the fixed record header; flip its first byte to break the block CRC.
	payloadStart := target.StartPos + codec.RecordHeaderSize
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

// TestStagedStorageService_Corruption_Active_OneReplicaDamaged covers B1: an Active segment where
// one replica's local data is truncated before finalize. Restart + fence re-derives the LAC from
// the two healthy replicas, so no data is lost, no reader stalls, and compaction succeeds.
func TestDataCorruptionService_Active_OneReplicaDamaged(t *testing.T) {
	const (
		clusterSize = 3
		entries     = 100
	)
	rootPath := t.TempDir()
	cfg := corruptionE2EConfig(t)

	cluster, cfg, gossipSeeds, seeds := utils.StartMiniClusterWithCfg(t, clusterSize, rootPath, cfg)
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
	_, err = cluster.RestartNode(t, target, gossipSeeds)
	require.NoError(t, err)

	// Re-open the writer to trigger fenceAllActiveSegments; LAC resolves from the healthy replicas.
	require.NoError(t, logWriter.Close(ctx))
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
}

// TestStagedStorageService_Corruption_Completed_OneReplicaDamaged covers A1: a Completed segment
// with one damaged replica. The reader fails over to the two healthy replicas and reads every
// entry; compaction seals on a healthy replica.
func TestDataCorruptionService_Completed_OneReplicaDamaged(t *testing.T) {
	const (
		clusterSize = 3
		entries     = 100
	)
	rootPath := t.TempDir()
	cfg := corruptionE2EConfig(t)

	cluster, cfg, gossipSeeds, seeds := utils.StartMiniClusterWithCfg(t, clusterSize, rootPath, cfg)
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
	require.NoError(t, logHandle.CompleteAllActiveSegmentIfExists(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Completed, 45*time.Second)
	require.NoError(t, logWriter.Close(ctx))

	segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, segHandle)
	quorumInfo, err := segHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	nodes := quorumNodeIndexes(t, cluster, quorumInfo.Nodes)

	// Damage one replica's local data.log.
	target := nodes[0]
	dataLogPath := corruptionDataLogPath(cluster, cfg, target, logHandle.GetId(), segID)
	require.FileExists(t, dataLogPath)
	corruptDataRecordAtEntry(t, dataLogPath, 0)

	// Restart the damaged node.
	_, err = cluster.LeaveNodeWithIndex(t, target)
	require.NoError(t, err)
	_, err = cluster.RestartNode(t, target, gossipSeeds)
	require.NoError(t, err)

	// The two healthy replicas still cover the full range: the reader sees every entry.
	reader, err := logHandle.OpenLogReader(ctx, &log.LogMessageId{SegmentId: segID, EntryId: 0}, "a1-reader")
	require.NoError(t, err)
	defer reader.Close(ctx)
	for i := 0; i < entries; i++ {
		msg, readErr := readEntry(ctx, reader)
		require.NoError(t, readErr, "read %d failed (must failover to healthy replicas)", i)
		require.Equal(t, int64(i), msg.Id.EntryId)
	}
}

// TestStagedStorageService_Corruption_Completed_AllReplicasDamagedRange covers A3 + skip-range
// recovery: a range of entries is damaged on every replica, so the earliest reader stalls at the
// first damaged entry. Declaring a skip range over exactly that range lets the reader move past it
// and continue with the next healthy entries. Truncate is verified to keep the reader parked
// rather than being the un-stall tool.
func TestDataCorruptionService_Completed_AllReplicasDamagedRange(t *testing.T) {
	const (
		clusterSize = 3
		entries     = 100
		fromEntry   = 10
		toEntry     = 19
	)
	rootPath := t.TempDir()
	cfg := corruptionE2EConfig(t)

	cluster, cfg, gossipSeeds, seeds := utils.StartMiniClusterWithCfg(t, clusterSize, rootPath, cfg)
	cfg.Woodpecker.Client.Quorum.SetBufferPoolSeeds(0, seeds)
	defer cluster.StopMultiNodeCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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
	require.NoError(t, logHandle.CompleteAllActiveSegmentIfExists(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Completed, 45*time.Second)
	require.NoError(t, logWriter.Close(ctx))

	segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, segHandle)
	quorumInfo, err := segHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	nodes := quorumNodeIndexes(t, cluster, quorumInfo.Nodes)

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
		_, err = cluster.RestartNode(t, nodeIndex, gossipSeeds)
		require.NoError(t, err)
	}

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

	// Truncate does not un-stall a parked reader; a skip range is the recovery tool.
	declareSkipRange(t, ctx, wpClient, logHandle.GetId(), segID, fromEntry, toEntry)

	// The reader must now move past the damaged range and keep reading the healthy tail.
	for i := toEntry + 1; i < entries; i++ {
		msg, readErr := readEntry(ctx, reader)
		require.NoError(t, readErr, "read %d after skip failed", i)
		require.Equal(t, i, msg.Id.EntryId)
	}
}

func isDeadline(err error) bool {
	return err != nil && errors.Is(err, context.DeadlineExceeded)
}

// truncateDataLogAfterEntry truncates a data.log right after the data record holding the given
// entry, so a full-scan recovery stops at the following entry. This models a torn write where
// everything past a point is incomplete, rather than a CRC-damaged block.
func truncateDataLogAfterEntry(t *testing.T, dataLogPath string, lastGoodEntry int64) {
	t.Helper()
	positions := parseFileRecordPositions(t, dataLogPath)
	var cutoff int64 = -1
	for i := range positions {
		if positions[i].RecordType == "data" && positions[i].EntryID == lastGoodEntry {
			cutoff = positions[i].EndPos
		}
	}
	require.NotEqual(t, int64(-1), cutoff, "entry %d not found in %s", lastGoodEntry, dataLogPath)
	require.NoError(t, os.Truncate(dataLogPath, cutoff))
}

// TestStagedStorageService_Corruption_Active_AllReplicasTruncated covers B3: an Active segment
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

	cluster, cfg, gossipSeeds, seeds := utils.StartMiniClusterWithCfg(t, clusterSize, rootPath, cfg)
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
		_, err = cluster.RestartNode(t, nodeIndex, gossipSeeds)
		require.NoError(t, err)
	}

	// Re-open the writer to trigger fenceAllActiveSegments; LAC resolves to lastGood.
	require.NoError(t, logWriter.Close(ctx))
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
}

// TestStagedStorageService_Corruption_Active_TwoReplicasTruncated covers B2: two Active replicas
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

	cluster, cfg, gossipSeeds, seeds := utils.StartMiniClusterWithCfg(t, clusterSize, rootPath, cfg)
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
		_, err = cluster.RestartNode(t, nodeIndex, gossipSeeds)
		require.NoError(t, err)
	}

	require.NoError(t, logWriter.Close(ctx))
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
}

// TestStagedStorageService_Corruption_TruncateReclaimsDamagedSegment covers truncate as the
// lifecycle exit for a damaged Completed segment: truncating past the damaged range advances the
// log's truncation point, and the log remains usable afterward.
func TestDataCorruptionService_TruncateReclaimsDamagedSegment(t *testing.T) {
	const (
		clusterSize = 3
		entries     = 20
	)
	rootPath := t.TempDir()
	cfg := corruptionE2EConfig(t)

	cluster, cfg, gossipSeeds, seeds := utils.StartMiniClusterWithCfg(t, clusterSize, rootPath, cfg)
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
	require.NoError(t, logHandle.CompleteAllActiveSegmentIfExists(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Completed, 45*time.Second)
	require.NoError(t, logWriter.Close(ctx))

	segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, segHandle)
	quorumInfo, err := segHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	nodes := quorumNodeIndexes(t, cluster, quorumInfo.Nodes)

	// Damage all replicas so the segment cannot be read or sealed.
	for _, nodeIndex := range nodes {
		dataLogPath := corruptionDataLogPath(cluster, cfg, nodeIndex, logHandle.GetId(), segID)
		require.FileExists(t, dataLogPath)
		corruptDataRecordAtEntry(t, dataLogPath, 0)
	}
	for _, nodeIndex := range nodes {
		_, err = cluster.LeaveNodeWithIndex(t, nodeIndex)
		require.NoError(t, err)
		_, err = cluster.RestartNode(t, nodeIndex, gossipSeeds)
		require.NoError(t, err)
	}

	// Truncate past the damaged range: Completed segments are valid truncate targets.
	truncatePoint := &log.LogMessageId{SegmentId: segID, EntryId: entries - 1}
	require.NoError(t, logHandle.Truncate(ctx, truncatePoint))

	truncated, err := logHandle.GetTruncatedRecordId(ctx)
	require.NoError(t, err)
	require.Equal(t, segID, truncated.SegmentId)
	require.Equal(t, int64(entries-1), truncated.EntryId)
}

// TestStagedStorageService_Corruption_Completed_TwoReplicasDamaged covers A2a: a Completed segment
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

	cluster, cfg, gossipSeeds, seeds := utils.StartMiniClusterWithCfg(t, clusterSize, rootPath, cfg)
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
	require.NoError(t, logHandle.CompleteAllActiveSegmentIfExists(ctx))
	requireCompactedCleanupSegmentState(t, ctx, logHandle, segID, proto.SegmentState_Completed, 45*time.Second)
	require.NoError(t, logWriter.Close(ctx))

	segHandle := logHandle.GetCurrentWritableSegmentHandle(ctx)
	require.NotNil(t, segHandle)
	quorumInfo, err := segHandle.GetQuorumInfo(ctx)
	require.NoError(t, err)
	nodes := quorumNodeIndexes(t, cluster, quorumInfo.Nodes)

	// Damage two replicas; the third stays intact and still covers the full LAC.
	for _, nodeIndex := range nodes[:2] {
		dataLogPath := corruptionDataLogPath(cluster, cfg, nodeIndex, logHandle.GetId(), segID)
		require.FileExists(t, dataLogPath)
		corruptDataRecordAtEntry(t, dataLogPath, 0)
	}
	for _, nodeIndex := range nodes[:2] {
		_, err = cluster.LeaveNodeWithIndex(t, nodeIndex)
		require.NoError(t, err)
		_, err = cluster.RestartNode(t, nodeIndex, gossipSeeds)
		require.NoError(t, err)
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
}

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

package woodpecker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	mocks_meta "github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	mocks_logstore_client "github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

// testCfg returns a minimal config with bucket/root for delete tests.
// testDeleteCfg builds a local-storage config: these tests exercise the fencing and metadata
// steps, and local mode is the one that legitimately has no object storage to clean.
func testDeleteCfg() *config.Configuration {
	cfg := &config.Configuration{
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-root",
		},
	}
	cfg.Woodpecker.Storage.Type = "local"
	return cfg
}

// buildLogMeta builds a meta.LogMeta with the given logId.
func buildLogMeta(logId int64) *meta.LogMeta {
	return &meta.LogMeta{
		Metadata: &proto.LogMeta{LogId: logId},
	}
}

// buildSegmentMeta builds a meta.SegmentMeta with the given quorum nodes.
func buildSegmentMeta(nodes []string) *meta.SegmentMeta {
	return &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			Quorum: &proto.QuorumInfo{Nodes: nodes},
		},
	}
}

// ============================================================
// Tests for deleteLogUnsafe
// ============================================================

// TestDeleteLog_MarksQuorumNodesThenDeletesMetadata verifies that deleteLogUnsafe
// collects the union of quorum nodes from all segments, calls MarkLogDeleted on
// each distinct node exactly once, and then calls DeleteLogMetadata (force=false).
func TestDeleteLog_MarksQuorumNodesThenDeletesMetadata(t *testing.T) {
	ctx := context.Background()
	cfg := testDeleteCfg()

	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// GetLogMeta returns logId=5
	mockMeta.EXPECT().GetLogMeta(mock.Anything, "foo").
		Return(buildLogMeta(5), nil).Once()

	// GetAllSegmentMetadata returns 2 segments with quorum nodes {n1,n2} and {n2,n3}
	segs := map[int64]*meta.SegmentMeta{
		0: buildSegmentMeta([]string{"n1", "n2"}),
		1: buildSegmentMeta([]string{"n2", "n3"}),
	}
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "foo").
		Return(segs, nil).Once()

	// One mock client per node; pool hands out the same client regardless of target.
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, mock.AnythingOfType("string")).
		Return(mockClient, nil).Times(3)

	// MarkLogDeleted called 3× (n1, n2, n3 — each distinct node once)
	mockClient.EXPECT().MarkLogDeleted(mock.Anything, "test-bucket", "test-root", int64(5), false).
		Return(false, nil).Times(3)

	// DeleteLogMetadata called once with force=false
	mockMeta.EXPECT().DeleteLogMetadata(mock.Anything, "foo", false).
		Return(nil).Once()

	_, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, nil, "foo", false, deleteOptions{})
	assert.NoError(t, err)
}

// TestDeleteLog_NodeMarkFailure_DoesNotDeleteMetadata verifies that if any node's
// MarkLogDeleted returns an error, deleteLogUnsafe returns that error and
// DeleteLogMetadata is never called.
func TestDeleteLog_NodeMarkFailure_DoesNotDeleteMetadata(t *testing.T) {
	ctx := context.Background()
	cfg := testDeleteCfg()

	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockMeta.EXPECT().GetLogMeta(mock.Anything, "foo").
		Return(buildLogMeta(7), nil).Once()

	segs := map[int64]*meta.SegmentMeta{
		0: buildSegmentMeta([]string{"n1"}),
	}
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "foo").
		Return(segs, nil).Once()

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "n1").
		Return(mockClient, nil).Once()

	markErr := errors.New("node unreachable")
	mockClient.EXPECT().MarkLogDeleted(mock.Anything, "test-bucket", "test-root", int64(7), false).
		Return(false, markErr).Once()

	// DeleteLogMetadata must NOT be called — enforced by testify (no EXPECT set).

	_, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, nil, "foo", false, deleteOptions{})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "node unreachable")
}

// TestDeleteLog_AlreadyGone_Idempotent verifies that when GetLogMeta returns an
// ErrMetadataRead-class error (log not found), deleteLogUnsafe returns nil without
// calling MarkLogDeleted or DeleteLogMetadata.
func TestDeleteLog_AlreadyGone_Idempotent(t *testing.T) {
	ctx := context.Background()
	cfg := testDeleteCfg()

	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	notFoundErr := werr.ErrMetadataRead.WithCauseErrMsg("log not found: foo")
	mockMeta.EXPECT().GetLogMeta(mock.Anything, "foo").
		Return(nil, notFoundErr).Once()

	// No pool or client calls expected — enforced by testify.

	_, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, nil, "foo", false, deleteOptions{})
	assert.NoError(t, err)
}

// TestDeleteLog_NoSegments_JustDeletesMetadata verifies that when a log has no
// segments, no MarkLogDeleted is called, but DeleteLogMetadata is still called.
func TestDeleteLog_NoSegments_JustDeletesMetadata(t *testing.T) {
	ctx := context.Background()
	cfg := testDeleteCfg()

	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockMeta.EXPECT().GetLogMeta(mock.Anything, "empty-log").
		Return(buildLogMeta(99), nil).Once()

	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "empty-log").
		Return(map[int64]*meta.SegmentMeta{}, nil).Once()

	// No quorum is recorded, but this is an embedded deployment: the in-process logstore may
	// still hold an open processor for the log, so it is marked deleted before anything is removed.
	// Skipping that would let a concurrent compaction write a fresh object after cleanup.
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "").Return(mockClient, nil).Once()
	mockClient.EXPECT().MarkLogDeleted(mock.Anything, "test-bucket", "test-root", int64(99), false).
		Return(false, nil).Once()

	mockMeta.EXPECT().DeleteLogMetadata(mock.Anything, "empty-log", false).
		Return(nil).Once()

	_, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, nil, "empty-log", false, deleteOptions{})
	assert.NoError(t, err)
}

// TestDeleteLog_NoSegments_ServiceMode_NothingToMark covers the other half: in service mode
// an empty quorum means every segment was already truncated away and no node is serving the
// log, so there is nothing to contact.
func TestDeleteLog_NoSegments_ServiceMode_NothingToMark(t *testing.T) {
	ctx := context.Background()
	cfg := testDeleteCfg()
	cfg.Woodpecker.Storage.Type = "service"

	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockMeta.EXPECT().GetLogMeta(mock.Anything, "empty-log").
		Return(buildLogMeta(99), nil).Once()
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "empty-log").
		Return(map[int64]*meta.SegmentMeta{}, nil).Once()
	mockMeta.EXPECT().DeleteLogMetadata(mock.Anything, "empty-log", false).
		Return(nil).Once()

	// Service mode still has object storage to sweep, even with no segment metadata left —
	// that is exactly the case where orphans hide. An empty prefix is a clean no-op.
	storage := mocks_objectstorage.NewObjectStorage(t)
	storage.EXPECT().
		WalkWithObjects(mock.Anything, "test-bucket", "test-root/99/", true, mock.Anything, mock.Anything, mock.Anything).
		Return(nil).Once()

	// No pool/client calls expected.
	_, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, storage, "empty-log", false, deleteOptions{})
	assert.NoError(t, err)
}

// TestDeleteLog_ObjectStorageUnavailable_KeepsMetadata pins the refusal that keeps a failed
// object cleanup from stranding objects: without a storage client the metadata that would let
// anyone find them later must not be deleted.
func TestDeleteLog_ObjectStorageUnavailable_KeepsMetadata(t *testing.T) {
	ctx := context.Background()
	cfg := testDeleteCfg()
	cfg.Woodpecker.Storage.Type = "service"

	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockMeta.EXPECT().GetLogMeta(mock.Anything, "foo").Return(buildLogMeta(11), nil).Once()
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "foo").
		Return(map[int64]*meta.SegmentMeta{}, nil).Once()
	// DeleteLogMetadata must NOT be called.

	_, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, nil, "foo", false, deleteOptions{})
	assert.Error(t, err)
}

// ============================================================
// Tests for deleteAllLogsUnsafe
// ============================================================

// TestDeleteAllLogs_DeletesEachLog verifies that deleteAllLogsUnsafe lists all
// logs and calls the per-log delete path (including DeleteLogMetadata) for each.
func TestDeleteAllLogs_DeletesEachLog(t *testing.T) {
	ctx := context.Background()
	cfg := testDeleteCfg()

	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockMeta.EXPECT().ListLogs(mock.Anything).
		Return([]string{"a", "b"}, nil).Once()

	// Embedded deployment: each log's in-process logstore is marked before its metadata goes.
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "").Return(mockClient, nil).Twice()
	mockClient.EXPECT().MarkLogDeleted(mock.Anything, "test-bucket", "test-root", int64(1), false).
		Return(false, nil).Once()
	mockClient.EXPECT().MarkLogDeleted(mock.Anything, "test-bucket", "test-root", int64(2), false).
		Return(false, nil).Once()

	// Log "a": logId=1, no segments
	mockMeta.EXPECT().GetLogMeta(mock.Anything, "a").
		Return(buildLogMeta(1), nil).Once()
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "a").
		Return(map[int64]*meta.SegmentMeta{}, nil).Once()
	mockMeta.EXPECT().DeleteLogMetadata(mock.Anything, "a", false).
		Return(nil).Once()

	// Log "b": logId=2, no segments
	mockMeta.EXPECT().GetLogMeta(mock.Anything, "b").
		Return(buildLogMeta(2), nil).Once()
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "b").
		Return(map[int64]*meta.SegmentMeta{}, nil).Once()
	mockMeta.EXPECT().DeleteLogMetadata(mock.Anything, "b", false).
		Return(nil).Once()

	_, err := deleteAllLogsUnsafe(ctx, mockMeta, mockPool, cfg, nil, false, deleteOptions{})
	assert.NoError(t, err)
}

// unavailableErr is what grpc-go produces for every transport-layer failure, and therefore
// what werr.IsTransportError recognises. Using the real shape matters: the retry predicate and
// the skip predicate both key off it, so a hand-rolled error would exercise neither.
func unavailableErr() error {
	return status.Error(codes.Unavailable, "connection refused")
}

// shrinkMarkBackoff makes the retry loop finish instantly. The production values are seconds
// apart on purpose; tests only care about the number of attempts.
func shrinkMarkBackoff(t *testing.T) {
	t.Helper()
	sleep, maxSleep := markRetrySleep, markMaxSleep
	markRetrySleep, markMaxSleep = time.Millisecond, time.Millisecond
	t.Cleanup(func() { markRetrySleep, markMaxSleep = sleep, maxSleep })
}

// TestDeleteMarkRetriesTransientUnavailability is the common case this exists for: a pod
// mid-rolling-restart or a DNS record that has not re-resolved yet. The pool evicts its
// cached connection on each transport failure so the next attempt re-dials — without a retry
// that recovery mechanism never gets a turn, and one blip aborts the whole run.
func TestDeleteMarkRetriesTransientUnavailability(t *testing.T) {
	shrinkMarkBackoff(t)
	ctx := context.Background()
	cfg := testDeleteCfg()

	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockMeta.EXPECT().GetLogMeta(mock.Anything, "foo").Return(buildLogMeta(7), nil).Once()
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "foo").
		Return(map[int64]*meta.SegmentMeta{0: buildSegmentMeta([]string{"n1"})}, nil).Once()

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "n1").Return(mockClient, nil).Times(3)
	// Unavailable twice, then the node comes back.
	mockClient.EXPECT().MarkLogDeleted(mock.Anything, "test-bucket", "test-root", int64(7), true).
		Return(false, unavailableErr()).Twice()
	mockClient.EXPECT().MarkLogDeleted(mock.Anything, "test-bucket", "test-root", int64(7), true).
		Return(true, nil).Once()
	mockMeta.EXPECT().DeleteLogMetadata(mock.Anything, "foo", false).Return(nil).Once()

	stats, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, nil, "foo", true, deleteOptions{})
	require.NoError(t, err)
	assert.Equal(t, 1, stats.Logs)
	assert.Equal(t, 1, stats.NodesMarked)
	assert.Empty(t, stats.SkippedNodes, "the node answered in the end; nothing was skipped")
}

// TestDeleteMarkExhaustedFailsClosedByDefault pins the default. Without an explicit opt-in an
// unreachable node stops the delete, so the log keeps its objects AND its metadata and stays
// enumerable for a later retry.
func TestDeleteMarkExhaustedFailsClosedByDefault(t *testing.T) {
	shrinkMarkBackoff(t)
	ctx := context.Background()
	cfg := testDeleteCfg()

	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockMeta.EXPECT().GetLogMeta(mock.Anything, "foo").Return(buildLogMeta(7), nil).Once()
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "foo").
		Return(map[int64]*meta.SegmentMeta{0: buildSegmentMeta([]string{"n1"})}, nil).Once()

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "n1").Return(mockClient, nil).Times(int(markAttempts))
	mockClient.EXPECT().MarkLogDeleted(mock.Anything, "test-bucket", "test-root", int64(7), true).
		Return(false, unavailableErr()).Times(int(markAttempts))
	// DeleteLogMetadata is deliberately NOT expected: reaching it would be the bug.

	stats, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, nil, "foo", true, deleteOptions{})
	require.Error(t, err)
	assert.True(t, werr.IsTransportError(err), "the classification must survive the retry loop")
	assert.Zero(t, stats.Logs, "metadata must not be deleted when a node could not be marked")
	assert.Empty(t, stats.SkippedNodes)
}

// TestDeleteMarkExhaustedSkipsOnlyWhenAsked covers the opt-in, and what it must report. The
// skipped node keeps that log's staged data forever, so the operator needs the node and the
// log id to decide between "scrap hardware, leave it" and "reclaim the space by hand".
func TestDeleteMarkExhaustedSkipsOnlyWhenAsked(t *testing.T) {
	shrinkMarkBackoff(t)
	ctx := context.Background()
	cfg := testDeleteCfg()

	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockMeta.EXPECT().GetLogMeta(mock.Anything, "foo").Return(buildLogMeta(7), nil).Once()
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "foo").
		Return(map[int64]*meta.SegmentMeta{0: buildSegmentMeta([]string{"n1", "n2"})}, nil).Once()

	dead := mocks_logstore_client.NewLogStoreClient(t)
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "n1").Return(dead, nil).Times(int(markAttempts))
	dead.EXPECT().MarkLogDeleted(mock.Anything, "test-bucket", "test-root", int64(7), true).
		Return(false, unavailableErr()).Times(int(markAttempts))

	alive := mocks_logstore_client.NewLogStoreClient(t)
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "n2").Return(alive, nil).Once()
	alive.EXPECT().MarkLogDeleted(mock.Anything, "test-bucket", "test-root", int64(7), true).
		Return(true, nil).Once()

	mockMeta.EXPECT().DeleteLogMetadata(mock.Anything, "foo", false).Return(nil).Once()

	stats, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, nil, "foo", true,
		newDeleteOptions([]DeleteOption{WithSkipUnreachableNodes()}))
	require.NoError(t, err)
	assert.Equal(t, 1, stats.Logs)
	assert.Equal(t, 1, stats.NodesMarked, "only the reachable node accepted the mark")
	assert.Equal(t, map[string][]int64{"n1": {7}}, stats.SkippedNodes,
		"the operator needs the node and the log id to locate the residue")
}

// TestSkipDoesNotCoverALogicalRejection keeps the option honest to its name. A node that
// answers and refuses is reporting a real problem, not a connectivity one, and swallowing
// that would hide a genuine failure behind a flag meant for dead hardware.
func TestSkipDoesNotCoverALogicalRejection(t *testing.T) {
	shrinkMarkBackoff(t)
	ctx := context.Background()
	cfg := testDeleteCfg()

	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockMeta.EXPECT().GetLogMeta(mock.Anything, "foo").Return(buildLogMeta(7), nil).Once()
	mockMeta.EXPECT().GetAllSegmentMetadata(mock.Anything, "foo").
		Return(map[int64]*meta.SegmentMeta{0: buildSegmentMeta([]string{"n1"})}, nil).Once()

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "n1").Return(mockClient, nil).Once()
	// A reachable node refusing: not a transport error, so neither retried nor skipped.
	mockClient.EXPECT().MarkLogDeleted(mock.Anything, "test-bucket", "test-root", int64(7), true).
		Return(false, errors.New("log is being compacted")).Once()

	stats, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, nil, "foo", true,
		newDeleteOptions([]DeleteOption{WithSkipUnreachableNodes()}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "being compacted")
	assert.Zero(t, stats.Logs)
	assert.Empty(t, stats.SkippedNodes)
}

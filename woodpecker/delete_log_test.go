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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

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

	_, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, nil, "foo", false)
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

	_, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, nil, "foo", false)
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

	_, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, nil, "foo", false)
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
	// still hold an open processor for the log, so it is fenced before anything is removed.
	// Skipping that would let a concurrent compaction write a fresh object after cleanup.
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockPool.EXPECT().GetLogStoreClient(mock.Anything, "").Return(mockClient, nil).Once()
	mockClient.EXPECT().MarkLogDeleted(mock.Anything, "test-bucket", "test-root", int64(99), false).
		Return(false, nil).Once()

	mockMeta.EXPECT().DeleteLogMetadata(mock.Anything, "empty-log", false).
		Return(nil).Once()

	_, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, nil, "empty-log", false)
	assert.NoError(t, err)
}

// TestDeleteLog_NoSegments_ServiceMode_NothingToFence covers the other half: in service mode
// an empty quorum means every segment was already truncated away and no node is serving the
// log, so there is nothing to contact.
func TestDeleteLog_NoSegments_ServiceMode_NothingToFence(t *testing.T) {
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
	_, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, storage, "empty-log", false)
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

	_, err := deleteLogUnsafe(ctx, mockMeta, mockPool, cfg, nil, "foo", false)
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

	// Embedded deployment: each log's in-process logstore is fenced before its metadata goes.
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

	_, err := deleteAllLogsUnsafe(ctx, mockMeta, mockPool, cfg, nil, false)
	assert.NoError(t, err)
}

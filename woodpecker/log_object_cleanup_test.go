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
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
)

// TestLogObjectPrefixMatchesWriterKeys pins the only property that matters: the enumeration
// prefix must be a prefix of the keys the writers actually produce, for every rootPath the
// config layer tolerates.
//
// The writers concatenate the configured value verbatim —
//
//	getSegmentFileKey:  fmt.Sprintf("%s/%d/%d", rootPath, logId, segmentId)
//	getFooterObjectKey: fmt.Sprintf("%s/%d/%d/footer.blk", rootPath, logId, segmentId)
//
// which is exactly why ValidateMinioConfig only warns about a non-canonical rootPath in
// minio/local mode instead of rejecting it: every site agrees, so the object space is
// self-consistent. Normalising here would break that agreement in the one place that decides
// what gets deleted.
func TestLogObjectPrefixMatchesWriterKeys(t *testing.T) {
	writerSegmentKey := func(rootPath string, logId, segId int64) string {
		return fmt.Sprintf("%s/%d/%d", rootPath, logId, segId)
	}
	// "root/" and "a//b" are non-canonical but tolerated in minio/local mode; "" is the
	// bucket root. All of them must round-trip.
	for _, rootPath := range []string{"root", "root/", "wp/data", "", "a//b"} {
		prefix := logObjectPrefix(rootPath, 7)
		key := writerSegmentKey(rootPath, 7, 3)
		assert.True(t, strings.HasPrefix(key, prefix),
			"rootPath %q: writer key %q must live under enumeration prefix %q", rootPath, key, prefix)
		// And a different log must not be swept up by it.
		other := writerSegmentKey(rootPath, 8, 3)
		assert.False(t, strings.HasPrefix(other, prefix),
			"rootPath %q: prefix %q must not cover logId 8 key %q", rootPath, prefix, other)
	}
}

// TestIsLogObject pins the matcher that bounds how much a wrong bucket/rootPath argument can
// destroy. Everything woodpecker writes must match; anything else must not.
func TestIsLogObject(t *testing.T) {
	for _, rel := range []string{
		"0/0.blk", "12/345.blk", "12/m_0.blk", "12/m_345.blk",
		"12/footer.blk", "12/write.lock",
	} {
		assert.True(t, isLogObject(rel), "should match: %s", rel)
	}
	for _, rel := range []string{
		"",                  // empty
		"12",                // no object part
		"12/",               // empty object name
		"abc/0.blk",         // segment id is not numeric
		"12/34/0.blk",       // nested deeper than the layout
		"12/notes.txt",      // foreign file
		"12/0.blk.bak",      // near miss
		"12/blk",            // near miss
		"12/m_.blk",         // merged block without an id
		"12/x_1.blk",        // wrong merged prefix
		"12/FOOTER.BLK",     // case matters
		"12/write.lock.tmp", // near miss
		"../../etc/passwd",  // traversal
	} {
		assert.False(t, isLogObject(rel), "should NOT match: %s", rel)
	}
}

func newCleanupCfg() *config.Configuration {
	cfg := &config.Configuration{}
	cfg.Minio.BucketName = "bucket"
	cfg.Minio.RootPath = "root"
	return cfg
}

// walkReturning makes the mock storage enumerate the given keys under any prefix.
func walkReturning(storage *mocks_objectstorage.ObjectStorage, keys ...string) {
	storage.EXPECT().
		WalkWithObjects(mock.Anything, "bucket", "root/7/", true, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, _ string, _ bool, fn storageclient.ChunkObjectWalkFunc, _ string, _ string) error {
			for _, k := range keys {
				if !fn(&storageclient.ChunkObjectInfo{FilePath: k}) {
					break
				}
			}
			return nil
		})
}

// TestDeleteLogObjectsDeletesOnlyMatchingKeys is the safety property: objects that do not
// look like woodpecker's own are reported, never removed.
func TestDeleteLogObjectsDeletesOnlyMatchingKeys(t *testing.T) {
	storage := mocks_objectstorage.NewObjectStorage(t)
	walkReturning(storage,
		"root/7/0/0.blk",
		"root/7/0/footer.blk",
		"root/7/1/write.lock",
		"root/7/1/m_3.blk",
		"root/7/somebody-elses-file.txt",
		"root/7/1/notes.txt",
	)

	var mu sync.Mutex
	var removed []string
	storage.EXPECT().RemoveObject(mock.Anything, "bucket", mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, key string, _ string, _ string) error {
			mu.Lock()
			removed = append(removed, key)
			mu.Unlock()
			return nil
		})

	deleted, skipped, err := deleteLogObjects(context.Background(), storage, newCleanupCfg(), 7)
	require.NoError(t, err)
	assert.Equal(t, 4, deleted)

	sort.Strings(removed)
	assert.Equal(t, []string{
		"root/7/0/0.blk", "root/7/0/footer.blk", "root/7/1/m_3.blk", "root/7/1/write.lock",
	}, removed)

	sort.Strings(skipped)
	assert.Equal(t, []string{"root/7/1/notes.txt", "root/7/somebody-elses-file.txt"}, skipped,
		"unrecognised objects must be reported so a wrong prefix or a new object type is visible")
}

// TestDeleteLogObjectsIsIdempotent covers the re-run after a partial failure: an empty prefix
// is a successful no-op, and an object that vanished between the walk and the delete counts
// as deleted rather than failing the run.
func TestDeleteLogObjectsIsIdempotent(t *testing.T) {
	t.Run("empty prefix", func(t *testing.T) {
		storage := mocks_objectstorage.NewObjectStorage(t)
		walkReturning(storage)
		deleted, skipped, err := deleteLogObjects(context.Background(), storage, newCleanupCfg(), 7)
		require.NoError(t, err)
		assert.Zero(t, deleted)
		assert.Empty(t, skipped)
	})

	t.Run("object already gone", func(t *testing.T) {
		storage := mocks_objectstorage.NewObjectStorage(t)
		walkReturning(storage, "root/7/0/0.blk")
		notFound := assert.AnError
		storage.EXPECT().RemoveObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(notFound)
		storage.EXPECT().IsObjectNotExistsError(notFound).Return(true)

		deleted, _, err := deleteLogObjects(context.Background(), storage, newCleanupCfg(), 7)
		require.NoError(t, err)
		assert.Equal(t, 1, deleted)
	})
}

// TestDeleteLogObjectsReportsFailure keeps a genuine storage error from being mistaken for a
// clean run — the caller must not go on to delete the metadata.
func TestDeleteLogObjectsReportsFailure(t *testing.T) {
	storage := mocks_objectstorage.NewObjectStorage(t)
	walkReturning(storage, "root/7/0/0.blk")
	storage.EXPECT().RemoveObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(assert.AnError)
	storage.EXPECT().IsObjectNotExistsError(assert.AnError).Return(false)

	_, _, err := deleteLogObjects(context.Background(), storage, newCleanupCfg(), 7)
	assert.ErrorIs(t, err, assert.AnError)
}

// TestDeleteLogObjectsNilStorage covers the local-disk mode, where there is no object store.
func TestDeleteLogObjectsNilStorage(t *testing.T) {
	deleted, skipped, err := deleteLogObjects(context.Background(), nil, newCleanupCfg(), 7)
	require.NoError(t, err)
	assert.Zero(t, deleted)
	assert.Empty(t, skipped)
}

// TestSweepParkedLogObjectsReclaimsSoftDeletedLogs pins the residue path that ClearMeta would
// otherwise make permanent. A log soft-deleted by an older client — one that removed metadata
// without touching object storage — lives on only in logs-deleted/. ListLogs does not cover
// that prefix, so DeleteAllLogs never sees it, and ClearMeta deletes the prefix outright. The
// sweep has to run first, or those objects lose their last handle.
func TestSweepParkedLogObjectsReclaimsSoftDeletedLogs(t *testing.T) {
	ctx := context.Background()
	cfg := newCleanupCfg()

	md := mocks_meta.NewMetadataProvider(t)
	md.EXPECT().ListParkedLogIds(mock.Anything).Return([]int64{7}, nil)

	storage := mocks_objectstorage.NewObjectStorage(t)
	walkReturning(storage, "root/7/0/0.blk", "root/7/0/footer.blk")
	storage.EXPECT().RemoveObject(mock.Anything, "bucket", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	deleted, err := sweepParkedLogObjects(ctx, md, cfg, storage)
	require.NoError(t, err)
	assert.Equal(t, 2, deleted)
}

// A sweep that cannot reach object storage must not let the caller clear the metadata: the
// parked record is the only thing that still knows those objects exist.
func TestSweepParkedLogObjectsRefusesWithoutStorage(t *testing.T) {
	md := mocks_meta.NewMetadataProvider(t)
	md.EXPECT().ListParkedLogIds(mock.Anything).Return([]int64{7}, nil)

	_, err := sweepParkedLogObjects(context.Background(), md, newCleanupCfg(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parked logs")
}

// Nothing parked is the common case and must stay a cheap no-op that never touches storage.
func TestSweepParkedLogObjectsNoParkedLogsIsANoOp(t *testing.T) {
	md := mocks_meta.NewMetadataProvider(t)
	md.EXPECT().ListParkedLogIds(mock.Anything).Return(nil, nil)

	deleted, err := sweepParkedLogObjects(context.Background(), md, newCleanupCfg(), nil)
	require.NoError(t, err)
	assert.Zero(t, deleted)
}

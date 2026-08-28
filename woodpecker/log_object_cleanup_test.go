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
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
)

func TestLogObjectPrefix(t *testing.T) {
	assert.Equal(t, "root/7/", logObjectPrefix("root", 7))
	// A configured rootPath with a trailing slash must not produce a doubled separator:
	// the prefix is compared against real object keys.
	assert.Equal(t, "root/7/", logObjectPrefix("root/", 7))
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

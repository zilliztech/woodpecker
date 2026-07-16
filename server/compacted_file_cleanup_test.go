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

package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
)

// setupCompactedCleanupStore builds a service-mode logStore over a temp RootPath, wired
// with a mock ObjectStorage so the test controls footer HEAD (StatObject) results.
func setupCompactedCleanupStore(t *testing.T) (*logStore, string, *mocks_objectstorage.ObjectStorage) {
	t.Helper()
	store := createTestLogStore()
	root := t.TempDir()
	store.cfg.Woodpecker.Storage.RootPath = root
	store.cfg.Woodpecker.Storage.Type = "service"

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	store.storageClient = mockStorage

	return store, root, mockStorage
}

// makeSegmentDir creates root/bucket/rootPath/logId/segId with a data.log inside it, and
// returns the segment directory path.
func makeSegmentDir(t *testing.T, root, bucket, rootPath string, logId, segId int64) string {
	t.Helper()
	segDir := filepath.Join(root, bucket, rootPath, fmt.Sprintf("%d", logId), fmt.Sprintf("%d", segId))
	require.NoError(t, os.MkdirAll(segDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(segDir, "data.log"), []byte("staged data"), 0o644))
	return segDir
}

func footerKeyFor(rootPath string, logId, segId int64) string {
	return fmt.Sprintf("%s/%d/%d/footer.blk", rootPath, logId, segId)
}

// ageDataLog backdates the segment's data.log mtime by `age` so it clears the reconcile
// min-data-log-age gate (the default config sets a 30m window, so reconcile tests must age
// their data.log to exercise the footer HEAD / mark-writing path).
func ageDataLog(t *testing.T, segDir string, age time.Duration) {
	t.Helper()
	old := time.Now().Add(-age)
	require.NoError(t, os.Chtimes(filepath.Join(segDir, "data.log"), old, old))
}

// TestCompactedFileCleanup_MarkedAndFooterPresent_DropsLocalData verifies row 1 of the
// decision matrix: marked + footer present -> data.log removed, mark removed, reader
// evicted, and the now-empty segment dir pruned.
func TestCompactedFileCleanup_MarkedAndFooterPresent_DropsLocalData(t *testing.T) {
	store, root, mockStorage := setupCompactedCleanupStore(t)
	ctx := context.Background()

	bucket, rp := "b", "rp"
	logId, segId := int64(10), int64(2)
	segDir := makeSegmentDir(t, root, bucket, rp, logId, segId)
	require.NoError(t, writeCompactedMark(ctx, segDir))

	mockStorage.EXPECT().
		StatObject(ctx, bucket, footerKeyFor(rp, logId, segId), bucket+"/"+rp, "10").
		Return(int64(128), false, nil)

	task := newCompactedFileCleanupTask(store)
	require.NoError(t, task.runOnce(ctx, false))

	_, statErr := os.Stat(filepath.Join(segDir, "data.log"))
	assert.True(t, os.IsNotExist(statErr), "data.log should have been removed")
	assert.False(t, hasCompactedMark(segDir), "compacted mark should have been removed")
	_, dirStatErr := os.Stat(segDir)
	assert.True(t, os.IsNotExist(dirStatErr), "now-empty segment dir should have been pruned")
}

// TestCompactedFileCleanup_MarkedButFooterAbsent_NeverDeletesData verifies row 2 (the
// core invariant): marked + footer absent -> data.log is NOT deleted, but the orphan
// mark is cleared so future passes don't keep re-checking a stale mark.
func TestCompactedFileCleanup_MarkedButFooterAbsent_NeverDeletesData(t *testing.T) {
	store, root, mockStorage := setupCompactedCleanupStore(t)
	ctx := context.Background()

	bucket, rp := "b", "rp"
	logId, segId := int64(11), int64(3)
	segDir := makeSegmentDir(t, root, bucket, rp, logId, segId)
	require.NoError(t, writeCompactedMark(ctx, segDir))

	mockStorage.EXPECT().
		StatObject(ctx, bucket, footerKeyFor(rp, logId, segId), bucket+"/"+rp, "11").
		Return(int64(0), false, minio.ErrorResponse{Code: "NoSuchKey"})

	task := newCompactedFileCleanupTask(store)
	require.NoError(t, task.runOnce(ctx, false))

	_, statErr := os.Stat(filepath.Join(segDir, "data.log"))
	assert.NoError(t, statErr, "data.log must NOT be deleted without a confirmed footer")
	assert.False(t, hasCompactedMark(segDir), "orphan compacted mark should have been cleared")
}

// TestCompactedFileCleanup_UnmarkedAndFooterPresent_ReconcileWritesMark verifies row 3:
// unmarked + footer present, on a reconcile pass -> the mark is written (pull
// reconcile) but data.log is left in place for the NEXT pass to delete.
func TestCompactedFileCleanup_UnmarkedAndFooterPresent_ReconcileWritesMark(t *testing.T) {
	store, root, mockStorage := setupCompactedCleanupStore(t)
	ctx := context.Background()

	bucket, rp := "b", "rp"
	logId, segId := int64(12), int64(4)
	segDir := makeSegmentDir(t, root, bucket, rp, logId, segId)
	ageDataLog(t, segDir, time.Hour) // old enough to clear the reconcile min-age gate
	require.False(t, hasCompactedMark(segDir))

	mockStorage.EXPECT().
		StatObject(ctx, bucket, footerKeyFor(rp, logId, segId), bucket+"/"+rp, "12").
		Return(int64(64), false, nil)

	task := newCompactedFileCleanupTask(store)
	require.NoError(t, task.runOnce(ctx, true)) // reconcile pass

	assert.True(t, hasCompactedMark(segDir), "reconcile pass should have written the compacted mark")
	_, statErr := os.Stat(filepath.Join(segDir, "data.log"))
	assert.NoError(t, statErr, "data.log should still be present; deletion happens next pass")
}

// TestCompactedFileCleanup_UnmarkedAndFooterAbsent_NothingChanges verifies row 4:
// unmarked + footer absent -> nothing changes, and (since this is a reconcile pass) the
// footer HEAD is still performed but yields no mutation.
func TestCompactedFileCleanup_UnmarkedAndFooterAbsent_NothingChanges(t *testing.T) {
	store, root, mockStorage := setupCompactedCleanupStore(t)
	ctx := context.Background()

	bucket, rp := "b", "rp"
	logId, segId := int64(13), int64(5)
	segDir := makeSegmentDir(t, root, bucket, rp, logId, segId)
	ageDataLog(t, segDir, time.Hour) // old enough to clear the reconcile min-age gate

	mockStorage.EXPECT().
		StatObject(ctx, bucket, footerKeyFor(rp, logId, segId), bucket+"/"+rp, "13").
		Return(int64(0), false, minio.ErrorResponse{Code: "NoSuchKey"})

	task := newCompactedFileCleanupTask(store)
	require.NoError(t, task.runOnce(ctx, true)) // reconcile pass

	assert.False(t, hasCompactedMark(segDir), "no mark should be written")
	_, statErr := os.Stat(filepath.Join(segDir, "data.log"))
	assert.NoError(t, statErr, "data.log should be untouched")
}

// TestCompactedFileCleanup_ReconcileSkipsFreshDataLog verifies the reconcile min-age gate:
// on a reconcile pass, an unmarked segment whose data.log is still fresh (within the
// default 30m min-age window) is skipped WITHOUT a footer HEAD. No StatObject expectation
// is set, so mockery's strict mode fails the test if a HEAD is issued; and no mark is written.
func TestCompactedFileCleanup_ReconcileSkipsFreshDataLog(t *testing.T) {
	store, root, _ := setupCompactedCleanupStore(t)
	ctx := context.Background()

	bucket, rp := "b", "rp"
	logId, segId := int64(14), int64(6)
	segDir := makeSegmentDir(t, root, bucket, rp, logId, segId) // fresh mtime (now)

	task := newCompactedFileCleanupTask(store)
	require.NoError(t, task.runOnce(ctx, true)) // reconcile pass, but data.log is too fresh to HEAD

	assert.False(t, hasCompactedMark(segDir), "fresh data.log must not be marked (reconcile age gate)")
	_, statErr := os.Stat(filepath.Join(segDir, "data.log"))
	assert.NoError(t, statErr, "data.log should be untouched")
}

// TestCompactedFileCleanup_UnmarkedSegment_SkipsFooterHeadWhenNotReconcilePass verifies
// the S3-cost-control throttle: on a non-reconcile pass, unmarked segments are left
// alone WITHOUT any footer HEAD at all (no mock expectation set, so an unexpected call
// would fail the test via mockery's strict mode).
func TestCompactedFileCleanup_UnmarkedSegment_SkipsFooterHeadWhenNotReconcilePass(t *testing.T) {
	store, root, _ := setupCompactedCleanupStore(t)
	ctx := context.Background()

	bucket, rp := "b", "rp"
	logId, segId := int64(14), int64(6)
	segDir := makeSegmentDir(t, root, bucket, rp, logId, segId)

	task := newCompactedFileCleanupTask(store)
	require.NoError(t, task.runOnce(ctx, false)) // NOT a reconcile pass; no StatObject expected

	assert.False(t, hasCompactedMark(segDir))
	_, statErr := os.Stat(filepath.Join(segDir, "data.log"))
	assert.NoError(t, statErr)
}

// TestCompactedFileCleanup_NonServiceStorage_NoOp verifies the gate: the task must be a
// no-op when storage is not service-mode (mirrors the other maintenance tasks' guard).
func TestCompactedFileCleanup_NonServiceStorage_NoOp(t *testing.T) {
	store, root, _ := setupCompactedCleanupStore(t)
	store.cfg.Woodpecker.Storage.Type = "minio"
	ctx := context.Background()

	segDir := makeSegmentDir(t, root, "b", "rp", 15, 7)
	require.NoError(t, writeCompactedMark(ctx, segDir))

	task := newCompactedFileCleanupTask(store)
	require.NoError(t, task.runOnce(ctx, true))

	// No StatObject call should have happened (none configured on the mock) and
	// nothing on disk should have changed.
	assert.True(t, hasCompactedMark(segDir))
	_, statErr := os.Stat(filepath.Join(segDir, "data.log"))
	assert.NoError(t, statErr)
}

// TestCompactedFileCleanup_EmptyRootPath_NoOp verifies the second gate: an empty
// RootPath (pure object-storage / no local staging) short-circuits before any walk.
func TestCompactedFileCleanup_EmptyRootPath_NoOp(t *testing.T) {
	store := createTestLogStore()
	store.cfg.Woodpecker.Storage.Type = "service"
	store.cfg.Woodpecker.Storage.RootPath = ""

	task := newCompactedFileCleanupTask(store)
	require.NoError(t, task.runOnce(context.Background(), true))
}

// TestParseSegmentDirUnderRoot covers the path-parsing rule directly, including the
// multi-segment rootPath case and malformed-path rejection.
func TestParseSegmentDirUnderRoot(t *testing.T) {
	root := "/data"

	bucket, rp, logId, segId, ok := parseSegmentDirUnderRoot(root, "/data/b/rp/10/2")
	require.True(t, ok)
	assert.Equal(t, "b", bucket)
	assert.Equal(t, "rp", rp)
	assert.Equal(t, int64(10), logId)
	assert.Equal(t, int64(2), segId)

	// Multi-component rootPath (rootPath itself contains slashes).
	bucket, rp, logId, segId, ok = parseSegmentDirUnderRoot(root, "/data/b/a/b/c/10/2")
	require.True(t, ok)
	assert.Equal(t, "b", bucket)
	assert.Equal(t, "a/b/c", rp)
	assert.Equal(t, int64(10), logId)
	assert.Equal(t, int64(2), segId)

	// Too few components under root.
	_, _, _, _, ok = parseSegmentDirUnderRoot(root, "/data/b/10")
	assert.False(t, ok)

	// Non-numeric segId.
	_, _, _, _, ok = parseSegmentDirUnderRoot(root, "/data/b/rp/10/notanumber")
	assert.False(t, ok)

	// Non-numeric logId.
	_, _, _, _, ok = parseSegmentDirUnderRoot(root, "/data/b/rp/notanumber/2")
	assert.False(t, ok)
}

// TestCompactedFileCleanupTask_NameAndInterval verifies the MaintenanceTask surface.
func TestCompactedFileCleanupTask_NameAndInterval(t *testing.T) {
	store := createTestLogStore()
	task := newCompactedFileCleanupTask(store)
	assert.Equal(t, "compacted-file-cleanup", task.Name())
	assert.Equal(t, store.cfg.Woodpecker.Logstore.MaintenanceStrategy.CompactedFileCleanupInterval.Duration.Duration(), task.Interval())
}

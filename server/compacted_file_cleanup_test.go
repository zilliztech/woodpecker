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

	"github.com/zilliztech/woodpecker/common/config"
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
// decision matrix: marked + footer present -> data.log removed and reader evicted, with the
// compacted mark KEPT as a tombstone (dir left in place) so a later reader can serve the
// segment from object storage without an object-storage HEAD.
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
	require.NoError(t, task.runOnce(ctx, true)) // reconcile walk discovers the marked segment, then drains

	_, statErr := os.Stat(filepath.Join(segDir, "data.log"))
	assert.True(t, os.IsNotExist(statErr), "data.log should have been removed")
	assert.True(t, hasCompactedMark(segDir), "compacted mark should be KEPT as a tombstone")
	_, dirStatErr := os.Stat(segDir)
	assert.NoError(t, dirStatErr, "segment dir should be kept (it holds the tombstone mark)")
}

// TestCompactedFileCleanup_MarkedButFooterAbsent_NeverDeletesData verifies the core invariant
// for the anomalous marked-but-no-footer state: data.log is NOT deleted, AND the mark is KEPT
// (it is removed only by the truncate/delete GC). The task logs a warning and leaves both for
// that GC rather than clearing the mark itself.
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
	require.NoError(t, task.runOnce(ctx, true)) // reconcile walk enqueues the marked segment, then drains

	_, statErr := os.Stat(filepath.Join(segDir, "data.log"))
	assert.NoError(t, statErr, "data.log must NOT be deleted without a confirmed footer")
	assert.True(t, hasCompactedMark(segDir), "the mark is KEPT (removed only by the truncate/delete GC), not cleared here")
}

// TestCompactedFileCleanup_UnmarkedAndFooterPresent_ReconcileWritesMark verifies the reconcile
// path: an unmarked but durably-compacted segment (footer present, data.log old enough) is
// enqueued by the walk and, on drain, has the mark backfilled and its data.log dropped in the
// same pass (tombstone kept).
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

	assert.True(t, hasCompactedMark(segDir), "reconcile should have written the compacted mark (tombstone kept)")
	_, statErr := os.Stat(filepath.Join(segDir, "data.log"))
	assert.True(t, os.IsNotExist(statErr), "reconcile marks AND drops the data.log in the same drain")
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

// TestCompactedFileCleanup_ReconcileAgeGateDisabled_HeadsFreshDataLog verifies the
// documented "<=0 disables the age gate" escape hatch: with ReconcileMinDataLogAge set to
// 0, even a brand-new (fresh mtime) unmarked data.log is HEADed on a reconcile pass and the
// mark is written when the footer is present. This is the direct counterpart to
// TestCompactedFileCleanup_ReconcileSkipsFreshDataLog, where the default 30m gate skips the
// same fresh file.
func TestCompactedFileCleanup_ReconcileAgeGateDisabled_HeadsFreshDataLog(t *testing.T) {
	store, root, mockStorage := setupCompactedCleanupStore(t)
	store.cfg.Woodpecker.Logstore.MaintenanceStrategy.ReconcileMinDataLogAge = config.NewDurationSecondsFromInt(0)
	ctx := context.Background()

	bucket, rp := "b", "rp"
	logId, segId := int64(16), int64(8)
	segDir := makeSegmentDir(t, root, bucket, rp, logId, segId) // fresh mtime (now)

	mockStorage.EXPECT().
		StatObject(ctx, bucket, footerKeyFor(rp, logId, segId), bucket+"/"+rp, "16").
		Return(int64(64), false, nil)

	task := newCompactedFileCleanupTask(store)
	require.NoError(t, task.runOnce(ctx, true)) // reconcile pass; gate disabled -> HEAD despite fresh mtime

	assert.True(t, hasCompactedMark(segDir), "gate disabled -> fresh data.log is reconciled and marked")
	_, statErr := os.Stat(filepath.Join(segDir, "data.log"))
	assert.True(t, os.IsNotExist(statErr), "gate disabled -> fresh data.log is marked AND dropped in the same drain")
}

// TestCompactedFileCleanup_ReconcileRespectsCustomAgeGate verifies the gate honors a
// non-default configured window (i.e. the value is actually read, not the hardcoded 30m
// default): with ReconcileMinDataLogAge set to 10m, a data.log aged 5m (within the window)
// is skipped WITHOUT a footer HEAD, while the same segment aged 15m (past the window) is
// HEADed and marked. The single StatObject expectation is .Once(), so a spurious HEAD on
// the within-window pass would exceed it and fail the test.
func TestCompactedFileCleanup_ReconcileRespectsCustomAgeGate(t *testing.T) {
	store, root, mockStorage := setupCompactedCleanupStore(t)
	store.cfg.Woodpecker.Logstore.MaintenanceStrategy.ReconcileMinDataLogAge = config.NewDurationSecondsFromInt(600) // 10m
	ctx := context.Background()

	bucket, rp := "b", "rp"
	logId, segId := int64(17), int64(9)
	segDir := makeSegmentDir(t, root, bucket, rp, logId, segId)

	mockStorage.EXPECT().
		StatObject(ctx, bucket, footerKeyFor(rp, logId, segId), bucket+"/"+rp, "17").
		Return(int64(64), false, nil).
		Once()

	task := newCompactedFileCleanupTask(store)

	// Within the 10m window: too fresh -> no HEAD, no mark.
	ageDataLog(t, segDir, 5*time.Minute)
	require.NoError(t, task.runOnce(ctx, true))
	assert.False(t, hasCompactedMark(segDir), "data.log within the custom age window must not be reconciled")

	// Past the 10m window: HEAD issued, mark written.
	ageDataLog(t, segDir, 15*time.Minute)
	require.NoError(t, task.runOnce(ctx, true))
	assert.True(t, hasCompactedMark(segDir), "data.log past the custom age window should be reconciled and marked")
}

// TestCompactedFileCleanup_PushEnqueue_DrainsWithoutWalk verifies the frequent event-driven
// path: a segment enqueued by the push (NotifySegmentCompacted) is dropped on a NON-reconcile
// tick (no tree walk), confirming the footer with a single HEAD.
func TestCompactedFileCleanup_PushEnqueue_DrainsWithoutWalk(t *testing.T) {
	store, root, mockStorage := setupCompactedCleanupStore(t)
	ctx := context.Background()

	bucket, rp := "b", "rp"
	logId, segId := int64(18), int64(10)
	segDir := makeSegmentDir(t, root, bucket, rp, logId, segId)
	require.NoError(t, writeCompactedMark(ctx, segDir))

	mockStorage.EXPECT().
		StatObject(ctx, bucket, footerKeyFor(rp, logId, segId), bucket+"/"+rp, "18").
		Return(int64(128), false, nil)

	task := newCompactedFileCleanupTask(store)
	task.enqueue(segDir, bucket, rp, logId, segId) // simulate the push notification

	require.NoError(t, task.runOnce(ctx, false)) // NOT a reconcile pass: no walk, drains the queue

	_, statErr := os.Stat(filepath.Join(segDir, "data.log"))
	assert.True(t, os.IsNotExist(statErr), "push-enqueued segment should be dropped on drain without a walk")
	assert.True(t, hasCompactedMark(segDir), "mark kept as tombstone")
}

// TestCompactedFileCleanup_NonReconcileTick_DoesNotWalk verifies the two cadences: a marked
// segment present on disk but NOT in the in-memory queue (e.g. after a restart) is left alone on
// a non-reconcile tick (no walk), then recovered and dropped by the reconcile walk.
func TestCompactedFileCleanup_NonReconcileTick_DoesNotWalk(t *testing.T) {
	store, root, mockStorage := setupCompactedCleanupStore(t)
	ctx := context.Background()

	bucket, rp := "b", "rp"
	logId, segId := int64(19), int64(11)
	segDir := makeSegmentDir(t, root, bucket, rp, logId, segId)
	require.NoError(t, writeCompactedMark(ctx, segDir))

	// Only the reconcile walk will HEAD; the non-reconcile tick must not (queue is empty).
	mockStorage.EXPECT().
		StatObject(ctx, bucket, footerKeyFor(rp, logId, segId), bucket+"/"+rp, "19").
		Return(int64(128), false, nil).
		Once()

	task := newCompactedFileCleanupTask(store)

	// Non-reconcile tick: empty queue, no walk -> nothing happens.
	require.NoError(t, task.runOnce(ctx, false))
	_, statErr := os.Stat(filepath.Join(segDir, "data.log"))
	require.NoError(t, statErr, "non-reconcile tick must not walk/drop an on-disk marked segment")

	// Reconcile walk: re-discovers the mark (restart recovery), enqueues, drains, drops.
	require.NoError(t, task.runOnce(ctx, true))
	_, statErr = os.Stat(filepath.Join(segDir, "data.log"))
	assert.True(t, os.IsNotExist(statErr), "reconcile walk recovers and drops the marked segment")
}

// TestCompactedFileCleanupTask_NameAndInterval verifies the MaintenanceTask surface.
func TestCompactedFileCleanupTask_NameAndInterval(t *testing.T) {
	store := createTestLogStore()
	task := newCompactedFileCleanupTask(store)
	assert.Equal(t, "compacted-file-cleanup", task.Name())
	assert.Equal(t, store.cfg.Woodpecker.Logstore.MaintenanceStrategy.CompactedFileCleanupInterval.Duration.Duration(), task.Interval())
}

// TestCompactedFileCleanup_TransientStatError_Requeues verifies the drain retry path: a
// transport error on the footer HEAD leaves data.log in place and re-enqueues the segment,
// and the next tick (with the footer reachable again) completes the drop.
func TestCompactedFileCleanup_TransientStatError_Requeues(t *testing.T) {
	store, root, mockStorage := setupCompactedCleanupStore(t)
	ctx := context.Background()

	bucket, rp := "b", "rp"
	logId, segId := int64(20), int64(12)
	segDir := makeSegmentDir(t, root, bucket, rp, logId, segId)
	require.NoError(t, writeCompactedMark(ctx, segDir))

	// First tick: transport error (NOT NoSuchKey) -> no delete, requeued.
	mockStorage.EXPECT().
		StatObject(ctx, bucket, footerKeyFor(rp, logId, segId), bucket+"/"+rp, "20").
		Return(int64(0), false, fmt.Errorf("i/o timeout")).Once()
	// Second tick: footer confirmed -> drop.
	mockStorage.EXPECT().
		StatObject(ctx, bucket, footerKeyFor(rp, logId, segId), bucket+"/"+rp, "20").
		Return(int64(128), false, nil).Once()

	task := newCompactedFileCleanupTask(store)
	task.enqueue(segDir, bucket, rp, logId, segId)

	require.NoError(t, task.runOnce(ctx, false))
	_, statErr := os.Stat(filepath.Join(segDir, "data.log"))
	require.NoError(t, statErr, "data.log must survive a transient footer stat error")

	require.NoError(t, task.runOnce(ctx, false)) // drains the requeued entry
	_, statErr = os.Stat(filepath.Join(segDir, "data.log"))
	assert.True(t, os.IsNotExist(statErr), "requeued segment should be dropped once the footer is reachable")
}

// TestCompactedFileCleanup_QueuedSegmentAlreadyGone verifies the early return: a queued
// segment whose data.log is already gone (dropped earlier or truncate-GC'ed) is skipped
// without any footer HEAD (no StatObject expectation set).
func TestCompactedFileCleanup_QueuedSegmentAlreadyGone(t *testing.T) {
	store, root, _ := setupCompactedCleanupStore(t)
	ctx := context.Background()

	bucket, rp := "b", "rp"
	logId, segId := int64(21), int64(13)
	segDir := makeSegmentDir(t, root, bucket, rp, logId, segId)
	require.NoError(t, os.Remove(filepath.Join(segDir, "data.log")))

	task := newCompactedFileCleanupTask(store)
	task.enqueue(segDir, bucket, rp, logId, segId)
	require.NoError(t, task.runOnce(ctx, false))
}

// TestCompactedFileCleanup_WalkSkipsUnparseableDir verifies the reconcile walk skips a
// data.log whose path does not parse as <bucket>/<rootPath>/<logId>/<segId> (no HEAD, no
// mark, no delete).
func TestCompactedFileCleanup_WalkSkipsUnparseableDir(t *testing.T) {
	store, root, _ := setupCompactedCleanupStore(t)
	ctx := context.Background()

	badDir := filepath.Join(root, "b", "rp", "notanumber", "2")
	require.NoError(t, os.MkdirAll(badDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(badDir, "data.log"), []byte("x"), 0o644))

	task := newCompactedFileCleanupTask(store)
	require.NoError(t, task.runOnce(ctx, true))

	_, statErr := os.Stat(filepath.Join(badDir, "data.log"))
	assert.NoError(t, statErr, "unparseable segment dir must be left untouched")
	assert.False(t, hasCompactedMark(badDir))
}

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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
)

// reconcileEveryNPasses throttles the pull-based reconcile branch (footer HEAD for
// data.log directories that have NO local compacted.mark yet) to roughly once every
// ~60s given the default 5s CompactedFileCleanupInterval. The push path (mark already
// written by NotifySegmentCompacted) is unaffected and still does its one confirming
// HEAD every pass, right before deleting — that HEAD is a required correctness check,
// not cost-control surface.
const reconcileEveryNPasses = 12

// compactedFileCleanupTask drops the local staged data.log for segments whose data is
// durably compacted in object storage, once that is confirmed via the local
// compacted.mark (T1) and, right before deletion, a footer HEAD against minio. It never
// deletes data.log without a confirmed footer present in object storage — that is the
// invariant this task exists to protect.
//
// Scope: this task does exactly two things — (1) on a throttled reconcile pass, backfill a
// mark for a compacted-but-unmarked segment, and (2) delete the data.log of a marked segment.
// It reclaims nothing else: the compacted.mark and the segment directory are KEPT as a
// tombstone (so a later reader can still serve the segment from object storage via the mark,
// with no HEAD), and object storage is never touched. The mark, the directory, and the
// object-storage data are removed only by the separate truncate/delete GC, when the log or
// segment is actually deleted.
//
// Decision matrix (marked = hasCompactedMark(segDir), footer = footer.blk present in minio):
//
//	marked && footer   -> delete data.log, evict cached reader; KEEP mark + dir as a tombstone
//	marked && !footer  -> do NOT delete data.log (invariant); remove the orphan mark and
//	                      leave data.log for truncate GC (or a future compaction pass)
//	!marked && footer  -> write the mark (pull reconcile); a later pass performs the delete
//	!marked && !footer -> not compacted yet; leave alone
type compactedFileCleanupTask struct {
	store *logStore
	pass  atomic.Uint64
}

func newCompactedFileCleanupTask(store *logStore) *compactedFileCleanupTask {
	return &compactedFileCleanupTask{store: store}
}

func (t *compactedFileCleanupTask) Name() string { return "compacted-file-cleanup" }

func (t *compactedFileCleanupTask) Interval() time.Duration {
	return t.store.cfg.Woodpecker.Logstore.MaintenanceStrategy.CompactedFileCleanupInterval.Duration.Duration()
}

func (t *compactedFileCleanupTask) Run(ctx context.Context) error {
	n := t.pass.Add(1)
	doReconcile := n%reconcileEveryNPasses == 1 // run on pass 1, then every Nth pass thereafter
	return t.runOnce(ctx, doReconcile)
}

// runOnce performs one scan pass. doReconcile controls whether the low-frequency
// unmarked-branch footer HEAD (pull reconcile) runs this pass; it is a parameter (rather
// than always reading the internal counter) so tests can exercise the reconcile branch
// deterministically.
func (t *compactedFileCleanupTask) runOnce(ctx context.Context, doReconcile bool) error {
	if !t.store.cfg.Woodpecker.Storage.IsStorageService() {
		return nil
	}
	root := t.store.cfg.Woodpecker.Storage.RootPath
	if root == "" {
		return nil
	}
	reconcileMinAge := t.store.cfg.Woodpecker.Logstore.MaintenanceStrategy.ReconcileMinDataLogAge.Duration.Duration()

	if doReconcile {
		logger.Ctx(ctx).Info("compacted-file-cleanup: running reconcile pass (unmarked segments footer HEAD)")
	}

	segDirs, err := findDataLogSegmentDirs(root)
	if err != nil {
		return err
	}

	for _, segDir := range segDirs {
		bucket, rootPath, logId, segId, ok := parseSegmentDirUnderRoot(root, segDir)
		if !ok {
			logger.Ctx(ctx).Debug("compacted-file-cleanup: skipping dir with unparseable path",
				zap.String("segDir", segDir))
			continue
		}

		marked := hasCompactedMark(segDir)

		if marked {
			// Marked segments get a confirming HEAD every pass — this is the one HEAD
			// right before deletion, not subject to reconcile throttling.
			footer, statErr := t.footerExistsInMinio(ctx, bucket, rootPath, logId, segId)
			if statErr != nil {
				logger.Ctx(ctx).Warn("compacted-file-cleanup: failed to stat footer; skipping this pass",
					zap.String("segDir", segDir), zap.Error(statErr))
				continue
			}
			if footer {
				t.dropSegmentLocalData(ctx, segDir, bucket, rootPath, logId, segId)
			} else {
				// Orphan mark: no confirmed footer in object storage. NEVER delete
				// data.log here (the invariant). Clear the stale mark and leave
				// data.log for truncate GC (or a future compaction pass) to handle.
				if rmErr := removeCompactedMark(ctx, segDir); rmErr != nil {
					logger.Ctx(ctx).Warn("compacted-file-cleanup: failed to remove orphan mark",
						zap.String("segDir", segDir), zap.Error(rmErr))
					continue
				}
				logger.Ctx(ctx).Info("compacted-file-cleanup: orphan compacted mark cleared (no confirmed footer); data.log left for truncate GC",
					zap.String("segDir", segDir), zap.Int64("logId", logId), zap.Int64("segId", segId))
			}
			continue
		}

		if !doReconcile {
			// Not marked, and this isn't a reconcile pass: skip the footer HEAD to
			// bound object-storage request cost.
			continue
		}

		// Age gate: only reconcile (HEAD) a data.log that has been idle long enough to be
		// certainly compacted, so we don't waste HEADs on segments still in the
		// write->roll->compact pipeline. A future mtime (clock skew) yields a negative age
		// and is likewise treated as too-fresh. Disabled when reconcileMinAge <= 0.
		if reconcileMinAge > 0 {
			info, statErr := os.Stat(filepath.Join(segDir, "data.log"))
			if statErr != nil {
				continue // gone or unreadable; nothing to reconcile
			}
			if age := time.Since(info.ModTime()); age < reconcileMinAge {
				logger.Ctx(ctx).Debug("compacted-file-cleanup: reconcile skipping fresh data.log (within min-age window)",
					zap.String("segDir", segDir), zap.Duration("age", age), zap.Duration("minAge", reconcileMinAge))
				continue
			}
		}

		footer, statErr := t.footerExistsInMinio(ctx, bucket, rootPath, logId, segId)
		if statErr != nil {
			logger.Ctx(ctx).Warn("compacted-file-cleanup: reconcile footer stat failed; skipping",
				zap.String("segDir", segDir), zap.Error(statErr))
			continue
		}
		if !footer {
			continue // not compacted yet
		}
		if markErr := writeCompactedMark(ctx, segDir); markErr != nil {
			logger.Ctx(ctx).Warn("compacted-file-cleanup: reconcile failed to write compacted mark",
				zap.String("segDir", segDir), zap.Error(markErr))
			continue
		}
		logger.Ctx(ctx).Info("compacted-file-cleanup: reconcile wrote compacted mark for unmarked-but-compacted segment",
			zap.String("segDir", segDir), zap.Int64("logId", logId), zap.Int64("segId", segId))
	}

	return nil
}

// dropSegmentLocalData removes the segment's local data.log and evicts any cached segment
// reader so subsequent reads rebuild against object storage. The compacted mark is KEPT as a
// durable tombstone (and the segment directory is left in place): it lets a later reader tell
// "compacted -> serve from object storage" from "no data here" without an object-storage
// HEAD. The tombstone is removed only when the segment is fully truncated/deleted (the
// truncate/delete GC path removes the mark and the directory).
func (t *compactedFileCleanupTask) dropSegmentLocalData(ctx context.Context, segDir, bucket, rootPath string, logId, segId int64) {
	dataLogPath := filepath.Join(segDir, "data.log")
	if rmErr := os.Remove(dataLogPath); rmErr != nil && !os.IsNotExist(rmErr) {
		logger.Ctx(ctx).Warn("compacted-file-cleanup: failed to remove data.log; will retry next pass",
			zap.String("path", dataLogPath), zap.Error(rmErr))
		return
	}

	if evictErr := t.store.EvictSegmentReader(ctx, bucket, rootPath, logId, segId); evictErr != nil {
		logger.Ctx(ctx).Warn("compacted-file-cleanup: failed to evict cached segment reader",
			zap.String("segDir", segDir), zap.Error(evictErr))
	}

	// The compacted.mark is intentionally KEPT (tombstone) and the segment dir is left in
	// place, so a reader that opens this segment after the data.log is gone can serve it
	// from object storage without an object-storage HEAD.
	logger.Ctx(ctx).Info("compacted-file-cleanup: removed local data.log for durably compacted segment (mark kept as tombstone)",
		zap.String("segDir", segDir), zap.Int64("logId", logId), zap.Int64("segId", segId))
}

// footerExistsInMinio checks whether the compacted footer object for (logId, segId)
// is present in object storage, using the exact same key format as the segment reader
// (getFooterBlockKey in server/storage/stagedstorage/reader_impl.go).
func (t *compactedFileCleanupTask) footerExistsInMinio(ctx context.Context, bucket, rootPath string, logId, segId int64) (bool, error) {
	footerKey := fmt.Sprintf("%s/%d/%d/footer.blk", rootPath, logId, segId)
	logNs := bucket + "/" + rootPath
	logIdStr := strconv.FormatInt(logId, 10)
	_, _, err := t.store.storageClient.StatObject(ctx, bucket, footerKey, logNs, logIdStr)
	if err != nil {
		if minioHandler.IsObjectNotExists(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// findDataLogSegmentDirs walks root looking for files named data.log, skipping the
// top-level delete-marker directory exactly like logStore.HasLocalSegmentData. It
// returns the parent directory (the segment directory) of each data.log found.
func findDataLogSegmentDirs(root string) ([]string, error) {
	var segDirs []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable entries
		}
		if d.IsDir() {
			// Skip ONLY the top-level marker dir, not any user dir that happens to be
			// named ".deleted" deeper under root.
			if path == filepath.Join(root, deleteMarkerDir) {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Name() == "data.log" {
			segDirs = append(segDirs, filepath.Dir(path))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return segDirs, nil
}

// parseSegmentDirUnderRoot parses (bucket, rootPath, logId, segId) out of segDir given
// it is laid out as root/<bucket>/<rootPath...>/<logId>/<segId>. Returns ok=false if
// segDir does not have enough path components under root, or if the last two
// components do not parse as int64 (logId, segId).
func parseSegmentDirUnderRoot(root, segDir string) (bucket, rootPath string, logId, segId int64, ok bool) {
	rel, err := filepath.Rel(root, segDir)
	if err != nil {
		return "", "", 0, 0, false
	}
	rel = filepath.ToSlash(rel)
	parts := strings.Split(rel, "/")
	if len(parts) < 3 {
		return "", "", 0, 0, false
	}
	last := len(parts) - 1
	segId, segErr := strconv.ParseInt(parts[last], 10, 64)
	if segErr != nil {
		return "", "", 0, 0, false
	}
	logId, logErr := strconv.ParseInt(parts[last-1], 10, 64)
	if logErr != nil {
		return "", "", 0, 0, false
	}
	bucket = parts[0]
	rootPath = strings.Join(parts[1:last-1], "/")
	return bucket, rootPath, logId, segId, true
}

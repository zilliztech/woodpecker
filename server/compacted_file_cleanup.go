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
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
)

// reconcileEveryNPasses gates the low-frequency reconcile walk (the "pull" safety net). The
// full-tree scan runs on pass 1 (startup) and then every Nth pass — roughly every 5m given the
// default 5s CompactedFileCleanupInterval. Between walks the frequent drop path never touches
// the tree: it drains the in-memory queue fed by the push (NotifySegmentCompacted) and by the
// walk. Raising this lowers the full-scan frequency; the push path is unaffected.
const reconcileEveryNPasses = 60

// pendingSeg identifies a segment whose local data.log should be dropped once its compacted
// footer is confirmed present in object storage. It carries just enough to run the footer HEAD
// and the drop; the marked state is re-read from disk when the segment is processed.
type pendingSeg struct {
	segDir   string
	bucket   string
	rootPath string
	logId    int64
	segId    int64
	// Retry state for transient footer-HEAD failures: attempts counts consecutive failed
	// HEADs; nextAttemptAt gates the next try (exponential backoff capped at the reconcile
	// cadence), so an object-storage outage costs a bounded, decaying HEAD rate per segment
	// instead of one probe per queued segment per 5s tick. Zero values mean "due now"; a
	// fresh enqueue (push notify or reconcile walk) resets both — new evidence earns an
	// immediate try.
	attempts      int
	nextAttemptAt time.Time
}

// compactedFileCleanupTask reclaims the local staged data.log for segments whose data is
// durably compacted in object storage, once that is confirmed by a footer HEAD against minio.
// It never deletes data.log without a confirmed footer — that is the invariant this task exists
// to protect.
//
// Cadence: the drop path is event-driven. The push (NotifySegmentCompacted) enqueues a segment
// the moment it is compacted, and every maintenance tick drains that queue (footer HEAD + drop)
// WITHOUT walking the tree. A full-tree reconcile walk runs only on pass 1 (startup) and then
// every reconcileEveryNPasses-th pass; it (a) re-enqueues marked segments still holding a
// data.log — recovering the in-memory queue after a restart and any push that was missed — and
// (b) enqueues unmarked segments whose data.log is old enough to reconcile. So a busy node no
// longer re-scans the whole tree every few seconds.
//
// Scope: this task only backfills marks and drops marked data.logs. It reclaims nothing else:
// the data.compacted mark and the segment directory are KEPT as a tombstone (so a later reader
// can serve the segment from object storage via the mark, with no HEAD), and object storage is
// never touched. The mark, the directory, and the object-storage data are removed only by the
// separate truncate/delete GC, when the log or segment is actually deleted.
//
// Per-segment decision at drop time (marked = hasCompactedMark(segDir), footer = footer.blk in minio):
//
//	footer             -> write the mark if missing (reconcile), then delete data.log; KEEP mark + dir as a tombstone
//	!footer && marked  -> anomaly (a mark is only ever written after the footer exists): WARN and leave
//	                      BOTH data.log and mark for the truncate/delete GC (the mark is never cleared here)
//	!footer && !marked -> not compacted yet; leave alone
type compactedFileCleanupTask struct {
	store *logStore
	pass  atomic.Uint64

	mu      sync.Mutex
	pending map[string]pendingSeg // segDir -> segment awaiting a footer-confirmed data.log drop
}

func newCompactedFileCleanupTask(store *logStore) *compactedFileCleanupTask {
	return &compactedFileCleanupTask{store: store, pending: make(map[string]pendingSeg)}
}

func (t *compactedFileCleanupTask) Name() string { return "compacted-file-cleanup" }

func (t *compactedFileCleanupTask) Interval() time.Duration {
	return t.store.cfg.Woodpecker.Logstore.MaintenanceStrategy.CompactedFileCleanupInterval.Duration.Duration()
}

// enqueue records a segment whose data.log should be dropped once its compacted footer is
// confirmed. Called from the push path (NotifySegmentCompacted) and from the reconcile walk;
// safe from any goroutine. Duplicates are harmless (keyed by segDir).
func (t *compactedFileCleanupTask) enqueue(segDir, bucket, rootPath string, logId, segId int64) {
	t.mu.Lock()
	t.pending[segDir] = pendingSeg{segDir: segDir, bucket: bucket, rootPath: rootPath, logId: logId, segId: segId}
	t.mu.Unlock()
}

// requeueAfterFailure re-inserts a segment whose footer HEAD failed transiently, doubling
// the delay per consecutive failure up to the reconcile cadence. A fresh enqueue for the
// same segment that raced in while this one was being processed wins (it reset the backoff),
// so the stale retry state is dropped rather than clobbering it.
func (t *compactedFileCleanupTask) requeueAfterFailure(s pendingSeg) {
	base := t.Interval()
	if base <= 0 {
		base = 5 * time.Second
	}
	maxBackoff := base * reconcileEveryNPasses
	backoff := base
	for i := 0; i < s.attempts && backoff < maxBackoff; i++ {
		backoff *= 2
	}
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	s.attempts++
	s.nextAttemptAt = time.Now().Add(backoff)
	t.mu.Lock()
	if _, exists := t.pending[s.segDir]; !exists {
		t.pending[s.segDir] = s
	}
	t.mu.Unlock()
}

func (t *compactedFileCleanupTask) Run(ctx context.Context) error {
	n := t.pass.Add(1)
	// Pass 1 (startup) and every reconcileEveryNPasses-th pass thereafter run the reconcile walk.
	return t.runOnce(ctx, n%reconcileEveryNPasses == 1)
}

// runOnce performs one maintenance tick: an optional reconcile walk (the low-frequency pull
// safety net) followed by draining the drop queue. doReconcile is a parameter (rather than
// reading the internal counter) so tests can drive the walk deterministically.
func (t *compactedFileCleanupTask) runOnce(ctx context.Context, doReconcile bool) error {
	if !t.store.cfg.Woodpecker.Storage.IsStorageService() {
		return nil
	}
	if t.store.cfg.Woodpecker.Storage.RootPath == "" {
		return nil
	}
	if doReconcile {
		// A walk error means only the root itself is unreadable (findDataLogSegmentDirs
		// swallows per-entry errors), e.g. a brief mount outage. The push-fed drop queue needs
		// no tree access, so log and drain anyway rather than skipping it.
		if err := t.reconcileWalk(ctx); err != nil {
			logger.Ctx(ctx).Warn("compacted-file-cleanup: reconcile walk failed; draining the push queue anyway",
				zap.Error(err))
		}
	}
	t.drainPending(ctx)
	return nil
}

// reconcileWalk is the low-frequency pull safety net. It walks the local segment tree and
// enqueues drop candidates: every marked segment that still has a data.log (restart recovery
// plus any push the in-memory queue missed), and every unmarked segment whose data.log has been
// idle past the age gate (a compacted-but-unmarked segment to reconcile). The footer HEAD, the
// mark backfill, and the drop all happen later in drainPending, so there is a single drop path.
func (t *compactedFileCleanupTask) reconcileWalk(ctx context.Context) error {
	root := t.store.cfg.Woodpecker.Storage.RootPath
	reconcileMinAge := t.store.cfg.Woodpecker.Logstore.MaintenanceStrategy.ReconcileMinDataLogAge.Duration.Duration()
	logger.Ctx(ctx).Info("compacted-file-cleanup: running reconcile walk (restart recovery + unmarked footer HEAD)")

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
		if hasCompactedMark(segDir) {
			// Marked and still holding a data.log: (re)enqueue for the drop path.
			t.enqueue(segDir, bucket, rootPath, logId, segId)
			continue
		}
		// Unmarked: only reconcile a data.log that has been idle long enough to be certainly
		// compacted, so we don't waste HEADs on segments still in the write->roll->compact
		// pipeline. A future mtime (clock skew) yields a negative age and is likewise treated as
		// too-fresh. Disabled when reconcileMinAge <= 0.
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
		t.enqueue(segDir, bucket, rootPath, logId, segId)
	}
	return nil
}

// drainPending processes every queued segment: it confirms the compacted footer in object
// storage and then drops the local data.log. This is the frequent, walk-free path fed by the
// push notification and the reconcile walk. The queue is snapshotted and cleared under the lock,
// then processed outside it so per-segment I/O does not block enqueue.
func (t *compactedFileCleanupTask) drainPending(ctx context.Context) {
	now := time.Now()
	t.mu.Lock()
	if len(t.pending) == 0 {
		t.mu.Unlock()
		return
	}
	batch := make([]pendingSeg, 0, len(t.pending))
	for key, s := range t.pending {
		if s.nextAttemptAt.After(now) {
			continue // still backing off after a failed footer HEAD; leave queued
		}
		batch = append(batch, s)
		delete(t.pending, key)
	}
	t.mu.Unlock()

	for _, s := range batch {
		t.processSegment(ctx, s)
	}
}

// processSegment confirms the compacted footer and drops the local data.log. It NEVER deletes
// data.log without a confirmed footer (the core invariant). For an unmarked-but-compacted
// segment it backfills the mark before dropping. A marked segment whose footer is confirmed
// ABSENT is an anomaly that should not occur — a mark is only ever written after the footer
// exists — so it is logged loudly and left, mark and data.log both, for the truncate/delete GC;
// the mark is never cleared here, upholding "the mark is removed only by the truncate/delete GC".
func (t *compactedFileCleanupTask) processSegment(ctx context.Context, s pendingSeg) {
	if _, err := os.Stat(filepath.Join(s.segDir, "data.log")); os.IsNotExist(err) {
		return // data.log already gone (dropped earlier, or removed by the truncate GC): nothing to do
	}
	footer, statErr := footerExistsInMinio(ctx, t.store.storageClient, s.bucket, s.rootPath, s.logId, s.segId)
	if statErr != nil {
		logger.Ctx(ctx).Warn("compacted-file-cleanup: failed to stat footer; will retry with backoff",
			zap.String("segDir", s.segDir), zap.Int("attempts", s.attempts+1), zap.Error(statErr))
		t.requeueAfterFailure(s) // transient: retried with exponential backoff, capped at the reconcile cadence
		return
	}
	marked := hasCompactedMark(s.segDir)
	if !footer {
		if marked {
			logger.Ctx(ctx).Warn("compacted-file-cleanup: marked segment has no compacted footer in object storage; leaving data.log AND mark for the truncate/delete GC (unexpected state)",
				zap.String("segDir", s.segDir), zap.Int64("logId", s.logId), zap.Int64("segId", s.segId))
		}
		// unmarked && !footer: not compacted yet; leave alone.
		return
	}
	if !marked {
		// Reconcile: a compacted-but-unmarked segment. Backfill the durable tombstone before the
		// drop so a reader can always tell "compacted -> serve from object storage" apart from
		// "no data here".
		if markErr := writeCompactedMark(ctx, s.segDir); markErr != nil {
			logger.Ctx(ctx).Warn("compacted-file-cleanup: reconcile failed to write compacted mark; will retry",
				zap.String("segDir", s.segDir), zap.Error(markErr))
			t.enqueue(s.segDir, s.bucket, s.rootPath, s.logId, s.segId)
			return
		}
		logger.Ctx(ctx).Info("compacted-file-cleanup: reconcile wrote compacted mark for unmarked-but-compacted segment",
			zap.String("segDir", s.segDir), zap.Int64("logId", s.logId), zap.Int64("segId", s.segId))
	}
	t.dropSegmentLocalData(ctx, s.segDir, s.bucket, s.rootPath, s.logId, s.segId)
}

// dropSegmentLocalData removes the segment's local data.log and evicts any cached segment
// reader so subsequent reads rebuild against object storage. The compacted mark is KEPT as a
// durable tombstone (and the segment directory is left in place): it lets a later reader tell
// "compacted -> serve from object storage" from "no data here" without an object-storage
// HEAD. The tombstone is removed only when the segment is fully truncated/deleted (the
// truncate/delete GC path removes the mark and the directory).
func (t *compactedFileCleanupTask) dropSegmentLocalData(ctx context.Context, segDir, bucket, rootPath string, logId, segId int64) {
	dataLogPath := filepath.Join(segDir, "data.log")
	// Size the file before removal so we can keep the local-storage gauges accurate. This
	// push+pull path bypasses deleteLocalFiles' accounting, so without this the "current local
	// bytes/files" gauges would never decrement for reclaimed data.logs and capacity dashboards
	// would persistently overestimate WAL usage.
	var dataLogSize int64
	sizeKnown := false
	if info, statErr := os.Stat(dataLogPath); statErr == nil {
		dataLogSize = info.Size()
		sizeKnown = true
	}
	// Release the cached writer's fd BEFORE unlinking: the node that ran the compaction still
	// holds data.log open O_APPEND in its cached StagedFileWriter, and an unlinked inode's
	// blocks are not freed while any fd is open — the drop would look done (ls) while df stays
	// flat until the idle cleanup (MaxIdleTime) closes the writer.
	if evictErr := t.store.EvictSegmentWriter(ctx, bucket, rootPath, logId, segId); evictErr != nil {
		logger.Ctx(ctx).Warn("compacted-file-cleanup: failed to evict cached segment writer",
			zap.String("segDir", segDir), zap.Error(evictErr))
	}
	rmErr := os.Remove(dataLogPath)
	if rmErr != nil && !os.IsNotExist(rmErr) {
		logger.Ctx(ctx).Warn("compacted-file-cleanup: failed to remove data.log; will retry next pass",
			zap.String("path", dataLogPath), zap.Error(rmErr))
		return
	}
	// Decrement the local-storage gauges ONLY for a removal this task actually performed
	// (mirroring deleteLocalFiles, which decrements only when its os.Remove succeeds). If the
	// file was already gone — e.g. the truncate/delete GC removed it between our existence
	// check and this unlink — the remover did its own accounting, and decrementing here too
	// would drive the series permanently negative. Skip the byte Sub when the pre-unlink Stat
	// failed (size unknown): a small residual over-report beats subtracting a fabricated 0/N.
	if rmErr == nil {
		// Same log_ns the staged writer used when incrementing these gauges: rootPath is
		// validated at the NotifySegmentCompacted boundary (push) and derived
		// path.Join-canonicalized from disk (pull reconcile), so both agree and this
		// decrement hits the writer's exact series.
		storedNs := bucket + "/" + rootPath
		logIdStr := strconv.FormatInt(logId, 10)
		if sizeKnown {
			metrics.WpFileStoredBytes.WithLabelValues(metrics.NodeID, storedNs, logIdStr).Sub(float64(dataLogSize))
		}
		metrics.WpFileStoredCount.WithLabelValues(metrics.NodeID, storedNs, logIdStr).Dec()
	}

	if evictErr := t.store.EvictSegmentReader(ctx, bucket, rootPath, logId, segId); evictErr != nil {
		logger.Ctx(ctx).Warn("compacted-file-cleanup: failed to evict cached segment reader",
			zap.String("segDir", segDir), zap.Error(evictErr))
	}

	// The data.compacted is intentionally KEPT (tombstone) and the segment dir is left in
	// place, so a reader that opens this segment after the data.log is gone can serve it
	// from object storage without an object-storage HEAD.
	logger.Ctx(ctx).Info("compacted-file-cleanup: removed local data.log for durably compacted segment (mark kept as tombstone)",
		zap.String("segDir", segDir), zap.Int64("logId", logId), zap.Int64("segId", segId))
}

// footerExistsInMinio checks whether the compacted footer object for (logId, segId)
// is present in object storage, using the exact same key format as the segment reader
// (getFooterBlockKey in server/storage/stagedstorage/reader_impl.go). Shared by the cleanup
// task (verify-before-drop / reconcile backfill) and the NotifySegmentCompacted handler
// (verify-before-mark), so every consumer of the "footer exists" fact probes the same key.
func footerExistsInMinio(ctx context.Context, client storageclient.ObjectStorage, bucket, rootPath string, logId, segId int64) (bool, error) {
	// Same verbatim key format as the staged writer/reader (getFooterBlockKey): rootPath is
	// validated clean at startup, so raw concatenation is the canonical key.
	footerKey := fmt.Sprintf("%s/%d/%d/footer.blk", rootPath, logId, segId)
	logNs := bucket + "/" + rootPath
	logIdStr := strconv.FormatInt(logId, 10)
	_, _, err := client.StatObject(ctx, bucket, footerKey, logNs, logIdStr)
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

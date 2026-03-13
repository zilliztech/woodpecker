// Copyright 2025 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/log/idempotent"
)

// IdempotentWriterConfig holds configuration for the idempotent writer.
type IdempotentWriterConfig struct {
	// DedupWindowConfig for the dedup window.
	DedupWindowConfig idempotent.DedupWindowConfig
	// SnapshotInterval is the time interval between snapshots.
	SnapshotInterval time.Duration
	// SnapshotThreshold is the number of new entries before triggering a snapshot.
	SnapshotThreshold int
}

// DefaultIdempotentWriterConfig returns the default configuration.
func DefaultIdempotentWriterConfig() IdempotentWriterConfig {
	return IdempotentWriterConfig{
		DedupWindowConfig: idempotent.DefaultDedupWindowConfig(),
		SnapshotInterval:  30 * time.Second,
		SnapshotThreshold: 10000,
	}
}

// IdempotentWriter wraps a LogWriter to provide idempotent write semantics.
type IdempotentWriter interface {
	LogWriter

	// GetDedupWindow returns the dedup window for testing/debugging purposes.
	GetDedupWindow() idempotent.DedupWindow
}

// idempotentWriterImpl is the implementation of IdempotentWriter.
type idempotentWriterImpl struct {
	mu sync.RWMutex

	logId           int64
	innerWriter     LogWriter
	dedupWindow     idempotent.DedupWindow
	snapshotManager idempotent.DedupSnapshotManager
	config          IdempotentWriterConfig

	// Tracking for snapshot triggering
	entriesSinceSnapshot int
	lastSnapshotTime     time.Time

	// Pending writes: idempotencyId -> list of channels waiting for result
	pendingWrites map[string][]chan *WriteResult

	// Snapshot goroutine control
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewIdempotentWriter creates a new IdempotentWriter.
// This does NOT automatically recover from snapshot - call Recover() first if needed.
func NewIdempotentWriter(
	logId int64,
	innerWriter LogWriter,
	snapshotManager idempotent.DedupSnapshotManager,
	config IdempotentWriterConfig,
) IdempotentWriter {
	w := &idempotentWriterImpl{
		logId:            logId,
		innerWriter:      innerWriter,
		dedupWindow:      idempotent.NewDedupWindow(config.DedupWindowConfig),
		snapshotManager:  snapshotManager,
		config:           config,
		lastSnapshotTime: time.Now(),
		pendingWrites:    make(map[string][]chan *WriteResult),
		stopCh:           make(chan struct{}),
	}

	// Start background snapshot goroutine
	go w.snapshotLoop()

	return w
}

// Recover loads the snapshot and rebuilds the dedup window from gap data.
// This MUST be called before the writer can accept writes.
func (w *idempotentWriterImpl) Recover(ctx context.Context, logHandle LogHandle) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Step 1: Load snapshot from object storage
	snapshot, err := w.snapshotManager.Load(ctx)
	if err != nil {
		return fmt.Errorf("failed to load snapshot: %w", err)
	}

	if snapshot != nil {
		// Load entries from snapshot
		w.dedupWindow.LoadFromSnapshot(snapshot.Entries)
		logger.Ctx(ctx).Info("Loaded dedup window from snapshot",
			zap.Int64("logId", w.logId),
			zap.Int("entriesCount", len(snapshot.Entries)),
		)
	}

	// Step 2: Fill the gap by reading entries after the snapshot watermarks
	err = w.fillGapFromLog(ctx, logHandle, snapshot)
	if err != nil {
		return fmt.Errorf("failed to fill gap from log: %w", err)
	}

	// Step 3: Apply window eviction
	evicted := w.dedupWindow.Evict()
	if evicted > 0 {
		logger.Ctx(ctx).Info("Evicted expired entries during recovery",
			zap.Int64("logId", w.logId),
			zap.Int("evictedCount", evicted),
		)
	}

	logger.Ctx(ctx).Info("Dedup window recovery completed",
		zap.Int64("logId", w.logId),
		zap.Int("windowSize", w.dedupWindow.Len()),
	)

	return nil
}

// fillGapFromLog reads entries from the log to fill the gap between snapshot and current state.
func (w *idempotentWriterImpl) fillGapFromLog(ctx context.Context, logHandle LogHandle, snapshot *idempotent.DedupSnapshot) error {
	// Get all segments
	segments, err := logHandle.GetSegments(ctx)
	if err != nil {
		return fmt.Errorf("failed to get segments: %w", err)
	}

	// Determine the starting point for each segment
	var snapshotWatermarks map[int64]int64
	if snapshot != nil {
		snapshotWatermarks = snapshot.SegmentWatermarks
	} else {
		snapshotWatermarks = make(map[int64]int64)
	}

	for segId, segMeta := range segments {
		// Skip truncated segments
		if segMeta.Metadata.State == proto.SegmentState_Truncated {
			continue
		}

		// Determine starting entry ID for this segment
		startEntryId := int64(0)
		if watermark, ok := snapshotWatermarks[segId]; ok {
			startEntryId = watermark + 1
		}

		// Read entries from this segment starting from startEntryId
		err := w.readSegmentEntries(ctx, logHandle, segId, startEntryId)
		if err != nil {
			// Log warning but continue with other segments
			logger.Ctx(ctx).Warn("Failed to read segment entries for gap recovery",
				zap.Int64("logId", w.logId),
				zap.Int64("segmentId", segId),
				zap.Int64("startEntryId", startEntryId),
				zap.Error(err),
			)
		}
	}

	return nil
}

// readSegmentEntries reads entries from a segment and adds them to the dedup window.
func (w *idempotentWriterImpl) readSegmentEntries(ctx context.Context, logHandle LogHandle, segId int64, startEntryId int64) error {
	// Open a reader starting from the specified position
	from := &LogMessageId{
		SegmentId: segId,
		EntryId:   startEntryId,
	}

	reader, err := logHandle.OpenLogReader(ctx, from, fmt.Sprintf("dedup_recovery_%d", segId))
	if err != nil {
		return fmt.Errorf("failed to open log reader: %w", err)
	}
	defer reader.Close(ctx)

	count := 0
	for {
		msg, err := reader.ReadNext(ctx)
		if err != nil {
			// Check if we've reached the end of file or no entry to read
			if werr.ErrFileReaderEndOfFile.Is(err) || werr.ErrEntryNotFound.Is(err) {
				break
			}
			return fmt.Errorf("failed to read entry: %w", err)
		}

		// Only process entries within this segment
		if msg.Id.SegmentId != segId {
			break
		}

		// Add to dedup window if it has an idempotencyId
		if msg.IdempotencyId != "" {
			w.dedupWindow.Put(msg.IdempotencyId, msg.Id.SegmentId, msg.Id.EntryId, idempotent.EntryStatusCommitted)
			count++
		}
	}

	if count > 0 {
		logger.Ctx(ctx).Debug("Recovered entries from segment",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", segId),
			zap.Int("entriesRecovered", count),
		)
	}

	return nil
}

// Write writes a log message with idempotency support.
func (w *idempotentWriterImpl) Write(ctx context.Context, msg *WriteMessage) *WriteResult {
	ch := w.WriteAsync(ctx, msg)
	result := <-ch
	return result
}

// WriteAsync writes a log message asynchronously with idempotency support.
func (w *idempotentWriterImpl) WriteAsync(ctx context.Context, msg *WriteMessage) <-chan *WriteResult {
	resultCh := make(chan *WriteResult, 1)

	// Derive idempotencyId if not provided
	idempotencyId := msg.IdempotencyId
	if idempotencyId == "" {
		idempotencyId = deriveIdempotencyId(msg.Payload)
		msg.IdempotencyId = idempotencyId
	}

	w.mu.Lock()

	// Check if this idempotencyId is already committed in the dedup window
	existing := w.dedupWindow.Get(idempotencyId)
	if existing != nil && existing.Status == idempotent.EntryStatusCommitted {
		// Already committed - return success immediately
		w.mu.Unlock()
		resultCh <- &WriteResult{
			LogMessageId: &LogMessageId{
				SegmentId: existing.SegmentId,
				EntryId:   existing.EntryId,
			},
			Err: nil,
		}
		close(resultCh)
		return resultCh
	}

	// Check if this idempotencyId is already pending (in-flight write)
	if waitingList, ok := w.pendingWrites[idempotencyId]; ok {
		// Entry is pending - wait for the same result
		w.pendingWrites[idempotencyId] = append(waitingList, resultCh)
		w.mu.Unlock()
		return resultCh
	}

	// New write - we need to call the inner writer
	// Register as pending (we'll update with actual ids when we get them)
	w.pendingWrites[idempotencyId] = []chan *WriteResult{resultCh}
	w.mu.Unlock()

	// Call inner writer
	innerCh := w.innerWriter.WriteAsync(ctx, msg)

	// Handle the result in a goroutine
	go w.handleWriteResult(ctx, idempotencyId, innerCh)

	return resultCh
}

// handleWriteResult processes the result from the inner writer.
func (w *idempotentWriterImpl) handleWriteResult(ctx context.Context, idempotencyId string, innerCh <-chan *WriteResult) {
	result := <-innerCh

	w.mu.Lock()
	defer w.mu.Unlock()

	// Get waiting channels
	waitingChannels := w.pendingWrites[idempotencyId]
	delete(w.pendingWrites, idempotencyId)

	if result.Err == nil {
		// Success - record in dedup window as committed
		w.dedupWindow.Put(idempotencyId, result.LogMessageId.SegmentId, result.LogMessageId.EntryId, idempotent.EntryStatusCommitted)
		w.entriesSinceSnapshot++

		// Trigger eviction periodically
		if w.entriesSinceSnapshot%1000 == 0 {
			w.dedupWindow.Evict()
		}
	}
	// On failure, we don't record anything - next retry will be treated as a new write

	// Notify all waiting channels
	for _, ch := range waitingChannels {
		ch <- result
		close(ch)
	}
}

// snapshotLoop runs the periodic snapshot goroutine.
func (w *idempotentWriterImpl) snapshotLoop() {
	ticker := time.NewTicker(w.config.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.maybeSnapshot(context.Background())
		}
	}
}

// maybeSnapshot checks if a snapshot is needed and saves one if so.
func (w *idempotentWriterImpl) maybeSnapshot(ctx context.Context) {
	w.mu.RLock()
	shouldSnapshot := w.entriesSinceSnapshot >= w.config.SnapshotThreshold ||
		time.Since(w.lastSnapshotTime) >= w.config.SnapshotInterval
	w.mu.RUnlock()

	if !shouldSnapshot {
		return
	}

	w.saveSnapshot(ctx)
}

// saveSnapshot saves the current dedup window state to object storage.
func (w *idempotentWriterImpl) saveSnapshot(ctx context.Context) {
	w.mu.Lock()
	snapshot := idempotent.CreateSnapshotFromWindow(w.logId, w.dedupWindow, w.config.DedupWindowConfig)
	w.entriesSinceSnapshot = 0
	w.lastSnapshotTime = time.Now()
	w.mu.Unlock()

	err := w.snapshotManager.Save(ctx, snapshot)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to save dedup snapshot",
			zap.Int64("logId", w.logId),
			zap.Error(err),
		)
		return
	}

	// Cleanup old snapshots
	err = w.snapshotManager.Cleanup(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to cleanup old snapshots",
			zap.Int64("logId", w.logId),
			zap.Error(err),
		)
	}
}

// Close closes the writer and saves a final snapshot.
func (w *idempotentWriterImpl) Close(ctx context.Context) error {
	// Stop snapshot goroutine
	w.stopOnce.Do(func() {
		close(w.stopCh)
	})

	// Save final snapshot
	w.saveSnapshot(ctx)

	// Close inner writer
	return w.innerWriter.Close(ctx)
}

// GetDedupWindow returns the dedup window for testing/debugging purposes.
func (w *idempotentWriterImpl) GetDedupWindow() idempotent.DedupWindow {
	return w.dedupWindow
}

// deriveIdempotencyId generates an idempotencyId from the payload using MD5 hash.
func deriveIdempotencyId(payload []byte) string {
	hash := md5.Sum(payload)
	return hex.EncodeToString(hash[:])
}

// recoverIdempotentWriter is a helper type for recovery that provides access to segments.
type recoverIdempotentWriter interface {
	Recover(ctx context.Context, logHandle LogHandle) error
}

// segmentMetaForRecovery wraps meta.SegmentMeta to provide the Metadata field access.
type segmentMetaForRecovery = meta.SegmentMeta

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

package stagedstorage

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/conc"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/serde"
)

var (
	WriterScope = "StagedFileWriter"
)

var _ storage.Writer = (*StagedFileWriter)(nil)

// blockFlushTask represents a task to flush a block to disk
type blockFlushTask struct {
	entries      []*cache.BufferEntry
	firstEntryId int64
	lastEntryId  int64
	blockNumber  int32
	flushFuture  *conc.Future[*blockFlushResult]
}

// blockFlushResult represents the result of a block flush operation
type blockFlushResult struct {
	blockNumber  int32
	firstEntryId int64
	lastEntryId  int64
	blockSize    int64
	err          error
	entries      []*cache.BufferEntry
}

// StagedFileWriter implements staged storage for segment files:
// writes to local disk during active phase, uploads to object storage during compact phase.
// Uses quorum fence mechanism and supports LAC maintenance.
// Now uses per-block file mode: each block is written to a separate file with atomic rename.
type StagedFileWriter struct {
	mu sync.Mutex

	// Disk
	localBaseDir string
	logId        int64
	segmentId    int64
	logIdStr     string // for metrics only

	// Configuration
	maxFlushSize     int64 // Max buffer size before triggering sync
	maxBufferEntries int64 // Maximum number of entries per buffer
	maxIntervalMs    int   // Max interval to sync buffer to disk

	// object cli, only used for uploading compacted blocks
	storageCli          objectstorage.ObjectStorage
	bucket              string
	rootPath            string
	compactPolicyConfig *config.SegmentCompactionPolicy

	// write buffer
	buffer            atomic.Pointer[cache.SequentialBuffer] // Write buffer
	lastSyncTimestamp atomic.Int64

	// file state (per-block file mode)
	currentBlockNumber atomic.Int64
	blockIndexes       []*codec.IndexRecord
	blockIndexesMu     sync.Mutex
	recoveredFooter    *codec.FooterRecord // Footer recovered during initialization (only for finalized segments)

	// Concurrent flush pool and ack channel
	pool        *conc.Pool[*blockFlushResult]
	ackTaskChan chan *blockFlushTask

	// written state
	firstEntryID   atomic.Int64 // The first entryId written to disk
	lastEntryID    atomic.Int64 // The last entryId written to disk
	finalized      atomic.Bool
	fenced         atomic.Bool
	inRecoveryMode atomic.Bool
	recovered      atomic.Bool

	// Async flush management
	storageWritable              atomic.Bool // Indicates whether the segment is writable
	lastSubmittedFlushingEntryID atomic.Int64
	lastSubmittedFlushingBlockID atomic.Int64
	allUploadingTaskDone         atomic.Bool

	// Close management
	closed    atomic.Bool
	runCtx    context.Context
	runCancel context.CancelFunc
}

// NewStagedFileWriter creates a new staged file writer
func NewStagedFileWriter(ctx context.Context, bucket string, rootPath string, localBaseDir string, logId int64, segmentId int64, storageCli objectstorage.ObjectStorage, cfg *config.Configuration) (*StagedFileWriter, error) {
	return NewStagedFileWriterWithMode(ctx, bucket, rootPath, localBaseDir, logId, segmentId, storageCli, cfg, false)
}

// NewStagedFileWriterWithMode creates a new staged file writer with recovery mode option
// Now uses per-block file mode for improved crash safety and concurrent flushing.
func NewStagedFileWriterWithMode(ctx context.Context, bucket string, rootPath string, localBaseDir string, logId int64, segmentId int64, storageCli objectstorage.ObjectStorage, cfg *config.Configuration, recoveryMode bool) (*StagedFileWriter, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "NewStagedFileWriterWithMode")
	defer sp.End()
	blockSize := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize.Int64()
	maxBufferEntries := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxEntries
	maxInterval := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage.Milliseconds()
	maxConcurrentFlush := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads
	if maxConcurrentFlush <= 0 {
		maxConcurrentFlush = 4 // Default to 4 concurrent flushes
	}
	ackChanSize := max(maxConcurrentFlush*2, 100)
	logger.Ctx(ctx).Debug("creating new staged file writer (per-block mode)",
		zap.String("localBaseDir", localBaseDir), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId),
		zap.String("bucket", bucket), zap.String("rootPath", rootPath), zap.Int64("maxBlockSize", blockSize),
		zap.Int("maxBufferEntries", maxBufferEntries), zap.Int("maxInterval", maxInterval),
		zap.Int("maxConcurrentFlush", maxConcurrentFlush), zap.Bool("recoveryMode", recoveryMode))

	segmentDir := getSegmentDir(localBaseDir, logId, segmentId)
	// Ensure directory exists
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		logger.Ctx(ctx).Warn("failed to create directory", zap.String("segmentDir", segmentDir), zap.Error(err))
		return nil, fmt.Errorf("create directory: %w", err)
	}

	logger.Ctx(ctx).Debug("segment directory prepared", zap.String("segmentDir", segmentDir))

	// Create context for async operations
	runCtx, runCancel := context.WithCancel(context.Background())

	writer := &StagedFileWriter{
		localBaseDir:        localBaseDir,
		logId:               logId,
		segmentId:           segmentId,
		logIdStr:            fmt.Sprintf("%d", logId),
		bucket:              bucket,
		rootPath:            rootPath,
		storageCli:          storageCli,
		compactPolicyConfig: &cfg.Woodpecker.Logstore.SegmentCompactionPolicy,
		blockIndexes:        make([]*codec.IndexRecord, 0),
		maxFlushSize:        blockSize,
		maxBufferEntries:    int64(maxBufferEntries),
		maxIntervalMs:       maxInterval,
		pool:                conc.NewPool[*blockFlushResult](maxConcurrentFlush, conc.WithPreAlloc(true)),
		ackTaskChan:         make(chan *blockFlushTask, ackChanSize),
		runCtx:              runCtx,
		runCancel:           runCancel,
	}

	// Initialize atomic values
	writer.firstEntryID.Store(-1)
	writer.lastEntryID.Store(-1)
	writer.currentBlockNumber.Store(0)
	writer.finalized.Store(false)
	writer.fenced.Store(false)
	writer.closed.Store(false)
	writer.recovered.Store(false)
	writer.storageWritable.Store(true)
	writer.lastSubmittedFlushingEntryID.Store(-1)
	writer.lastSubmittedFlushingBlockID.Store(-1)
	writer.allUploadingTaskDone.Store(false)
	writer.lastSyncTimestamp.Store(0) // Set to 0 so first write will trigger sync after interval

	// Set default block size if not specified
	if writer.maxFlushSize <= 0 {
		writer.maxFlushSize = 2 * 1024 * 1024 // 2MB default
		logger.Ctx(ctx).Debug("using default block size", zap.Int64("defaultBlockSize", writer.maxFlushSize))
	}

	logger.Ctx(ctx).Debug("writer configuration initialized",
		zap.Int64("maxFlushSize", writer.maxFlushSize), zap.Int64("maxBufferEntries", writer.maxBufferEntries), zap.Int("maxIntervalMs", writer.maxIntervalMs))

	// Initialize buffer
	startEntryId := int64(0)

	shouldInRecoveryMode := writer.determineIfNeedRecoveryMode(recoveryMode)
	writer.inRecoveryMode.Store(shouldInRecoveryMode)

	if shouldInRecoveryMode {
		logger.Ctx(ctx).Debug("recovery mode enabled, attempting to recover from block files")
		// Try to recover from existing block files
		if err := writer.recoverFromBlockFilesUnsafe(ctx); err != nil {
			logger.Ctx(ctx).Warn("failed to recover from block files", zap.String("segmentDir", segmentDir), zap.Error(err))
			runCancel()
			writer.pool.Release()
			return nil, fmt.Errorf("recover from block files: %w", err)
		}
		if writer.lastEntryID.Load() != -1 {
			startEntryId = writer.lastEntryID.Load() + 1
			logger.Ctx(ctx).Debug("recovered writer state",
				zap.Int64("lastEntryID", writer.lastEntryID.Load()), zap.Int64("nextStartEntryId", startEntryId), zap.Bool("recovered", writer.recovered.Load()))
		}
	} else {
		// Clean up any existing block files for fresh start
		if err := writer.cleanupBlockFilesUnsafe(ctx); err != nil {
			logger.Ctx(ctx).Warn("failed to cleanup existing block files", zap.String("segmentDir", segmentDir), zap.Error(err))
		}
	}

	initialBuffer := cache.NewSequentialBuffer(logId, segmentId, startEntryId, writer.maxBufferEntries)
	writer.buffer.Store(initialBuffer)

	logger.Ctx(ctx).Debug("buffer initialized",
		zap.Int64("startEntryId", startEntryId), zap.Int64("maxBufferEntries", writer.maxBufferEntries), zap.String("bufferInstance", fmt.Sprintf("%p", initialBuffer)))

	// Start async goroutines: run() for periodic sync, ack() for sequential confirmation
	go writer.run()
	go writer.ack()

	logger.Ctx(ctx).Info("staged file writer created successfully (per-block mode)",
		zap.String("segmentDir", segmentDir), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId),
		zap.Int64("blockSize", blockSize), zap.Bool("recoveryMode", recoveryMode), zap.Int64("startEntryId", startEntryId),
		zap.Bool("recovered", writer.recovered.Load()), zap.String("writerInstance", fmt.Sprintf("%p", writer)))

	return writer, nil
}

// run is the async goroutine that handles periodic sync
func (w *StagedFileWriter) run() {
	ticker := time.NewTicker(time.Duration(w.maxIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	logger.Ctx(w.runCtx).Debug("StagedFileWriter run goroutine started", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))

	for {
		select {
		case <-w.runCtx.Done():
			// Context cancelled, exit
			logger.Ctx(w.runCtx).Debug("StagedFileWriter run goroutine stopping due to context cancellation")
			return
		case <-ticker.C:
			// Periodic sync check
			autoSyncErr := w.Sync(w.runCtx)
			if autoSyncErr != nil {
				logger.Ctx(w.runCtx).Warn("auto sync error", zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId), zap.Error(autoSyncErr))
			}
			// Reset ticker after sync to ensure consistent intervals
			ticker.Reset(time.Duration(w.maxIntervalMs) * time.Millisecond)
		}
	}
}

// ack processes completed flush tasks in order and performs final rename
func (w *StagedFileWriter) ack() {
	logger.Ctx(w.runCtx).Debug("StagedFileWriter ack goroutine started", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))

	for {
		select {
		case <-w.runCtx.Done():
			logger.Ctx(w.runCtx).Debug("StagedFileWriter ack goroutine stopping due to context cancellation")
			return
		case task, ok := <-w.ackTaskChan:
			if !ok {
				logger.Ctx(w.runCtx).Debug("StagedFileWriter ack goroutine stopping due to channel close")
				return
			}
			if task.entries == nil {
				// Termination signal
				logger.Ctx(context.TODO()).Debug("received termination signal in ack goroutine", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))
				w.allUploadingTaskDone.Store(true)
				return
			}

			// Wait for the specific flush task to complete
			if task.flushFuture == nil {
				logger.Ctx(w.runCtx).Warn("task has no flush future, skipping ack",
					zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int32("blockNumber", task.blockNumber))
				continue
			}

			result := task.flushFuture.Value()
			flushErr := task.flushFuture.Err()

			if flushErr != nil || (result != nil && result.err != nil) {
				// Flush failed
				actualErr := flushErr
				if actualErr == nil && result != nil {
					actualErr = result.err
				}
				logger.Ctx(w.runCtx).Warn("flush task failed",
					zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int32("blockNumber", task.blockNumber), zap.Error(actualErr))
				w.storageWritable.Store(false)
				w.notifyFlushError(task.entries, actualErr)
				continue
			}

			// Check for flush errors (set by processFlushTask)
			if !w.storageWritable.Load() {
				logger.Ctx(w.runCtx).Warn("storage not writable, skipping ack",
					zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int32("blockNumber", task.blockNumber))
				continue
			}

			// Perform final rename: completed -> final
			blockId := int64(task.blockNumber)
			completedPath := getCompletedBlockPath(w.localBaseDir, w.logId, w.segmentId, blockId)
			finalPath := getBlockFilePath(w.localBaseDir, w.logId, w.segmentId, blockId)

			if err := os.Rename(completedPath, finalPath); err != nil {
				logger.Ctx(w.runCtx).Warn("failed to rename completed block to final",
					zap.String("completedPath", completedPath), zap.String("finalPath", finalPath), zap.Error(err))
				w.notifyFlushError(task.entries, werr.ErrStorageNotWritable.WithCauseErr(err))
				w.storageWritable.Store(false)
				continue
			}

			// Update entry IDs
			if w.firstEntryID.Load() == -1 {
				w.firstEntryID.Store(result.firstEntryId)
			}
			w.lastEntryID.Store(result.lastEntryId)

			// Update entry tracking
			if w.firstEntryID.Load() == -1 {
				w.firstEntryID.Store(task.firstEntryId)
			}
			w.lastEntryID.Store(task.lastEntryId)

			logger.Ctx(w.runCtx).Debug("block flush completed and confirmed",
				zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int32("blockNumber", task.blockNumber),
				zap.Int64("firstEntryId", task.firstEntryId), zap.Int64("lastEntryId", task.lastEntryId))

			// Notify success
			w.notifyFlushSuccess(task.entries)
		}
	}
}

// Sync forces immediate sync of all buffered data
func (w *StagedFileWriter) Sync(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "Sync")
	defer sp.End()
	startTime := time.Now()
	// Use syncMu to ensure only one sync can proceed at a time
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.storageWritable.Load() {
		return nil
	}

	currentBuffer := w.buffer.Load()
	if currentBuffer == nil {
		return nil
	}

	// Check if sync is needed - simply check if there's any data to sync
	bufferSize := currentBuffer.DataSize.Load()
	entryCount := currentBuffer.GetExpectedNextEntryId() - currentBuffer.GetFirstEntryId()
	hasEntries := entryCount > 0

	// Sync if there's any data to sync
	needSync := bufferSize > 0 || hasEntries

	if needSync {
		logger.Ctx(ctx).Debug("Sync: triggering rollBufferAndFlush", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int64("entryCount", entryCount), zap.Int64("bufferSize", bufferSize), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		// Add global writer lock to prevent race conditions with WriteDataAsync
		w.rollBufferAndSubmitFlushTaskUnsafe(ctx)
	}

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "sync", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "sync", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return nil
}

// rollBufferAndSubmitFlushTaskUnsafe rolls the current buffer and submits flush task to pool (must be called with mu held)
func (w *StagedFileWriter) rollBufferAndSubmitFlushTaskUnsafe(ctx context.Context) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "rollBufferAndFlushUnsafe")
	defer sp.End()
	startTime := time.Now()
	currentBuffer := w.buffer.Load()
	if currentBuffer == nil || currentBuffer.GetExpectedNextEntryId() <= currentBuffer.GetFirstEntryId() {
		return
	}

	// Create new buffer for incoming writes
	nextStartEntryId := currentBuffer.GetExpectedNextEntryId()
	newBuffer := cache.NewSequentialBuffer(w.logId, w.segmentId, nextStartEntryId, w.maxBufferEntries)
	w.buffer.Store(newBuffer)

	// Get entries from old buffer
	firstEntryId := currentBuffer.GetFirstEntryId()

	entries, err := currentBuffer.ReadEntriesRange(firstEntryId, currentBuffer.GetExpectedNextEntryId())
	if err != nil || len(entries) == 0 {
		return
	}

	// Create flush task
	blockNumber := w.currentBlockNumber.Load()

	flushTask := &blockFlushTask{
		entries:      entries,
		firstEntryId: entries[0].EntryId,
		lastEntryId:  entries[len(entries)-1].EntryId,
		blockNumber:  int32(blockNumber),
	}

	// Update flushing size
	w.lastSubmittedFlushingEntryID.Store(flushTask.lastEntryId)
	w.lastSubmittedFlushingBlockID.Store(int64(flushTask.blockNumber))

	// Increment block number for next block
	w.currentBlockNumber.Add(1)
	w.lastSyncTimestamp.Store(time.Now().UnixMilli())

	logger.Ctx(ctx).Debug("Submitting flush task to pool",
		zap.Int64("firstEntryId", flushTask.firstEntryId), zap.Int64("lastEntryId", flushTask.lastEntryId),
		zap.Int32("blockNumber", flushTask.blockNumber), zap.Int("entriesCount", len(flushTask.entries)))

	// Submit flush task to pool for concurrent execution, capture the future
	taskCopy := flushTask
	future := w.pool.Submit(func() (*blockFlushResult, error) {
		return w.processFlushTask(w.runCtx, taskCopy), nil
	})
	flushTask.flushFuture = future

	// Submit to ack channel (for ordered confirmation)
	select {
	case w.ackTaskChan <- flushTask:
		// Submitted successfully
	case <-ctx.Done():
		logger.Ctx(ctx).Warn("Context cancelled while submitting to ack channel", zap.Int32("blockNumber", flushTask.blockNumber))
		w.notifyFlushError(flushTask.entries, ctx.Err())
		return
	case <-w.runCtx.Done():
		logger.Ctx(ctx).Warn("Writer context cancelled while submitting to ack channel", zap.Int32("blockNumber", flushTask.blockNumber))
		w.notifyFlushError(flushTask.entries, werr.ErrFileWriterAlreadyClosed)
		return
	}

	logger.Ctx(ctx).Debug("Flush task submitted to pool successfully",
		zap.Int64("firstEntryId", flushTask.firstEntryId), zap.Int64("lastEntryId", flushTask.lastEntryId),
		zap.Int32("blockNumber", flushTask.blockNumber), zap.Int("entriesCount", len(flushTask.entries)))

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "rollBuffer", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "rollBuffer", "success").Observe(float64(time.Since(startTime).Milliseconds()))
}

// processFlushTask processes a flush task by writing data to per-block file
// Uses three-stage atomic write: inflight -> completed -> final (final rename done by ack goroutine)
func (w *StagedFileWriter) processFlushTask(ctx context.Context, task *blockFlushTask) *blockFlushResult {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "processFlushTask")
	defer sp.End()
	startTime := time.Now()

	result := &blockFlushResult{
		blockNumber:  task.blockNumber,
		firstEntryId: task.firstEntryId,
		lastEntryId:  task.lastEntryId,
		entries:      task.entries,
	}

	// Add nil check for safety
	if task == nil {
		result.err = fmt.Errorf("nil task")
		return result
	}

	if w.fenced.Load() {
		logger.Ctx(ctx).Debug("processFlushTask: writer fenced", zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId), zap.Int32("blockID", task.blockNumber))
		result.err = werr.ErrSegmentFenced
		w.notifyFlushError(task.entries, werr.ErrSegmentFenced)
		return result
	}

	if w.finalized.Load() {
		logger.Ctx(ctx).Debug("processFlushTask: writer finalized", zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId), zap.Int32("blockID", task.blockNumber))
		result.err = werr.ErrFileWriterFinalized
		w.notifyFlushError(task.entries, werr.ErrFileWriterFinalized)
		return result
	}

	if !w.storageWritable.Load() {
		logger.Ctx(ctx).Warn("processFlushTask: storage not writable", zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId), zap.Int32("blockID", task.blockNumber))
		result.err = werr.ErrStorageNotWritable
		w.notifyFlushError(task.entries, werr.ErrStorageNotWritable)
		return result
	}

	blockId := int64(task.blockNumber)

	logger.Ctx(ctx).Debug("starting to process flush task (per-block mode)",
		zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId), zap.Int64("blockId", blockId),
		zap.Int64("firstEntryId", task.firstEntryId), zap.Int64("lastEntryId", task.lastEntryId), zap.Int("entriesCount", len(task.entries)))

	// Serialize all data records to calculate block length and CRC
	var blockDataBuffer []byte
	for _, entry := range task.entries {
		dataRecord := &codec.DataRecord{
			Payload: entry.Data,
		}
		encodedRecord := codec.EncodeRecord(dataRecord)
		blockDataBuffer = append(blockDataBuffer, encodedRecord...)
	}

	// Calculate block length and CRC
	blockLength := uint32(len(blockDataBuffer))
	blockCrc := crc32.ChecksumIEEE(blockDataBuffer)

	// Create block header record
	blockHeaderRecord := &codec.BlockHeaderRecord{
		BlockNumber:  task.blockNumber,
		FirstEntryID: task.firstEntryId,
		LastEntryID:  task.lastEntryId,
		BlockLength:  blockLength,
		BlockCrc:     blockCrc,
	}
	encodedHeader := codec.EncodeRecord(blockHeaderRecord)

	// Combine header and data
	blockContent := append(encodedHeader, blockDataBuffer...)
	totalBlockSize := int64(len(blockContent))

	// Stage 1: Write to inflight file
	inflightPath := getInflightBlockPath(w.localBaseDir, w.logId, w.segmentId, blockId)
	inflightFile, err := os.Create(inflightPath)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to create inflight block file", zap.String("inflightPath", inflightPath), zap.Error(err))
		result.err = werr.ErrStorageNotWritable.WithCauseErr(err)
		w.storageWritable.Store(false)
		w.notifyFlushError(task.entries, result.err)
		return result
	}

	n, err := inflightFile.Write(blockContent)
	if err != nil || n != len(blockContent) {
		inflightFile.Close()
		os.Remove(inflightPath)
		if err == nil {
			err = fmt.Errorf("incomplete write: wrote %d of %d bytes", n, len(blockContent))
		}
		logger.Ctx(ctx).Warn("failed to write inflight block file", zap.String("inflightPath", inflightPath), zap.Error(err))
		result.err = werr.ErrStorageNotWritable.WithCauseErr(err)
		w.storageWritable.Store(false)
		w.notifyFlushError(task.entries, result.err)
		return result
	}

	// Sync to disk
	if err := inflightFile.Sync(); err != nil {
		inflightFile.Close()
		os.Remove(inflightPath)
		logger.Ctx(ctx).Warn("failed to sync inflight block file", zap.String("inflightPath", inflightPath), zap.Error(err))
		result.err = werr.ErrStorageNotWritable.WithCauseErr(err)
		w.storageWritable.Store(false)
		w.notifyFlushError(task.entries, result.err)
		return result
	}
	inflightFile.Close()

	// Stage 2: Rename inflight -> completed
	completedPath := getCompletedBlockPath(w.localBaseDir, w.logId, w.segmentId, blockId)
	if err := os.Rename(inflightPath, completedPath); err != nil {
		os.Remove(inflightPath)
		logger.Ctx(ctx).Warn("failed to rename inflight to completed",
			zap.String("inflightPath", inflightPath), zap.String("completedPath", completedPath), zap.Error(err))
		result.err = werr.ErrStorageNotWritable.WithCauseErr(err)
		w.storageWritable.Store(false)
		w.notifyFlushError(task.entries, result.err)
		return result
	}

	// Add block index (final rename will be done by ack goroutine)
	indexRecord := &codec.IndexRecord{
		BlockNumber:  task.blockNumber,
		StartOffset:  0, // Not meaningful for per-block files
		BlockSize:    uint32(totalBlockSize),
		FirstEntryID: task.firstEntryId,
		LastEntryID:  task.lastEntryId,
	}

	w.blockIndexesMu.Lock()
	w.blockIndexes = append(w.blockIndexes, indexRecord)
	w.blockIndexesMu.Unlock()

	result.blockSize = totalBlockSize

	logger.Ctx(ctx).Debug("block flush completed (waiting for ack)",
		zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId), zap.Int64("blockId", blockId),
		zap.Int64("firstEntryId", task.firstEntryId), zap.Int64("lastEntryId", task.lastEntryId), zap.Int64("blockSize", totalBlockSize))

	// Update metrics
	metrics.WpFileFlushBytesWritten.WithLabelValues(w.logIdStr).Add(float64(totalBlockSize))
	metrics.WpFileFlushLatency.WithLabelValues(w.logIdStr).Observe(float64(time.Since(startTime).Milliseconds()))

	return result
}

func (f *StagedFileWriter) awaitAllFlushTasks(ctx context.Context) error {
	logger.Ctx(ctx).Info("awaiting completion of all block flush tasks", zap.Int64("logId", f.logId), zap.Int64("segmentId", f.segmentId))

	// Wait for all pool tasks to complete using polling
	maxWaitTime := 10 * time.Second
	startTime := time.Now()

	for {
		runningTasks := f.pool.Running()
		if runningTasks == 0 {
			logger.Ctx(ctx).Info("all pool tasks completed", zap.Int64("logId", f.logId), zap.Int64("segmentId", f.segmentId))
			break
		}

		if time.Since(startTime) > maxWaitTime {
			logger.Ctx(ctx).Warn("timeout waiting for pool tasks",
				zap.Int64("logId", f.logId), zap.Int64("segmentId", f.segmentId), zap.Int("runningTasks", runningTasks))
			return errors.New("timeout waiting for pool tasks to complete")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
			continue
		}
	}

	// Send termination signal to ack goroutine
	select {
	case f.ackTaskChan <- &blockFlushTask{
		entries:      nil,
		firstEntryId: -1,
		lastEntryId:  -1,
		blockNumber:  -1,
	}:
		logger.Ctx(ctx).Info("termination signal sent to ack goroutine", zap.Int64("logId", f.logId), zap.Int64("segmentId", f.segmentId))
	case <-ctx.Done():
		return ctx.Err()
	}

	// Wait for ack goroutine to set the done flag
	ackWaitTime := 15 * time.Second // TODO configurable
	ackStartTime := time.Now()

	for {
		if f.allUploadingTaskDone.Load() {
			logger.Ctx(ctx).Info("ack goroutine completed processing all tasks", zap.Int64("logId", f.logId), zap.Int64("segmentId", f.segmentId))
			return nil
		}

		// Check timeout
		if time.Since(ackStartTime) > ackWaitTime {
			logger.Ctx(ctx).Warn("timeout waiting for ack goroutine to complete",
				zap.Int64("logId", f.logId), zap.Int64("segmentId", f.segmentId),
				zap.Bool("allUploadingTaskDone", f.allUploadingTaskDone.Load()), zap.Duration("elapsed", time.Since(ackStartTime)))
			return errors.New("timeout waiting for ack goroutine to complete")
		}

		// Short sleep to avoid busy waiting
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
			continue
		}
	}
}

// notifyFlushError notifies all result channels of flush error
func (w *StagedFileWriter) notifyFlushError(entries []*cache.BufferEntry, err error) {
	// Notify all pending entries result channels in sequential order
	for _, entry := range entries {
		if entry.NotifyChan != nil {
			cache.NotifyPendingEntryDirectly(context.TODO(), w.logId, w.segmentId, entry.EntryId, entry.NotifyChan, entry.EntryId, err)
		}
	}
}

// notifyFlushSuccess notifies all result channels of flush success
func (w *StagedFileWriter) notifyFlushSuccess(entries []*cache.BufferEntry) {
	// Notify all pending entries result channels in sequential order
	for _, entry := range entries {
		if entry.NotifyChan != nil {
			cache.NotifyPendingEntryDirectly(context.TODO(), w.logId, w.segmentId, entry.EntryId, entry.NotifyChan, entry.EntryId, nil)
		}
	}
}

// WriteDataAsync writes data asynchronously using buffer
func (w *StagedFileWriter) WriteDataAsync(ctx context.Context, entryId int64, data []byte, resultCh channel.ResultChannel) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "WriteDataAsync")
	defer sp.End()
	logger.Ctx(ctx).Debug("WriteDataAsync called", zap.Int64("entryId", entryId), zap.Int("dataLen", len(data)))

	if w.closed.Load() {
		logger.Ctx(ctx).Debug("WriteDataAsync: writer closed")
		return entryId, werr.ErrFileWriterAlreadyClosed
	}

	if w.finalized.Load() {
		logger.Ctx(ctx).Debug("WriteDataAsync: writer finalized")
		return entryId, werr.ErrFileWriterFinalized
	}

	if !w.storageWritable.Load() {
		logger.Ctx(ctx).Debug("WriteDataAsync: storage not writable")
		return entryId, werr.ErrStorageNotWritable
	}

	if w.inRecoveryMode.Load() {
		logger.Ctx(ctx).Debug("WriteDataAsync: writer in recovery mode")
		return entryId, werr.ErrFileWriterInRecoveryMode
	}

	// Validate empty payload
	if len(data) == 0 {
		logger.Ctx(ctx).Warn("WriteDataAsync: attempting to write rejected, data cannot be empty", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("inst", fmt.Sprintf("%p", w)))
		return entryId, werr.ErrEmptyPayload
	}

	// Check for duplicates
	w.mu.Lock()
	if entryId <= w.lastEntryID.Load() {
		// If entryId is less than or equal to lastEntryID, it indicates that the entry has already been written to storage. Return immediately.
		cache.NotifyPendingEntryDirectly(ctx, w.logId, w.segmentId, entryId, resultCh, entryId, nil)
		w.mu.Unlock()
		return entryId, nil
	}
	if entryId <= w.lastSubmittedFlushingEntryID.Load() {
		// If entryId is less than or equal to lastSubmittedFlushingEntryID, it indicates that the entry has already been submitted for flushing. Store the channel for later notification.
		w.mu.Unlock()
		return entryId, nil
	}

	// Add entry to buffer (keep lock held to prevent race with rollBufferAndFlushUnsafe)
	currentBuffer := w.buffer.Load()
	if currentBuffer == nil {
		w.mu.Unlock()
		return entryId, werr.ErrFileWriterAlreadyClosed
	}

	// Try to add to buffer
	id, err := currentBuffer.WriteEntryWithNotify(entryId, data, resultCh)
	if err != nil {
		// write to buffer failed
		logger.Ctx(ctx).Warn("failed to write entry to buffer", zap.Int64("entryId", entryId), zap.Int("dataLen", len(data)), zap.Error(err))
		w.mu.Unlock()
		return id, err
	}
	logger.Ctx(ctx).Debug("AppendAsync: successfully written to buffer", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int64("entryId", entryId), zap.Int64("id", id), zap.Int64("expectedNextEntryId", currentBuffer.ExpectedNextEntryId.Load()), zap.String("inst", fmt.Sprintf("%p", w)), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))

	// Check if immediate sync is needed (still holding lock)
	bufferSize := currentBuffer.DataSize.Load()
	entryCount := currentBuffer.GetExpectedNextEntryId() - currentBuffer.GetFirstEntryId()
	timeSinceLastSync := time.Now().UnixMilli() - w.lastSyncTimestamp.Load()
	w.mu.Unlock()

	logger.Ctx(ctx).Debug("WriteDataAsync sync check",
		zap.Int64("bufferSize", bufferSize), zap.Int64("maxFlushSize", w.maxFlushSize),
		zap.Int64("entryCount", entryCount), zap.Int64("maxBufferEntries", w.maxBufferEntries),
		zap.Int64("timeSinceLastSync", timeSinceLastSync), zap.Int64("lastSyncTimestamp", w.lastSyncTimestamp.Load()))

	// Immediate sync conditions:
	// - Buffer size exceeded
	// - Too many entries
	if bufferSize >= w.maxFlushSize || entryCount >= w.maxBufferEntries {
		logger.Ctx(ctx).Info("Triggering immediate sync from WriteDataAsync",
			zap.Int64("bufferSize", bufferSize), zap.Int64("entryCount", entryCount), zap.Int64("timeSinceLastSync", timeSinceLastSync))
		triggerSyncErr := w.Sync(ctx)
		if triggerSyncErr != nil {
			logger.Ctx(ctx).Warn("reach max buffer size, but trigger sync failed",
				zap.Int64("bufferSize", bufferSize), zap.Int64("entryCount", entryCount),
				zap.Int64("timeSinceLastSync", timeSinceLastSync), zap.Error(triggerSyncErr))
		}
	}
	return entryId, nil
}

// Finalize finalizes the writer and writes the footer
func (w *StagedFileWriter) Finalize(ctx context.Context, lac int64) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "Finalize")
	defer sp.End()
	startTime := time.Now()

	if !w.finalized.CompareAndSwap(false, true) {
		// if already finalized, return fast
		return w.lastEntryID.Load(), nil
	}

	// Sync all pending data first
	if err := w.Sync(ctx); err != nil {
		return w.lastEntryID.Load(), err
	}

	// wait all flush
	waitErr := w.awaitAllFlushTasks(ctx)
	if waitErr != nil {
		logger.Ctx(ctx).Warn("wait flush error before finalize", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Error(waitErr))
		return -1, waitErr
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Lock to read blockIndexes safely
	w.blockIndexesMu.Lock()
	blockIndexesCopy := make([]*codec.IndexRecord, len(w.blockIndexes))
	copy(blockIndexesCopy, w.blockIndexes)
	blockIndexesLen := len(w.blockIndexes)
	w.blockIndexesMu.Unlock()

	// Serialize footer and indexes using serde
	footerData, footer := serde.SerializeFooterAndIndexes(blockIndexesCopy, lac)

	// Two-stage atomic write for footer: write to inflight, then rename
	footerInflightPath := getLocalFooterInflightPath(w.localBaseDir, w.logId, w.segmentId)
	footerPath := getLocalFooterBlockPath(w.localBaseDir, w.logId, w.segmentId)

	// Stage 1: Write to footer.blk.inflight
	footerFile, err := os.OpenFile(footerInflightPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to create footer inflight file",
			zap.String("footerInflightPath", footerInflightPath), zap.Error(err))
		return w.lastEntryID.Load(), fmt.Errorf("create footer inflight file: %w", err)
	}

	n, err := footerFile.Write(footerData)
	if err != nil || n != len(footerData) {
		footerFile.Close()
		os.Remove(footerInflightPath)
		if err == nil {
			err = fmt.Errorf("incomplete write: wrote %d of %d bytes", n, len(footerData))
		}
		logger.Ctx(ctx).Warn("failed to write footer inflight file",
			zap.String("footerInflightPath", footerInflightPath), zap.Error(err))
		return w.lastEntryID.Load(), fmt.Errorf("write footer inflight file: %w", err)
	}

	if err := footerFile.Sync(); err != nil {
		footerFile.Close()
		os.Remove(footerInflightPath)
		logger.Ctx(ctx).Warn("failed to sync footer inflight file",
			zap.String("footerInflightPath", footerInflightPath), zap.Error(err))
		return w.lastEntryID.Load(), fmt.Errorf("sync footer inflight file: %w", err)
	}
	footerFile.Close()

	// Stage 2: Rename footer.blk.inflight to footer.blk
	if err := os.Rename(footerInflightPath, footerPath); err != nil {
		os.Remove(footerInflightPath)
		logger.Ctx(ctx).Warn("failed to rename footer inflight to final",
			zap.String("footerInflightPath", footerInflightPath),
			zap.String("footerPath", footerPath), zap.Error(err))
		return w.lastEntryID.Load(), fmt.Errorf("rename footer inflight to final: %w", err)
	}

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "finalize", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "finalize", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	logger.Ctx(ctx).Debug("finalized staged file (per-block mode)",
		zap.Int64("lastEntryId", w.lastEntryID.Load()), zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId),
		zap.Int("blockCount", blockIndexesLen), zap.Uint64("totalSize", footer.TotalSize))
	w.recoveredFooter = footer
	w.finalized.Store(true)
	return w.lastEntryID.Load(), nil
}


// GetLastEntryId returns the last entry ID written
func (w *StagedFileWriter) GetLastEntryId(ctx context.Context) int64 {
	return w.lastEntryID.Load()
}

// GetFirstEntryId returns the first entry ID written
func (w *StagedFileWriter) GetFirstEntryId(ctx context.Context) int64 {
	return w.firstEntryID.Load()
}

func (w *StagedFileWriter) GetBlockCount(ctx context.Context) int64 {
	return w.lastSubmittedFlushingBlockID.Load()
}

// Close closes the writer
func (w *StagedFileWriter) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "Close")
	defer sp.End()
	startTime := time.Now()
	if !w.closed.CompareAndSwap(false, true) { // mark close, and there will be no more add and sync in the future
		logger.Ctx(ctx).Info("Close: already closed, skip", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))
		return nil
	}
	logger.Ctx(ctx).Info("Close: trigger sync before close", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))
	err := w.Sync(ctx) // manual sync all pending append operation
	if err != nil {
		logger.Ctx(ctx).Warn("sync error before close", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Error(err))
	}

	// wait all flush
	waitErr := w.awaitAllFlushTasks(ctx)
	if waitErr != nil {
		logger.Ctx(ctx).Warn("wait flush error before close", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Error(waitErr))
		return waitErr
	}

	// Cancel async operations
	w.runCancel()

	// Release pool
	if w.pool != nil {
		w.pool.Release()
	}

	// Close channels
	close(w.ackTaskChan)

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "close", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "close", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

// Fence marks the writer as fenced
func (w *StagedFileWriter) Fence(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "Fence")
	defer sp.End()
	startTime := time.Now()

	// If already fenced, return idempotently
	if w.fenced.Load() {
		logger.Ctx(ctx).Debug("StagedFileWriter already fenced, returning idempotently", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))
		return w.GetLastEntryId(ctx), nil
	}

	// Sync all pending data first
	if err := w.Sync(ctx); err != nil {
		return w.GetLastEntryId(ctx), err
	}

	// Wait for all pending flush tasks using polling
	maxWaitTime := 10 * time.Second
	startTime2 := time.Now()
	for {
		runningTasks := w.pool.Running()
		if runningTasks == 0 {
			break
		}
		if time.Since(startTime2) > maxWaitTime {
			logger.Ctx(ctx).Warn("timeout waiting for pool tasks in Fence",
				zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int("runningTasks", runningTasks))
			break
		}
		select {
		case <-ctx.Done():
			return w.GetLastEntryId(ctx), ctx.Err()
		case <-time.After(50 * time.Millisecond):
			continue
		}
	}

	// Mark as fenced and stop accepting writes
	w.fenced.Store(true)

	lastEntryId := w.GetLastEntryId(ctx)
	logger.Ctx(ctx).Info("Successfully marked StagedFileWriter as fenced",
		zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int64("lastEntryId", lastEntryId))

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "fence", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "fence", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return lastEntryId, nil
}

// Compact performs compaction by reading local file, merging blocks and uploading to minio
func (w *StagedFileWriter) Compact(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "Compact")
	defer sp.End()
	startTime := time.Now()

	w.mu.Lock()
	defer w.mu.Unlock()

	logger.Ctx(ctx).Info("starting staged file segment compaction",
		zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int("currentBlockCount", len(w.blockIndexes)))

	// Ensure segment is finalized before compaction
	if !w.finalized.Load() {
		logger.Ctx(ctx).Warn("segment must be finalized before compaction",
			zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))
		return -1, fmt.Errorf("segment must be finalized before compaction")
	}

	// Check if we have any blocks to compact
	if len(w.blockIndexes) == 0 {
		logger.Ctx(ctx).Info("no blocks to compact",
			zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))
		return -1, nil
	}

	// Read and validate footer LAC against segment data for completeness
	lac, err := w.validateLACAlignment(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("LAC validation failed", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Error(err))
		return -1, err
	}

	logger.Ctx(ctx).Info("LAC validation passed for compaction",
		zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int64("lac", lac),
		zap.Int64("firstEntryID", w.firstEntryID.Load()), zap.Int64("lastEntryID", w.lastEntryID.Load()))

	// Get target block size for compaction
	maxCompactedBlockSize := w.compactPolicyConfig.MaxBytes.Int64()
	if maxCompactedBlockSize <= 0 {
		maxCompactedBlockSize = 2 * 1024 * 1024 // Default 2MB
	}

	// Read and merge blocks from local file, then upload to minio
	newBlockIndexes, fileSizeAfterCompact, err := w.readLocalFileAndUploadToMinio(ctx, maxCompactedBlockSize)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to read local file and upload to minio", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Error(err))
		return -1, fmt.Errorf("failed to read local file and upload to minio: %w", err)
	}

	if len(newBlockIndexes) == 0 {
		logger.Ctx(ctx).Info("no blocks uploaded during compaction",
			zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))
		return -1, nil
	}

	// Create footer with compacted flag and LAC, then upload
	footerSize, err := w.uploadCompactedFooter(ctx, newBlockIndexes, fileSizeAfterCompact, lac)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to upload compacted footer", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Error(err))
		return -1, fmt.Errorf("failed to upload compacted footer: %w", err)
	}

	totalSize := fileSizeAfterCompact + footerSize

	// Update internal state
	originalBlockCount := len(w.blockIndexes)
	w.blockIndexes = newBlockIndexes

	logger.Ctx(ctx).Info("successfully compacted staged file segment",
		zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int("originalBlockCount", originalBlockCount),
		zap.Int("compactedBlockCount", len(newBlockIndexes)), zap.Int64("totalSizeAfterCompact", totalSize),
		zap.Int64("maxCompactedBlockSize", maxCompactedBlockSize), zap.Int64("costMs", time.Since(startTime).Milliseconds()))

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "compact", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "compact", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return totalSize, nil
}

// mergeBlockTask represents a task to merge multiple blocks into one
type mergeBlockTask struct {
	blocks      []*codec.IndexRecord // Original blocks to be merged
	nextEntryID int64                // Next entry ID after this merge block
}

// blockReadResult represents the result of reading a block from local file
type blockReadResult struct {
	blockIndex *codec.IndexRecord
	blockData  []byte
	error      error
}

// mergedBlockUploadResult represents the result of uploading a merged block
type mergedBlockUploadResult struct {
	blockIndex *codec.IndexRecord
	blockSize  int64
	error      error
}

// readLocalFileAndUploadToMinio reads blocks from local file, merges them and uploads to minio
func (w *StagedFileWriter) readLocalFileAndUploadToMinio(ctx context.Context, targetBlockSize int64) ([]*codec.IndexRecord, int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "readLocalFileAndUploadToMinio")
	defer sp.End()

	// Plan merge tasks
	mergeTasks := w.planMergeBlockTasks(targetBlockSize)
	if len(mergeTasks) == 0 {
		return nil, 0, nil
	}

	logger.Ctx(ctx).Info("planned merge tasks",
		zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int("originalBlocks", len(w.blockIndexes)), zap.Int("mergeTasks", len(mergeTasks)))

	// Create pool for concurrent merge block uploads
	maxConcurrentUploads := w.compactPolicyConfig.MaxParallelUploads
	if maxConcurrentUploads <= 0 {
		maxConcurrentUploads = 4 // Default to 4 concurrent uploads
	}

	uploadPool := conc.NewPool[*mergedBlockUploadResult](maxConcurrentUploads, conc.WithPreAlloc(true))
	defer uploadPool.Release()

	// Submit all merge tasks to the pool
	var futures []*conc.Future[*mergedBlockUploadResult]
	for i, task := range mergeTasks {
		// Capture variables for closure
		taskCopy := task
		mergedBlockID := int64(i)

		future := uploadPool.Submit(func() (*mergedBlockUploadResult, error) {
			return w.processMergeTask(ctx, taskCopy, mergedBlockID), nil
		})
		futures = append(futures, future)
	}

	// Collect results from all futures
	var newBlockIndexes []*codec.IndexRecord
	totalSize := int64(0)

	for _, future := range futures {
		result := future.Value()
		if result.error != nil {
			return nil, 0, fmt.Errorf("merge task failed: %w", result.error)
		}
		newBlockIndexes = append(newBlockIndexes, result.blockIndex)
		totalSize += result.blockSize
	}

	// Sort blocks by block number to ensure correct order
	sort.Slice(newBlockIndexes, func(i, j int) bool {
		return newBlockIndexes[i].BlockNumber < newBlockIndexes[j].BlockNumber
	})

	return newBlockIndexes, totalSize, nil
}

// planMergeBlockTasks plans how to group blocks for merging
func (w *StagedFileWriter) planMergeBlockTasks(targetBlockSize int64) []*mergeBlockTask {
	var tasks []*mergeBlockTask
	var currentTask *mergeBlockTask
	var currentSize int64 = 0

	// Initialize entry ID tracking
	currentEntryID := w.firstEntryID.Load()
	if currentEntryID == -1 {
		currentEntryID = 0
	}

	for _, blockIndex := range w.blockIndexes {
		// Estimate data size for this block
		estimatedDataSize := int64(blockIndex.BlockSize)

		// Check if adding this block would exceed target size
		if currentTask != nil && currentSize+estimatedDataSize >= targetBlockSize {
			// Complete current task
			currentTask.nextEntryID = currentEntryID
			tasks = append(tasks, currentTask)

			// Start new task
			currentTask = &mergeBlockTask{
				blocks: []*codec.IndexRecord{blockIndex},
			}
			currentSize = estimatedDataSize
		} else {
			// Add to current task or start new one
			if currentTask == nil {
				currentTask = &mergeBlockTask{
					blocks: []*codec.IndexRecord{blockIndex},
				}
				currentSize = estimatedDataSize
			} else {
				currentTask.blocks = append(currentTask.blocks, blockIndex)
				currentSize += estimatedDataSize
			}
		}

		// Update entry ID for next block
		currentEntryID = blockIndex.LastEntryID + 1
	}

	// Add the last task if it exists
	if currentTask != nil {
		currentTask.nextEntryID = currentEntryID
		tasks = append(tasks, currentTask)
	}

	return tasks
}

// processMergeTask processes a single merge task: read blocks, merge and upload
func (w *StagedFileWriter) processMergeTask(ctx context.Context, task *mergeBlockTask, mergedBlockID int64) *mergedBlockUploadResult {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "processMergeTask")
	defer sp.End()

	logger.Ctx(ctx).Debug("processing merge task",
		zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int64("mergedBlockID", mergedBlockID), zap.Int("originalBlocks", len(task.blocks)))

	// Create pool for concurrent block reading
	maxConcurrentReads := w.compactPolicyConfig.MaxParallelReads
	if maxConcurrentReads <= 0 {
		maxConcurrentReads = min(8, len(task.blocks)) // Default to min(8, block count)
	}

	readPool := conc.NewPool[*blockReadResult](maxConcurrentReads, conc.WithPreAlloc(true))
	defer readPool.Release()

	// Submit all block read tasks to the pool
	var readFutures []*conc.Future[*blockReadResult]
	for _, blockIndex := range task.blocks {
		// Capture variable for closure
		blockIndexCopy := blockIndex

		future := readPool.Submit(func() (*blockReadResult, error) {
			return w.readBlockDataFromLocalFile(ctx, blockIndexCopy), nil
		})
		readFutures = append(readFutures, future)
	}

	// Collect block data from all futures
	var allBlockData [][]byte
	firstEntryID := int64(-1)
	lastEntryID := int64(-1)

	for _, future := range readFutures {
		result := future.Value()
		if result.error != nil {
			return &mergedBlockUploadResult{
				error: fmt.Errorf("failed to read block data: %w", result.error),
			}
		}

		allBlockData = append(allBlockData, result.blockData)

		// Track entry ID range
		if firstEntryID == -1 || result.blockIndex.FirstEntryID < firstEntryID {
			firstEntryID = result.blockIndex.FirstEntryID
		}
		if lastEntryID == -1 || result.blockIndex.LastEntryID > lastEntryID {
			lastEntryID = result.blockIndex.LastEntryID
		}
	}

	// Merge block data
	mergedData := make([]byte, 0)
	for _, blockData := range allBlockData {
		mergedData = append(mergedData, blockData...)
	}

	// Create block key for upload
	blockKey := w.getCompactedBlockKey(mergedBlockID)

	// Upload merged block to minio
	err := w.storageCli.PutObject(ctx, w.bucket, blockKey, bytes.NewReader(mergedData), int64(len(mergedData)))
	if err != nil {
		return &mergedBlockUploadResult{
			error: fmt.Errorf("failed to upload merged block: %w", err),
		}
	}

	// Create new block index
	newBlockIndex := &codec.IndexRecord{
		BlockNumber:  int32(mergedBlockID),
		StartOffset:  0, // For object storage, offset is not meaningful
		BlockSize:    uint32(len(mergedData)),
		FirstEntryID: firstEntryID,
		LastEntryID:  lastEntryID,
	}

	logger.Ctx(ctx).Debug("uploaded merged block",
		zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.String("blockKey", blockKey),
		zap.Int64("blockSize", int64(len(mergedData))), zap.Int64("firstEntryID", firstEntryID), zap.Int64("lastEntryID", lastEntryID))

	return &mergedBlockUploadResult{
		blockIndex: newBlockIndex,
		blockSize:  int64(len(mergedData)),
		error:      nil,
	}

}

// readBlockDataFromLocalFile reads data for a specific block from the per-block file
func (w *StagedFileWriter) readBlockDataFromLocalFile(ctx context.Context, blockIndex *codec.IndexRecord) *blockReadResult {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "readBlockDataFromLocalFile")
	defer sp.End()

	// Get path to the per-block file
	blockFilePath := getBlockFilePath(w.localBaseDir, w.logId, w.segmentId, int64(blockIndex.BlockNumber))

	// Read the entire block file
	blockData, err := os.ReadFile(blockFilePath)
	if err != nil {
		return &blockReadResult{
			blockIndex: blockIndex,
			error:      fmt.Errorf("failed to read block file %s: %w", blockFilePath, err),
		}
	}

	return &blockReadResult{
		blockIndex: blockIndex,
		blockData:  blockData,
		error:      nil,
	}
}

// uploadCompactedFooter creates and uploads the footer for compacted segment
func (w *StagedFileWriter) uploadCompactedFooter(ctx context.Context, blockIndexes []*codec.IndexRecord, fileSizeAfterCompact int64, lac int64) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "uploadCompactedFooter")
	defer sp.End()

	// Create footer with compacted flag and LAC
	footer := &codec.FooterRecord{
		TotalBlocks:  int32(len(blockIndexes)),
		TotalRecords: uint32(w.lastEntryID.Load() - w.firstEntryID.Load() + 1),
		TotalSize:    uint64(fileSizeAfterCompact),
		IndexOffset:  0,
		IndexLength:  uint32(len(blockIndexes) * (codec.RecordHeaderSize + codec.IndexRecordSize)),
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(0), // Set compacted flag
		LAC:          lac,                   // Set Last Add Confirmed ID from validation
	}

	// Serialize footer and indexes
	footerData := w.serializeCompactedFooterAndIndexes(ctx, blockIndexes, footer)

	// Upload footer
	footerKey := w.getFooterBlockKey()
	err := w.storageCli.PutObject(ctx, w.bucket, footerKey, bytes.NewReader(footerData), int64(len(footerData)))
	if err != nil {
		return 0, fmt.Errorf("failed to upload footer: %w", err)
	}

	logger.Ctx(ctx).Info("uploaded compacted footer",
		zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.String("footerKey", footerKey), zap.Int64("footerSize", int64(len(footerData))))

	return int64(len(footerData)), nil
}

// serializeCompactedFooterAndIndexes serializes footer and indexes for compacted segment
func (w *StagedFileWriter) serializeCompactedFooterAndIndexes(ctx context.Context, blockIndexes []*codec.IndexRecord, footer *codec.FooterRecord) []byte {
	serializedData := make([]byte, 0)

	// Serialize all block index records
	for _, record := range blockIndexes {
		encodedRecord := codec.EncodeRecord(record)
		serializedData = append(serializedData, encodedRecord...)
	}

	// Serialize footer record
	encodedFooter := codec.EncodeRecord(footer)
	serializedData = append(serializedData, encodedFooter...)

	return serializedData
}

// getFooterBlockKey generates the object key for the footer block
func (w *StagedFileWriter) getFooterBlockKey() string {
	return fmt.Sprintf("%s/%d/%d/footer.blk", w.rootPath, w.logId, w.segmentId)
}

// getCompactedBlockKey generates the object key for a compacted block
func (w *StagedFileWriter) getCompactedBlockKey(blockID int64) string {
	return fmt.Sprintf("%s/%d/%d/m_%d.blk", w.rootPath, w.logId, w.segmentId, blockID)
}

// Path helper functions - delegate to serde package for consistency across all backends
func getSegmentDir(baseDir string, logId int64, segmentId int64) string {
	return serde.GetSegmentDir(baseDir, logId, segmentId)
}

func getBlockFilePath(baseDir string, logId int64, segmentId int64, blockId int64) string {
	return serde.GetBlockFilePath(baseDir, logId, segmentId, blockId)
}

func getInflightBlockPath(baseDir string, logId int64, segmentId int64, blockId int64) string {
	return serde.GetInflightBlockPath(baseDir, logId, segmentId, blockId)
}

func getCompletedBlockPath(baseDir string, logId int64, segmentId int64, blockId int64) string {
	return serde.GetCompletedBlockPath(baseDir, logId, segmentId, blockId)
}

func getLocalFooterBlockPath(baseDir string, logId int64, segmentId int64) string {
	return serde.GetFooterBlockPath(baseDir, logId, segmentId)
}

func getLocalFooterInflightPath(baseDir string, logId int64, segmentId int64) string {
	return serde.GetFooterInflightPath(baseDir, logId, segmentId)
}

// recoverFromBlockFilesUnsafe recovers state from existing per-block files
func (w *StagedFileWriter) recoverFromBlockFilesUnsafe(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "recoverFromBlockFiles")
	defer sp.End()
	startTime := time.Now()

	segmentDir := getSegmentDir(w.localBaseDir, w.logId, w.segmentId)

	// First, try to recover from footer.blk if it exists (finalized segment)
	footerPath := getLocalFooterBlockPath(w.localBaseDir, w.logId, w.segmentId)
	if footerData, err := os.ReadFile(footerPath); err == nil {
		if err := w.recoverFromFooterBlockUnsafe(ctx, footerData); err == nil {
			w.finalized.Store(true)
			w.recovered.Store(true)
			logger.Ctx(ctx).Info("recovered from footer.blk", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int("blockCount", len(w.blockIndexes)))
			return nil
		}
	}

	// Check for footer.blk.inflight (crashed during finalization)
	footerInflightPath := getLocalFooterInflightPath(w.localBaseDir, w.logId, w.segmentId)
	if _, err := os.Stat(footerInflightPath); err == nil {
		// Try to recover from inflight footer file
		footerInflightData, readErr := os.ReadFile(footerInflightPath)
		if readErr == nil {
			if err := w.recoverFromFooterBlockUnsafe(ctx, footerInflightData); err != nil {
				logger.Ctx(ctx).Warn("invalid footer.blk.inflight file, removing",
					zap.String("footerInflightPath", footerInflightPath), zap.Error(err))
				os.Remove(footerInflightPath)
			} else {
				// Inflight file is valid, complete the rename
				if err := os.Rename(footerInflightPath, footerPath); err != nil {
					logger.Ctx(ctx).Warn("failed to rename footer.blk.inflight to footer.blk",
						zap.String("footerInflightPath", footerInflightPath), zap.String("footerPath", footerPath), zap.Error(err))
					os.Remove(footerInflightPath)
				} else {
					w.finalized.Store(true)
					w.recovered.Store(true)
					logger.Ctx(ctx).Info("recovered from footer.blk.inflight and completed rename",
						zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int("blockCount", len(w.blockIndexes)))
					return nil
				}
			}
		} else {
			logger.Ctx(ctx).Warn("failed to read footer.blk.inflight, removing",
				zap.String("footerInflightPath", footerInflightPath), zap.Error(readErr))
			os.Remove(footerInflightPath)
		}
	}

	// Clean up any incomplete block files (inflight or completed)
	w.cleanupIncompleteBlocksUnsafe(ctx)

	// List and recover from completed block files
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read segment dir: %w", err)
	}

	var blockIds []int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		// Only consider completed block files (n.blk)
		if strings.HasSuffix(name, ".blk") &&
			!strings.HasSuffix(name, ".blk.inflight") &&
			!strings.HasSuffix(name, ".blk.completed") &&
			name != "footer.blk" {
			blockIdStr := strings.TrimSuffix(name, ".blk")
			blockId, err := strconv.ParseInt(blockIdStr, 10, 64)
			if err != nil {
				continue
			}
			blockIds = append(blockIds, blockId)
		}
	}

	// Sort block IDs
	sort.Slice(blockIds, func(i, j int) bool {
		return blockIds[i] < blockIds[j]
	})

	// Load block indexes
	w.blockIndexes = make([]*codec.IndexRecord, 0, len(blockIds))
	for _, blockId := range blockIds {
		blockPath := getBlockFilePath(w.localBaseDir, w.logId, w.segmentId, blockId)
		blockData, err := os.ReadFile(blockPath)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to read block file during recovery", zap.String("blockPath", blockPath), zap.Error(err))
			continue
		}

		// Parse block header
		if len(blockData) < codec.RecordHeaderSize+codec.BlockHeaderRecordSize {
			continue
		}

		headerData := blockData[:codec.RecordHeaderSize+codec.BlockHeaderRecordSize]
		record, err := codec.DecodeRecord(headerData)
		if err != nil || record.Type() != codec.BlockHeaderRecordType {
			continue
		}

		blockHeader := record.(*codec.BlockHeaderRecord)

		// Verify integrity
		blockDataContent := blockData[codec.RecordHeaderSize+codec.BlockHeaderRecordSize:]
		if err := codec.VerifyBlockDataIntegrity(blockHeader, blockDataContent); err != nil {
			logger.Ctx(ctx).Warn("block integrity check failed during recovery", zap.Int64("blockId", blockId), zap.Error(err))
			continue
		}

		indexRecord := &codec.IndexRecord{
			BlockNumber:  int32(blockId),
			StartOffset:  0,
			BlockSize:    uint32(len(blockData)),
			FirstEntryID: blockHeader.FirstEntryID,
			LastEntryID:  blockHeader.LastEntryID,
		}
		w.blockIndexes = append(w.blockIndexes, indexRecord)

		// Update entry tracking
		if w.firstEntryID.Load() == -1 || blockHeader.FirstEntryID < w.firstEntryID.Load() {
			w.firstEntryID.Store(blockHeader.FirstEntryID)
		}
		if blockHeader.LastEntryID > w.lastEntryID.Load() {
			w.lastEntryID.Store(blockHeader.LastEntryID)
		}
	}

	w.currentBlockNumber.Store(int64(len(w.blockIndexes)))
	w.recovered.Store(true)

	logger.Ctx(ctx).Info("recovered from block files",
		zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int("blockCount", len(w.blockIndexes)),
		zap.Int64("firstEntryID", w.firstEntryID.Load()), zap.Int64("lastEntryID", w.lastEntryID.Load()))

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "recover_blocks", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "recover_blocks", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return nil
}

// recoverFromFooterBlockUnsafe recovers state from footer.blk file
func (w *StagedFileWriter) recoverFromFooterBlockUnsafe(ctx context.Context, footerData []byte) error {
	// Parse footer record
	footerRecord, err := codec.ParseFooterFromBytes(footerData)
	if err != nil {
		return fmt.Errorf("parse footer: %w", err)
	}

	w.recoveredFooter = footerRecord

	// Parse index records from footer data
	indexLength := int(footerRecord.IndexLength)
	if len(footerData) < indexLength+codec.RecordHeaderSize+codec.GetFooterRecordSize(footerRecord.Version) {
		return fmt.Errorf("footer data too short for indexes")
	}

	// Index records are at the beginning of footer data
	w.blockIndexes = make([]*codec.IndexRecord, 0, footerRecord.TotalBlocks)
	offset := 0

	for offset < indexLength {
		if offset+codec.RecordHeaderSize+codec.IndexRecordSize > len(footerData) {
			break
		}

		record, err := codec.DecodeRecord(footerData[offset:])
		if err != nil {
			break
		}

		if record.Type() != codec.IndexRecordType {
			break
		}

		indexRecord := record.(*codec.IndexRecord)
		w.blockIndexes = append(w.blockIndexes, indexRecord)

		offset += codec.RecordHeaderSize + codec.IndexRecordSize
	}

	// Update entry tracking
	if len(w.blockIndexes) > 0 {
		w.firstEntryID.Store(w.blockIndexes[0].FirstEntryID)
		w.lastEntryID.Store(w.blockIndexes[len(w.blockIndexes)-1].LastEntryID)
		w.currentBlockNumber.Store(int64(len(w.blockIndexes)))
	}

	return nil
}

// cleanupIncompleteBlocksUnsafe removes inflight and completed block files
func (w *StagedFileWriter) cleanupIncompleteBlocksUnsafe(ctx context.Context) {
	segmentDir := getSegmentDir(w.localBaseDir, w.logId, w.segmentId)
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".blk.inflight") || strings.HasSuffix(name, ".blk.completed") {
			path := filepath.Join(segmentDir, name)
			if err := os.Remove(path); err != nil {
				logger.Ctx(ctx).Warn("failed to remove incomplete block file", zap.String("path", path), zap.Error(err))
			} else {
				logger.Ctx(ctx).Debug("removed incomplete block file", zap.String("path", path))
			}
		}
	}
}

// cleanupBlockFilesUnsafe removes all block files for a fresh start
func (w *StagedFileWriter) cleanupBlockFilesUnsafe(ctx context.Context) error {
	segmentDir := getSegmentDir(w.localBaseDir, w.logId, w.segmentId)
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".blk") || strings.HasSuffix(name, ".blk.inflight") || strings.HasSuffix(name, ".blk.completed") {
			path := filepath.Join(segmentDir, name)
			if err := os.Remove(path); err != nil {
				logger.Ctx(ctx).Warn("failed to remove block file during cleanup",
					zap.String("path", path),
					zap.Error(err))
			}
		}
	}

	return nil
}

// validateLACAlignment validates that the segment contains complete data for the LAC range
// by using the already recovered footer and comparing LAC with the segment's entry range
func (w *StagedFileWriter) validateLACAlignment(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "validateLACAlignment")
	defer sp.End()

	// For finalized segments, use the recovered footer
	// For non-finalized segments, we need to read the footer first (shouldn't happen in normal flow)
	var footer *codec.FooterRecord
	if w.finalized.Load() && w.recoveredFooter != nil {
		footer = w.recoveredFooter
	} else {
		// This shouldn't happen in normal compact flow, but handle it for safety
		return -1, fmt.Errorf("segment not finalized or footer not recovered, cannot validate LAC")
	}

	lac := footer.LAC
	firstEntryID := w.firstEntryID.Load()
	lastEntryID := w.lastEntryID.Load()

	logger.Ctx(ctx).Debug("validating LAC alignment",
		zap.Int64("lac", lac), zap.Int64("firstEntryID", firstEntryID), zap.Int64("lastEntryID", lastEntryID), zap.Bool("hasLAC", footer.HasLAC()))

	// LAC validation logic:
	// 1. If footer has no valid LAC (LAC < 0), we cannot compact
	// 2. If LAC >= 0, validate that segment contains complete 0-LAC sequence
	if !footer.HasLAC() || lac < 0 {
		return -1, werr.ErrInvalidLACAlignment.WithCauseErrMsg(
			fmt.Sprintf("footer has no valid LAC: LAC=%d", lac))
	}

	// Check that segment starts from entry 0 or close to 0
	// This ensures we have the complete sequence from the beginning
	if firstEntryID > 0 {
		return -1, werr.ErrInvalidLACAlignment.WithCauseErrMsg(
			fmt.Sprintf("segment does not start from entry 0: firstEntryID=%d", firstEntryID))
	}

	// Check that segment contains all entries up to LAC
	// The segment must contain the complete range [0, LAC]
	if lastEntryID < lac {
		return -1, werr.ErrInvalidLACAlignment.WithCauseErrMsg(
			fmt.Sprintf("segment incomplete: lastEntryID=%d < LAC=%d", lastEntryID, lac))
	}

	// Validate against the maximum lastEntryID from all block indexes
	maxBlockLastEntryID := int64(-1)
	for _, blockIndex := range w.blockIndexes {
		if blockIndex.LastEntryID > maxBlockLastEntryID {
			maxBlockLastEntryID = blockIndex.LastEntryID
		}
	}

	// Ensure LAC doesn't exceed the maximum entry ID in any block
	if lac > maxBlockLastEntryID {
		return -1, werr.ErrInvalidLACAlignment.WithCauseErrMsg(
			fmt.Sprintf("LAC exceeds maximum block entry ID: LAC=%d > maxBlockLastEntryID=%d",
				lac, maxBlockLastEntryID))
	}

	logger.Ctx(ctx).Info("LAC alignment validation successful",
		zap.Int64("lac", lac), zap.Int64("firstEntryID", firstEntryID), zap.Int64("lastEntryID", lastEntryID), zap.Int64("maxBlockLastEntryID", maxBlockLastEntryID))

	return lac, nil
}

// determineIfNeedRecoveryMode determines if the writer should enter recovery mode
// by checking if block files already exist in the segment directory.
// This is critical for node restart scenarios to prevent data loss.
func (w *StagedFileWriter) determineIfNeedRecoveryMode(forceRecoveryMode bool) bool {
	if forceRecoveryMode {
		return true
	}

	segmentDir := getSegmentDir(w.localBaseDir, w.logId, w.segmentId)

	// Check if segment directory exists
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.Ctx(context.Background()).Debug("Failed to read segment directory, assuming no recovery needed", zap.String("segmentDir", segmentDir), zap.Error(err))
		}
		return false
	}

	// Check if any block files exist
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".blk") {
			logger.Ctx(context.Background()).Info("Auto-detected existing block files, entering recovery mode",
				zap.String("segmentDir", segmentDir), zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))
			return true
		}
	}

	return false
}

// GetRecoveredFooter Test Only
func (w *StagedFileWriter) GetRecoveredFooter() *codec.FooterRecord {
	return w.recoveredFooter
}

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

package disk

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
	"github.com/gofrs/flock"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/conc"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/serde"
)

var (
	WriterScope = "LocalFileWriter"
)

var _ storage.Writer = (*LocalFileWriter)(nil)

// blockFlushResult represents the result of a block flush operation
type blockFlushResult struct {
	blockId      int64
	firstEntryId int64
	lastEntryId  int64
	blockSize    int64
	err          error
}

// blockFlushTask represents a task to flush a block to disk
type blockFlushTask struct {
	entries      []*cache.BufferEntry
	firstEntryId int64
	lastEntryId  int64
	blockNumber  int32
	flushFuture  *conc.Future[*blockFlushResult]
}

// LocalFileWriter implements AbstractFileWriter for local filesystem storage with async buffer management
type LocalFileWriter struct {
	mu        sync.Mutex
	baseDir   string
	logId     int64
	segmentId int64
	logIdStr  string // for metrics only

	// Configuration
	maxFlushSize     int64 // Max buffer size before triggering sync
	maxBufferEntries int64 // Maximum number of entries per buffer
	maxIntervalMs    int   // Max interval to sync buffer to disk
	maxFlushThreads  int   // Maximum number of concurrent flush threads

	// write buffer
	buffer            atomic.Pointer[cache.SequentialBuffer] // Write buffer
	lastSyncTimestamp atomic.Int64

	// block state
	currentBlockNumber atomic.Int64
	blockIndexes       []*codec.IndexRecord
	blockIndexesMu     sync.Mutex // Protects blockIndexes

	// written state
	firstEntryID atomic.Int64 // The first entryId written to disk
	lastEntryID  atomic.Int64 // The last entryId written to disk
	finalizeMu   sync.Mutex   // Ensures that the finalize operation is done in a single thread
	finalized    atomic.Bool
	fenced       atomic.Bool
	recovered    atomic.Bool
	lockFile     *flock.Flock // Lock file handle
	lockFilePath string       // Path to the lock file

	// Async flush management - concurrent flush with pool
	pool                         *conc.Pool[*blockFlushResult] // Concurrent flush pool
	ackTaskChan                  chan *blockFlushTask          // Channel for ack goroutine to process completed tasks
	storageWritable              atomic.Bool                   // Indicates whether the segment is writable
	lastSubmittedFlushingEntryID atomic.Int64
	lastSubmittedFlushingBlockID atomic.Int64
	allUploadingTaskDone         atomic.Bool
	firstFlushErr                error // First flush error encountered

	// Close management
	closed    atomic.Bool
	runCtx    context.Context
	runCancel context.CancelFunc

	// ==================== Legacy Mode Fields (backward compatibility) ====================
	// These fields are only used when recovering from old data.log format.
	// New segments always use per-block files. Legacy mode is auto-detected during recovery.
	legacyMode       atomic.Bool // true if recovered from old data.log format
	segmentFilePath  string      // Path to legacy data.log file
	file             *os.File    // File handle for legacy mode
	headerWritten    atomic.Bool // Legacy: whether header has been written
	writtenBytes     int64       // Legacy: total bytes written
	lastModifiedTime time.Time   // Legacy: last modification time
}

// NewLocalFileWriter creates a new local filesystem writer
func NewLocalFileWriter(ctx context.Context, baseDir string, logId int64, segmentId int64, cfg *config.Configuration) (*LocalFileWriter, error) {
	return NewLocalFileWriterWithMode(ctx, baseDir, logId, segmentId, cfg, false)
}

// NewLocalFileWriterWithMode creates a new local filesystem writer with recovery mode option
func NewLocalFileWriterWithMode(ctx context.Context, baseDir string, logId int64, segmentId int64, cfg *config.Configuration, recoveryMode bool) (*LocalFileWriter, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "NewLocalFileWriterWithMode")
	defer sp.End()
	blockSize := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize.Int64()
	maxBufferEntries := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxEntries
	maxBytes := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes.Int64()
	ackQueueSize := max(int(maxBytes/blockSize), 300)
	maxInterval := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage.Milliseconds()
	maxFlushThreads := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads
	logger.Ctx(ctx).Debug("creating new local file writer", zap.String("baseDir", baseDir),
		zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Int64("maxBlockSize", blockSize),
		zap.Int("maxBufferEntries", maxBufferEntries), zap.Int("maxInterval", maxInterval),
		zap.Int("ackQueueSize", ackQueueSize), zap.Int("maxFlushThreads", maxFlushThreads), zap.Bool("recoveryMode", recoveryMode))

	segmentDir := getSegmentDir(baseDir, logId, segmentId)
	// Ensure directory exists
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		logger.Ctx(ctx).Warn("failed to create directory", zap.String("segmentDir", segmentDir), zap.Error(err))
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// Create context for async operations
	runCtx, runCancel := context.WithCancel(context.Background())

	writer := &LocalFileWriter{
		baseDir:          baseDir,
		logId:            logId,
		segmentId:        segmentId,
		logIdStr:         fmt.Sprintf("%d", logId),
		blockIndexes:     make([]*codec.IndexRecord, 0),
		maxFlushSize:     blockSize,
		maxBufferEntries: int64(maxBufferEntries),
		maxIntervalMs:    maxInterval,
		maxFlushThreads:  maxFlushThreads,
		pool:             conc.NewPool[*blockFlushResult](maxFlushThreads, conc.WithPreAlloc(true)),
		ackTaskChan:      make(chan *blockFlushTask, ackQueueSize),
		runCtx:           runCtx,
		runCancel:        runCancel,
		segmentFilePath:  getSegmentFilePath(baseDir, logId, segmentId), // Legacy mode support
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
	writer.lastSyncTimestamp.Store(0)
	writer.legacyMode.Store(false)
	writer.headerWritten.Store(false)

	// Set default block size if not specified
	if writer.maxFlushSize <= 0 {
		writer.maxFlushSize = 2 * 1024 * 1024 // 2MB default
		logger.Ctx(ctx).Debug("using default block size", zap.Int64("defaultBlockSize", writer.maxFlushSize))
	}

	logger.Ctx(ctx).Debug("writer configuration initialized", zap.Int64("maxFlushSize", writer.maxFlushSize),
		zap.Int64("maxBufferEntries", writer.maxBufferEntries), zap.Int("maxIntervalMs", writer.maxIntervalMs),
		zap.Int("maxFlushThreads", writer.maxFlushThreads))

	// Initialize buffer
	startEntryId := int64(0)

	if recoveryMode {
		logger.Ctx(ctx).Debug("recovery mode enabled, attempting to recover from existing blocks")
		if err := writer.recoverFromBlockFilesUnsafe(ctx); err != nil {
			logger.Ctx(ctx).Warn("failed to recover from block files", zap.String("baseDir", baseDir), zap.Error(err))
			runCancel()
			return nil, fmt.Errorf("recover from block files: %w", err)
		}
		if writer.lastEntryID.Load() != -1 {
			startEntryId = writer.lastEntryID.Load() + 1
			logger.Ctx(ctx).Debug("recovered writer state", zap.Int64("lastEntryID", writer.lastEntryID.Load()),
				zap.Int64("nextStartEntryId", startEntryId), zap.Bool("recovered", writer.recovered.Load()))
		}
	} else {
		logger.Ctx(ctx).Debug("normal mode, creating segment lock and cleaning up old files")
		if err := writer.createSegmentLock(ctx); err != nil {
			logger.Ctx(ctx).Warn("failed to create segment lock", zap.String("baseDir", baseDir), zap.Error(err))
			runCancel()
			return nil, errors.Wrap(err, "failed to create segment lock")
		}
		logger.Ctx(ctx).Debug("segment lock created successfully")
		if err := writer.cleanupBlockFilesUnsafe(ctx); err != nil {
			logger.Ctx(ctx).Warn("failed to cleanup block files", zap.String("baseDir", baseDir), zap.Error(err))
		}
	}

	initialBuffer := cache.NewSequentialBuffer(logId, segmentId, startEntryId, writer.maxBufferEntries)
	writer.buffer.Store(initialBuffer)
	logger.Ctx(ctx).Debug("buffer initialized", zap.Int64("startEntryId", startEntryId),
		zap.Int64("maxBufferEntries", writer.maxBufferEntries), zap.String("bufferInstance", fmt.Sprintf("%p", initialBuffer)))

	go writer.run()
	go writer.ack()

	logger.Ctx(ctx).Info("local file writer created successfully", zap.String("baseDir", baseDir),
		zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Int64("blockSize", blockSize),
		zap.Bool("recoveryMode", recoveryMode), zap.Int64("startEntryId", startEntryId),
		zap.Bool("recovered", writer.recovered.Load()), zap.String("writerInstance", fmt.Sprintf("%p", writer)))

	return writer, nil
}

// run is the main async goroutine that handles periodic sync
func (w *LocalFileWriter) run() {
	ticker := time.NewTicker(time.Duration(w.maxIntervalMs) * time.Millisecond)
	defer ticker.Stop()
	logger.Ctx(w.runCtx).Debug("LocalFileWriter run goroutine started", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))

	for {
		select {
		case <-w.runCtx.Done():
			// Context cancelled, exit
			logger.Ctx(w.runCtx).Debug("LocalFileWriter run goroutine stopping due to context cancellation")
			return
		case <-ticker.C:
			// Periodic check fenced flag made by other process
			foundFencedFlag, _ := checkFenceFlagFileExists(w.runCtx, w.baseDir, w.logId, w.segmentId)
			if foundFencedFlag {
				w.fenced.Store(true)
			}
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

// ack is the goroutine that processes completed flush tasks in order
func (w *LocalFileWriter) ack() {
	logger.Ctx(w.runCtx).Debug("LocalFileWriter ack goroutine started", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))

	for task := range w.ackTaskChan {
		if task.entries == nil {
			logger.Ctx(context.TODO()).Debug("received termination signal in ack goroutine", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))
			w.allUploadingTaskDone.Store(true)
			return
		}

		// Wait for flush to complete
		if task.flushFuture == nil {
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
			if w.firstFlushErr == nil {
				w.firstFlushErr = actualErr
				w.storageWritable.Store(false)
			}
			logger.Ctx(context.TODO()).Warn("flush task failed", zap.Int64("logId", w.logId),
				zap.Int64("segmentId", w.segmentId), zap.Int32("blockNumber", task.blockNumber), zap.Error(actualErr))
			w.notifyFlushError(task.entries, actualErr)
			continue
		}

		if w.firstFlushErr != nil {
			logger.Ctx(context.TODO()).Warn("failing flush due to previous error", zap.Int64("logId", w.logId),
				zap.Int64("segmentId", w.segmentId), zap.Int32("blockNumber", task.blockNumber), zap.Error(w.firstFlushErr))
			w.notifyFlushError(task.entries, w.firstFlushErr)
			continue
		}

		// Flush succeeded - rename completed block to final block
		completedPath := getCompletedBlockPath(w.baseDir, w.logId, w.segmentId, result.blockId)
		finalPath := getBlockFilePath(w.baseDir, w.logId, w.segmentId, result.blockId)

		if err := os.Rename(completedPath, finalPath); err != nil {
			// Check if this is a fence - either a fence directory or fence block file
			// Key insight: renaming a file to a directory path fails on all OSes
			// This is the core mechanism for directory-based fencing
			if isFenceDirectory(finalPath) || w.checkAndHandleFenceBlockUnsafe(context.TODO(), finalPath) {
				logger.Ctx(context.TODO()).Warn("detected fence during rename (target is directory or fence block), marking writer as fenced",
					zap.String("finalPath", finalPath), zap.Int64("blockId", result.blockId),
					zap.Bool("isDirectory", isFenceDirectory(finalPath)))
				w.fenced.Store(true)
				w.storageWritable.Store(false)
				if w.firstFlushErr == nil {
					w.firstFlushErr = werr.ErrSegmentFenced
				}
				w.notifyFlushError(task.entries, werr.ErrSegmentFenced)
				os.Remove(completedPath)
				continue
			}
			logger.Ctx(context.TODO()).Warn("failed to rename completed block to final",
				zap.String("completedPath", completedPath), zap.String("finalPath", finalPath), zap.Error(err))
			if w.firstFlushErr == nil {
				w.firstFlushErr = err
				w.storageWritable.Store(false)
			}
			w.notifyFlushError(task.entries, err)
			continue
		}

		// Update state
		if w.firstEntryID.Load() == -1 {
			w.firstEntryID.Store(result.firstEntryId)
		}
		w.lastEntryID.Store(result.lastEntryId)

		// Add block index
		w.blockIndexesMu.Lock()
		w.blockIndexes = append(w.blockIndexes, &codec.IndexRecord{
			BlockNumber:  int32(result.blockId),
			StartOffset:  result.blockId, // Use blockId as virtual offset
			BlockSize:    uint32(result.blockSize),
			FirstEntryID: result.firstEntryId,
			LastEntryID:  result.lastEntryId,
		})
		w.blockIndexesMu.Unlock()

		logger.Ctx(context.TODO()).Debug("block confirmed and renamed to final", zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId), zap.Int64("blockId", result.blockId),
			zap.Int64("firstEntryId", result.firstEntryId), zap.Int64("lastEntryId", result.lastEntryId))

		// Notify success
		w.notifyFlushSuccess(task.entries)
	}
}

// Sync forces immediate sync of all buffered data
func (w *LocalFileWriter) Sync(ctx context.Context) error {
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

// rollBufferAndSubmitFlushTaskUnsafe rolls the current buffer and submits flush tasks to the pool (must be called with mu held)
func (w *LocalFileWriter) rollBufferAndSubmitFlushTaskUnsafe(ctx context.Context) {
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

	// Split entries into blocks based on maxFlushSize
	blockDataList, blockFirstEntryIdList := w.prepareMultiBlockData(entries, firstEntryId)

	for i, blockEntries := range blockDataList {
		blockId := w.currentBlockNumber.Add(1) - 1 // Get current and increment
		blockFirstEntryId := blockFirstEntryIdList[i]
		blockLastEntryId := blockFirstEntryId + int64(len(blockEntries)) - 1

		// Update flushing state
		w.lastSubmittedFlushingEntryID.Store(blockLastEntryId)
		w.lastSubmittedFlushingBlockID.Store(blockId)

		// Submit flush task to pool
		blockEntriesCopy := blockEntries
		blockIdCopy := blockId
		blockFirstEntryIdCopy := blockFirstEntryId
		blockLastEntryIdCopy := blockLastEntryId

		future := w.pool.Submit(func() (*blockFlushResult, error) {
			return w.processFlushTask(ctx, blockIdCopy, blockFirstEntryIdCopy, blockLastEntryIdCopy, blockEntriesCopy)
		})

		// Create task for ack goroutine
		flushTask := &blockFlushTask{
			entries:      blockEntries,
			firstEntryId: blockFirstEntryId,
			lastEntryId:  blockLastEntryId,
			blockNumber:  int32(blockId),
			flushFuture:  future,
		}

		logger.Ctx(ctx).Debug("Submitting flush task to ack channel", zap.Int64("blockId", blockId),
			zap.Int64("firstEntryId", blockFirstEntryId), zap.Int64("lastEntryId", blockLastEntryId), zap.Int("entriesCount", len(blockEntries)))

		// Send to ack channel
		select {
		case w.ackTaskChan <- flushTask:
			logger.Ctx(ctx).Debug("Flush task submitted to ack channel successfully")
		case <-ctx.Done():
			logger.Ctx(ctx).Warn("Context cancelled while submitting flush task")
			w.notifyFlushError(blockEntries, ctx.Err())
		case <-w.runCtx.Done():
			logger.Ctx(ctx).Warn("Writer context cancelled while submitting flush task")
			w.notifyFlushError(blockEntries, werr.ErrFileWriterAlreadyClosed)
		}
	}

	w.lastSyncTimestamp.Store(time.Now().UnixMilli())
	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "rollBuffer", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "rollBuffer", "success").Observe(float64(time.Since(startTime).Milliseconds()))
}

// prepareMultiBlockData splits entries into multiple blocks based on maxFlushSize
func (w *LocalFileWriter) prepareMultiBlockData(entries []*cache.BufferEntry, firstEntryId int64) ([][]*cache.BufferEntry, []int64) {
	var blockDataList [][]*cache.BufferEntry
	var blockFirstEntryIdList []int64

	var currentBlockData []*cache.BufferEntry
	var currentBlockSize int64
	currentBlockFirstEntryId := firstEntryId

	for _, entry := range entries {
		entrySize := int64(len(entry.Data))

		// If adding this entry would exceed maxFlushSize and we have some data, start new block
		if currentBlockSize+entrySize > w.maxFlushSize && len(currentBlockData) > 0 {
			blockDataList = append(blockDataList, currentBlockData)
			blockFirstEntryIdList = append(blockFirstEntryIdList, currentBlockFirstEntryId)

			currentBlockData = nil
			currentBlockSize = 0
			currentBlockFirstEntryId = entry.EntryId
		}

		currentBlockData = append(currentBlockData, entry)
		currentBlockSize += entrySize
	}

	// Add remaining entries as last block
	if len(currentBlockData) > 0 {
		blockDataList = append(blockDataList, currentBlockData)
		blockFirstEntryIdList = append(blockFirstEntryIdList, currentBlockFirstEntryId)
	}

	return blockDataList, blockFirstEntryIdList
}

// processFlushTask processes a flush task by writing data to a per-block file
// Uses inflight → completed naming convention for crash safety
func (w *LocalFileWriter) processFlushTask(ctx context.Context, blockId int64, firstEntryId int64, lastEntryId int64, entries []*cache.BufferEntry) (*blockFlushResult, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "processFlushTask")
	defer sp.End()
	startTime := time.Now()

	result := &blockFlushResult{
		blockId:      blockId,
		firstEntryId: firstEntryId,
		lastEntryId:  lastEntryId,
	}

	// Check preconditions
	if w.fenced.Load() {
		result.err = werr.ErrSegmentFenced
		return result, result.err
	}

	if w.finalized.Load() {
		result.err = werr.ErrFileWriterFinalized
		return result, result.err
	}

	if !w.storageWritable.Load() {
		result.err = werr.ErrStorageNotWritable
		return result, result.err
	}

	// Pre-check: verify the target block path is not a fence directory
	// This provides early detection of fencing before doing expensive serialization
	finalPath := getBlockFilePath(w.baseDir, w.logId, w.segmentId, blockId)
	if isFenceDirectory(finalPath) {
		logger.Ctx(ctx).Warn("detected fence directory during pre-flush check, marking writer as fenced",
			zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId),
			zap.Int64("blockId", blockId), zap.String("finalPath", finalPath))
		w.fenced.Store(true)
		w.storageWritable.Store(false)
		result.err = werr.ErrSegmentFenced
		return result, result.err
	}

	logger.Ctx(ctx).Debug("starting to process flush task", zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId),
		zap.Int64("blockId", blockId), zap.Int64("firstEntryId", firstEntryId), zap.Int64("lastEntryId", lastEntryId), zap.Int("entriesCount", len(entries)))

	// Serialize all data records to calculate block length and CRC
	var blockDataBuffer bytes.Buffer
	for _, entry := range entries {
		dataRecord := &codec.DataRecord{
			Payload: entry.Data,
		}
		encodedRecord := codec.EncodeRecord(dataRecord)
		blockDataBuffer.Write(encodedRecord)
	}

	blockData := blockDataBuffer.Bytes()
	blockLength := uint32(len(blockData))
	blockCrc := crc32.ChecksumIEEE(blockData)

	// Create block header record
	blockHeaderRecord := &codec.BlockHeaderRecord{
		BlockNumber:  int32(blockId),
		FirstEntryID: firstEntryId,
		LastEntryID:  lastEntryId,
		BlockLength:  blockLength,
		BlockCrc:     blockCrc,
	}
	encodedHeader := codec.EncodeRecord(blockHeaderRecord)

	// Combine header and data
	var fullBlockData bytes.Buffer
	fullBlockData.Write(encodedHeader)
	fullBlockData.Write(blockData)
	fullBlockBytes := fullBlockData.Bytes()
	result.blockSize = int64(len(fullBlockBytes))

	// Write to inflight file
	inflightPath := getInflightBlockPath(w.baseDir, w.logId, w.segmentId, blockId)
	completedPath := getCompletedBlockPath(w.baseDir, w.logId, w.segmentId, blockId)

	file, err := os.OpenFile(inflightPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to create inflight file", zap.String("inflightPath", inflightPath), zap.Error(err))
		result.err = fmt.Errorf("create inflight file: %w", err)
		return result, result.err
	}

	n, err := file.Write(fullBlockBytes)
	if err != nil {
		file.Close()
		os.Remove(inflightPath)
		logger.Ctx(ctx).Warn("failed to write block data", zap.String("inflightPath", inflightPath), zap.Error(err))
		result.err = fmt.Errorf("write block data: %w", err)
		return result, result.err
	}

	if n != len(fullBlockBytes) {
		file.Close()
		os.Remove(inflightPath)
		logger.Ctx(ctx).Warn("incomplete block write", zap.String("inflightPath", inflightPath),
			zap.Int("expected", len(fullBlockBytes)), zap.Int("actual", n))
		result.err = fmt.Errorf("incomplete write: wrote %d of %d bytes", n, len(fullBlockBytes))
		return result, result.err
	}

	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(inflightPath)
		logger.Ctx(ctx).Warn("failed to sync inflight file", zap.String("inflightPath", inflightPath), zap.Error(err))
		result.err = fmt.Errorf("sync inflight file: %w", err)
		return result, result.err
	}

	if err := file.Close(); err != nil {
		os.Remove(inflightPath)
		logger.Ctx(ctx).Warn("failed to close inflight file", zap.String("inflightPath", inflightPath), zap.Error(err))
		result.err = fmt.Errorf("close inflight file: %w", err)
		return result, result.err
	}

	if err := os.Rename(inflightPath, completedPath); err != nil {
		os.Remove(inflightPath)
		logger.Ctx(ctx).Warn("failed to rename inflight to completed",
			zap.String("inflightPath", inflightPath), zap.String("completedPath", completedPath), zap.Error(err))
		result.err = fmt.Errorf("rename inflight to completed: %w", err)
		return result, result.err
	}

	logger.Ctx(ctx).Debug("block written and renamed to completed", zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId),
		zap.Int64("blockId", blockId), zap.Int64("firstEntryId", firstEntryId), zap.Int64("lastEntryId", lastEntryId), zap.Int64("blockSize", result.blockSize))

	// Update metrics
	metrics.WpFileFlushBytesWritten.WithLabelValues(w.logIdStr).Add(float64(result.blockSize))
	metrics.WpFileFlushLatency.WithLabelValues(w.logIdStr).Observe(float64(time.Since(startTime).Milliseconds()))

	return result, nil
}

func (f *LocalFileWriter) awaitAllFlushTasks(ctx context.Context) error {
	segmentDir := getSegmentDir(f.baseDir, f.logId, f.segmentId)
	logger.Ctx(ctx).Info("awaiting completion of all block flush tasks", zap.String("segmentDir", segmentDir))

	// First, wait for all pool tasks to complete
	maxWaitTime := 10 * time.Second
	startTime := time.Now()

	for {
		runningTasks := f.pool.Running()
		if runningTasks == 0 {
			logger.Ctx(ctx).Info("all pool tasks completed", zap.String("segmentDir", segmentDir))
			break
		}

		if time.Since(startTime) > maxWaitTime {
			logger.Ctx(ctx).Warn("timeout waiting for pool tasks", zap.String("segmentDir", segmentDir), zap.Int("runningTasks", runningTasks))
			return errors.New("timeout waiting for pool tasks to complete")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
			continue
		}
	}

	// Now wait for the ack goroutine to process all completed tasks
	logger.Ctx(ctx).Info("waiting for ack goroutine to process completed tasks", zap.String("segmentDir", segmentDir))

	// Send termination signal to ack goroutine
	select {
	case f.ackTaskChan <- &blockFlushTask{
		entries:      nil,
		firstEntryId: -1,
		lastEntryId:  -1,
		blockNumber:  -1,
	}:
		logger.Ctx(ctx).Info("termination signal sent to ack goroutine", zap.String("segmentDir", segmentDir))
	case <-ctx.Done():
		return ctx.Err()
	}

	// Wait for ack goroutine to set the done flag
	ackWaitTime := 15 * time.Second // TODO configurable
	ackStartTime := time.Now()

	for {
		if f.allUploadingTaskDone.Load() {
			logger.Ctx(ctx).Info("ack goroutine completed processing all tasks", zap.String("segmentDir", segmentDir))
			return nil
		}

		if time.Since(ackStartTime) > ackWaitTime {
			logger.Ctx(ctx).Warn("timeout waiting for ack goroutine to complete", zap.String("segmentDir", segmentDir),
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
func (w *LocalFileWriter) notifyFlushError(entries []*cache.BufferEntry, err error) {
	// Notify all pending entries result channels in sequential order
	for _, entry := range entries {
		if entry.NotifyChan != nil {
			cache.NotifyPendingEntryDirectly(context.TODO(), w.logId, w.segmentId, entry.EntryId, entry.NotifyChan, entry.EntryId, err)
		}
	}
}

// notifyFlushSuccess notifies all result channels of flush success
func (w *LocalFileWriter) notifyFlushSuccess(entries []*cache.BufferEntry) {
	// Notify all pending entries result channels in sequential order
	for _, entry := range entries {
		if entry.NotifyChan != nil {
			cache.NotifyPendingEntryDirectly(context.TODO(), w.logId, w.segmentId, entry.EntryId, entry.NotifyChan, entry.EntryId, nil)
		}
	}
}

// WriteDataAsync writes data asynchronously using buffer
func (w *LocalFileWriter) WriteDataAsync(ctx context.Context, entryId int64, data []byte, resultCh channel.ResultChannel) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "WriteDataAsync")
	defer sp.End()
	logger.Ctx(ctx).Debug("WriteDataAsync called", zap.Int64("entryId", entryId), zap.Int("dataLen", len(data)))

	// Validate that data is not empty
	if len(data) == 0 {
		logger.Ctx(ctx).Debug("AppendAsync: attempting to write rejected, data cannot be empty", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("inst", fmt.Sprintf("%p", w)))
		return -1, werr.ErrEmptyPayload
	}

	if w.closed.Load() {
		logger.Ctx(ctx).Debug("WriteDataAsync: writer closed")
		return entryId, werr.ErrFileWriterAlreadyClosed
	}

	if w.finalized.Load() {
		logger.Ctx(ctx).Debug("WriteDataAsync: writer finalized")
		return entryId, werr.ErrFileWriterFinalized
	}

	if w.fenced.Load() {
		// quick fail and return a fenced Err
		logger.Ctx(ctx).Debug("WriteDataAsync: attempting to write rejected, segment is fenced", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("inst", fmt.Sprintf("%p", w)))
		return -1, werr.ErrSegmentFenced
	}

	if !w.storageWritable.Load() {
		logger.Ctx(ctx).Debug("WriteDataAsync: storage not writable")
		return entryId, werr.ErrStorageNotWritable
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
		logger.Ctx(ctx).Warn("failed to write entry to buffer",
			zap.Int64("entryId", entryId),
			zap.Int("dataLen", len(data)),
			zap.Error(err))
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
		zap.Int64("bufferSize", bufferSize),
		zap.Int64("maxFlushSize", w.maxFlushSize),
		zap.Int64("entryCount", entryCount),
		zap.Int64("maxBufferEntries", w.maxBufferEntries),
		zap.Int64("timeSinceLastSync", timeSinceLastSync),
		zap.Int64("lastSyncTimestamp", w.lastSyncTimestamp.Load()))

	// Immediate sync conditions:
	// - Buffer size exceeded
	// - Too many entries
	if bufferSize >= w.maxFlushSize || entryCount >= w.maxBufferEntries {
		logger.Ctx(ctx).Info("Triggering immediate sync from WriteDataAsync",
			zap.Int64("bufferSize", bufferSize),
			zap.Int64("entryCount", entryCount),
			zap.Int64("timeSinceLastSync", timeSinceLastSync))
		triggerSyncErr := w.Sync(ctx)
		if triggerSyncErr != nil {
			logger.Ctx(ctx).Warn("reach max buffer size, but trigger sync failed",
				zap.Int64("bufferSize", bufferSize),
				zap.Int64("entryCount", entryCount),
				zap.Int64("timeSinceLastSync", timeSinceLastSync),
				zap.Error(triggerSyncErr),
			)
		}
	}
	return entryId, nil
}

// writeHeader writes the header record
// Finalize finalizes the writer and writes the footer
func (w *LocalFileWriter) Finalize(ctx context.Context, lac int64 /*not used, cause it always same as last flushed entryID */) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "Finalize")
	defer sp.End()
	startTime := time.Now()

	w.finalizeMu.Lock()
	defer w.finalizeMu.Unlock()

	if w.finalized.Load() {
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
		logger.Ctx(ctx).Warn("wait flush error before close",
			zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId),
			zap.Error(waitErr))
		return -1, waitErr
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Copy block indexes
	w.blockIndexesMu.Lock()
	blockIndexesCopy := make([]*codec.IndexRecord, len(w.blockIndexes))
	copy(blockIndexesCopy, w.blockIndexes)
	blockIndexesLen := len(w.blockIndexes)
	w.blockIndexesMu.Unlock()

	// Calculate total size of all blocks
	var totalBlockSize uint64
	lastEntryID := int64(-1)
	for _, indexRecord := range blockIndexesCopy {
		totalBlockSize += uint64(indexRecord.BlockSize)
		if indexRecord.LastEntryID > lastEntryID {
			lastEntryID = indexRecord.LastEntryID
		}
	}

	// Check if in legacy mode - finalize legacy file format
	if w.legacyMode.Load() {
		if err := w.finalizeLegacyFileUnsafe(ctx, blockIndexesCopy, totalBlockSize, lastEntryID); err != nil {
			return w.lastEntryID.Load(), fmt.Errorf("finalize legacy file: %w", err)
		}
		metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "finalize_legacy", "success").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "finalize_legacy", "success").Observe(float64(time.Since(startTime).Milliseconds()))
		logger.Ctx(ctx).Debug("finalized legacy log file",
			zap.Int64("lastEntryId", w.lastEntryID.Load()),
			zap.String("segmentFilePath", w.segmentFilePath),
			zap.Int("blockCount", blockIndexesLen))
		w.finalized.Store(true)
		return w.lastEntryID.Load(), nil
	}

	// New per-block format: Build footer.blk content: IndexRecords + FooterRecord
	footerData, _ := serde.SerializeFooterAndIndexes(blockIndexesCopy, lastEntryID)

	// Two-stage atomic write for footer: write to inflight, then rename
	footerInflightPath := getFooterInflightPath(w.baseDir, w.logId, w.segmentId)
	footerPath := getFooterBlockPath(w.baseDir, w.logId, w.segmentId)

	// Stage 1: Write to footer.blk.inflight
	footerFile, err := os.OpenFile(footerInflightPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return w.lastEntryID.Load(), fmt.Errorf("create footer inflight file: %w", err)
	}

	if _, err := footerFile.Write(footerData); err != nil {
		footerFile.Close()
		os.Remove(footerInflightPath)
		return w.lastEntryID.Load(), fmt.Errorf("write footer inflight file: %w", err)
	}

	if err := footerFile.Sync(); err != nil {
		footerFile.Close()
		os.Remove(footerInflightPath)
		return w.lastEntryID.Load(), fmt.Errorf("sync footer inflight file: %w", err)
	}

	if err := footerFile.Close(); err != nil {
		os.Remove(footerInflightPath)
		return w.lastEntryID.Load(), fmt.Errorf("close footer inflight file: %w", err)
	}

	// Stage 2: Rename footer.blk.inflight to footer.blk
	if err := os.Rename(footerInflightPath, footerPath); err != nil {
		os.Remove(footerInflightPath)
		return w.lastEntryID.Load(), fmt.Errorf("rename footer inflight to final: %w", err)
	}

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "finalize", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "finalize", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	logger.Ctx(ctx).Debug("finalized log file",
		zap.Int64("lastEntryId", w.lastEntryID.Load()),
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int("blockCount", blockIndexesLen))
	w.finalized.Store(true)
	return w.lastEntryID.Load(), nil
}

// GetLastEntryId returns the last entry ID written
func (w *LocalFileWriter) GetLastEntryId(ctx context.Context) int64 {
	return w.lastEntryID.Load()
}

// GetFirstEntryId returns the first entry ID written
func (w *LocalFileWriter) GetFirstEntryId(ctx context.Context) int64 {
	return w.firstEntryID.Load()
}

func (w *LocalFileWriter) GetBlockCount(ctx context.Context) int64 {
	return w.lastSubmittedFlushingBlockID.Load()
}

// Close closes the writer
func (w *LocalFileWriter) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "Close")
	defer sp.End()
	startTime := time.Now()
	if !w.closed.CompareAndSwap(false, true) { // mark close, and there will be no more add and sync in the future
		logger.Ctx(ctx).Info("run: received close signal, but it already closed,skip", zap.String("inst", fmt.Sprintf("%p", w)))
		return nil
	}
	logger.Ctx(ctx).Info("run: received close signal,trigger sync before close ", zap.String("inst", fmt.Sprintf("%p", w)))
	err := w.Sync(ctx) // manual sync all pending append operation
	if err != nil {
		logger.Ctx(ctx).Warn("sync error before close",
			zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId),
			zap.Error(err))
	}

	// wait all flush
	waitErr := w.awaitAllFlushTasks(ctx)
	if waitErr != nil {
		logger.Ctx(ctx).Warn("wait flush error before close",
			zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId),
			zap.Error(waitErr))
		return waitErr
	}

	// Cancel async operations
	w.runCancel()

	// Close channels
	close(w.ackTaskChan)

	// Release segment lock
	if err := w.releaseSegmentLock(ctx); err != nil {
		logger.Ctx(ctx).Warn("Failed to release segment lock during close",
			zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId),
			zap.Error(err))
	}
	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "close", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "close", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return nil
}

// Fence marks the writer as fenced
// For legacy mode: creates a flag file (write.fence) and waits for detection
// For per-block mode: creates a fence block at position n+1 (after the last block)
func (w *LocalFileWriter) Fence(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "Fence")
	defer sp.End()
	startTime := time.Now()

	// If already fenced, return idempotently
	if w.fenced.Load() {
		logger.Ctx(ctx).Debug("LocalFileWriter already fenced, returning idempotently",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId))
		return w.GetLastEntryId(ctx), nil
	}

	// Sync all pending data first
	if err := w.Sync(ctx); err != nil {
		return w.GetLastEntryId(ctx), err
	}

	// Branch based on mode: legacy vs per-block
	if w.legacyMode.Load() {
		// Legacy mode: use flag file approach
		return w.fenceLegacyModeUnsafe(ctx, startTime)
	}

	// Per-block mode: create fence block at position n+1
	return w.fencePerBlockModeUnsafe(ctx, startTime)
}

// fencePerBlockModeUnsafe creates a fence block for per-block mode
func (w *LocalFileWriter) fencePerBlockModeUnsafe(ctx context.Context, startTime time.Time) (int64, error) {
	// Calculate fence block ID: it should be at position n+1 where n is the last confirmed block
	// Use currentBlockNumber as the fence block ID (this is the next block that would be written)
	fenceBlockId := w.currentBlockNumber.Load()

	logger.Ctx(ctx).Info("Creating fence block for per-block mode",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int64("fenceBlockId", fenceBlockId),
		zap.Int64("lastSubmittedFlushingBlockID", w.lastSubmittedFlushingBlockID.Load()))

	// Create the fence block
	if err := w.createFenceBlockUnsafe(ctx, fenceBlockId); err != nil {
		logger.Ctx(ctx).Warn("Failed to create fence block",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId),
			zap.Int64("fenceBlockId", fenceBlockId),
			zap.Error(err))
		return w.GetLastEntryId(ctx), fmt.Errorf("failed to create fence block: %w", err)
	}

	// Also create a fence flag file for backward compatibility and quick detection
	fenceFlagPath := getFenceFlagPath(w.baseDir, w.logId, w.segmentId)
	fenceFile, err := os.Create(fenceFlagPath)
	if err == nil {
		fenceInfo := fmt.Sprintf("logId=%d\nsegmentId=%d\npid=%d\ntimestamp=%d\nreason=manual_fence\ntype=async_writer\nmode=per_block\nfence_block_id=%d\n",
			w.logId, w.segmentId, os.Getpid(), time.Now().Unix(), fenceBlockId)
		fenceFile.WriteString(fenceInfo)
		fenceFile.Close()
	}

	// Mark as fenced and stop accepting writes
	w.fenced.Store(true)

	lastEntryId := w.GetLastEntryId(ctx)
	logger.Ctx(ctx).Info("Successfully created fence block (per-block mode)",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int64("fenceBlockId", fenceBlockId),
		zap.Int64("lastEntryId", lastEntryId))

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "fence_per_block", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "fence_per_block", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return lastEntryId, nil
}

// =============================================================================
// Compact Implementation
// =============================================================================

// mergeBlockTask represents a single merge block task for compaction
type mergeBlockTask struct {
	blocks        []*codec.IndexRecord // Original blocks to be merged
	mergedBlockID int64                // The new merged block ID
	firstEntryID  int64                // First entry ID for header (only used for first merged block)
	isFirstBlock  bool                 // Whether this is the first merged block
	nextEntryID   int64                // Next expected entry ID for sequential validation
}

// mergedBlockResult represents the result of a merge block task
type mergedBlockResult struct {
	blockIndex *codec.IndexRecord
	blockSize  int64
	error      error
}

// Compact performs compaction by merging small blocks into larger blocks
func (w *LocalFileWriter) Compact(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "Compact")
	defer sp.End()
	startTime := time.Now()

	w.mu.Lock()
	defer w.mu.Unlock()

	logger.Ctx(ctx).Info("starting segment compaction",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int("currentBlockCount", len(w.blockIndexes)))

	// Check if segment is finalized
	if !w.finalized.Load() {
		logger.Ctx(ctx).Warn("segment must be finalized before compaction",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId))
		return -1, fmt.Errorf("segment must be finalized before compaction")
	}

	// Read footer to check if already compacted
	footerPath := serde.GetFooterBlockPath(w.baseDir, w.logId, w.segmentId)
	footerData, err := os.ReadFile(footerPath)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to read footer for compaction check",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId),
			zap.Error(err))
		return -1, fmt.Errorf("read footer: %w", err)
	}

	footerAndIndexes, err := serde.DeserializeFooterAndIndexes(footerData)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to parse footer",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId),
			zap.Error(err))
		return -1, fmt.Errorf("parse footer: %w", err)
	}
	footerRecord := footerAndIndexes.Footer
	blockIndexes := footerAndIndexes.Indexes

	// Check if already compacted
	if codec.IsCompacted(footerRecord.Flags) {
		logger.Ctx(ctx).Info("segment is already compacted, checking for cleanup of original files",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId))

		// Clean up any remaining original files
		cleanupErr := w.cleanupOriginalFilesIfCompacted(ctx)
		if cleanupErr != nil {
			logger.Ctx(ctx).Warn("failed to cleanup remaining original files after detecting compacted segment",
				zap.Int64("logId", w.logId),
				zap.Int64("segmentId", w.segmentId),
				zap.Error(cleanupErr))
		}

		return int64(footerRecord.TotalSize), nil
	}

	// Get target block size for compaction (use maxFlushSize as target)
	targetBlockSize := w.maxFlushSize
	if targetBlockSize <= 0 {
		targetBlockSize = 2 * 1024 * 1024 // Default 2MB
	}

	// Stream merge and write blocks
	newBlockIndexes, fileSizeAfterCompact, err := w.streamMergeAndWriteBlocks(ctx, blockIndexes, targetBlockSize)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to stream merge and write blocks",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId),
			zap.Error(err))
		return -1, fmt.Errorf("failed to stream merge and write blocks: %w", err)
	}

	if len(newBlockIndexes) == 0 {
		logger.Ctx(ctx).Info("no blocks to compact",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId))
		return -1, nil
	}

	// Create new footer with compacted flag
	newFooter := &codec.FooterRecord{
		TotalBlocks:  int32(len(newBlockIndexes)),
		TotalRecords: footerRecord.TotalRecords,
		TotalSize:    uint64(fileSizeAfterCompact),
		IndexOffset:  0,
		IndexLength:  uint32(len(newBlockIndexes) * (codec.RecordHeaderSize + codec.IndexRecordSize)),
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(footerRecord.Flags),
		LAC:          footerRecord.LAC,
	}

	// Serialize new footer and indexes
	newFooterData := w.serializeCompactedFooterAndIndexes(ctx, newBlockIndexes, newFooter)

	// Write new footer using two-stage atomic write
	footerInflightPath := serde.GetFooterInflightPath(w.baseDir, w.logId, w.segmentId)

	// Stage 1: Write to footer.blk.inflight
	if err := os.WriteFile(footerInflightPath, newFooterData, 0644); err != nil {
		logger.Ctx(ctx).Warn("failed to write compacted footer inflight",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId),
			zap.Error(err))
		return -1, fmt.Errorf("write compacted footer inflight: %w", err)
	}

	// Stage 2: Rename footer.blk.inflight to footer.blk
	if err := os.Rename(footerInflightPath, footerPath); err != nil {
		os.Remove(footerInflightPath)
		logger.Ctx(ctx).Warn("failed to rename compacted footer",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId),
			zap.Error(err))
		return -1, fmt.Errorf("rename compacted footer: %w", err)
	}

	// Clean up original block files after successful compaction
	originalBlockCount := len(blockIndexes)

	// Update internal state
	w.blockIndexes = newBlockIndexes

	logger.Ctx(ctx).Info("compaction completed, starting cleanup of original blocks",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int("originalBlockCount", originalBlockCount),
		zap.Int("compactedBlockCount", len(newBlockIndexes)))

	// Delete original block files concurrently
	cleanupErr := w.deleteOriginalBlocks(ctx, blockIndexes)
	if cleanupErr != nil {
		// Log error but don't fail the compaction - the compacted files are already created
		logger.Ctx(ctx).Warn("failed to cleanup some original block files after compaction",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId),
			zap.Error(cleanupErr))
	} else {
		logger.Ctx(ctx).Info("successfully cleaned up all original block files",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId),
			zap.Int("deletedBlocks", originalBlockCount))
	}

	logger.Ctx(ctx).Info("successfully compacted segment",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int("originalBlockCount", originalBlockCount),
		zap.Int("compactedBlockCount", len(newBlockIndexes)),
		zap.Int64("fileSizeAfterCompact", fileSizeAfterCompact),
		zap.Int64("costMs", time.Since(startTime).Milliseconds()))
	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "compact", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "compact", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return fileSizeAfterCompact, nil
}

// streamMergeAndWriteBlocks reads original blocks, merges them, and writes as new merged blocks
func (w *LocalFileWriter) streamMergeAndWriteBlocks(ctx context.Context, blockIndexes []*codec.IndexRecord, targetBlockSize int64) ([]*codec.IndexRecord, int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "streamMergeAndWriteBlocks")
	defer sp.End()
	startTime := time.Now()

	if len(blockIndexes) == 0 {
		return nil, 0, nil
	}

	// Plan merge tasks
	mergeTasks := w.planMergeTasks(ctx, blockIndexes, targetBlockSize)
	logger.Ctx(ctx).Info("planned merge block tasks",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int("originalBlockCount", len(blockIndexes)),
		zap.Int("mergeBlockTasks", len(mergeTasks)))

	// Create a pool for concurrent merge tasks
	maxMergeWorkers := w.maxFlushThreads
	if maxMergeWorkers <= 0 {
		maxMergeWorkers = 4
	}
	mergePool := conc.NewPool[*mergedBlockResult](maxMergeWorkers, conc.WithPreAlloc(true))
	defer mergePool.Release()

	// Submit all merge tasks concurrently
	var mergeFutures []*conc.Future[*mergedBlockResult]
	for _, task := range mergeTasks {
		taskCopy := task
		future := mergePool.Submit(func() (*mergedBlockResult, error) {
			result := w.processMergeTask(ctx, taskCopy)
			return result, result.error
		})
		mergeFutures = append(mergeFutures, future)
	}

	// Wait for all futures to complete and collect results
	type pendingRename struct {
		completedPath string
		finalPath     string
		blockIndex    *codec.IndexRecord
		blockSize     int64
	}
	var pendingRenames []pendingRename
	var firstError error

	for i, future := range mergeFutures {
		result := future.Value()
		if result.error != nil {
			if firstError == nil {
				firstError = result.error
			}
			logger.Ctx(ctx).Warn("merge task failed",
				zap.Int("taskIndex", i),
				zap.Error(result.error))
			continue
		}

		// Collect pending rename info
		completedPath := serde.GetMergedBlockCompletedPath(w.baseDir, w.logId, w.segmentId, int64(result.blockIndex.BlockNumber))
		finalPath := serde.GetMergedBlockFilePath(w.baseDir, w.logId, w.segmentId, int64(result.blockIndex.BlockNumber))
		pendingRenames = append(pendingRenames, pendingRename{
			completedPath: completedPath,
			finalPath:     finalPath,
			blockIndex:    result.blockIndex,
			blockSize:     result.blockSize,
		})
	}

	if firstError != nil {
		// Clean up any completed files
		for _, pr := range pendingRenames {
			os.Remove(pr.completedPath)
		}
		return nil, -1, fmt.Errorf("merge task failed: %w", firstError)
	}

	// Sequential rename: completed -> final (Stage 3)
	var newBlockIndexes []*codec.IndexRecord
	var fileSizeAfterCompact int64

	for _, pr := range pendingRenames {
		if err := os.Rename(pr.completedPath, pr.finalPath); err != nil {
			logger.Ctx(ctx).Warn("failed to rename completed to final",
				zap.String("completedPath", pr.completedPath),
				zap.String("finalPath", pr.finalPath),
				zap.Error(err))
			// Clean up remaining completed files
			for _, remaining := range pendingRenames {
				os.Remove(remaining.completedPath)
			}
			return nil, -1, fmt.Errorf("rename completed to final: %w", err)
		}
		newBlockIndexes = append(newBlockIndexes, pr.blockIndex)
		fileSizeAfterCompact += pr.blockSize
	}

	logger.Ctx(ctx).Info("completed parallel merge block processing",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int("originalBlockCount", len(blockIndexes)),
		zap.Int("mergedBlockCount", len(newBlockIndexes)),
		zap.Int64("fileSizeAfterCompact", fileSizeAfterCompact),
		zap.Int64("costMs", time.Since(startTime).Milliseconds()))

	return newBlockIndexes, fileSizeAfterCompact, nil
}

// planMergeTasks creates merge tasks by grouping blocks based on target size
func (w *LocalFileWriter) planMergeTasks(ctx context.Context, blockIndexes []*codec.IndexRecord, targetBlockSize int64) []*mergeBlockTask {
	var tasks []*mergeBlockTask
	var currentBlocks []*codec.IndexRecord
	var currentSize int64
	mergedBlockID := int64(0)
	isFirst := true
	var firstEntryID int64

	for _, block := range blockIndexes {
		if isFirst {
			firstEntryID = block.FirstEntryID
		}

		blockSize := int64(block.BlockSize)
		if currentSize+blockSize > targetBlockSize && len(currentBlocks) > 0 {
			// Create task for current group
			tasks = append(tasks, &mergeBlockTask{
				blocks:        currentBlocks,
				mergedBlockID: mergedBlockID,
				firstEntryID:  firstEntryID,
				isFirstBlock:  mergedBlockID == 0,
				nextEntryID:   currentBlocks[len(currentBlocks)-1].LastEntryID + 1,
			})
			mergedBlockID++
			currentBlocks = nil
			currentSize = 0
			firstEntryID = block.FirstEntryID
		}
		currentBlocks = append(currentBlocks, block)
		currentSize += blockSize
		isFirst = false
	}

	// Add remaining blocks as final task
	if len(currentBlocks) > 0 {
		tasks = append(tasks, &mergeBlockTask{
			blocks:        currentBlocks,
			mergedBlockID: mergedBlockID,
			firstEntryID:  firstEntryID,
			isFirstBlock:  mergedBlockID == 0,
			nextEntryID:   currentBlocks[len(currentBlocks)-1].LastEntryID + 1,
		})
	}

	return tasks
}

// processMergeTask reads original blocks, merges them, and writes a new merged block
func (w *LocalFileWriter) processMergeTask(ctx context.Context, task *mergeBlockTask) *mergedBlockResult {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "processMergeTask")
	defer sp.End()
	startTime := time.Now()

	blocks := task.blocks
	mergedBlockID := task.mergedBlockID

	logger.Ctx(ctx).Debug("processing merge task",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int("originalBlockCount", len(blocks)),
		zap.Int64("mergedBlockID", mergedBlockID))

	// Read all block data concurrently
	type blockData struct {
		index int
		data  []byte
		err   error
	}

	blockDataChan := make(chan blockData, len(blocks))
	var wg sync.WaitGroup

	for i, block := range blocks {
		wg.Add(1)
		go func(idx int, blk *codec.IndexRecord) {
			defer wg.Done()
			blockPath := serde.GetBlockFilePath(w.baseDir, w.logId, w.segmentId, int64(blk.BlockNumber))
			data, err := os.ReadFile(blockPath)
			blockDataChan <- blockData{index: idx, data: data, err: err}
		}(i, block)
	}

	wg.Wait()
	close(blockDataChan)

	// Collect and sort results
	blockDataList := make([][]byte, len(blocks))
	for bd := range blockDataChan {
		if bd.err != nil {
			return &mergedBlockResult{error: fmt.Errorf("read block %d: %w", blocks[bd.index].BlockNumber, bd.err)}
		}
		blockDataList[bd.index] = bd.data
	}

	// Merge block data - extract entries from each block and combine
	var mergedEntries []*serde.BlockEntry
	for i, data := range blockDataList {
		entries, err := serde.ParseBlockEntries(data)
		if err != nil {
			return &mergedBlockResult{error: fmt.Errorf("parse block %d entries: %w", blocks[i].BlockNumber, err)}
		}
		mergedEntries = append(mergedEntries, entries...)
	}

	// Serialize merged block
	mergedData := serde.SerializeBlock(mergedBlockID, mergedEntries, false)

	// Three-stage atomic write for merged block
	inflightPath := serde.GetMergedBlockInflightPath(w.baseDir, w.logId, w.segmentId, mergedBlockID)
	completedPath := serde.GetMergedBlockCompletedPath(w.baseDir, w.logId, w.segmentId, mergedBlockID)

	// Stage 1: Write to inflight
	if err := os.WriteFile(inflightPath, mergedData, 0644); err != nil {
		return &mergedBlockResult{error: fmt.Errorf("write merged block inflight: %w", err)}
	}

	// Stage 2: Rename inflight to completed
	if err := os.Rename(inflightPath, completedPath); err != nil {
		os.Remove(inflightPath)
		return &mergedBlockResult{error: fmt.Errorf("rename inflight to completed: %w", err)}
	}

	// Create index record for merged block
	var firstEntry, lastEntry int64
	if len(mergedEntries) > 0 {
		firstEntry = mergedEntries[0].EntryId
		lastEntry = mergedEntries[len(mergedEntries)-1].EntryId
	}

	indexRecord := &codec.IndexRecord{
		BlockNumber:  int32(mergedBlockID),
		StartOffset:  0,
		BlockSize:    uint32(len(mergedData)),
		FirstEntryID: firstEntry,
		LastEntryID:  lastEntry,
	}

	logger.Ctx(ctx).Debug("completed merge task",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int64("mergedBlockID", mergedBlockID),
		zap.Int("originalBlockCount", len(blocks)),
		zap.Int("mergedEntryCount", len(mergedEntries)),
		zap.Int64("mergedBlockSize", int64(len(mergedData))),
		zap.Int64("costMs", time.Since(startTime).Milliseconds()))

	return &mergedBlockResult{
		blockIndex: indexRecord,
		blockSize:  int64(len(mergedData)),
	}
}

// serializeCompactedFooterAndIndexes serializes the footer and indexes for compacted segment
func (w *LocalFileWriter) serializeCompactedFooterAndIndexes(ctx context.Context, blockIndexes []*codec.IndexRecord, footer *codec.FooterRecord) []byte {
	serializedData := make([]byte, 0)

	// Serialize all block index records
	for _, record := range blockIndexes {
		encodedIndex := codec.EncodeRecord(record)
		serializedData = append(serializedData, encodedIndex...)
	}

	// Serialize footer record
	encodedFooter := codec.EncodeRecord(footer)
	serializedData = append(serializedData, encodedFooter...)

	logger.Ctx(ctx).Debug("serialized compacted footer and indexes",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int("blockCount", len(blockIndexes)),
		zap.Int("totalSize", len(serializedData)))

	return serializedData
}

// deleteOriginalBlocks deletes original block files after successful compaction
func (w *LocalFileWriter) deleteOriginalBlocks(ctx context.Context, blockIndexes []*codec.IndexRecord) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "deleteOriginalBlocks")
	defer sp.End()
	startTime := time.Now()

	var failedDeletions []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Delete concurrently
	sem := make(chan struct{}, w.maxFlushThreads)

	for _, block := range blockIndexes {
		wg.Add(1)
		go func(blockNum int32) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			blockPath := serde.GetBlockFilePath(w.baseDir, w.logId, w.segmentId, int64(blockNum))
			if err := os.Remove(blockPath); err != nil && !os.IsNotExist(err) {
				mu.Lock()
				failedDeletions = append(failedDeletions, blockPath)
				mu.Unlock()
				logger.Ctx(ctx).Warn("failed to delete original block",
					zap.String("blockPath", blockPath),
					zap.Error(err))
			}
		}(block.BlockNumber)
	}

	wg.Wait()

	logger.Ctx(ctx).Info("completed deletion of original block files",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int("totalBlocks", len(blockIndexes)),
		zap.Int("failedDeletions", len(failedDeletions)),
		zap.Int64("deletionTimeMs", time.Since(startTime).Milliseconds()))

	if len(failedDeletions) > 0 {
		return fmt.Errorf("failed to delete %d out of %d original block files", len(failedDeletions), len(blockIndexes))
	}

	return nil
}

// cleanupOriginalFilesIfCompacted cleans up any remaining original block files
// when the segment is already compacted. This handles interrupted compact operations.
func (w *LocalFileWriter) cleanupOriginalFilesIfCompacted(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "cleanupOriginalFilesIfCompacted")
	defer sp.End()

	segmentDir := serde.GetSegmentDir(w.baseDir, w.logId, w.segmentId)
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read segment dir: %w", err)
	}

	var originalBlockPaths []string
	for _, entry := range entries {
		name := entry.Name()
		if serde.IsOriginalBlockFile(name) {
			originalBlockPaths = append(originalBlockPaths, filepath.Join(segmentDir, name))
		}
	}

	if len(originalBlockPaths) == 0 {
		logger.Ctx(ctx).Debug("no original block files found to cleanup",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId))
		return nil
	}

	logger.Ctx(ctx).Info("found original block files to cleanup after compaction",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int("originalBlockCount", len(originalBlockPaths)))

	// Delete the original block files
	var failedDeletions []string
	for _, path := range originalBlockPaths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			failedDeletions = append(failedDeletions, path)
			logger.Ctx(ctx).Warn("failed to delete original block file",
				zap.String("path", path),
				zap.Error(err))
		}
	}

	if len(failedDeletions) > 0 {
		return fmt.Errorf("failed to delete %d out of %d original block files", len(failedDeletions), len(originalBlockPaths))
	}

	return nil
}

// cleanupBlockFilesUnsafe removes all block files in the segment directory (for fresh start)
func (w *LocalFileWriter) cleanupBlockFilesUnsafe(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "cleanupBlockFiles")
	defer sp.End()

	segmentDir := getSegmentDir(w.baseDir, w.logId, w.segmentId)

	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read segment dir: %w", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		// Remove block files (.blk, .blk.inflight, .blk.completed, .blk.fence.inflight), footer.blk and footer.blk.inflight
		// But keep write.lock and write.fence
		if strings.HasSuffix(name, ".blk") || strings.HasSuffix(name, ".blk.inflight") || strings.HasSuffix(name, ".blk.completed") || strings.HasSuffix(name, ".blk.fence.inflight") {
			filePath := filepath.Join(segmentDir, name)
			if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
				logger.Ctx(ctx).Warn("failed to remove block file",
					zap.String("filePath", filePath),
					zap.Error(err))
			} else {
				logger.Ctx(ctx).Debug("removed block file",
					zap.String("filePath", filePath))
			}
		}
	}

	return nil
}

// recoverFromBlockFilesUnsafe recovers state from existing per-block files or legacy data.log
func (w *LocalFileWriter) recoverFromBlockFilesUnsafe(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "recoverFromBlockFiles")
	defer sp.End()
	startTime := time.Now()

	segmentDir := getSegmentDir(w.baseDir, w.logId, w.segmentId)

	// Check if directory exists
	if _, err := os.Stat(segmentDir); os.IsNotExist(err) {
		return nil
	}

	// Recover fence state
	isFenceFlagExists, _ := checkFenceFlagFileExists(ctx, w.baseDir, w.logId, w.segmentId)
	w.fenced.Store(isFenceFlagExists)

	// Check for footer.blk first (finalized segment in new format)
	footerPath := getFooterBlockPath(w.baseDir, w.logId, w.segmentId)
	footerInflightPath := getFooterInflightPath(w.baseDir, w.logId, w.segmentId)

	if _, err := os.Stat(footerPath); err == nil {
		// Footer exists, segment is finalized (new format)
		// Clean up any leftover footer.blk.inflight
		os.Remove(footerInflightPath)
		if err := w.recoverFromFooterBlockUnsafe(ctx, footerPath); err != nil {
			return fmt.Errorf("recover from footer block: %w", err)
		}
		w.finalized.Store(true)
		w.recovered.Store(true)
		return nil
	}

	// Check for footer.blk.inflight (crashed during finalization)
	if _, err := os.Stat(footerInflightPath); err == nil {
		// Try to recover from inflight footer file
		if err := w.recoverFromFooterBlockUnsafe(ctx, footerInflightPath); err != nil {
			// Inflight file is invalid, remove it and continue with block recovery
			logger.Ctx(ctx).Warn("invalid footer.blk.inflight file, removing",
				zap.String("footerInflightPath", footerInflightPath), zap.Error(err))
			os.Remove(footerInflightPath)
		} else {
			// Inflight file is valid, complete the rename
			if err := os.Rename(footerInflightPath, footerPath); err != nil {
				logger.Ctx(ctx).Warn("failed to rename footer.blk.inflight to footer.blk",
					zap.String("footerInflightPath", footerInflightPath), zap.Error(err))
				os.Remove(footerInflightPath)
			} else {
				logger.Ctx(ctx).Info("recovered footer from inflight file",
					zap.String("footerPath", footerPath))
				w.finalized.Store(true)
				w.recovered.Store(true)
				return nil
			}
		}
	}

	// Check for legacy data.log file (backward compatibility)
	if stat, err := os.Stat(w.segmentFilePath); err == nil && stat.Size() > 0 {
		logger.Ctx(ctx).Info("detected legacy data.log format, using legacy recovery",
			zap.String("segmentFilePath", w.segmentFilePath),
			zap.Int64("fileSize", stat.Size()))
		return w.recoverFromLegacyFileUnsafe(ctx)
	}

	// List directory for block files
	entries, err := os.ReadDir(segmentDir)
	if err != nil {
		return fmt.Errorf("read segment dir: %w", err)
	}

	// Clean up inflight, completed, and fence.inflight files (incomplete writes)
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".blk.inflight") || strings.HasSuffix(name, ".blk.completed") || strings.HasSuffix(name, ".blk.fence.inflight") {
			filePath := filepath.Join(segmentDir, name)
			if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
				logger.Ctx(ctx).Warn("failed to remove incomplete block file",
					zap.String("filePath", filePath),
					zap.Error(err))
			} else {
				logger.Ctx(ctx).Debug("removed incomplete block file",
					zap.String("filePath", filePath))
			}
		}
	}

	_ = startTime // Used for metrics at end of function

	// Collect final block files and detect fence directories
	var blockIds []int64
	var fenceDirBlockId int64 = -1 // -1 means no fence directory detected
	for _, entry := range entries {
		name := entry.Name()
		// Check for fence directories first
		// A fence directory has the same name pattern as a block file (N.blk) but is a directory
		if entry.IsDir() {
			isFence, fenceId := serde.IsFenceDirectory(name, true)
			if isFence {
				logger.Ctx(ctx).Info("detected fence directory during recovery",
					zap.Int64("logId", w.logId),
					zap.Int64("segmentId", w.segmentId),
					zap.String("name", name),
					zap.Int64("fenceBlockId", fenceId))
				// Track the lowest fence block ID (earliest fence position)
				if fenceDirBlockId == -1 || fenceId < fenceDirBlockId {
					fenceDirBlockId = fenceId
				}
			}
			continue
		}
		if strings.HasSuffix(name, ".blk") && !strings.Contains(name, ".blk.") && name != "footer.blk" {
			// Parse block ID from filename
			blockIdStr := strings.TrimSuffix(name, ".blk")
			blockId, err := strconv.ParseInt(blockIdStr, 10, 64)
			if err != nil {
				logger.Ctx(ctx).Warn("invalid block filename",
					zap.String("filename", name),
					zap.Error(err))
				continue
			}
			blockIds = append(blockIds, blockId)
		}
	}

	// If fence directory detected, mark as fenced early
	if fenceDirBlockId >= 0 {
		logger.Ctx(ctx).Info("fence directory detected, filtering block IDs",
			zap.Int64("fenceDirBlockId", fenceDirBlockId),
			zap.Int("totalBlockIds", len(blockIds)))
		// Filter out blocks at or after the fence position
		var filteredIds []int64
		for _, id := range blockIds {
			if id < fenceDirBlockId {
				filteredIds = append(filteredIds, id)
			}
		}
		blockIds = filteredIds
		w.fenced.Store(true)
	}

	// Sort block IDs
	sort.Slice(blockIds, func(i, j int) bool { return blockIds[i] < blockIds[j] })

	// Verify block sequence is continuous (0, 1, 2, ...)
	var validBlockIds []int64
	for i, blockId := range blockIds {
		if int64(i) != blockId {
			logger.Ctx(ctx).Warn("block sequence gap detected, stopping recovery",
				zap.Int64("expectedBlockId", int64(i)),
				zap.Int64("actualBlockId", blockId))
			break
		}
		validBlockIds = append(validBlockIds, blockId)
	}

	// Parse each valid block file
	var firstEntryId int64 = -1
	var lastEntryId int64 = -1
	var fenceBlockDetected bool
	var actualValidBlockIds []int64
	for _, blockId := range validBlockIds {
		blockPath := getBlockFilePath(w.baseDir, w.logId, w.segmentId, blockId)

		// Check if this is a fence directory first
		if isFenceDirectory(blockPath) {
			logger.Ctx(ctx).Info("detected fence directory during block parsing, stopping",
				zap.Int64("blockId", blockId),
				zap.String("blockPath", blockPath))
			fenceBlockDetected = true
			w.fenced.Store(true)
			break
		}

		blockHeader, err := w.parseBlockHeaderFromFile(ctx, blockPath)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to parse block header",
				zap.String("blockPath", blockPath),
				zap.Error(err))
			break
		}

		// Check if this is a fence block file (legacy mechanism)
		if isFenceBlock(blockHeader) {
			logger.Ctx(ctx).Info("detected fence block file during recovery, stopping",
				zap.Int64("blockId", blockId),
				zap.String("blockPath", blockPath))
			fenceBlockDetected = true
			w.fenced.Store(true)
			break
		}

		// Get file size for block size
		stat, err := os.Stat(blockPath)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to stat block file",
				zap.String("blockPath", blockPath),
				zap.Error(err))
			break
		}

		// Create index record
		indexRecord := &codec.IndexRecord{
			BlockNumber:  int32(blockId),
			StartOffset:  blockId, // Use blockId as virtual offset
			BlockSize:    uint32(stat.Size()),
			FirstEntryID: blockHeader.FirstEntryID,
			LastEntryID:  blockHeader.LastEntryID,
		}
		w.blockIndexes = append(w.blockIndexes, indexRecord)
		actualValidBlockIds = append(actualValidBlockIds, blockId)

		if firstEntryId == -1 || blockHeader.FirstEntryID < firstEntryId {
			firstEntryId = blockHeader.FirstEntryID
		}
		if blockHeader.LastEntryID > lastEntryId {
			lastEntryId = blockHeader.LastEntryID
		}

		logger.Ctx(ctx).Debug("recovered block",
			zap.Int64("blockId", blockId),
			zap.Int64("firstEntryId", blockHeader.FirstEntryID),
			zap.Int64("lastEntryId", blockHeader.LastEntryID))
	}

	// Log if fence block was detected
	if fenceBlockDetected {
		logger.Ctx(ctx).Info("recovery stopped at fence block, writer marked as fenced",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId),
			zap.Int("recoveredBlockCount", len(actualValidBlockIds)))
	}

	// Update state
	if firstEntryId != -1 {
		w.firstEntryID.Store(firstEntryId)
	}
	if lastEntryId != -1 {
		w.lastEntryID.Store(lastEntryId)
	}
	if len(actualValidBlockIds) > 0 {
		w.currentBlockNumber.Store(int64(len(actualValidBlockIds)))
		w.lastSubmittedFlushingBlockID.Store(int64(len(actualValidBlockIds)) - 1)
		w.lastSubmittedFlushingEntryID.Store(lastEntryId)
	}

	w.recovered.Store(true)

	logger.Ctx(ctx).Info("recovered from block files",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int("blockCount", len(actualValidBlockIds)),
		zap.Int64("firstEntryId", firstEntryId),
		zap.Int64("lastEntryId", lastEntryId),
		zap.Bool("fenceBlockDetected", fenceBlockDetected),
		zap.Int64("costMs", time.Since(startTime).Milliseconds()))

	return nil
}

// recoverFromFooterBlockUnsafe recovers state from footer.blk file
func (w *LocalFileWriter) recoverFromFooterBlockUnsafe(ctx context.Context, footerPath string) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "recoverFromFooterBlock")
	defer sp.End()

	// Read footer.blk file
	data, err := os.ReadFile(footerPath)
	if err != nil {
		return fmt.Errorf("read footer file: %w", err)
	}

	if len(data) < codec.RecordHeaderSize+codec.FooterRecordSizeV5 {
		return fmt.Errorf("footer file too small: %d bytes", len(data))
	}

	// Parse footer from end
	maxFooterSize := codec.GetMaxFooterReadSize()
	footerData := data[len(data)-maxFooterSize:]
	footerRecord, err := codec.ParseFooterFromBytes(footerData)
	if err != nil {
		return fmt.Errorf("parse footer: %w", err)
	}

	// Parse index records from the beginning
	offset := 0
	var firstEntryId int64 = -1
	var lastEntryId int64 = -1
	var maxBlockId int64 = -1

	footerRecordSize := codec.RecordHeaderSize + codec.GetFooterRecordSize(footerRecord.Version)
	indexDataEnd := len(data) - footerRecordSize

	for offset < indexDataEnd {
		if offset+codec.RecordHeaderSize+codec.IndexRecordSize > indexDataEnd {
			break
		}

		record, err := codec.DecodeRecord(data[offset:])
		if err != nil {
			break
		}

		if record.Type() != codec.IndexRecordType {
			break
		}

		indexRecord := record.(*codec.IndexRecord)
		w.blockIndexes = append(w.blockIndexes, indexRecord)

		if firstEntryId == -1 || indexRecord.FirstEntryID < firstEntryId {
			firstEntryId = indexRecord.FirstEntryID
		}
		if indexRecord.LastEntryID > lastEntryId {
			lastEntryId = indexRecord.LastEntryID
		}
		if int64(indexRecord.BlockNumber) > maxBlockId {
			maxBlockId = int64(indexRecord.BlockNumber)
		}

		offset += codec.RecordHeaderSize + codec.IndexRecordSize
	}

	// Update state
	if firstEntryId != -1 {
		w.firstEntryID.Store(firstEntryId)
	}
	if lastEntryId != -1 {
		w.lastEntryID.Store(lastEntryId)
	}
	if maxBlockId != -1 {
		w.currentBlockNumber.Store(maxBlockId + 1)
		w.lastSubmittedFlushingBlockID.Store(maxBlockId)
		w.lastSubmittedFlushingEntryID.Store(lastEntryId)
	}

	logger.Ctx(ctx).Info("recovered from footer block",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int("blockCount", len(w.blockIndexes)),
		zap.Int64("firstEntryId", firstEntryId),
		zap.Int64("lastEntryId", lastEntryId))

	return nil
}

// parseBlockHeaderFromFile reads and parses the block header from a block file
func (w *LocalFileWriter) parseBlockHeaderFromFile(ctx context.Context, blockPath string) (*codec.BlockHeaderRecord, error) {
	file, err := os.Open(blockPath)
	if err != nil {
		return nil, fmt.Errorf("open block file: %w", err)
	}
	defer file.Close()

	// Read block header (RecordHeader + BlockHeaderRecord)
	headerSize := codec.RecordHeaderSize + codec.BlockHeaderRecordSize
	headerData := make([]byte, headerSize)
	n, err := file.Read(headerData)
	if err != nil || n < headerSize {
		return nil, fmt.Errorf("read block header: %w", err)
	}

	record, err := codec.DecodeRecord(headerData)
	if err != nil {
		return nil, fmt.Errorf("decode block header: %w", err)
	}

	if record.Type() != codec.BlockHeaderRecordType {
		return nil, fmt.Errorf("unexpected record type: %d", record.Type())
	}

	return record.(*codec.BlockHeaderRecord), nil
}

// =========================================================================================
// UTILITY FUNCTIONS (shared by both per-block mode and legacy mode)
// =========================================================================================
// NOTE: Legacy mode support functions have been moved to writer_impl_legacy.go for better
// code organization. See that file for backward compatibility with data.log format.
// =========================================================================================

// createSegmentLock creates a lock file for the segment to ensure exclusive access
func (s *LocalFileWriter) createSegmentLock(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "createSegmentLock")
	defer sp.End()

	// Create lock file path
	s.lockFilePath = getSegmentLockPath(s.baseDir, s.logId, s.segmentId)

	// Create flock instance
	s.lockFile = flock.New(s.lockFilePath)

	// Try to acquire exclusive lock (non-blocking)
	locked, err := s.lockFile.TryLock()
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to try lock file",
			zap.String("lockFilePath", s.lockFilePath),
			zap.Error(err))
		return errors.Wrapf(err, "failed to try lock file: %s", s.lockFilePath)
	}

	if !locked {
		logger.Ctx(ctx).Warn("Failed to acquire exclusive lock - file is already locked",
			zap.String("lockFilePath", s.lockFilePath))
		return errors.Errorf("failed to acquire exclusive lock on file: %s (already locked by another process)", s.lockFilePath)
	}

	// Write segment info to lock file (optional, for debugging purposes)
	lockInfo := fmt.Sprintf("logId=%d\nsegmentId=%d\npid=%d\ntimestamp=%d\n",
		s.logId, s.segmentId, os.Getpid(), time.Now().Unix())

	// Create or write to the lock file for information purposes
	if infoFile, err := os.OpenFile(s.lockFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644); err == nil {
		infoFile.WriteString(lockInfo)
		infoFile.Sync()
		infoFile.Close()
	} else {
		logger.Ctx(ctx).Warn("Failed to write lock info",
			zap.String("lockFilePath", s.lockFilePath),
			zap.Error(err))
		// Continue even if writing lock info fails, as the lock itself is more important
	}

	logger.Ctx(ctx).Info("Successfully created and locked segment lock file",
		zap.String("lockFilePath", s.lockFilePath),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId))

	return nil
}

// releaseSegmentLock releases the segment lock and removes the lock file
func (s *LocalFileWriter) releaseSegmentLock(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "releaseSegmentLock")
	defer sp.End()

	if s.lockFile == nil {
		logger.Ctx(ctx).Debug("No lock file to release")
		return nil
	}

	// Release the flock
	err := s.lockFile.Unlock()
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to release flock",
			zap.String("lockFilePath", s.lockFilePath),
			zap.Error(err))
	}

	// Remove the lock file
	if s.lockFilePath != "" {
		if err := os.Remove(s.lockFilePath); err != nil {
			logger.Ctx(ctx).Warn("Failed to remove lock file",
				zap.String("lockFilePath", s.lockFilePath),
				zap.Error(err))
		} else {
			logger.Ctx(ctx).Info("Successfully removed segment lock file",
				zap.String("lockFilePath", s.lockFilePath),
				zap.Int64("logId", s.logId),
				zap.Int64("segmentId", s.segmentId))
		}
	}

	s.lockFile = nil
	s.lockFilePath = ""
	return nil
}

// Path utility functions (pure functions, no state)
func getSegmentLockPath(baseDir string, logId int64, segmentId int64) string {
	return filepath.Join(baseDir, fmt.Sprintf("%d/%d/write.lock", logId, segmentId))
}

// Path helper functions - delegate to serde package for consistency across all backends
func getSegmentDir(baseDir string, logId int64, segmentId int64) string {
	return serde.GetSegmentDir(baseDir, logId, segmentId)
}

// Note: getSegmentFilePath is now defined in paths_legacy.go as it's for the legacy data.log format.

func getBlockFilePath(baseDir string, logId int64, segmentId int64, blockId int64) string {
	return serde.GetBlockFilePath(baseDir, logId, segmentId, blockId)
}

func getInflightBlockPath(baseDir string, logId int64, segmentId int64, blockId int64) string {
	return serde.GetInflightBlockPath(baseDir, logId, segmentId, blockId)
}

func getCompletedBlockPath(baseDir string, logId int64, segmentId int64, blockId int64) string {
	return serde.GetCompletedBlockPath(baseDir, logId, segmentId, blockId)
}

func getFooterBlockPath(baseDir string, logId int64, segmentId int64) string {
	return serde.GetFooterBlockPath(baseDir, logId, segmentId)
}

func getFooterInflightPath(baseDir string, logId int64, segmentId int64) string {
	return serde.GetFooterInflightPath(baseDir, logId, segmentId)
}

// Note: getFenceFlagPath and getFenceBlockInflightPath are now defined in paths_legacy.go
// as they are disk-specific legacy functions.

// isFenceBlock checks if a block header indicates a fence block
func isFenceBlock(header *codec.BlockHeaderRecord) bool {
	return serde.IsFenceBlock(header)
}

// checkAndHandleFenceBlockUnsafe checks if a block path is fenced.
// It checks for both fence directories (new mechanism) and fence block files (legacy).
// Returns true if the path is fenced (either a directory or a fence block file), false otherwise.
func (w *LocalFileWriter) checkAndHandleFenceBlockUnsafe(ctx context.Context, blockPath string) bool {
	// Check if something exists at this path
	info, err := os.Stat(blockPath)
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		logger.Ctx(ctx).Debug("failed to stat block path for fence check",
			zap.String("blockPath", blockPath),
			zap.Error(err))
		return false
	}

	// Check if it's a fence directory (new mechanism)
	if info.IsDir() {
		logger.Ctx(ctx).Info("detected fence directory",
			zap.String("blockPath", blockPath))
		return true
	}

	// Check if it's a fence block file (legacy mechanism for backward compatibility)
	blockHeader, err := w.parseBlockHeaderFromFile(ctx, blockPath)
	if err != nil {
		logger.Ctx(ctx).Debug("failed to parse block header for fence check",
			zap.String("blockPath", blockPath),
			zap.Error(err))
		return false
	}

	// Check if it's a fence block
	if isFenceBlock(blockHeader) {
		logger.Ctx(ctx).Info("detected fence block file (legacy)",
			zap.String("blockPath", blockPath),
			zap.Int32("blockNumber", blockHeader.BlockNumber),
			zap.Int64("firstEntryID", blockHeader.FirstEntryID),
			zap.Int64("lastEntryID", blockHeader.LastEntryID))
		return true
	}

	return false
}

// createFenceBlockUnsafe creates a fence directory at the specified block ID.
// The fence is implemented as a directory named {blockId}.blk. When a stale writer
// tries to rename a file to this path, the rename will fail on all operating systems
// because renaming a file to a directory path is not allowed.
// This provides a cross-platform fencing mechanism.
func (w *LocalFileWriter) createFenceBlockUnsafe(ctx context.Context, blockId int64) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "createFenceBlockUnsafe")
	defer sp.End()

	// Get the fence directory path (same as block file path, but will be a directory)
	fenceDirPath := getFenceBlockDirPath(w.baseDir, w.logId, w.segmentId, blockId)

	// Check if something already exists at this path
	info, err := os.Stat(fenceDirPath)
	if err == nil {
		// Something exists
		if info.IsDir() {
			// Already a fence directory, idempotent success
			logger.Ctx(ctx).Info("fence directory already exists",
				zap.Int64("logId", w.logId),
				zap.Int64("segmentId", w.segmentId),
				zap.Int64("blockId", blockId),
				zap.String("fenceDirPath", fenceDirPath))
			return nil
		}
		// It's a file (data block exists), we need to remove it first
		// This should only happen in edge cases where fence is called on an already written block
		logger.Ctx(ctx).Warn("removing existing block file to create fence directory",
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId),
			zap.Int64("blockId", blockId),
			zap.String("fenceDirPath", fenceDirPath))
		if removeErr := os.Remove(fenceDirPath); removeErr != nil {
			return fmt.Errorf("remove existing block file for fence: %w", removeErr)
		}
	}

	// Create the fence directory
	// Using MkdirAll to handle any parent directory issues
	if err := os.MkdirAll(fenceDirPath, 0755); err != nil {
		return fmt.Errorf("create fence directory: %w", err)
	}

	logger.Ctx(ctx).Info("created fence directory",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int64("blockId", blockId),
		zap.String("fenceDirPath", fenceDirPath))

	return nil
}

// isFenceDirectory checks if the given path is a fence directory
func isFenceDirectory(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

// checkFenceFlagFileExists checks for the existence of a fence flag file and returns (exists, error)
func checkFenceFlagFileExists(ctx context.Context, baseDir string, logId int64, segmentId int64) (bool, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "checkFenceFlagFileExists")
	defer sp.End()

	// Get fence flag file path
	fenceFlagPath := getFenceFlagPath(baseDir, logId, segmentId)

	// Check if fence flag file exists
	if _, err := os.Stat(fenceFlagPath); err == nil {
		// Fence flag file exists
		logger.Ctx(ctx).Debug("Fence flag file detected",
			zap.String("fenceFlagPath", fenceFlagPath),
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segmentId))
		return true, nil
	} else if os.IsNotExist(err) {
		// File doesn't exist, that's normal - no fence flag
		return false, nil
	} else {
		// Other error occurred while checking fence flag file
		logger.Ctx(ctx).Warn("Error checking fence flag file",
			zap.String("fenceFlagPath", fenceFlagPath),
			zap.Error(err))
		return false, err
	}
}

// waitForFenceCheckIntervalIfLockExists checks if lock file exists and waits for fence check interval
func waitForFenceCheckIntervalIfLockExists(ctx context.Context, baseDir string, logId int64, segmentId int64, fenceFlagPath string) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "waitForFenceCheckIntervalIfLockExists")
	defer sp.End()

	// Check if lock file exists
	lockFilePath := getSegmentLockPath(baseDir, logId, segmentId)
	if _, err := os.Stat(lockFilePath); err == nil {
		// Lock file exists, wait for fence check interval (5 seconds)
		logger.Ctx(ctx).Info("Lock file exists, waiting for fence check interval to ensure other processes detect fence flag",
			zap.String("lockFilePath", lockFilePath),
			zap.String("fenceFlagPath", fenceFlagPath),
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segmentId))

		select {
		case <-ctx.Done():
			logger.Ctx(ctx).Warn("Context cancelled while waiting for fence check interval",
				zap.String("fenceFlagPath", fenceFlagPath))
			return ctx.Err()
		case <-time.After(5 * time.Second):
			// Wait completed
			logger.Ctx(ctx).Info("Fence check interval wait completed",
				zap.String("fenceFlagPath", fenceFlagPath))
			return nil
		}
	} else if !os.IsNotExist(err) {
		// Error checking lock file (not "file not exists")
		logger.Ctx(ctx).Warn("Error checking lock file existence",
			zap.String("lockFilePath", lockFilePath),
			zap.Error(err))
		// Continue anyway, as fence flag file is already created
	}

	return nil
}

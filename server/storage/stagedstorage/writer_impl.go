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
}

// StagedFileWriter implements staged storage for segment files:
// writes to local disk during active phase, uploads to object storage during compact phase.
// Uses quorum fence mechanism and supports LAC maintenance.
// TODO: refactor to reuse code with MinioFileWriter & LocalFileWriter
type StagedFileWriter struct {
	mu sync.Mutex

	// Disk
	localBaseDir    string
	segmentFilePath string
	logId           int64
	segmentId       int64
	file            *os.File
	logIdStr        string // for metrics only

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

	// file state
	currentBlockNumber atomic.Int64
	blockIndexes       []*codec.IndexRecord
	writtenBytes       int64
	lastModifiedTime   time.Time
	recoveredFooter    *codec.FooterRecord // Footer recovered during initialization (only for finalized segments)

	// written state
	firstEntryID   atomic.Int64 // The first entryId written to disk
	lastEntryID    atomic.Int64 // The last entryId written to disk
	headerWritten  atomic.Bool  // Ensures header is written before data
	finalized      atomic.Bool
	fenced         atomic.Bool
	inRecoveryMode atomic.Bool
	recovered      atomic.Bool

	// Async flush management
	flushTaskChan                chan *blockFlushTask
	storageWritable              atomic.Bool // Indicates whether the segment is writable
	lastSubmittedFlushingEntryID atomic.Int64
	lastSubmittedFlushingBlockID atomic.Int64
	allUploadingTaskDone         atomic.Bool
	flushMu                      sync.Mutex // the mutex ensures sequential writing for each flush batch

	// Close management
	fileClose chan struct{} // Close signal
	closed    atomic.Bool
	runCtx    context.Context
	runCancel context.CancelFunc
}

// NewStagedFileWriter creates a new staged file writer
func NewStagedFileWriter(ctx context.Context, bucket string, rootPath string, localBaseDir string, logId int64, segmentId int64, storageCli objectstorage.ObjectStorage, cfg *config.Configuration) (*StagedFileWriter, error) {
	return NewStagedFileWriterWithMode(ctx, bucket, rootPath, localBaseDir, logId, segmentId, storageCli, cfg, false)
}

// NewStagedFileWriterWithMode creates a new staged file writer with recovery mode option
func NewStagedFileWriterWithMode(ctx context.Context, bucket string, rootPath string, localBaseDir string, logId int64, segmentId int64, storageCli objectstorage.ObjectStorage, cfg *config.Configuration, recoveryMode bool) (*StagedFileWriter, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "NewStagedFileWriterWithMode")
	defer sp.End()
	blockSize := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize.Int64()
	maxBufferEntries := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxEntries
	maxBytes := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes.Int64()
	flushQueueSize := max(int(maxBytes/blockSize), 300)
	maxInterval := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage.Milliseconds()
	logger.Ctx(ctx).Debug("creating new staged file writer",
		zap.String("localBaseDir", localBaseDir),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.String("bucket", bucket),
		zap.String("rootPath", rootPath),
		zap.Int64("maxBlockSize", blockSize),
		zap.Int("maxBufferEntries", maxBufferEntries),
		zap.Int("maxInterval", maxInterval),
		zap.Int("flushQueueSize", flushQueueSize),
		zap.Bool("recoveryMode", recoveryMode))

	segmentDir := getSegmentDir(localBaseDir, logId, segmentId)
	// Ensure directory exists
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		logger.Ctx(ctx).Warn("failed to create directory",
			zap.String("segmentDir", segmentDir),
			zap.Error(err))
		return nil, fmt.Errorf("create directory: %w", err)
	}

	filePath := getSegmentFilePath(localBaseDir, logId, segmentId)
	logger.Ctx(ctx).Debug("segment directory and file path prepared",
		zap.String("segmentDir", segmentDir),
		zap.String("filePath", filePath))

	// Create context for async operations
	runCtx, runCancel := context.WithCancel(context.Background())

	writer := &StagedFileWriter{
		localBaseDir:        localBaseDir,
		segmentFilePath:     filePath,
		logId:               logId,
		segmentId:           segmentId,
		logIdStr:            fmt.Sprintf("%d", logId),
		bucket:              bucket,
		rootPath:            rootPath,
		storageCli:          storageCli,
		compactPolicyConfig: &cfg.Woodpecker.Logstore.SegmentCompactionPolicy,
		blockIndexes:        make([]*codec.IndexRecord, 0),
		writtenBytes:        0,
		maxFlushSize:        blockSize,
		maxBufferEntries:    int64(maxBufferEntries),                    // Default max entries per buffer
		maxIntervalMs:       maxInterval,                                // 10ms default sync interval for more responsive syncing
		flushTaskChan:       make(chan *blockFlushTask, flushQueueSize), // Increased buffer size to reduce blocking
		runCtx:              runCtx,
		runCancel:           runCancel,
	}

	// Initialize atomic values
	writer.firstEntryID.Store(-1)
	writer.lastEntryID.Store(-1)
	writer.currentBlockNumber.Store(0)
	writer.headerWritten.Store(false)
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
		logger.Ctx(ctx).Debug("using default block size",
			zap.Int64("defaultBlockSize", writer.maxFlushSize))
	}

	logger.Ctx(ctx).Debug("writer configuration initialized",
		zap.Int64("maxFlushSize", writer.maxFlushSize),
		zap.Int64("maxBufferEntries", writer.maxBufferEntries),
		zap.Int("maxIntervalMs", writer.maxIntervalMs))

	// Initialize buffer
	startEntryId := int64(0)
	var file *os.File
	var err error

	shouldInRecoveryMode := writer.determineIfNeedRecoveryMode(recoveryMode)
	writer.inRecoveryMode.Store(shouldInRecoveryMode)

	if shouldInRecoveryMode {
		logger.Ctx(ctx).Debug("recovery mode enabled, attempting to recover from existing file")
		// Try to recover from existing file
		if err := writer.recoverFromExistingFileUnsafe(ctx); err != nil {
			logger.Ctx(ctx).Warn("failed to recover from existing file",
				zap.String("filePath", filePath),
				zap.Error(err))
			runCancel()
			return nil, fmt.Errorf("recover from existing file: %w", err)
		}
		if writer.lastEntryID.Load() != -1 {
			startEntryId = writer.lastEntryID.Load() + 1
			logger.Ctx(ctx).Debug("recovered writer state",
				zap.Int64("lastEntryID", writer.lastEntryID.Load()),
				zap.Int64("nextStartEntryId", startEntryId),
				zap.Bool("recovered", writer.recovered.Load()))
		}
		// Open file for appending (create if not exists)
		logger.Ctx(ctx).Debug("opening file for appending in recovery mode")
		file, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	} else {
		// Open file for writing (create if not exists, truncate if exists)
		logger.Ctx(ctx).Debug("opening file for writing (truncate mode)")
		file, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	}

	initialBuffer := cache.NewSequentialBuffer(logId, segmentId, startEntryId, writer.maxBufferEntries)
	writer.buffer.Store(initialBuffer)

	if err != nil {
		logger.Ctx(ctx).Warn("failed to open file",
			zap.String("filePath", filePath),
			zap.Bool("recoveryMode", recoveryMode),
			zap.Error(err))
		runCancel()
		if file != nil {
			closeFdErr := file.Close()
			if closeFdErr != nil {
				logger.Ctx(ctx).Warn("failed to close file",
					zap.String("filePath", filePath),
					zap.Error(closeFdErr))
			}
		}
		return nil, fmt.Errorf("open file: %w", err)
	}

	logger.Ctx(ctx).Debug("file opened successfully",
		zap.String("filePath", filePath),
		zap.Bool("recoveryMode", recoveryMode))

	writer.file = file

	logger.Ctx(ctx).Debug("buffer initialized",
		zap.Int64("startEntryId", startEntryId),
		zap.Int64("maxBufferEntries", writer.maxBufferEntries),
		zap.String("bufferInstance", fmt.Sprintf("%p", initialBuffer)))

	// Start async flush goroutine
	go writer.run()

	logger.Ctx(ctx).Info("staged file writer created successfully",
		zap.String("filePath", filePath),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.Int64("blockSize", blockSize),
		zap.Bool("recoveryMode", recoveryMode),
		zap.Int64("startEntryId", startEntryId),
		zap.Bool("recovered", writer.recovered.Load()),
		zap.String("writerInstance", fmt.Sprintf("%p", writer)))

	return writer, nil
}

// run is the main async goroutine that handles buffer flushing
func (w *StagedFileWriter) run() {
	ticker := time.NewTicker(time.Duration(w.maxIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	logger.Ctx(w.runCtx).Debug("StagedFileWriter run goroutine started",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId))

	for {
		select {
		case <-w.runCtx.Done():
			// Context cancelled, exit
			logger.Ctx(w.runCtx).Debug("StagedFileWriter run goroutine stopping due to context cancellation")
			return
		case flushTask, ok := <-w.flushTaskChan:
			// Process flush tasks with higher priority
			if !ok {
				// Channel closed, exit
				logger.Ctx(w.runCtx).Debug("StagedFileWriter run goroutine stopping due to channel close")
				return
			}
			if flushTask.entries == nil {
				logger.Ctx(context.TODO()).Debug("received termination signal, marking all upload tasks as done",
					zap.String("segmentFilePath", w.segmentFilePath))
				w.allUploadingTaskDone.Store(true)
				return
			}
			// Process flush task synchronously
			logger.Ctx(w.runCtx).Debug("Processing flush task from channel",
				zap.Int64("logId", w.logId),
				zap.Int64("segId", w.segmentId),
				zap.Int32("blockNumber", flushTask.blockNumber),
				zap.Int64("firstEntryId", flushTask.firstEntryId),
				zap.Int64("lastEntryId", flushTask.lastEntryId),
				zap.Int("entriesCount", len(flushTask.entries)))
			w.processFlushTask(w.runCtx, flushTask)
			logger.Ctx(w.runCtx).Debug("Flush task processing completed")
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

// rollBufferAndSubmitFlushTaskUnsafe rolls the current buffer and flushes it to disk (must be called with mu held)
func (w *StagedFileWriter) rollBufferAndSubmitFlushTaskUnsafe(ctx context.Context) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "rollBufferAndFlushUnsafe")
	defer sp.End()
	startTime := time.Now()
	currentBuffer := w.buffer.Load()
	if currentBuffer == nil {
		return
	}

	expectedNextEntryId := currentBuffer.ExpectedNextEntryId.Load()
	if expectedNextEntryId-currentBuffer.FirstEntryId == 0 {
		return
	}

	// Get entries from old buffer
	toFlushEntries, err := currentBuffer.ReadEntriesRange(currentBuffer.GetFirstEntryId(), currentBuffer.GetExpectedNextEntryId())
	if err != nil || len(toFlushEntries) == 0 {
		return
	}

	// Create flush task
	blockNumber := w.currentBlockNumber.Load()
	flushTask := &blockFlushTask{
		entries:      toFlushEntries,
		firstEntryId: toFlushEntries[0].EntryId,
		lastEntryId:  toFlushEntries[len(toFlushEntries)-1].EntryId,
		blockNumber:  int32(blockNumber),
	}

	restData, err := currentBuffer.ReadEntriesToLast(expectedNextEntryId)
	if err != nil {
		return
	}
	restDataFirstEntryId := expectedNextEntryId
	newBuffer := cache.NewSequentialBufferWithData(w.logId, w.segmentId, restDataFirstEntryId, w.maxBufferEntries, restData)

	// Submit flush task
	logger.Ctx(ctx).Debug("Submitting flush task to channel",
		zap.Int64("firstEntryId", flushTask.firstEntryId),
		zap.Int64("lastEntryId", flushTask.lastEntryId),
		zap.Int32("blockNumber", flushTask.blockNumber),
		zap.Int("entriesCount", len(flushTask.entries)))

	select {
	case w.flushTaskChan <- flushTask:
		logger.Ctx(ctx).Debug("Flush task submitted successfully",
			zap.Int64("firstEntryId", flushTask.firstEntryId),
			zap.Int64("lastEntryId", flushTask.lastEntryId),
			zap.Int32("blockNumber", flushTask.blockNumber),
			zap.Int("entriesCount", len(flushTask.entries)))
		// Update stats only after successful submission
		// Update flushing size
		w.lastSubmittedFlushingEntryID.Store(flushTask.lastEntryId)
		w.lastSubmittedFlushingBlockID.Store(int64(flushTask.blockNumber))
		// Increment block number for next block
		w.currentBlockNumber.Add(1)
		w.lastSyncTimestamp.Store(time.Now().UnixMilli())
		// switch buffer
		w.buffer.Store(newBuffer)
	case <-ctx.Done():
		logger.Ctx(ctx).Warn("Context cancelled while submitting flush task", zap.Int32("blockNumber", flushTask.blockNumber))
		// Notify entries of cancellation
		w.notifyFlushError(flushTask.entries, ctx.Err())
	case <-w.runCtx.Done():
		logger.Ctx(ctx).Warn("Writer context cancelled while submitting flush task", zap.Int32("blockNumber", flushTask.blockNumber))
		// Notify entries of cancellation
		w.notifyFlushError(flushTask.entries, werr.ErrFileWriterAlreadyClosed)
	}

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "rollBuffer", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "rollBuffer", "success").Observe(float64(time.Since(startTime).Milliseconds()))
}

// processFlushTask processes a flush task by writing data to disk
func (w *StagedFileWriter) processFlushTask(ctx context.Context, task *blockFlushTask) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "processFlushTask")
	defer sp.End()
	startTime := time.Now()
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	// Add nil check for safety
	if task == nil {
		return
	}

	if w.fenced.Load() {
		logger.Ctx(ctx).Debug("WriteDataAsync: writer fenced", zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId), zap.Int32("blockID", task.blockNumber), zap.Int64("firstEntryId", task.firstEntryId), zap.Int64("lastEntryId", task.lastEntryId))
		w.notifyFlushError(task.entries, werr.ErrSegmentFenced)
		return
	}

	if w.finalized.Load() {
		logger.Ctx(ctx).Debug("WriteDataAsync: writer finalized", zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId), zap.Int32("blockID", task.blockNumber), zap.Int64("firstEntryId", task.firstEntryId), zap.Int64("lastEntryId", task.lastEntryId))
		w.notifyFlushError(task.entries, werr.ErrFileWriterFinalized)
		return
	}

	if !w.storageWritable.Load() {
		logger.Ctx(ctx).Warn("process flush task: storage not writable", zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId), zap.Int32("blockID", task.blockNumber), zap.Int64("firstEntryId", task.firstEntryId), zap.Int64("lastEntryId", task.lastEntryId))
		w.notifyFlushError(task.entries, werr.ErrStorageNotWritable)
		return
	}

	// Write header if not written yet
	if !w.headerWritten.Load() {
		if err := w.writeHeader(ctx); err != nil {
			logger.Ctx(ctx).Warn("write header error", zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId), zap.Int32("blockID", task.blockNumber), zap.Int64("firstEntryId", task.firstEntryId), zap.Int64("lastEntryId", task.lastEntryId), zap.Error(err))
			w.storageWritable.Store(false)
			w.notifyFlushError(task.entries, werr.ErrStorageNotWritable.WithCauseErr(err))
			return
		}
		w.headerWritten.Store(true)
	}

	// Record block start offset before writing block header
	blockStartOffset := w.writtenBytes

	logger.Ctx(ctx).Debug("starting to process flush task",
		zap.Int64("logId", w.logId),
		zap.Int64("segId", w.segmentId),
		zap.Int64("blockStartOffset", blockStartOffset),
		zap.Int64("firstEntryId", task.firstEntryId),
		zap.Int64("lastEntryId", task.lastEntryId),
		zap.Int32("blockNumber", task.blockNumber),
		zap.Int("entriesCount", len(task.entries)))

	// First, serialize all data records to calculate block length and CRC
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

	logger.Ctx(ctx).Debug("calculated block metadata",
		zap.Int64("logId", w.logId),
		zap.Int64("segId", w.segmentId),
		zap.Int32("blockNumber", task.blockNumber),
		zap.Uint32("blockLength", blockLength),
		zap.Uint32("blockCrc", blockCrc),
		zap.Int("dataRecordsCount", len(task.entries)))

	// Write block header record with calculated values
	blockHeaderRecord := &codec.BlockHeaderRecord{
		BlockNumber:  task.blockNumber,
		FirstEntryID: task.firstEntryId,
		LastEntryID:  task.lastEntryId,
		BlockLength:  blockLength,
		BlockCrc:     blockCrc,
	}
	if err := w.writeRecord(ctx, blockHeaderRecord); err != nil {
		logger.Ctx(ctx).Warn("write block header record error", zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId), zap.Int32("blockNumber", task.blockNumber), zap.Error(err))
		w.storageWritable.Store(false)
		w.notifyFlushError(task.entries, werr.ErrStorageNotWritable.WithCauseErr(err))
		return
	}

	logger.Ctx(ctx).Debug("block header written, now writing data records",
		zap.Int64("logId", w.logId),
		zap.Int64("segId", w.segmentId),
		zap.Int32("blockNumber", task.blockNumber),
		zap.Int("blockDataSize", len(blockDataBuffer)))

	// Write the pre-serialized data records
	n, err := w.file.Write(blockDataBuffer)
	if err != nil {
		logger.Ctx(ctx).Warn("write block data error", zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId), zap.Int32("blockNumber", task.blockNumber), zap.Error(err))
		w.storageWritable.Store(false)
		w.notifyFlushError(task.entries, werr.ErrStorageNotWritable.WithCauseErr(err))
		return
	}
	if n != len(blockDataBuffer) {
		logger.Ctx(ctx).Warn("incomplete block data write", zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId), zap.Int32("blockNumber", task.blockNumber), zap.Int("expected", len(blockDataBuffer)), zap.Int("actual", n))
		w.storageWritable.Store(false)
		w.notifyFlushError(task.entries, werr.ErrStorageNotWritable.WithCauseErr(fmt.Errorf("incomplete write: wrote %d of %d bytes", n, len(blockDataBuffer))))
		return
	}
	w.writtenBytes += int64(n)

	logger.Ctx(ctx).Debug("block data written successfully",
		zap.Int64("logId", w.logId),
		zap.Int64("segId", w.segmentId),
		zap.Int32("blockNumber", task.blockNumber),
		zap.Int64("firstEntryId", task.firstEntryId),
		zap.Int64("lastEntryId", task.lastEntryId),
		zap.Int("bytesWritten", n),
		zap.Int64("totalWrittenBytes", w.writtenBytes))

	// Sync to disk
	if err := w.file.Sync(); err != nil {
		logger.Ctx(ctx).Warn("sync file error", zap.Int64("logId", w.logId), zap.Int64("segId", w.segmentId), zap.Error(err))
		w.storageWritable.Store(false)
		w.notifyFlushError(task.entries, werr.ErrStorageNotWritable.WithCauseErr(err))
		return
	}

	logger.Ctx(ctx).Debug("file synced to disk successfully",
		zap.Int64("logId", w.logId),
		zap.Int64("segId", w.segmentId),
		zap.Int32("blockNumber", task.blockNumber),
		zap.Int64("firstEntryId", task.firstEntryId),
		zap.Int64("lastEntryId", task.lastEntryId))

	// Create index record for this block
	actualDataSize := w.writtenBytes - blockStartOffset
	indexRecord := &codec.IndexRecord{
		BlockNumber:  task.blockNumber,
		StartOffset:  blockStartOffset,
		BlockSize:    uint32(actualDataSize), // Size of this block including header and data
		FirstEntryID: task.firstEntryId,
		LastEntryID:  task.lastEntryId,
	}

	// Lock to protect blockIndexes from concurrent access
	w.blockIndexes = append(w.blockIndexes, indexRecord)

	// Update entry tracking
	if w.firstEntryID.Load() == -1 {
		w.firstEntryID.Store(task.firstEntryId)
	}
	w.lastEntryID.Store(task.lastEntryId)

	logger.Ctx(ctx).Debug("block processing completed successfully",
		zap.Int64("logId", w.logId),
		zap.Int64("segId", w.segmentId),
		zap.String("filePath", w.segmentFilePath),
		zap.Int32("blockNumber", task.blockNumber),
		zap.Int64("firstEntryId", task.firstEntryId),
		zap.Int64("lastEntryId", task.lastEntryId),
		zap.Int64("blockStartOffset", blockStartOffset),
		zap.Int("totalBlockIndexes", len(w.blockIndexes)))

	// update metrics
	metrics.WpFileFlushBytesWritten.WithLabelValues(w.logIdStr).Add(float64(actualDataSize))
	metrics.WpFileFlushLatency.WithLabelValues(w.logIdStr).Observe(float64(time.Since(startTime).Milliseconds()))

	// Notify success - notify each entry channel with success
	w.notifyFlushSuccess(task.entries)
}

func (f *StagedFileWriter) awaitAllFlushTasks(ctx context.Context) error {
	logger.Ctx(ctx).Info("awaiting completion of all block flush tasks", zap.String("segmentFilePath", f.segmentFilePath))

	// Now wait for the ack goroutine to process all completed tasks
	// This ensures that blockIndexes are properly populated
	logger.Ctx(ctx).Info("waiting for ack goroutine to process completed tasks", zap.String("segmentFilePath", f.segmentFilePath))

	// Send termination signal to ack goroutine
	select {
	case f.flushTaskChan <- &blockFlushTask{
		entries:      nil,
		firstEntryId: -1,
		lastEntryId:  -1,
		blockNumber:  -1,
	}:
		logger.Ctx(ctx).Info("termination signal sent to ack goroutine", zap.String("segmentFilePath", f.segmentFilePath))
	case <-ctx.Done():
		return ctx.Err()
	}

	// Wait for ack goroutine to set the done flag
	ackWaitTime := 15 * time.Second // TODO configurable
	ackStartTime := time.Now()

	for {
		if f.allUploadingTaskDone.Load() {
			logger.Ctx(ctx).Info("ack goroutine completed processing all tasks", zap.String("segmentFilePath", f.segmentFilePath))
			return nil
		}

		// Check timeout
		if time.Since(ackStartTime) > ackWaitTime {
			logger.Ctx(ctx).Warn("timeout waiting for ack goroutine to complete",
				zap.String("segmentFilePath", f.segmentFilePath),
				zap.Bool("allUploadingTaskDone", f.allUploadingTaskDone.Load()),
				zap.Duration("elapsed", time.Since(ackStartTime)))
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
	logger.Ctx(ctx).Debug("WriteDataAsync called",
		zap.Int64("entryId", entryId),
		zap.Int("dataLen", len(data)))

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
		logger.Ctx(ctx).Warn("WriteDataAsync: attempting to write rejected, data cannot be empty", zap.String("segmentFilePath", w.segmentFilePath), zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("inst", fmt.Sprintf("%p", w)))
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
	logger.Ctx(ctx).Debug("AppendAsync: successfully written to buffer", zap.String("segmentFilePath", w.segmentFilePath), zap.Int64("entryId", entryId), zap.Int64("id", id), zap.Int64("expectedNextEntryId", currentBuffer.ExpectedNextEntryId.Load()), zap.String("inst", fmt.Sprintf("%p", w)), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))

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
func (w *StagedFileWriter) writeHeader(ctx context.Context) error {
	header := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	return w.writeRecord(ctx, header)
}

// writeRecord writes a record to the file
func (w *StagedFileWriter) writeRecord(ctx context.Context, record codec.Record) error {
	// Encode the record
	encoded := codec.EncodeRecord(record)

	logger.Ctx(ctx).Debug("writing record to file",
		zap.Uint8("recordType", record.Type()),
		zap.Int("encodedSize", len(encoded)),
		zap.Int64("currentWrittenBytes", w.writtenBytes),
		zap.String("segmentFilePath", w.segmentFilePath))

	// Write the record
	n, err := w.file.Write(encoded)
	if err != nil {
		return fmt.Errorf("write record: %w", err)
	}

	if n != len(encoded) {
		return fmt.Errorf("incomplete write: wrote %d of %d bytes", n, len(encoded))
	}

	w.writtenBytes += int64(n)

	logger.Ctx(ctx).Debug("record written successfully",
		zap.Uint8("recordType", record.Type()),
		zap.Int("bytesWritten", n),
		zap.Int64("totalWrittenBytes", w.writtenBytes))

	return nil
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
		logger.Ctx(ctx).Warn("wait flush error before close",
			zap.String("segmentFilePath", w.segmentFilePath),
			zap.Error(waitErr))
		return -1, waitErr
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Write header if not written yet
	if !w.headerWritten.Load() {
		if err := w.writeHeader(ctx); err != nil {
			return w.lastEntryID.Load(), err
		}
		w.headerWritten.Store(true)
	}

	// Write all index records
	indexStartOffset := w.writtenBytes

	// Lock to read blockIndexes safely
	blockIndexesCopy := make([]*codec.IndexRecord, len(w.blockIndexes))
	copy(blockIndexesCopy, w.blockIndexes)
	blockIndexesLen := len(w.blockIndexes)

	for _, indexRecord := range blockIndexesCopy {
		if err := w.writeRecord(ctx, indexRecord); err != nil {
			return w.lastEntryID.Load(), fmt.Errorf("write index record: %w", err)
		}
	}
	indexLength := uint32(w.writtenBytes - indexStartOffset)

	// Write footer record
	footer := &codec.FooterRecord{
		TotalBlocks:  int32(blockIndexesLen),
		TotalRecords: uint32(blockIndexesLen),  // Simplified - each block is one record for index
		TotalSize:    uint64(w.writtenBytes),   // Total size of the file
		IndexOffset:  uint64(indexStartOffset), // Will be calculated by the codec
		IndexLength:  indexLength,              // Will be calculated by the codec
		Version:      codec.FormatVersion,
		Flags:        0,
		LAC:          lac, // Last add confirmed ID acknowledged by majority of replicas
	}

	if err := w.writeRecord(ctx, footer); err != nil {
		return w.lastEntryID.Load(), fmt.Errorf("write footer: %w", err)
	}

	// Final sync
	if err := w.file.Sync(); err != nil {
		return w.lastEntryID.Load(), fmt.Errorf("final sync: %w", err)
	}

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "finalize", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "finalize", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	logger.Ctx(ctx).Debug("finalized staged file", zap.Int64("lastEntryId", w.lastEntryID.Load()), zap.String("file", w.segmentFilePath), zap.Int64("writtenBytes", w.writtenBytes))
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
		logger.Ctx(ctx).Info("run: received close signal, but it already closed,skip", zap.String("inst", fmt.Sprintf("%p", w)))
		return nil
	}
	logger.Ctx(ctx).Info("Close: trigger sync before close", zap.Int64("logId", w.logId), zap.Int64("segmentId", w.segmentId))
	err := w.Sync(ctx) // manual sync all pending append operation
	if err != nil {
		logger.Ctx(ctx).Warn("sync error before close",
			zap.String("segmentFilePath", w.segmentFilePath),
			zap.Error(err))
	}

	// wait all flush
	waitErr := w.awaitAllFlushTasks(ctx)
	if waitErr != nil {
		logger.Ctx(ctx).Warn("wait flush error before close",
			zap.String("segmentFilePath", w.segmentFilePath),
			zap.Error(waitErr))
		return waitErr
	}

	// Cancel async operations
	w.runCancel()
	// Close file
	if w.file != nil {
		w.file.Close()
		w.file = nil
	}

	// Close channels
	close(w.flushTaskChan)

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
		logger.Ctx(ctx).Debug("StagedFileWriter already fenced, returning idempotently",
			zap.String("segmentFile", w.segmentFilePath))
		return w.GetLastEntryId(ctx), nil
	}

	// Sync all pending data first
	if err := w.Sync(ctx); err != nil {
		return w.GetLastEntryId(ctx), err
	}

	// Mark as fenced and stop accepting writes
	w.fenced.Store(true)

	lastEntryId := w.GetLastEntryId(ctx)
	logger.Ctx(ctx).Info("Successfully created fence flag file and marked StagedFileWriter as fenced",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int64("lastEntryId", lastEntryId))

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
		zap.String("segmentFilePath", w.segmentFilePath),
		zap.Int("currentBlockCount", len(w.blockIndexes)))

	// Ensure segment is finalized before compaction
	if !w.finalized.Load() {
		logger.Ctx(ctx).Warn("segment must be finalized before compaction",
			zap.String("segmentFilePath", w.segmentFilePath))
		return -1, fmt.Errorf("segment must be finalized before compaction")
	}

	// Check if we have any blocks to compact
	if len(w.blockIndexes) == 0 {
		logger.Ctx(ctx).Info("no blocks to compact",
			zap.String("segmentFilePath", w.segmentFilePath))
		return -1, nil
	}

	// Read and validate footer LAC against segment data for completeness
	lac, err := w.validateLACAlignment(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("LAC validation failed",
			zap.String("segmentFilePath", w.segmentFilePath),
			zap.Error(err))
		return -1, err
	}

	logger.Ctx(ctx).Info("LAC validation passed for compaction",
		zap.String("segmentFilePath", w.segmentFilePath),
		zap.Int64("lac", lac),
		zap.Int64("firstEntryID", w.firstEntryID.Load()),
		zap.Int64("lastEntryID", w.lastEntryID.Load()))

	// Get target block size for compaction
	maxCompactedBlockSize := w.compactPolicyConfig.MaxBytes.Int64()
	if maxCompactedBlockSize <= 0 {
		maxCompactedBlockSize = 2 * 1024 * 1024 // Default 2MB
	}

	// Read and merge blocks from local file, then upload to minio
	newBlockIndexes, fileSizeAfterCompact, err := w.readLocalFileAndUploadToMinio(ctx, maxCompactedBlockSize)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to read local file and upload to minio",
			zap.String("segmentFilePath", w.segmentFilePath),
			zap.Error(err))
		return -1, fmt.Errorf("failed to read local file and upload to minio: %w", err)
	}

	if len(newBlockIndexes) == 0 {
		logger.Ctx(ctx).Info("no blocks uploaded during compaction",
			zap.String("segmentFilePath", w.segmentFilePath))
		return -1, nil
	}

	// Create footer with compacted flag and LAC, then upload
	footerSize, err := w.uploadCompactedFooter(ctx, newBlockIndexes, fileSizeAfterCompact, lac)
	if err != nil {
		logger.Ctx(ctx).Warn("failed to upload compacted footer",
			zap.String("segmentFilePath", w.segmentFilePath),
			zap.Error(err))
		return -1, fmt.Errorf("failed to upload compacted footer: %w", err)
	}

	totalSize := fileSizeAfterCompact + footerSize

	// Update internal state
	originalBlockCount := len(w.blockIndexes)
	w.blockIndexes = newBlockIndexes

	logger.Ctx(ctx).Info("successfully compacted staged file segment",
		zap.String("segmentFilePath", w.segmentFilePath),
		zap.Int("originalBlockCount", originalBlockCount),
		zap.Int("compactedBlockCount", len(newBlockIndexes)),
		zap.Int64("totalSizeAfterCompact", totalSize),
		zap.Int64("maxCompactedBlockSize", maxCompactedBlockSize),
		zap.Int64("costMs", time.Since(startTime).Milliseconds()))

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
		zap.String("segmentFilePath", w.segmentFilePath),
		zap.Int("originalBlocks", len(w.blockIndexes)),
		zap.Int("mergeTasks", len(mergeTasks)))

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
		zap.String("segmentFilePath", w.segmentFilePath),
		zap.Int64("mergedBlockID", mergedBlockID),
		zap.Int("originalBlocks", len(task.blocks)))

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
		zap.String("segmentFilePath", w.segmentFilePath),
		zap.String("blockKey", blockKey),
		zap.Int64("blockSize", int64(len(mergedData))),
		zap.Int64("firstEntryID", firstEntryID),
		zap.Int64("lastEntryID", lastEntryID))

	return &mergedBlockUploadResult{
		blockIndex: newBlockIndex,
		blockSize:  int64(len(mergedData)),
		error:      nil,
	}

}

// readBlockDataFromLocalFile reads data for a specific block from the local file
func (w *StagedFileWriter) readBlockDataFromLocalFile(ctx context.Context, blockIndex *codec.IndexRecord) *blockReadResult {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "readBlockDataFromLocalFile")
	defer sp.End()

	// Open local file for reading
	file, err := os.Open(w.segmentFilePath)
	if err != nil {
		return &blockReadResult{
			blockIndex: blockIndex,
			error:      fmt.Errorf("failed to open local file: %w", err),
		}
	}
	defer file.Close()

	// Read block data from file
	blockData := make([]byte, blockIndex.BlockSize)
	_, err = file.ReadAt(blockData, blockIndex.StartOffset)
	if err != nil {
		return &blockReadResult{
			blockIndex: blockIndex,
			error:      fmt.Errorf("failed to read block data: %w", err),
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
		zap.String("segmentFilePath", w.segmentFilePath),
		zap.String("footerKey", footerKey),
		zap.Int64("footerSize", int64(len(footerData))))

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

// recoverFromExistingFile attempts to recover state from an existing incomplete file
func (w *StagedFileWriter) recoverFromExistingFileUnsafe(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "recoverFromExistingFile")
	defer sp.End()
	// Check if file exists
	stat, err := os.Stat(w.segmentFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, start fresh
			return nil
		}
		return fmt.Errorf("stat file: %w", err)
	}

	if stat.Size() == 0 {
		// Empty file, start fresh
		return nil
	}

	// Open file for reading to analyze its content
	file, err := os.Open(w.segmentFilePath)
	if err != nil {
		return fmt.Errorf("open file for reading: %w", err)
	}
	defer file.Close()

	w.writtenBytes = stat.Size()
	w.lastModifiedTime = stat.ModTime()

	// Try to parse the file to determine its state
	// First, check if it has a complete footer (finalized file)
	maxFooterSize := codec.GetMaxFooterReadSize()
	if w.writtenBytes >= int64(maxFooterSize) {
		// Try to read footer from the end using compatibility parsing
		footerData := make([]byte, maxFooterSize)
		_, err := file.ReadAt(footerData, w.writtenBytes-int64(len(footerData)))
		if err == nil {
			// Try to parse footer with version compatibility
			footerRecord, err := codec.ParseFooterFromBytes(footerData)
			if err == nil {
				// File is already finalized, can't recover for writing
				w.finalized.Store(true)
				return w.recoverBlocksFromFooterUnsafe(context.TODO(), file, footerRecord)
			}
		}
	}

	// File is incomplete, try to recover blocks from whole file scan
	if err := w.recoverBlocksFromFullScanUnsafe(ctx, file); err != nil {
		return fmt.Errorf("recover blocks: %w", err)
	}
	// Mark as recovered
	w.recovered.Store(true)
	return nil
}

func (w *StagedFileWriter) recoverBlocksFromFooterUnsafe(ctx context.Context, file *os.File, footerRecord *codec.FooterRecord) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "recoverBlocksFromFooter")
	defer sp.End()
	startTime := time.Now()
	logger.Ctx(ctx).Info("Recovering blocks from footer",
		zap.String("segmentFilePath", w.segmentFilePath),
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Uint64("indexOffset", footerRecord.IndexOffset),
		zap.Uint32("indexLength", footerRecord.IndexLength),
		zap.Int32("totalBlocks", footerRecord.TotalBlocks))

	if footerRecord.IndexLength == 0 {
		// empty index length, no data blocks at all, fast return
		w.blockIndexes = make([]*codec.IndexRecord, 0, footerRecord.TotalBlocks)
		w.recoveredFooter = footerRecord
		w.recovered.Store(true)
		logger.Ctx(ctx).Info("Recovered no blocks from footer")
		return nil
	}

	// Read index section from file
	indexData := make([]byte, footerRecord.IndexLength)
	_, err := file.ReadAt(indexData, int64(footerRecord.IndexOffset))
	if err != nil {
		return fmt.Errorf("failed to read index section: %w", err)
	}

	logger.Ctx(ctx).Debug("Read index section",
		zap.Int("indexDataLength", len(indexData)),
		zap.Uint64("indexOffset", footerRecord.IndexOffset))

	// Parse index records sequentially
	offset := 0
	blockIndexes := make([]*codec.IndexRecord, 0, footerRecord.TotalBlocks)

	for offset < len(indexData) {
		if offset+codec.RecordHeaderSize > len(indexData) {
			logger.Ctx(ctx).Warn("Not enough data for complete record header",
				zap.Int("offset", offset),
				zap.Int("remaining", len(indexData)-offset))
			break
		}

		// Decode the record
		record, err := codec.DecodeRecord(indexData[offset:])
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to decode index record",
				zap.Int("offset", offset),
				zap.Error(err))
			return fmt.Errorf("failed to decode index record at offset %d: %w", offset, err)
		}

		// Verify it's an index record
		if record.Type() != codec.IndexRecordType {
			logger.Ctx(ctx).Warn("Unexpected record type in index section",
				zap.Int("offset", offset),
				zap.Uint8("expectedType", codec.IndexRecordType),
				zap.Uint8("actualType", record.Type()))
			return fmt.Errorf("expected index record type %d, got %d at offset %d",
				codec.IndexRecordType, record.Type(), offset)
		}

		indexRecord := record.(*codec.IndexRecord)
		blockIndexes = append(blockIndexes, indexRecord)

		logger.Ctx(ctx).Debug("Parsed index record",
			zap.Int32("blockNumber", indexRecord.BlockNumber),
			zap.Int64("startOffset", indexRecord.StartOffset),
			zap.Int64("firstEntryID", indexRecord.FirstEntryID),
			zap.Int64("lastEntryID", indexRecord.LastEntryID))

		// Move to next record (header + IndexRecord payload size)
		recordSize := codec.RecordHeaderSize + codec.IndexRecordSize // IndexRecord payload size
		offset += recordSize
	}

	// Validate recovered blocks count
	if len(blockIndexes) != int(footerRecord.TotalBlocks) {
		logger.Ctx(ctx).Warn("Block count mismatch",
			zap.Int("recoveredBlocks", len(blockIndexes)),
			zap.Int32("expectedBlocks", footerRecord.TotalBlocks))
		// Continue anyway, use what we recovered
	}

	// Update writer state
	w.blockIndexes = blockIndexes
	w.recoveredFooter = footerRecord

	// Update entry ID tracking
	if len(blockIndexes) > 0 {
		firstBlock := blockIndexes[0]
		lastBlock := blockIndexes[len(blockIndexes)-1]

		w.firstEntryID.Store(firstBlock.FirstEntryID)
		w.lastEntryID.Store(lastBlock.LastEntryID)
		w.currentBlockNumber.Store(int64(len(blockIndexes))) // Next block number

		logger.Ctx(ctx).Info("Updated entry ID tracking from recovered blocks",
			zap.Int64("firstEntryID", firstBlock.FirstEntryID),
			zap.Int64("lastEntryID", lastBlock.LastEntryID),
			zap.Int64("nextBlockNumber", int64(len(blockIndexes))))
	}

	logger.Ctx(ctx).Info("Successfully recovered blocks from footer",
		zap.String("segmentFilePath", w.segmentFilePath),
		zap.Int("recoveredBlocks", len(blockIndexes)),
		zap.Int64("firstEntryID", w.firstEntryID.Load()),
		zap.Int64("lastEntryID", w.lastEntryID.Load()))

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "recover_footer", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "recover_footer", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	w.recovered.Store(true)
	return nil
}

// recoverBlocks recovers block information from an incomplete file
func (w *StagedFileWriter) recoverBlocksFromFullScanUnsafe(ctx context.Context, file *os.File) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "recoverBlocksFromFullScan")
	defer sp.End()
	startTime := time.Now()
	// Read the entire file content
	data := make([]byte, w.writtenBytes)
	_, err := file.ReadAt(data, 0)
	if err != nil {
		return fmt.Errorf("read file content: %w", err)
	}

	// Parse records sequentially
	offset := 0
	currentBlockStart := int64(0)
	currentBlockNumber := int64(0)
	var headerFound bool
	currentEntryId := int64(0)
	var currentBlockFirstEntryID int64 = -1
	var currentBlockLastEntryID int64 = -1
	var inBlock bool = false

	for offset < len(data) {
		// Check if we have enough data for a record header
		if offset+codec.RecordHeaderSize > len(data) {
			break
		}

		// Try to decode the record
		record, err := codec.DecodeRecord(data[offset:])
		if err != nil {
			// If we can't decode a record, the file might be truncated
			break
		}

		recordSize := codec.RecordHeaderSize
		switch record.Type() {
		case codec.HeaderRecordType:
			headerFound = true
			w.headerWritten.Store(true)
			recordSize += 16 // HeaderRecord size: Version(2) + Flags(2) + FirstEntryID(8) + Magic(4)

			// Set first entry ID from header
			headerRecord := record.(*codec.HeaderRecord)
			currentEntryId = headerRecord.FirstEntryID
			if w.firstEntryID.Load() == -1 {
				w.firstEntryID.Store(currentEntryId)
			}

		case codec.DataRecordType:
			dataRecord := record.(*codec.DataRecord)
			recordSize += len(dataRecord.Payload)

			// If we're not in a block yet, this shouldn't happen in normal files
			// but we can handle it for legacy compatibility
			if !inBlock {
				// Start a new block with this DataRecord
				currentBlockStart = int64(offset)
				currentBlockFirstEntryID = currentEntryId
				inBlock = true
			}

			// Update last entry ID for current block
			currentBlockLastEntryID = currentEntryId
			// Update global last entry ID
			w.lastEntryID.Store(currentEntryId)
			currentEntryId++

		case codec.BlockHeaderRecordType:
			blockHeaderRecord := record.(*codec.BlockHeaderRecord)
			recordSize += codec.BlockHeaderRecordSize

			// If we were in a previous block, finalize it first
			if inBlock {
				// Create index record for the previous block
				indexRecord := &codec.IndexRecord{
					BlockNumber:  int32(currentBlockNumber),
					StartOffset:  currentBlockStart,
					BlockSize:    uint32(int64(offset) - currentBlockStart), // Calculate block size
					FirstEntryID: currentBlockFirstEntryID,
					LastEntryID:  currentBlockLastEntryID,
				}
				w.blockIndexes = append(w.blockIndexes, indexRecord)
				currentBlockNumber++
			}

			// Start new block with this BlockHeaderRecord
			currentBlockStart = int64(offset)
			if currentBlockNumber != int64(blockHeaderRecord.BlockNumber) {
				logger.Ctx(ctx).Warn("Block number mismatch",
					zap.Int64("logId", w.logId),
					zap.Int64("segId", w.segmentId),
					zap.Int32("expectedBlockNumber", blockHeaderRecord.BlockNumber),
					zap.Int64("actualBlockNumber", currentBlockNumber))
				currentBlockNumber = int64(blockHeaderRecord.BlockNumber)
			}
			currentBlockFirstEntryID = blockHeaderRecord.FirstEntryID
			currentBlockLastEntryID = blockHeaderRecord.LastEntryID
			inBlock = true

		default:
			// Unknown record type, might be corrupted
			goto exitLoop
		}

		offset += recordSize
	}
exitLoop:

	// Handle the last block if we were still in one
	if inBlock {
		indexRecord := &codec.IndexRecord{
			BlockNumber:  int32(currentBlockNumber),
			StartOffset:  currentBlockStart,
			BlockSize:    uint32(int64(offset) - currentBlockStart), // Calculate block size
			FirstEntryID: currentBlockFirstEntryID,
			LastEntryID:  currentBlockLastEntryID,
		}
		w.blockIndexes = append(w.blockIndexes, indexRecord)
		currentBlockNumber++
	}

	w.currentBlockNumber.Store(currentBlockNumber)

	// If no header was found, we'll need to write one
	if !headerFound {
		w.headerWritten.Store(false)
		w.writtenBytes = 0
		// Lock to protect blockIndexes during reset
		w.blockIndexes = w.blockIndexes[:0]
		w.firstEntryID.Store(-1)
		w.lastEntryID.Store(-1)
		w.currentBlockNumber.Store(0)
	}

	// update metrics
	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "recover_raw", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "recover_raw", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return nil
}

func getSegmentDir(baseDir string, logId int64, segmentId int64) string {
	return filepath.Join(baseDir, fmt.Sprintf("%d/%d", logId, segmentId))
}

func getSegmentFilePath(baseDir string, logId int64, segmentId int64) string {
	return filepath.Join(baseDir, fmt.Sprintf("%d/%d/data.log", logId, segmentId))
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
		zap.Int64("lac", lac),
		zap.Int64("firstEntryID", firstEntryID),
		zap.Int64("lastEntryID", lastEntryID),
		zap.Bool("hasLAC", footer.HasLAC()))

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
		zap.Int64("lac", lac),
		zap.Int64("firstEntryID", firstEntryID),
		zap.Int64("lastEntryID", lastEntryID),
		zap.Int64("maxBlockLastEntryID", maxBlockLastEntryID))

	return lac, nil
}

// determineIfNeedRecoveryMode determines if the writer should enter recovery mode
// by checking if the segment file already exists with data.
// This is critical for node restart scenarios to prevent data loss.
func (w *StagedFileWriter) determineIfNeedRecoveryMode(forceRecoveryMode bool) bool {
	if forceRecoveryMode {
		return true
	}

	// Check if segment file exists and has data
	stat, err := os.Stat(w.segmentFilePath)
	if err != nil {
		// File doesn't exist or other error - no recovery needed
		if !os.IsNotExist(err) {
			// Log unexpected errors (but don't fail the writer creation)
			logger.Ctx(context.Background()).Debug("Failed to stat segment file, assuming no recovery needed",
				zap.String("filePath", w.segmentFilePath),
				zap.Error(err))
		}
		return false
	}

	// File exists - check if it has data
	if stat.Size() > 0 {
		logger.Ctx(context.Background()).Info("Auto-detected existing segment file with data, entering recovery mode",
			zap.String("filePath", w.segmentFilePath),
			zap.Int64("fileSize", stat.Size()),
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId))
		return true
	}

	// File exists but is empty - no recovery needed
	return false
}

// GetRecoveredFooter Test Only
func (w *StagedFileWriter) GetRecoveredFooter() *codec.FooterRecord {
	return w.recoveredFooter
}

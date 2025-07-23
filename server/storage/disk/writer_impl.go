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
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gofrs/flock"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

var (
	WriterScope = "LocalFileWriter"
)

var _ storage.Writer = (*LocalFileWriter)(nil)

// blockFlushTask represents a task to flush a block to disk
type blockFlushTask struct {
	entries      []*cache.BufferEntry
	firstEntryId int64
	lastEntryId  int64
	blockNumber  int32
}

// LocalFileWriter implements AbstractFileWriter for local filesystem storage with async buffer management
type LocalFileWriter struct {
	mu              sync.Mutex
	baseDir         string
	segmentFilePath string
	logId           int64
	segmentId       int64
	file            *os.File
	logIdStr        string // for metrics only

	// Configuration
	maxFlushSize     int64 // Max buffer size before triggering sync
	maxBufferEntries int64 // Maximum number of entries per buffer
	maxIntervalMs    int   // Max interval to sync buffer to disk

	// write buffer
	buffer            atomic.Pointer[cache.SequentialBuffer] // Write buffer
	lastSyncTimestamp atomic.Int64

	// file state
	currentBlockNumber atomic.Int64
	blockIndexes       []*codec.IndexRecord
	writtenBytes       int64
	lastModifiedTime   time.Time

	// written state
	firstEntryID  atomic.Int64 // The first entryId written to disk
	lastEntryID   atomic.Int64 // The last entryId written to disk
	headerWritten atomic.Bool  // Ensures header is written before data
	finalized     atomic.Bool
	fenced        atomic.Bool
	recovered     atomic.Bool
	lockFile      *flock.Flock // Lock file handle
	lockFilePath  string       // Path to the lock file

	// Async flush management
	flushTaskChan                chan *blockFlushTask
	storageWritable              atomic.Bool  // Indicates whether the segment is writable
	flushingSize                 atomic.Int64 // The size of data being flushed
	lastSubmittedFlushingEntryID atomic.Int64
	allUploadingTaskDone         atomic.Bool

	// Close management
	fileClose chan struct{} // Close signal
	closed    atomic.Bool
	runCtx    context.Context
	runCancel context.CancelFunc
}

// NewLocalFileWriter creates a new local filesystem writer
func NewLocalFileWriter(ctx context.Context, baseDir string, logId int64, segmentId int64, cfg *config.Configuration) (*LocalFileWriter, error) {
	return NewLocalFileWriterWithMode(ctx, baseDir, logId, segmentId, cfg, false)
}

// NewLocalFileWriterWithMode creates a new local filesystem writer with recovery mode option
func NewLocalFileWriterWithMode(ctx context.Context, baseDir string, logId int64, segmentId int64, cfg *config.Configuration, recoveryMode bool) (*LocalFileWriter, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "NewLocalFileWriterWithMode")
	defer sp.End()
	blockSize := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize
	maxBufferEntries := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxEntries
	maxInterval := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage
	logger.Ctx(ctx).Debug("creating new local file writer",
		zap.String("baseDir", baseDir),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.Int64("maxBlockSize", blockSize),
		zap.Int("maxBufferEntries", maxBufferEntries),
		zap.Int("maxInterval", maxInterval),
		zap.Bool("recoveryMode", recoveryMode))

	segmentDir := getSegmentDir(baseDir, logId, segmentId)
	// Ensure directory exists
	if err := os.MkdirAll(segmentDir, 0755); err != nil {
		logger.Ctx(ctx).Warn("failed to create directory",
			zap.String("segmentDir", segmentDir),
			zap.Error(err))
		return nil, fmt.Errorf("create directory: %w", err)
	}

	filePath := getSegmentFilePath(baseDir, logId, segmentId)
	logger.Ctx(ctx).Debug("segment directory and file path prepared",
		zap.String("segmentDir", segmentDir),
		zap.String("filePath", filePath))

	// Create context for async operations
	runCtx, runCancel := context.WithCancel(context.Background())

	writer := &LocalFileWriter{
		baseDir:          baseDir,
		segmentFilePath:  filePath,
		logId:            logId,
		segmentId:        segmentId,
		logIdStr:         fmt.Sprintf("%d", logId),
		blockIndexes:     make([]*codec.IndexRecord, 0),
		writtenBytes:     0,
		maxFlushSize:     blockSize,
		maxBufferEntries: int64(maxBufferEntries),         // Default max entries per buffer
		maxIntervalMs:    maxInterval,                     // 10ms default sync interval for more responsive syncing
		flushTaskChan:    make(chan *blockFlushTask, 100), // Increased buffer size to reduce blocking
		runCtx:           runCtx,
		runCancel:        runCancel,
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
	writer.flushingSize.Store(0)
	writer.lastSubmittedFlushingEntryID.Store(-1)
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

	if recoveryMode {
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
		logger.Ctx(ctx).Debug("normal mode, creating segment lock and opening file for writing")
		// Create segment lock file
		if err := writer.createSegmentLock(ctx); err != nil {
			logger.Ctx(ctx).Warn("failed to create segment lock",
				zap.String("filePath", filePath),
				zap.Error(err))
			return nil, errors.Wrap(err, "failed to create segment lock")
		}
		logger.Ctx(ctx).Debug("segment lock created successfully")

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

	logger.Ctx(ctx).Info("local file writer created successfully",
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
func (w *LocalFileWriter) run() {
	ticker := time.NewTicker(time.Duration(w.maxIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	logger.Ctx(w.runCtx).Debug("LocalFileWriter run goroutine started",
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId))

	for {
		select {
		case <-w.runCtx.Done():
			// Context cancelled, exit
			logger.Ctx(w.runCtx).Debug("LocalFileWriter run goroutine stopping due to context cancellation")
			return
		case flushTask, ok := <-w.flushTaskChan:
			// Process flush tasks with higher priority
			if !ok {
				// Channel closed, exit
				logger.Ctx(w.runCtx).Debug("LocalFileWriter run goroutine stopping due to channel close")
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
				zap.Int64("firstEntryId", flushTask.firstEntryId),
				zap.Int64("lastEntryId", flushTask.lastEntryId),
				zap.Int32("blockNumber", flushTask.blockNumber),
				zap.Int("entriesCount", len(flushTask.entries)))
			w.processFlushTask(w.runCtx, flushTask)
			logger.Ctx(w.runCtx).Debug("Flush task processing completed")
		case <-ticker.C:
			// Periodic check fenced flag make by other process
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

	logger.Ctx(ctx).Debug("Sync evaluation",
		zap.Bool("needSync", needSync),
		zap.Bool("hasEntries", hasEntries),
		zap.Int64("bufferSize", bufferSize),
		zap.Int64("entryCount", entryCount))

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

	// Create flush task
	blockNumber := w.currentBlockNumber.Load()

	flushTask := &blockFlushTask{
		entries:      entries,
		firstEntryId: entries[0].EntryId,
		lastEntryId:  entries[len(entries)-1].EntryId,
		blockNumber:  int32(blockNumber),
	}

	// Update flushing size
	bufferSize := currentBuffer.DataSize.Load()
	w.flushingSize.Add(bufferSize)
	w.lastSubmittedFlushingEntryID.Store(flushTask.lastEntryId)

	// Increment block number for next block
	w.currentBlockNumber.Add(1)
	w.lastSyncTimestamp.Store(time.Now().UnixMilli())

	// Submit flush task asynchronously to avoid deadlock
	go func() {
		logger.Ctx(ctx).Debug("Submitting flush task to channel",
			zap.Int64("firstEntryId", flushTask.firstEntryId),
			zap.Int64("lastEntryId", flushTask.lastEntryId),
			zap.Int32("blockNumber", flushTask.blockNumber),
			zap.Int("entriesCount", len(flushTask.entries)))

		select {
		case w.flushTaskChan <- flushTask:
			logger.Ctx(ctx).Debug("Flush task submitted successfully")
		case <-ctx.Done():
			logger.Ctx(ctx).Warn("Context cancelled while submitting flush task")
			// Notify entries of cancellation
			w.notifyFlushError(flushTask.entries, ctx.Err())
		case <-w.runCtx.Done():
			logger.Ctx(ctx).Warn("Writer context cancelled while submitting flush task")
			// Notify entries of cancellation
			w.notifyFlushError(flushTask.entries, werr.ErrFileWriterAlreadyClosed)
		}
	}()

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "rollBuffer", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "rollBuffer", "success").Observe(float64(time.Since(startTime).Milliseconds()))
}

// processFlushTask processes a flush task by writing data to disk
func (w *LocalFileWriter) processFlushTask(ctx context.Context, task *blockFlushTask) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "processFlushTask")
	defer sp.End()
	startTime := time.Now()
	// Add nil check for safety
	if task == nil {
		return
	}

	defer func() {
		// Calculate flushed size and update counter
		flushedSize := int64(0)
		for _, entry := range task.entries {
			flushedSize += int64(len(entry.Data))
		}
		w.flushingSize.Add(-flushedSize)
	}()

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
	w.mu.Lock()
	w.blockIndexes = append(w.blockIndexes, indexRecord)
	w.mu.Unlock()

	// Update entry tracking
	if w.firstEntryID.Load() == -1 {
		w.firstEntryID.Store(task.firstEntryId)
	}
	w.lastEntryID.Store(task.lastEntryId)

	logger.Ctx(ctx).Debug("block processing completed successfully",
		zap.Int64("logId", w.logId),
		zap.Int64("segId", w.segmentId),
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

func (f *LocalFileWriter) awaitAllFlushTasks(ctx context.Context) error {
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
func (w *LocalFileWriter) notifyFlushError(entries []*cache.BufferEntry, err error) {
	// Notify all pending entries result channels in sequential order
	for _, entry := range entries {
		if entry.NotifyChan != nil {
			cache.NotifyPendingEntryDirectly(context.TODO(), w.logId, w.segmentId, entry.EntryId, entry.NotifyChan, entry.EntryId, err)
		}
	}
}

// notifyFlushError notifies all result channels of flush error
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
		return entryId, werr.ErrFileWriterAlreadyClosed
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
func (w *LocalFileWriter) writeHeader(ctx context.Context) error {
	header := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	return w.writeRecord(ctx, header)
}

// writeRecord writes a record to the file
func (w *LocalFileWriter) writeRecord(ctx context.Context, record codec.Record) error {
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
func (w *LocalFileWriter) Finalize(ctx context.Context) (int64, error) {
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
	logger.Ctx(ctx).Debug("finalized log file", zap.Int64("lastEntryId", w.lastEntryID.Load()), zap.String("file", w.segmentFilePath), zap.Int64("writtenBytes", w.writtenBytes))
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
	err := w.Sync(context.Background()) // manual sync all pending append operation
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

	// Release segment lock
	if err := w.releaseSegmentLock(ctx); err != nil {
		logger.Ctx(ctx).Warn("Failed to release segment lock during close",
			zap.String("segmentFilePath", w.segmentFilePath),
			zap.Error(err))
	}
	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "close", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "close", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return nil
}

// Fence marks the writer as fenced
func (w *LocalFileWriter) Fence(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, WriterScope, "Fence")
	defer sp.End()
	startTime := time.Now()

	// If already fenced, return idempotently
	if w.fenced.Load() {
		logger.Ctx(ctx).Debug("LocalFileWriter already fenced, returning idempotently",
			zap.String("segmentFile", w.segmentFilePath))
		return w.GetLastEntryId(ctx), nil
	}

	// Sync all pending data first
	if err := w.Sync(ctx); err != nil {
		return w.GetLastEntryId(ctx), err
	}

	// Get fence flag file path
	fenceFlagPath := getFenceFlagPath(w.baseDir, w.logId, w.segmentId)

	// Create fence flag file
	fenceFile, err := os.Create(fenceFlagPath)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to create fence flag file",
			zap.String("fenceFlagPath", fenceFlagPath),
			zap.Int64("logId", w.logId),
			zap.Int64("segmentId", w.segmentId),
			zap.Error(err))
		return w.GetLastEntryId(ctx), fmt.Errorf("failed to create fence flag file %s: %w", fenceFlagPath, err)
	}

	// Write fence information to the file
	fenceInfo := fmt.Sprintf("logId=%d\nsegmentId=%d\npid=%d\ntimestamp=%d\nreason=manual_fence\ntype=async_writer\n",
		w.logId, w.segmentId, os.Getpid(), time.Now().Unix())

	_, writeErr := fenceFile.WriteString(fenceInfo)
	fenceFile.Close()

	if writeErr != nil {
		logger.Ctx(ctx).Warn("Failed to write fence info to file, but file created successfully",
			zap.String("fenceFlagPath", fenceFlagPath),
			zap.Error(writeErr))
	}

	// Mark as fenced and stop accepting writes
	w.fenced.Store(true)

	// wait if necessary
	_ = waitForFenceCheckIntervalIfLockExists(ctx, w.baseDir, w.logId, w.segmentId, fenceFlagPath)

	lastEntryId := w.GetLastEntryId(ctx)
	logger.Ctx(ctx).Info("Successfully created fence flag file and marked LocalFileWriter as fenced",
		zap.String("fenceFlagPath", fenceFlagPath),
		zap.Int64("logId", w.logId),
		zap.Int64("segmentId", w.segmentId),
		zap.Int64("lastEntryId", lastEntryId))

	metrics.WpFileOperationsTotal.WithLabelValues(w.logIdStr, "fence", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(w.logIdStr, "fence", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	return lastEntryId, nil
}

// Compact performs compaction (placeholder for future implementation)
func (w *LocalFileWriter) Compact(ctx context.Context) (int64, error) {
	// Purpose: merge small blocks into larger blocks for more efficient indexing
	return -1, werr.ErrSegmentNotFound.WithCauseErrMsg("not need to compact local file currently")
}

// recoverFromExistingFile attempts to recover state from an existing incomplete file
func (w *LocalFileWriter) recoverFromExistingFileUnsafe(ctx context.Context) error {
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

	// recover fence state
	isFenceFlagExists, _ := checkFenceFlagFileExists(w.runCtx, w.baseDir, w.logId, w.segmentId)
	w.fenced.Store(isFenceFlagExists)

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
	if w.writtenBytes >= int64(codec.RecordHeaderSize+codec.FooterRecordSize) {
		// Try to read footer from the end
		footerData := make([]byte, codec.RecordHeaderSize+codec.FooterRecordSize)
		_, err := file.ReadAt(footerData, w.writtenBytes-int64(len(footerData)))
		if err == nil {
			// Try to decode footer
			footerRecord, err := codec.DecodeRecord(footerData)
			if err == nil && footerRecord.Type() == codec.FooterRecordType {
				// File is already finalized, can't recover for writing
				w.finalized.Store(true)
				return w.recoverBlocksFromFooterUnsafe(context.TODO(), file, footerRecord.(*codec.FooterRecord))
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

func (w *LocalFileWriter) recoverBlocksFromFooterUnsafe(ctx context.Context, file *os.File, footerRecord *codec.FooterRecord) error {
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

	// Validate footer record
	if footerRecord.IndexOffset == 0 || footerRecord.IndexLength == 0 {
		return fmt.Errorf("invalid footer record: IndexOffset=%d, IndexLength=%d",
			footerRecord.IndexOffset, footerRecord.IndexLength)
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
func (w *LocalFileWriter) recoverBlocksFromFullScanUnsafe(ctx context.Context, file *os.File) error {
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
			currentBlockFirstEntryID = blockHeaderRecord.FirstEntryID
			currentBlockLastEntryID = blockHeaderRecord.LastEntryID
			inBlock = true

		default:
			// Unknown record type, might be corrupted
			break
		}

		offset += recordSize
	}

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

// createSegmentLock creates a lock file for the segment to ensure exclusive access
func (s *LocalFileWriter) createSegmentLock(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "createSegmentLock")
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
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "releaseSegmentLock")
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

// Utility functions
func getSegmentLockPath(baseDir string, logId int64, segmentId int64) string {
	return filepath.Join(baseDir, fmt.Sprintf("%d/%d/write.lock", logId, segmentId))
}

func getSegmentDir(baseDir string, logId int64, segmentId int64) string {
	return filepath.Join(baseDir, fmt.Sprintf("%d/%d", logId, segmentId))
}

func getSegmentFilePath(baseDir string, logId int64, segmentId int64) string {
	return filepath.Join(baseDir, fmt.Sprintf("%d/%d/data.log", logId, segmentId))
}

// getFenceFlagPath returns the path to the fence flag file for a segment
func getFenceFlagPath(baseDir string, logId int64, segmentId int64) string {
	return filepath.Join(baseDir, fmt.Sprintf("%d/%d/write.fence", logId, segmentId))
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

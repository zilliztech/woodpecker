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
	"encoding/binary"
	"fmt"
	"github.com/zilliztech/woodpecker/server/storage/disk/legacy"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zilliztech/woodpecker/common/channel"

	"github.com/cockroachdb/errors"
	"github.com/gofrs/flock"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
)

const (
	SegmentScopeName = "DiskSegment"
)

var _ storage.Segment = (*DiskSegmentImpl)(nil)

// DiskSegmentImpl is used to write data to disk as a logical segment
type DiskSegmentImpl struct {
	mu           sync.RWMutex
	logId        int64
	segmentId    int64
	logFileDir   string // Log file directory
	currFragment storage.AppendableFragment

	// Configuration parameters
	fragmentSize    int64 // Maximum size of each fragment
	maxEntryPerFile int   // Maximum number of entries per fragment

	// State
	lastFragmentID atomic.Int64
	lastEntryID    atomic.Int64
	firstEntryID   atomic.Int64

	// Use SequentialBuffer to store entries within the window
	buffer           atomic.Pointer[cache.SequentialBuffer]
	maxBufferSize    int64        // Maximum buffer size (bytes)
	maxBufferEntries int64        // Maximum number of entries per buffer
	maxIntervalMs    int          // Maximum interval between syncs
	lastSync         atomic.Int64 // Last sync time

	autoSync bool // Whether to automatically sync data

	// File lock for segment exclusivity
	lockFile     *flock.Flock // Lock file handle
	lockFilePath string       // Path to the lock file
	// For fence state: true confirms it is fenced, while false requires verification by checking the file system for a fence flag file.
	fenced atomic.Bool

	// For async writes and control
	closed    atomic.Bool
	closeCh   chan struct{}
	closeOnce sync.Once
}

// NewDiskSegmentImpl creates a new DiskSegmentImpl instance
func NewDiskSegmentImpl(ctx context.Context, logId int64, segId int64, parentDir string, options ...Option) (*DiskSegmentImpl, error) {
	// Set default configuration
	s := &DiskSegmentImpl{
		logId:            logId,
		segmentId:        segId,
		logFileDir:       parentDir,
		fragmentSize:     128 * 1024 * 1024, // Default 128MB
		maxEntryPerFile:  100000,            // Default 100k entries per flush
		maxBufferSize:    32 * 1024 * 1024,  // Default 32MB buffer
		maxBufferEntries: 10000,
		maxIntervalMs:    1000, // Default 1s
		closeCh:          make(chan struct{}, 1),
		autoSync:         true,
	}
	s.lastSync.Store(time.Now().UnixMilli())
	s.firstEntryID.Store(-1)
	s.lastEntryID.Store(-1)
	s.lastFragmentID.Store(-1)
	s.fenced.Store(false)
	s.closed.Store(false)

	// Apply options
	for _, opt := range options {
		opt(s)
	}

	// Create log directory
	if err := os.MkdirAll(s.logFileDir, 0755); err != nil {
		return nil, err
	}

	// writer should open an empty dir too begin
	err := s.checkDirIsEmpty(ctx)
	if err != nil {
		return nil, err
	}

	// Create segment lock file
	if err := s.createSegmentLock(ctx); err != nil {
		return nil, errors.Wrap(err, "failed to create segment lock")
	}

	newBuffer := cache.NewSequentialBuffer(s.logId, s.segmentId, s.lastEntryID.Load()+1, s.maxBufferEntries) // Default cache for 10000 entries
	s.buffer.Store(newBuffer)

	// Start periodic sync goroutine
	if s.autoSync {
		go s.run()
	}

	logger.Ctx(ctx).Info("NewDiskSegmentImpl", zap.String("logFileDir", s.logFileDir))
	return s, nil
}

// run performs periodic sync operations, similar to the sync mechanism in ObjectStorage
func (s *DiskSegmentImpl) run() {
	// Timer
	ticker := time.NewTicker(time.Duration(s.maxIntervalMs * int(time.Millisecond)))
	defer ticker.Stop()

	// Fence check timer - check every 5 seconds
	fenceCheckTicker := time.NewTicker(5 * time.Second)
	defer fenceCheckTicker.Stop()

	logIdStr := fmt.Sprintf("%d", s.logId)
	metrics.WpFileWriters.WithLabelValues(logIdStr).Inc()
	for {
		select {
		case <-ticker.C:
			// Check if closed
			if s.closed.Load() {
				return
			}

			// Check last sync time to avoid too frequent syncs
			if time.Now().UnixMilli()-s.lastSync.Load() < int64(s.maxIntervalMs) {
				continue
			}

			ctx, sp := logger.NewIntentCtx(SegmentScopeName, fmt.Sprintf("run_%d_%d", s.logId, s.segmentId))
			err := s.Sync(ctx)
			if err != nil {
				logger.Ctx(ctx).Warn("disk log file sync error",
					zap.String("logFileDir", s.logFileDir),
					zap.Error(err))
			}
			sp.End()
			ticker.Reset(time.Duration(s.maxIntervalMs * int(time.Millisecond)))
		case <-fenceCheckTicker.C:
			// Check if closed
			if s.closed.Load() {
				return
			}

			// Check for fence flag file
			ctx, sp := logger.NewIntentCtx(SegmentScopeName, fmt.Sprintf("checkFence_%d_%d", s.logId, s.segmentId))
			if exists, err := checkFenceFlagFileExists(ctx, s.logFileDir, s.logId, s.segmentId); err != nil {
				logger.Ctx(ctx).Warn("Error checking fence flag file", zap.Error(err))
			} else if exists {
				// Mark as fenced and trigger close sequence
				if !s.fenced.Load() {
					logger.Ctx(ctx).Warn("Fence flag file detected, marking segment as fenced",
						zap.String("logFileDir", s.logFileDir),
						zap.Int64("logId", s.logId),
						zap.Int64("segmentId", s.segmentId))

					s.fenced.Store(true)

					// Trigger close sequence when fenced
					go func() {
						closeCtx, sp := logger.NewIntentCtx(SegmentScopeName, fmt.Sprintf("fenced_close_%d_%d", s.logId, s.segmentId))
						defer sp.End()

						logger.Ctx(closeCtx).Info("Initiating close sequence due to fence detection",
							zap.String("logFileDir", s.logFileDir),
							zap.Int64("logId", s.logId),
							zap.Int64("segmentId", s.segmentId))

						if err := s.Close(closeCtx); err != nil {
							logger.Ctx(closeCtx).Error("Failed to close segment after fence detection",
								zap.String("logFileDir", s.logFileDir),
								zap.Error(err))
						}
					}()
				}
			}
			sp.End()
		case <-s.closeCh:
			logger.Ctx(context.Background()).Info("DiskSegmentImpl successfully closed",
				zap.String("logFileDir", s.logFileDir))
			metrics.WpFileWriters.WithLabelValues(logIdStr).Dec()
			return
		}
	}
}

// GetId returns the log file ID
func (s *DiskSegmentImpl) GetId() int64 {
	return s.segmentId
}

// Append synchronously appends a log entry
// Deprecated TODO
func (s *DiskSegmentImpl) Append(ctx context.Context, data []byte) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Append")
	defer sp.End()
	// Get current max ID and increment by 1
	entryId := s.lastEntryID.Add(1) // TODO delete this

	logger.Ctx(ctx).Debug("Append: synchronous write", zap.Int64("entryId", entryId))

	resultChannel := channel.NewLocalResultChannel(fmt.Sprintf("%d/%d/%d", s.logId, s.segmentId, entryId))

	// Use AppendAsync for asynchronous write
	_, err := s.AppendAsync(ctx, entryId, data, resultChannel)
	if err != nil {
		logger.Ctx(ctx).Debug("Append: async write failed", zap.Error(err))
		return err
	}

	syncedResult, readResultErr := resultChannel.ReadResult(ctx)
	if readResultErr != nil {
		logger.Ctx(ctx).Debug("Append: read result failed", zap.Error(readResultErr))
		return readResultErr
	}
	if syncedResult.SyncedId < 0 {
		logger.Ctx(ctx).Warn("Append: write failed",
			zap.Int64("result", syncedResult.SyncedId),
			zap.Error(syncedResult.Err))
		return fmt.Errorf("failed to append entry, got result %d", syncedResult.SyncedId)
	}
	logger.Ctx(ctx).Debug("Append: write succeeded",
		zap.Int64("result", syncedResult.SyncedId))
	return nil
}

// AppendAsync appends data to the log file asynchronously.
func (s *DiskSegmentImpl) AppendAsync(ctx context.Context, entryId int64, value []byte, resultCh channel.ResultChannel) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "AppendAsync")
	defer sp.End()
	logger.Ctx(ctx).Debug("AppendAsync: attempting to write", zap.String("logFileDir", s.logFileDir), zap.Int64("entryId", entryId), zap.Int("dataLength", len(value)), zap.String("logFileInst", fmt.Sprintf("%p", s)))

	// Handle closed file
	if s.closed.Load() {
		logger.Ctx(ctx).Debug("AppendAsync: failed - segment writer is closed", zap.String("logFileDir", s.logFileDir), zap.Int64("entryId", entryId), zap.Int("dataLength", len(value)), zap.String("logFileInst", fmt.Sprintf("%p", s)))
		return -1, werr.ErrLogFileClosed
	}
	if s.fenced.Load() {
		// quick fail and return a fenced Err
		logger.Ctx(ctx).Debug("AppendAsync: failed - segment is fenced", zap.String("logFileDir", s.logFileDir), zap.Int64("entryId", entryId), zap.Int("dataLength", len(value)), zap.String("logFileInst", fmt.Sprintf("%p", s)))
		return -1, werr.ErrSegmentFenced
	}

	// First check if ID already exists in synced data
	lastId := s.lastEntryID.Load()
	if entryId <= lastId {
		logger.Ctx(ctx).Debug("AppendAsync: ID already exists, returning success", zap.Int64("entryId", entryId))
		// For data already written to disk, don't try to rewrite, just return success
		sendResultErr := resultCh.SendResult(ctx, &channel.AppendResult{
			SyncedId: entryId,
			Err:      nil,
		})
		if sendResultErr != nil {
			logger.Ctx(ctx).Warn("AppendAsync: failed to send result to channel", zap.String("logFileDir", s.logFileDir), zap.Int64("entryId", entryId), zap.Error(sendResultErr))
		}
		return entryId, nil
	}

	s.mu.Lock()
	currentBuffer := s.buffer.Load()
	// Write to buffer with notification channel
	id, err := currentBuffer.WriteEntryWithNotify(entryId, value, resultCh)
	if err != nil {
		logger.Ctx(ctx).Debug("AppendAsync: writing to buffer failed", zap.Error(err))
		s.mu.Unlock()
		return -1, err
	}
	logger.Ctx(ctx).Debug("AppendAsync: successfully written to buffer", zap.Int64("id", id), zap.Int64("expectedNextEntryId", currentBuffer.ExpectedNextEntryId.Load()))
	s.mu.Unlock()

	// Check if sync needs to be triggered
	dataSize := currentBuffer.DataSize.Load()
	if dataSize >= int64(s.maxBufferSize) {
		logger.Ctx(ctx).Debug("reach max buffer size, trigger flush",
			zap.String("logFileDir", s.logFileDir),
			zap.Int64("bufferSize", dataSize),
			zap.Int64("maxSize", int64(s.maxBufferSize)))
		syncErr := s.Sync(ctx)
		if syncErr != nil {
			logger.Ctx(ctx).Warn("reach max buffer size, but trigger flush failed",
				zap.String("logFileDir", s.logFileDir),
				zap.Int64("bufferSize", dataSize),
				zap.Int64("maxSize", int64(s.maxBufferSize)),
				zap.Error(syncErr))
		}
	}

	return id, nil
}

// Sync syncs the log file to disk.
func (s *DiskSegmentImpl) Sync(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Sync")
	defer sp.End()
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() {
		s.lastSync.Store(time.Now().UnixMilli())
	}()

	startTime := time.Now()
	logId := fmt.Sprintf("%d", s.logId)

	currentBuffer := s.buffer.Load()

	logger.Ctx(ctx).Debug("Try sync if necessary",
		zap.Int("bufferSize", len(currentBuffer.Entries)),
		zap.Int64("firstEntryId", currentBuffer.FirstEntryId),
		zap.Int64("expectedNextEntryId", currentBuffer.ExpectedNextEntryId.Load()),
		zap.String("SegmentImplInst", fmt.Sprintf("%p", s)))

	toFlushData, toFlushDataFirstEntryId := s.getToFlushData(ctx, currentBuffer)
	if len(toFlushData) == 0 {
		// no data to flush
		return nil
	}

	// Write data to fragment
	var originWrittenEntryID int64 = s.lastEntryID.Load()
	writeError := s.flushData(ctx, toFlushData, toFlushDataFirstEntryId)
	afterFlushEntryID := s.lastEntryID.Load()

	// Process result notifications
	if originWrittenEntryID == afterFlushEntryID {
		// No entries were successfully written, notify all channels of write failure, let clients retry
		// no flush success, notify all entries with error
		currentBuffer.NotifyAllPendingEntries(ctx, -1, writeError)
		// reset buffer as empty
		currentBuffer.Reset(ctx)

		metrics.WpFileOperationsTotal.WithLabelValues(logId, "sync", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, "sync", "error").Observe(float64(time.Since(startTime).Milliseconds()))
	} else if originWrittenEntryID < afterFlushEntryID {
		if writeError == nil { // Indicates all successful
			restDataFirstEntryId := currentBuffer.ExpectedNextEntryId.Load()
			restData, err := currentBuffer.ReadEntriesToLast(restDataFirstEntryId)
			if err != nil {
				logger.Ctx(ctx).Warn("Call Sync, but ReadEntriesToLastData failed", zap.String("logFileDir", s.logFileDir), zap.Error(err))
				metrics.WpFileOperationsTotal.WithLabelValues(logId, "sync", "error").Inc()
				metrics.WpFileOperationLatency.WithLabelValues(logId, "sync", "error").Observe(float64(time.Since(startTime).Milliseconds()))
				return err
			}
			newBuffer := cache.NewSequentialBufferWithData(s.logId, s.segmentId, restDataFirstEntryId, 10000, restData)
			s.buffer.Store(newBuffer)

			// notify all successfully flushed entries
			currentBuffer.NotifyEntriesInRange(ctx, toFlushDataFirstEntryId, restDataFirstEntryId, afterFlushEntryID, nil)

			metrics.WpFileOperationsTotal.WithLabelValues(logId, "sync", "success").Inc()
			metrics.WpFileOperationLatency.WithLabelValues(logId, "sync", "success").Observe(float64(time.Since(startTime).Milliseconds()))
		} else { // Indicates partial success
			restDataFirstEntryId := afterFlushEntryID + 1

			// Notify successful entries with success and failed entries with error
			currentBuffer.NotifyEntriesInRange(ctx, toFlushDataFirstEntryId, restDataFirstEntryId, afterFlushEntryID, nil)
			currentBuffer.NotifyEntriesInRange(ctx, restDataFirstEntryId, currentBuffer.ExpectedNextEntryId.Load(), -1, writeError)

			// Need to recreate buffer to allow client retry
			// new a empty buffer
			newBuffer := cache.NewSequentialBuffer(s.logId, s.segmentId, restDataFirstEntryId, s.maxBufferEntries)
			s.buffer.Store(newBuffer)

			metrics.WpFileOperationsTotal.WithLabelValues(logId, "sync", "partial").Inc()
			metrics.WpFileOperationLatency.WithLabelValues(logId, "sync", "partial").Observe(float64(time.Since(startTime).Milliseconds()))
		}
	}

	// Update lastEntryID
	if originWrittenEntryID < afterFlushEntryID {
		logger.Ctx(ctx).Debug("Sync completed, lastEntryID updated",
			zap.Int64("from", originWrittenEntryID),
			zap.Int64("to", afterFlushEntryID),
			zap.String("fragmentKey", s.currFragment.GetFragmentKey()))
	}

	logger.Ctx(ctx).Debug("Sync completed")
	return nil
}

func (s *DiskSegmentImpl) getToFlushData(ctx context.Context, currentBuffer *cache.SequentialBuffer) ([]*cache.BufferEntry, int64) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "getToFlushData")
	defer sp.End()
	entryCount := len(currentBuffer.Entries)
	if entryCount == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("logFileDir", s.logFileDir))
		return nil, -1
	}

	expectedNextEntryId := currentBuffer.ExpectedNextEntryId.Load()
	dataSize := currentBuffer.DataSize.Load()
	sequentialReadyDataSize := currentBuffer.SequentialReadyDataSize.Load()

	// Check if there is data that needs to be flushed
	if expectedNextEntryId-currentBuffer.FirstEntryId == 0 {
		logger.Ctx(ctx).Debug("Call Sync, expected id not received yet, skip ... ",
			zap.String("logFileDir", s.logFileDir),
			zap.Int64("bufferSize", dataSize),
			zap.Int64("sequentialReadyDataSize", sequentialReadyDataSize),
			zap.Int64("expectedNextEntryId", expectedNextEntryId))
		return nil, -1
	}

	// Read data that needs to be flushed
	logger.Ctx(ctx).Debug("Sync reading sequential data from buffer start",
		zap.Int64("firstEntryId", currentBuffer.FirstEntryId),
		zap.Int64("expectedNextEntryId", expectedNextEntryId))
	toFlushData, err := currentBuffer.ReadEntriesRange(currentBuffer.FirstEntryId, expectedNextEntryId)
	if err != nil {
		logger.Ctx(ctx).Warn("Call Sync, but ReadEntriesRangeData failed",
			zap.String("logFileDir", s.logFileDir),
			zap.Error(err))
		return nil, -1
	}
	toFlushDataFirstEntryId := currentBuffer.FirstEntryId
	logger.Ctx(ctx).Debug("Sync data to be written", zap.Int("count", len(toFlushData)), zap.Int64("startingID", toFlushDataFirstEntryId))
	return toFlushData, toFlushDataFirstEntryId
}

func (s *DiskSegmentImpl) flushData(ctx context.Context, toFlushData []*cache.BufferEntry, toFlushDataFirstEntryId int64) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "flushData")
	defer sp.End()
	var writeError error
	var lastWrittenToBuffEntryID int64 = s.lastEntryID.Load()
	var pendingFlushBytes int = 0

	startTime := time.Now()
	logId := fmt.Sprintf("%d", s.logId)

	// Ensure fragment is created
	if s.currFragment == nil {
		logger.Ctx(ctx).Debug("Sync needs to create a new fragment")
		if rotateErr := s.rotateFragment(ctx, s.lastEntryID.Load()+1); rotateErr != nil {
			logger.Ctx(ctx).Debug("Sync failed to create new fragment", zap.Error(rotateErr))
			metrics.WpFileOperationsTotal.WithLabelValues(logId, "flush", "rotate_error").Inc()
			metrics.WpFileOperationLatency.WithLabelValues(logId, "flush", "rotate_error").Observe(float64(time.Since(startTime).Milliseconds()))
			return rotateErr
		}
	}

	logger.Ctx(ctx).Debug("Sync starting to write data to fragment")
	for i, bufferItem := range toFlushData {
		if bufferItem == nil {
			// Empty data means no data or a gap, end this flush
			logger.Ctx(ctx).Warn("write entry to fragment failed, empty entry data found",
				zap.String("logFileDir", s.logFileDir),
				zap.Int("index", i),
				zap.Int64("toFlushDataFirstEntryId", toFlushDataFirstEntryId))
			break
		}

		// Check if current fragment is full, if so create a new one
		if s.needNewFragment(ctx) {
			logger.Ctx(ctx).Debug("Sync detected need for new fragment")
			// First flush current fragment to disk
			logger.Ctx(ctx).Debug("Sync flushing current fragment",
				zap.Int64("lastWrittenToBuffEntryID", lastWrittenToBuffEntryID),
				zap.Int64("lastEntryID", s.lastEntryID.Load()))
			if err := s.currFragment.Flush(ctx); err != nil {
				logger.Ctx(ctx).Warn("Sync failed to flush current fragment",
					zap.Error(err))
				writeError = err
				break
			}
			metrics.WpFragmentFlushBytes.WithLabelValues(logId).Add(float64(pendingFlushBytes))
			s.lastEntryID.Store(lastWrittenToBuffEntryID)
			pendingFlushBytes = 0

			// Create new fragment
			if err := s.rotateFragment(ctx, s.lastEntryID.Load()+1); err != nil {
				logger.Ctx(ctx).Debug("Sync failed to create new fragment",
					zap.Error(err))
				writeError = err
				break
			}
		}

		// Create data with EntryID - ID as first 8 bytes of data
		entryID := toFlushDataFirstEntryId + int64(i)
		entryIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(entryIDBytes, uint64(entryID))
		dataWithID := append(entryIDBytes, bufferItem.Data...)

		// Write data
		logger.Ctx(ctx).Debug("Sync writing data",
			zap.Int64("entryID", entryID),
			zap.Int("dataLen", len(bufferItem.Data)))
		writeErr := s.currFragment.Append(ctx, dataWithID, entryID)
		// If fragment full error, rotate fragment and retry
		if writeErr != nil && werr.ErrDiskFragmentNoSpace.Is(writeErr) {
			logger.Ctx(ctx).Debug("Sync flushing current fragment",
				zap.Int64("lastWrittenToBuffEntryID", lastWrittenToBuffEntryID),
				zap.Int64("lastEntryID", s.lastEntryID.Load()))
			if flushErr := s.currFragment.Flush(ctx); flushErr != nil {
				logger.Ctx(ctx).Warn("Sync failed to flush current fragment",
					zap.Error(flushErr))
				writeError = flushErr
				break
			}
			metrics.WpFragmentFlushBytes.WithLabelValues(logId).Add(float64(pendingFlushBytes))
			s.lastEntryID.Store(lastWrittenToBuffEntryID)
			pendingFlushBytes = 0

			if rotateErr := s.rotateFragment(ctx, s.lastEntryID.Load()+1); rotateErr != nil {
				logger.Ctx(ctx).Warn("Sync failed to create new fragment",
					zap.Error(rotateErr))
				writeError = rotateErr
				break
			}
			// Retry once
			writeErr = s.currFragment.Append(ctx, dataWithID, entryID)
		}

		// If still failed, abort this sync
		if writeErr != nil {
			logger.Ctx(ctx).Warn("write entry to fragment failed",
				zap.String("logFileDir", s.logFileDir),
				zap.Int64("entryId", entryID),
				zap.Error(writeErr))
			break
		}
		// write success, update monitor bytesSize
		pendingFlushBytes += len(dataWithID)
		// update last written entryId
		lastWrittenToBuffEntryID = entryID
	}

	// If written data not yet flushed to disk, perform a flush
	if lastWrittenToBuffEntryID > s.lastEntryID.Load() {
		logger.Ctx(ctx).Debug("Sync flushing current fragment",
			zap.Int64("lastWrittenToBuffEntryID", lastWrittenToBuffEntryID),
			zap.Int64("lastEntryID", s.lastEntryID.Load()))
		flushErr := s.currFragment.Flush(ctx)
		if flushErr != nil {
			logger.Ctx(ctx).Warn("Sync failed to flush current fragment", zap.Error(flushErr))
			writeError = flushErr
		} else {
			metrics.WpFragmentFlushBytes.WithLabelValues(logId).Add(float64(pendingFlushBytes))
			s.lastEntryID.Store(lastWrittenToBuffEntryID)
			pendingFlushBytes = 0
		}
	}

	return writeError
}

// needNewFragment checks if a new fragment needs to be created
func (s *DiskSegmentImpl) needNewFragment(ctx context.Context) bool {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "needNewFragment")
	defer sp.End()
	if s.currFragment == nil {
		return true
	}

	// Check if fragment is closed
	if s.currFragment.IsClosed(ctx) {
		return true
	}

	if s.currFragment.IsFull(ctx, 1024) {
		logger.Ctx(ctx).Debug("Need new fragment due to size limit",
			zap.String("logFileDir", s.logFileDir),
			zap.Int64("fragmentSize", s.fragmentSize))
		return true
	}

	// Check if reached entry count limit
	lastEntry, err := s.currFragment.GetLastEntryId(ctx)
	if err != nil {
		// If get failed, possibly the fragment is problematic, create a new one
		logger.Ctx(ctx).Warn("Cannot get last entry ID, rotating fragment",
			zap.String("logFileDir", s.logFileDir),
			zap.Error(err))
		return true
	}

	firstEntry, err := s.currFragment.GetFirstEntryId(ctx)
	if err != nil {
		// If get failed, possibly the fragment is problematic, create a new one
		logger.Ctx(ctx).Warn("Cannot get first entry ID, rotating fragment",
			zap.String("logFileDir", s.logFileDir),
			zap.Error(err))
		return true
	}

	entriesInFragment := lastEntry - firstEntry + 1
	if entriesInFragment >= int64(s.maxEntryPerFile) {
		logger.Ctx(ctx).Debug("Need new fragment due to entry count limit",
			zap.String("logFileDir", s.logFileDir),
			zap.Int64("entriesInFragment", entriesInFragment),
			zap.Int("maxEntryPerFile", s.maxEntryPerFile))
		return true
	}

	return false
}

// rotateFragment closes the current fragment and creates a new one
func (s *DiskSegmentImpl) rotateFragment(ctx context.Context, fragmentFirstEntryId int64) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "rotateFragment")
	defer sp.End()
	logger.Ctx(ctx).Info("rotating fragment",
		zap.String("logFileDir", s.logFileDir),
		zap.Int64("fragmentFirstEntryId", fragmentFirstEntryId))

	// If current fragment exists, close it first
	if s.currFragment != nil {
		logger.Ctx(ctx).Debug("Sync flushing current fragment before rotate",
			zap.Int64("lastEntryID", s.lastEntryID.Load()))
		if err := s.currFragment.Finalize(context.Background()); err != nil {
			return errors.Wrap(err, "flush current fragment")
		}
		logger.Ctx(ctx).Debug("Sync flushing current fragment after rotate",
			zap.Int64("lastEntryID", s.lastEntryID.Load()))

		// Release immediately frees resources
		if err := s.currFragment.Release(ctx); err != nil {
			return errors.Wrap(err, "close current fragment")
		}
	}

	// Create new fragment ID
	fragmentID := s.lastFragmentID.Add(1)

	logger.Ctx(ctx).Info("creating new fragment",
		zap.String("logFileDir", s.logFileDir),
		zap.Int64("fragmentID", fragmentID),
		zap.Int64("firstEntryID", fragmentFirstEntryId))

	// Create new fragment
	fragmentPath := getFragmentFileKey(s.logFileDir, fragmentID)
	fragment, err := legacy.NewLegacyFragmentFileWriter(ctx, fragmentPath, s.fragmentSize, s.logId, s.segmentId, fragmentID, fragmentFirstEntryId)
	if err != nil {
		return errors.Wrapf(err, "create new fragment: %s", fragmentPath)
	}

	// Update current fragment
	s.currFragment = fragment
	return nil
}

// NewReader creates a new Reader instance
func (s *DiskSegmentImpl) NewReader(ctx context.Context, opt storage.ReaderOpt) (storage.Reader, error) {
	return nil, werr.ErrNotSupport.WithCauseErrMsg("DiskSegmentImpl writer support write only, cannot create reader")
}

// Load loads data from disk
func (s *DiskSegmentImpl) Load(ctx context.Context) (int64, storage.Fragment, error) {
	return -1, nil, werr.ErrNotSupport.WithCauseErrMsg("not support DiskSegmentImpl writer to load currently")
}

// Merge merges log file fragments
func (s *DiskSegmentImpl) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	return nil, nil, nil, werr.ErrNotSupport.WithCauseErrMsg("not support DiskSegmentImpl writer to merge currently")
}

// Close closes the log file and releases resources
func (s *DiskSegmentImpl) Close(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Close")
	defer sp.End()
	if !s.closed.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Info("run: received close signal, but it already closed,skip", zap.String("SegmentImplInst", fmt.Sprintf("%p", s)))
		return nil
	}

	logger.Ctx(ctx).Info("run: received close signal,trigger sync before close")
	// Try to sync remaining data
	if err := s.Sync(context.Background()); err != nil {
		logger.Ctx(ctx).Warn("sync failed during close",
			zap.String("logFileDir", s.logFileDir),
			zap.Error(err))
	}
	// Close current fragment
	if s.currFragment != nil {
		if err := s.currFragment.Finalize(ctx); err != nil {
			logger.Ctx(ctx).Warn("failed to finalize fragment",
				zap.String("logFileDir", s.logFileDir),
				zap.Error(err))
		}
		if err := s.currFragment.Release(ctx); err != nil {
			logger.Ctx(ctx).Warn("failed to release fragment",
				zap.String("logFileDir", s.logFileDir),
				zap.Error(err))
		}
		s.currFragment.Close(ctx)
		s.currFragment = nil
	}

	logger.Ctx(ctx).Info("Closing DiskSegmentImpl", zap.String("logFileDir", s.logFileDir))

	// Release segment lock
	if err := s.releaseSegmentLock(ctx); err != nil {
		logger.Ctx(ctx).Warn("Failed to release segment lock during close",
			zap.String("logFileDir", s.logFileDir),
			zap.Error(err))
	}

	// Send close signal
	s.closeOnce.Do(func() {
		s.closeCh <- struct{}{}
		close(s.closeCh)
	})

	return nil
}

func (s *DiskSegmentImpl) checkDirIsEmpty(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "checkDirIsEmpty")
	defer sp.End()
	// Read directory contents
	entries, err := os.ReadDir(s.logFileDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(entries) > 0 {
		return werr.ErrNotSupport.WithCauseErrMsg("DiskSegmentImpl writer to should open en empty dir")
	}
	return nil
}

// LastFragmentId returns the last fragment ID
func (s *DiskSegmentImpl) LastFragmentId() int64 {
	return s.lastFragmentID.Load()
}

// GetLastEntryId returns the last entry ID
func (s *DiskSegmentImpl) GetLastEntryId(ctx context.Context) (int64, error) {
	return s.lastEntryID.Load(), nil
}

func (s *DiskSegmentImpl) DeleteFragments(ctx context.Context, flag int) error {
	return werr.ErrNotSupport.WithCauseErrMsg("not support DiskSegmentImpl writer to delete fragments currently")
}

// createSegmentLock creates a lock file for the segment to ensure exclusive access
func (s *DiskSegmentImpl) createSegmentLock(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "createSegmentLock")
	defer sp.End()

	// Create lock file path
	s.lockFilePath = filepath.Join(s.logFileDir, fmt.Sprintf("segment_%d_%d.lock", s.logId, s.segmentId))

	// Create flock instance
	s.lockFile = flock.New(s.lockFilePath)

	// Try to acquire exclusive lock (non-blocking)
	locked, err := s.lockFile.TryLock()
	if err != nil {
		logger.Ctx(ctx).Error("Failed to try lock file",
			zap.String("lockFilePath", s.lockFilePath),
			zap.Error(err))
		return errors.Wrapf(err, "failed to try lock file: %s", s.lockFilePath)
	}

	if !locked {
		logger.Ctx(ctx).Error("Failed to acquire exclusive lock - file is already locked",
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
func (s *DiskSegmentImpl) releaseSegmentLock(ctx context.Context) error {
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

// checkFenceFlagFileExists checks for the existence of a fence flag file and returns (exists, error)
func checkFenceFlagFileExists(ctx context.Context, logFileDir string, logId int64, segmentId int64) (bool, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "checkFenceFlagFileExists")
	defer sp.End()

	// Get fence flag file path
	fenceFlagPath := getFenceFlagPath(logFileDir, logId, segmentId)

	// Check if fence flag file exists
	if _, err := os.Stat(fenceFlagPath); err == nil {
		// Fence flag file exists
		logger.Ctx(ctx).Debug("Fence flag file detected",
			zap.String("logFileDir", logFileDir),
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
			zap.String("logFileDir", logFileDir),
			zap.String("fenceFlagPath", fenceFlagPath),
			zap.Error(err))
		return false, err
	}
}

// getFenceFlagPath returns the path to the fence flag file for a segment
func getFenceFlagPath(logFileDir string, logId int64, segmentId int64) string {
	return filepath.Join(logFileDir, fmt.Sprintf("segment_%d_%d.fence", logId, segmentId))
}

// waitForFenceCheckIntervalIfLockExists checks if lock file exists and waits for fence check interval
// to ensure other processes have time to detect the fence flag
func waitForFenceCheckIntervalIfLockExists(ctx context.Context, logFileDir string, logId int64, segmentId int64, fenceFlagPath string) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "waitForFenceCheckIntervalIfLockExists")
	defer sp.End()

	// Check if lock file exists
	lockFilePath := filepath.Join(logFileDir, fmt.Sprintf("segment_%d_%d.lock", logId, segmentId))
	if _, err := os.Stat(lockFilePath); err == nil {
		// Lock file exists, wait for fence check interval (5 seconds)
		logger.Ctx(ctx).Info("Lock file exists, waiting for fence check interval to ensure other processes detect fence flag",
			zap.String("logFileDir", logFileDir),
			zap.String("lockFilePath", lockFilePath),
			zap.String("fenceFlagPath", fenceFlagPath),
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segmentId))

		select {
		case <-ctx.Done():
			logger.Ctx(ctx).Warn("Context cancelled while waiting for fence check interval",
				zap.String("logFileDir", logFileDir))
			return ctx.Err()
		case <-time.After(5 * time.Second):
			// Wait completed
			logger.Ctx(ctx).Info("Fence check interval wait completed",
				zap.String("logFileDir", logFileDir))
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

func (s *DiskSegmentImpl) IsFenced(ctx context.Context) (bool, error) {
	// if already fenced, no need to check again
	if s.fenced.Load() {
		return true, nil
	}
	// otherwise, check fence flag file
	exists, err := checkFenceFlagFileExists(ctx, s.logFileDir, s.logId, s.segmentId)
	if err != nil {
		return false, err
	}
	if exists {
		s.fenced.Store(true)
		return true, nil
	}
	return false, nil
}

func (s *DiskSegmentImpl) Fence(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Fence")
	defer sp.End()

	// If already fenced, return idempotently
	if s.fenced.Load() {
		logger.Ctx(ctx).Debug("Segment already fenced, returning idempotently",
			zap.String("logFileDir", s.logFileDir),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId))
		return s.lastEntryID.Load(), nil
	}

	// Get fence flag file path
	fenceFlagPath := getFenceFlagPath(s.logFileDir, s.logId, s.segmentId)

	// Create fence flag file
	fenceFile, err := os.Create(fenceFlagPath)
	if err != nil {
		logger.Ctx(ctx).Error("Failed to create fence flag file",
			zap.String("logFileDir", s.logFileDir),
			zap.String("fenceFlagPath", fenceFlagPath),
			zap.Int64("logId", s.logId),
			zap.Int64("segmentId", s.segmentId),
			zap.Error(err))
		return -1, fmt.Errorf("failed to create fence flag file %s: %w", fenceFlagPath, err)
	}

	// Write fence information to the file
	fenceInfo := fmt.Sprintf("logId=%d\nsegmentId=%d\npid=%d\ntimestamp=%d\nreason=manual_fence\n",
		s.logId, s.segmentId, os.Getpid(), time.Now().Unix())

	_, writeErr := fenceFile.WriteString(fenceInfo)
	fenceFile.Close()

	if writeErr != nil {
		logger.Ctx(ctx).Warn("Failed to write fence info to file, but file created successfully",
			zap.String("fenceFlagPath", fenceFlagPath),
			zap.Error(writeErr))
		// Continue even if writing info fails, as the file existence is what matters
	}

	// Mark as fenced
	s.fenced.Store(true)
	lastEntryId := s.lastEntryID.Load()

	// Wait for fence check interval if lock file exists
	if err := waitForFenceCheckIntervalIfLockExists(ctx, s.logFileDir, s.logId, s.segmentId, fenceFlagPath); err != nil {
		logger.Ctx(ctx).Warn("Error during fence check interval wait",
			zap.String("logFileDir", s.logFileDir),
			zap.Error(err))
		return lastEntryId, err
	}

	logger.Ctx(ctx).Info("Successfully created fence flag file and marked segment as fenced",
		zap.String("logFileDir", s.logFileDir),
		zap.String("fenceFlagPath", fenceFlagPath),
		zap.Int64("logId", s.logId),
		zap.Int64("segmentId", s.segmentId),
		zap.Int64("lastEntryId", lastEntryId))

	return lastEntryId, nil
}

var _ storage.Segment = (*RODiskSegmentImpl)(nil)

// RODiskSegmentImpl is used to manage and read exists data from disk-based storage as a logical segment
type RODiskSegmentImpl struct {
	mu         sync.RWMutex
	logId      int64
	segmentId  int64
	logFileDir string

	// Configuration parameters
	fragmentSize  int64 // Maximum size of each fragment
	readBatchSize int64 // Batch size for reading

	// State
	fragments []storage.AppendableFragment // LogFile cached fragments in order
	// For fence state: true confirms it is fenced, while false requires verification by checking the file system for a fence flag file.
	fenced atomic.Bool
}

func NewRODiskSegmentImpl(ctx context.Context, logId int64, segId int64, basePath string, options ...ROption) (*RODiskSegmentImpl, error) {
	// Set default configuration
	rs := &RODiskSegmentImpl{
		logId:        logId,
		segmentId:    segId,
		logFileDir:   basePath,
		fragmentSize: 128 * 1024 * 1024, // Default 128MB
		fragments:    make([]storage.AppendableFragment, 0),
	}
	rs.fenced.Store(false)

	// Apply options
	for _, opt := range options {
		opt(rs)
	}

	// Create log directory
	if err := os.MkdirAll(rs.logFileDir, 0755); err != nil {
		return nil, err
	}

	// Load existing fragments
	_, _, err := rs.fetchROFragments(ctx)
	if err != nil {
		return nil, err
	}
	logger.Ctx(ctx).Info("NewRODiskSegmentImpl", zap.String("logFileDir", rs.logFileDir), zap.Int("fragments", len(rs.fragments)))
	return rs, nil
}

// GetId returns the segment ID
func (rs *RODiskSegmentImpl) GetId() int64 {
	return rs.segmentId
}

func (rs *RODiskSegmentImpl) Append(ctx context.Context, data []byte) error {
	return werr.ErrNotSupport.WithCauseErrMsg("RODiskSegmentImpl not support append")
}

// AppendAsync appends data to the log file asynchronously.
func (rs *RODiskSegmentImpl) AppendAsync(ctx context.Context, entryId int64, value []byte, resultCh channel.ResultChannel) (int64, error) {
	return entryId, werr.ErrNotSupport.WithCauseErrMsg("RODiskSegmentImpl not support append")
}

// Sync syncs the log file to disk.
func (rs *RODiskSegmentImpl) Sync(ctx context.Context) error {
	return werr.ErrNotSupport.WithCauseErrMsg("RODiskSegmentImpl not support sync")
}

// NewReader creates a new Reader instance
func (rs *RODiskSegmentImpl) NewReader(ctx context.Context, opt storage.ReaderOpt) (storage.Reader, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "NewReader")
	defer sp.End()
	// Create new DiskReader
	// TODO current only support config batch size
	reader := NewDiskEntryReader(ctx, opt, rs.readBatchSize, rs)
	logger.Ctx(ctx).Debug("NewReader", zap.Any("reader", reader), zap.Any("opt", opt))
	return reader, nil
}

// Load loads data from disk
func (rs *RODiskSegmentImpl) Load(ctx context.Context) (int64, storage.Fragment, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Load")
	defer sp.End()
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	if len(rs.fragments) == 0 {
		return 0, nil, nil
	}

	startTime := time.Now()
	logId := fmt.Sprintf("%d", rs.logId)

	// Calculate total size and return the last fragment
	totalSize := int64(0)
	lastFragment := rs.fragments[len(rs.fragments)-1]

	// Calculate sum of all fragment sizes
	for _, frag := range rs.fragments {
		size := frag.GetSize()
		totalSize += size
	}

	// Update metrics
	metrics.WpFileOperationsTotal.WithLabelValues(logId, "load", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, "load", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	// FragmentManager is responsible for managing fragment lifecycle, no need to release manually
	return totalSize, lastFragment, nil
}

func (rs *RODiskSegmentImpl) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	return nil, nil, nil, errors.New("no need to merge fragments for local filesystem")
}

// Close closes the log file and releases resources
func (rs *RODiskSegmentImpl) Close(ctx context.Context) error {
	return nil
}

// fetchROFragments returns all exists fragments in the log file, which is readonly
func (rs *RODiskSegmentImpl) fetchROFragments(ctx context.Context) (bool, storage.AppendableFragment, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	startTime := time.Now()
	logId := fmt.Sprintf("%d", rs.logId)

	var fetchedLastFragment storage.AppendableFragment = nil
	fragId := int64(0)
	if len(rs.fragments) > 0 {
		lastFrag := rs.fragments[len(rs.fragments)-1]
		fragId = int64(lastFrag.GetFragmentId()) + 1
		fetchedLastFragment = lastFrag
	}
	existsNewFragment := false
	for {
		// check if the fragment exists
		fragmentPath := getFragmentFileKey(rs.logFileDir, fragId)
		fileInfo, err := os.Stat(fragmentPath)
		if err != nil {
			// This file does not exist, which means there are no files later,
			// because the exists file ids always sequential and have no holes
			break
		}

		// Create FragmentFile instance and load
		fragment, err := legacy.NewLegacyFragmentFileReader(ctx, fragmentPath, fileInfo.Size(), rs.logId, rs.segmentId, fragId)
		if err != nil {
			break
		}
		if !fragment.IsMMapReadable(ctx) {
			logger.Ctx(ctx).Warn("found a new fragment unreadable", zap.String("logFileDir", rs.logFileDir), zap.String("fragmentPath", fragmentPath), zap.Error(err))
			break
		}

		fetchedLastFragment = fragment
		rs.fragments = append(rs.fragments, fragment)
		existsNewFragment = true
		fragId++
		logger.Ctx(ctx).Debug("fetch fragment info", zap.String("logFileDir", rs.logFileDir), zap.Int64("lastFragId", fragId-1))
	}

	// Update metrics
	metrics.WpFileOperationsTotal.WithLabelValues(logId, "fetch_fragments", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, "fetch_fragments", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	// Update file bytes metric
	totalSize := int64(0)
	for _, frag := range rs.fragments {
		totalSize += frag.GetSize()
	}

	logger.Ctx(ctx).Debug("fetch fragment infos", zap.String("logFileDir", rs.logFileDir), zap.Int("fragments", len(rs.fragments)), zap.Int64("lastFragId", fragId-1))
	return existsNewFragment, fetchedLastFragment, nil
}

// Deprecated, no use
// LastFragmentId returns the last fragment ID
func (rs *RODiskSegmentImpl) LastFragmentId() int64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	if len(rs.fragments) == 0 {
		return -1
	}
	return rs.fragments[len(rs.fragments)-1].GetFragmentId()
}

// GetLastEntryId returns the last entry ID
func (rs *RODiskSegmentImpl) GetLastEntryId(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "GetLastEntryId")
	defer sp.End()
	// prefetch fragmentInfos if any new fragment created
	_, lastFragment, err := rs.fetchROFragments(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("get fragments failed",
			zap.String("logFileDir", rs.logFileDir),
			zap.Error(err))
		return -1, err
	}
	if lastFragment == nil {
		return -1, nil
	}
	return legacy.GetFragmentFileLastEntryIdWithoutDataLoadedIfPossible(ctx, lastFragment)
}

// findFragmentFrom returns the fragment index and fragment object
func (rs *RODiskSegmentImpl) findFragmentFrom(ctx context.Context, fromIdx int, currEntryID int64, endEntryID int64) (int, storage.AppendableFragment, int64, error) {
	rs.mu.RLock()
	pendingReadEntryID := currEntryID
	lastIdx := fromIdx
	for i := fromIdx; i < len(rs.fragments); i++ {
		lastIdx = i
		fragment := rs.fragments[i]
		firstID, err := legacy.GetFragmentFileFirstEntryIdWithoutDataLoadedIfPossible(ctx, fragment)
		if err != nil {
			rs.mu.RUnlock()
			logger.Ctx(ctx).Warn("get fragment firstEntryId failed",
				zap.String("logFileDir", rs.logFileDir),
				zap.String("fragmentKey", fragment.GetFragmentKey()),
				zap.Error(err))
			return -1, nil, -1, err
		}
		lastID, err := legacy.GetFragmentFileLastEntryIdWithoutDataLoadedIfPossible(ctx, fragment)
		if err != nil {
			rs.mu.RUnlock()
			logger.Ctx(ctx).Warn("get fragment lastEntryId failed",
				zap.String("logFileDir", rs.logFileDir),
				zap.String("fragmentKey", fragment.GetFragmentKey()),
				zap.Error(err))
			return -1, nil, -1, err
		}

		// If currentID less than fragment's firstID, update currentID
		if pendingReadEntryID < firstID {
			// If endID less than fragment's firstID, means no more data
			if endEntryID > 0 && endEntryID <= firstID {
				logger.Ctx(ctx).Debug("No more entries to read", zap.String("logFileDir", rs.logFileDir), zap.Int64("currEntryId", currEntryID), zap.Int64("endEntryId", endEntryID), zap.Int64("firstId", firstID), zap.Int64("lastId", lastID))
				rs.mu.RUnlock()
				return -1, nil, -1, nil
			}
			pendingReadEntryID = firstID
		}

		// Check if currentID in fragment's range
		if pendingReadEntryID <= lastID {
			rs.mu.RUnlock()
			return i, fragment, pendingReadEntryID, nil
		}
	}
	rs.mu.RUnlock()
	newFragmentExists, _, err := rs.fetchROFragments(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("fetch fragments failed",
			zap.String("logFileDir", rs.logFileDir),
			zap.Error(err))
		return -1, nil, -1, err
	}

	if newFragmentExists {
		return rs.findFragmentFrom(ctx, lastIdx+1, pendingReadEntryID, endEntryID)
	}
	logger.Ctx(ctx).Debug("No more entries to read", zap.String("logFileDir", rs.logFileDir), zap.Int64("currEntryId", currEntryID), zap.Int64("endEntryId", endEntryID))
	return -1, nil, -1, nil
}

func (rs *RODiskSegmentImpl) DeleteFragments(ctx context.Context, flag int) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "DeleteFragments")
	defer sp.End()
	rs.mu.Lock()
	defer rs.mu.Unlock()

	startTime := time.Now()
	logId := fmt.Sprintf("%d", rs.logId)

	logger.Ctx(ctx).Info("Starting to delete fragments",
		zap.String("logFileDir", rs.logFileDir),
		zap.Int("flag", flag))

	// Read directory contents
	entries, err := os.ReadDir(rs.logFileDir)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Ctx(ctx).Info("Directory does not exist, nothing to delete",
				zap.String("logFileDir", rs.logFileDir))
			return nil
		}
		metrics.WpFileOperationsTotal.WithLabelValues(logId, "delete_fragments", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, "delete_fragments", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return err
	}

	var deleteErrors []error
	deletedCount := 0

	// Filter and delete fragment files
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".frag") {
			fragmentPath := filepath.Join(rs.logFileDir, entry.Name())

			// Delete file
			if err := os.Remove(fragmentPath); err != nil {
				logger.Ctx(ctx).Warn("Failed to delete fragment file",
					zap.String("fragmentPath", fragmentPath),
					zap.Error(err))
				deleteErrors = append(deleteErrors, err)
			} else {
				logger.Ctx(ctx).Info("Successfully deleted fragment file",
					zap.String("fragmentPath", fragmentPath))
				deletedCount++
			}
		}
	}

	// clear state
	rs.fragments = make([]storage.AppendableFragment, 0)

	// Update metrics
	if len(deleteErrors) > 0 {
		metrics.WpFileOperationsTotal.WithLabelValues(logId, "delete_fragments", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, "delete_fragments", "error").Observe(float64(time.Since(startTime).Milliseconds()))
	} else {
		metrics.WpFileOperationsTotal.WithLabelValues(logId, "delete_fragments", "success").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, "delete_fragments", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	}

	logger.Ctx(ctx).Info("Completed fragment deletion",
		zap.String("logFileDir", rs.logFileDir),
		zap.Int("deletedCount", deletedCount),
		zap.Int("errorCount", len(deleteErrors)))

	if len(deleteErrors) > 0 {
		return fmt.Errorf("failed to delete %d fragment files: ", len(deleteErrors))
	}
	return nil
}

func (rs *RODiskSegmentImpl) IsFenced(ctx context.Context) (bool, error) {
	// if already fenced, no need to check again
	if rs.fenced.Load() {
		return true, nil
	}
	// otherwise, check fence flag file
	exists, err := checkFenceFlagFileExists(ctx, rs.logFileDir, rs.logId, rs.segmentId)
	if err != nil {
		return false, err
	}
	if exists {
		rs.fenced.Store(true)
		return true, nil
	}
	return false, nil
}

func (rs *RODiskSegmentImpl) Fence(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Fence")
	defer sp.End()

	// If already fenced, return idempotently
	if rs.fenced.Load() {
		logger.Ctx(ctx).Debug("RODiskSegment already fenced, returning idempotently",
			zap.String("logFileDir", rs.logFileDir),
			zap.Int64("logId", rs.logId),
			zap.Int64("segmentId", rs.segmentId))
		// For read-only segment, get the last entry ID
		lastEntryId, err := rs.GetLastEntryId(ctx)
		if err != nil {
			return -1, err
		}
		return lastEntryId, nil
	}

	// Get fence flag file path
	fenceFlagPath := getFenceFlagPath(rs.logFileDir, rs.logId, rs.segmentId)

	// Create fence flag file
	fenceFile, err := os.Create(fenceFlagPath)
	if err != nil {
		logger.Ctx(ctx).Error("Failed to create fence flag file",
			zap.String("logFileDir", rs.logFileDir),
			zap.String("fenceFlagPath", fenceFlagPath),
			zap.Int64("logId", rs.logId),
			zap.Int64("segmentId", rs.segmentId),
			zap.Error(err))
		return -1, fmt.Errorf("failed to create fence flag file %s: %w", fenceFlagPath, err)
	}

	// Write fence information to the file
	fenceInfo := fmt.Sprintf("logId=%d\nsegmentId=%d\npid=%d\ntimestamp=%d\nreason=manual_fence\ntype=readonly\n",
		rs.logId, rs.segmentId, os.Getpid(), time.Now().Unix())

	_, writeErr := fenceFile.WriteString(fenceInfo)
	fenceFile.Close()

	if writeErr != nil {
		logger.Ctx(ctx).Warn("Failed to write fence info to file, but file created successfully",
			zap.String("fenceFlagPath", fenceFlagPath),
			zap.Error(writeErr))
		// Continue even if writing info fails, as the file existence is what matters
	}

	// Mark as fenced
	rs.fenced.Store(true)

	// For read-only segment, get the last entry ID
	lastEntryId, err := rs.GetLastEntryId(ctx)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get last entry ID after fencing",
			zap.String("logFileDir", rs.logFileDir),
			zap.Error(err))
		return -1, err
	}

	// Wait for fence check interval if lock file exists
	if err := waitForFenceCheckIntervalIfLockExists(ctx, rs.logFileDir, rs.logId, rs.segmentId, fenceFlagPath); err != nil {
		logger.Ctx(ctx).Warn("Error during fence check interval wait",
			zap.String("logFileDir", rs.logFileDir),
			zap.Error(err))
		return lastEntryId, err
	}

	logger.Ctx(ctx).Info("Successfully created fence flag file and marked RODiskSegment as fenced",
		zap.String("logFileDir", rs.logFileDir),
		zap.String("fenceFlagPath", fenceFlagPath),
		zap.Int64("logId", rs.logId),
		zap.Int64("segmentId", rs.segmentId),
		zap.Int64("lastEntryId", lastEntryId))

	return lastEntryId, nil
}

// Option is a function type for configuration options
type Option func(*DiskSegmentImpl)

// WithWriteFragmentSize sets the fragment size
func WithWriteFragmentSize(size int64) Option {
	return func(s *DiskSegmentImpl) {
		s.fragmentSize = size
	}
}

func WithWriteMaxBufferSize(size int64) Option {
	return func(s *DiskSegmentImpl) {
		s.maxBufferSize = size
	}
}

// WithWriteMaxEntryPerFile sets the maximum number of entries per fragment
func WithWriteMaxEntryPerFile(count int) Option {
	return func(s *DiskSegmentImpl) {
		s.maxEntryPerFile = count
	}
}

func WithWriteMaxIntervalMs(interval int) Option {
	return func(s *DiskSegmentImpl) {
		s.maxIntervalMs = interval
	}
}

func WithWriteDisableAutoSync() Option {
	return func(s *DiskSegmentImpl) {
		s.autoSync = false
	}
}

// ROption is a function type for configuration options
type ROption func(*RODiskSegmentImpl)

// WithReadFragmentSize sets the fragment size
func WithReadFragmentSize(size int64) ROption {
	return func(rs *RODiskSegmentImpl) {
		rs.fragmentSize = size
	}
}

// WithReadBatchSize sets the max read batch size
func WithReadBatchSize(size int64) ROption {
	return func(rs *RODiskSegmentImpl) {
		rs.fragmentSize = size
	}
}

// utils to get fragment file key
func getFragmentFileKey(logFileDir string, fragmentId int64) string {
	return fmt.Sprintf("%s/%d.frag", logFileDir, fragmentId)
}

// utils to get merged fragment file key
func getMergedFragmentFileKey(logFileDir string, mergedFragmentId int64) string {
	return fmt.Sprintf("%s/m_%d.frag", logFileDir, mergedFragmentId)
}

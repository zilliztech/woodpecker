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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
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
	currFragment *FragmentFileWriter

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
	logIdStr := fmt.Sprintf("%d", s.logId)
	segIdStr := fmt.Sprintf("%d", s.segmentId)
	metrics.WpFileWriters.WithLabelValues(logIdStr, segIdStr).Inc()
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
		case <-s.closeCh:
			logger.Ctx(context.Background()).Info("DiskSegmentImpl successfully closed",
				zap.String("logFileDir", s.logFileDir))
			metrics.WpFileWriters.WithLabelValues(logIdStr, segIdStr).Dec()
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

	resultCh := make(chan int64, 1)

	// Use AppendAsync for asynchronous write
	_, err := s.AppendAsync(ctx, entryId, data, resultCh)
	if err != nil {
		logger.Ctx(ctx).Debug("Append: async write failed", zap.Error(err))
		return err
	}

	// Wait for completion
	select {
	case result := <-resultCh:
		if result < 0 {
			logger.Ctx(ctx).Debug("Append: write failed",
				zap.Int64("result", result))
			return fmt.Errorf("failed to append entry, got result %d", result)
		}
		logger.Ctx(ctx).Debug("Append: write succeeded",
			zap.Int64("result", result))
		return nil
	case <-ctx.Done():
		logger.Ctx(ctx).Debug("Append: write timeout or canceled")
		return ctx.Err()
	}
}

// AppendAsync appends data to the log file asynchronously.
func (s *DiskSegmentImpl) AppendAsync(ctx context.Context, entryId int64, value []byte, resultCh chan<- int64) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "AppendAsync")
	defer sp.End()
	logger.Ctx(ctx).Debug("AppendAsync: attempting to write", zap.String("logFileDir", s.logFileDir), zap.Int64("entryId", entryId), zap.Int("dataLength", len(value)), zap.String("logFileInst", fmt.Sprintf("%p", s)))

	startTime := time.Now()
	logId := fmt.Sprintf("%d", s.logId)
	segmentId := fmt.Sprintf("%d", s.segmentId)

	// Handle closed file
	if s.closed.Load() {
		logger.Ctx(ctx).Debug("AppendAsync: failed - file is closed", zap.String("logFileDir", s.logFileDir), zap.Int64("entryId", entryId), zap.Int("dataLength", len(value)), zap.String("logFileInst", fmt.Sprintf("%p", s)))
		metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "append", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "append", "error").Observe(float64(time.Since(startTime).Milliseconds()))
		return -1, errors.New("DiskSegmentImpl closed")
	}

	// First check if ID already exists in synced data
	lastId := s.lastEntryID.Load()
	if entryId <= lastId {
		logger.Ctx(ctx).Debug("AppendAsync: ID already exists, returning success", zap.Int64("entryId", entryId))
		// For data already written to disk, don't try to rewrite, just return success
		resultCh <- entryId
		metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "append", "success").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "append", "success").Observe(float64(time.Since(startTime).Milliseconds()))
		return entryId, nil
	}

	s.mu.Lock()
	currentBuffer := s.buffer.Load()
	// Write to buffer with notification channel
	id, err := currentBuffer.WriteEntryWithNotify(entryId, value, resultCh)
	if err != nil {
		logger.Ctx(ctx).Debug("AppendAsync: writing to buffer failed", zap.Error(err))
		s.mu.Unlock()
		metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "append", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "append", "error").Observe(float64(time.Since(startTime).Milliseconds()))
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

	metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "append", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "append", "success").Observe(float64(time.Since(startTime).Milliseconds()))
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
	segmentId := fmt.Sprintf("%d", s.segmentId)

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
		currentBuffer.NotifyAllPendingEntries(ctx, -1)
		// reset buffer as empty
		currentBuffer.Reset(ctx)

		metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "sync", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "sync", "error").Observe(float64(time.Since(startTime).Milliseconds()))
	} else if originWrittenEntryID < afterFlushEntryID {
		if writeError == nil { // Indicates all successful
			restDataFirstEntryId := currentBuffer.ExpectedNextEntryId.Load()
			restData, err := currentBuffer.ReadEntriesToLast(restDataFirstEntryId)
			if err != nil {
				logger.Ctx(ctx).Error("Call Sync, but ReadEntriesToLastData failed", zap.String("logFileDir", s.logFileDir), zap.Error(err))
				metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "sync", "error").Inc()
				metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "sync", "error").Observe(float64(time.Since(startTime).Milliseconds()))
				return err
			}
			newBuffer := cache.NewSequentialBufferWithData(s.logId, s.segmentId, restDataFirstEntryId, 10000, restData)
			s.buffer.Store(newBuffer)

			// notify all successfully flushed entries
			currentBuffer.NotifyEntriesInRange(ctx, toFlushDataFirstEntryId, restDataFirstEntryId, afterFlushEntryID)

			metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "sync", "success").Inc()
			metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "sync", "success").Observe(float64(time.Since(startTime).Milliseconds()))
		} else { // Indicates partial success
			restDataFirstEntryId := afterFlushEntryID + 1

			// Notify successful entries with success and failed entries with error
			currentBuffer.NotifyEntriesInRange(ctx, toFlushDataFirstEntryId, restDataFirstEntryId, afterFlushEntryID)
			currentBuffer.NotifyEntriesInRange(ctx, restDataFirstEntryId, currentBuffer.ExpectedNextEntryId.Load(), -1)

			// Need to recreate buffer to allow client retry
			// new a empty buffer
			newBuffer := cache.NewSequentialBuffer(s.logId, s.segmentId, restDataFirstEntryId, s.maxBufferEntries)
			s.buffer.Store(newBuffer)

			metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "sync", "partial").Inc()
			metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "sync", "partial").Observe(float64(time.Since(startTime).Milliseconds()))
		}
	}

	// Update lastEntryID
	if originWrittenEntryID < afterFlushEntryID {
		logger.Ctx(ctx).Debug("Sync completed, lastEntryID updated",
			zap.Int64("from", originWrittenEntryID),
			zap.Int64("to", afterFlushEntryID),
			zap.String("filePath", s.currFragment.filePath))
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

	// Check if there is data that needs to be flushed
	if expectedNextEntryId-currentBuffer.FirstEntryId == 0 {
		logger.Ctx(ctx).Debug("Call Sync, expected id not received yet, skip ... ",
			zap.String("logFileDir", s.logFileDir),
			zap.Int64("bufferSize", dataSize),
			zap.Int64("expectedNextEntryId", expectedNextEntryId))
		return nil, -1
	}

	// Read data that needs to be flushed
	logger.Ctx(ctx).Debug("Sync reading sequential data from buffer start",
		zap.Int64("firstEntryId", currentBuffer.FirstEntryId),
		zap.Int64("expectedNextEntryId", expectedNextEntryId))
	toFlushData, err := currentBuffer.ReadEntriesRange(currentBuffer.FirstEntryId, expectedNextEntryId)
	if err != nil {
		logger.Ctx(ctx).Error("Call Sync, but ReadEntriesRangeData failed",
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
	segmentId := fmt.Sprintf("%d", s.segmentId)

	// Ensure fragment is created
	if s.currFragment == nil {
		logger.Ctx(ctx).Debug("Sync needs to create a new fragment")
		if rotateErr := s.rotateFragment(ctx, s.lastEntryID.Load()+1); rotateErr != nil {
			logger.Ctx(ctx).Debug("Sync failed to create new fragment", zap.Error(rotateErr))
			metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "flush", "rotate_error").Inc()
			metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "flush", "rotate_error").Observe(float64(time.Since(startTime).Milliseconds()))
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
			metrics.WpFragmentFlushBytes.WithLabelValues(logId, segmentId).Add(float64(pendingFlushBytes))
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
		writeErr := s.currFragment.Write(ctx, dataWithID, entryID)
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
			metrics.WpFragmentFlushBytes.WithLabelValues(logId, segmentId).Add(float64(pendingFlushBytes))
			s.lastEntryID.Store(lastWrittenToBuffEntryID)
			pendingFlushBytes = 0

			if rotateErr := s.rotateFragment(ctx, s.lastEntryID.Load()+1); rotateErr != nil {
				logger.Ctx(ctx).Warn("Sync failed to create new fragment",
					zap.Error(rotateErr))
				writeError = rotateErr
				break
			}
			// Retry once
			writeErr = s.currFragment.Write(ctx, dataWithID, entryID)
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
			metrics.WpFragmentFlushBytes.WithLabelValues(logId, segmentId).Add(float64(pendingFlushBytes))
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
	if s.currFragment.closed {
		return true
	}

	// Check if already reached file size limit
	currentLeftSize := s.currFragment.indexOffset - s.currFragment.dataOffset
	if currentLeftSize <= 1024 { // Leave some margin to prevent overflow
		logger.Ctx(ctx).Debug("Need new fragment due to size limit",
			zap.String("logFileDir", s.logFileDir),
			zap.Uint32("currentLeftSize", currentLeftSize),
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
		s.currFragment.isGrowing = false
		if err := s.currFragment.Flush(context.Background()); err != nil {
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
	fragment, err := NewFragmentFileWriter(ctx, fragmentPath, s.fragmentSize, s.logId, s.segmentId, fragmentID, fragmentFirstEntryId)
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

func mergeFragmentsAndReleaseAfterCompleted(ctx context.Context, mergedFragPath string, logId int64, segmentId int64, mergeFragId int64, mergeFragSize int, fragments []*FragmentFileReader) (storage.Fragment, error) {
	// check args
	if len(fragments) == 0 {
		return nil, errors.New("no fragments to merge")
	}

	// merge
	fragmentFirstEntryId := fragments[0].lastEntryID
	mergedFragmentWriter, err := NewFragmentFileWriter(ctx, mergedFragPath, int64(mergeFragSize), logId, segmentId, mergeFragId, fragmentFirstEntryId)
	if err != nil {
		return nil, errors.Wrapf(err, "create new fragment: %s", mergedFragPath)
	}

	expectedEntryId := int64(-1)
	for _, fragment := range fragments {
		err := fragment.Load(ctx)
		if err != nil {
			// Merge failed, explicitly release fragment
			mergedFragmentWriter.Release(ctx)
			return nil, err
		}
		// check the order of entries
		if expectedEntryId == -1 {
			// the first segment
			expectedEntryId = fragment.lastEntryID + 1
		} else {
			if expectedEntryId != fragment.firstEntryID {
				// Merge failed, explicitly release fragments
				mergedFragmentWriter.Release(ctx)
				return nil, errors.New("fragments are not in order")
			}
			expectedEntryId = fragment.lastEntryID + 1
		}

		// TODO
		// merge index
		// Copy fragment data to merged fragment data area
		// Copy fragment index to merged fragment index area, adjusting all index lengths
	}

	// Add merged fragment to cache
	mergedFragmentWriter.Release(ctx)

	return mergedFragmentWriter, nil
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
		s.currFragment.isGrowing = false
		if err := s.currFragment.Flush(context.TODO()); err != nil {
			logger.Ctx(ctx).Warn("failed to flush fragment",
				zap.String("logFileDir", s.logFileDir),
				zap.Error(err))
		}
		if err := s.currFragment.Release(ctx); err != nil {
			logger.Ctx(ctx).Warn("failed to release fragment",
				zap.String("logFileDir", s.logFileDir),
				zap.Error(err))
		}
		s.currFragment.Close()
		s.currFragment = nil
	}

	logger.Ctx(ctx).Info("Closing DiskSegmentImpl", zap.String("logFileDir", s.logFileDir))

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
	fragments []*FragmentFileReader // LogFile cached fragments in order
}

func NewRODiskSegmentImpl(ctx context.Context, logId int64, segId int64, basePath string, options ...ROption) (*RODiskSegmentImpl, error) {
	// Set default configuration
	rs := &RODiskSegmentImpl{
		logId:        logId,
		segmentId:    segId,
		logFileDir:   basePath,
		fragmentSize: 128 * 1024 * 1024, // Default 128MB
		fragments:    make([]*FragmentFileReader, 0),
	}

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
func (rs *RODiskSegmentImpl) AppendAsync(ctx context.Context, entryId int64, value []byte, resultCh chan<- int64) (int64, error) {
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
	reader := &DiskReader{
		ctx:       ctx,
		logFile:   rs,
		batchSize: rs.readBatchSize,
		//fragments:       fragments,
		currFragmentIdx: 0,
		currFragment:    nil,
		currEntryID:     opt.StartSequenceNum,
		endEntryID:      opt.EndSequenceNum,
		closed:          false,
	}
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
	segmentId := fmt.Sprintf("%d", rs.segmentId)

	// Calculate total size and return the last fragment
	totalSize := int64(0)
	lastFragment := rs.fragments[len(rs.fragments)-1]

	// Calculate sum of all fragment sizes
	for _, frag := range rs.fragments {
		size := frag.GetSize()
		totalSize += size
	}

	// Update metrics
	metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "load", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "load", "success").Observe(float64(time.Since(startTime).Milliseconds()))

	// FragmentManager is responsible for managing fragment lifecycle, no need to release manually
	return totalSize, lastFragment, nil
}

// Deprecated
// Merge merges log file fragments
func (rs *RODiskSegmentImpl) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "Merge")
	defer sp.End()
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	if len(rs.fragments) == 0 {
		return nil, nil, nil, nil
	}

	// TODO should be config
	// file max size, default 128MB
	fileMaxSize := 128_000_000
	mergedFrags := make([]storage.Fragment, 0)
	mergedFragId := int64(0)
	entryOffset := make([]int32, 0)
	fragmentIdOffset := make([]int32, 0)

	totalMergeSize := 0
	pendingMergeSize := 0
	pendingMergeFrags := make([]*FragmentFileReader, 0)

	// No need to define cleanup function, because FragmentManager is responsible for managing fragments lifecycle

	// load all fragment in memory
	for _, frag := range rs.fragments {
		loadFragErr := frag.Load(ctx)
		if loadFragErr != nil {
			return nil, nil, nil, loadFragErr
		}

		pendingMergeFrags = append(pendingMergeFrags, frag)
		pendingMergeSize += int(frag.fileSize) // TODO should get fragment actual accurate data size, including header/footer/index/data actual data size.
		if pendingMergeSize >= fileMaxSize {
			// merge immediately
			mergedFrag, mergeErr := mergeFragmentsAndReleaseAfterCompleted(ctx, getMergedFragmentFileKey(rs.logFileDir, mergedFragId), rs.logId, rs.segmentId, mergedFragId, pendingMergeSize, pendingMergeFrags)
			if mergeErr != nil {
				return nil, nil, nil, mergeErr
			}
			mergedFrags = append(mergedFrags, mergedFrag)
			mergedFragId++
			mergedFragFirstEntryId, _ := mergedFrag.GetFirstEntryId(ctx)
			entryOffset = append(entryOffset, int32(mergedFragFirstEntryId))
			fragmentIdOffset = append(fragmentIdOffset, int32(pendingMergeFrags[0].fragmentId))
			pendingMergeFrags = make([]*FragmentFileReader, 0)
			totalMergeSize += pendingMergeSize
			pendingMergeSize = 0
		}
	}
	if pendingMergeSize > 0 && len(pendingMergeFrags) > 0 {
		// merge immediately
		mergedFrag, mergeErr := mergeFragmentsAndReleaseAfterCompleted(ctx, getMergedFragmentFileKey(rs.logFileDir, mergedFragId), rs.logId, rs.segmentId, mergedFragId, pendingMergeSize, pendingMergeFrags)
		if mergeErr != nil {
			return nil, nil, nil, mergeErr
		}
		mergedFrags = append(mergedFrags, mergedFrag)
		mergedFragId++
		mergedFragFirstEntryId, _ := mergedFrag.GetFirstEntryId(ctx)
		entryOffset = append(entryOffset, int32(mergedFragFirstEntryId))
		fragmentIdOffset = append(fragmentIdOffset, int32(pendingMergeFrags[0].fragmentId))
		pendingMergeFrags = make([]*FragmentFileReader, 0)
		totalMergeSize += pendingMergeSize
		pendingMergeSize = 0
	}

	return mergedFrags, entryOffset, fragmentIdOffset, nil
}

// Close closes the log file and releases resources
func (rs *RODiskSegmentImpl) Close(ctx context.Context) error {
	return nil
}

// fetchROFragments returns all exists fragments in the log file, which is readonly
func (rs *RODiskSegmentImpl) fetchROFragments(ctx context.Context) (bool, *FragmentFileReader, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	startTime := time.Now()
	logId := fmt.Sprintf("%d", rs.logId)
	segmentId := fmt.Sprintf("%d", rs.segmentId)

	var fetchedLastFragment *FragmentFileReader = nil
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
		fragment, err := NewFragmentFileReader(ctx, fragmentPath, fileInfo.Size(), rs.logId, rs.segmentId, fragId)
		if err != nil {
			metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "fetch_fragments", "error").Inc()
			metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "fetch_fragments", "error").Observe(float64(time.Since(startTime).Milliseconds()))
			break
		}
		if !fragment.IsMMapReadable(context.TODO()) {
			logger.Ctx(ctx).Warn("found a new fragment unreadable", zap.String("logFileDir", rs.logFileDir), zap.String("fragmentPath", fragmentPath), zap.Error(err))
			metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "fetch_fragments", "error").Inc()
			metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "fetch_fragments", "error").Observe(float64(time.Since(startTime).Milliseconds()))
			break
		}

		fetchedLastFragment = fragment
		rs.fragments = append(rs.fragments, fragment)
		existsNewFragment = true
		fragId++
		logger.Ctx(ctx).Debug("fetch fragment info", zap.String("logFileDir", rs.logFileDir), zap.Int64("lastFragId", fragId-1))
	}

	// Update metrics
	metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "fetch_fragments", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "fetch_fragments", "success").Observe(float64(time.Since(startTime).Milliseconds()))

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
	return getFragmentFileLastEntryIdWithoutDataLoadedIfPossible(context.TODO(), lastFragment)
}

// findFragmentFrom returns the fragment index and fragment object
func (rs *RODiskSegmentImpl) findFragmentFrom(ctx context.Context, fromIdx int, currEntryID int64, endEntryID int64) (int, *FragmentFileReader, int64, error) {
	rs.mu.RLock()
	pendingReadEntryID := currEntryID
	lastIdx := fromIdx
	for i := fromIdx; i < len(rs.fragments); i++ {
		lastIdx = i
		fragment := rs.fragments[i]
		firstID, err := getFragmentFileFirstEntryIdWithoutDataLoadedIfPossible(context.TODO(), fragment)
		if err != nil {
			rs.mu.RUnlock()
			logger.Ctx(ctx).Warn("get fragment firstEntryId failed",
				zap.String("logFileDir", rs.logFileDir),
				zap.String("fragmentFile", fragment.filePath),
				zap.Error(err))
			return -1, nil, -1, err
		}
		lastID, err := getFragmentFileLastEntryIdWithoutDataLoadedIfPossible(context.TODO(), fragment)
		if err != nil {
			rs.mu.RUnlock()
			logger.Ctx(ctx).Warn("get fragment lastEntryId failed",
				zap.String("logFileDir", rs.logFileDir),
				zap.String("fragmentFile", fragment.filePath),
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
	segmentId := fmt.Sprintf("%d", rs.segmentId)

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
		metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "delete_fragments", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "delete_fragments", "error").Observe(float64(time.Since(startTime).Milliseconds()))
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
				logger.Ctx(ctx).Debug("Successfully deleted fragment file",
					zap.String("fragmentPath", fragmentPath))
				deletedCount++
			}
		}
	}

	// clear state
	rs.fragments = make([]*FragmentFileReader, 0)

	// Update metrics
	if len(deleteErrors) > 0 {
		metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "delete_fragments", "error").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "delete_fragments", "error").Observe(float64(time.Since(startTime).Milliseconds()))
	} else {
		metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "delete_fragments", "success").Inc()
		metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "delete_fragments", "success").Observe(float64(time.Since(startTime).Milliseconds()))
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

// DiskReader implements the Reader interface
type DiskReader struct {
	ctx             context.Context
	logFile         *RODiskSegmentImpl
	batchSize       int64               // Max batch size
	currFragmentIdx int                 // Current fragment index
	currFragment    *FragmentFileReader // Current fragment reader
	currEntryID     int64               // Current entry ID being read
	endEntryID      int64               // End ID (not included)
	closed          bool                // Whether closed
}

// HasNext returns true if there are more entries to read
func (dr *DiskReader) HasNext(ctx context.Context) (bool, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "HasNext")
	defer sp.End()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", dr.logFile.logId)
	segmentId := fmt.Sprintf("%d", dr.logFile.segmentId)

	if dr.closed {
		logger.Ctx(ctx).Debug("No more entries to read, current reader is closed", zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID))
		return false, nil
	}

	// If reached endID, return false
	if dr.endEntryID > 0 && dr.currEntryID >= dr.endEntryID {
		logger.Ctx(ctx).Debug("No more entries to read, reach the end entryID", zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID))
		return false, nil
	}

	// current fragment contains this entry, fast return
	if dr.currFragment != nil {
		first, err := getFragmentFileFirstEntryIdWithoutDataLoadedIfPossible(context.TODO(), dr.currFragment)
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to get first entry id", zap.String("fragmentFile", dr.currFragment.filePath), zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID), zap.Error(err))
			return false, err
		}
		last, err := getFragmentFileLastEntryIdWithoutDataLoadedIfPossible(context.TODO(), dr.currFragment)
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to get last entry id", zap.String("fragmentFile", dr.currFragment.filePath), zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID), zap.Error(err))
			return false, err
		}
		// fast return if current entry is in this current fragment
		if dr.currEntryID >= first && dr.currEntryID < last {
			return true, nil
		}
	}

	idx, f, pendingReadEntryID, err := dr.logFile.findFragmentFrom(ctx, dr.currFragmentIdx, dr.currEntryID, dr.endEntryID)
	if err != nil {
		return false, err
	}
	if f == nil {
		// no more fragment
		return false, nil
	}
	dr.currFragmentIdx = idx
	dr.currFragment = f
	dr.currEntryID = pendingReadEntryID
	metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "has_next", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "has_next", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return true, nil
}

func (dr *DiskReader) ReadNextBatch(ctx context.Context, size int64) ([]*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "ReadNextBatch")
	defer sp.End()
	if size != -1 {
		// TODO add batch size limit.
		return nil, werr.ErrNotSupport.WithCauseErrMsg("custom batch size not supported currently")
	}

	startTime := time.Now()
	logId := fmt.Sprintf("%d", dr.logFile.logId)
	segmentId := fmt.Sprintf("%d", dr.logFile.segmentId)
	if dr.closed {
		return nil, errors.New("reader is closed")
	}
	if dr.currFragment == nil {
		return nil, errors.New("no readable Fragment")
	}

	// Get current fragment lastEntryId
	lastID, err := getFragmentFileLastEntryIdWithoutDataLoadedIfPossible(context.TODO(), dr.currFragment)
	if err != nil {
		return nil, err
	}

	// read a fragment as a batch
	if dr.currFragment == nil {
		return nil, errors.New("no readable Fragment")
	}
	loadErr := dr.currFragment.Load(context.TODO())
	if loadErr != nil {
		return nil, loadErr
	}
	defer dr.currFragment.Release(ctx)
	entries := make([]*proto.LogEntry, 0, 32)
	maxSize := dr.batchSize
	readSize := int64(0)
	for {
		// Read data from current fragment
		data, err := dr.currFragment.GetEntry(ctx, dr.currEntryID)
		if err != nil {
			// If current entryID not in fragment, may need to move to next fragment
			logger.Ctx(ctx).Warn("Failed to read entry",
				zap.Int64("entryId", dr.currEntryID),
				zap.String("fragmentFile", dr.currFragment.filePath),
				zap.Error(err))
			return nil, err
		}

		// Ensure data length is reasonable
		if len(data) < 8 {
			logger.Ctx(ctx).Warn("Invalid data format: data too short",
				zap.Int64("entryId", dr.currEntryID),
				zap.Int("dataLength", len(data)))
			return nil, fmt.Errorf("invalid data format for entry %d: data too short", dr.currEntryID)
		}

		logger.Ctx(ctx).Debug("Data read complete",
			zap.Int64("entryId", dr.currEntryID),
			zap.Int64("fragmentId", dr.currFragment.fragmentId),
			zap.String("fragmentPath", dr.currFragment.filePath))

		// Extract entryID and actual data
		actualID := int64(binary.LittleEndian.Uint64(data[:8]))
		actualData := data[8:]

		// Ensure read ID matches expected ID
		if actualID != dr.currEntryID {
			logger.Ctx(ctx).Warn("EntryID mismatch",
				zap.Int64("expectedId", dr.currEntryID),
				zap.Int64("actualId", actualID))
		}

		// Create LogEntry
		entry := &proto.LogEntry{
			EntryId: actualID,
			Values:  actualData,
		}
		entries = append(entries, entry)

		// Move to next ID
		dr.currEntryID++

		// If beyond current fragment range, prepare to move to next fragment
		if dr.currEntryID > lastID {
			break
		}
		// If read size exceeds max size, stop reading
		readSize += int64(len(data))
		if readSize >= maxSize {
			break
		}
	}

	metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "read_next", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "read_next", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return entries, nil
}

// ReadNext reads the next entry
func (dr *DiskReader) ReadNext(ctx context.Context) (*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "ReadNext")
	defer sp.End()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", dr.logFile.logId)
	segmentId := fmt.Sprintf("%d", dr.logFile.segmentId)
	if dr.closed {
		return nil, errors.New("reader is closed")
	}

	// Get current fragment
	lastID, err := getFragmentFileLastEntryIdWithoutDataLoadedIfPossible(context.TODO(), dr.currFragment)
	if err != nil {
		return nil, err
	}

	// Read data from current fragment
	if dr.currFragment == nil {
		return nil, errors.New("no readable Fragment")
	}
	loadErr := dr.currFragment.Load(context.TODO())
	if loadErr != nil {
		return nil, loadErr
	}
	defer dr.currFragment.Release(ctx)
	data, err := dr.currFragment.GetEntry(ctx, dr.currEntryID)
	if err != nil {
		// If current entryID not in fragment, may need to move to next fragment
		logger.Ctx(ctx).Warn("Failed to read entry",
			zap.Int64("entryId", dr.currEntryID),
			zap.String("fragmentFile", dr.currFragment.filePath),
			zap.Error(err))
		return nil, err
	}

	// Ensure data length is reasonable
	if len(data) < 8 {
		logger.Ctx(ctx).Warn("Invalid data format: data too short",
			zap.Int64("entryId", dr.currEntryID),
			zap.Int("dataLength", len(data)))
		return nil, fmt.Errorf("invalid data format for entry %d: data too short", dr.currEntryID)
	}

	logger.Ctx(ctx).Debug("Data read complete",
		zap.Int64("entryId", dr.currEntryID),
		zap.Int64("fragmentId", dr.currFragment.fragmentId),
		zap.String("fragmentPath", dr.currFragment.filePath))

	// Extract entryID and actual data
	actualID := int64(binary.LittleEndian.Uint64(data[:8]))
	actualData := data[8:]

	// Ensure read ID matches expected ID
	if actualID != dr.currEntryID {
		logger.Ctx(ctx).Warn("EntryID mismatch",
			zap.Int64("expectedId", dr.currEntryID),
			zap.Int64("actualId", actualID))
	}

	// Create LogEntry
	entry := &proto.LogEntry{
		EntryId: actualID,
		Values:  actualData,
	}

	// Move to next ID
	dr.currEntryID++

	// If beyond current fragment range, prepare to move to next fragment
	if dr.currEntryID > lastID {
		dr.currFragmentIdx++
	}

	metrics.WpFileOperationsTotal.WithLabelValues(logId, segmentId, "read_next", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, segmentId, "read_next", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return entry, nil
}

// Close closes the reader
func (dr *DiskReader) Close() error {
	if dr.closed {
		return nil
	}

	// No need to explicitly release fragments, they are managed by FragmentManager
	// Fragments are now managed by the FragmentManager, no need to release here
	//dr.fragments = nil
	dr.currFragment = nil
	dr.currFragmentIdx = 0
	dr.logFile = nil

	dr.closed = true
	return nil
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

func getFragmentFileFirstEntryIdWithoutDataLoadedIfPossible(ctx context.Context, fragment *FragmentFileReader) (int64, error) {
	firstEntryId, err := fragment.GetFirstEntryId(ctx)
	if werr.ErrFragmentInfoNotFetched.Is(err) {
		loadErr := fragment.Load(ctx)
		if loadErr != nil {
			return -1, loadErr
		}
		firstEntryId, err = fragment.GetFirstEntryId(ctx)
		fragment.Release(ctx)
		if err != nil {
			return -1, err
		}
	}
	return firstEntryId, nil
}

func getFragmentFileLastEntryIdWithoutDataLoadedIfPossible(ctx context.Context, fragment *FragmentFileReader) (int64, error) {
	lastEntryId, err := fragment.GetLastEntryId(ctx)
	if err != nil && werr.ErrFragmentInfoNotFetched.Is(err) {
		loadErr := fragment.Load(ctx)
		if loadErr != nil {
			return -1, loadErr
		}
		lastEntryId, err = fragment.GetFetchedLastEntryId(ctx)
		fragment.Release(ctx)
		if err != nil {
			return -1, err
		}
		return lastEntryId, nil
	}
	return lastEntryId, nil
}

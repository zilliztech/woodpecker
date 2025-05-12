package disk

import (
	"context"
	"encoding/binary"
	"fmt"
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

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
)

var _ storage.LogFile = (*DiskLogFile)(nil)

// DiskLogFile is used to write data to disk-based storage as a logical file
type DiskLogFile struct {
	mu           sync.RWMutex
	id           int64
	basePath     string
	currFragment *FragmentFileWriter

	// Configuration parameters
	fragmentSize    int64 // Maximum size of each fragment
	maxEntryPerFile int   // Maximum number of entries per fragment

	// State
	lastFragmentID atomic.Int64
	lastEntryID    atomic.Int64
	firstEntryID   atomic.Int64

	// Use SequentialBuffer to store entries within the window
	buffer        atomic.Pointer[cache.SequentialBuffer]
	maxBufferSize int64                // Maximum buffer size (bytes)
	maxIntervalMs int                  // Maximum interval between syncs
	lastSync      atomic.Int64         // Last sync time
	syncedChan    map[int64]chan int64 // Channels for sync completion

	autoSync bool // Whether to automatically sync data

	// For async writes and control
	closed  bool
	closeCh chan struct{}
}

// NewDiskLogFile creates a new DiskLogFile instance
func NewDiskLogFile(id int64, basePath string, options ...Option) (*DiskLogFile, error) {
	// Set default configuration
	dlf := &DiskLogFile{
		id:              id,
		basePath:        filepath.Join(basePath, fmt.Sprintf("log_%d", id)),
		fragmentSize:    128 * 1024 * 1024, // Default 128MB
		maxEntryPerFile: 100000,            // Default 100k entries per flush
		maxBufferSize:   32 * 1024 * 1024,  // Default 32MB buffer
		maxIntervalMs:   1000,              // Default 1s
		syncedChan:      make(map[int64]chan int64),
		closeCh:         make(chan struct{}),
		autoSync:        true,
	}
	dlf.lastSync.Store(time.Now().UnixMilli())
	dlf.firstEntryID.Store(-1)
	dlf.lastEntryID.Store(-1)
	dlf.lastFragmentID.Store(-1)

	// Apply options
	for _, opt := range options {
		opt(dlf)
	}

	// Create log directory
	if err := os.MkdirAll(dlf.basePath, 0755); err != nil {
		return nil, err
	}

	// writer should open an empty dir too begin
	err := dlf.checkDirIsEmpty()
	if err != nil {
		return nil, err
	}

	newBuffer := cache.NewSequentialBuffer(dlf.lastEntryID.Load()+1, 10000) // Default cache for 10000 entries
	dlf.buffer.Store(newBuffer)

	// Start periodic sync goroutine
	if dlf.autoSync {
		go dlf.run()
	}

	logger.Ctx(context.Background()).Info("NewDiskLogFile", zap.String("basePath", dlf.basePath), zap.Int64("id", dlf.id))
	return dlf, nil
}

// run performs periodic sync operations, similar to the sync mechanism in objectstorage
func (dlf *DiskLogFile) run() {
	// Timer
	ticker := time.NewTicker(500 * time.Millisecond) // Sync every 500ms
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if closed
			if dlf.closed {
				return
			}

			// Check last sync time to avoid too frequent syncs
			if time.Now().UnixMilli()-dlf.lastSync.Load() < 200 {
				continue
			}

			err := dlf.Sync(context.Background())
			if err != nil {
				logger.Ctx(context.Background()).Warn("disk log file sync error",
					zap.String("basePath", dlf.basePath),
					zap.Int64("logFileId", dlf.id),
					zap.Error(err))
			}
			ticker.Reset(time.Duration(500 * int(time.Millisecond)))
		case <-dlf.closeCh:
			logger.Ctx(context.Background()).Info("run: received close signal, exiting goroutine")
			// Try to sync remaining data
			if err := dlf.Sync(context.Background()); err != nil {
				logger.Ctx(context.Background()).Warn("sync failed during close",
					zap.String("basePath", dlf.basePath),
					zap.Int64("logFileId", dlf.id),
					zap.Error(err))
			}
			// Close current fragment
			if dlf.currFragment != nil {
				if err := dlf.currFragment.Release(); err != nil {
					logger.Ctx(context.Background()).Warn("failed to close fragment",
						zap.String("basePath", dlf.basePath),
						zap.Int64("logFileId", dlf.id),
						zap.Error(err))
					return
				}
				dlf.currFragment.Close()
				dlf.currFragment = nil
			}
			logger.Ctx(context.Background()).Info("DiskLogFile successfully closed",
				zap.String("basePath", dlf.basePath),
				zap.Int64("logFileId", dlf.id))
			return
		}
	}
}

// GetId returns the log file ID
func (dlf *DiskLogFile) GetId() int64 {
	return dlf.id
}

// Append synchronously appends a log entry
// Deprecated TODO
func (dlf *DiskLogFile) Append(ctx context.Context, data []byte) error {
	// Get current max ID and increment by 1
	entryId := dlf.lastEntryID.Add(1) // TODO delete this

	logger.Ctx(ctx).Debug("Append: synchronous write", zap.Int64("entryId", entryId))

	// Use AppendAsync for asynchronous write
	_, resultCh, err := dlf.AppendAsync(ctx, entryId, data)
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
func (dlf *DiskLogFile) AppendAsync(ctx context.Context, entryId int64, value []byte) (int64, <-chan int64, error) {
	logger.Ctx(ctx).Debug("AppendAsync: attempting to write", zap.Int64("entryId", entryId), zap.Int("dataLength", len(value)), zap.String("logFileInst", fmt.Sprintf("%p", dlf)))

	// Handle closed file
	if dlf.closed {
		logger.Ctx(ctx).Debug("AppendAsync: failed - file is closed")
		return -1, nil, errors.New("diskLogFile closed")
	}

	// Create result channel
	ch := make(chan int64, 1)

	// First check if ID already exists in synced data
	lastId := dlf.lastEntryID.Load()
	if entryId <= lastId {
		logger.Ctx(ctx).Debug("AppendAsync: ID already exists, returning success", zap.Int64("entryId", entryId))
		// For data already written to disk, don't try to rewrite, just return success
		ch <- entryId
		close(ch)
		return entryId, ch, nil
	}

	dlf.mu.Lock()
	currentBuffer := dlf.buffer.Load()
	// Write to buffer
	id, err := currentBuffer.WriteEntry(entryId, value)
	if err != nil {
		logger.Ctx(ctx).Debug("AppendAsync: writing to buffer failed", zap.Error(err))
		ch <- -1
		close(ch)
		dlf.mu.Unlock()
		return -1, ch, err
	}
	// Save to pending sync channels
	dlf.syncedChan[id] = ch
	logger.Ctx(ctx).Debug("AppendAsync: successfully written to buffer", zap.Int64("id", id), zap.Int64("expectedNextEntryId", currentBuffer.ExpectedNextEntryId.Load()))
	dlf.mu.Unlock()

	// Check if sync needs to be triggered
	dataSize := currentBuffer.DataSize.Load()
	if dataSize >= int64(dlf.maxBufferSize) {
		logger.Ctx(ctx).Debug("reach max buffer size, trigger flush",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id),
			zap.Int64("bufferSize", dataSize),
			zap.Int64("maxSize", int64(dlf.maxBufferSize)))
		syncErr := dlf.Sync(ctx)
		if syncErr != nil {
			logger.Ctx(ctx).Warn("reach max buffer size, but trigger flush failed",
				zap.String("basePath", dlf.basePath),
				zap.Int64("logFileId", dlf.id),
				zap.Int64("bufferSize", dataSize),
				zap.Int64("maxSize", int64(dlf.maxBufferSize)),
				zap.Error(syncErr))
		}
	}
	return id, ch, nil
}

// Sync syncs the log file to disk.
func (dlf *DiskLogFile) Sync(ctx context.Context) error {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()
	defer func() {
		dlf.lastSync.Store(time.Now().UnixMilli())
	}()
	currentBuffer := dlf.buffer.Load()

	logger.Ctx(ctx).Debug("Try sync if necessary",
		zap.Int("bufferSize", len(currentBuffer.Values)),
		zap.Int64("firstEntryId", currentBuffer.FirstEntryId),
		zap.Int64("expectedNextEntryId", currentBuffer.ExpectedNextEntryId.Load()),
		zap.String("logFileInst", fmt.Sprintf("%p", dlf)))

	toFlushData, toFlushDataFirstEntryId := dlf.getToFlushData(ctx, currentBuffer)
	if len(toFlushData) == 0 {
		// no data to flush
		return nil
	}

	// Write data to fragment
	var originWrittenEntryID int64 = dlf.lastEntryID.Load()
	writeError := dlf.flushData(ctx, toFlushData, toFlushDataFirstEntryId)
	afterFlushEntryID := dlf.lastEntryID.Load()

	// Process result notifications
	if originWrittenEntryID == afterFlushEntryID {
		// No entries were successfully written, notify all channels of write failure, let clients retry
		// no flush success, callback all append sync error
		for syncingId, ch := range dlf.syncedChan {
			// append error
			ch <- -1
			delete(dlf.syncedChan, syncingId)
			close(ch)
			logger.Ctx(ctx).Warn("Call Sync, but flush failed",
				zap.String("basePath", dlf.basePath),
				zap.Int64("logFileId", dlf.id),
				zap.Int64("syncingId", syncingId),
				zap.Error(writeError))
		}
		// reset buffer as empty
		currentBuffer.Reset()
	} else if originWrittenEntryID < afterFlushEntryID {
		if writeError == nil { // Indicates all successful
			restDataFirstEntryId := currentBuffer.ExpectedNextEntryId.Load()
			restData, err := currentBuffer.ReadEntriesToLast(restDataFirstEntryId)
			if err != nil {
				logger.Ctx(ctx).Error("Call Sync, but ReadEntriesToLast failed",
					zap.Int64("logFileId", dlf.id),
					zap.Error(err))
				return err
			}
			newBuffer := cache.NewSequentialBufferWithData(restDataFirstEntryId, 10000, restData)
			dlf.buffer.Store(newBuffer)

			// notify all waiting channels
			for syncingId, ch := range dlf.syncedChan {
				if syncingId < restDataFirstEntryId {
					ch <- syncingId
					delete(dlf.syncedChan, syncingId)
					close(ch)
				}
			}
		} else { // Indicates partial success
			restDataFirstEntryId := afterFlushEntryID + 1
			for syncingId, ch := range dlf.syncedChan {
				if syncingId < restDataFirstEntryId { // Notify success for entries flushed to disk
					// append success
					ch <- syncingId
					delete(dlf.syncedChan, syncingId)
					close(ch)
				} else { // Notify failure for entries not flushed to disk
					// append error
					ch <- -1
					delete(dlf.syncedChan, syncingId)
					close(ch)
					logger.Ctx(ctx).Warn("Call Sync, but flush failed",
						zap.String("basePath", dlf.basePath),
						zap.Int64("logFileId", dlf.id),
						zap.Int64("syncingId", syncingId),
						zap.Error(writeError))
				}
			}
			// Need to recreate buffer to allow client retry
			// new a empty buffer
			newBuffer := cache.NewSequentialBuffer(restDataFirstEntryId, int64(10000)) // TODO config
			dlf.buffer.Store(newBuffer)
		}
	}

	// Update lastEntryID
	if originWrittenEntryID < afterFlushEntryID {
		logger.Ctx(ctx).Debug("Sync completed, lastEntryID updated",
			zap.Int64("from", originWrittenEntryID),
			zap.Int64("to", afterFlushEntryID),
			zap.String("filePath", dlf.currFragment.filePath))
	}

	logger.Ctx(ctx).Debug("Sync completed")
	return nil
}

func (dlf *DiskLogFile) getToFlushData(ctx context.Context, currentBuffer *cache.SequentialBuffer) ([][]byte, int64) {
	entryCount := len(currentBuffer.Values)
	if entryCount == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id))
		return nil, -1
	}

	expectedNextEntryId := currentBuffer.ExpectedNextEntryId.Load()
	dataSize := currentBuffer.DataSize.Load()

	// Check if there is data that needs to be flushed
	if expectedNextEntryId-currentBuffer.FirstEntryId == 0 {
		logger.Ctx(ctx).Debug("Call Sync, expected id not received yet, skip ... ",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id),
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
		logger.Ctx(ctx).Error("Call Sync, but ReadEntriesRange failed",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id),
			zap.Error(err))
		return nil, -1
	}
	toFlushDataFirstEntryId := currentBuffer.FirstEntryId
	logger.Ctx(ctx).Debug("Sync data to be written", zap.Int("count", len(toFlushData)), zap.Int64("startingID", toFlushDataFirstEntryId))
	return toFlushData, toFlushDataFirstEntryId
}

func (dlf *DiskLogFile) flushData(ctx context.Context, toFlushData [][]byte, toFlushDataFirstEntryId int64) error {
	var writeError error
	var lastWrittenToBuffEntryID int64 = dlf.lastEntryID.Load()
	var pendingFlushBytes int = 0

	// Ensure fragment is created
	if dlf.currFragment == nil {
		logger.Ctx(ctx).Debug("Sync needs to create a new fragment")
		if err := dlf.rotateFragment(dlf.lastEntryID.Load() + 1); err != nil {
			logger.Ctx(ctx).Debug("Sync failed to create new fragment", zap.Error(err))
			return err
		}
	}

	logger.Ctx(ctx).Debug("Sync starting to write data to fragment")
	for i, data := range toFlushData {
		if data == nil {
			// Empty data means no data or a gap, end this flush
			logger.Ctx(ctx).Warn("write entry to fragment failed, empty entry data found",
				zap.String("basePath", dlf.basePath),
				zap.Int64("logFileId", dlf.id),
				zap.Int("index", i),
				zap.Int64("toFlushDataFirstEntryId", toFlushDataFirstEntryId))
			break
		}

		// Check if current fragment is full, if so create a new one
		if dlf.needNewFragment() {
			logger.Ctx(ctx).Debug("Sync detected need for new fragment")
			// First flush current fragment to disk
			startFlush := time.Now()
			logger.Ctx(ctx).Debug("Sync flushing current fragment",
				zap.Int64("lastWrittenToBuffEntryID", lastWrittenToBuffEntryID),
				zap.Int64("lastEntryID", dlf.lastEntryID.Load()))
			if err := dlf.currFragment.Flush(ctx); err != nil {
				logger.Ctx(ctx).Warn("Sync failed to flush current fragment",
					zap.Error(err))
				writeError = err
				break
			}
			flushDuration := time.Now().Sub(startFlush)
			metrics.WpFragmentFlushBytes.WithLabelValues("0").Observe(float64(pendingFlushBytes))
			metrics.WpFragmentFlushLatency.WithLabelValues("0").Observe(float64(flushDuration.Milliseconds()))
			dlf.lastEntryID.Store(lastWrittenToBuffEntryID)
			pendingFlushBytes = 0

			// Create new fragment
			if err := dlf.rotateFragment(dlf.lastEntryID.Load() + 1); err != nil {
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
		dataWithID := append(entryIDBytes, data...)

		// Write data
		logger.Ctx(ctx).Debug("Sync writing data",
			zap.Int64("entryID", entryID),
			zap.Int("dataLen", len(data)))
		writeErr := dlf.currFragment.Write(ctx, dataWithID, entryID)
		// If fragment full error, rotate fragment and retry
		if writeErr != nil && werr.ErrDiskFragmentNoSpace.Is(writeErr) {
			startFlush := time.Now()
			logger.Ctx(ctx).Debug("Sync flushing current fragment",
				zap.Int64("lastWrittenToBuffEntryID", lastWrittenToBuffEntryID),
				zap.Int64("lastEntryID", dlf.lastEntryID.Load()))
			if flushErr := dlf.currFragment.Flush(ctx); flushErr != nil {
				logger.Ctx(ctx).Warn("Sync failed to flush current fragment",
					zap.Error(flushErr))
				writeError = flushErr
				break
			}
			flushDuration := time.Now().Sub(startFlush)
			metrics.WpFragmentFlushBytes.WithLabelValues("0").Observe(float64(pendingFlushBytes))
			metrics.WpFragmentFlushLatency.WithLabelValues("0").Observe(float64(flushDuration.Milliseconds()))
			dlf.lastEntryID.Store(lastWrittenToBuffEntryID)
			pendingFlushBytes = 0

			if rotateErr := dlf.rotateFragment(dlf.lastEntryID.Load() + 1); rotateErr != nil {
				logger.Ctx(ctx).Warn("Sync failed to create new fragment",
					zap.Error(rotateErr))
				writeError = rotateErr
				break
			}
			// Retry once
			writeErr = dlf.currFragment.Write(ctx, dataWithID, entryID)
		}

		// If still failed, abort this sync
		if writeErr != nil {
			logger.Ctx(ctx).Warn("write entry to fragment failed",
				zap.String("basePath", dlf.basePath),
				zap.Int64("logFileId", dlf.id),
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
	if lastWrittenToBuffEntryID > dlf.lastEntryID.Load() {
		startFlush := time.Now()
		logger.Ctx(ctx).Debug("Sync flushing current fragment",
			zap.Int64("lastWrittenToBuffEntryID", lastWrittenToBuffEntryID),
			zap.Int64("lastEntryID", dlf.lastEntryID.Load()))
		flushErr := dlf.currFragment.Flush(ctx)
		if flushErr != nil {
			logger.Ctx(ctx).Warn("Sync failed to flush current fragment", zap.Error(flushErr))
			writeError = flushErr
		} else {
			flushDuration := time.Now().Sub(startFlush)
			metrics.WpFragmentFlushBytes.WithLabelValues("0").Observe(float64(pendingFlushBytes))
			metrics.WpFragmentFlushLatency.WithLabelValues("0").Observe(float64(flushDuration.Milliseconds()))
			dlf.lastEntryID.Store(lastWrittenToBuffEntryID)
			pendingFlushBytes = 0
		}
	}

	return writeError
}

// needNewFragment checks if a new fragment needs to be created
func (dlf *DiskLogFile) needNewFragment() bool {
	if dlf.currFragment == nil {
		return true
	}

	// Check if fragment is closed
	if dlf.currFragment.closed {
		return true
	}

	// Check if already reached file size limit
	currentLeftSize := dlf.currFragment.indexOffset - dlf.currFragment.dataOffset
	if currentLeftSize <= 1024 { // Leave some margin to prevent overflow
		logger.Ctx(context.Background()).Debug("Need new fragment due to size limit",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id),
			zap.Uint32("currentLeftSize", currentLeftSize),
			zap.Int64("fragmentSize", dlf.fragmentSize))
		return true
	}

	// Check if reached entry count limit
	lastEntry, err := dlf.currFragment.GetLastEntryId()
	if err != nil {
		// If get failed, possibly the fragment is problematic, create a new one
		logger.Ctx(context.Background()).Warn("Cannot get last entry ID, rotating fragment",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id),
			zap.Error(err))
		return true
	}

	firstEntry, err := dlf.currFragment.GetFirstEntryId()
	if err != nil {
		// If get failed, possibly the fragment is problematic, create a new one
		logger.Ctx(context.Background()).Warn("Cannot get first entry ID, rotating fragment",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id),
			zap.Error(err))
		return true
	}

	entriesInFragment := lastEntry - firstEntry + 1
	if entriesInFragment >= int64(dlf.maxEntryPerFile) {
		logger.Ctx(context.Background()).Debug("Need new fragment due to entry count limit",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id),
			zap.Int64("entriesInFragment", entriesInFragment),
			zap.Int("maxEntryPerFile", dlf.maxEntryPerFile))
		return true
	}

	return false
}

// rotateFragment closes the current fragment and creates a new one
func (dlf *DiskLogFile) rotateFragment(fragmentFirstEntryId int64) error {
	logger.Ctx(context.Background()).Info("rotating fragment",
		zap.String("basePath", dlf.basePath),
		zap.Int64("logFileId", dlf.id),
		zap.Int64("fragmentFirstEntryId", fragmentFirstEntryId))

	// If current fragment exists, close it first
	if dlf.currFragment != nil {
		logger.Ctx(context.Background()).Debug("Sync flushing current fragment before rotate",
			zap.Int64("lastEntryID", dlf.lastEntryID.Load()))
		if err := dlf.currFragment.Flush(context.Background()); err != nil {
			return errors.Wrap(err, "flush current fragment")
		}
		logger.Ctx(context.Background()).Debug("Sync flushing current fragment after rotate",
			zap.Int64("lastEntryID", dlf.lastEntryID.Load()))

		// Release immediately frees resources
		if err := dlf.currFragment.Release(); err != nil {
			return errors.Wrap(err, "close current fragment")
		}
	}

	// Create new fragment ID
	fragmentID := dlf.lastFragmentID.Add(1)

	logger.Ctx(context.Background()).Info("creating new fragment",
		zap.String("basePath", dlf.basePath),
		zap.Int64("logFileId", dlf.id),
		zap.Int64("fragmentID", fragmentID),
		zap.Int64("firstEntryID", fragmentFirstEntryId))

	// Create new fragment
	fragmentPath := filepath.Join(dlf.basePath, fmt.Sprintf("fragment_%d", fragmentID))
	fragment, err := NewFragmentFileWriter(fragmentPath, dlf.fragmentSize, fragmentID, fragmentFirstEntryId)
	if err != nil {
		return errors.Wrapf(err, "create new fragment: %s", fragmentPath)
	}

	// Update current fragment
	dlf.currFragment = fragment
	return nil
}

// NewReader creates a new Reader instance
func (dlf *DiskLogFile) NewReader(ctx context.Context, opt storage.ReaderOpt) (storage.Reader, error) {
	return nil, werr.ErrNotSupport.WithCauseErrMsg("DiskLogFile writer support write only, cannot create reader")
}

// Load loads data from disk
func (dlf *DiskLogFile) Load(ctx context.Context) (int64, storage.Fragment, error) {
	return -1, nil, werr.ErrNotSupport.WithCauseErrMsg("not support DiskLogFile writer to load currently")
}

// Merge merges log file fragments
func (dlf *DiskLogFile) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	return nil, nil, nil, werr.ErrNotSupport.WithCauseErrMsg("not support DiskLogFile writer to merge currently")
}

func (dlf *DiskLogFile) getMergedFragmentKey(mergedFragmentId uint64) string {
	return fmt.Sprintf("%s/%d/m_%d.frag", dlf.basePath, dlf.id, mergedFragmentId)
}

func mergeFragmentsAndReleaseAfterCompleted(ctx context.Context, mergedFragPath string, mergeFragId uint64, mergeFragSize int, fragments []*FragmentFileReader) (storage.Fragment, error) {
	// check args
	if len(fragments) == 0 {
		return nil, errors.New("no fragments to merge")
	}

	// merge
	fragmentFirstEntryId := fragments[0].lastEntryID
	mergedFragmentWriter, err := NewFragmentFileWriter(mergedFragPath, int64(mergeFragSize), int64(mergeFragId), fragmentFirstEntryId)
	if err != nil {
		return nil, errors.Wrapf(err, "create new fragment: %s", mergedFragPath)
	}

	expectedEntryId := int64(-1)
	for _, fragment := range fragments {
		err := fragment.Load(ctx)
		if err != nil {
			// Merge failed, explicitly release fragment
			mergedFragmentWriter.Release()
			return nil, err
		}
		// check the order of entries
		if expectedEntryId == -1 {
			// the first segment
			expectedEntryId = fragment.lastEntryID + 1
		} else {
			if expectedEntryId != fragment.firstEntryID {
				// Merge failed, explicitly release fragments
				mergedFragmentWriter.Release()
				return nil, errors.New("fragments are not in order")
			}
			expectedEntryId = fragment.lastEntryID + 1
		}

		// merge index
		// TODO
		// Copy fragment data to merged fragment data area
		// Copy fragment index to merged fragment index area, adjusting all index lengths
	}

	// Add merged fragment to cache
	mergedFragmentWriter.Release()

	return mergedFragmentWriter, nil
}

// Close closes the log file and releases resources
func (dlf *DiskLogFile) Close() error {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	if dlf.closed {
		return nil
	}

	logger.Ctx(context.Background()).Info("Closing DiskLogFile",
		zap.Int64("id", dlf.id),
		zap.String("basePath", dlf.basePath))

	// Mark as closed to prevent new operations
	dlf.closed = true

	// Send close signal
	close(dlf.closeCh)

	return nil
}

func (dlf *DiskLogFile) checkDirIsEmpty() error {
	// Read directory contents
	entries, err := os.ReadDir(dlf.basePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(entries) > 0 {
		return werr.ErrNotSupport.WithCauseErrMsg("DiskLogFile writer to should open en empty dir")
	}
	return nil
}

// LastFragmentId returns the last fragment ID
func (dlf *DiskLogFile) LastFragmentId() uint64 {
	return uint64(dlf.lastFragmentID.Load())
}

// GetLastEntryId returns the last entry ID
func (dlf *DiskLogFile) GetLastEntryId() (int64, error) {
	return dlf.lastEntryID.Load(), nil
}

func (dlf *DiskLogFile) DeleteFragments(ctx context.Context, flag int) error {
	return werr.ErrNotSupport.WithCauseErrMsg("not support DiskLogFile writer to delete fragments currently")
}

var _ storage.LogFile = (*RODiskLogFile)(nil)

// RODiskLogFile is used to read data from disk-based storage as a logical file
type RODiskLogFile struct {
	mu       sync.RWMutex
	id       int64
	basePath string

	// Configuration parameters
	fragmentSize int64 // Maximum size of each fragment

	// State
	firstEntryID   atomic.Int64
	lastEntryID    atomic.Int64
	lastFragmentID atomic.Int64
}

// GetId returns the log file ID
func (rlf *RODiskLogFile) GetId() int64 {
	return rlf.id
}

func (dlf *RODiskLogFile) Append(ctx context.Context, data []byte) error {
	return werr.ErrNotSupport.WithCauseErrMsg("RODiskLogFile not support append")
}

// AppendAsync appends data to the log file asynchronously.
func (dlf *RODiskLogFile) AppendAsync(ctx context.Context, entryId int64, value []byte) (int64, <-chan int64, error) {
	return entryId, nil, werr.ErrNotSupport.WithCauseErrMsg("RODiskLogFile not support append")
}

// Sync syncs the log file to disk.
func (dlf *RODiskLogFile) Sync(ctx context.Context) error {
	return werr.ErrNotSupport.WithCauseErrMsg("RODiskLogFile not support sync")
}

// NewReader creates a new Reader instance
func (dlf *RODiskLogFile) NewReader(ctx context.Context, opt storage.ReaderOpt) (storage.Reader, error) {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	// TODO should use dlf instead, not scan all the time
	// Get all synced fragments
	fragments, err := dlf.getROFragments()
	if err != nil {
		return nil, err
	}

	// Create new DiskReader
	reader := &DiskReader{
		ctx:             ctx,
		fragments:       fragments,
		currFragmentIdx: 0,
		currEntryID:     opt.StartSequenceNum,
		endEntryID:      opt.EndSequenceNum,
		closed:          false,
	}
	logger.Ctx(ctx).Debug("NewReader", zap.Any("reader", reader), zap.Any("opt", opt))
	return reader, nil
}

// Load loads data from disk
func (dlf *RODiskLogFile) Load(ctx context.Context) (int64, storage.Fragment, error) {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	// Load all fragments
	fragments, err := dlf.getROFragments()
	if err != nil {
		return 0, nil, err
	}

	if len(fragments) == 0 {
		return 0, nil, nil
	}

	// Calculate total size and return the last fragment
	totalSize := int64(0)
	lastFragment := fragments[len(fragments)-1]

	// Calculate sum of all fragment sizes
	for _, frag := range fragments {
		size := frag.GetSize()
		totalSize += size
	}

	// FragmentManager is responsible for managing fragment lifecycle, no need to release manually

	return totalSize, lastFragment, nil
}

// Merge merges log file fragments
func (dlf *RODiskLogFile) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	// First sync all data to disk
	if err := dlf.Sync(ctx); err != nil {
		return nil, nil, nil, err
	}

	// Get all fragments
	fragments, err := dlf.getROFragments()
	if err != nil {
		return nil, nil, nil, err
	}

	if len(fragments) == 0 {
		return nil, nil, nil, nil
	}

	start := time.Now()
	// TODO should be config
	// file max size, default 128MB
	fileMaxSize := 128_000_000
	mergedFrags := make([]storage.Fragment, 0)
	mergedFragId := uint64(0)
	entryOffset := make([]int32, 0)
	fragmentIdOffset := make([]int32, 0)

	totalMergeSize := 0
	pendingMergeSize := 0
	pendingMergeFrags := make([]*FragmentFileReader, 0)

	// No need to define cleanup function, because FragmentManager is responsible for managing fragments lifecycle

	// load all fragment in memory
	for _, frag := range fragments {
		loadFragErr := frag.Load(ctx)
		if loadFragErr != nil {
			return nil, nil, nil, loadFragErr
		}

		pendingMergeFrags = append(pendingMergeFrags, frag)
		pendingMergeSize += int(frag.fileSize) // TODO should get fragment actual accurate data size, including header/footer/index/data actual data size.
		if pendingMergeSize >= fileMaxSize {
			// merge immediately
			mergedFrag, mergeErr := mergeFragmentsAndReleaseAfterCompleted(ctx, dlf.getMergedFragmentKey(mergedFragId), mergedFragId, pendingMergeSize, pendingMergeFrags)
			if mergeErr != nil {
				return nil, nil, nil, mergeErr
			}
			mergedFrags = append(mergedFrags, mergedFrag)
			mergedFragId++
			mergedFragFirstEntryId, _ := mergedFrag.GetFirstEntryId()
			entryOffset = append(entryOffset, int32(mergedFragFirstEntryId))
			fragmentIdOffset = append(fragmentIdOffset, int32(pendingMergeFrags[0].fragmentId))
			pendingMergeFrags = make([]*FragmentFileReader, 0)
			totalMergeSize += pendingMergeSize
			pendingMergeSize = 0
		}
	}
	if pendingMergeSize > 0 && len(pendingMergeFrags) > 0 {
		// merge immediately
		mergedFrag, mergeErr := mergeFragmentsAndReleaseAfterCompleted(ctx, dlf.getMergedFragmentKey(mergedFragId), mergedFragId, pendingMergeSize, pendingMergeFrags)
		if mergeErr != nil {
			return nil, nil, nil, mergeErr
		}
		mergedFrags = append(mergedFrags, mergedFrag)
		mergedFragId++
		mergedFragFirstEntryId, _ := mergedFrag.GetFirstEntryId()
		entryOffset = append(entryOffset, int32(mergedFragFirstEntryId))
		fragmentIdOffset = append(fragmentIdOffset, int32(pendingMergeFrags[0].fragmentId))
		pendingMergeFrags = make([]*FragmentFileReader, 0)
		totalMergeSize += pendingMergeSize
		pendingMergeSize = 0
	}
	cost := time.Now().Sub(start)
	metrics.WpCompactReqLatency.WithLabelValues(fmt.Sprintf("%d", dlf.id)).Observe(float64(cost.Milliseconds()))
	metrics.WpCompactBytes.WithLabelValues(fmt.Sprintf("%d", dlf.id)).Observe(float64(totalMergeSize))

	return mergedFrags, entryOffset, fragmentIdOffset, nil
}

func (dlf *RODiskLogFile) getMergedFragmentKey(mergedFragmentId uint64) string {
	return fmt.Sprintf("%s/%d/m_%d.frag", dlf.basePath, dlf.id, mergedFragmentId)
}

// Close closes the log file and releases resources
func (dlf *RODiskLogFile) Close() error {
	return nil
}

// getFragments returns all exists fragments in the log file, which is readonly
func (dlf *RODiskLogFile) getROFragments() ([]*FragmentFileReader, error) {
	// Read directory contents
	entries, err := os.ReadDir(dlf.basePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	// Filter out fragment files
	fragments := make([]*FragmentFileReader, 0)
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "fragment_") {
			// Extract fragmentID
			idStr := strings.TrimPrefix(entry.Name(), "fragment_")
			id, err := strconv.ParseInt(idStr, 10, 64)
			if err != nil {
				continue
			}

			// Open fragment
			fragmentPath := filepath.Join(dlf.basePath, entry.Name())
			fileInfo, err := os.Stat(fragmentPath)
			if err != nil {
				continue
			}

			// First try to get from cache
			fragment, ok := cache.GetCachedFragment(context.Background(), fragmentPath)
			if ok {
				if cachedFragReader, isReader := fragment.(*FragmentFileReader); isReader {
					// Found in cache
					fragments = append(fragments, cachedFragReader)
					continue
				}
			}

			// Create FragmentFile instance and load
			fragment, err = NewFragmentFileReader(fragmentPath, fileInfo.Size(), id) // firstEntryID will be ignored, loaded from actual file
			if err != nil {
				continue
			}

			fragments = append(fragments, fragment.(*FragmentFileReader))
		}
	}

	// Sort by fragmentID
	sort.Slice(fragments, func(i, j int) bool {
		return fragments[i].GetFragmentId() < fragments[j].GetFragmentId()
	})

	return fragments, nil
}

// Deprecated, no use
// LastFragmentId returns the last fragment ID
func (dlf *RODiskLogFile) LastFragmentId() uint64 {
	return uint64(dlf.lastFragmentID.Load())
}

// GetLastEntryId returns the last entry ID
func (dlf *RODiskLogFile) GetLastEntryId() (int64, error) {
	dlf.mu.RLock()
	defer dlf.mu.RUnlock()

	// Use lastEntryID stored in atomic variable
	lastID := dlf.lastEntryID.Load()
	if lastID >= 0 {
		return lastID, nil
	}

	// If no current fragment or get failed, try to find from all fragments
	fragments, err := dlf.getROFragments()
	if err != nil {
		return -1, err
	}

	if len(fragments) > 0 {
		// Get last entry ID from last fragment
		lastFragment := fragments[len(fragments)-1]
		fragmentLastID, err := lastFragment.GetLastEntryId()

		// FragmentManager is responsible for managing fragment lifecycle, no need to release manually

		if err == nil && fragmentLastID >= 0 {
			// Update atomic variable
			dlf.lastEntryID.Store(fragmentLastID)
			return fragmentLastID, nil
		}
	}

	// If no fragments or unable to get ID, return -1
	return -1, nil
}

func (dlf *RODiskLogFile) DeleteFragments(ctx context.Context, flag int) error {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	logger.Ctx(ctx).Info("Starting to delete fragments",
		zap.String("basePath", dlf.basePath),
		zap.Int64("logFileId", dlf.id),
		zap.Int("flag", flag))

	// 读取目录内容
	entries, err := os.ReadDir(dlf.basePath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Ctx(ctx).Info("Directory does not exist, nothing to delete",
				zap.String("basePath", dlf.basePath))
			return nil
		}
		return err
	}

	var deleteErrors []error
	deletedCount := 0

	// 筛选并删除 fragment 文件
	for _, entry := range entries {
		if !entry.IsDir() && (strings.HasPrefix(entry.Name(), "fragment_") || strings.HasPrefix(entry.Name(), "m_")) {
			fragmentPath := filepath.Join(dlf.basePath, entry.Name())

			// 从缓存中移除，使用正确的方法名
			// 从搜索结果看，应该使用 GetCachedFragment 和 RemoveFragment
			if cachedFrag, found := cache.GetCachedFragment(ctx, fragmentPath); found {
				_ = cache.RemoveCachedFragment(ctx, cachedFrag)
			}

			// 删除文件
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
	dlf.lastFragmentID.Store(-1)
	dlf.lastEntryID.Store(-1)
	dlf.firstEntryID.Store(-1)

	logger.Ctx(ctx).Info("Completed fragment deletion",
		zap.String("basePath", dlf.basePath),
		zap.Int64("logFileId", dlf.id),
		zap.Int("deletedCount", deletedCount),
		zap.Int("errorCount", len(deleteErrors)))

	if len(deleteErrors) > 0 {
		return fmt.Errorf("failed to delete %d fragment files: ", len(deleteErrors))
	}
	return nil
}

func NewRODiskLogFile(id int64, basePath string, options ...ROption) (*RODiskLogFile, error) {
	// Set default configuration
	dlf := &RODiskLogFile{
		id:           id,
		basePath:     filepath.Join(basePath, fmt.Sprintf("log_%d", id)),
		fragmentSize: 128 * 1024 * 1024, // Default 128MB
	}

	// Apply options
	for _, opt := range options {
		opt(dlf)
	}

	// Create log directory
	if err := os.MkdirAll(dlf.basePath, 0755); err != nil {
		return nil, err
	}

	// Load existing fragments
	fragments, err := dlf.getROFragments()
	if err != nil {
		return nil, err
	}

	// Initialize state from existing fragments
	if len(fragments) > 0 {
		// Find max fragment ID
		maxFragID := fragments[len(fragments)-1].GetFragmentId()
		dlf.lastFragmentID.Store(maxFragID)
		// Find max entry ID
		lastEntryID := int64(-1)
		for i := 0; i < len(fragments); i++ {
			id, err = fragments[len(fragments)-1-i].GetLastEntryId()
			if err != nil {
				return nil, err
			}
			if id > lastEntryID {
				lastEntryID = id
				break
			}
		}
		dlf.lastEntryID.Store(lastEntryID)
		// find first entry ID
		firstEntryID := int64(-1)
		for i := 0; i < len(fragments); i++ {
			id, err = fragments[i].GetLastEntryId()
			if err != nil {
				return nil, err
			}
			if id >= 0 {
				first, err := fragments[i].GetFirstEntryId()
				if err != nil {
					return nil, err
				}
				firstEntryID = first
				break
			}
		}
		dlf.firstEntryID.Store(firstEntryID)
	} else {
		dlf.firstEntryID.Store(-1)
		dlf.lastEntryID.Store(-1)
		dlf.lastFragmentID.Store(-1)
	}
	logger.Ctx(context.Background()).Info("NewRODiskLogFile", zap.String("basePath", dlf.basePath), zap.Int64("id", dlf.id))
	return dlf, nil
}

// DiskReader implements the Reader interface
type DiskReader struct {
	ctx             context.Context
	fragments       []*FragmentFileReader
	currFragmentIdx int   // Current fragment index
	currEntryID     int64 // Current entry ID being read
	endEntryID      int64 // End ID (not included)
	closed          bool  // Whether closed
}

// HasNext returns true if there are more entries to read
func (dr *DiskReader) HasNext() bool {
	if dr.closed {
		logger.Ctx(context.Background()).Debug("No more entries to read, current reader is closed", zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID))
		return false
	}

	// If reached endID, return false
	if dr.endEntryID > 0 && dr.currEntryID >= dr.endEntryID {
		logger.Ctx(context.Background()).Debug("No more entries to read", zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID))
		return false
	}

	// Find currEntryID in current and subsequent fragments
	for i := dr.currFragmentIdx; i < len(dr.fragments); i++ {
		fragment := dr.fragments[i]
		firstID, err := fragment.GetFirstEntryId()
		if err != nil {
			continue
		}
		lastID, err := fragment.GetLastEntryId()
		if err != nil {
			continue
		}

		// If currentID less than fragment's firstID, update currentID
		if dr.currEntryID < firstID {
			// If endID less than fragment's firstID, means no more data
			if dr.endEntryID > 0 && dr.endEntryID <= firstID {
				logger.Ctx(context.Background()).Debug("No more entries to read", zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID), zap.Int64("firstId", firstID), zap.Int64("lastId", lastID))
				return false
			}
			dr.currEntryID = firstID
		}

		// Check if currentID in fragment's range
		if dr.currEntryID <= lastID {
			dr.currFragmentIdx = i
			return true
		}
	}

	return false
}

// ReadNext reads the next entry
func (dr *DiskReader) ReadNext() (*proto.LogEntry, error) {
	if dr.closed {
		return nil, errors.New("reader is closed")
	}

	if !dr.HasNext() {
		return nil, errors.New("no more entries to read")
	}

	// Get current fragment
	fragment := dr.fragments[dr.currFragmentIdx]
	firstID, _ := fragment.GetFirstEntryId()
	lastID, _ := fragment.GetLastEntryId()

	// Read data from current fragment
	data, err := fragment.GetEntry(dr.currEntryID)
	if err != nil {
		// If current entryID not in fragment, may need to move to next fragment
		logger.Ctx(context.Background()).Warn("Failed to read entry, may need to try next fragment",
			zap.Int64("entryId", dr.currEntryID),
			zap.Int64("fragmentFirstId", firstID),
			zap.Int64("fragmentLastId", lastID),
			zap.Error(err))

		// Move to next ID and check if need to switch fragment
		dr.currEntryID++
		if dr.currEntryID > lastID && dr.currFragmentIdx < len(dr.fragments)-1 {
			dr.currFragmentIdx++
		}
		return dr.ReadNext() // Recursively try to read next
	}

	// Ensure data length is reasonable
	if len(data) < 8 {
		logger.Ctx(context.Background()).Warn("Invalid data format: data too short",
			zap.Int64("entryId", dr.currEntryID),
			zap.Int("dataLength", len(data)))
		return nil, fmt.Errorf("invalid data format for entry %d: data too short", dr.currEntryID)
	}

	logger.Ctx(context.Background()).Debug("Data read complete",
		zap.Int64("entryId", dr.currEntryID),
		zap.Int64("fragmentId", fragment.fragmentId),
		zap.String("fragmentPath", fragment.filePath))

	// Extract entryID and actual data
	actualID := int64(binary.LittleEndian.Uint64(data[:8]))
	actualData := data[8:]

	// Ensure read ID matches expected ID
	if actualID != dr.currEntryID {
		logger.Ctx(context.Background()).Warn("EntryID mismatch",
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
	if dr.currEntryID > lastID && dr.currFragmentIdx < len(dr.fragments)-1 {
		dr.currFragmentIdx++
	}

	return entry, nil
}

// Close closes the reader
func (dr *DiskReader) Close() error {
	if dr.closed {
		return nil
	}

	// No need to explicitly release fragments, they are managed by FragmentManager
	// Fragments are now managed by the FragmentManager, no need to release here
	dr.fragments = nil

	dr.closed = true
	return nil
}

// Option is a function type for configuration options
type Option func(*DiskLogFile)

// WithWriteFragmentSize sets the fragment size
func WithWriteFragmentSize(size int64) Option {
	return func(dlf *DiskLogFile) {
		dlf.fragmentSize = size
	}
}

func WithWriteMaxBufferSize(size int64) Option {
	return func(dlf *DiskLogFile) {
		dlf.maxBufferSize = size
	}
}

// WithWriteMaxEntryPerFile sets the maximum number of entries per fragment
func WithWriteMaxEntryPerFile(count int) Option {
	return func(dlf *DiskLogFile) {
		dlf.maxEntryPerFile = count
	}
}

func WithWriteMaxIntervalMs(interval int) Option {
	return func(dlf *DiskLogFile) {
		dlf.maxIntervalMs = interval
	}
}

func WithWriteDisableAutoSync() Option {
	return func(dlf *DiskLogFile) {
		dlf.autoSync = false
	}
}

// ROption is a function type for configuration options
type ROption func(*RODiskLogFile)

// WithReadFragmentSize sets the fragment size
func WithReadFragmentSize(size int64) ROption {
	return func(dlf *RODiskLogFile) {
		dlf.fragmentSize = size
	}
}

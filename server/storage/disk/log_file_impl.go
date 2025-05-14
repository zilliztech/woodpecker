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

var _ storage.LogFile = (*DiskLogFile)(nil)

// DiskLogFile is used to write data to disk-based storage as a logical file
type DiskLogFile struct {
	mu           sync.RWMutex
	id           int64
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
	buffer        atomic.Pointer[cache.SequentialBuffer]
	maxBufferSize int64                // Maximum buffer size (bytes)
	maxIntervalMs int                  // Maximum interval between syncs
	lastSync      atomic.Int64         // Last sync time
	syncedChan    map[int64]chan int64 // Channels for sync completion

	autoSync bool // Whether to automatically sync data

	// For async writes and control
	closed  atomic.Bool
	closeCh chan struct{}
}

// NewDiskLogFile creates a new DiskLogFile instance
func NewDiskLogFile(id int64, parentDir string, options ...Option) (*DiskLogFile, error) {
	// Set default configuration
	dlf := &DiskLogFile{
		id:              id,
		logFileDir:      filepath.Join(parentDir, fmt.Sprintf("%d", id)),
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
	dlf.closed.Store(false)

	// Apply options
	for _, opt := range options {
		opt(dlf)
	}

	// Create log directory
	if err := os.MkdirAll(dlf.logFileDir, 0755); err != nil {
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

	logger.Ctx(context.Background()).Info("NewDiskLogFile", zap.String("logFileDir", dlf.logFileDir))
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
			if dlf.closed.Load() {
				return
			}

			// Check last sync time to avoid too frequent syncs
			if time.Now().UnixMilli()-dlf.lastSync.Load() < 200 {
				continue
			}

			err := dlf.Sync(context.Background())
			if err != nil {
				logger.Ctx(context.Background()).Warn("disk log file sync error",
					zap.String("logFileDir", dlf.logFileDir),
					zap.Error(err))
			}
			ticker.Reset(time.Duration(500 * int(time.Millisecond)))
		case <-dlf.closeCh:
			logger.Ctx(context.Background()).Info("run: received close signal, exiting goroutine")
			// Try to sync remaining data
			if err := dlf.Sync(context.Background()); err != nil {
				logger.Ctx(context.Background()).Warn("sync failed during close",
					zap.String("logFileDir", dlf.logFileDir),
					zap.Error(err))
			}
			// Close current fragment
			if dlf.currFragment != nil {
				if err := dlf.currFragment.Release(); err != nil {
					logger.Ctx(context.Background()).Warn("failed to close fragment",
						zap.String("logFileDir", dlf.logFileDir),
						zap.Error(err))
					return
				}
				dlf.currFragment.Close()
				dlf.currFragment = nil
			}
			logger.Ctx(context.Background()).Info("DiskLogFile successfully closed",
				zap.String("logFileDir", dlf.logFileDir))
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
	logger.Ctx(ctx).Debug("AppendAsync: attempting to write", zap.String("logFileDir", dlf.logFileDir), zap.Int64("entryId", entryId), zap.Int("dataLength", len(value)), zap.String("logFileInst", fmt.Sprintf("%p", dlf)))

	// Handle closed file
	if dlf.closed.Load() {
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
			zap.String("logFileDir", dlf.logFileDir),
			zap.Int64("bufferSize", dataSize),
			zap.Int64("maxSize", int64(dlf.maxBufferSize)))
		syncErr := dlf.Sync(ctx)
		if syncErr != nil {
			logger.Ctx(ctx).Warn("reach max buffer size, but trigger flush failed",
				zap.String("logFileDir", dlf.logFileDir),
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
				zap.String("logFileDir", dlf.logFileDir),
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
					zap.String("logFileDir", dlf.logFileDir),
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
						zap.String("logFileDir", dlf.logFileDir),
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
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("logFileDir", dlf.logFileDir))
		return nil, -1
	}

	expectedNextEntryId := currentBuffer.ExpectedNextEntryId.Load()
	dataSize := currentBuffer.DataSize.Load()

	// Check if there is data that needs to be flushed
	if expectedNextEntryId-currentBuffer.FirstEntryId == 0 {
		logger.Ctx(ctx).Debug("Call Sync, expected id not received yet, skip ... ",
			zap.String("logFileDir", dlf.logFileDir),
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
			zap.String("logFileDir", dlf.logFileDir),
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
				zap.String("logFileDir", dlf.logFileDir),
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
				zap.String("logFileDir", dlf.logFileDir),
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
			zap.String("logFileDir", dlf.logFileDir),
			zap.Uint32("currentLeftSize", currentLeftSize),
			zap.Int64("fragmentSize", dlf.fragmentSize))
		return true
	}

	// Check if reached entry count limit
	lastEntry, err := dlf.currFragment.GetLastEntryId()
	if err != nil {
		// If get failed, possibly the fragment is problematic, create a new one
		logger.Ctx(context.Background()).Warn("Cannot get last entry ID, rotating fragment",
			zap.String("logFileDir", dlf.logFileDir),
			zap.Error(err))
		return true
	}

	firstEntry, err := dlf.currFragment.GetFirstEntryId()
	if err != nil {
		// If get failed, possibly the fragment is problematic, create a new one
		logger.Ctx(context.Background()).Warn("Cannot get first entry ID, rotating fragment",
			zap.String("logFileDir", dlf.logFileDir),
			zap.Error(err))
		return true
	}

	entriesInFragment := lastEntry - firstEntry + 1
	if entriesInFragment >= int64(dlf.maxEntryPerFile) {
		logger.Ctx(context.Background()).Debug("Need new fragment due to entry count limit",
			zap.String("logFileDir", dlf.logFileDir),
			zap.Int64("entriesInFragment", entriesInFragment),
			zap.Int("maxEntryPerFile", dlf.maxEntryPerFile))
		return true
	}

	return false
}

// rotateFragment closes the current fragment and creates a new one
func (dlf *DiskLogFile) rotateFragment(fragmentFirstEntryId int64) error {
	logger.Ctx(context.Background()).Info("rotating fragment",
		zap.String("logFileDir", dlf.logFileDir),
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
		zap.String("logFileDir", dlf.logFileDir),
		zap.Int64("fragmentID", fragmentID),
		zap.Int64("firstEntryID", fragmentFirstEntryId))

	// Create new fragment
	fragmentPath := getFragmentFileKey(dlf.logFileDir, fragmentID)
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

func mergeFragmentsAndReleaseAfterCompleted(ctx context.Context, mergedFragPath string, mergeFragId int64, mergeFragSize int, fragments []*FragmentFileReader) (storage.Fragment, error) {
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

		// TODO
		// merge index
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

	if dlf.closed.Load() {
		return nil
	}

	logger.Ctx(context.Background()).Info("Closing DiskLogFile",
		zap.String("logFileDir", dlf.logFileDir))

	// Mark as closed to prevent new operations
	dlf.closed.Store(true)

	// Send close signal
	close(dlf.closeCh)

	return nil
}

func (dlf *DiskLogFile) checkDirIsEmpty() error {
	// Read directory contents
	entries, err := os.ReadDir(dlf.logFileDir)
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

// RODiskLogFile is used to manage and read exists data from disk-based storage as a logical file
type RODiskLogFile struct {
	mu         sync.RWMutex
	id         int64 // Log file ID
	logFileDir string

	// Configuration parameters
	fragmentSize int64 // Maximum size of each fragment

	// State
	fragments []*FragmentFileReader // LogFile cached fragments in order
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
	// Create new DiskReader
	reader := &DiskReader{
		ctx:     ctx,
		logFile: dlf,
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
func (dlf *RODiskLogFile) Load(ctx context.Context) (int64, storage.Fragment, error) {
	dlf.mu.RLock()
	defer dlf.mu.RUnlock()
	if len(dlf.fragments) == 0 {
		return 0, nil, nil
	}

	// Calculate total size and return the last fragment
	totalSize := int64(0)
	lastFragment := dlf.fragments[len(dlf.fragments)-1]

	// Calculate sum of all fragment sizes
	for _, frag := range dlf.fragments {
		size := frag.GetSize()
		totalSize += size
	}

	// FragmentManager is responsible for managing fragment lifecycle, no need to release manually
	return totalSize, lastFragment, nil
}

// Merge merges log file fragments
func (dlf *RODiskLogFile) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	dlf.mu.RLock()
	defer dlf.mu.RUnlock()
	if len(dlf.fragments) == 0 {
		return nil, nil, nil, nil
	}

	start := time.Now()
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
	for _, frag := range dlf.fragments {
		loadFragErr := frag.Load(ctx)
		if loadFragErr != nil {
			return nil, nil, nil, loadFragErr
		}

		pendingMergeFrags = append(pendingMergeFrags, frag)
		pendingMergeSize += int(frag.fileSize) // TODO should get fragment actual accurate data size, including header/footer/index/data actual data size.
		if pendingMergeSize >= fileMaxSize {
			// merge immediately
			mergedFrag, mergeErr := mergeFragmentsAndReleaseAfterCompleted(ctx, getMergedFragmentFileKey(dlf.logFileDir, mergedFragId), mergedFragId, pendingMergeSize, pendingMergeFrags)
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
		mergedFrag, mergeErr := mergeFragmentsAndReleaseAfterCompleted(ctx, getMergedFragmentFileKey(dlf.logFileDir, mergedFragId), mergedFragId, pendingMergeSize, pendingMergeFrags)
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

// Close closes the log file and releases resources
func (dlf *RODiskLogFile) Close() error {
	return nil
}

// fetchROFragments returns all exists fragments in the log file, which is readonly
func (dlf *RODiskLogFile) fetchROFragments() (bool, *FragmentFileReader, error) {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()
	var fetchedLastFragment *FragmentFileReader = nil
	fragId := int64(0)
	if len(dlf.fragments) > 0 {
		lastFrag := dlf.fragments[len(dlf.fragments)-1]
		fragId = int64(lastFrag.GetFragmentId()) + 1
		fetchedLastFragment = lastFrag
	}
	existsNewFragment := false
	for {
		fragKey := getFragmentFileKey(dlf.logFileDir, fragId)
		// check if the fragment is already cached
		if frag, cached := cache.GetCachedFragment(context.TODO(), fragKey); cached {
			cachedFrag := frag.(*FragmentFileReader)
			fetchedLastFragment = cachedFrag
			dlf.fragments = append(dlf.fragments, cachedFrag)
			fragId++
			existsNewFragment = true
			continue
		}

		// check if the fragment exists
		fragmentPath := getFragmentFileKey(dlf.logFileDir, fragId)
		fileInfo, err := os.Stat(fragmentPath)
		if err != nil {
			// This file does not exist, which means there are no files later,
			// because the exists file ids always sequential and have no holes
			break
		}

		// Create FragmentFile instance and load
		fragment, err := NewFragmentFileReader(fragmentPath, fileInfo.Size(), fragId)
		if err != nil {
			break
		}
		if !fragment.isMMapReadable(context.TODO()) {
			logger.Ctx(context.TODO()).Warn("found a new fragment unreadable", zap.String("logFileDir", dlf.logFileDir), zap.String("fragmentPath", fragmentPath), zap.Error(err))
			break
		}

		fetchedLastFragment = fragment
		dlf.fragments = append(dlf.fragments, fragment)
		existsNewFragment = true
		fragId++
		logger.Ctx(context.Background()).Debug("fetch fragment info", zap.String("logFileDir", dlf.logFileDir), zap.Int64("lastFragId", fragId-1))
	}

	logger.Ctx(context.Background()).Debug("fetch fragment infos", zap.String("logFileDir", dlf.logFileDir), zap.Int("fragments", len(dlf.fragments)), zap.Int64("lastFragId", fragId-1))
	return existsNewFragment, fetchedLastFragment, nil
}

// Deprecated, no use
// LastFragmentId returns the last fragment ID
func (dlf *RODiskLogFile) LastFragmentId() uint64 {
	dlf.mu.RLock()
	defer dlf.mu.RUnlock()
	if len(dlf.fragments) == 0 {
		return 0
	}
	return uint64(dlf.fragments[len(dlf.fragments)-1].GetFragmentId())
}

// GetLastEntryId returns the last entry ID
func (dlf *RODiskLogFile) GetLastEntryId() (int64, error) {
	// prefetch fragmentInfos if any new fragment created
	_, lastFragment, err := dlf.fetchROFragments()
	if err != nil {
		logger.Ctx(context.TODO()).Warn("get fragments failed",
			zap.String("logFileDir", dlf.logFileDir),
			zap.Error(err))
		return -1, err
	}
	if lastFragment == nil {
		return -1, nil
	}
	return lastFragment.GetLastEntryId()
}

// findFragmentFrom returns the fragment index and fragment object
func (dlf *RODiskLogFile) findFragmentFrom(fromIdx int, currEntryID int64, endEntryID int64) (int, *FragmentFileReader, int64, error) {
	dlf.mu.RLock()
	pendingReadEntryID := currEntryID
	lastIdx := fromIdx
	for i := fromIdx; i < len(dlf.fragments); i++ {
		lastIdx = i
		fragment := dlf.fragments[i]
		firstID, err := fragment.GetFirstEntryId()
		if err != nil {
			dlf.mu.RUnlock()
			logger.Ctx(context.TODO()).Warn("get fragment firstEntryId failed",
				zap.String("logFileDir", dlf.logFileDir),
				zap.String("fragmentFile", fragment.filePath),
				zap.Error(err))
			return -1, nil, -1, err
		}
		lastID, err := fragment.GetLastEntryId()
		if err != nil {
			dlf.mu.RUnlock()
			logger.Ctx(context.TODO()).Warn("get fragment lastEntryId failed",
				zap.String("logFileDir", dlf.logFileDir),
				zap.String("fragmentFile", fragment.filePath),
				zap.Error(err))
			return -1, nil, -1, err
		}

		// If currentID less than fragment's firstID, update currentID
		if pendingReadEntryID < firstID {
			// If endID less than fragment's firstID, means no more data
			if endEntryID > 0 && endEntryID <= firstID {
				logger.Ctx(context.Background()).Debug("No more entries to read", zap.String("logFileDir", dlf.logFileDir), zap.Int64("currEntryId", currEntryID), zap.Int64("endEntryId", endEntryID), zap.Int64("firstId", firstID), zap.Int64("lastId", lastID))
				dlf.mu.RUnlock()
				return -1, nil, -1, nil
			}
			pendingReadEntryID = firstID
		}

		// Check if currentID in fragment's range
		if pendingReadEntryID <= lastID {
			dlf.mu.RUnlock()
			return i, fragment, pendingReadEntryID, nil
		}
	}
	dlf.mu.RUnlock()
	newFragmentExists, _, err := dlf.fetchROFragments()
	if err != nil {
		logger.Ctx(context.TODO()).Warn("fetch fragments failed",
			zap.String("logFileDir", dlf.logFileDir),
			zap.Error(err))
		return -1, nil, -1, err
	}

	if newFragmentExists {
		return dlf.findFragmentFrom(lastIdx+1, pendingReadEntryID, endEntryID)
	}
	logger.Ctx(context.Background()).Debug("No more entries to read", zap.String("logFileDir", dlf.logFileDir), zap.Int64("currEntryId", currEntryID), zap.Int64("endEntryId", endEntryID))
	return -1, nil, -1, nil
}

func (dlf *RODiskLogFile) DeleteFragments(ctx context.Context, flag int) error {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	logger.Ctx(ctx).Info("Starting to delete fragments",
		zap.String("logFileDir", dlf.logFileDir),
		zap.Int("flag", flag))

	// Read directory contents
	entries, err := os.ReadDir(dlf.logFileDir)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Ctx(ctx).Info("Directory does not exist, nothing to delete",
				zap.String("logFileDir", dlf.logFileDir))
			return nil
		}
		return err
	}

	var deleteErrors []error
	deletedCount := 0

	// Filter and delete fragment files
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".frag") {
			fragmentPath := filepath.Join(dlf.logFileDir, entry.Name())

			// Remove from cache using the correct method name
			// Based on search results, we should use GetCachedFragment and RemoveFragment
			if cachedFrag, found := cache.GetCachedFragment(ctx, fragmentPath); found {
				_ = cache.RemoveCachedFragment(ctx, cachedFrag)
			}

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
	dlf.fragments = make([]*FragmentFileReader, 0)

	logger.Ctx(ctx).Info("Completed fragment deletion",
		zap.String("logFileDir", dlf.logFileDir),
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
		logFileDir:   filepath.Join(basePath, fmt.Sprintf("%d", id)),
		fragmentSize: 128 * 1024 * 1024, // Default 128MB
		fragments:    make([]*FragmentFileReader, 0),
	}

	// Apply options
	for _, opt := range options {
		opt(dlf)
	}

	// Create log directory
	if err := os.MkdirAll(dlf.logFileDir, 0755); err != nil {
		return nil, err
	}

	// Load existing fragments
	_, _, err := dlf.fetchROFragments()
	if err != nil {
		return nil, err
	}
	logger.Ctx(context.Background()).Info("NewRODiskLogFile", zap.String("logFileDir", dlf.logFileDir), zap.Int("fragments", len(dlf.fragments)))
	return dlf, nil
}

// DiskReader implements the Reader interface
type DiskReader struct {
	ctx             context.Context
	logFile         *RODiskLogFile
	currFragmentIdx int                 // Current fragment index
	currFragment    *FragmentFileReader // Current fragment reader
	currEntryID     int64               // Current entry ID being read
	endEntryID      int64               // End ID (not included)
	closed          bool                // Whether closed
}

// HasNext returns true if there are more entries to read
func (dr *DiskReader) HasNext() (bool, error) {
	if dr.closed {
		logger.Ctx(context.Background()).Debug("No more entries to read, current reader is closed", zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID))
		return false, nil
	}

	// If reached endID, return false
	if dr.endEntryID > 0 && dr.currEntryID >= dr.endEntryID {
		logger.Ctx(context.Background()).Debug("No more entries to read, reach the end entryID", zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID))
		return false, nil
	}

	// current fragment contains this entry, fast return
	if dr.currFragment != nil {
		first, err := dr.currFragment.GetFirstEntryId()
		if err != nil {
			logger.Ctx(context.Background()).Warn("Failed to get first entry id", zap.String("fragmentFile", dr.currFragment.filePath), zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID), zap.Error(err))
			return false, err
		}
		last, err := dr.currFragment.GetLastEntryId()
		if err != nil {
			logger.Ctx(context.Background()).Warn("Failed to get last entry id", zap.String("fragmentFile", dr.currFragment.filePath), zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID), zap.Error(err))
			return false, err
		}
		// fast return if current entry is in this current fragment
		if dr.currEntryID >= first && dr.currEntryID < last {
			return true, nil
		}
	}

	idx, f, pendingReadEntryID, err := dr.logFile.findFragmentFrom(dr.currFragmentIdx, dr.currEntryID, dr.endEntryID)
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
	return true, nil
}

// ReadNext reads the next entry
func (dr *DiskReader) ReadNext() (*proto.LogEntry, error) {
	if dr.closed {
		return nil, errors.New("reader is closed")
	}

	// Get current fragment
	lastID, err := dr.currFragment.GetLastEntryId()
	if err != nil {
		return nil, err
	}

	// Read data from current fragment
	data, err := dr.currFragment.GetEntry(dr.currEntryID)
	if err != nil {
		// If current entryID not in fragment, may need to move to next fragment
		logger.Ctx(context.Background()).Warn("Failed to read entry",
			zap.Int64("entryId", dr.currEntryID),
			zap.String("fragmentFile", dr.currFragment.filePath),
			zap.Error(err))
		return nil, err
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
		zap.Int64("fragmentId", dr.currFragment.fragmentId),
		zap.String("fragmentPath", dr.currFragment.filePath))

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
	if dr.currEntryID > lastID {
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
	//dr.fragments = nil
	dr.currFragment = nil
	dr.currFragmentIdx = 0
	dr.logFile = nil

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

// utils to get fragment file key
func getFragmentFileKey(logFileDir string, fragmentId int64) string {
	return fmt.Sprintf("%s/%d.frag", logFileDir, fragmentId)
}

// utils to get merged fragment file key
func getMergedFragmentFileKey(logFileDir string, mergedFragmentId int64) string {
	return fmt.Sprintf("%s/m_%d.frag", logFileDir, mergedFragmentId)
}

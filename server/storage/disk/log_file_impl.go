package disk

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
)

var _ storage.LogFile = (*DiskLogFile)(nil)

// DiskReader implements the Reader interface for sequential reading of log entries
type DiskReader struct {
	ctx            context.Context
	fragment       *FragmentFile
	currEntryID    int64
	endEntryID     int64
	hasNext        bool
	hasInitialized bool
}

// ReadNext reads the next log entry
func (dr *DiskReader) ReadNext() (*proto.LogEntry, error) {
	if !dr.hasNext {
		return nil, io.EOF
	}

	payload, err := dr.fragment.GetEntry(dr.currEntryID)
	if err != nil {
		return nil, err
	}

	entry := &proto.LogEntry{
		EntryId: dr.currEntryID,
		Values:  payload,
	}

	// Update current entry ID
	dr.currEntryID++
	dr.hasNext = dr.currEntryID < dr.endEntryID

	return entry, nil
}

// HasNext checks if there are more entries to read
func (dr *DiskReader) HasNext() bool {
	if !dr.hasInitialized {
		dr.hasNext = dr.currEntryID < dr.endEntryID
		dr.hasInitialized = true
	}
	return dr.hasNext
}

// Close closes the reader
func (dr *DiskReader) Close() error {
	return nil
}

// DiskLogFile implements the LogFile interface for disk-based storage
type DiskLogFile struct {
	mu           sync.RWMutex
	id           int64
	basePath     string
	currFragment *FragmentFile

	// Configuration parameters
	fragmentSize    int // Maximum size of each fragment
	maxEntryPerFile int // Maximum number of entries per fragment

	// State
	lastFragmentID atomic.Int64
	lastEntryID    atomic.Int64

	// For async writes
	appendCh       chan appendRequest
	appendResultCh chan appendResult
	closed         bool
	closeCh        chan struct{}
}

type appendRequest struct {
	entryID  int64
	data     []byte
	resultCh chan int64
}

type appendResult struct {
	entryID int64
	err     error
}

// NewDiskLogFile creates a new DiskLogFile instance
func NewDiskLogFile(id int64, basePath string, options ...Option) (*DiskLogFile, error) {
	// Set default configuration
	dlf := &DiskLogFile{
		id:              id,
		basePath:        filepath.Join(basePath, fmt.Sprintf("log_%d", id)),
		fragmentSize:    4 * 1024 * 1024, // Default 4MB
		maxEntryPerFile: 100000,          // Default 100k entries per fragment
		appendCh:        make(chan appendRequest, 1000),
		appendResultCh:  make(chan appendResult, 1000),
		closeCh:         make(chan struct{}),
	}

	// Apply options
	for _, opt := range options {
		opt(dlf)
	}

	// Create base directory
	if err := os.MkdirAll(dlf.basePath, 0755); err != nil {
		return nil, errors.Wrapf(err, "failed to create directory: %s", dlf.basePath)
	}

	// Start async processing goroutine
	go dlf.processAppendRequests()

	return dlf, nil
}

// Option is a function type for configuration options
type Option func(*DiskLogFile)

// WithFragmentSize sets the fragment size
func WithFragmentSize(size int) Option {
	return func(dlf *DiskLogFile) {
		dlf.fragmentSize = size
	}
}

// WithMaxEntryPerFile sets the maximum number of entries per fragment
func WithMaxEntryPerFile(count int) Option {
	return func(dlf *DiskLogFile) {
		dlf.maxEntryPerFile = count
	}
}

// GetId returns the log file ID
func (dlf *DiskLogFile) GetId() int64 {
	return dlf.id
}

// Append synchronously appends a log entry
func (dlf *DiskLogFile) Append(ctx context.Context, data []byte) error {
	resultCh := make(chan int64, 1)
	entryID := dlf.lastEntryID.Add(1)

	select {
	case dlf.appendCh <- appendRequest{entryID: entryID, data: data, resultCh: resultCh}:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-resultCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AppendAsync asynchronously appends a log entry
func (dlf *DiskLogFile) AppendAsync(ctx context.Context, entryId int64, data []byte) (int64, <-chan int64, error) {
	if entryId <= 0 {
		entryId = dlf.lastEntryID.Add(1)
	} else {
		// Update lastEntryID
		for {
			current := dlf.lastEntryID.Load()
			if entryId <= current {
				break
			}
			if dlf.lastEntryID.CompareAndSwap(current, entryId) {
				break
			}
		}
	}

	resultCh := make(chan int64, 1)

	select {
	case dlf.appendCh <- appendRequest{entryID: entryId, data: data, resultCh: resultCh}:
		return entryId, resultCh, nil
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

// processAppendRequests processes async append requests
func (dlf *DiskLogFile) processAppendRequests() {
	for {
		select {
		case req := <-dlf.appendCh:
			dlf.mu.Lock()
			// Initialize or rotate fragment if needed
			if dlf.currFragment == nil || dlf.needNewFragment() {
				if err := dlf.rotateFragment(); err != nil {
					dlf.mu.Unlock()
					req.resultCh <- -1
					dlf.appendResultCh <- appendResult{entryID: req.entryID, err: err}
					continue
				}
			}

			// Write data
			err := dlf.currFragment.Write(context.Background(), req.data)
			dlf.mu.Unlock()

			if err != nil {
				req.resultCh <- -1
				dlf.appendResultCh <- appendResult{entryID: req.entryID, err: err}
				continue
			}

			req.resultCh <- req.entryID
		case <-dlf.closeCh:
			return
		}
	}
}

// needNewFragment checks if a new fragment needs to be created
func (dlf *DiskLogFile) needNewFragment() bool {
	if dlf.currFragment == nil {
		return true
	}

	// Check if fragment is full
	currentSize := dlf.currFragment.GetSize()
	if currentSize >= int64(dlf.fragmentSize) {
		return true
	}

	// Check if entry count limit is reached
	lastEntry, _ := dlf.currFragment.GetLastEntryId()
	firstEntry, _ := dlf.currFragment.GetFirstEntryId()
	entriesInFragment := lastEntry - firstEntry + 1
	if entriesInFragment >= int64(dlf.maxEntryPerFile) {
		return true
	}

	return false
}

// rotateFragment creates a new fragment
func (dlf *DiskLogFile) rotateFragment() error {
	// Save current fragment
	if dlf.currFragment != nil {
		if err := dlf.currFragment.Flush(context.Background()); err != nil {
			return err
		}
		if err := dlf.currFragment.Release(); err != nil {
			return err
		}
	}

	// Create new fragment ID
	fragmentID := dlf.lastFragmentID.Add(1)
	startOffset := uint64(fragmentID * 1000000) // Simple mapping to ensure uniqueness

	var err error
	dlf.currFragment, err = NewFragmentFile(
		filepath.Join(dlf.basePath, fmt.Sprintf("fragment_%d", startOffset)),
		int64(dlf.fragmentSize),
		int64(startOffset),
	)
	if err != nil {
		return err
	}

	return nil
}

// NewReader creates a new Reader instance
func (dlf *DiskLogFile) NewReader(ctx context.Context, opt storage.ReaderOpt) (storage.Reader, error) {
	dlf.mu.RLock()
	defer dlf.mu.RUnlock()

	// Return error if no current fragment
	if dlf.currFragment == nil {
		return nil, fmt.Errorf("no fragment available")
	}

	// Create reader
	reader := &DiskReader{
		ctx:         ctx,
		fragment:    dlf.currFragment,
		currEntryID: opt.StartSequenceNum,
		endEntryID:  opt.EndSequenceNum,
	}

	return reader, nil
}

// LastFragmentId returns the last fragment ID
func (dlf *DiskLogFile) LastFragmentId() uint64 {
	return uint64(dlf.lastFragmentID.Load())
}

// GetLastEntryId returns the last entry ID
func (dlf *DiskLogFile) GetLastEntryId() (int64, error) {
	dlf.mu.RLock()
	defer dlf.mu.RUnlock()

	if dlf.currFragment == nil {
		return -1, nil
	}

	return dlf.currFragment.GetLastEntryId()
}

// Sync ensures all data is written to persistent storage
func (dlf *DiskLogFile) Sync(ctx context.Context) error {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	if dlf.currFragment == nil {
		return nil
	}

	return dlf.currFragment.Flush(ctx)
}

// Merge merges log file fragments
func (dlf *DiskLogFile) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	// In local filesystem implementation, we can simply return empty values
	// Or implement actual fragment merging logic
	return nil, nil, nil, nil
}

// Load loads the log file information
func (dlf *DiskLogFile) Load(ctx context.Context) (int64, storage.Fragment, error) {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	// Find all fragment files in the directory
	fragmentFiles, err := filepath.Glob(filepath.Join(dlf.basePath, "fragment_*"))
	if err != nil {
		return 0, nil, err
	}

	if len(fragmentFiles) == 0 {
		// No fragments found, create new one
		if err := dlf.rotateFragment(); err != nil {
			return 0, nil, err
		}
		return 0, dlf.currFragment, nil
	}

	// Find the latest fragment file
	latestFragment := fragmentFiles[0]
	latestModTime := int64(0)
	for _, file := range fragmentFiles {
		info, err := os.Stat(file)
		if err != nil {
			continue
		}

		modTime := info.ModTime().UnixNano()
		if modTime > latestModTime {
			latestModTime = modTime
			latestFragment = file
		}
	}

	// Extract fragment ID
	var fragmentID int64
	_, err = fmt.Sscanf(filepath.Base(latestFragment), "fragment_%d", &fragmentID)
	if err != nil {
		fragmentID = time.Now().UnixNano()
	}

	// Update lastFragmentID
	dlf.lastFragmentID.Store(fragmentID)

	// Load fragment
	dlf.currFragment, err = NewFragmentFile(latestFragment, int64(dlf.fragmentSize), fragmentID)
	if err != nil {
		return 0, nil, err
	}

	// Load fragment data
	if err := dlf.currFragment.Load(ctx); err != nil {
		return 0, nil, err
	}

	// Get last entry ID
	lastEntryID, err := dlf.currFragment.GetLastEntryId()
	if err != nil {
		lastEntryID = 0
	}

	// Update lastEntryID
	dlf.lastEntryID.Store(lastEntryID)

	return lastEntryID, dlf.currFragment, nil
}

// Close closes the log file
func (dlf *DiskLogFile) Close() error {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	if dlf.closed {
		return nil
	}

	// Close async processing goroutine
	close(dlf.closeCh)
	dlf.closed = true

	// Close current fragment if exists
	if dlf.currFragment != nil {
		if err := dlf.currFragment.Flush(context.Background()); err != nil {
			return err
		}
		if err := dlf.currFragment.Release(); err != nil {
			return err
		}
		dlf.currFragment = nil
	}

	return nil
}

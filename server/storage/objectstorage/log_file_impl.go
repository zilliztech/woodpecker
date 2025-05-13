package objectstorage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/retry"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
)

var _ storage.LogFile = (*LogFile)(nil)

// LogFile is used to write data to object storage as a logical file
type LogFile struct {
	mu                sync.Mutex
	lastSyncTimestamp atomic.Int64
	client            minioHandler.MinioHandler
	segmentPrefixKey  string // The prefix key for the segment to which this LogFile belongs
	bucket            string // The bucket name
	id                int64  // LogFile Id in object storage

	// write buffer
	buffer           atomic.Pointer[cache.SequentialBuffer] // Write buffer
	maxBufferSize    int64                                  // Max buffer size to sync buffer to object storage
	maxIntervalMs    int                                    // Max interval to sync buffer to object storage
	syncPolicyConfig *config.LogFileSyncPolicyConfig
	syncedChan       map[int64]chan int64 // Synced entryId chan map
	fileClose        chan struct{}        // Close signal

	// written info
	firstEntryId   int64  // The first entryId of this LogFile which already written to object storage
	lastEntryId    int64  // The last entryId of this LogFile which already written to object storage
	lastFragmentId uint64 // The last fragmentId of this LogFile which already written to object storage
}

// NewLogFile is used to create a new LogFile, which is used to write data to object storage
func NewLogFile(logFileId int64, segmentPrefixKey string, bucket string, objectCli minioHandler.MinioHandler, cfg *config.Configuration) storage.LogFile {
	logger.Ctx(context.TODO()).Debug("new LogFile created", zap.Int64("logFileId", logFileId), zap.String("segmentPrefixKey", segmentPrefixKey))
	syncPolicyConfig := &cfg.Woodpecker.Logstore.LogFileSyncPolicy
	newBuffer := cache.NewSequentialBuffer(0, int64(syncPolicyConfig.MaxEntries))
	objFile := &LogFile{
		id:               logFileId,
		client:           objectCli,
		segmentPrefixKey: segmentPrefixKey,
		bucket:           bucket,

		maxBufferSize:    syncPolicyConfig.MaxBytes,
		maxIntervalMs:    syncPolicyConfig.MaxInterval,
		syncPolicyConfig: syncPolicyConfig,
		fileClose:        make(chan struct{}),
		syncedChan:       make(map[int64]chan int64),

		firstEntryId:   -1,
		lastEntryId:    -1,
		lastFragmentId: 0,
	}
	objFile.buffer.Store(newBuffer)
	go objFile.run()
	return objFile
}

// Like OS file fsync dirty pageCache periodically, objectStoreFile will sync buffer to object storage periodically
func (f *LogFile) run() {
	// time ticker
	ticker := time.NewTicker(time.Duration(f.maxIntervalMs * int(time.Millisecond)))
	defer ticker.Stop()
	f.lastSyncTimestamp.Store(time.Now().UnixMilli())
	for {
		select {
		case <-ticker.C:
			if time.Now().UnixMilli()-f.lastSyncTimestamp.Load() < int64(f.maxIntervalMs) {
				continue
			}
			err := f.Sync(context.Background())
			if err != nil {
				logger.Ctx(context.TODO()).Warn("sync error",
					zap.String("segmentPrefixKey", f.segmentPrefixKey),
					zap.Int64("logFileId", f.id),
					zap.Error(err))
			}
			ticker.Reset(time.Duration(f.maxIntervalMs * int(time.Millisecond)))
		case <-f.fileClose:
			err := f.Sync(context.Background())
			if err != nil {
				logger.Ctx(context.TODO()).Warn("sync error",
					zap.String("segmentPrefixKey", f.segmentPrefixKey),
					zap.Int64("logFileId", f.id),
					zap.Error(err))
			}
			logger.Ctx(context.TODO()).Debug("close LogFile", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.String("logFileInst", fmt.Sprintf("%p", f)))
			return
		}
	}
}

func (f *LogFile) GetId() int64 {
	return f.id
}

func (f *LogFile) AppendAsync(ctx context.Context, entryId int64, data []byte) (int64, <-chan int64, error) {
	logger.Ctx(ctx).Debug("AppendAsync: attempting to write", zap.Int64("entryId", entryId), zap.Int("dataLength", len(data)), zap.String("logFileInst", fmt.Sprintf("%p", f)))
	ch := make(chan int64, 1)
	// trigger sync by max buffer entries num
	currentBuffer := f.buffer.Load()
	pendingAppendId := currentBuffer.ExpectedNextEntryId.Load() + 1
	if pendingAppendId >= int64(currentBuffer.FirstEntryId+currentBuffer.MaxSize) {
		logger.Ctx(context.TODO()).Debug("buffer full, trigger flush",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int64("logFileId", f.id),
			zap.Int64("pendingAppendId", pendingAppendId),
			zap.Int64("bufferFirstId", currentBuffer.FirstEntryId),
			zap.Int64("bufferLastId", currentBuffer.FirstEntryId+currentBuffer.MaxSize))
		err := f.Sync(ctx)
		if err != nil {
			// sync does not success
			ch <- -1
			close(ch)
			return entryId, ch, err
		}
	}

	f.mu.Lock()
	currentBuffer = f.buffer.Load()
	// write buffer
	id, err := currentBuffer.WriteEntry(entryId, data)
	if err != nil {
		// sync does not success
		ch <- -1
		close(ch)
		f.mu.Unlock()
		return id, ch, err
	}
	f.syncedChan[id] = ch
	logger.Ctx(ctx).Debug("AppendAsync: successfully written to buffer", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("entryId", entryId), zap.Int64("id", id), zap.Int64("expectedNextEntryId", currentBuffer.ExpectedNextEntryId.Load()), zap.String("logFileInst", fmt.Sprintf("%p", f)), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
	f.mu.Unlock()

	// trigger sync by max buffer entries bytes size
	dataSize := currentBuffer.DataSize.Load()
	if dataSize >= f.maxBufferSize {
		logger.Ctx(context.TODO()).Debug("reach max buffer size, trigger flush", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Int64("bufferSize", dataSize), zap.Int64("maxSize", f.maxBufferSize))
		syncErr := f.Sync(ctx)
		if syncErr != nil {
			logger.Ctx(context.TODO()).Warn("reach max buffer size, but trigger flush failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Int64("bufferSize", dataSize), zap.Int64("maxSize", f.maxBufferSize), zap.Error(syncErr))
		}
	}
	return id, ch, nil
}

// Deprecated: use AppendAsync instead
func (f *LogFile) Append(ctx context.Context, data []byte) error {
	panic("not support sync append, it's too slow")
}

func (f *LogFile) NewReader(ctx context.Context, opt storage.ReaderOpt) (storage.Reader, error) {
	return nil, werr.ErrNotSupport.WithCauseErrMsg("LogFile writer support write only, cannot create reader")
}

// LastFragmentId returns the last fragmentId of the log file.
func (f *LogFile) LastFragmentId() uint64 {
	return f.lastFragmentId
}

func (f *LogFile) getFirstEntryId() int64 {
	return f.firstEntryId
}

func (f *LogFile) GetLastEntryId() (int64, error) {
	return f.lastEntryId, nil
}

// flushResult is the result of flush operation
type flushResult struct {
	target *FragmentObject
	err    error
}

func (f *LogFile) Sync(ctx context.Context) error {
	// Implement sync logic, e.g., flush to persistent storage
	f.mu.Lock()
	defer f.mu.Unlock()
	defer func() {
		f.lastSyncTimestamp.Store(time.Now().UnixMilli())
	}()

	currentBuffer := f.buffer.Load()

	entryCount := len(currentBuffer.Values)
	if entryCount == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		return nil
	}

	expectedNextEntryId := currentBuffer.ExpectedNextEntryId.Load()

	// get flush point to flush
	if expectedNextEntryId-currentBuffer.FirstEntryId == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		return nil
	}

	toFlushData, err := currentBuffer.ReadEntriesRange(currentBuffer.FirstEntryId, expectedNextEntryId)
	if err != nil {
		logger.Ctx(ctx).Error("Call Sync, but ReadEntriesRange failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Error(err), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
		return err
	}
	toFlushDataFirstEntryId := currentBuffer.FirstEntryId

	partitions, partitionFirstEntryIds := f.repackIfNecessary(toFlushData, toFlushDataFirstEntryId)
	concurrentCh := make(chan int, f.syncPolicyConfig.MaxFlushThreads)
	flushResultCh := make(chan *flushResult, len(partitions))
	var concurrentWg sync.WaitGroup
	logger.Ctx(ctx).Debug("get flush partitions finish", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Int("partitions", len(partitions)), zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
	lastFragmentId := f.LastFragmentId()
	for i, partition := range partitions {
		concurrentWg.Add(1)
		go func() {
			concurrentCh <- i                        // take one flush goroutine to start
			fragId := lastFragmentId + 1 + uint64(i) // fragment id
			logger.Ctx(ctx).Debug("start flush part of buffer as fragment", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Uint64("fragId", fragId))
			key := getFragmentObjectKey(f.segmentPrefixKey, f.id, fragId)
			part := partition
			fragment := NewFragmentObject(f.client, f.bucket, fragId, key, part, partitionFirstEntryIds[i], true, false, true)
			err = retry.Do(ctx,
				func() error {
					return fragment.Flush(ctx)
				},
				retry.Attempts(uint(f.syncPolicyConfig.MaxFlushRetries)),
				retry.Sleep(time.Duration(f.syncPolicyConfig.RetryInterval)*time.Millisecond),
			)
			if err != nil {
				logger.Ctx(ctx).Warn("flush part of buffer as fragment failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Uint64("fragId", fragId), zap.Error(err))
			}
			flushResultCh <- &flushResult{
				target: fragment,
				err:    err,
			}
			concurrentWg.Done() // finish a flush goroutine
			<-concurrentCh      // release a flush goroutine
			logger.Ctx(ctx).Debug("complete flush part of buffer as fragment", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Uint64("fragId", fragId))
		}()
	}

	logger.Ctx(ctx).Debug("wait for all parts of buffer to be flushed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id))
	concurrentWg.Wait() // Wait for all tasks to complete
	close(flushResultCh)
	logger.Ctx(ctx).Debug("all parts of buffer have been flushed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id))
	resultFrags := make([]*flushResult, 0)
	for r := range flushResultCh {
		resultFrags = append(resultFrags, r)
	}
	sort.Slice(resultFrags, func(i, j int) bool {
		return resultFrags[i].target.GetFragmentId() < resultFrags[j].target.GetFragmentId()
	})
	successFrags := make([]*FragmentObject, 0)
	flushedLastEntryId := int64(-1) // last entry id of the last fragment that was successfully flushed in sequence
	flushedLastFragmentId := uint64(0)
	for _, r := range resultFrags {
		first, _ := r.target.GetFirstEntryId()
		last, _ := r.target.GetLastEntryId()
		fragId := r.target.GetFragmentId()
		if r.err != nil {
			logger.Ctx(ctx).Warn("flush fragment failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Int64("fragId", fragId), zap.Int64("firstEntryId", first), zap.Int64("lastEntryId", last))
			// Can only succeed sequentially without holes
			break
		} else {
			logger.Ctx(ctx).Debug("flush fragment success", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Int64("fragId", fragId), zap.Int64("firstEntryId", first), zap.Int64("lastEntryId", last))
			successFrags = append(successFrags, r.target)
			flushedLastEntryId = last
			flushedLastFragmentId = uint64(fragId)
			cacheErr := cache.AddCacheFragment(ctx, r.target)
			if cacheErr != nil {
				logger.Ctx(ctx).Warn("add fragment to cache failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Int64("fragId", fragId), zap.Error(cacheErr))
			}
		}
	}
	// Update the first and last entry IDs after successful flush
	if flushedLastEntryId >= 0 {
		f.lastEntryId = flushedLastEntryId
		f.lastFragmentId = flushedLastFragmentId
		if f.firstEntryId == -1 {
			// Initialize firstEntryId on first successful flush
			// This should always be 0 for the initial flush
			f.firstEntryId = toFlushDataFirstEntryId
		}
	}

	// callback to notify all waiting append request channels
	if len(successFrags) == len(resultFrags) {
		restData, err := currentBuffer.ReadEntriesToLast(expectedNextEntryId)
		if err != nil {
			logger.Ctx(ctx).Error("Call Sync, but ReadEntriesToLast failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Error(err))
			return err
		}
		restDataFirstEntryId := expectedNextEntryId
		newBuffer := cache.NewSequentialBufferWithData(restDataFirstEntryId, int64(f.syncPolicyConfig.MaxEntries), restData)
		f.buffer.Store(newBuffer)

		// notify all waiting channels
		for syncingId, ch := range f.syncedChan {
			if syncingId < restDataFirstEntryId {
				ch <- syncingId
				delete(f.syncedChan, syncingId)
				close(ch)
			}
		}
		logger.Ctx(ctx).Debug("Sync to object storage success",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int64("logFileId", f.id),
			zap.Int("partitions", len(partitions)),
			zap.Int64("successFirstEntryId", toFlushDataFirstEntryId),
			zap.Int64("restDataFirstEntryId", restDataFirstEntryId),
			zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)),
			zap.String("newBufInst", fmt.Sprintf("%p", newBuffer)))
	} else if len(successFrags) > 0 {
		lastSuccessFrag := successFrags[len(successFrags)-1]
		last, _ := lastSuccessFrag.GetLastEntryId()
		restDataFirstEntryId := last + 1
		for syncingId, ch := range f.syncedChan {
			if syncingId < restDataFirstEntryId {
				// append success
				ch <- syncingId
				delete(f.syncedChan, syncingId)
				close(ch)
			} else {
				// append error
				ch <- -1
				delete(f.syncedChan, syncingId)
				close(ch)
			}
		}
		// new a empty buffer
		newBuffer := cache.NewSequentialBuffer(restDataFirstEntryId, int64(f.syncPolicyConfig.MaxEntries))
		f.buffer.Store(newBuffer)
		logger.Ctx(ctx).Debug("Sync to object storage partial success",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int64("logFileId", f.id),
			zap.Int("partitions", len(partitions)),
			zap.Int64("successFirstEntryId", toFlushDataFirstEntryId),
			zap.Int64("restDataFirstEntryId", restDataFirstEntryId),
			zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)),
			zap.String("newBufInst", fmt.Sprintf("%p", newBuffer)))
	} else {
		// no flush success, callback all append sync error
		for syncingId, ch := range f.syncedChan {
			// append error
			ch <- -1
			delete(f.syncedChan, syncingId)
			close(ch)
		}
		// reset buffer as empty
		currentBuffer.Reset()
		logger.Ctx(ctx).Debug("Sync to object storage all failed,reset buffer",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int64("logFileId", f.id),
			zap.Int("partitions", len(partitions)),
			zap.String("bufInst", fmt.Sprintf("%p", currentBuffer)))
	}
	return nil
}

func (f *LogFile) Close() error {
	// Implement close logic, e.g., release resources
	f.fileClose <- struct{}{}
	close(f.fileClose)
	return nil
}

func (f *LogFile) repackIfNecessary(toFlushData [][]byte, toFlushDataFirstEntryId int64) ([][][]byte, []int64) {
	maxPartitionSize := f.syncPolicyConfig.MaxFlushSize
	var partitions = make([][][]byte, 0)
	var partition = make([][]byte, 0)
	var currentSize = 0

	for _, entry := range toFlushData {
		entrySize := len(entry)
		if int64(currentSize+entrySize) > maxPartitionSize && currentSize > 0 {
			partitions = append(partitions, partition)
			partition = make([][]byte, 0)
			currentSize = 0
		}
		partition = append(partition, entry)
		currentSize += entrySize
	}
	if len(partition) > 0 {
		partitions = append(partitions, partition)
	}

	var partitionFirstEntryIds = make([]int64, 0)
	offset := toFlushDataFirstEntryId
	for _, part := range partitions {
		partitionFirstEntryIds = append(partitionFirstEntryIds, offset)
		offset += int64(len(part))
	}

	return partitions, partitionFirstEntryIds
}

func (f *LogFile) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	return nil, nil, nil, werr.ErrNotSupport.WithCauseErrMsg("not support LogFile writer to merge currently")
}

func (f *LogFile) Load(ctx context.Context) (int64, storage.Fragment, error) {
	return -1, nil, werr.ErrNotSupport.WithCauseErrMsg("not support LogFile writer to load currently")
}

func (f *LogFile) DeleteFragments(ctx context.Context, flag int) error {
	return werr.ErrNotSupport.WithCauseErrMsg("not support LogFile writer to delete fragments currently")
}

var _ storage.LogFile = (*ROLogFile)(nil)

// ROLogFile is used to read data from object storage as a logical file
type ROLogFile struct {
	mu               sync.RWMutex
	lastSync         atomic.Int64
	client           minioHandler.MinioHandler
	segmentPrefixKey string // The prefix key for the segment to which this LogFile belongs
	bucket           string // The bucket name

	id        int64             // LogFile Id in object storage
	fragments []*FragmentObject // LogFile cached fragments in order
}

// NewROLogFile is used to read only LogFile
func NewROLogFile(logFileId int64, segmentPrefixKey string, bucket string, objectCli minioHandler.MinioHandler) storage.LogFile {
	objFile := &ROLogFile{
		id:               logFileId,
		client:           objectCli,
		segmentPrefixKey: segmentPrefixKey,
		bucket:           bucket,
		fragments:        make([]*FragmentObject, 0),
	}
	existsNewFragment, _, err := objFile.prefetchFragmentInfos()
	if err != nil {
		logger.Ctx(context.TODO()).Warn("prefetch fragment infos failed when create Read-only LogFile",
			zap.String("segmentPrefixKey", segmentPrefixKey),
			zap.Int64("logFileId", logFileId),
			zap.Bool("existsNewFragment", existsNewFragment),
			zap.Error(err))
	}
	return objFile
}

func (f *ROLogFile) GetId() int64 {
	return f.id
}

func (f *ROLogFile) AppendAsync(ctx context.Context, entryId int64, data []byte) (int64, <-chan int64, error) {
	return entryId, nil, werr.ErrNotSupport.WithCauseErrMsg("read only LogFile reader cannot support append")
}

// Deprecated: use AppendAsync instead
func (f *ROLogFile) Append(ctx context.Context, data []byte) error {
	return werr.ErrNotSupport.WithCauseErrMsg("read only LogFile reader cannot support append")
}

// get the fragment for the entryId
func (f *ROLogFile) getFragment(entryId int64) (*FragmentObject, error) {
	logger.Ctx(context.TODO()).Debug("get fragment for entryId", zap.Int64("logFileId", f.id), zap.Int64("entryId", entryId))
	// fragmentId: 0~n
	foundFrag, err := f.findFragment(entryId)
	if err != nil {
		logger.Ctx(context.TODO()).Warn("get fragment from cache failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Int64("entryId", entryId), zap.Error(err))
		return nil, err
	}
	if foundFrag != nil {
		logger.Ctx(context.TODO()).Debug("get no fragment from cache for entryId", zap.Int64("logFileId", f.id), zap.Int64("entryId", entryId), zap.Int64("fragmentId", foundFrag.GetFragmentId()))
		return foundFrag, nil
	}

	// try to fetch new fragments if exists
	existsNewFragment, fetchedlastFragment, err := f.prefetchFragmentInfos()
	if err != nil {
		logger.Ctx(context.TODO()).Warn("prefetch fragment info failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Int64("entryId", entryId), zap.Error(err))
		return nil, err
	}
	if !existsNewFragment {
		// means get no fragment for this entryId
		return nil, nil
	}

	fetchedLastEntryId, err := fetchedlastFragment.GetLastEntryId()
	if err != nil {
		logger.Ctx(context.TODO()).Warn("get fragment lastEntryId failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Int64("entryId", entryId), zap.String("fragmentKey", fetchedlastFragment.fragmentKey), zap.Error(err))
		return nil, err
	}
	if entryId > fetchedLastEntryId {
		// fast return, no fragment for this entryId
		return nil, nil
	}
	if entryId == fetchedLastEntryId {
		// fast return this fragment
		return fetchedlastFragment, nil
	}

	// find again
	foundFrag, err = f.findFragment(entryId)
	if err != nil {
		return nil, err
	}
	if foundFrag != nil {
		logger.Ctx(context.TODO()).Debug("get fragment from cache for entryId", zap.Int64("logFileId", f.id), zap.Int64("entryId", entryId), zap.Int64("fragmentId", foundFrag.GetFragmentId()))
		return foundFrag, nil
	}

	// means get no fragment for this entryId
	return nil, nil
}

// findFragment finds the exists cache fragments for the entryId
func (f *ROLogFile) findFragment(entryId int64) (*FragmentObject, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	low, high := 0, len(f.fragments)-1
	var candidate *FragmentObject

	for low <= high {
		mid := (low + high) / 2
		frag := f.fragments[mid]

		first, err := frag.GetFirstEntryId()
		if err != nil {
			return nil, err
		}

		if first > entryId {
			high = mid - 1
		} else {
			last, err := frag.GetLastEntryId()
			if err != nil {
				return nil, err
			}
			if last >= entryId {
				candidate = frag
				return candidate, nil
			} else {
				low = mid + 1
			}
		}
	}
	return candidate, nil
}

// objectExists checks if an object exists in the MinIO bucket
func (f *ROLogFile) objectExists(ctx context.Context, objectKey string) (bool, error) {
	_, err := f.client.StatObject(ctx, f.bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (f *ROLogFile) NewReader(ctx context.Context, opt storage.ReaderOpt) (storage.Reader, error) {
	reader := NewLogFileReader(opt, f)
	return reader, nil
}

// LastFragmentId returns the last fragmentId of the log file.
func (f *ROLogFile) LastFragmentId() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.fragments) == 0 {
		return 0
	}
	return uint64(f.fragments[len(f.fragments)-1].GetFragmentId())
}

func (f *ROLogFile) getFirstEntryId() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if len(f.fragments) == 0 {
		return -1
	}
	first, _ := f.fragments[0].GetFirstEntryId()
	return first
}

func (f *ROLogFile) GetLastEntryId() (int64, error) {
	// prefetch fragmentInfos if any new fragment created
	_, lastFragment, err := f.prefetchFragmentInfos()
	if err != nil {
		logger.Ctx(context.TODO()).Warn("get last entryId failed",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int64("logFileId", f.id),
			zap.Error(err))
		return -1, err
	}
	lastEntryId, err := lastFragment.GetLastEntryId()
	if err != nil {
		return -1, err
	}
	logger.Ctx(context.TODO()).Debug("get last entryId failed",
		zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.Int64("logFileId", f.id),
		zap.Int64("lastFragId", lastFragment.GetFragmentId()),
		zap.Int64("lastEntryId", lastEntryId))
	return lastEntryId, nil
}

func (f *ROLogFile) Sync(ctx context.Context) error {
	return werr.ErrNotSupport.WithCauseErrMsg("RODiskLogFile not support sync")
}

func (f *ROLogFile) Close() error {
	return nil
}

func (f *ROLogFile) prefetchFragmentInfos() (bool, *FragmentObject, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var fetchedLastFragment *FragmentObject = nil

	fragId := uint64(1)
	if len(f.fragments) > 0 {
		lastFrag := f.fragments[len(f.fragments)-1]
		fragId = uint64(lastFrag.GetFragmentId()) + 1
		fetchedLastFragment = lastFrag
	}
	existsNewFragment := false
	for {
		fragKey := getFragmentObjectKey(f.segmentPrefixKey, f.id, fragId)
		// check if the fragment is already cached
		if frag, cached := cache.GetCachedFragment(context.TODO(), fragKey); cached {
			cachedFrag := frag.(*FragmentObject)
			fetchedLastFragment = cachedFrag
			f.fragments = append(f.fragments, cachedFrag)
			fragId++
			existsNewFragment = true
			continue
		}

		// check if the fragment exists in object storage
		exists, err := f.objectExists(context.Background(), fragKey)
		if err != nil {
			// indicates that the prefetching of fragments has completed.
			//fmt.Println("object storage read fragment err: ", err)
			return existsNewFragment, nil, err
		}

		if exists {
			fragment := NewFragmentObject(f.client, f.bucket, fragId, fragKey, nil, 0, false, true, false)
			fetchedLastFragment = fragment
			f.fragments = append(f.fragments, fragment)
			existsNewFragment = true
			fragId++
			logger.Ctx(context.Background()).Debug("prefetch fragment info", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Uint64("lastFragId", fragId-1))
		} else {
			// no more fragment exists, cause the id sequence is broken
			break
		}
	}
	logger.Ctx(context.Background()).Debug("prefetch fragment infos", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Int("fragments", len(f.fragments)), zap.Uint64("lastFragId", fragId-1))
	return existsNewFragment, fetchedLastFragment, nil
}

func (f *ROLogFile) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	start := time.Now()
	// TODO should be config
	// file max size, default 128MB
	fileMaxSize := int64(128_000_000)
	mergedFrags := make([]storage.Fragment, 0)
	mergedFragId := uint64(0)
	entryOffset := make([]int32, 0)
	fragmentIdOffset := make([]int32, 0)

	totalMergeSize := int64(0)
	pendingMergeSize := int64(0)
	pendingMergeFrags := make([]*FragmentObject, 0)
	// load all fragment in memory
	for _, frag := range f.fragments {
		loadFragErr := frag.Load(ctx)
		if loadFragErr != nil {
			return nil, nil, nil, loadFragErr
		}
		pendingMergeFrags = append(pendingMergeFrags, frag)
		pendingMergeSize += frag.GetSize()
		if pendingMergeSize >= fileMaxSize {
			// merge immediately
			mergedFrag, mergeErr := mergeFragmentsAndReleaseAfterCompleted(ctx, getMergedFragmentObjectKey(f.segmentPrefixKey, f.id, mergedFragId), mergedFragId, pendingMergeFrags)
			if mergeErr != nil {
				return nil, nil, nil, mergeErr
			}
			mergedFrags = append(mergedFrags, mergedFrag)
			mergedFragId++
			mergedFragFirstEntryId, _ := mergedFrag.GetFirstEntryId()
			entryOffset = append(entryOffset, int32(mergedFragFirstEntryId))
			fragmentIdOffset = append(fragmentIdOffset, int32(pendingMergeFrags[0].GetFragmentId()))
			pendingMergeFrags = make([]*FragmentObject, 0)
			totalMergeSize += pendingMergeSize
			pendingMergeSize = 0
		}
	}
	if pendingMergeSize > 0 && len(pendingMergeFrags) > 0 {
		// merge immediately
		mergedFrag, mergeErr := mergeFragmentsAndReleaseAfterCompleted(ctx, getMergedFragmentObjectKey(f.segmentPrefixKey, f.id, mergedFragId), mergedFragId, pendingMergeFrags)
		if mergeErr != nil {
			return nil, nil, nil, mergeErr
		}
		mergedFrags = append(mergedFrags, mergedFrag)
		mergedFragId++
		mergedFragFirstEntryId, _ := mergedFrag.GetFirstEntryId()
		entryOffset = append(entryOffset, int32(mergedFragFirstEntryId))
		fragmentIdOffset = append(fragmentIdOffset, int32(pendingMergeFrags[0].GetFragmentId()))
		pendingMergeFrags = make([]*FragmentObject, 0)
		totalMergeSize += pendingMergeSize
		pendingMergeSize = 0
	}
	cost := time.Now().Sub(start)
	metrics.WpCompactReqLatency.WithLabelValues(fmt.Sprintf("%d", f.id)).Observe(float64(cost.Milliseconds()))
	metrics.WpCompactBytes.WithLabelValues(fmt.Sprintf("%d", f.id)).Observe(float64(totalMergeSize))
	return mergedFrags, entryOffset, fragmentIdOffset, nil
}

// TODO improve， here we just need to load the last fragment
func (f *ROLogFile) Load(ctx context.Context) (int64, storage.Fragment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.fragments) == 0 {
		return 0, nil, nil
	}
	totalSize := int64(0)
	cachedList := make(map[int]storage.Fragment)
	for fid, frag := range f.fragments {
		// if the fragment is already cached, use it later
		if cachedFrag, cached := cache.GetCachedFragment(ctx, frag.GetFragmentKey()); cached {
			cachedList[fid] = cachedFrag
			cf := cachedFrag.(*FragmentObject)
			totalSize += cf.GetSize()
			continue
		}
		// otherwise, load from object storage
		loadFragErr := frag.Load(ctx)
		if loadFragErr != nil {
			return 0, nil, loadFragErr
		}
		totalSize += frag.GetSize()
	}
	// replace with cached fragment
	for fid, frag := range cachedList {
		f.fragments[fid] = frag.(*FragmentObject)
	}

	lastFragment := f.fragments[len(f.fragments)-1]
	lastEntryId, err := lastFragment.GetLastEntryId()
	if err != nil {
		return -1, nil, err
	}
	fragId := lastFragment.GetFragmentId()
	logger.Ctx(ctx).Debug("Load fragments", zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.Int64("logFileId", f.id),
		zap.Int("fragments", len(f.fragments)),
		zap.Int64("totalSize", totalSize),
		zap.Int64("lastFragId", fragId),
		zap.Int64("lastEntryId", lastEntryId),
	)
	return int64(totalSize), lastFragment, nil
}

func (f *ROLogFile) DeleteFragments(ctx context.Context, flag int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	logger.Ctx(ctx).Info("Starting to delete fragments",
		zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.Int64("logFileId", f.id),
		zap.Int("flag", flag))

	// List all fragment objects in object storage
	listPrefix := fmt.Sprintf("%s/%d/", f.segmentPrefixKey, f.id)
	objectCh := f.client.ListObjects(ctx, f.bucket, listPrefix, false, minio.ListObjectsOptions{})

	var deleteErrors []error
	var normalFragmentCount int = 0
	var mergedFragmentCount int = 0

	// Iterate through all found objects
	for objInfo := range objectCh {
		if objInfo.Err != nil {
			logger.Ctx(ctx).Warn("Error listing objects",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Int64("logFileId", f.id),
				zap.Error(objInfo.Err))
			deleteErrors = append(deleteErrors, objInfo.Err)
			continue
		}

		// Only process fragment files
		if !strings.HasSuffix(objInfo.Key, ".frag") {
			continue
		}

		// Remove from cache
		if cachedFrag, found := cache.GetCachedFragment(ctx, objInfo.Key); found {
			_ = cache.RemoveCachedFragment(ctx, cachedFrag)
		}

		// Delete object
		err := f.client.RemoveObject(ctx, f.bucket, objInfo.Key, minio.RemoveObjectOptions{})
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to delete fragment object",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Int64("logFileId", f.id),
				zap.String("objectKey", objInfo.Key),
				zap.Error(err))
			deleteErrors = append(deleteErrors, err)
			continue
		}

		// Count deleted fragment types
		if strings.Contains(objInfo.Key, "/m_") {
			logger.Ctx(ctx).Debug("Successfully deleted merged fragment object",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Int64("logFileId", f.id),
				zap.String("objectKey", objInfo.Key))
			mergedFragmentCount++
		} else {
			logger.Ctx(ctx).Debug("Successfully deleted normal fragment object",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Int64("logFileId", f.id),
				zap.String("objectKey", objInfo.Key))
			normalFragmentCount++
		}
	}

	// Clean up internal state
	f.fragments = make([]*FragmentObject, 0)

	logger.Ctx(ctx).Info("Completed fragment deletion",
		zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.Int64("logFileId", f.id),
		zap.Int("normalFragmentCount", normalFragmentCount),
		zap.Int("mergedFragmentCount", mergedFragmentCount),
		zap.Int("errorCount", len(deleteErrors)))

	if len(deleteErrors) > 0 {
		return fmt.Errorf("failed to delete %d fragment objects", len(deleteErrors))
	}

	return nil
}

// NewLogFileReader creates a new LogFileReader instance.
func NewLogFileReader(opt storage.ReaderOpt, objectFile *ROLogFile) storage.Reader {
	return &logFileReader{
		opt:                opt,
		logfile:            objectFile,
		pendingReadEntryId: opt.StartSequenceNum,
	}
}

var _ storage.Reader = (*logFileReader)(nil)

type logFileReader struct {
	opt     storage.ReaderOpt
	logfile *ROLogFile

	pendingReadEntryId int64
	currentFragment    *FragmentObject
}

func (o *logFileReader) Close() error {
	// NO OP
	return nil
}

func (o *logFileReader) ReadNext() (*proto.LogEntry, error) {
	if o.currentFragment == nil {
		return nil, errors.New("no readable Fragment")
	}
	entryValue, err := o.currentFragment.GetEntry(o.pendingReadEntryId)
	if err != nil {
		return nil, err
	}
	entry := &proto.LogEntry{
		EntryId: o.pendingReadEntryId,
		Values:  entryValue,
	}
	o.pendingReadEntryId++
	return entry, nil
}

func (o *logFileReader) HasNext() bool {
	if o.pendingReadEntryId >= int64(o.opt.EndSequenceNum) && o.opt.EndSequenceNum > 0 {
		// reach the end of range
		return false
	}
	f, err := o.logfile.getFragment(o.pendingReadEntryId)
	if err != nil {
		logger.Ctx(context.Background()).Warn("Failed to get fragment",
			zap.String("segmentPrefixKey", o.logfile.segmentPrefixKey),
			zap.Int64("logFileId", o.logfile.id),
			zap.Int64("pendingReadEntryId", o.pendingReadEntryId),
			zap.Error(err))
		return false
	}
	if f == nil {
		// no more fragment
		return false
	}
	//
	o.currentFragment = f
	return true
}

// utils for fragment object key
func getFragmentObjectKey(segmentPrefixKey string, logFileId int64, fragmentId uint64) string {
	return fmt.Sprintf("%s/%d/%d.frag", segmentPrefixKey, logFileId, fragmentId)
}

// utils for merged fragment object key
func getMergedFragmentObjectKey(segmentPrefixKey string, logFileId int64, mergedFragmentId uint64) string {
	return fmt.Sprintf("%s/%d/m_%d.frag", segmentPrefixKey, logFileId, mergedFragmentId)
}

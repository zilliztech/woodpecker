package objectstorage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/retry"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
)

var _ storage.LogFile = (*LogFile)(nil)

type LogFile struct {
	mu               sync.Mutex
	lastSync         atomic.Int64
	client           minioHandler.MinioHandler
	segmentPrefixKey string // The prefix key for the segment to which this LogFile belongs
	bucket           string // The bucket name

	id        int64             // LogFile Id in object storage
	fragments []*FragmentObject // LogFile cached fragments
	// write buffer
	buffer           *SequentialBuffer // Write buffer
	maxBufferSize    int               // Max buffer size to sync buffer to object storage
	maxIntervalMs    int               // Max interval to sync buffer to object storage
	syncPolicyConfig *config.LogFileSyncPolicyConfig
	syncedChan       map[int64]chan int64 // Synced entryId chan map
	fileClose        chan struct{}        // Close signal

	// status
	sealed atomic.Bool // If this LogFile is sealed or growing
}

// NewLogFile is used to create a new LogFile, which is used to write data to object storage
func NewLogFile(logFileId int64, segmentPrefixKey string, bucket string, objectCli minioHandler.MinioHandler, cfg *config.Configuration) storage.LogFile {
	logger.Ctx(context.TODO()).Debug("new LogFile created", zap.Int64("logFileId", logFileId), zap.String("segmentPrefixKey", segmentPrefixKey))
	syncPolicyConfig := &cfg.Woodpecker.Logstore.LogFileSyncPolicy
	objFile := &LogFile{
		id:               logFileId,
		client:           objectCli,
		segmentPrefixKey: segmentPrefixKey,
		bucket:           bucket,
		fragments:        make([]*FragmentObject, 0),

		buffer:           NewSequentialBuffer(0, int64(syncPolicyConfig.MaxEntries)),
		maxBufferSize:    syncPolicyConfig.MaxBytes,
		maxIntervalMs:    syncPolicyConfig.MaxInterval,
		syncPolicyConfig: syncPolicyConfig,
		fileClose:        make(chan struct{}),
		syncedChan:       make(map[int64]chan int64),
	}
	objFile.sealed.Store(false)
	go objFile.run()
	return objFile
}

// NewROLogFile is used to read only LogFile
func NewROLogFile(logFileId int64, segmentPrefixKey string, bucket string, objectCli minioHandler.MinioHandler) storage.LogFile {
	objFile := &LogFile{
		id:               logFileId,
		client:           objectCli,
		segmentPrefixKey: segmentPrefixKey,
		bucket:           bucket,
		fragments:        make([]*FragmentObject, 0),

		buffer:        NewSequentialBuffer(0, 100_000),
		maxBufferSize: 16 * 1024 * 1024,
		maxIntervalMs: 1000,
		fileClose:     make(chan struct{}),
		syncedChan:    make(map[int64]chan int64),
	}
	objFile.sealed.Store(false)
	objFile.prefetchFragmentInfos()
	return objFile
}

// Like OS file fsync dirty pageCache periodically, objectStoreFile will sync buffer to object storage periodically
func (f *LogFile) run() {
	// time ticker
	ticker := time.NewTicker(time.Duration(f.maxIntervalMs / 4 * int(time.Millisecond)))
	defer ticker.Stop()
	f.lastSync.Store(time.Now().UnixMilli())
	for {
		select {
		case <-ticker.C:
			if time.Now().UnixMilli()-f.lastSync.Load() < int64(f.maxIntervalMs) {
				continue
			}
			err := f.Sync(context.Background())
			if err != nil {
				logger.Ctx(context.TODO()).Warn("sync error",
					zap.String("segmentPrefixKey", f.segmentPrefixKey),
					zap.Int64("logFileId", f.id),
					zap.Error(err))
			}
		case <-f.fileClose:
			err := f.Sync(context.Background())
			if err != nil {
				logger.Ctx(context.TODO()).Warn("sync error",
					zap.String("segmentPrefixKey", f.segmentPrefixKey),
					zap.Int64("logFileId", f.id),
					zap.Error(err))
			}
			f.sealed.Store(true)
			ReleaseFragments(context.Background(), f.fragments)
			return
		}
	}
}

func (f *LogFile) GetId() int64 {
	return f.id
}

func (f *LogFile) AppendAsync(ctx context.Context, entryId int64, data []byte) (int64, <-chan int64, error) {
	ch := make(chan int64, 1)
	// trigger sync by max buffer entries num
	sizeAfterAppend := f.buffer.expectedNextEntryId.Load() + 1
	if sizeAfterAppend >= int64(f.buffer.firstEntryId+f.buffer.maxSize) {
		logger.Ctx(context.TODO()).Debug("buffer full, trigger flush",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int64("logFileId", f.id),
			zap.Int64("sizeAfterAppend", sizeAfterAppend),
			zap.Int64("maxSize", f.buffer.firstEntryId+f.buffer.maxSize))
		err := f.Sync(ctx)
		if err != nil {
			// sync does not success
			ch <- -1
			close(ch)
			return entryId, ch, err
		}
	}

	f.mu.Lock()
	// write buffer
	id, err := f.buffer.WriteEntry(entryId, data)
	if err != nil {
		// sync does not success
		ch <- -1
		close(ch)
		f.mu.Unlock()
		return id, ch, err
	}
	f.syncedChan[id] = ch
	f.mu.Unlock()

	// trigger sync by max buffer entries bytes size
	if f.buffer.dataSize.Load() >= int64(f.maxBufferSize) {
		logger.Ctx(context.TODO()).Debug("reach max buffer size, trigger flush", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Int64("bufferSize", f.buffer.dataSize.Load()), zap.Int64("maxSize", int64(f.maxBufferSize)))
		syncErr := f.Sync(ctx)
		if syncErr != nil {
			logger.Ctx(context.TODO()).Warn("reach max buffer size, but trigger flush failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Int64("bufferSize", f.buffer.dataSize.Load()), zap.Int64("maxSize", int64(f.maxBufferSize)), zap.Error(syncErr))
		}
	}
	return id, ch, nil
}

// Deprecated: use AppendAsync instead
func (f *LogFile) Append(ctx context.Context, data []byte) error {
	//f.mu.Lock()
	//defer f.mu.Unlock()
	//
	//offset := f.LastOffset() + 1
	//key := f.getFragmentKey(offset)
	//fragment := NewObjectStorageFragment(f.client, f.bucket, key, 0)
	//err := fragment.Write(ctx, data)
	//if err != nil {
	//	return err
	//}
	//f.fragments = append(f.fragments, fragment)
	return nil
}

func (f *LogFile) getFragmentKey(fragmentId uint64) string {
	return fmt.Sprintf("%s/%d/%d.frag", f.segmentPrefixKey, f.id, fragmentId)
}

func (f *LogFile) getMergedFragmentKey(mergedFragmentId uint64) string {
	return fmt.Sprintf("%s/%d/m_%d.frag", f.segmentPrefixKey, f.id, mergedFragmentId)
}

// get the fragment for the entryId
func (f *LogFile) getFragment(entryId int64) (*FragmentObject, error) {
	// fragmentId: 0~n
	for _, fragment := range f.fragments {
		lastEntryId, err := fragment.GetLastEntryId()
		if err != nil {
			return nil, err
		}
		if lastEntryId >= entryId {
			return fragment, nil
		}
	}
	// try next fragment which has been uploaded in objectStorage
	nextFragmentId := f.LastFragmentId() + 1
	nextFragmentKey := f.getFragmentKey(nextFragmentId)
	exists, err := f.objectExists(context.Background(), nextFragmentKey)
	if err != nil {
		return nil, err
	}

	if exists {
		fragment := NewFragmentObject(f.client, f.bucket, nextFragmentId, nextFragmentKey, nil, 0, false, true)
		f.fragments = append(f.fragments, fragment)
		return fragment, nil
	}
	return nil, nil
}

// objectExists checks if an object exists in the MinIO bucket
func (f *LogFile) objectExists(ctx context.Context, objectKey string) (bool, error) {
	_, err := f.client.StatObject(ctx, f.bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (f *LogFile) NewReader(ctx context.Context, opt storage.ReaderOpt) (storage.Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	lastEntryIdInFile, err := f.GetLastEntryId()
	if err != nil {
		return nil, err
	}
	if opt.StartSequenceNum < 0 || opt.StartSequenceNum > lastEntryIdInFile && lastEntryIdInFile != -1 {
		return nil, werr.ErrEntryNotFound.WithCauseErrMsg(
			fmt.Sprintf("startEntryId:%d must less than lastEntryId:%d of the file",
				opt.StartSequenceNum, lastEntryIdInFile))
	}

	reader := NewLogFileReader(opt, f)
	return reader, nil
}

// LastFragmentId returns the last fragmentId of the log file.
func (f *LogFile) LastFragmentId() uint64 {
	if len(f.fragments) == 0 {
		return 0
	}
	return f.fragments[len(f.fragments)-1].fragmentId
}

func (f *LogFile) getFirstEntryId() int64 {
	if len(f.fragments) == 0 {
		return -1
	}
	return f.fragments[0].firstEntryId
}

func (f *LogFile) GetLastEntryId() (int64, error) {
	// prefetch fragmentInfos if any new fragment created
	f.prefetchFragmentInfos()

	// load dynamic fragment
	if len(f.fragments) == 0 {
		// -1 represent no entry data yet
		return -1, nil
	}

	lastFrag := f.fragments[len(f.fragments)-1]
	if !lastFrag.loaded {
		err := lastFrag.Load(context.Background())
		if err != nil {
			logger.Ctx(context.TODO()).Warn("get last entryId failed",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Int64("logFileId", f.id),
				zap.Uint64("fragId", lastFrag.fragmentId),
				zap.Error(err))
			return -1, err
		}
	}
	return lastFrag.lastEntryId, nil
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
		f.lastSync.Store(time.Now().UnixMilli())
	}()

	entryCount := len(f.buffer.values)
	if entryCount == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id))
		return nil
	}

	// get flush point to flush
	if f.buffer.expectedNextEntryId.Load()-f.buffer.firstEntryId == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id))
		return nil
	}

	toFlushData, err := f.buffer.ReadEntriesRange(f.buffer.firstEntryId, f.buffer.expectedNextEntryId.Load())
	if err != nil {
		logger.Ctx(ctx).Error("Call Sync, but ReadEntriesRange failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Error(err))
		return err
	}
	toFlushDataFirstEntryId := f.buffer.firstEntryId

	partitions, partitionFirstEntryIds := f.repackIfNecessary(toFlushData, toFlushDataFirstEntryId)
	concurrentCh := make(chan int, f.syncPolicyConfig.MaxFlushThreads)
	flushResultCh := make(chan *flushResult, len(partitions))
	var concurrentWg sync.WaitGroup
	logger.Ctx(ctx).Debug("get flush partitions finish", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Int("partitions", len(partitions)))
	lastFragmentId := f.LastFragmentId()
	for i, partition := range partitions {
		concurrentWg.Add(1)
		go func() {
			concurrentCh <- i                        // take one flush goroutine to start
			fragId := lastFragmentId + 1 + uint64(i) // fragment id
			logger.Ctx(ctx).Debug("start flush part of buffer as fragment", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Uint64("fragId", fragId))
			key := f.getFragmentKey(fragId)
			fragment := NewFragmentObject(f.client, f.bucket, fragId, key, partition, partitionFirstEntryIds[i], true, false)
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
	concurrentWg.Wait() // 等待所有任务结束
	close(flushResultCh)
	logger.Ctx(ctx).Debug("all parts of buffer have been flushed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id))
	resultFrags := make([]*flushResult, 0)
	for r := range flushResultCh {
		resultFrags = append(resultFrags, r)
	}
	sort.Slice(resultFrags, func(i, j int) bool {
		return resultFrags[i].target.fragmentId < resultFrags[j].target.fragmentId
	})
	successFrags := make([]*FragmentObject, 0)
	for _, r := range resultFrags {
		if r.err != nil {
			logger.Ctx(ctx).Warn("flush fragment failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Uint64("fragId", r.target.fragmentId), zap.Int64("firstEntryId", r.target.firstEntryId), zap.Int64("lastEntryId", r.target.lastEntryId))
			// Can only succeed sequentially without holes
			break
		} else {
			logger.Ctx(ctx).Debug("flush fragment success", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Uint64("fragId", r.target.fragmentId), zap.Int64("firstEntryId", r.target.firstEntryId), zap.Int64("lastEntryId", r.target.lastEntryId))
			successFrags = append(successFrags, r.target)
		}
	}
	f.fragments = append(f.fragments, successFrags...)

	// callback to notify all waiting append request channels
	if len(successFrags) == len(resultFrags) {
		restData, err := f.buffer.ReadEntriesToLast(f.buffer.expectedNextEntryId.Load())
		if err != nil {
			logger.Ctx(ctx).Error("Call Sync, but ReadEntriesToLast failed", zap.String("segmentPrefixKey", f.segmentPrefixKey), zap.Int64("logFileId", f.id), zap.Error(err))
			return err
		}
		restDataFirstEntryId := f.buffer.expectedNextEntryId.Load()
		f.buffer = NewSequentialBufferWithData(restDataFirstEntryId, int64(f.syncPolicyConfig.MaxEntries), restData)

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
			zap.Int("partitions", len(partitions)))
	} else if len(successFrags) > 0 {
		restDataFirstEntryId := successFrags[len(successFrags)-1].lastEntryId + 1
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
		f.buffer = NewSequentialBuffer(restDataFirstEntryId, int64(f.syncPolicyConfig.MaxEntries))
	} else {
		// no flush success, callback all append sync error
		for syncingId, ch := range f.syncedChan {
			// append error
			ch <- -1
			delete(f.syncedChan, syncingId)
			close(ch)
		}
		// reset buffer as empty
		f.buffer.Reset()
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
		if currentSize+entrySize > maxPartitionSize && currentSize > 0 {
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

func (f *LogFile) prefetchFragmentInfos() {
	fragId := uint64(1)
	if len(f.fragments) > 0 {
		lastFrag := f.fragments[len(f.fragments)-1]
		fragId = lastFrag.fragmentId + 1
	}
	for {
		fragKey := f.getFragmentKey(fragId)
		exists, err := f.objectExists(context.Background(), fragKey)
		if err != nil {
			// indicates that the prefetching of fragments has completed.
			//fmt.Println("object storage read fragment err: ", err)
			return
		}
		if exists {
			fragment := NewFragmentObject(f.client, f.bucket, fragId, fragKey, nil, 0, false, true)
			f.fragments = append(f.fragments, fragment)
			fragId++
		} else {
			break
		}
	}
}

func (f *LogFile) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// TODO should be config
	// file max size, default 128MB
	fileMaxSize := 128_000_000
	mergedFrags := make([]storage.Fragment, 0)
	mergedFragId := uint64(0)
	entryOffset := make([]int32, 0)
	fragmentIdOffset := make([]int32, 0)

	pendingMergeSize := 0
	pendingMergeFrags := make([]*FragmentObject, 0)
	// load all fragment in memory
	for _, frag := range f.fragments {
		loadFragErr := frag.Load(ctx)
		if loadFragErr != nil {
			return nil, nil, nil, loadFragErr
		}
		pendingMergeFrags = append(pendingMergeFrags, frag)
		pendingMergeSize += len(frag.entriesData) + len(frag.indexes)
		if pendingMergeSize >= fileMaxSize {
			// merge immediately
			mergedFrag, mergeErr := MergeFragmentsAndReleaseAfterCompleted(ctx, f.getMergedFragmentKey(mergedFragId), mergedFragId, pendingMergeFrags)
			if mergeErr != nil {
				return nil, nil, nil, mergeErr
			}
			mergedFrags = append(mergedFrags, mergedFrag)
			mergedFragId++
			entryOffset = append(entryOffset, int32(mergedFrag.GetFirstEntryIdDirectly()))
			fragmentIdOffset = append(fragmentIdOffset, int32(pendingMergeFrags[0].fragmentId))
			pendingMergeFrags = make([]*FragmentObject, 0)
			pendingMergeSize = 0
		}
	}
	if pendingMergeSize > 0 && len(pendingMergeFrags) > 0 {
		// merge immediately
		mergedFrag, mergeErr := MergeFragmentsAndReleaseAfterCompleted(ctx, f.getMergedFragmentKey(mergedFragId), mergedFragId, pendingMergeFrags)
		if mergeErr != nil {
			return nil, nil, nil, mergeErr
		}
		mergedFrags = append(mergedFrags, mergedFrag)
		mergedFragId++
		entryOffset = append(entryOffset, int32(mergedFrag.GetFirstEntryIdDirectly()))
		fragmentIdOffset = append(fragmentIdOffset, int32(pendingMergeFrags[0].fragmentId))
		pendingMergeFrags = make([]*FragmentObject, 0)
		pendingMergeSize = 0
	}
	return mergedFrags, entryOffset, fragmentIdOffset, nil
}

func (f *LogFile) Load(ctx context.Context) (int64, storage.Fragment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.fragments) == 0 {
		return 0, nil, nil
	}
	// TODO load fragment meta index only
	totalSize := 0
	for _, frag := range f.fragments {
		loadFragErr := frag.Load(ctx)
		if loadFragErr != nil {
			return 0, nil, loadFragErr
		}
		totalSize += len(frag.entriesData) + len(frag.indexes)
	}
	lastFragment := f.fragments[len(f.fragments)-1]
	loadFragErr := lastFragment.Load(ctx)
	if loadFragErr != nil {
		return 0, nil, loadFragErr
	}
	return int64(totalSize), lastFragment, nil
}

// NewLogFileReader creates a new LogFileReader instance.
func NewLogFileReader(opt storage.ReaderOpt, objectFile *LogFile) storage.Reader {
	return &logFileReader{
		opt:                opt,
		logfile:            objectFile,
		pendingReadEntryId: opt.StartSequenceNum,
	}
}

var _ storage.Reader = (*logFileReader)(nil)

type logFileReader struct {
	opt     storage.ReaderOpt
	logfile *LogFile

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
	f, _ := o.logfile.getFragment(o.pendingReadEntryId)
	if f == nil {
		// no more fragment
		return false
	}
	//
	o.currentFragment = f
	return true
}

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

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
)

var _ storage.LogFile = (*LogFile)(nil)

type LogFile struct {
	mu               sync.Mutex
	client           *minio.Client
	segmentPrefixKey string // The prefix key for the segment to which this LogFile belongs
	bucket           string // The bucket name

	id        int64             // LogFile Id in object storage
	fragments []*FragmentObject // LogFile cached fragments
	// write buffer
	buffer        *SequentialBuffer    // Write buffer
	maxBufferSize int                  // Max buffer size to sync buffer to object storage
	maxIntervalMs int                  // Max interval to sync buffer to object storage
	syncedChan    map[int64]chan int64 // Synced entryId chan map
	fileClose     chan struct{}        // Close signal

	// status
	sealed atomic.Bool // If this LogFile is sealed or growing
}

func NewLogFile(logFileId int64, segmentPrefixKey string, bucket string, objectCli *minio.Client) storage.LogFile {
	logger.Ctx(context.TODO()).Debug("new LogFile created", zap.Int64("logFileId", logFileId), zap.String("segmentPrefixKey", segmentPrefixKey))
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
	go objFile.run()
	return objFile
}

func NewROLogFile(logFileId int64, segmentPrefixKey string, bucket string, objectCli *minio.Client) storage.LogFile {
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
	ticker := time.NewTicker(time.Duration(f.maxIntervalMs * int(time.Millisecond)))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
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
			return
		}
	}
}

func (f *LogFile) GetId() int64 {
	return f.id
}

func (f *LogFile) AppendAsync(ctx context.Context, entryId int64, data []byte) (int64, <-chan int64, error) {
	ch := make(chan int64, 1)
	// first check buffer size, trigger flush if necessary
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
			return entryId, ch, err
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	// write buffer
	id, err := f.buffer.WriteEntry(entryId, data)
	if err != nil {
		// sync does not success
		ch <- -1
		return id, ch, err
	}
	f.syncedChan[id] = ch
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
	nextFragmentId := f.LastOffset() + 1
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

	lastEntryIdInFile := f.getLastEntryId()
	if opt.StartSequenceNum < 0 || opt.StartSequenceNum > lastEntryIdInFile && lastEntryIdInFile != -1 {
		return nil, werr.ErrEntryNotFound.WithCauseErrMsg(
			fmt.Sprintf("startEntryId:%d must less than lastEntryId:%d of the file",
				opt.StartSequenceNum, lastEntryIdInFile))
	}

	reader := NewLogFileReader(opt, f)
	return reader, nil
}

// LastOffset returns the last fragmentId of the log file.
func (f *LogFile) LastOffset() uint64 {
	if len(f.fragments) == 0 {
		return 0
	}
	return f.fragments[len(f.fragments)-1].fragmentId
}

func (f *LogFile) getFirstEntryId() int64 {
	if len(f.fragments) == 0 {
		return 0
	}
	return f.fragments[0].firstEntryId
}

func (f *LogFile) getLastEntryId() int64 {
	// prefetch fragmentInfos if any new fragment created
	f.prefetchFragmentInfos()

	// load dynamic fragment
	if len(f.fragments) == 0 {
		// -1 represent no entry data yet
		return -1
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
			return -1
		}
	}
	return lastFrag.lastEntryId
}

func (f *LogFile) Sync(ctx context.Context) error {
	// Implement sync logic, e.g., flush to persistent storage
	f.mu.Lock()
	defer f.mu.Unlock()

	entryCount := len(f.buffer.values)
	if entryCount == 0 {
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int64("logFileId", f.id))
		return nil
	}

	// get flush point to flush
	if f.buffer.expectedNextEntryId.Load()-f.buffer.firstEntryId == 0 {
		//log.Printf("Call Sync, but no data, skip ... ")
		return nil
	}

	toFlushData, err := f.buffer.ReadEntriesRange(f.buffer.firstEntryId, f.buffer.expectedNextEntryId.Load())
	if err != nil {
		logger.Ctx(ctx).Error("Call Sync, but ReadEntriesRange failed",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int64("logFileId", f.id),
			zap.Error(err))
		return err
	}
	toFlushDataFirstEntryId := f.buffer.firstEntryId
	restData, err := f.buffer.ReadEntriesToLast(f.buffer.expectedNextEntryId.Load())
	if err != nil {
		logger.Ctx(ctx).Error("Call Sync, but ReadEntriesToLast failed",
			zap.String("segmentPrefixKey", f.segmentPrefixKey),
			zap.Int64("logFileId", f.id),
			zap.Error(err))
		return err
	}
	restDataFirstEntryId := f.buffer.expectedNextEntryId.Load()
	f.buffer = NewSequentialBufferWithData(restDataFirstEntryId, 100_000, restData)

	partitions, partitionFirstEntryIds := f.repackIfNecessary(toFlushData, toFlushDataFirstEntryId)
	concurrentCh := make(chan int, 4)
	resultCh := make(chan *FragmentObject, len(partitions))
	var concurrentWg sync.WaitGroup
	logger.Ctx(ctx).Debug("get flush partitions finish",
		zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.Int64("logFileId", f.id),
		zap.Int("partitions", len(partitions)))
	for i, partition := range partitions {
		// TODO 记录一个 flushing的 metrics，看看并发有多少个线程在写minio
		concurrentWg.Add(1) // 开始一个任务
		go func() {
			concurrentCh <- i                        // 获得一个线程执行
			fragId := f.LastOffset() + 1 + uint64(i) // fragment id
			logger.Ctx(ctx).Debug("start flush part of buffer as fragment",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Int64("logFileId", f.id),
				zap.Uint64("fragId", fragId))
			key := f.getFragmentKey(fragId)
			fragment := NewFragmentObject(f.client, f.bucket, fragId, key, partition, partitionFirstEntryIds[i], true, false)
			err = fragment.Flush(ctx)
			if err != nil {
				logger.Ctx(ctx).Error("flush part of buffer as fragment failed",
					zap.String("segmentPrefixKey", f.segmentPrefixKey),
					zap.Int64("logFileId", f.id),
					zap.Uint64("fragId", fragId),
					zap.Error(err))
				//return err
				//panic(err) // TODO test only
				resultCh <- nil
			} else {
				//f.fragments = append(f.fragments, fragment)
				//fmt.Println("synced to object storage: ", f.id)
				resultCh <- fragment
			}
			concurrentWg.Done() // 结束一个任务
			<-concurrentCh      // 释放一个线程
			logger.Ctx(ctx).Debug("finish flush part of buffer as fragment",
				zap.String("segmentPrefixKey", f.segmentPrefixKey),
				zap.Int64("logFileId", f.id),
				zap.Uint64("fragId", fragId))
		}()
	}

	logger.Ctx(ctx).Debug("wait for all parts of buffer to be flushed",
		zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.Int64("logFileId", f.id))
	concurrentWg.Wait() // 等待所有任务结束
	close(resultCh)
	logger.Ctx(ctx).Debug("all parts of buffer have been flushed",
		zap.String("segmentPrefixKey", f.segmentPrefixKey),
		zap.Int64("logFileId", f.id))
	resultFrags := make([]*FragmentObject, 0)
	for r := range resultCh {
		if r != nil {
			resultFrags = append(resultFrags, r)
		} else {
			return fmt.Errorf("flush fragment error ... ")
		}
	}
	sort.Slice(resultFrags, func(i, j int) bool {
		return resultFrags[i].fragmentId < resultFrags[j].fragmentId
	})
	f.fragments = append(f.fragments, resultFrags...)

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
	return nil
}

func (f *LogFile) Close() error {
	// Implement close logic, e.g., release resources
	f.fileClose <- struct{}{}
	return nil
}

func (f *LogFile) repackIfNecessary(toFlushData [][]byte, toFlushDataFirstEntryId int64) ([][][]byte, []int64) {
	const maxPartitionSize = 8 * 1024 * 1024 // 每个分区最大为8MB
	var partitions [][][]byte = make([][][]byte, 0)
	var partition [][]byte = make([][]byte, 0)
	var currentSize int = 0

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

	var partitionFirstEntryIds []int64 = make([]int64, 0)
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
	//TODO implement me
	panic("implement me")
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

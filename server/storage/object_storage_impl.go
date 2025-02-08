package storage

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

var _ LogFile = (*objectStorageLogFile)(nil)

type objectStorageLogFile struct {
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

func NewObjectStorageLogFile(logFileId int64, segmentPrefixKey string, bucket string, objectCli *minio.Client) LogFile {
	objFile := &objectStorageLogFile{
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

func NewObjectStorageLogFileReadonly(logFileId int64, segmentPrefixKey string, bucket string, objectCli *minio.Client) LogFile {
	objFile := &objectStorageLogFile{
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
func (f *objectStorageLogFile) run() {
	ticker := time.NewTicker(time.Duration(f.maxIntervalMs * int(time.Millisecond)))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := f.Sync(context.Background())
			if err != nil {
				log.Printf("sync error:%v", err)
			}
		case <-f.fileClose:
			err := f.Sync(context.Background())
			if err != nil {
				log.Printf("sync error:%v", err)
			}
			log.Printf("logfile done")
			f.sealed.Store(true)
			return
		}
	}
}

func (f *objectStorageLogFile) GetId() int64 {
	return f.id
}

func (f *objectStorageLogFile) AppendAsync(ctx context.Context, entryId int64, data []byte) (int64, <-chan int64) {
	ch := make(chan int64, 1)
	// first check buffer size, trigger flush if necessary
	if f.buffer.expectedNextEntryId.Load()+1 >= int64(f.buffer.firstEntryId+f.buffer.maxSize) {
		fmt.Println("buffer full, trigger flush, case")
		err := f.Sync(ctx)
		if err != nil {
			// sync does not success
			ch <- -1
			return entryId, ch
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	// write buffer
	id, err := f.buffer.WriteEntry(entryId, data)
	if err != nil {
		// sync does not success
		ch <- -1
		return id, ch
	}
	f.syncedChan[id] = ch
	return id, ch
}

// Deprecated: use AppendAsync instead
func (f *objectStorageLogFile) Append(ctx context.Context, data []byte) error {
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

func (f *objectStorageLogFile) getFragmentKey(fragmentId uint64) string {
	return fmt.Sprintf("%s/%d/%d.frag", f.segmentPrefixKey, f.id, fragmentId)
}

// get the fragment for the entryId
func (f *objectStorageLogFile) getFragmentForReader(entryId int64) *FragmentObject {
	// fragmentId: 0~n
	for _, fragment := range f.fragments {
		lastEntryId, err := fragment.GetLastEntryId()
		if err != nil {
			fmt.Println("get fragment error")
			return nil
		}
		if lastEntryId >= entryId {
			return fragment
		}
	}
	// try next fragment which has been uploaded in objectStorage
	nextFragmentId := f.LastOffset() + 1
	nextFragmentKey := f.getFragmentKey(nextFragmentId)
	exists, err := f.objectExists(context.Background(), nextFragmentKey)
	if err != nil {
		return nil
	}

	if exists {
		fragment := NewObjectStorageFragment(f.client, f.bucket, nextFragmentId, nextFragmentKey, nil, 0, false, true)
		f.fragments = append(f.fragments, fragment)
		return fragment
	}
	return nil
}

// objectExists checks if an object exists in the MinIO bucket
func (f *objectStorageLogFile) objectExists(ctx context.Context, objectKey string) (bool, error) {
	_, err := f.client.StatObject(ctx, f.bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (f *objectStorageLogFile) NewReader(ctx context.Context, opt ReaderOpt) (Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if opt.StartSequenceNum < 0 || opt.StartSequenceNum >= f.getLastEntryId() {
		return nil, werr.ErrInvalidEntryId
	}

	reader := NewObjectStorageLogFileReader(opt, f)
	return reader, nil
}

// LastOffset returns the last fragmentId of the log file.
func (f *objectStorageLogFile) LastOffset() uint64 {
	if len(f.fragments) == 0 {
		return 0
	}
	return f.fragments[len(f.fragments)-1].fragmentId
}

func (f *objectStorageLogFile) getFirstEntryId() int64 {
	if len(f.fragments) == 0 {
		return 0
	}
	return f.fragments[0].firstEntryId
}

func (f *objectStorageLogFile) getLastEntryId() int64 {
	if len(f.fragments) == 0 {
		return 0
	}
	lastFrag := f.fragments[len(f.fragments)-1]
	if !lastFrag.loaded {
		err := lastFrag.Load(context.Background())
		if err != nil {
			log.Printf("get last entryId error:%v", err)
			return -1
		}
	}
	return lastFrag.lastEntryId
}

func (f *objectStorageLogFile) Sync(ctx context.Context) error {
	// Implement sync logic, e.g., flush to persistent storage
	f.mu.Lock()
	defer f.mu.Unlock()

	entryCount := len(f.buffer.values)
	if entryCount == 0 {
		log.Printf("Call Sync, but empty, skip ... ")
		return nil
	}

	// get flush point to flush
	if f.buffer.expectedNextEntryId.Load()-f.buffer.firstEntryId == 0 {
		log.Printf("Call Sync, but no data, skip ... ")
		return nil
	}

	toFlushData, err := f.buffer.ReadEntriesRange(f.buffer.firstEntryId, f.buffer.expectedNextEntryId.Load())
	if err != nil {
		log.Printf("Call Sync, but ReadEntriesRange err : %v", err)
		return err
	}
	toFlushDataFirstEntryId := f.buffer.firstEntryId
	restData, err := f.buffer.ReadEntriesToLast(f.buffer.expectedNextEntryId.Load())
	if err != nil {
		log.Printf("Call Sync, but ReadEntriesToLast err : %v", err)
		return err
	}
	restDataFirstEntryId := f.buffer.expectedNextEntryId.Load()
	f.buffer = NewSequentialBufferWithData(restDataFirstEntryId, 100_000, restData)

	// write to fragment Object
	fragId := f.LastOffset() + 1 // fragment id
	key := f.getFragmentKey(fragId)
	fragment := NewObjectStorageFragment(f.client, f.bucket, fragId, key, toFlushData, toFlushDataFirstEntryId, true, false)
	err = fragment.Flush(ctx)
	if err != nil {
		log.Printf("Call Sync, but fragment write err : %v", err)
		return err
	}
	f.fragments = append(f.fragments, fragment)
	fmt.Println("synced to object storage: ", f.id)

	// notify all waiting channels
	for syncingId, ch := range f.syncedChan {
		if syncingId < restDataFirstEntryId {
			ch <- syncingId
			delete(f.syncedChan, syncingId)
			close(ch)
		}
	}
	fmt.Println("synced to object storage completed: ", f.id)
	return nil
}

func (f *objectStorageLogFile) Close() error {
	// Implement close logic, e.g., release resources
	f.fileClose <- struct{}{}
	return nil
}

func (f *objectStorageLogFile) prefetchFragmentInfos() {
	fragId := uint64(1)
	for {
		fragKey := f.getFragmentKey(fragId)
		exists, err := f.objectExists(context.Background(), fragKey)
		if err != nil {
			// indicates that the prefetching of fragments has completed.
			//fmt.Println("object storage read fragment err: ", err)
			return
		}
		if exists {
			fragment := NewObjectStorageFragment(f.client, f.bucket, fragId, fragKey, nil, 0, false, true)
			f.fragments = append(f.fragments, fragment)
			fragId++
		} else {
			break
		}
	}
}

func NewObjectStorageLogFileReader(opt ReaderOpt, objectFile *objectStorageLogFile) Reader {
	return &objectStorageLogFileReader{
		opt:                opt,
		logfile:            objectFile,
		pendingReadEntryId: opt.StartSequenceNum,
	}
}

var _ Reader = (*objectStorageLogFileReader)(nil)

type objectStorageLogFileReader struct {
	opt     ReaderOpt
	logfile *objectStorageLogFile

	pendingReadEntryId int64
	currentFragment    *FragmentObject
}

func (o *objectStorageLogFileReader) Close() error {
	//TODO implement me
	panic("implement me")
}

func (o *objectStorageLogFileReader) ReadNext() (*proto.LogEntry, error) {
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

func (o *objectStorageLogFileReader) HasNext() bool {
	if o.pendingReadEntryId >= int64(o.opt.EndSequenceNum) && o.opt.EndSequenceNum > 0 {
		// reach the end of range
		return false
	}
	f := o.logfile.getFragmentForReader(o.pendingReadEntryId)
	if f == nil {
		// no more fragment
		return false
	}
	//
	o.currentFragment = f
	return true
}

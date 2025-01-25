package storage

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
)

var _ LogFile = (*objectStorageLogFile)(nil)

type objectStorageLogFile struct {
	id               uint64
	fragments        []*FragmentObject
	client           *minio.Client
	segmentKeyPrefix string
	bucket           string
	mu               sync.Mutex

	// write buffer
	buffer         *Buffer
	bufferSnapshot *Buffer
	maxBufferSize  int
	maxIntervalMs  int
	fileClose      chan struct{}

	// synced seqNum
	synedChan map[int]chan int
}

func NewObjectStorageLogFile(logFileId uint64, segmentKeyPrefix string, bucket string, objectCli *minio.Client) LogFile {
	objFile := &objectStorageLogFile{
		id:               logFileId,
		client:           objectCli,
		segmentKeyPrefix: segmentKeyPrefix,
		bucket:           bucket,
		fragments:        make([]*FragmentObject, 0),

		buffer:        NewBuffer(0, 0),
		maxBufferSize: 16 * 1024 * 1024,
		maxIntervalMs: 1000,
		fileClose:     make(chan struct{}),
		synedChan:     make(map[int]chan int),
	}
	go objFile.run()
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
				log.Printf("sync error:", err)
			}
		case <-f.fileClose:
			err := f.Sync(context.Background())
			if err != nil {
				fmt.Println("sync error:", err)
			}
			log.Printf("logfile done")
			return
		}
	}
}

func (f *objectStorageLogFile) GetId() uint64 {
	return f.id
}

func (f *objectStorageLogFile) AppendAsync(ctx context.Context, data []byte) (int, <-chan int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seqNo := f.buffer.WriteEntry(data)
	ch := make(chan int)
	f.synedChan[seqNo] = ch
	return seqNo, ch
}

// Deprecated: use AppendAsync instead
func (f *objectStorageLogFile) Append(ctx context.Context, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	offset := f.LastOffset() + 1
	key := f.getFragmentKey(offset)
	fragment := NewObjectStorageFragment(f.client, f.bucket, key, 0)
	err := fragment.Write(ctx, data)
	if err != nil {
		return err
	}
	f.fragments = append(f.fragments, fragment)
	return nil
}

func (f *objectStorageLogFile) getFragmentKey(fragmentOffset uint64) string {
	return fmt.Sprintf("%s/%d/%d.frag", f.segmentKeyPrefix, f.id, fragmentOffset)
}

func (f *objectStorageLogFile) NewReader(ctx context.Context, opt ReaderOpt) (Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if opt.StartSequenceNum < 0 || opt.StartSequenceNum >= f.LastOffset() {
		return nil, ErrInvalidEntryId
	}

	return nil, nil
}

func (f *objectStorageLogFile) LastOffset() uint64 {
	if len(f.fragments) == 0 {
		return 0
	}
	return uint64(f.fragments[len(f.fragments)-1].lastEntrySequence)
}

func (f *objectStorageLogFile) Sync(ctx context.Context) error {
	// Implement sync logic, e.g., flush to persistent storage
	f.mu.Lock()
	defer f.mu.Unlock()

	entryCount := len(f.buffer.entryOffsets)
	if entryCount == 0 {
		return nil
	}

	f.bufferSnapshot = f.buffer
	f.buffer = NewBuffer(0, f.bufferSnapshot.GetLastSequenceNum())

	entryData := f.bufferSnapshot.buffer
	indexData := f.bufferSnapshot.entryOffsets
	fragmentData := make([]byte, 0)
	fragmentData = append(fragmentData, intToBytes(len(indexData), 4)...) // 4 bytes for index data length
	for _, i := range indexData {
		fragmentData = append(fragmentData, intToBytes(i, 4)...) // 4 byte for entryId
	}
	fragmentData = append(fragmentData, intToBytes(len(entryData), 4)...) // 4 bytes for entry data length
	fragmentData = append(fragmentData, entryData...)

	// write to fragment Object
	offset := f.LastOffset() + 1 // fragment id
	key := f.getFragmentKey(offset)
	fragment := NewObjectStorageFragment(f.client, f.bucket, key, f.bufferSnapshot.GetLastSequenceNum())
	err := fragment.Write(ctx, fragmentData)
	if err != nil {
		return err
	}
	f.fragments = append(f.fragments, fragment)
	fmt.Println("synced to object storage ")

	// notify all waiting channels
	for seqNo, ch := range f.synedChan {
		if seqNo <= f.bufferSnapshot.GetLastSequenceNum() {
			ch <- seqNo
			delete(f.synedChan, seqNo)
		}
	}

	//
	return nil
}

func (f *objectStorageLogFile) Close() error {
	// Implement close logic, e.g., release resources
	f.fileClose <- struct{}{}
	return nil
}

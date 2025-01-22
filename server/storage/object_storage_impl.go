package storage

import (
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"sync"
)

type objectStorageLogFile struct {
	id               uint64
	fragments        []*FragmentObject
	client           *minio.Client
	segmentKeyPrefix string
	bucket           string
	mu               sync.Mutex
}

func NewObjectStorageLogFile(logFileId uint64, segmentKeyPrefix string, bucket string, objectCli *minio.Client) LogFile {
	return &objectStorageLogFile{
		id:               logFileId,
		client:           objectCli,
		segmentKeyPrefix: segmentKeyPrefix,
		bucket:           bucket,
		fragments:        make([]*FragmentObject, 0),
	}
}

func (f *objectStorageLogFile) GetId() uint64 {
	return f.id
}

func (f *objectStorageLogFile) Append(ctx context.Context, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	offset := f.LastOffset() + 1
	key := f.getFragmentKey(offset)
	fragment := NewObjectStorageFragment(f.client, f.bucket, key)
	err := fragment.Write(ctx, data)
	if err != nil {
		return err
	}
	f.fragments = append(f.fragments, fragment)
	return nil
}

func (f *objectStorageLogFile) getFragmentKey(fragmentOffset uint64) string {
	return fmt.Sprintf("%s/%d/objectKey-%d", f.segmentKeyPrefix, f.id, fragmentOffset)
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
	//f.mu.Lock() TODO should be reentrant able
	//defer f.mu.Unlock()

	if len(f.fragments) == 0 {
		// -1 is overflows for uint64
		return 0
	}
	return uint64(f.fragments[len(f.fragments)-1].lastEntrySequence)
}

func (f *objectStorageLogFile) Sync(ctx context.Context) error {
	// Implement sync logic, e.g., flush to persistent storage
	return nil
}

func (f *objectStorageLogFile) Close() error {
	// Implement close logic, e.g., release resources
	return nil
}

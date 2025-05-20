package objectstorage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/codec"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/pool"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
)

const (
	FragmentVersion = 1
)

var _ storage.Fragment = (*FragmentObject)(nil)

// FragmentObject uses MinIO for object storage.
type FragmentObject struct {
	mu     sync.RWMutex
	client minioHandler.MinioHandler

	// info
	bucket       string
	fragmentId   uint64
	fragmentKey  string
	firstEntryId int64 // First entryId in the fragment
	lastEntryId  int64 // Last entryId in the fragment, inclusive
	lastModified int64 // last modified time

	// data
	entriesData []byte // Bytes of entries
	indexes     []byte // Every 8 bytes represent one index, where each index consists of an offset and a length. The high 32 bits represent the offset, and the low 32 bits represent the length.
	size        int64  // Size of the fragment, including entries and indexes. it is lock free, used for metrics only

	// status
	dataLoaded   bool // If this fragment has been loaded to memory
	dataUploaded bool // If this fragment has been uploaded to MinIO
	infoFetched  bool // if info of this fragment has been fetched from MinIO
}

// NewFragmentObject initializes a new FragmentObject.
func NewFragmentObject(client minioHandler.MinioHandler, bucket string, fragmentId uint64, fragmentKey string, entries [][]byte, firstEntryId int64, dataLoaded, dataUploaded, infoFetched bool) *FragmentObject {
	index, data := genFragmentDataFromRaw(entries)
	lastEntryId := firstEntryId + int64(len(entries)) - 1
	size := int64(len(data) + len(index))
	metrics.WpFragmentBufferBytes.WithLabelValues(bucket).Add(float64(len(data) + len(index)))
	metrics.WpFragmentLoadedGauge.WithLabelValues(bucket).Inc()
	return &FragmentObject{
		client:       client,
		bucket:       bucket,
		fragmentId:   fragmentId,
		fragmentKey:  fragmentKey,
		entriesData:  data,
		indexes:      index,
		size:         size,
		firstEntryId: firstEntryId,
		lastEntryId:  lastEntryId,
		lastModified: time.Now().UnixMilli(),
		dataLoaded:   dataLoaded,
		dataUploaded: dataUploaded,
		infoFetched:  infoFetched,
	}
}

func (f *FragmentObject) GetFragmentId() int64 {
	// This field is immutable after initialization, so no lock is needed
	return int64(f.fragmentId)
}

func (f *FragmentObject) GetFragmentKey() string {
	// This field is immutable after initialization, so no lock is needed
	return f.fragmentKey
}

func (f *FragmentObject) GetSize() int64 {
	return f.size
}

// Flush uploads the data to MinIO.
func (f *FragmentObject) Flush(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dataLoaded {
		return werr.ErrFragmentEmpty
	}

	if len(f.entriesData) == 0 {
		return werr.ErrFragmentEmpty
	}

	start := time.Now()
	fullData, err := serializeFragment(f)
	if err != nil {
		return err
	}
	_, err = f.client.PutObject(ctx, f.bucket, f.fragmentKey, bytes.NewReader(fullData), int64(len(fullData)), minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}
	cost := time.Now().Sub(start)
	metrics.WpFragmentFlushBytes.WithLabelValues(f.bucket).Observe(float64(len(fullData)))
	metrics.WpFragmentFlushLatency.WithLabelValues(f.bucket).Observe(float64(cost.Milliseconds()))
	f.dataUploaded = true

	// Put back to pool
	pool.PutByteBuffer(fullData)
	return nil
}

// Load reads the data from MinIO.
func (f *FragmentObject) Load(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dataLoaded {
		// already loaded, no need to load again
		return nil
	}
	if !f.dataUploaded {
		return werr.ErrFragmentNotUploaded
	}

	fragObjectReader, objDataSize, objLastModified, err := f.client.GetObjectDataAndInfo(ctx, f.bucket, f.fragmentKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object: %w", err)
	}
	defer fragObjectReader.Close()
	f.lastModified = objLastModified

	// Read the entire object into memory
	data, err := minioHandler.ReadObjectFull(fragObjectReader, objDataSize)
	if err != nil || len(data) != int(objDataSize) {
		return fmt.Errorf("failed to read object: %v", err)
	}

	tmpFrag, deserializeErr := deserializeFragment(data, objDataSize)
	if deserializeErr != nil {
		return deserializeErr
	}

	// Reset the buffer with the loaded data
	f.entriesData = tmpFrag.entriesData
	f.indexes = tmpFrag.indexes
	f.size = int64(len(tmpFrag.entriesData) + len(tmpFrag.indexes))
	f.firstEntryId = tmpFrag.firstEntryId
	f.lastEntryId = tmpFrag.lastEntryId
	f.dataLoaded = true
	f.dataUploaded = true
	f.infoFetched = true

	// update metrics
	metrics.WpFragmentBufferBytes.WithLabelValues(f.bucket).Add(float64(len(f.entriesData) + len(f.indexes)))
	metrics.WpFragmentLoadedGauge.WithLabelValues(f.bucket).Inc()

	// update cache
	return cache.AddCacheFragment(ctx, f)
}

func (f *FragmentObject) GetLastEntryId() (int64, error) {
	// First check with read lock
	f.mu.RLock()
	if f.infoFetched {
		lastId := f.lastEntryId
		f.mu.RUnlock()
		return lastId, nil
	}

	// If we need to load data, we'll need to acquire the lock again
	if !f.dataLoaded && f.dataUploaded {
		f.mu.RUnlock()
		err := f.Load(context.Background())
		if err != nil {
			return -1, err
		}
		f.mu.RLock()
	}

	// Check again with read lock
	defer f.mu.RUnlock()
	if !f.infoFetched {
		return -1, errors.New("fragment no data&info to load")
	}
	return f.lastEntryId, nil
}

func (f *FragmentObject) GetFirstEntryId() (int64, error) {
	// First check with read lock
	f.mu.RLock()
	if f.infoFetched {
		firstId := f.firstEntryId
		f.mu.RUnlock()
		return firstId, nil
	}

	// If we need to load data, we'll need to acquire the lock again
	if !f.dataLoaded && f.dataUploaded {
		f.mu.RUnlock()
		err := f.Load(context.Background())
		if err != nil {
			return -1, err
		}
		f.mu.RLock()
	}

	// Check again with read lock
	defer f.mu.RUnlock()
	if !f.infoFetched {
		return -1, errors.New("fragment no data&info to load")
	}
	return f.firstEntryId, nil
}

func (f *FragmentObject) GetLastModified() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastModified
}

func (f *FragmentObject) GetEntry(entryId int64) ([]byte, error) {
	// First check if we need to load data
	f.mu.RLock()
	dataLoaded := f.dataLoaded
	dataUploaded := f.dataUploaded
	f.mu.RUnlock()

	if !dataLoaded && dataUploaded {
		err := f.Load(context.Background())
		if err != nil {
			return nil, err
		}
	}

	// Now read with lock held
	f.mu.RLock()
	defer f.mu.RUnlock()
	dataLoaded = f.dataLoaded

	if !f.dataLoaded {
		return nil, errors.New("fragment no data to load")
	}

	relatedIdx := (entryId - f.firstEntryId) * 8
	if relatedIdx+8 > int64(len(f.indexes)) {
		return nil, werr.ErrEntryNotFound
	}

	entryOffset := binary.BigEndian.Uint32(f.indexes[relatedIdx : relatedIdx+4])
	entryLength := binary.BigEndian.Uint32(f.indexes[relatedIdx+4 : relatedIdx+8])

	// Create a copy of the data to avoid potential race conditions after lock release
	result := make([]byte, entryLength)
	copy(result, f.entriesData[entryOffset:entryOffset+entryLength])

	return result, nil
}

// Release releases the memory used by the fragment.
func (f *FragmentObject) Release() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dataLoaded {
		// empty, no need to release again
		return nil
	}
	metrics.WpFragmentBufferBytes.WithLabelValues(f.bucket).Sub(float64(len(f.entriesData) + len(f.indexes)))
	metrics.WpFragmentLoadedGauge.WithLabelValues(f.bucket).Dec()

	// Return buffers to the pool
	if f.entriesData != nil {
		pool.PutByteBuffer(f.entriesData)
	}
	if f.indexes != nil {
		pool.PutByteBuffer(f.indexes)
	}

	f.indexes = nil
	f.entriesData = nil
	f.dataLoaded = false
	return nil
}

// mergeFragmentsAndReleaseAfterCompleted merge fragments and release after completed
func mergeFragmentsAndReleaseAfterCompleted(ctx context.Context, mergedFragKey string, mergeFragId uint64, fragments []*FragmentObject, releaseImmediately bool) (storage.Fragment, error) {
	// check args
	if len(fragments) == 0 {
		return nil, errors.New("no fragments to merge")
	}

	// merge
	mergedFrag := &FragmentObject{
		client:       fragments[0].client,
		bucket:       fragments[0].bucket,
		fragmentId:   mergeFragId,
		fragmentKey:  mergedFragKey,
		entriesData:  make([]byte, 0),
		indexes:      make([]byte, 0),
		firstEntryId: fragments[0].firstEntryId,
		lastEntryId:  fragments[len(fragments)-1].lastEntryId,
		dataUploaded: false,
		dataLoaded:   false,
		infoFetched:  true,
	}
	expectedEntryId := int64(-1)
	fragIds := make([]uint64, 0)
	for _, fragment := range fragments {
		err := fragment.Load(ctx)
		if err != nil {
			return nil, err
		}
		// check the order of entries
		if expectedEntryId == -1 {
			// the first segment
			expectedEntryId = fragment.lastEntryId + 1
		} else {
			if expectedEntryId != fragment.firstEntryId {
				return nil, errors.New("fragments are not in order")
			}
			expectedEntryId = fragment.lastEntryId + 1
		}
		// merge index
		baseOffset := len(mergedFrag.entriesData)
		for index := 0; index < len(fragment.indexes); index = index + 8 {
			newEntryOffset := binary.BigEndian.Uint32(fragment.indexes[index:index+4]) + uint32(baseOffset)
			entryLength := binary.BigEndian.Uint32(fragment.indexes[index+4 : index+8])

			newIndex := make([]byte, 8)
			binary.BigEndian.PutUint32(newIndex[:4], newEntryOffset)
			binary.BigEndian.PutUint32(newIndex[4:], entryLength)

			mergedFrag.indexes = append(mergedFrag.indexes, newIndex...)
		}
		// merge data
		mergedFrag.entriesData = append(mergedFrag.entriesData, fragment.entriesData...)
		fragIds = append(fragIds, fragment.fragmentId)
	}

	// set data cache ready
	mergedFrag.dataLoaded = true

	// upload the mergedFragment
	flushErr := mergedFrag.Flush(ctx)
	if flushErr != nil {
		return nil, flushErr
	}
	// set flag
	mergedFrag.dataUploaded = true
	mergedFrag.infoFetched = true

	// release immediately
	if releaseImmediately {
		// release immediately
		mergedFrag.entriesData = nil
		mergedFrag.indexes = nil
		mergedFrag.dataLoaded = false
	}

	logger.Ctx(ctx).Info("merge fragments and release after completed", zap.String("mergedFragKey", mergedFrag.fragmentKey), zap.Uint64("mergeFragId", mergeFragId), zap.Uint64s("fragmentIds", fragIds))
	return mergedFrag, nil
}

// serializeFragment to object data bytes
func serializeFragment(f *FragmentObject) ([]byte, error) {
	// Calculate required space
	headerSize := 24 // 3 int64 fields (version+firstEntryId+lastEntryId)
	totalSize := headerSize + len(f.indexes) + len(f.entriesData)

	// Get buffer from pool
	fullData := pool.GetByteBuffer(totalSize)

	// Write header information
	fullData = append(fullData, codec.Int64ToBytes(FragmentVersion)...)
	fullData = append(fullData, codec.Int64ToBytes(f.firstEntryId)...)
	fullData = append(fullData, codec.Int64ToBytes(f.lastEntryId)...)

	// Write indexes and data
	fullData = append(fullData, f.indexes...)
	fullData = append(fullData, f.entriesData...)

	return fullData, nil
}

// deserializeFragment from object data bytes
func deserializeFragment(data []byte, maxFragmentSize int64) (*FragmentObject, error) {
	// Create a buffer to read from the data
	buf := bytes.NewBuffer(data)

	// Read version (4 bytes)
	var version uint64
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read version: %v", err)
	}
	if version != FragmentVersion {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	// Read firstEntryID (8 bytes)
	var firstEntryID uint64
	if err := binary.Read(buf, binary.BigEndian, &firstEntryID); err != nil {
		return nil, fmt.Errorf("failed to read firstEntryID: %v", err)
	}

	// Read lastEntryId (8 bytes)
	var lastEntryId uint64
	if err := binary.Read(buf, binary.BigEndian, &lastEntryId); err != nil {
		return nil, fmt.Errorf("failed to read lastEntryId: %v", err)
	}

	// Calculate the number of indexes
	numIndexes := lastEntryId - firstEntryID + 1
	indexes := make([]byte, numIndexes*8)

	// Read indexes (each index is 8 bytes)
	if err := binary.Read(buf, binary.BigEndian, &indexes); err != nil {
		return nil, fmt.Errorf("failed to read index %v", err)
	}
	// Read entriesData (remaining data)
	entriesData := buf.Bytes()

	return &FragmentObject{
		indexes:      indexes,
		entriesData:  entriesData,
		firstEntryId: int64(firstEntryID),
		lastEntryId:  int64(lastEntryId),
	}, nil
}

func genFragmentDataFromRaw(rawEntries [][]byte) ([]byte, []byte) {
	// Calculate total data size
	entriesCount := len(rawEntries)
	if entriesCount == 0 {
		return make([]byte, 0), make([]byte, 0)
	}

	// Calculate total data and index size
	totalDataSize := 0
	for _, entry := range rawEntries {
		totalDataSize += len(entry)
	}
	indexSize := entriesCount * 8 // Each index entry occupies 8 bytes (offset+length)

	// Get buffers from byte pool
	data := pool.GetByteBuffer(totalDataSize)
	index := pool.GetByteBuffer(indexSize)

	// Temporary index buffer, reuse to reduce memory allocation
	entryIndex := make([]byte, 8)

	// Fill data and indexes
	offset := 0
	for i := 0; i < entriesCount; i++ {
		entry := rawEntries[i]
		entryLength := uint32(len(entry))
		entryOffset := uint32(offset)

		// Fill index data
		binary.BigEndian.PutUint32(entryIndex[:4], entryOffset)
		binary.BigEndian.PutUint32(entryIndex[4:], entryLength)

		// Add to index buffer
		index = append(index, entryIndex...)

		// Add to data buffer
		data = append(data, entry...)

		offset += len(entry)
	}

	// No need to return buffers before returning, as they will be held by FragmentObject
	// They will be returned to the pool when FragmentObject is released
	return index, data
}

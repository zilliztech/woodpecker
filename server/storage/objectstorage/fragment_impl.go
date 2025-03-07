package objectstorage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/codec"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
)

const (
	FragmentVersion = 1
)

var _ storage.Fragment = (*FragmentObject)(nil)

// FragmentObject uses MinIO for object storage.
type FragmentObject struct {
	client      minioHandler.MinioHandler
	bucket      string
	fragmentId  uint64
	fragmentKey string

	entriesData  []byte // Bytes of entries
	indexes      []byte // Every 8 bytes represent one index, where each index consists of an offset and a length. The high 32 bits represent the offset, and the low 32 bits represent the length.
	firstEntryId int64  // First entryId in the fragment
	lastEntryId  int64  // Last entryId in the fragment, inclusive

	lastModified int64 // last modified time

	// status
	loaded   bool // If this fragment has been loaded to memory
	uploaded bool // If this fragment has been uploaded to MinIO
}

// NewFragmentObject initializes a new FragmentObject.
func NewFragmentObject(client minioHandler.MinioHandler, bucket string, fragmentId uint64, fragmentKey string, entries [][]byte, firstEntryId int64, loaded, uploaded bool) *FragmentObject {
	data := make([]byte, 0)
	index := make([]byte, 0)
	offset := 0
	for i := 0; i < len(entries); i++ {
		entryLength := uint32(len(entries[i]))
		entryOffset := offset
		entryIndex := make([]byte, 8)
		binary.BigEndian.PutUint32(entryIndex[:4], uint32(entryOffset))
		binary.BigEndian.PutUint32(entryIndex[4:], entryLength)

		data = append(data, entries[i]...)
		index = append(index, entryIndex...)

		offset = offset + len(entries[i])
	}
	lastEntryId := firstEntryId + int64(len(entries)) - 1
	metrics.WpFragmentBufferBytes.WithLabelValues(bucket).Add(float64(len(data) + len(index)))
	metrics.WpFragmentLoadedGauge.WithLabelValues(bucket).Inc()
	return &FragmentObject{
		client:       client,
		bucket:       bucket,
		fragmentId:   fragmentId,
		fragmentKey:  fragmentKey,
		entriesData:  data,
		indexes:      index,
		firstEntryId: firstEntryId,
		lastEntryId:  lastEntryId,
		lastModified: time.Now().UnixMilli(),
		loaded:       loaded,
		uploaded:     uploaded,
	}
}

// Flush uploads the data to MinIO.
func (f *FragmentObject) Flush(ctx context.Context) error {
	if !f.loaded {
		return werr.ErrFragmentEmpty
	}

	if len(f.entriesData) == 0 {
		return werr.ErrFragmentEmpty
	}

	start := time.Now()
	fullData, err := SerializeFragment(f)
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
	f.uploaded = true
	return nil
}

// Load reads the data from MinIO.
func (f *FragmentObject) Load(ctx context.Context) error {
	if f.loaded {
		// already loaded, no need to load again
		return nil
	}
	if !f.uploaded {
		return werr.ErrFragmentNotUploaded
	}

	fragObjectReader, objLastModified, err := f.client.GetObjectDataAndInfo(ctx, f.bucket, f.fragmentKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object: %w", err)
	}
	f.lastModified = objLastModified
	// Read the entire object into memory
	data, err := io.ReadAll(fragObjectReader)
	if err != nil {
		return fmt.Errorf("failed to read object: %v", err)
	}
	tmpFrag, deserializeErr := DeserializeFragment(data)
	if deserializeErr != nil {
		return deserializeErr
	}

	// Reset the buffer with the loaded data
	f.entriesData = tmpFrag.entriesData
	f.indexes = tmpFrag.indexes
	f.firstEntryId = tmpFrag.firstEntryId
	f.lastEntryId = tmpFrag.lastEntryId
	tmpFrag.Release() // release

	//
	f.loaded = true
	metrics.WpFragmentBufferBytes.WithLabelValues(f.bucket).Add(float64(len(f.entriesData) + len(f.indexes)))
	metrics.WpFragmentLoadedGauge.WithLabelValues(f.bucket).Inc()
	return nil
}

func (f *FragmentObject) GetLastEntryId() (int64, error) {
	if !f.loaded && f.uploaded {
		err := f.Load(context.Background())
		if err != nil {
			return -1, err
		}
	}
	if !f.loaded {
		return -1, errors.New("fragment no data to load")
	}
	return f.lastEntryId, nil
}

// only for merged fragment, TODO
func (f *FragmentObject) GetLastEntryIdDirectly() int64 {
	return f.lastEntryId
}

// only for merged fragment, TODO
func (f *FragmentObject) GetFirstEntryIdDirectly() int64 {
	return f.firstEntryId
}

func (f *FragmentObject) GetLastModified() int64 {
	return f.lastModified
}

func (f *FragmentObject) GetEntry(entryId int64) ([]byte, error) {
	if !f.loaded && f.uploaded {
		err := f.Load(context.Background())
		if err != nil {
			return nil, err
		}
	}
	if !f.loaded {
		return nil, errors.New("fragment no data to load")
	}
	relatedIdx := (entryId - f.firstEntryId) * 8
	if relatedIdx+8 > int64(len(f.indexes)) {
		return nil, werr.ErrEntryNotFound
	}
	entryOffset := binary.BigEndian.Uint32(f.indexes[relatedIdx : relatedIdx+4])
	entryLength := binary.BigEndian.Uint32(f.indexes[relatedIdx+4 : relatedIdx+8])
	return f.entriesData[entryOffset : entryOffset+entryLength], nil
}

// Release releases the memory used by the fragment.
func (f *FragmentObject) Release() error {
	if !f.loaded {
		// empty, no need to release again
		return nil
	}
	metrics.WpFragmentBufferBytes.WithLabelValues(f.bucket).Sub(float64(len(f.entriesData) + len(f.indexes)))
	metrics.WpFragmentLoadedGauge.WithLabelValues(f.bucket).Dec()
	f.indexes = nil
	f.entriesData = nil
	f.loaded = false
	return nil
}

// MergeFragmentsAndReleaseAfterCompleted merge fragments and release after completed
func MergeFragmentsAndReleaseAfterCompleted(ctx context.Context, mergedFragKey string, mergeFragId uint64, fragments []*FragmentObject) (storage.Fragment, error) {
	// release pending merge fragments before finish
	defer ReleaseFragments(ctx, fragments)
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
		uploaded:     false,
		loaded:       false,
	}
	expectedEntryId := int64(-1)
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
	}

	// upload the mergedFragment
	mergedFrag.loaded = true
	flushErr := mergedFrag.Flush(ctx)
	if flushErr != nil {
		return nil, flushErr
	}
	// mark uploaded
	mergedFrag.uploaded = true

	//
	return mergedFrag, nil
}

func ReleaseFragments(ctx context.Context, fragments []*FragmentObject) {
	for _, fragment := range fragments {
		err := fragment.Release()
		if err != nil {
			logger.Ctx(ctx).Warn("release fragment failed when LogFile closing", zap.String("fragmentKey", fragment.fragmentKey), zap.Uint64("fragmentId", fragment.fragmentId), zap.Error(err))
		}
	}
}

// SerializeFragment to object data bytes
func SerializeFragment(f *FragmentObject) ([]byte, error) {
	fullData := make([]byte, 0)
	fullData = append(fullData, codec.Int64ToBytes(FragmentVersion)...)
	fullData = append(fullData, codec.Int64ToBytes(f.firstEntryId)...)
	fullData = append(fullData, codec.Int64ToBytes(f.lastEntryId)...)
	fullData = append(fullData, f.indexes...)
	fullData = append(fullData, f.entriesData...)
	return fullData, nil
}

// DeserializeFragment from object data bytes
func DeserializeFragment(data []byte) (*FragmentObject, error) {
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

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

	"github.com/zilliztech/woodpecker/common/codec"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
)

// FragmentObject uses MinIO for object storage.
type FragmentObject struct {
	client      *minio.Client
	bucket      string
	fragmentId  uint64
	fragmentKey string

	entriesData  []byte // Bytes of entries
	indexes      []byte // Every 8 bytes represent one index, where each index consists of an offset and a length. The high 32 bits represent the offset, and the low 32 bits represent the length.
	firstEntryId int64  // First entryId in the fragment
	lastEntryId  int64  // Last entryId in the fragment, inclusive

	// status
	loaded   bool // If this fragment has been loaded to memory
	uploaded bool // If this fragment has been uploaded to MinIO
}

// NewFragmentObject initializes a new FragmentObject.
func NewFragmentObject(client *minio.Client, bucket string, fragmentId uint64, fragmentKey string, entries [][]byte, firstEntryId int64, loaded, uploaded bool) *FragmentObject {
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
		loaded:       loaded,
		uploaded:     uploaded,
	}
}

// Flush uploads the data to MinIO.
func (f *FragmentObject) Flush(ctx context.Context) error {
	if !f.loaded {
		return fmt.Errorf("fragment is empty")
	}
	start := time.Now()
	fullData := make([]byte, 0)
	fullData = append(fullData, codec.Int64ToBytes(1)...)
	fullData = append(fullData, codec.Int64ToBytes(f.firstEntryId)...)
	fullData = append(fullData, codec.Int64ToBytes(f.lastEntryId)...)
	fullData = append(fullData, f.indexes...)
	fullData = append(fullData, f.entriesData...)

	_, err := f.client.PutObject(ctx, f.bucket, f.fragmentKey, bytes.NewReader(fullData), int64(len(fullData)), minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}
	cost := time.Now().Sub(start)
	metrics.WpFragmentFlushBytes.WithLabelValues(f.bucket).Observe(float64(len(fullData)))
	metrics.WpFragmentFlushLatency.WithLabelValues(f.bucket).Observe(float64(cost.Milliseconds()))
	f.uploaded = true

	// TODO test only , immediately release memory
	f.Release()

	return nil
}

// Load reads the data from MinIO.
func (f *FragmentObject) Load(ctx context.Context) error {
	if f.loaded {
		// already loaded, no need to load again
		return nil
	}
	if !f.uploaded {
		return fmt.Errorf("fragment is not uploaded")
	}

	fragObject, err := f.client.GetObject(ctx, f.bucket, f.fragmentKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object: %w", err)
	}
	defer fragObject.Close()

	// Read the entire object into memory
	data, err := io.ReadAll(fragObject)
	if err != nil {
		return fmt.Errorf("failed to read object: %v", err)
	}

	// Create a buffer to read from the data
	buf := bytes.NewBuffer(data)

	// Read version (4 bytes)
	var version uint64
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return fmt.Errorf("failed to read version: %v", err)
	}

	// Read firstEntryID (8 bytes)
	var firstEntryID uint64
	if err := binary.Read(buf, binary.BigEndian, &firstEntryID); err != nil {
		return fmt.Errorf("failed to read firstEntryID: %v", err)
	}

	// Read lastEntryId (8 bytes)
	var lastEntryId uint64
	if err := binary.Read(buf, binary.BigEndian, &lastEntryId); err != nil {
		return fmt.Errorf("failed to read lastEntryId: %v", err)
	}

	// Calculate the number of indexes
	numIndexes := lastEntryId - firstEntryID + 1
	indexes := make([]byte, numIndexes*8)

	// Read indexes (each index is 8 bytes)
	if err = binary.Read(buf, binary.BigEndian, &indexes); err != nil {
		return fmt.Errorf("failed to read index %v", err)
	}

	// Read entriesData (remaining data)
	entriesData := buf.Bytes()

	// Reset the buffer with the loaded data
	f.entriesData = entriesData
	f.indexes = indexes
	f.firstEntryId = int64(firstEntryID)
	f.lastEntryId = int64(lastEntryId)

	//
	f.loaded = true
	metrics.WpFragmentBufferBytes.WithLabelValues(f.bucket).Add(float64(len(entriesData) + len(indexes)))
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

package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/minio/minio-go/v7"
	"io"
)

var _ Fragment = (*FragmentObject)(nil)

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

// NewObjectStorageFragment initializes a new FragmentObject.
func NewObjectStorageFragment(client *minio.Client, bucket string, fragmentId uint64, fragmentKey string, entries [][]byte, firstEntryId int64, loaded, uploaded bool) *FragmentObject {
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

// NewReader retrieves the object from MinIO.
func (osf *FragmentObject) Read(ctx context.Context, opt ReaderOpt) ([]*LogEntry, error) {
	//object, err := osf.client.GetObject(ctx, osf.bucket, osf.objectKey, minio.GetObjectOptions{})
	//if err != nil {
	//	return nil, fmt.Errorf("failed to get object: %w", err)
	//}

	//1. read index data from tail of the object
	//offset := int64(0)

	// 2. find the fileLastOffset of the data in the object
	// Seek to the specified fileLastOffset
	//return object, nil
	panic("to implement me")
}

// Write uploads the data to MinIO.
func (osf *FragmentObject) Write(ctx context.Context, data []byte) error {
	if !osf.loaded {
		return fmt.Errorf("fragment is empty")
	}
	fullData := make([]byte, 0)
	fullData = append(fullData, int64ToBytes(1)...)
	fullData = append(fullData, int64ToBytes(osf.firstEntryId)...)
	fullData = append(fullData, int64ToBytes(osf.lastEntryId)...)
	fullData = append(fullData, osf.indexes...)
	fullData = append(fullData, osf.entriesData...)

	_, err := osf.client.PutObject(ctx, osf.bucket, osf.fragmentKey, bytes.NewReader(fullData), int64(len(fullData)), minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}
	osf.uploaded = true
	return nil
}

func (osf *FragmentObject) Load(ctx context.Context) error {
	if osf.loaded {
		// already loaded, no need to load again
		return nil
	}
	if !osf.uploaded {
		return fmt.Errorf("fragment is not uploaded")
	}

	fragObject, err := osf.client.GetObject(ctx, osf.bucket, osf.fragmentKey, minio.GetObjectOptions{})
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
	numIndexes := (lastEntryId - firstEntryID + 1)
	indexes := make([]byte, numIndexes*8)

	// Read indexes (each index is 8 bytes)
	if err = binary.Read(buf, binary.BigEndian, &indexes); err != nil {
		return fmt.Errorf("failed to read index %v", err)
	}

	// Read entriesData (remaining data)
	entriesData := buf.Bytes()

	// Reset the buffer with the loaded data
	osf.entriesData = entriesData
	osf.indexes = indexes
	osf.firstEntryId = int64(firstEntryID)
	osf.lastEntryId = int64(lastEntryId)

	//
	osf.loaded = true
	return nil
}

func (osf *FragmentObject) GetLastEntryId() (int64, error) {
	if !osf.loaded && osf.uploaded {
		err := osf.Load(context.Background())
		if err != nil {
			return -1, err
		}
	}
	if !osf.loaded {
		return -1, errors.New("fragment no data to load")
	}
	return osf.lastEntryId, nil
}

func (osf *FragmentObject) GetEntry(entryId int64) ([]byte, error) {
	if !osf.loaded && osf.uploaded {
		err := osf.Load(context.Background())
		if err != nil {
			return nil, err
		}
	}
	if !osf.loaded {
		return nil, errors.New("fragment no data to load")
	}
	relatedIdx := (entryId - osf.firstEntryId) * 8
	entryOffset := binary.BigEndian.Uint32(osf.indexes[relatedIdx : relatedIdx+4])
	entryLength := binary.BigEndian.Uint32(osf.indexes[relatedIdx+4 : relatedIdx+8])
	return osf.entriesData[entryOffset : entryOffset+entryLength], nil
}

func (osf *FragmentObject) Release() error {
	if !osf.loaded {
		// empty, no need to release again
		return nil
	}
	osf.indexes = nil
	osf.entriesData = nil
	osf.loaded = false
	return nil
}

func (osf *FragmentObject) Close() error {
	//TODO implement me
	panic("implement me")
}

func (osf *FragmentObject) Flush(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

// Footer serialization to bytes
// TODO reduce memory copy, directly write to the file
func (osf *FragmentObject) encodeFooter() ([]byte, error) {
	//var buf bytes.Buffer
	//if err := binary.Write(&buf, binary.LittleEndian, osf.footer.EntryOffset); err != nil {
	//	return nil, err
	//}
	//
	//osf.footer.CRC = crc32.ChecksumIEEE(buf.Bytes())
	//if err := binary.Write(&buf, binary.LittleEndian, osf.footer.CRC); err != nil {
	//	return nil, err
	//}
	//
	//if err := binary.Write(&buf, binary.LittleEndian, osf.footer.IndexSize); err != nil {
	//	return nil, err
	//}
	//return buf.Bytes(), nil
	//TODO implement me
	panic("implement me")
}

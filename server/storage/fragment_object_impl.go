package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/minio/minio-go/v7"
)

var _ Fragment = (*FragmentObject)(nil)

// FragmentObject uses MinIO for object storage.
type FragmentObject struct {
	client    *minio.Client
	bucket    string
	objectKey string

	footer            Footer
	lastEntrySequence uint32 // Last entry sequence number, starts from 0 that corresponds to the first index item
}

// NewObjectStorageFragment initializes a new FragmentObject.
func NewObjectStorageFragment(client *minio.Client, bucket, objectKey string) *FragmentObject {
	return &FragmentObject{
		client:    client,
		bucket:    bucket,
		objectKey: objectKey,
	}
}

// NewReader retrieves the object from MinIO.
func (osf *FragmentObject) Read(ctx context.Context, opt ReaderOpt) ([]*LogEntry, error) {
	//object, err := osf.client.GetObject(ctx, osf.bucket, osf.objectKey, minio.GetObjectOptions{})
	//if err != nil {
	//	return nil, fmt.Errorf("failed to get object: %w", err)
	//}
	//
	//// 1. read index data from tail of the object
	//offset := int64(0)
	//
	//// 2. find the fileLastOffset of the data in the object
	//// Seek to the specified fileLastOffset
	//return object, nil
	panic("implement me")
}

// Write uploads the data to MinIO.
func (osf *FragmentObject) Write(ctx context.Context, data []byte) error {
	_, err := osf.client.PutObject(ctx, osf.bucket, osf.objectKey, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}
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
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, osf.footer.EntryOffset); err != nil {
		return nil, err
	}

	osf.footer.CRC = crc32.ChecksumIEEE(buf.Bytes())
	if err := binary.Write(&buf, binary.LittleEndian, osf.footer.CRC); err != nil {
		return nil, err
	}

	if err := binary.Write(&buf, binary.LittleEndian, osf.footer.IndexSize); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

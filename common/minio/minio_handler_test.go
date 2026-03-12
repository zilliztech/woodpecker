// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package minio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
)

// testFileReader implements FileReader interface for testing
type testFileReader struct {
	data        []byte
	position    int
	maxReadSize int
	readError   error
}

func (t *testFileReader) Read(p []byte) (int, error) {
	if t.readError != nil {
		return 0, t.readError
	}

	if t.position >= len(t.data) {
		return 0, io.EOF
	}

	// Limit read size if specified
	readLen := len(p)
	if t.maxReadSize > 0 && readLen > t.maxReadSize {
		readLen = t.maxReadSize
	}

	// Calculate how much we can actually read
	remaining := len(t.data) - t.position
	if readLen > remaining {
		readLen = remaining
	}

	// Copy data and update position
	n := copy(p[:readLen], t.data[t.position:t.position+readLen])
	t.position += n

	var err error
	if t.position >= len(t.data) {
		err = io.EOF
	}

	return n, err
}

func (t *testFileReader) Close() error {
	return nil
}

func (t *testFileReader) ReadAt(p []byte, off int64) (int, error) {
	// Simple implementation for interface compliance
	return 0, io.ErrUnexpectedEOF
}

func (t *testFileReader) Seek(offset int64, whence int) (int64, error) {
	// Simple implementation for interface compliance
	return 0, io.ErrUnexpectedEOF
}

func (t *testFileReader) Size() (int64, error) {
	return int64(len(t.data)), nil
}

func TestIsPreconditionFailed(t *testing.T) {
	// Not a minio error → false
	assert.False(t, IsPreconditionFailed(errors.New("random error")))

	// PreconditionFailed
	precondErr := minio.ErrorResponse{Code: "PreconditionFailed"}
	assert.True(t, IsPreconditionFailed(precondErr))

	// FileAlreadyExists
	existsErr := minio.ErrorResponse{Code: "FileAlreadyExists"}
	assert.True(t, IsPreconditionFailed(existsErr))

	// Some other minio error
	otherErr := minio.ErrorResponse{Code: "NoSuchBucket"}
	assert.False(t, IsPreconditionFailed(otherErr))
}

func TestIsObjectNotExists(t *testing.T) {
	assert.False(t, IsObjectNotExists(errors.New("random")))

	noKeyErr := minio.ErrorResponse{Code: "NoSuchKey"}
	assert.True(t, IsObjectNotExists(noKeyErr))

	otherErr := minio.ErrorResponse{Code: "PreconditionFailed"}
	assert.False(t, IsObjectNotExists(otherErr))
}

func TestIsFencedObject(t *testing.T) {
	// Fenced
	fenced := minio.ObjectInfo{
		UserMetadata: map[string]string{FencedObjectMetaKey: "true"},
	}
	assert.True(t, IsFencedObject(fenced))

	// Not fenced — value is "false"
	notFenced := minio.ObjectInfo{
		UserMetadata: map[string]string{FencedObjectMetaKey: "false"},
	}
	assert.False(t, IsFencedObject(notFenced))

	// Not fenced — key missing
	noKey := minio.ObjectInfo{
		UserMetadata: map[string]string{},
	}
	assert.False(t, IsFencedObject(noKey))

	// Not fenced — nil metadata
	nilMeta := minio.ObjectInfo{}
	assert.False(t, IsFencedObject(nilMeta))
}

func TestNewMinioHandlerWithClient(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	// Create a client with a dummy endpoint (no actual connection)
	client, err := minio.New("localhost:9999", &minio.Options{
		Secure: false,
	})
	assert.NoError(t, err)

	handler, err := NewMinioHandlerWithClient(context.Background(), cfg, client)
	assert.NoError(t, err)
	assert.NotNil(t, handler)
}

func TestReadObjectFull(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		readSize int // Size of each read block
		wantErr  bool
	}{
		{
			name:     "Empty data",
			input:    []byte{},
			readSize: 1024,
			wantErr:  false,
		},
		{
			name:     "Small data read at once",
			input:    []byte("hello world"),
			readSize: 1024,
			wantErr:  false,
		},
		{
			name:     "Large data read in chunks",
			input:    bytes.Repeat([]byte("abcdefgh"), 1000), // 8KB
			readSize: 1024,                                   // Read 1KB each time, requires multiple reads
			wantErr:  false,
		},
		{
			name:     "Very small block read",
			input:    bytes.Repeat([]byte("xyz"), 100), // 300 bytes
			readSize: 7,                                // Not a multiple of 8, testing edge cases
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test reader with specified read size limit
			reader := &testFileReader{
				data:        tt.input,
				maxReadSize: tt.readSize,
			}

			// Call the function being tested
			got, err := ReadObjectFull(context.TODO(), reader, 1024*1024, "test-ns", "0")

			// Verify results
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.input, got, "Read data does not match input")
			}
		})
	}

	// Test read error
	t.Run("Read error", func(t *testing.T) {
		reader := &testFileReader{
			readError: io.ErrClosedPipe, // Simulate a read error
		}

		_, err := ReadObjectFull(context.TODO(), reader, 1024*1024, "test-ns", "0")
		assert.Error(t, err)
		assert.Equal(t, io.ErrClosedPipe, err)
	})
}

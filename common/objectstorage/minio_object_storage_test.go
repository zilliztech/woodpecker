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

package objectstorage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	mocks_minio "github.com/zilliztech/woodpecker/mocks/mocks_minio"
)

// helper to create a MinioObjectStorage with a mock handler
func newTestMinioObjectStorage(t *testing.T) (*MinioObjectStorage, *mocks_minio.MinioHandler) {
	mockHandler := mocks_minio.NewMinioHandler(t)
	cfg := &config.Configuration{}
	cfg.Minio.ListObjectsMaxKeys = 1000
	return &MinioObjectStorage{
		minioHandler: mockHandler,
		cfg:          cfg,
	}, mockHandler
}

// ============================================================================
// GetObject tests
// ============================================================================

func TestMinioObjectStorage_GetObject_Success(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	// GetObject with positive offset should set range
	mockHandler.On("GetObject", ctx, "bucket", "key", mock.AnythingOfType("minio.GetObjectOptions"), "ns", "log1").
		Return(&minio.Object{}, nil).Once()

	reader, err := storage.GetObject(ctx, "bucket", "key", 0, 100, "ns", "log1")
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	// Verify it's wrapped in ObjectReader
	_, ok := reader.(*minioHandler.ObjectReader)
	assert.True(t, ok)
}

func TestMinioObjectStorage_GetObject_Error(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	mockHandler.On("GetObject", ctx, "bucket", "missing-key", mock.AnythingOfType("minio.GetObjectOptions"), "ns", "log1").
		Return(nil, errors.New("object not found")).Once()

	reader, err := storage.GetObject(ctx, "bucket", "missing-key", 0, 100, "ns", "log1")
	assert.Error(t, err)
	assert.Nil(t, reader)
	assert.Contains(t, err.Error(), "object not found")
}

func TestMinioObjectStorage_GetObject_NegativeOffset(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	// With negative offset, no range is set
	mockHandler.On("GetObject", ctx, "bucket", "key", mock.AnythingOfType("minio.GetObjectOptions"), "ns", "log1").
		Return(&minio.Object{}, nil).Once()

	reader, err := storage.GetObject(ctx, "bucket", "key", -1, 100, "ns", "log1")
	assert.NoError(t, err)
	assert.NotNil(t, reader)
}

func TestMinioObjectStorage_GetObject_SetRangeError(t *testing.T) {
	storage, _ := newTestMinioObjectStorage(t)
	ctx := context.Background()

	// offset >= 0 but size = 0 → SetRange(5, 4) → end < start → error
	reader, err := storage.GetObject(ctx, "bucket", "key", 5, 0, "ns", "log1")
	assert.Error(t, err)
	assert.Nil(t, reader)
}

// ============================================================================
// PutObject tests
// ============================================================================

func TestMinioObjectStorage_PutObject_Success(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	mockHandler.On("PutObject", ctx, "bucket", "key", mock.Anything, int64(10), mock.AnythingOfType("minio.PutObjectOptions"), "ns", "log1").
		Return(minio.UploadInfo{}, nil).Once()

	err := storage.PutObject(ctx, "bucket", "key", strings.NewReader("test data!"), 10, "ns", "log1")
	assert.NoError(t, err)
}

func TestMinioObjectStorage_PutObject_Error(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	mockHandler.On("PutObject", ctx, "bucket", "key", mock.Anything, int64(5), mock.AnythingOfType("minio.PutObjectOptions"), "ns", "log1").
		Return(minio.UploadInfo{}, errors.New("write error")).Once()

	err := storage.PutObject(ctx, "bucket", "key", strings.NewReader("hello"), 5, "ns", "log1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "write error")
}

// ============================================================================
// PutObjectIfNoneMatch tests
// ============================================================================

func TestMinioObjectStorage_PutObjectIfNoneMatch_Success(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	mockHandler.On("PutObjectIfNoneMatch", ctx, "bucket", "new-key", mock.Anything, int64(5), "ns", "log1").
		Return(minio.UploadInfo{}, nil).Once()

	err := storage.PutObjectIfNoneMatch(ctx, "bucket", "new-key", strings.NewReader("hello"), 5, "ns", "log1")
	assert.NoError(t, err)
}

func TestMinioObjectStorage_PutObjectIfNoneMatch_Error(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	mockHandler.On("PutObjectIfNoneMatch", ctx, "bucket", "existing-key", mock.Anything, int64(5), "ns", "log1").
		Return(minio.UploadInfo{}, errors.New("precondition failed")).Once()

	err := storage.PutObjectIfNoneMatch(ctx, "bucket", "existing-key", strings.NewReader("hello"), 5, "ns", "log1")
	assert.Error(t, err)
}

// ============================================================================
// PutFencedObject tests
// ============================================================================

func TestMinioObjectStorage_PutFencedObject_Success(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	mockHandler.On("PutFencedObject", ctx, "bucket", "fence-key", "ns", "log1").
		Return(minio.UploadInfo{}, nil).Once()

	err := storage.PutFencedObject(ctx, "bucket", "fence-key", "ns", "log1")
	assert.NoError(t, err)
}

func TestMinioObjectStorage_PutFencedObject_Error(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	mockHandler.On("PutFencedObject", ctx, "bucket", "fence-key", "ns", "log1").
		Return(minio.UploadInfo{}, errors.New("fence error")).Once()

	err := storage.PutFencedObject(ctx, "bucket", "fence-key", "ns", "log1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fence error")
}

// ============================================================================
// StatObject tests
// ============================================================================

func TestMinioObjectStorage_StatObject_Success_NotFenced(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	// Object without fenced metadata
	objInfo := minio.ObjectInfo{
		Size:         256,
		UserMetadata: map[string]string{},
	}
	mockHandler.On("StatObject", ctx, "bucket", "key", mock.AnythingOfType("minio.GetObjectOptions"), "ns", "log1").
		Return(objInfo, nil).Once()

	size, isFenced, err := storage.StatObject(ctx, "bucket", "key", "ns", "log1")
	assert.NoError(t, err)
	assert.Equal(t, int64(256), size)
	assert.False(t, isFenced)
}

func TestMinioObjectStorage_StatObject_Success_Fenced(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	// Object with fenced metadata
	objInfo := minio.ObjectInfo{
		Size: 1,
		UserMetadata: map[string]string{
			minioHandler.FencedObjectMetaKey: "true",
		},
	}
	mockHandler.On("StatObject", ctx, "bucket", "fenced-key", mock.AnythingOfType("minio.GetObjectOptions"), "ns", "log1").
		Return(objInfo, nil).Once()

	size, isFenced, err := storage.StatObject(ctx, "bucket", "fenced-key", "ns", "log1")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), size)
	assert.True(t, isFenced)
}

func TestMinioObjectStorage_StatObject_Error(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	mockHandler.On("StatObject", ctx, "bucket", "missing-key", mock.AnythingOfType("minio.GetObjectOptions"), "ns", "log1").
		Return(minio.ObjectInfo{}, errors.New("not found")).Once()

	size, isFenced, err := storage.StatObject(ctx, "bucket", "missing-key", "ns", "log1")
	assert.Error(t, err)
	assert.Equal(t, int64(-1), size)
	assert.False(t, isFenced)
}

// ============================================================================
// WalkWithObjects tests
// ============================================================================

func TestMinioObjectStorage_WalkWithObjects_Success(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	ch := make(chan minio.ObjectInfo, 3)
	now := time.Now()
	ch <- minio.ObjectInfo{Key: "prefix/obj1", LastModified: now, Size: 100}
	ch <- minio.ObjectInfo{Key: "prefix/obj2", LastModified: now, Size: 200}
	ch <- minio.ObjectInfo{Key: "prefix/obj3", LastModified: now, Size: 300}
	close(ch)

	mockHandler.On("ListObjects", ctx, "bucket", "prefix/", true, mock.AnythingOfType("minio.ListObjectsOptions"), "ns", "log1").
		Return((<-chan minio.ObjectInfo)(ch)).Once()

	var collected []ChunkObjectInfo
	walkFunc := func(info *ChunkObjectInfo) bool {
		collected = append(collected, *info)
		return true
	}

	err := storage.WalkWithObjects(ctx, "bucket", "prefix/", true, walkFunc, "ns", "log1")
	assert.NoError(t, err)
	assert.Len(t, collected, 3)
	assert.Equal(t, "prefix/obj1", collected[0].FilePath)
	assert.Equal(t, int64(100), collected[0].Size)
	assert.Equal(t, "prefix/obj2", collected[1].FilePath)
	assert.Equal(t, int64(200), collected[1].Size)
	assert.Equal(t, "prefix/obj3", collected[2].FilePath)
	assert.Equal(t, int64(300), collected[2].Size)
}

func TestMinioObjectStorage_WalkWithObjects_Empty(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	ch := make(chan minio.ObjectInfo)
	close(ch)

	mockHandler.On("ListObjects", ctx, "bucket", "prefix/", true, mock.AnythingOfType("minio.ListObjectsOptions"), "ns", "log1").
		Return((<-chan minio.ObjectInfo)(ch)).Once()

	var collected []ChunkObjectInfo
	walkFunc := func(info *ChunkObjectInfo) bool {
		collected = append(collected, *info)
		return true
	}

	err := storage.WalkWithObjects(ctx, "bucket", "prefix/", true, walkFunc, "ns", "log1")
	assert.NoError(t, err)
	assert.Len(t, collected, 0)
}

func TestMinioObjectStorage_WalkWithObjects_StopsEarly(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	ch := make(chan minio.ObjectInfo, 3)
	now := time.Now()
	ch <- minio.ObjectInfo{Key: "prefix/obj1", LastModified: now, Size: 100}
	ch <- minio.ObjectInfo{Key: "prefix/obj2", LastModified: now, Size: 200}
	ch <- minio.ObjectInfo{Key: "prefix/obj3", LastModified: now, Size: 300}
	close(ch)

	mockHandler.On("ListObjects", ctx, "bucket", "prefix/", false, mock.AnythingOfType("minio.ListObjectsOptions"), "ns", "log1").
		Return((<-chan minio.ObjectInfo)(ch)).Once()

	var count int
	walkFunc := func(info *ChunkObjectInfo) bool {
		count++
		return count < 2 // stop after second item
	}

	err := storage.WalkWithObjects(ctx, "bucket", "prefix/", false, walkFunc, "ns", "log1")
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestMinioObjectStorage_WalkWithObjects_Error(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	ch := make(chan minio.ObjectInfo, 2)
	now := time.Now()
	ch <- minio.ObjectInfo{Key: "prefix/obj1", LastModified: now, Size: 100}
	ch <- minio.ObjectInfo{Err: errors.New("listing error")}
	close(ch)

	mockHandler.On("ListObjects", ctx, "bucket", "prefix/", true, mock.AnythingOfType("minio.ListObjectsOptions"), "ns", "log1").
		Return((<-chan minio.ObjectInfo)(ch)).Once()

	var count int
	walkFunc := func(info *ChunkObjectInfo) bool {
		count++
		return true
	}

	err := storage.WalkWithObjects(ctx, "bucket", "prefix/", true, walkFunc, "ns", "log1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "listing error")
	assert.Equal(t, 1, count) // only the first object was processed
}

// ============================================================================
// RemoveObject tests
// ============================================================================

func TestMinioObjectStorage_RemoveObject_Success(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	mockHandler.On("RemoveObject", ctx, "bucket", "key", mock.AnythingOfType("minio.RemoveObjectOptions"), "ns", "log1").
		Return(nil).Once()

	err := storage.RemoveObject(ctx, "bucket", "key", "ns", "log1")
	assert.NoError(t, err)
}

func TestMinioObjectStorage_RemoveObject_Error(t *testing.T) {
	storage, mockHandler := newTestMinioObjectStorage(t)
	ctx := context.Background()

	mockHandler.On("RemoveObject", ctx, "bucket", "key", mock.AnythingOfType("minio.RemoveObjectOptions"), "ns", "log1").
		Return(errors.New("remove failed")).Once()

	err := storage.RemoveObject(ctx, "bucket", "key", "ns", "log1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "remove failed")
}

// ============================================================================
// IsObjectNotExistsError / IsPreconditionFailedError tests
// ============================================================================

func TestMinioObjectStorage_IsObjectNotExistsError(t *testing.T) {
	storage, _ := newTestMinioObjectStorage(t)

	// nil error should return false
	assert.False(t, storage.IsObjectNotExistsError(nil))

	// Generic error should return false
	assert.False(t, storage.IsObjectNotExistsError(errors.New("some error")))

	// MinIO NoSuchKey error should return true
	noSuchKeyResp := minio.ErrorResponse{Code: "NoSuchKey"}
	assert.True(t, storage.IsObjectNotExistsError(noSuchKeyResp))

	// Other MinIO error should return false
	otherResp := minio.ErrorResponse{Code: "AccessDenied"}
	assert.False(t, storage.IsObjectNotExistsError(otherResp))
}

func TestMinioObjectStorage_IsPreconditionFailedError(t *testing.T) {
	storage, _ := newTestMinioObjectStorage(t)

	// nil error should return false
	assert.False(t, storage.IsPreconditionFailedError(nil))

	// Generic error should return false
	assert.False(t, storage.IsPreconditionFailedError(errors.New("some error")))

	// PreconditionFailed error
	preconditionResp := minio.ErrorResponse{Code: "PreconditionFailed"}
	assert.True(t, storage.IsPreconditionFailedError(preconditionResp))

	// FileAlreadyExists error
	fileExistsResp := minio.ErrorResponse{Code: "FileAlreadyExists"}
	assert.True(t, storage.IsPreconditionFailedError(fileExistsResp))

	// Other MinIO error should return false
	otherResp := minio.ErrorResponse{Code: "InternalError"}
	assert.False(t, storage.IsPreconditionFailedError(otherResp))
}

// ============================================================================
// newMinioObjectStorageWithConfig tests
// ============================================================================

// Note: newMinioObjectStorageWithConfig with invalid config involves real MinIO
// client creation with retry loops, so we only test the struct construction
// path via newTestMinioObjectStorage helper above.

// ============================================================================
// MinioObjectStorage struct field tests
// ============================================================================

func TestMinioObjectStorage_StructFields(t *testing.T) {
	mockHandler := mocks_minio.NewMinioHandler(t)
	cfg := &config.Configuration{}
	cfg.Minio.ListObjectsMaxKeys = 500

	storage := &MinioObjectStorage{
		minioHandler: mockHandler,
		cfg:          cfg,
	}

	assert.NotNil(t, storage.minioHandler)
	assert.NotNil(t, storage.cfg)
	assert.Equal(t, 500, storage.cfg.Minio.ListObjectsMaxKeys)
}

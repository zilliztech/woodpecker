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

package integration

import (
	"bytes"
	"context"
	"fmt"
	"github.com/zilliztech/woodpecker/common/werr"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
)

// newExternalObjectStorage creates an ExternalObjectStorage instance for testing
func newExternalObjectStorage(t *testing.T, ctx context.Context) (storageclient.ObjectStorage, *config.Configuration) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	storageCli, err := storageclient.NewExternalObjectStorageIfNecessary(ctx, cfg, true)
	require.NoError(t, err)

	return storageCli, cfg
}

func TestExternalObjectStoragePutObject(t *testing.T) {
	ctx := context.Background()
	storageCli, cfg := newExternalObjectStorage(t, ctx)

	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-external-put-object-%d", time.Now().UnixNano())

	// Test data
	testData := []byte("test content for external put object")

	// 1. Test successful upload
	err := storageCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)
	t.Logf("Object uploaded successfully: %s", objectName)

	// 2. Verify object exists and content is correct
	objSize, isFenced, err := storageCli.StatObject(ctx, bucketName, objectName)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testData)), objSize)
	assert.False(t, isFenced)

	// 3. Read and verify content
	reader, err := storageCli.GetObject(ctx, bucketName, objectName, 0, objSize)
	require.NoError(t, err)
	defer reader.Close()

	readData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, readData)

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName)
	assert.NoError(t, err)
}

func TestExternalObjectStoragePutObjectIfNoneMatch(t *testing.T) {
	ctx := context.Background()
	storageCli, cfg := newExternalObjectStorage(t, ctx)

	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-external-put-if-none-match-%d", time.Now().UnixNano())

	// Test data
	testData := []byte("test content for if-none-match")

	// 1. Test successful upload to non-existent object (should succeed)
	err := storageCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)
	t.Logf("Initial upload succeeded: %s", objectName)

	// 2. Test failed upload to existing object (should return error)
	newData := []byte("should not be uploaded")
	err = storageCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader(newData), int64(len(newData)))
	require.Error(t, err)
	assert.True(t, werr.ErrObjectAlreadyExists.Is(err), "Should return object already existing error")
	t.Logf("Expected error for existing object: %v", err)

	// 3. Verify original content is unchanged
	objSize, isFenced, err := storageCli.StatObject(ctx, bucketName, objectName)
	require.NoError(t, err)
	assert.False(t, isFenced)

	reader, err := storageCli.GetObject(ctx, bucketName, objectName, 0, objSize)
	require.NoError(t, err)
	defer reader.Close()

	readData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, readData, "Original content should be unchanged")

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName)
	assert.NoError(t, err)
}

func TestExternalObjectStoragePutFencedObject(t *testing.T) {
	ctx := context.Background()
	storageCli, cfg := newExternalObjectStorage(t, ctx)

	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-external-put-fenced-%d", time.Now().UnixNano())

	// 1. Test successful fenced object creation
	err := storageCli.PutFencedObject(ctx, bucketName, objectName)
	require.NoError(t, err)
	t.Logf("Fenced object created successfully: %s", objectName)

	// 2. Verify object is fenced
	objSize, isFenced, err := storageCli.StatObject(ctx, bucketName, objectName)
	require.NoError(t, err)
	assert.True(t, isFenced, "Object should be marked as fenced")
	assert.Equal(t, int64(1), objSize, "Fenced object should have size 1")

	// 3. Test idempotent behavior - calling PutFencedObject again should succeed
	err = storageCli.PutFencedObject(ctx, bucketName, objectName)
	require.NoError(t, err)
	t.Logf("Idempotent fenced object creation succeeded")

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName)
	assert.NoError(t, err)
}

func TestExternalObjectStorageStatObject(t *testing.T) {
	ctx := context.Background()
	storageCli, cfg := newExternalObjectStorage(t, ctx)

	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-external-stat-object-%d", time.Now().UnixNano())

	// 1. Test StatObject on non-existent object
	_, _, err := storageCli.StatObject(ctx, bucketName, objectName)
	require.Error(t, err)
	assert.True(t, storageCli.IsObjectNotExistsError(err), "Should return object not exists error")

	// 2. Create a regular object
	testData := []byte("test data for stat")
	err = storageCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	// 3. Test StatObject on existing object
	objSize, isFenced, err := storageCli.StatObject(ctx, bucketName, objectName)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testData)), objSize)
	assert.False(t, isFenced, "Regular object should not be fenced")

	// 4. Create a fenced object
	fencedObjectName := fmt.Sprintf("test-external-stat-fenced-%d", time.Now().UnixNano())
	err = storageCli.PutFencedObject(ctx, bucketName, fencedObjectName)
	require.NoError(t, err)

	// 5. Test StatObject on fenced object
	fencedSize, isFenced, err := storageCli.StatObject(ctx, bucketName, fencedObjectName)
	require.NoError(t, err)
	assert.Equal(t, int64(1), fencedSize, "Fenced object should have size 1")
	assert.True(t, isFenced, "Object should be marked as fenced")

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName)
	assert.NoError(t, err)
	err = storageCli.RemoveObject(ctx, bucketName, fencedObjectName)
	assert.NoError(t, err)
}

func TestExternalObjectStorageGetObject(t *testing.T) {
	ctx := context.Background()
	storageCli, cfg := newExternalObjectStorage(t, ctx)

	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-external-get-object-%d", time.Now().UnixNano())

	// Create test data
	testData := []byte("Hello, World! This is a test file for external object storage.")

	// 1. Upload object
	err := storageCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	// 2. Read full object
	reader, err := storageCli.GetObject(ctx, bucketName, objectName, 0, int64(len(testData)))
	require.NoError(t, err)
	defer reader.Close()

	readData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, readData)

	// 3. Test ReadAt
	reader2, err := storageCli.GetObject(ctx, bucketName, objectName, 0, int64(len(testData)))
	require.NoError(t, err)
	defer reader2.Close()

	buf := make([]byte, 5)
	n, err := reader2.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, testData[:5], buf)

	// 4. Test Seek
	reader3, err := storageCli.GetObject(ctx, bucketName, objectName, 0, int64(len(testData)))
	require.NoError(t, err)
	defer reader3.Close()

	pos, err := reader3.Seek(7, io.SeekStart)
	require.NoError(t, err)
	assert.Equal(t, int64(7), pos)

	buf2 := make([]byte, 5)
	n2, err := reader3.Read(buf2)
	require.NoError(t, err)
	assert.Equal(t, 5, n2)
	assert.Equal(t, testData[7:12], buf2)

	// 5. Test Size method
	size, err := reader3.Size()
	require.NoError(t, err)
	assert.Equal(t, int64(len(testData)), size)

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName)
	assert.NoError(t, err)
}

func TestExternalObjectStorageGetObjectWithOffsetAndSize(t *testing.T) {
	ctx := context.Background()
	storageCli, cfg := newExternalObjectStorage(t, ctx)

	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-external-get-offset-size-%d", time.Now().UnixNano())

	// Create 100 bytes of test data
	testData := make([]byte, 100)
	for i := 0; i < 100; i++ {
		testData[i] = byte(i % 256)
	}

	// 1. Upload the 100-byte object
	err := storageCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	// 2. Read only first 50 bytes using offset=0, size=50
	partialReader, err := storageCli.GetObject(ctx, bucketName, objectName, 0, 50)
	require.NoError(t, err)
	defer partialReader.Close()

	partialData, err := io.ReadAll(partialReader)
	require.NoError(t, err)
	assert.Equal(t, 50, len(partialData))
	assert.Equal(t, testData[:50], partialData)

	// 3. Read middle 30 bytes using offset=35, size=30
	middleReader, err := storageCli.GetObject(ctx, bucketName, objectName, 35, 30)
	require.NoError(t, err)
	defer middleReader.Close()

	middleData, err := io.ReadAll(middleReader)
	require.NoError(t, err)
	assert.Equal(t, 30, len(middleData))
	assert.Equal(t, testData[35:65], middleData)

	// 4. Read last 25 bytes using offset=75, size=25
	lastReader, err := storageCli.GetObject(ctx, bucketName, objectName, 75, 25)
	require.NoError(t, err)
	defer lastReader.Close()

	lastData, err := io.ReadAll(lastReader)
	require.NoError(t, err)
	assert.Equal(t, 25, len(lastData))
	assert.Equal(t, testData[75:100], lastData)

	// 5. Test edge case: read beyond object size
	beyondReader, err := storageCli.GetObject(ctx, bucketName, objectName, 90, 20)
	require.NoError(t, err)
	defer beyondReader.Close()

	beyondData, err := io.ReadAll(beyondReader)
	require.NoError(t, err)
	assert.Equal(t, 10, len(beyondData), "Reading beyond object should return only available bytes")
	assert.Equal(t, testData[90:100], beyondData)

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName)
	assert.NoError(t, err)
}

func TestExternalObjectStorageRemoveObject(t *testing.T) {
	ctx := context.Background()
	storageCli, cfg := newExternalObjectStorage(t, ctx)

	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-external-remove-object-%d", time.Now().UnixNano())

	// 1. Create an object
	testData := []byte("test data to be removed")
	err := storageCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)

	// 2. Verify object exists
	_, _, err = storageCli.StatObject(ctx, bucketName, objectName)
	require.NoError(t, err)

	// 3. Remove the object
	err = storageCli.RemoveObject(ctx, bucketName, objectName)
	require.NoError(t, err)

	// 4. Verify object no longer exists
	_, _, err = storageCli.StatObject(ctx, bucketName, objectName)
	require.Error(t, err)
	assert.True(t, storageCli.IsObjectNotExistsError(err))

	// 5. Test removing non-existent object (should return error)
	err = storageCli.RemoveObject(ctx, bucketName, "non-existent-object")
	require.Error(t, err)
}

func TestExternalObjectStorageIsObjectNotExistsError(t *testing.T) {
	ctx := context.Background()
	storageCli, cfg := newExternalObjectStorage(t, ctx)

	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-external-error-check-%d", time.Now().UnixNano())

	// 1. Test on non-existent object
	_, _, err := storageCli.StatObject(ctx, bucketName, objectName)
	require.Error(t, err)
	assert.True(t, storageCli.IsObjectNotExistsError(err), "Should identify object not exists error")

	// 2. Test on nil error
	assert.False(t, storageCli.IsObjectNotExistsError(nil))

	// 3. Test on other error (create then try to create again with PutObjectIfNoneMatch)
	err = storageCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader([]byte("test")), 4)
	require.NoError(t, err)

	err = storageCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader([]byte("test")), 4)
	require.Error(t, err)
	assert.True(t, werr.ErrObjectAlreadyExists.Is(err), "should be object already exists error")

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName)
	assert.NoError(t, err)
}

func TestExternalObjectStorageIsPreconditionFailedError(t *testing.T) {
	ctx := context.Background()
	storageCli, cfg := newExternalObjectStorage(t, ctx)

	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-external-precondition-%d", time.Now().UnixNano())

	// 1. Create an object
	err := storageCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader([]byte("test")), 4)
	require.NoError(t, err)

	// 2. Try PutObjectIfNoneMatch on existing object (should return precondition failed)
	err = storageCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader([]byte("test")), 4)
	require.Error(t, err)
	assert.True(t, werr.ErrObjectAlreadyExists.Is(err), "Should identify object already exists error")

	// 3. Test on nil error
	assert.False(t, storageCli.IsPreconditionFailedError(nil))

	// 4. Test on object not exists error
	_, _, err = storageCli.StatObject(ctx, bucketName, "non-existent-object")
	require.Error(t, err)
	assert.False(t, storageCli.IsPreconditionFailedError(err), "Object not exists error should not be identified as precondition failed")

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName)
	assert.NoError(t, err)
}

func TestExternalObjectStorageWalkWithObjects(t *testing.T) {
	ctx := context.Background()
	storageCli, cfg := newExternalObjectStorage(t, ctx)

	bucketName := cfg.Minio.BucketName
	basePath := fmt.Sprintf("test-walk-%d", time.Now().UnixNano())

	// Create test objects
	testObjects := []string{
		fmt.Sprintf("%s/obj1.txt", basePath),
		fmt.Sprintf("%s/obj2.txt", basePath),
		fmt.Sprintf("%s/subdir/obj3.txt", basePath),
	}

	testData := []byte("test data for walk")
	for _, objName := range testObjects {
		err := storageCli.PutObject(ctx, bucketName, objName,
			bytes.NewReader(testData), int64(len(testData)))
		require.NoError(t, err)
	}

	// Test WalkWithObjects
	var foundObjects []string
	err := storageCli.WalkWithObjects(ctx, bucketName, basePath, true, func(objInfo *storageclient.ChunkObjectInfo) bool {
		foundObjects = append(foundObjects, objInfo.FilePath)
		return true // continue walking
	})
	require.NoError(t, err)

	// Verify all objects were found
	assert.GreaterOrEqual(t, len(foundObjects), len(testObjects), "Should find at least the created objects")
	for _, expectedObj := range testObjects {
		found := false
		for _, foundObj := range foundObjects {
			if foundObj == expectedObj {
				found = true
				break
			}
		}
		assert.True(t, found, "Should find object: %s", expectedObj)
	}

	t.Logf("WalkWithObjects found %d objects with prefix %s", len(foundObjects), basePath)

	// Cleanup
	for _, objName := range testObjects {
		err = storageCli.RemoveObject(ctx, bucketName, objName)
		assert.NoError(t, err)
	}
}

func TestExternalObjectStorageEmptyFile(t *testing.T) {
	ctx := context.Background()
	storageCli, cfg := newExternalObjectStorage(t, ctx)

	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-external-empty-file-%d", time.Now().UnixNano())

	// 1. Upload empty file
	err := storageCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader([]byte{}), 0)
	require.NoError(t, err)

	// 2. Verify empty file
	objSize, isFenced, err := storageCli.StatObject(ctx, bucketName, objectName)
	require.NoError(t, err)
	assert.Equal(t, int64(0), objSize)
	assert.False(t, isFenced)

	// 3. Read empty file
	reader, err := storageCli.GetObject(ctx, bucketName, objectName, 0, 0)
	require.NoError(t, err)
	defer reader.Close()

	readData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, 0, len(readData))

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName)
	assert.NoError(t, err)
}

func TestExternalObjectStorageLargeFile(t *testing.T) {
	ctx := context.Background()
	storageCli, cfg := newExternalObjectStorage(t, ctx)

	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-external-large-file-%d", time.Now().UnixNano())

	// Create a 1MB file
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	// 1. Upload large file
	err := storageCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(largeData), int64(len(largeData)))
	require.NoError(t, err)

	// 2. Verify large file
	objSize, isFenced, err := storageCli.StatObject(ctx, bucketName, objectName)
	require.NoError(t, err)
	assert.Equal(t, int64(len(largeData)), objSize)
	assert.False(t, isFenced)

	// 3. Read large file in chunks
	reader, err := storageCli.GetObject(ctx, bucketName, objectName, 0, int64(len(largeData)))
	require.NoError(t, err)
	defer reader.Close()

	// Read first 1KB
	buf := make([]byte, 1024)
	n, err := reader.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 1024, n)
	assert.Equal(t, largeData[:1024], buf)

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName)
	assert.NoError(t, err)
}

func TestExternalObjectStorageCheckIfConditionWriteSupport(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	ctx := context.Background()
	storageCli, cfg := newExternalObjectStorage(t, ctx)
	require.NoError(t, err)

	bucketName := cfg.Minio.BucketName
	basePath := fmt.Sprintf("test-support-check-%d", time.Now().UnixNano())

	// Test that the function runs successfully without panicking
	assert.NotPanics(t, func() {
		storageclient.CheckIfConditionWriteSupport(ctx, storageCli, bucketName, basePath)
	}, "CheckIfConditionWriteSupport should not panic with valid ObjectStorage")

	t.Logf("CheckIfConditionWriteSupport passed successfully for bucket: %s", bucketName)
}

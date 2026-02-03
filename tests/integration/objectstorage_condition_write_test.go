package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
)

func TestObjectStoragePutObject(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	storageCli, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-put-object-%d", time.Now().UnixNano())

	// Test data
	testData := []byte("test content for put object")

	// 1. Test successful upload
	err = storageCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)), "test-ns", "0")
	require.NoError(t, err)
	t.Logf("Object uploaded successfully: %s", objectName)

	// 2. Verify object exists and content is correct
	objSize, isFenced, err := storageCli.StatObject(ctx, bucketName, objectName, "test-ns", "0")
	require.NoError(t, err)
	assert.Equal(t, int64(len(testData)), objSize)
	assert.False(t, isFenced)

	// 3. Read and verify content
	reader, err := storageCli.GetObject(ctx, bucketName, objectName, 0, objSize, "test-ns", "0")
	require.NoError(t, err)
	defer reader.Close()

	readData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, readData)

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName, "test-ns", "0")
	assert.NoError(t, err)
}

func TestObjectStoragePutObjectIfNoneMatch(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	storageCli, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-put-if-none-match-%d", time.Now().UnixNano())

	// Test data
	testData := []byte("test content for if-none-match")

	// 1. Test successful upload to non-existent object (should succeed)
	err = storageCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)), "test-ns", "0")
	require.NoError(t, err)
	t.Logf("Initial upload succeeded: %s", objectName)

	// 2. Test failed upload to existing object (should return ErrObjectAlreadyExists)
	newData := []byte("should not be uploaded")
	err = storageCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader(newData), int64(len(newData)), "test-ns", "0")
	require.Error(t, err)
	require.True(t, werr.ErrObjectAlreadyExists.Is(err), "Should return ErrObjectAlreadyExists for existing object")
	t.Logf("Expected error for existing object: %v", err)

	// 3. Verify original content is unchanged
	objSize, isFenced, err := storageCli.StatObject(ctx, bucketName, objectName, "test-ns", "0")
	require.NoError(t, err)
	assert.False(t, isFenced)

	reader, err := storageCli.GetObject(ctx, bucketName, objectName, 0, objSize, "test-ns", "0")
	require.NoError(t, err)
	defer reader.Close()

	readData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, readData, "Original content should be unchanged")

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName, "test-ns", "0")
	assert.NoError(t, err)
}

func TestObjectStoragePutFencedObject(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	storageCli, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-put-fenced-%d", time.Now().UnixNano())

	// 1. Test successful fenced object creation
	err = storageCli.PutFencedObject(ctx, bucketName, objectName, "test-ns", "0")
	require.NoError(t, err)
	t.Logf("Fenced object created successfully: %s", objectName)

	// 2. Verify object is fenced
	objSize, isFenced, err := storageCli.StatObject(ctx, bucketName, objectName, "test-ns", "0")
	require.NoError(t, err)
	assert.True(t, isFenced, "Object should be marked as fenced")
	assert.Greater(t, objSize, int64(0), "Fenced object should have some size")

	// 3. Test idempotent behavior - calling PutFencedObject again should succeed
	err = storageCli.PutFencedObject(ctx, bucketName, objectName, "test-ns", "0")
	require.NoError(t, err)
	t.Logf("Idempotent fenced object creation succeeded")

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName, "test-ns", "0")
	assert.NoError(t, err)
}

func TestObjectStoragePutIfNoneMatchWithFencedObject(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	storageCli, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-put-if-none-match-fenced-%d", time.Now().UnixNano())

	// 1. Create a fenced object
	err = storageCli.PutFencedObject(ctx, bucketName, objectName, "test-ns", "0")
	require.NoError(t, err)
	t.Logf("Fenced object created: %s", objectName)

	// 2. Test PutObjectIfNoneMatch with fenced object (should return ErrSegmentFenced)
	testData := []byte("should not be uploaded to fenced object")
	err = storageCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)), "test-ns", "0")
	require.Error(t, err)
	require.True(t, werr.ErrSegmentFenced.Is(err), "Should return ErrSegmentFenced for fenced object")
	t.Logf("Expected fenced error: %v", err)

	// 3. Verify object is still fenced
	_, isFenced, err := storageCli.StatObject(ctx, bucketName, objectName, "test-ns", "0")
	require.NoError(t, err)
	assert.True(t, isFenced, "Object should remain fenced")

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName, "test-ns", "0")
	assert.NoError(t, err)
}

func TestObjectStoragePutIfNoneMatchConcurrency(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	storageCli, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-put-if-none-match-concurrency-%d", time.Now().UnixNano())

	// Test concurrent uploads to the same object
	const numGoroutines = 5
	var wg sync.WaitGroup
	results := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			data := []byte(fmt.Sprintf("concurrent data from goroutine %d", id))
			err := storageCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
				bytes.NewReader(data), int64(len(data)), "test-ns", "0")
			results <- err
		}(i)
	}

	wg.Wait()
	close(results)

	// Collect results
	var successCount, objectExistsCount, otherErrorCount int
	for err := range results {
		if err == nil {
			successCount++
		} else if werr.ErrObjectAlreadyExists.Is(err) {
			objectExistsCount++
		} else {
			otherErrorCount++
			t.Logf("Unexpected error: %v", err)
		}
	}

	// Only one upload should succeed, others should return ErrObjectAlreadyExists
	assert.Equal(t, 1, successCount, "Only one concurrent upload should succeed")
	assert.Equal(t, numGoroutines-1, objectExistsCount, "All other uploads should return ErrObjectAlreadyExists")
	assert.Equal(t, 0, otherErrorCount, "No other errors should occur")

	t.Logf("Concurrency test results: %d success, %d object exists, %d other errors",
		successCount, objectExistsCount, otherErrorCount)

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName, "test-ns", "0")
	assert.NoError(t, err)
}

func TestObjectStoragePutFencedObjectConcurrency(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	storageCli, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-put-fenced-concurrency-%d", time.Now().UnixNano())

	// Test concurrent fence operations on the same object
	const numGoroutines = 5
	var wg sync.WaitGroup
	results := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			err := storageCli.PutFencedObject(ctx, bucketName, objectName, "test-ns", "0")
			results <- err
		}(i)
	}

	wg.Wait()
	close(results)

	// Collect results
	var successCount, errorCount int
	for err := range results {
		if err == nil {
			successCount++
		} else {
			errorCount++
			t.Logf("Fence operation error: %v", err)
		}
	}

	// All fence operations should succeed due to idempotent behavior
	assert.Equal(t, numGoroutines, successCount, "All concurrent fence operations should succeed")
	assert.Equal(t, 0, errorCount, "No fence operations should fail")

	// Verify object is fenced
	_, isFenced, err := storageCli.StatObject(ctx, bucketName, objectName, "test-ns", "0")
	require.NoError(t, err)
	assert.True(t, isFenced, "Object should be fenced after concurrent operations")

	t.Logf("Fenced concurrency test results: %d success, %d errors", successCount, errorCount)

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName, "test-ns", "0")
	assert.NoError(t, err)
}

func TestObjectStoragePutObjectIdempotency(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	storageCli, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-put-object-idempotency-%d", time.Now().UnixNano())

	// Test data
	testData := []byte("test content for idempotency")

	// 1. First upload should succeed
	err = storageCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)), "test-ns", "0")
	require.NoError(t, err)
	t.Logf("First upload succeeded: %s", objectName)

	// 2. Second upload with same content should return ErrObjectAlreadyExists (idempotent)
	err = storageCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)), "test-ns", "0")
	require.Error(t, err)
	require.True(t, werr.ErrObjectAlreadyExists.Is(err), "Second upload should return ErrObjectAlreadyExists")

	// 3. Third upload with different content should also return ErrObjectAlreadyExists
	differentData := []byte("different content")
	err = storageCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader(differentData), int64(len(differentData)), "test-ns", "0")
	require.Error(t, err)
	require.True(t, werr.ErrObjectAlreadyExists.Is(err), "Upload with different content should also return ErrObjectAlreadyExists")

	// 4. Verify original content is preserved
	objSize, _, err := storageCli.StatObject(ctx, bucketName, objectName, "test-ns", "0")
	require.NoError(t, err)

	reader, err := storageCli.GetObject(ctx, bucketName, objectName, 0, objSize, "test-ns", "0")
	require.NoError(t, err)
	defer reader.Close()

	readData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, readData, "Original content should be preserved")

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName, "test-ns", "0")
	assert.NoError(t, err)
}

func TestObjectStorageCheckIfConditionWriteSupport(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	storageCli, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	basePath := fmt.Sprintf("test-support-check-%d", time.Now().UnixNano())

	// Test that the function runs successfully without panicking
	assert.NotPanics(t, func() {
		storageclient.CheckIfConditionWriteSupport(ctx, storageCli, bucketName, basePath)
	}, "CheckIfConditionWriteSupport should not panic with valid ObjectStorage")

	t.Logf("CheckIfConditionWriteSupport passed successfully for bucket: %s", bucketName)
}

func TestObjectStorageCheckIfConditionWriteSupportMultipleCalls(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	storageCli, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	basePath := fmt.Sprintf("test-support-check-multi-%d", time.Now().UnixNano())

	// Call multiple times to test sync.Once behavior
	for i := 0; i < 3; i++ {
		assert.NotPanics(t, func() {
			storageclient.CheckIfConditionWriteSupport(ctx, storageCli, bucketName, basePath)
		}, "Multiple calls to CheckIfConditionWriteSupport should not panic")
	}

	t.Logf("Multiple calls to CheckIfConditionWriteSupport handled correctly")
}

func TestObjectStorageCheckIfConditionWriteSupportConcurrency(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	storageCli, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	basePath := fmt.Sprintf("test-support-check-concurrent-%d", time.Now().UnixNano())

	// Test concurrent calls to ensure sync.Once works correctly
	const numGoroutines = 5
	var wg sync.WaitGroup
	panicChan := make(chan interface{}, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicChan <- r
				}
			}()

			storageclient.CheckIfConditionWriteSupport(ctx, storageCli, bucketName, basePath)
		}(i)
	}

	wg.Wait()
	close(panicChan)

	// Check that no goroutines panicked
	panicCount := 0
	for panic := range panicChan {
		panicCount++
		t.Errorf("Goroutine panicked: %v", panic)
	}

	assert.Equal(t, 0, panicCount, "No goroutines should panic during concurrent calls")
	t.Logf("Concurrent calls to CheckIfConditionWriteSupport handled correctly")
}

func TestObjectStorageWalkWithObjects(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	storageCli, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
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
		err = storageCli.PutObject(ctx, bucketName, objName,
			bytes.NewReader(testData), int64(len(testData)), "test-ns", "0")
		require.NoError(t, err)
	}

	// Test WalkWithObjects
	var foundObjects []string
	err = storageCli.WalkWithObjects(ctx, bucketName, basePath, true, func(objInfo *storageclient.ChunkObjectInfo) bool {
		foundObjects = append(foundObjects, objInfo.FilePath)
		return true // continue walking
	}, "test-ns", "0")
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
		err = storageCli.RemoveObject(ctx, bucketName, objName, "test-ns", "0")
		assert.NoError(t, err)
	}
}

func TestObjectStorageGetObjectWithOffsetAndSize(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	storageCli, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-get-object-offset-size-%d", time.Now().UnixNano())

	// Create 100 bytes of test data
	testData := make([]byte, 100)
	for i := 0; i < 100; i++ {
		testData[i] = byte(i % 256) // Fill with pattern 0-99
	}

	// 1. Upload the 100-byte object
	err = storageCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)), "test-ns", "0")
	require.NoError(t, err)
	t.Logf("Uploaded 100-byte object: %s", objectName)

	// 2. Verify full object size
	objSize, isFenced, err := storageCli.StatObject(ctx, bucketName, objectName, "test-ns", "0")
	require.NoError(t, err)
	assert.Equal(t, int64(100), objSize, "Object should be 100 bytes")
	assert.False(t, isFenced, "Object should not be fenced")

	// 3. Read full object to verify content
	reader, err := storageCli.GetObject(ctx, bucketName, objectName, 0, objSize, "test-ns", "0")
	require.NoError(t, err)
	defer reader.Close()

	fullData, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, fullData, "Full object content should match")
	assert.Equal(t, 100, len(fullData), "Full read should return 100 bytes")

	// 4. Read only first 50 bytes using offset=0, size=50
	partialReader, err := storageCli.GetObject(ctx, bucketName, objectName, 0, 50, "test-ns", "0")
	require.NoError(t, err)
	defer partialReader.Close()

	partialData, err := io.ReadAll(partialReader)
	require.NoError(t, err)
	assert.Equal(t, 50, len(partialData), "Partial read should return exactly 50 bytes")
	assert.Equal(t, testData[:50], partialData, "Partial data should match first 50 bytes of original")

	// 5. Read middle 30 bytes using offset=35, size=30 (bytes 35-64)
	middleReader, err := storageCli.GetObject(ctx, bucketName, objectName, 35, 30, "test-ns", "0")
	require.NoError(t, err)
	defer middleReader.Close()

	middleData, err := io.ReadAll(middleReader)
	require.NoError(t, err)
	assert.Equal(t, 30, len(middleData), "Middle read should return exactly 30 bytes")
	assert.Equal(t, testData[35:65], middleData, "Middle data should match bytes 35-64 of original")

	// 6. Read last 25 bytes using offset=75, size=25
	lastReader, err := storageCli.GetObject(ctx, bucketName, objectName, 75, 25, "test-ns", "0")
	require.NoError(t, err)
	defer lastReader.Close()

	lastData, err := io.ReadAll(lastReader)
	require.NoError(t, err)
	assert.Equal(t, 25, len(lastData), "Last read should return exactly 25 bytes")
	assert.Equal(t, testData[75:100], lastData, "Last data should match bytes 75-99 of original")

	// 7. Test edge case: read beyond object size (should only return available bytes)
	beyondReader, err := storageCli.GetObject(ctx, bucketName, objectName, 90, 20, "test-ns", "0")
	require.NoError(t, err)
	defer beyondReader.Close()

	beyondData, err := io.ReadAll(beyondReader)
	require.NoError(t, err)
	assert.Equal(t, 10, len(beyondData), "Reading beyond object should return only available bytes (10)")
	assert.Equal(t, testData[90:100], beyondData, "Beyond data should match last 10 bytes")

	t.Logf("GetObject offset/size test completed successfully:")
	t.Logf("  - Full object: %d bytes", len(fullData))
	t.Logf("  - First 50 bytes: %d bytes", len(partialData))
	t.Logf("  - Middle 30 bytes (35-64): %d bytes", len(middleData))
	t.Logf("  - Last 25 bytes (75-99): %d bytes", len(lastData))
	t.Logf("  - Beyond object (90+20): %d bytes", len(beyondData))

	// Cleanup
	err = storageCli.RemoveObject(ctx, bucketName, objectName, "test-ns", "0")
	assert.NoError(t, err)
}

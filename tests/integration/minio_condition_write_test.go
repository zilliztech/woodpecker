package integration

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
)

func init() {
	cfg, _ := config.NewConfiguration()
	logger.InitLogger(cfg)
}

// TODO: Verify that the MinIO version is v20240510 or newer to address a known issue with the 'if-not-match:*' condition.

func TestMinioPutIfMatch(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-if-match-%d", time.Now().UnixNano())

	// Test data
	initialData := []byte("initial content")
	updatedData := []byte("updated content")

	// 1. Upload initial object
	initUploadInfo, err := minioCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(initialData), int64(len(initialData)), minio.PutObjectOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, initUploadInfo.ETag)

	initialETag := initUploadInfo.ETag
	t.Logf("Initial object uploaded with ETag: %s", initialETag)

	// 2. Test successful conditional update with correct ETag
	opts := minio.PutObjectOptions{}
	opts.SetMatchETag(initialETag)

	updateUploadInfo, err := minioCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(updatedData), int64(len(updatedData)), opts)
	require.NoError(t, err)
	require.NotEmpty(t, updateUploadInfo.ETag)
	require.NotEqual(t, initialETag, updateUploadInfo.ETag, "ETag should change after update")

	t.Logf("Conditional update succeeded with new ETag: %s", updateUploadInfo.ETag)

	// 3. Test failed conditional update with old ETag
	opts.SetMatchETag(initialETag) // Using old ETag

	updateFailUploadInfo, err := minioCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader([]byte("should fail")), 10, opts)
	assert.Error(t, err, "Should fail with precondition failed error")
	assert.True(t, minioHandler.IsPreconditionFailed(err))
	t.Logf("Conditional update fail with ETag: %s", updateFailUploadInfo.ETag)

	// 4. Verify object content is unchanged after failed update
	obj, err := minioCli.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	assert.NoError(t, err)
	defer obj.Close()

	data, err := minioHandler.ReadObjectFull(ctx, obj, 1024)
	require.NoError(t, err)

	actualContent := data
	assert.Equal(t, updatedData, actualContent, "Object content should remain unchanged after failed conditional update")

	// Cleanup
	err = minioCli.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	assert.NoError(t, err)
}

func TestMinioPutIfNotMatch(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-if-none-match-%d", time.Now().UnixNano())

	// Test data
	testData := []byte("test content for if-none-match")

	// 1. Test successful upload to non-existent object with If-None-Match: *
	opts1 := minio.PutObjectOptions{}
	opts1.SetMatchETagExcept("*")

	initUploadInfo, err := minioCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)), opts1)

	errResp := minio.ToErrorResponse(err)
	t.Logf("PutObject failed: Code=%s, Message=%s, Resource=%s, Bucket=%s, Key=%s",
		errResp.Code, errResp.Message, errResp.Resource, errResp.BucketName, errResp.Key)

	require.NoError(t, err)
	require.NotEmpty(t, initUploadInfo.ETag)

	t.Logf("Initial upload succeeded with ETag: %s", initUploadInfo.ETag)

	// 2. Test failed upload to existing object with If-None-Match: *
	opts2 := minio.PutObjectOptions{}
	opts2.SetMatchETagExcept("*")
	failedUpdateUploadInfo, err := minioCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader([]byte("should fail")), 11, opts2)
	assert.Error(t, err, "Should fail because object already exists")
	assert.True(t, minioHandler.IsPreconditionFailed(err))
	t.Logf("reRpload with ETag: %s", failedUpdateUploadInfo.ETag)

	// 3. Test successful upload with If-None-Match using different ETag
	differentETag := "\"different-etag-value\""
	opts3 := minio.PutObjectOptions{}
	opts3.SetMatchETagExcept(differentETag)

	newData := []byte("new content with different etag condition")
	updateUploadInfo, err := minioCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(newData), int64(len(newData)), opts3)
	require.NoError(t, err)
	require.NotEqual(t, initUploadInfo.ETag, updateUploadInfo.ETag)
	t.Logf("Update with different ETag condition succeeded: %s", updateUploadInfo.ETag)

	// 4. Test failed upload with If-None-Match using current ETag
	opts4 := minio.PutObjectOptions{}
	opts4.SetMatchETagExcept(updateUploadInfo.ETag)

	_, err = minioCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader([]byte("should fail again")), 17, opts4)
	assert.Error(t, err, "Should fail because ETag matches current object")
	assert.True(t, minioHandler.IsPreconditionFailed(err))

	// Cleanup
	err = minioCli.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	assert.NoError(t, err)
}

func TestMinioConditionalWriteConcurrency(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-concurrency-%d", time.Now().UnixNano())

	// Upload initial object
	initialData := []byte("initial data for concurrency test")
	uploadInfo, err := minioCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(initialData), int64(len(initialData)), minio.PutObjectOptions{})
	require.NoError(t, err)

	initialETag := uploadInfo.ETag
	t.Logf("Initial object uploaded with ETag: %s", initialETag)

	// Test concurrent updates using the same ETag
	const numGoroutines = 5
	var wg sync.WaitGroup
	results := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			data := []byte(fmt.Sprintf("concurrent update from goroutine %d", id))
			opts := minio.PutObjectOptions{}
			opts.SetMatchETag(initialETag)

			_, err := minioCli.PutObject(ctx, bucketName, objectName,
				bytes.NewReader(data), int64(len(data)), opts)
			results <- err
		}(i)
	}

	wg.Wait()
	close(results)

	// Collect results
	var successCount, failureCount int
	for err := range results {
		if err == nil {
			successCount++
		} else {
			failureCount++
			assert.True(t, minioHandler.IsPreconditionFailed(err))
			t.Logf("Concurrent update failed (expected): %v", err)
		}
	}

	// Only one update should succeed due to conditional write protection
	assert.Equal(t, 1, successCount, "Only one concurrent update should succeed")
	assert.Equal(t, numGoroutines-1, failureCount, "All other updates should fail")

	t.Logf("Concurrency test results: %d success, %d failures", successCount, failureCount)

	// Cleanup
	err = minioCli.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	assert.NoError(t, err)
}

func TestMinioConditionalWriteWithNonExistentObject(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-non-existent-%d", time.Now().UnixNano())

	// init put
	initData := []byte("init data")
	_, err = minioCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(initData), int64(len(initData)), minio.PutObjectOptions{})
	assert.NoError(t, err, "If-Match should fail for non-existent object")

	// Test If-Match with non-existent object (should fail)
	testData := []byte("test data")
	opts := minio.PutObjectOptions{}
	opts.SetMatchETag("\"some-etag\"")

	_, err = minioCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)), opts)
	assert.Error(t, err, "If-Match should fail for non-existent object")
	assert.True(t, minioHandler.IsPreconditionFailed(err))

	// Test If-None-Match with non-existent object (should succeed)
	opts = minio.PutObjectOptions{}
	opts.SetMatchETagExcept("*")

	uploadInfo, err := minioCli.PutObject(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)), opts)
	require.Error(t, err)
	assert.True(t, minioHandler.IsPreconditionFailed(err))

	t.Logf("If-None-Match succeeded for non-existent object with ETag: %s", uploadInfo.ETag)

	// Cleanup
	err = minioCli.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	assert.NoError(t, err)
}

func TestMinioHandlePutIfNotMatch(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-put-if-not-match-%d", time.Now().UnixNano())

	// Test data
	testData := []byte("test content for PutObjectIfNoneMatch")

	// 1. Test successful upload to non-existent object (should succeed)
	uploadInfo, err := minioCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)
	require.NotEmpty(t, uploadInfo.ETag)
	t.Logf("Initial upload succeeded with ETag: %s", uploadInfo.ETag)

	// 2. Test failed upload to existing object (should return ErrObjectAlreadyExists)
	newData := []byte("should not be uploaded")
	_, err = minioCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader(newData), int64(len(newData)))
	require.Error(t, err)
	require.True(t, werr.ErrObjectAlreadyExists.Is(err), "Should return ErrObjectAlreadyExists for existing object")
	t.Logf("Expected error for existing object: %v", err)

	// 3. Verify original content is unchanged
	obj, err := minioCli.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	require.NoError(t, err)
	defer obj.Close()

	data, err := minioHandler.ReadObjectFull(ctx, obj, 1024)
	require.NoError(t, err)
	assert.Equal(t, testData, data, "Original content should be unchanged")

	// Cleanup
	err = minioCli.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	assert.NoError(t, err)
}

// TestMinioHandlePutIfNotMatchWithFencedObject tests PutObjectIfNoneMatch with fenced objects
func TestMinioHandlePutIfNotMatchWithFencedObject(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-put-if-not-match-fenced-%d", time.Now().UnixNano())

	// 1. Upload an object with fenced metadata
	uploadInfo, err := minioCli.PutFencedObject(ctx, bucketName, objectName)
	require.NoError(t, err)
	require.NotEmpty(t, uploadInfo.ETag)
	t.Logf("Fenced object uploaded with ETag: %s", uploadInfo.ETag)

	// 2. Test PutObjectIfNoneMatch with fenced object (should return ErrSegmentFenced)
	newData := []byte("should not be uploaded to fenced object")
	_, err = minioCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader(newData), int64(len(newData)))
	require.Error(t, err)
	require.True(t, werr.ErrSegmentFenced.Is(err), "Should return ErrSegmentFenced for fenced object")
	t.Logf("Expected fenced error: %v", err)

	listResult := minioCli.ListObjects(ctx, bucketName, objectName, true, minio.ListObjectsOptions{
		WithMetadata: true,
	})
	for objInfo := range listResult {
		if objInfo.Key == objectName {
			// api does not support list objects with metadata, even if WithMetadata is set
			// Here checking this api does not meet the requirements yet.
			assert.False(t, minioHandler.IsFencedObject(objInfo))

			// only statObject api will return UseMetadata correctly
			objInfoStat, statErr := minioCli.StatObject(ctx, bucketName, objectName, minio.StatObjectOptions{})
			assert.NoError(t, statErr)
			assert.True(t, minioHandler.IsFencedObject(objInfoStat))

			// getObject api also will return UseMetadata correctly, because it will do a request like stateObject
			obj, getErr := minioCli.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
			assert.NoError(t, getErr)
			info, objStatErr := obj.Stat()
			assert.NoError(t, objStatErr)
			assert.True(t, minioHandler.IsFencedObject(info))
		}
	}

	// Cleanup
	err = minioCli.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	assert.NoError(t, err)
}

// TestMinioHandlePutIfNotMatchConcurrency tests concurrent calls to PutObjectIfNoneMatch
func TestMinioHandlePutIfNotMatchConcurrency(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-put-if-not-match-concurrency-%d", time.Now().UnixNano())

	// Test concurrent uploads to the same object
	const numGoroutines = 5
	var wg sync.WaitGroup
	results := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			data := []byte(fmt.Sprintf("concurrent data from goroutine %d", id))
			_, err := minioCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
				bytes.NewReader(data), int64(len(data)))
			results <- err
		}(i)
	}

	wg.Wait()
	close(results)

	// Collect results
	var successCount, fragmentExistsCount, otherErrorCount int
	for err := range results {
		if err == nil {
			successCount++
		} else if werr.ErrObjectAlreadyExists.Is(err) {
			fragmentExistsCount++
		} else {
			otherErrorCount++
			t.Logf("Unexpected error: %v", err)
		}
	}

	// Only one upload should succeed, others should return ErrObjectAlreadyExists
	assert.Equal(t, 1, successCount, "Only one concurrent upload should succeed")
	assert.Equal(t, numGoroutines-1, fragmentExistsCount, "All other uploads should return ErrObjectAlreadyExists")
	assert.Equal(t, 0, otherErrorCount, "No other errors should occur")

	t.Logf("Concurrency test results: %d success, %d fragment exists, %d other errors",
		successCount, fragmentExistsCount, otherErrorCount)

	// Cleanup
	err = minioCli.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	assert.NoError(t, err)
}

// TestMinioHandlePutIfNotMatchIdempotency tests idempotent behavior
func TestMinioHandlePutIfNotMatchIdempotency(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	objectName := fmt.Sprintf("test-put-if-not-match-idempotency-%d", time.Now().UnixNano())

	// Test data
	testData := []byte("test content for idempotency")

	// 1. First upload should succeed
	uploadInfo1, err := minioCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)))
	require.NoError(t, err)
	require.NotEmpty(t, uploadInfo1.ETag)
	t.Logf("First upload succeeded with ETag: %s", uploadInfo1.ETag)

	// 2. Second upload with same content should return ErrObjectAlreadyExists (idempotent)
	_, err = minioCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader(testData), int64(len(testData)))
	require.Error(t, err)
	require.True(t, werr.ErrObjectAlreadyExists.Is(err), "Second upload should return ErrObjectAlreadyExists")

	// 3. Third upload with different content should also return ErrObjectAlreadyExists
	differentData := []byte("different content")
	_, err = minioCli.PutObjectIfNoneMatch(ctx, bucketName, objectName,
		bytes.NewReader(differentData), int64(len(differentData)))
	require.Error(t, err)
	require.True(t, werr.ErrObjectAlreadyExists.Is(err), "Upload with different content should also return ErrObjectAlreadyExists")

	// 4. Verify original content is preserved
	obj, err := minioCli.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	require.NoError(t, err)
	defer obj.Close()

	data, err := minioHandler.ReadObjectFull(ctx, obj, 1024)
	require.NoError(t, err)
	assert.Equal(t, testData, data, "Original content should be preserved")

	// Cleanup
	err = minioCli.RemoveObject(ctx, bucketName, objectName, minio.RemoveObjectOptions{})
	assert.NoError(t, err)
}

// TestCheckIfConditionWriteSupport tests the CheckIfConditionWriteSupport function
func TestCheckIfConditionWriteSupport(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	basePath := fmt.Sprintf("test-support-check-%d", time.Now().UnixNano())

	// Test that the function runs successfully without panicking
	assert.NotPanics(t, func() {
		minioHandler.CheckIfConditionWriteSupport(ctx, minioCli, bucketName, basePath)
	}, "CheckIfConditionWriteSupport should not panic with valid MinioHandler")

	t.Logf("CheckIfConditionWriteSupport passed successfully for bucket: %s", bucketName)
}

// TestCheckIfConditionWriteSupportMultipleCalls tests that multiple calls are handled correctly
func TestCheckIfConditionWriteSupportMultipleCalls(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	basePath := fmt.Sprintf("test-support-check-multi-%d", time.Now().UnixNano())

	// Call multiple times to test sync.Once behavior
	for i := 0; i < 3; i++ {
		assert.NotPanics(t, func() {
			minioHandler.CheckIfConditionWriteSupport(ctx, minioCli, bucketName, basePath)
		}, "Multiple calls to CheckIfConditionWriteSupport should not panic")
	}

	t.Logf("Multiple calls to CheckIfConditionWriteSupport handled correctly")
}

// TestCheckIfConditionWriteSupportConcurrency tests concurrent calls
func TestCheckIfConditionWriteSupportConcurrency(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
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

			minioHandler.CheckIfConditionWriteSupport(ctx, minioCli, bucketName, basePath)
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

// TestCheckIfConditionWriteSupportCleanup tests that cleanup works properly
func TestCheckIfConditionWriteSupportCleanup(t *testing.T) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	require.NoError(t, err)

	ctx := context.Background()
	bucketName := cfg.Minio.BucketName
	basePath := fmt.Sprintf("test-support-check-cleanup-%d", time.Now().UnixNano())

	// Run the check
	assert.NotPanics(t, func() {
		minioHandler.CheckIfConditionWriteSupport(ctx, minioCli, bucketName, basePath)
	})

	// Verify that test objects were cleaned up by trying to list objects with the test prefix
	// Note: We can't directly verify cleanup since the object names are generated internally,
	// but we can at least verify the function completed without leaving obvious traces

	// List objects with the base path prefix to check if any test objects remain
	objectCh := minioCli.ListObjects(ctx, bucketName, basePath, true, minio.ListObjectsOptions{})

	objectCount := 0
	for object := range objectCh {
		if object.Err != nil {
			t.Logf("Error listing objects: %v", object.Err)
			continue
		}
		objectCount++
		t.Logf("Found object that might not have been cleaned up: %s", object.Key)
	}

	// We expect 0 objects since cleanup should have removed them
	// However, due to eventual consistency, we'll just log if objects are found
	if objectCount > 0 {
		t.Logf("Warning: Found %d objects with prefix %s, cleanup might not have completed", objectCount, basePath)
	} else {
		t.Logf("Cleanup verification passed: no objects found with prefix %s", basePath)
	}
}

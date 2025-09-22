package minio

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
)

var (
	checkOnce sync.Once
)

func CheckIfConditionWriteSupport(ctx context.Context, minioHandler MinioHandler, bucketName string, basePath string) {
	checkOnce.Do(func() {
		doCheckIfConditionWriteSupport(ctx, minioHandler, bucketName, basePath)
	})
}

// CheckIfConditionWriteSupport checks if MinioHandler supports PutObjectIfNoneMatch and PutFencedObject
// If not supported, it will panic to prevent runtime errors
func doCheckIfConditionWriteSupport(ctx context.Context, minioHandler MinioHandler, bucketName string, basePath string) {
	var testObjectName = fmt.Sprintf("%s/%s", basePath, generateUniqueTestID())

	// Create a temporary bucket for testing (if it doesn't exist)
	// Note: This assumes the minioHandler has access to create buckets or the bucket exists

	defer func() {
		// Clean up test objects with proper error handling and logging
		var fencedObjectName = testObjectName + "-fenced" // Clean up regular test object
		if err := minioHandler.RemoveObject(ctx, bucketName, testObjectName, minio.RemoveObjectOptions{}); err != nil {
			// Only log if it's not a "object not found" error
			if !IsObjectNotExists(err) {
				logger.Ctx(ctx).Warn("Failed to clean up test object during support check",
					zap.String("bucket", bucketName),
					zap.String("object", testObjectName),
					zap.Error(err))
			}
		} else {
			logger.Ctx(ctx).Debug("Successfully cleaned up test object",
				zap.String("bucket", bucketName),
				zap.String("object", testObjectName))
		}

		// Clean up fenced test object
		if err := minioHandler.RemoveObject(ctx, bucketName, fencedObjectName, minio.RemoveObjectOptions{}); err != nil {
			// Only log if it's not a "object not found" error
			if !IsObjectNotExists(err) {
				logger.Ctx(ctx).Warn("Failed to clean up fenced test object during support check",
					zap.String("bucket", bucketName),
					zap.String("object", fencedObjectName),
					zap.Error(err))
			}
		} else {
			logger.Ctx(ctx).Debug("Successfully cleaned up fenced test object",
				zap.String("bucket", bucketName),
				zap.String("object", fencedObjectName))
		}
	}()

	// Test 1: Check PutObjectIfNoneMatch support
	testData := []byte("test-support-check")

	// First upload should succeed
	_, err := minioHandler.PutObjectIfNoneMatch(ctx, bucketName, testObjectName,
		bytes.NewReader(testData), int64(len(testData)))
	if err != nil {
		// Check if it's a bucket not found error or other infrastructure issue
		panic(fmt.Sprintf("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch not supported or failed. "+
			"This indicates the underlying storage system doesn't support conditional writes (If-None-Match). Error: %v", err))
	}

	// Second upload to same object should return ErrObjectAlreadyExists
	_, err = minioHandler.PutObjectIfNoneMatch(ctx, bucketName, testObjectName,
		bytes.NewReader(testData), int64(len(testData)))
	if err == nil {
		panic("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch should return error for existing object, " +
			"but it succeeded. This indicates conditional write protection is not working properly.")
	}
	if !werr.ErrObjectAlreadyExists.Is(err) {
		panic(fmt.Sprintf("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch should return ErrObjectAlreadyExists "+
			"for existing object, but got: %v. This indicates the error handling for conditional writes is incorrect.", err))
	}

	// Test 2: Check PutFencedObject support
	fencedObjectName := testObjectName + "-fenced"

	// First fenced object upload should succeed
	_, err = minioHandler.PutFencedObject(ctx, bucketName, fencedObjectName)
	if err != nil {
		panic(fmt.Sprintf("CheckIfConditionWriteSupport failed: PutFencedObject not supported or failed. "+
			"This indicates the storage system doesn't support fenced objects (conditional writes with metadata). Error: %v", err))
	}

	// Test that PutObjectIfNoneMatch detects fenced objects correctly
	_, err = minioHandler.PutObjectIfNoneMatch(ctx, bucketName, fencedObjectName,
		bytes.NewReader([]byte("should-fail")), 11)
	if err == nil {
		panic("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch should return error for fenced object, " +
			"but it succeeded. This indicates fenced object detection is not working properly.")
	}
	if !werr.ErrSegmentFenced.Is(err) {
		panic(fmt.Sprintf("CheckIfConditionWriteSupport failed: PutObjectIfNoneMatch should return ErrSegmentFenced "+
			"for fenced object, but got: %v. This indicates fenced object detection or error handling is incorrect.", err))
	}

	// Test 3: Check that fenced object upload is idempotent
	_, err = minioHandler.PutFencedObject(ctx, bucketName, fencedObjectName)
	if err != nil {
		panic(fmt.Sprintf("CheckIfConditionWriteSupport failed: PutFencedObject should be idempotent "+
			"(succeed when fenced object already exists), but got error: %v", err))
	}

	logger.Ctx(ctx).Info("Conditional write support check passed successfully",
		zap.String("bucket", bucketName),
		zap.String("testObject", testObjectName))
}

// generateUniqueTestID generates a unique identifier for test objects
// combining timestamp, process ID and random bytes to avoid conflicts
func generateUniqueTestID() string {
	// Get current timestamp in nanoseconds
	timestamp := time.Now().UnixNano()

	// Get process ID
	pid := os.Getpid()

	// Generate 8 random bytes
	randomBytes := make([]byte, 8)
	_, err := rand.Read(randomBytes)
	if err != nil {
		// Fallback to timestamp-based randomness if crypto/rand fails
		randomBytes = []byte(fmt.Sprintf("%08x", timestamp&0xffffffff))
	}
	randomHex := hex.EncodeToString(randomBytes)

	// Combine all components: timestamp-pid-random
	return fmt.Sprintf("test-%d-%d-%s", timestamp, pid, randomHex)
}

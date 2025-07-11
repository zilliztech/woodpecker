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
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/zilliztech/woodpecker/common/werr"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zilliztech/woodpecker/common/logger"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

var (
	testBucket = "a-bucket"
)

func setupMinioFileWriterTest(t *testing.T) (minioHandler.MinioHandler, *config.Configuration) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	// Set log level to debug for detailed logging
	cfg.Log.Level = "debug"

	// Initialize logger with debug level
	logger.InitLogger(cfg)

	testBucket = cfg.Minio.BucketName
	minioHdl, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	require.NoError(t, err)
	return minioHdl, cfg
}

func cleanupMinioFileWriterObjects(t *testing.T, client minioHandler.MinioHandler, prefix string) {
	ctx := context.Background()
	objectCh := client.ListObjects(ctx, testBucket, prefix, true, minio.ListObjectsOptions{})

	for object := range objectCh {
		if object.Err != nil {
			t.Logf("Warning: failed to list object for cleanup: %v", object.Err)
			continue
		}
		err := client.RemoveObject(ctx, testBucket, object.Key, minio.RemoveObjectOptions{})
		if err != nil {
			t.Logf("Warning: failed to cleanup object %s: %v", object.Key, err)
		}
	}
}

func generateTestData(size int) []byte {
	data := make([]byte, size)
	_, err := rand.Read(data)
	if err != nil {
		panic(err)
	}
	return data
}

func TestMinioFileWriter_BasicWriteAndSync(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(1)
	segmentId := int64(100)
	baseDir := fmt.Sprintf("test-basic-write-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// Create MinioFileWriter
	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	t.Run("WriteDataAsync", func(t *testing.T) {
		// Test writing data
		testData := [][]byte{
			[]byte("Hello, MinIO!"),
			[]byte("This is test data"),
			generateTestData(1024), // 1KB
			generateTestData(2048), // 2KB
		}

		for i, data := range testData {
			entryId := int64(i)
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-basic-%d", entryId))

			returnedId, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)
			require.NoError(t, err)
			assert.Equal(t, entryId, returnedId)

			// Wait for result
			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			assert.Equal(t, entryId, result.SyncedId)
		}

		// Verify writer state
		assert.Equal(t, int64(0), writer.GetFirstEntryId(ctx))
		assert.Equal(t, int64(3), writer.GetLastEntryId(ctx))
	})

	t.Run("ManualSync", func(t *testing.T) {
		// Test manual sync
		err := writer.Sync(ctx)
		require.NoError(t, err)
	})

	// Close writer
	err = writer.Close(ctx)
	require.NoError(t, err)
}

func TestMinioFileWriter_LargeDataAndMultipleBlocks(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	// Configure for smaller blocks to test multi-block scenario
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 256 * 1024 // 256KB per block

	logId := int64(2)
	segmentId := int64(200)
	baseDir := fmt.Sprintf("test-large-data-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	defer writer.Close(ctx)

	// Write data that will span multiple blocks
	largeData := [][]byte{
		generateTestData(200 * 1024), // 200KB
		generateTestData(200 * 1024), // 200KB - should trigger new block
		generateTestData(100 * 1024), // 100KB
		generateTestData(150 * 1024), // 150KB - should trigger another block
		[]byte("Final small entry"),
	}

	var wg sync.WaitGroup
	for i, data := range largeData {
		wg.Add(1)
		go func(entryId int64, entryData []byte) {
			defer wg.Done()

			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-large-%d", entryId))
			returnedId, err := writer.WriteDataAsync(ctx, entryId, entryData, resultCh)
			require.NoError(t, err)
			assert.Equal(t, entryId, returnedId)

			// Wait for result
			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			assert.Equal(t, entryId, result.SyncedId)
		}(int64(i), data)
	}

	wg.Wait()

	// Verify final state
	assert.Equal(t, int64(0), writer.GetFirstEntryId(ctx))
	assert.Equal(t, int64(len(largeData)-1), writer.GetLastEntryId(ctx))

	// Verify objects were created in MinIO
	objectCh := minioHdl.ListObjects(ctx, testBucket, baseDir, true, minio.ListObjectsOptions{})
	objectCount := 0
	for object := range objectCh {
		require.NoError(t, object.Err)
		objectCount++
		t.Logf("Created object: %s (size: %d)", object.Key, object.Size)
	}
	assert.Greater(t, objectCount, 1, "Should have created multiple objects")
}

func TestMinioFileWriter_ConcurrentWrites(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(3)
	segmentId := int64(300)
	baseDir := fmt.Sprintf("test-concurrent-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	defer writer.Close(ctx)

	// Test concurrent writes
	const numGoroutines = 10
	const entriesPerGoroutine = 5

	var wg sync.WaitGroup
	results := make(chan error, numGoroutines*entriesPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineId int) {
			defer wg.Done()

			for e := 0; e < entriesPerGoroutine; e++ {
				entryId := int64(goroutineId*entriesPerGoroutine + e)
				data := []byte(fmt.Sprintf("Goroutine %d, Entry %d: %s", goroutineId, e, generateTestData(512)))

				resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-concurrent-%d", entryId))
				_, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)
				if err != nil {
					results <- err
					continue
				}

				// Wait for result
				result, err := resultCh.ReadResult(ctx)
				if err != nil {
					results <- err
				} else {
					results <- result.Err
				}
			}
		}(g)
	}

	wg.Wait()
	close(results)

	// Check results
	successCount := 0
	for err := range results {
		if err == nil {
			successCount++
		} else {
			t.Logf("Write error: %v", err)
		}
	}

	expectedEntries := numGoroutines * entriesPerGoroutine
	assert.Equal(t, expectedEntries, successCount, "All concurrent writes should succeed")

	// Verify final state
	assert.Equal(t, int64(0), writer.GetFirstEntryId(ctx))
	assert.Equal(t, int64(expectedEntries-1), writer.GetLastEntryId(ctx))
}

func TestMinioFileWriter_Finalize(t *testing.T) {

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(4)
	segmentId := int64(400)
	baseDir := fmt.Sprintf("test-finalize-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)

	// Write some test data
	testData := [][]byte{
		[]byte("Entry 1"),
		[]byte("Entry 2"),
		generateTestData(1024),
		[]byte("Final entry"),
	}

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-finalize-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		// Wait for result
		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	// Test finalize
	lastEntryId, err := writer.Finalize(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testData)-1), lastEntryId)

	// Verify objects were created including footer
	objectCh := minioHdl.ListObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, minio.ListObjectsOptions{})
	objects := make([]string, 0)
	for object := range objectCh {
		require.NoError(t, object.Err)
		objects = append(objects, object.Key)
		t.Logf("Finalized object: %s (size: %d)", object.Key, object.Size)
	}
	assert.Greater(t, len(objects), 1, "Should have data objects and footer object")

	// Verify we can't write after finalize
	resultCh := channel.NewLocalResultChannel("test-finalize-fail")
	_, err = writer.WriteDataAsync(ctx, 999, []byte("should fail"), resultCh)
	assert.Error(t, err, "Should not be able to write after finalize")
}

func TestMinioFileWriter_ErrorHandling(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	t.Run("DuplicateEntryIdWithWrittenID", func(t *testing.T) {
		logId := int64(5)
		segmentId := int64(500)
		baseDir := fmt.Sprintf("test-duplicate-entry-%d", time.Now().Unix())
		defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

		writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		defer writer.Close(ctx)

		// Write first entry
		resultCh1 := channel.NewLocalResultChannel("test-duplicate-1")
		_, err = writer.WriteDataAsync(ctx, 0, []byte("first"), resultCh1)
		require.NoError(t, err)

		// Force sync to ensure first entry is processed
		err = writer.Sync(ctx)
		require.NoError(t, err)

		// Wait for first write with timeout
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		result, err := resultCh1.ReadResult(ctxWithTimeout)
		cancel()
		require.NoError(t, err)
		require.NoError(t, result.Err)

		// Try to write same entry ID again (should be handled gracefully)
		resultCh2 := channel.NewLocalResultChannel("test-duplicate-2")
		_, err = writer.WriteDataAsync(ctx, 0, []byte("duplicate"), resultCh2)
		require.NoError(t, err)

		// Wait for second write with timeout - this should return immediately since it's a duplicate
		ctxWithTimeout, cancel = context.WithTimeout(ctx, 5*time.Second)
		result, err = resultCh2.ReadResult(ctxWithTimeout)
		cancel()
		require.NoError(t, err)
		require.NoError(t, result.Err) // Should succeed (idempotent)
	})

	t.Run("DuplicateEntryIdInBuffer", func(t *testing.T) {
		logId := int64(5)
		segmentId := int64(500)
		baseDir := fmt.Sprintf("test-duplicate-entry-%d", time.Now().Unix())
		defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

		writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		defer writer.Close(ctx)

		// Write first entry
		resultCh1 := channel.NewLocalResultChannel("test-duplicate-1")
		_, err = writer.WriteDataAsync(ctx, 0, []byte("first"), resultCh1)
		require.NoError(t, err)
		// Try to write same entry ID again (should be handled gracefully)
		resultCh2 := channel.NewLocalResultChannel("test-duplicate-2")
		_, err = writer.WriteDataAsync(ctx, 0, []byte("duplicate"), resultCh2)
		require.NoError(t, err)

		// Force sync to ensure first entry is processed
		err = writer.Sync(ctx)
		require.NoError(t, err)

		// Wait for second write with timeout - this should return immediately since it's a duplicate
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		result, err := resultCh2.ReadResult(ctxWithTimeout)
		cancel()
		require.NoError(t, err)
		require.NoError(t, result.Err) // Should succeed (idempotent)

		// Wait for first write with timeout
		ctxWithTimeout, cancel = context.WithTimeout(ctx, 5*time.Second)
		result, err = resultCh1.ReadResult(ctxWithTimeout)
		cancel()
		//require.NoError(t, err) // TODO maybe handle this notify gracefully
		//require.NoError(t, result.Err)
		assert.Error(t, err)
		assert.True(t, errors.IsAny(err, context.Canceled, context.DeadlineExceeded))

	})

	t.Run("DuplicateEntryIdInUploading", func(t *testing.T) {
		logId := int64(5)
		segmentId := int64(500)
		baseDir := fmt.Sprintf("test-duplicate-entry-%d", time.Now().Unix())
		defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

		writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		defer writer.Close(ctx)

		// Write first entry
		resultCh1 := channel.NewLocalResultChannel("test-duplicate-1")
		_, err = writer.WriteDataAsync(ctx, 0, []byte("first"), resultCh1)
		require.NoError(t, err)

		// Force sync to ensure first entry is processed
		err = writer.Sync(ctx)
		require.NoError(t, err)

		// Try to write same entry ID again (should be handled gracefully)
		resultCh2 := channel.NewLocalResultChannel("test-duplicate-2")
		_, err = writer.WriteDataAsync(ctx, 0, []byte("duplicate"), resultCh2)
		require.NoError(t, err)

		// Wait for first write with timeout
		ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		result, err := resultCh1.ReadResult(ctxWithTimeout)
		cancel()
		require.NoError(t, err)
		require.NoError(t, result.Err)

		//// Wait for second write with timeout - this should return immediately since it's a duplicate
		//ctxWithTimeout, cancel = context.WithTimeout(ctx, 5*time.Second)
		//result, err = resultCh2.ReadResult(ctxWithTimeout)
		//cancel()
		//require.NoError(t, err)
		//require.NoError(t, result.Err) // Should succeed (idempotent)
	})

	t.Run("WriteAfterClose", func(t *testing.T) {
		logId := int64(6)
		segmentId := int64(600)
		baseDir := fmt.Sprintf("test-write-after-close-%d", time.Now().Unix())
		defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

		writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)

		// Close writer
		err = writer.Close(ctx)
		require.NoError(t, err)

		// Try to write after close
		resultCh := channel.NewLocalResultChannel("test-write-after-close")
		_, err = writer.WriteDataAsync(ctx, 1, []byte("should fail"), resultCh)
		assert.Error(t, err, "Should not be able to write after close")
	})
}

func TestMinioFileWriter_BlockLastRecordVerification(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	// Configure for small blocks to ensure we get BlockLastRecord
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 200 // Very small to force block creation

	logId := int64(7)
	segmentId := int64(700)
	baseDir := fmt.Sprintf("test-block-last-record-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	defer writer.Close(ctx)

	// Write data that will create multiple blocks
	testData := [][]byte{
		[]byte("Entry 0: " + string(generateTestData(100))),
		[]byte("Entry 1: " + string(generateTestData(100))),
		[]byte("Entry 2: " + string(generateTestData(100))),
	}

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-block-last-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	// Force final sync
	err = writer.Sync(ctx)
	require.NoError(t, err)

	// Read back the objects and verify they contain BlockLastRecord
	objectCh := minioHdl.ListObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, minio.ListObjectsOptions{})

	for object := range objectCh {
		require.NoError(t, object.Err)

		// Skip lock files
		if len(object.Key) >= 5 && object.Key[len(object.Key)-5:] == ".lock" {
			continue
		}

		t.Logf("Verifying object: %s (size: %d)", object.Key, object.Size)

		// Read object content
		obj, err := minioHdl.GetObject(ctx, testBucket, object.Key, minio.GetObjectOptions{})
		require.NoError(t, err)

		data, err := minioHandler.ReadObjectFull(ctx, obj, int64(object.Size))
		require.NoError(t, err)
		obj.Close()

		// Parse records from the object
		records, err := codec.DecodeRecordList(data)
		require.NoError(t, err)

		if len(records) == 0 {
			t.Logf("Object contains no records, skipping verification")
			continue
		}

		t.Logf("Object contains %d records", len(records))

		// Check if first record is BlockHeaderRecord (after optional HeaderRecord)
		var blockHeaderRecord *codec.BlockHeaderRecord
		for _, record := range records {
			if record.Type() == codec.BlockHeaderRecordType {
				blockHeaderRecord = record.(*codec.BlockHeaderRecord)
				break
			}
		}

		if blockHeaderRecord != nil {
			t.Logf("Found BlockHeaderRecord: FirstEntryID=%d, LastEntryID=%d",
				blockHeaderRecord.FirstEntryID, blockHeaderRecord.LastEntryID)

			// Verify the entry IDs are valid
			assert.GreaterOrEqual(t, blockHeaderRecord.FirstEntryID, int64(0))
			assert.GreaterOrEqual(t, blockHeaderRecord.LastEntryID, blockHeaderRecord.FirstEntryID)
		} else {
			t.Logf("No BlockHeaderRecord found in this object")
		}
	}
}

func TestMinioFileWriter_SegmentLocking(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(8)
	segmentId := int64(800)
	baseDir := fmt.Sprintf("test-segment-locking-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// Create first writer
	writer1, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)

	// Try to create second writer with same segment (should fail due to lock)
	writer2, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	assert.Error(t, err, "Should not be able to create second writer for same segment")
	assert.Nil(t, writer2)

	// Close first writer
	err = writer1.Close(ctx)
	require.NoError(t, err)

	// Now should be able to create new writer
	writer3, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer3)

	err = writer3.Close(ctx)
	require.NoError(t, err)
}

func TestMinioFileWriter_RecoveryAfterInterruption(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(10)
	segmentId := int64(1000)
	baseDir := fmt.Sprintf("test-recovery-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	t.Run("WriteDataAndInterrupt", func(t *testing.T) {
		// Create first writer and write some data
		writer1, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		require.NotNil(t, writer1)

		// Write test data
		testData := [][]byte{
			[]byte("Entry 0: Initial data"),
			[]byte("Entry 1: More data"),
			generateTestData(1024), // 1KB
			[]byte("Entry 3: Final entry before interruption"),
		}

		for i, data := range testData {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-recovery-write-%d", i))
			_, err := writer1.WriteDataAsync(ctx, int64(i), data, resultCh)
			require.NoError(t, err)

			// Wait for result
			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
		}

		// Force sync to ensure data is written to object storage
		err = writer1.Sync(ctx)
		require.NoError(t, err)

		// Wait a bit for async operations to complete
		time.Sleep(100 * time.Millisecond)

		// Verify initial state
		assert.Equal(t, int64(0), writer1.GetFirstEntryId(ctx))
		assert.Equal(t, int64(3), writer1.GetLastEntryId(ctx))

		// Simulate interruption by closing the writer without finalize
		// Note: Close may trigger Complete() which calls Finalize(), but this simulates a real-world scenario
		err = writer1.Close(ctx)
		if err != nil {
			t.Logf("Expected error during close (simulating interruption): %v", err)
		}

		// Verify objects were created in MinIO
		objectCh := minioHdl.ListObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, minio.ListObjectsOptions{})
		objectCount := 0
		hasFooter := false
		for object := range objectCh {
			require.NoError(t, object.Err)
			if !strings.HasSuffix(object.Key, ".lock") {
				objectCount++
				if strings.HasSuffix(object.Key, "footer.blk") {
					hasFooter = true
				}
				t.Logf("Created object after interruption: %s (size: %d)", object.Key, object.Size)
			}
		}
		assert.Greater(t, objectCount, 0, "Should have created at least one data object")

		// Check if footer exists to determine test path
		if hasFooter {
			t.Log("Footer found - segment was completed during close, testing recovery from completed segment")
		} else {
			t.Log("No footer found - segment was interrupted, testing recovery from incomplete segment")
		}
	})

	var recoverySegmentFileKey string
	t.Run("RecoverAndContinueWriting", func(t *testing.T) {
		// Use the same segment key to test true recovery
		recoverySegmentFileKey = baseDir

		// Check if segment has footer first
		objectCh := minioHdl.ListObjects(ctx, testBucket, baseDir, true, minio.ListObjectsOptions{})
		hasFooter := false
		for object := range objectCh {
			require.NoError(t, object.Err)
			if strings.HasSuffix(object.Key, "footer.blk") {
				hasFooter = true
				break
			}
		}

		if hasFooter {
			// Segment is completed - test that we cannot write to it
			t.Log("Testing recovery from completed segment (should not be writable)")

			// Create a new writer for the completed segment
			writer2, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, recoverySegmentFileKey, logId, segmentId, minioHdl, cfg, true)
			require.NoError(t, err)
			require.NotNil(t, writer2)

			// Check recovered state
			recoveredFirstEntryId := writer2.GetFirstEntryId(ctx)
			recoveredLastEntryId := writer2.GetLastEntryId(ctx)
			t.Logf("Recovered state: firstEntryId=%d, lastEntryId=%d", recoveredFirstEntryId, recoveredLastEntryId)

			// Verify we cannot write to a completed segment
			resultCh := channel.NewLocalResultChannel("test-recovery-fail")
			_, err = writer2.WriteDataAsync(ctx, recoveredLastEntryId+1, []byte("Should fail"), resultCh)
			assert.Error(t, err, "Should not be able to write to a completed segment")
			assert.Contains(t, err.Error(), "not writable", "Error should indicate segment is not writable")

			// Close the writer
			err = writer2.Close(ctx)
			require.NoError(t, err)
		} else {
			// Segment is incomplete - test normal recovery
			t.Log("Testing recovery from incomplete segment (should be writable)")

			// Create a new writer for continuation (recovery scenario)
			writer2, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, recoverySegmentFileKey, logId, segmentId, minioHdl, cfg, true)
			require.NoError(t, err)
			require.NotNil(t, writer2)

			// Check recovered state
			recoveredFirstEntryId := writer2.GetFirstEntryId(ctx)
			recoveredLastEntryId := writer2.GetLastEntryId(ctx)
			t.Logf("Recovered state: firstEntryId=%d, lastEntryId=%d", recoveredFirstEntryId, recoveredLastEntryId)

			// Continue writing additional data from where we left off
			nextEntryId := recoveredLastEntryId + 1
			additionalData := [][]byte{
				[]byte("Recovery data 1"),
				[]byte("Recovery data 2"),
				generateTestData(512), // 512B
			}

			for i, data := range additionalData {
				entryId := nextEntryId + int64(i)
				resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-recovery-continue-%d", entryId))
				_, err := writer2.WriteDataAsync(ctx, entryId, data, resultCh)
				require.NoError(t, err)

				// Wait for result
				result, err := resultCh.ReadResult(ctx)
				require.NoError(t, err)
				require.NoError(t, result.Err)
			}

			// Force sync
			err = writer2.Sync(ctx)
			require.NoError(t, err)

			// Wait a bit for async operations to complete
			time.Sleep(100 * time.Millisecond)

			// Verify final state before finalize
			expectedLastEntryId := nextEntryId + int64(len(additionalData)) - 1
			assert.Equal(t, recoveredFirstEntryId, writer2.GetFirstEntryId(ctx))
			assert.Equal(t, expectedLastEntryId, writer2.GetLastEntryId(ctx))

			// Finalize the segment with footer and index
			lastEntryId, err := writer2.Finalize(ctx)
			require.NoError(t, err)
			assert.Equal(t, expectedLastEntryId, lastEntryId)

			// Close the writer
			err = writer2.Close(ctx)
			require.NoError(t, err)
		}
	})

	t.Run("VerifyOriginalSegmentObjects", func(t *testing.T) {
		// Verify that the objects were created
		objectCh := minioHdl.ListObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, minio.ListObjectsOptions{})

		dataObjects := make([]string, 0)
		for object := range objectCh {
			require.NoError(t, object.Err)
			if strings.HasSuffix(object.Key, ".blk") {
				dataObjects = append(dataObjects, object.Key)
				t.Logf("Found segment object: %s (size: %d)", object.Key, object.Size)
			}
		}

		assert.Greater(t, len(dataObjects), 0, "Should have created data objects")
		t.Logf("Total data objects: %d", len(dataObjects))
	})

	t.Run("VerifyFinalSegmentState", func(t *testing.T) {
		// Create reader to verify the final segment state
		reader, err := objectstorage.NewMinioFileReader(ctx, testBucket, recoverySegmentFileKey, logId, segmentId, minioHdl)
		require.NoError(t, err)
		require.NotNil(t, reader)

		// Verify segment has footer (should be completed by now)
		footer := reader.GetFooter()
		require.NotNil(t, footer, "Footer should exist after completion")
		assert.Greater(t, footer.TotalBlocks, int32(0), "Should have at least one block")
		assert.Greater(t, footer.TotalRecords, uint32(0), "Should have at least one record")

		// Verify block indexes
		blocks := reader.GetBlockIndexes()
		require.Greater(t, len(blocks), 0, "Should have at least one block index")

		// Verify we can read all entries
		lastEntryId, err := reader.GetLastEntryID(ctx)
		require.NoError(t, err)
		t.Logf("Reader last entry ID: %d", lastEntryId)

		// Verify we can read data from the beginning
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    20, // Read all entries
		})
		require.NoError(t, err)
		t.Logf("Read %d entries", len(entries))

		// We should have at least the original 4 entries
		assert.GreaterOrEqual(t, len(entries), 4, "Should have at least the original 4 entries")

		// Verify some entry content (at least the first few)
		if len(entries) > 0 {
			assert.Equal(t, int64(0), entries[0].EntryId)
			assert.Equal(t, []byte("Entry 0: Initial data"), entries[0].Values)
		}
		if len(entries) > 1 {
			assert.Equal(t, int64(1), entries[1].EntryId)
			assert.Equal(t, []byte("Entry 1: More data"), entries[1].Values)
		}

		err = reader.Close(ctx)
		require.NoError(t, err)
	})

	t.Run("VerifyAllObjectsExist", func(t *testing.T) {
		// List all objects and verify structure
		objectCh := minioHdl.ListObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, minio.ListObjectsOptions{})

		dataObjects := make([]string, 0)

		for object := range objectCh {
			require.NoError(t, object.Err)
			if strings.HasSuffix(object.Key, ".lock") {
				continue // Skip lock files
			}

			if strings.Contains(object.Key, ".blk") {
				dataObjects = append(dataObjects, object.Key)
			}

			t.Logf("Final object: %s (size: %d)", object.Key, object.Size)
		}

		assert.Greater(t, len(dataObjects), 0, "Should have data objects")
		t.Logf("Total data objects: %d", len(dataObjects))
	})
}

// Benchmark tests
func BenchmarkMinioFileWriter_WriteDataAsync(b *testing.B) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		b.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(&testing.T{})
	ctx := context.Background()

	logId := int64(9)
	segmentId := int64(900)
	baseDir := fmt.Sprintf("bench-write-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(&testing.T{}, minioHdl, baseDir)

	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer writer.Close(ctx)

	data := generateTestData(1024) // 1KB per entry

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("bench-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		if err != nil {
			b.Fatal(err)
		}

		// Wait for result
		result, err := resultCh.ReadResult(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if result.Err != nil {
			b.Fatal(result.Err)
		}
	}
}

// testSerialize replicates the serialize function for testing
func testSerialize(entries []*cache.BufferEntry) []byte {
	if len(entries) == 0 {
		return []byte{}
	}

	serializedData := make([]byte, 0)

	// Serialize all data records
	for _, entry := range entries {
		dataRecord, _ := codec.ParseData(entry.Data)
		encodedRecord := codec.EncodeRecord(dataRecord)
		serializedData = append(serializedData, encodedRecord...)
	}

	// Add BlockHeaderRecord at the beginning of the block
	firstEntryID := entries[0].EntryId
	lastEntryID := entries[len(entries)-1].EntryId

	blockHeaderRecord := &codec.BlockHeaderRecord{
		FirstEntryID: firstEntryID,
		LastEntryID:  lastEntryID,
	}

	encodedBlockHeaderRecord := codec.EncodeRecord(blockHeaderRecord)
	// Insert at the beginning instead of appending at the end
	finalData := make([]byte, 0, len(serializedData)+len(encodedBlockHeaderRecord))
	finalData = append(finalData, encodedBlockHeaderRecord...)
	finalData = append(finalData, serializedData...)
	return finalData

	return serializedData
}

// TestSerializeDecodeCompatibility tests that serialize and DecodeRecordList work together
func TestSerializeDecodeCompatibility(t *testing.T) {
	// Create test data
	entries := []*cache.BufferEntry{
		{EntryId: 0, Data: []byte("Test data 1")},
		{EntryId: 1, Data: []byte("Test data 2")},
	}

	// Serialize the entries
	serializedData := testSerialize(entries)
	t.Logf("Serialized data size: %d bytes", len(serializedData))

	// Try to decode the records
	records, err := codec.DecodeRecordList(serializedData)
	require.NoError(t, err)
	t.Logf("Decoded %d records", len(records))

	// Print record types
	for i, record := range records {
		t.Logf("Record %d: type=%T", i, record)
		if blockHeaderRecord, ok := record.(*codec.BlockHeaderRecord); ok {
			t.Logf("  BlockHeaderRecord: FirstEntryID=%d, LastEntryID=%d", blockHeaderRecord.FirstEntryID, blockHeaderRecord.LastEntryID)
		}
	}

	// Verify we have the expected records
	assert.Greater(t, len(records), 0, "Should have at least one record")

	// Look for BlockHeaderRecord
	var blockHeaderRecord *codec.BlockHeaderRecord
	for _, record := range records {
		if bhr, ok := record.(*codec.BlockHeaderRecord); ok {
			blockHeaderRecord = bhr
			break
		}
	}

	require.NotNil(t, blockHeaderRecord, "Should find BlockHeaderRecord")
	assert.Equal(t, int64(0), blockHeaderRecord.FirstEntryID)
	assert.Equal(t, int64(1), blockHeaderRecord.LastEntryID)
}

// TestMinioFileWriter_VerifyBlockLastRecord tests that written data contains BlockLastRecord
func TestMinioFileWriter_VerifyBlockLastRecord(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(1)
	segmentId := int64(100)
	baseDir := fmt.Sprintf("test-block-last-record-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// Create writer
	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Write some data
	testData := [][]byte{
		[]byte("Test data 1"),
		[]byte("Test data 2"),
	}

	for i, data := range testData {
		entryId := int64(i)
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-block-last-record-%d", entryId))
		_, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)
		require.NoError(t, err)

		// Wait for result
		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	// Force sync
	err = writer.Sync(ctx)
	require.NoError(t, err)

	// Close writer
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Now read the objects directly and check their content
	objectCh := minioHdl.ListObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, minio.ListObjectsOptions{})

	for object := range objectCh {
		require.NoError(t, object.Err)
		if strings.HasSuffix(object.Key, ".blk") {
			t.Logf("Checking object: %s (size: %d)", object.Key, object.Size)

			// Read the object
			obj, err := minioHdl.GetObject(ctx, testBucket, object.Key, minio.GetObjectOptions{})
			require.NoError(t, err)

			data, err := io.ReadAll(obj)
			obj.Close()
			require.NoError(t, err)

			t.Logf("Object data size: %d bytes", len(data))

			// Print raw data for debugging
			if len(data) > 0 {
				maxLen := 50
				if len(data) < maxLen {
					maxLen = len(data)
				}
				t.Logf("Raw data (first %d bytes): %x", maxLen, data[:maxLen])
				t.Logf("Raw data as string: %q", string(data[:maxLen]))
			}

			// Try to decode records
			records, err := codec.DecodeRecordList(data)
			if err != nil {
				t.Logf("Error decoding records: %v", err)
				continue
			}
			t.Logf("Decoded %d records from object %s", len(records), object.Key)

			// Print record types
			foundBlockHeaderRecord := false
			for i, record := range records {
				t.Logf("  Record %d: type=%T", i, record)
				if blockHeaderRecord, ok := record.(*codec.BlockHeaderRecord); ok {
					foundBlockHeaderRecord = true
					t.Logf("    BlockHeaderRecord: FirstEntryID=%d, LastEntryID=%d",
						blockHeaderRecord.FirstEntryID, blockHeaderRecord.LastEntryID)
				}
			}

			assert.True(t, foundBlockHeaderRecord, "Should find BlockHeaderRecord in object %s", object.Key)
		}
	}
}

func TestMinioFileReader_ReadNextBatchModes(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(10)
	segmentId := int64(1000)
	baseDir := fmt.Sprintf("test-batch-modes-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// Create writer and write test data across multiple blocks
	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Write data that will create multiple blocks
	testData := [][]byte{
		[]byte("Block 0 - Entry 0"),
		[]byte("Block 1 - Entry 1"),
		[]byte("Block 2 - Entry 2"),
		[]byte("Block 3 - Entry 3"),
		[]byte("Block 4 - Entry 4"),
	}

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-batch-write-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		// Wait for result and sync each entry to ensure separate blocks
		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)

		err = writer.Sync(ctx)
		require.NoError(t, err)
	}

	// Finalize the segment
	lastEntryId, err := writer.Finalize(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(4), lastEntryId)

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Create reader
	reader, err := objectstorage.NewMinioFileReader(ctx, testBucket, baseDir, logId, segmentId, minioHdl)
	require.NoError(t, err)
	require.NotNil(t, reader)

	t.Run("AutoBatchMode_BatchSizeNegativeOne", func(t *testing.T) {
		// Test auto batch mode (BatchSize = -1): should return only entries from one block
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 2,  // Start from entry 2 (which should be in block 2)
			BatchSize:    -1, // Auto batch mode
		})
		require.NoError(t, err)

		// Should only return entries from the single block containing entry 2
		assert.Equal(t, 1, len(entries), "Auto batch mode should return only entries from one block")
		assert.Equal(t, int64(2), entries[0].EntryId)
		assert.Equal(t, []byte("Block 2 - Entry 2"), entries[0].Values)

		t.Logf("Auto batch mode returned %d entries starting from entry %d", len(entries), entries[0].EntryId)
	})

	t.Run("SpecifiedBatchMode_BatchSize3", func(t *testing.T) {
		// Test specified batch size mode: should return exactly 3 entries across multiple blocks
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 1, // Start from entry 1
			BatchSize:    3, // Request exactly 3 entries
		})
		require.NoError(t, err)

		// Should return exactly 3 entries (entries 1, 2, 3) across multiple blocks
		assert.Equal(t, 3, len(entries), "Specified batch mode should return exactly 3 entries")
		assert.Equal(t, int64(1), entries[0].EntryId)
		assert.Equal(t, int64(2), entries[1].EntryId)
		assert.Equal(t, int64(3), entries[2].EntryId)
		assert.Equal(t, []byte("Block 1 - Entry 1"), entries[0].Values)
		assert.Equal(t, []byte("Block 2 - Entry 2"), entries[1].Values)
		assert.Equal(t, []byte("Block 3 - Entry 3"), entries[2].Values)

		t.Logf("Specified batch mode returned %d entries from entry %d to %d",
			len(entries), entries[0].EntryId, entries[len(entries)-1].EntryId)
	})

	t.Run("SpecifiedBatchMode_BatchSize2", func(t *testing.T) {
		// Test specified batch size mode: should return exactly 2 entries
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 3, // Start from entry 3
			BatchSize:    2, // Request exactly 2 entries
		})
		require.NoError(t, err)

		// Should return exactly 2 entries (entries 3, 4)
		assert.Equal(t, 2, len(entries), "Specified batch mode should return exactly 2 entries")
		assert.Equal(t, int64(3), entries[0].EntryId)
		assert.Equal(t, int64(4), entries[1].EntryId)
		assert.Equal(t, []byte("Block 3 - Entry 3"), entries[0].Values)
		assert.Equal(t, []byte("Block 4 - Entry 4"), entries[1].Values)

		t.Logf("Specified batch mode returned %d entries from entry %d to %d",
			len(entries), entries[0].EntryId, entries[len(entries)-1].EntryId)
	})

	t.Run("AutoBatchMode_StartFromFirstEntry", func(t *testing.T) {
		// Test auto batch mode starting from first entry
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,  // Start from entry 0 (first entry)
			BatchSize:    -1, // Auto batch mode
		})
		require.NoError(t, err)

		// Should only return entries from the first block
		assert.Equal(t, 1, len(entries), "Auto batch mode should return only entries from first block")
		assert.Equal(t, int64(0), entries[0].EntryId)
		assert.Equal(t, []byte("Block 0 - Entry 0"), entries[0].Values)

		t.Logf("Auto batch mode from first entry returned %d entries", len(entries))
	})

	err = reader.Close(ctx)
	require.NoError(t, err)
}

// TestMinioFileWriter_DataIntegrityWithDifferentSizes tests data integrity with various data sizes
func TestMinioFileWriter_DataIntegrityWithDifferentSizes(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(20)
	segmentId := int64(2000)
	baseDir := fmt.Sprintf("test-data-integrity-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// Test various data sizes: single byte, small, medium, large
	// Note: We skip empty data as it's not a valid log entry
	testCases := []struct {
		name string
		data []byte
	}{
		{"SingleByte", []byte("a")},
		{"Small", []byte("Hello, World!")},
		{"Medium", generateTestData(1024)},            // 1KB
		{"Large", generateTestData(64 * 1024)},        // 64KB
		{"ExtraLarge", generateTestData(1024 * 1024)}, // 1MB
	}

	// Write data
	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			entryId := int64(i)
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-integrity-%d", entryId))

			returnedId, err := writer.WriteDataAsync(ctx, entryId, tc.data, resultCh)
			require.NoError(t, err)
			assert.Equal(t, entryId, returnedId)

			// Wait for result
			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
			assert.Equal(t, entryId, result.SyncedId)
		})
	}

	// Finalize segment
	lastEntryId, err := writer.Finalize(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testCases)-1), lastEntryId)

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Read back and verify data integrity
	reader, err := objectstorage.NewMinioFileReader(ctx, testBucket, baseDir, logId, segmentId, minioHdl)
	require.NoError(t, err)

	// Read all entries
	entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
		StartEntryID: 0,
		BatchSize:    int64(len(testCases)),
	})
	require.NoError(t, err)
	assert.Equal(t, len(testCases), len(entries))

	// Verify each entry
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Verify_%s", tc.name), func(t *testing.T) {
			assert.Equal(t, int64(i), entries[i].EntryId)
			assert.Equal(t, tc.data, entries[i].Values, "Data mismatch for %s", tc.name)
		})
	}

	err = reader.Close(ctx)
	require.NoError(t, err)
}

// TestMinioFileReader_SequentialReading tests sequential reading patterns
func TestMinioFileReader_SequentialReading(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(21)
	segmentId := int64(2100)
	baseDir := fmt.Sprintf("test-sequential-reading-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// Create writer and write test data
	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)

	// Write 20 entries to create multiple blocks
	totalEntries := 20
	testData := make([][]byte, totalEntries)
	for i := 0; i < totalEntries; i++ {
		testData[i] = []byte(fmt.Sprintf("Entry %d: %s", i, generateTestData(512)))

		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-seq-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), testData[i], resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)

		// Force sync every 5 entries to create separate blocks
		if (i+1)%5 == 0 {
			err = writer.Sync(ctx)
			require.NoError(t, err)
		}
	}

	// Finalize
	_, err = writer.Finalize(ctx)
	require.NoError(t, err)
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Create reader
	reader, err := objectstorage.NewMinioFileReader(ctx, testBucket, baseDir, logId, segmentId, minioHdl)
	require.NoError(t, err)

	t.Run("ReadFromBeginning", func(t *testing.T) {
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    int64(totalEntries),
		})
		require.NoError(t, err)
		assert.Equal(t, totalEntries, len(entries))

		// Verify order and content
		for i, entry := range entries {
			assert.Equal(t, int64(i), entry.EntryId)
			assert.Equal(t, testData[i], entry.Values)
		}
	})

	t.Run("ReadFromMiddle", func(t *testing.T) {
		startId := int64(10)
		expectedCount := totalEntries - int(startId)

		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: startId,
			BatchSize:    int64(expectedCount),
		})
		require.NoError(t, err)
		assert.Equal(t, expectedCount, len(entries))

		// Verify order and content
		for i, entry := range entries {
			expectedId := startId + int64(i)
			assert.Equal(t, expectedId, entry.EntryId)
			assert.Equal(t, testData[expectedId], entry.Values)
		}
	})

	t.Run("ReadWithSmallBatchSize", func(t *testing.T) {
		batchSize := int64(3)
		startId := int64(5)

		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: startId,
			BatchSize:    batchSize,
		})
		require.NoError(t, err)
		assert.Equal(t, int(batchSize), len(entries))

		// Verify content
		for i, entry := range entries {
			expectedId := startId + int64(i)
			assert.Equal(t, expectedId, entry.EntryId)
			assert.Equal(t, testData[expectedId], entry.Values)
		}
	})

	t.Run("ReadAutoBatchFromDifferentBlocks", func(t *testing.T) {
		// Test auto batch mode from different starting points
		testCases := []int64{0, 5, 10, 15, 19}

		for _, startId := range testCases {
			entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
				StartEntryID: startId,
				BatchSize:    -1, // Auto batch mode
			})
			require.NoError(t, err)
			assert.Greater(t, len(entries), 0)

			// Should return at least one entry
			assert.Equal(t, startId, entries[0].EntryId)
			assert.Equal(t, testData[startId], entries[0].Values)
		}
	})

	err = reader.Close(ctx)
	require.NoError(t, err)
}

// TestMinioFileWriter_ConcurrentReadWrite tests concurrent read and write operations
func TestMinioFileWriter_ConcurrentReadWrite(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(22)
	segmentId := int64(2200)
	baseDir := fmt.Sprintf("test-concurrent-rw-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// Create writer
	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)

	// Write initial data
	initialEntries := 10
	for i := 0; i < initialEntries; i++ {
		data := []byte(fmt.Sprintf("Initial entry %d", i))
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-concurrent-initial-%d", i))

		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	// Force sync to ensure data is written
	err = writer.Sync(ctx)
	require.NoError(t, err)

	// Test concurrent operations
	var wg sync.WaitGroup
	errors := make(chan error, 10)

	// Goroutine 1: Continue writing
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := initialEntries; i < initialEntries+10; i++ {
			data := []byte(fmt.Sprintf("Additional entry %d", i))
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-concurrent-additional-%d", i))

			_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
			if err != nil {
				errors <- err
				return
			}

			result, err := resultCh.ReadResult(ctx)
			if err != nil {
				errors <- err
				return
			}
			if result.Err != nil {
				errors <- result.Err
				return
			}
		}
	}()

	// Goroutine 2: Read existing data (should work while writing continues)
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Wait a bit for some writes to complete
		time.Sleep(100 * time.Millisecond)

		// Try to read existing data
		reader, err := objectstorage.NewMinioFileReader(ctx, testBucket, baseDir, logId, segmentId, minioHdl)
		if err != nil {
			errors <- err
			return
		}
		defer reader.Close(ctx)

		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    int64(initialEntries),
		})
		if err != nil {
			errors <- err
			return
		}

		if len(entries) != initialEntries {
			errors <- fmt.Errorf("expected %d entries, got %d", initialEntries, len(entries))
			return
		}

		// Verify initial data
		for i, entry := range entries {
			expected := fmt.Sprintf("Initial entry %d", i)
			if string(entry.Values) != expected {
				errors <- fmt.Errorf("data mismatch at entry %d: expected %s, got %s", i, expected, string(entry.Values))
				return
			}
		}
	}()

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent operation error: %v", err)
	}

	// Finalize and verify all data
	_, err = writer.Finalize(ctx)
	require.NoError(t, err)
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Final verification
	reader, err := objectstorage.NewMinioFileReader(ctx, testBucket, baseDir, logId, segmentId, minioHdl)
	require.NoError(t, err)

	entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
		StartEntryID: 0,
		BatchSize:    int64(initialEntries + 10),
	})
	require.NoError(t, err)
	assert.Equal(t, initialEntries+10, len(entries))

	err = reader.Close(ctx)
	require.NoError(t, err)
}

// TestMinioFileReader_ErrorHandling tests error handling in reader
func TestMinioFileReader_ErrorHandling(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, _ := setupMinioFileWriterTest(t)
	ctx := context.Background()

	t.Run("NonExistentSegment", func(t *testing.T) {
		nonExistentBaseDir := fmt.Sprintf("non-existent-segment-%d", time.Now().Unix())

		reader, err := objectstorage.NewMinioFileReader(ctx, testBucket, nonExistentBaseDir, 1000, 2000, minioHdl)
		require.NoError(t, err) // Should succeed in creating reader

		// But reading should fail gracefully
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    10,
		})
		assert.Error(t, err)
		assert.Nil(t, entries)

		err = reader.Close(ctx)
		require.NoError(t, err)
	})

	t.Run("InvalidEntryId", func(t *testing.T) {
		// Create a valid segment first
		logId := int64(23)
		segmentId := int64(2300)
		baseDir := fmt.Sprintf("test-invalid-entry-%d", time.Now().Unix())
		defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

		// Create and write some data
		cfg, _ := config.NewConfiguration("../../config/woodpecker.yaml")
		writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)

		// Write a few entries
		for i := 0; i < 5; i++ {
			data := []byte(fmt.Sprintf("Entry %d", i))
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-invalid-%d", i))

			_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
			require.NoError(t, err)

			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
		}

		_, err = writer.Finalize(ctx)
		require.NoError(t, err)
		err = writer.Close(ctx)
		require.NoError(t, err)

		// Create reader and test invalid entry IDs
		reader, err := objectstorage.NewMinioFileReader(ctx, testBucket, baseDir, logId, segmentId, minioHdl)
		require.NoError(t, err)

		// Test reading from non-existent entry ID
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 100, // Way beyond available entries
			BatchSize:    10,
		})
		assert.Error(t, err)
		assert.Nil(t, entries)

		// Test reading from negative entry ID
		entries, err = reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: -1,
			BatchSize:    10,
		})
		assert.Error(t, err)
		assert.Nil(t, entries)

		err = reader.Close(ctx)
		require.NoError(t, err)
	})
}

// TestMinioFileWriter_LargeEntryHandling tests handling of very large entries
func TestMinioFileWriter_LargeEntryHandling(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	// Increase buffer sizes for large entries
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes = 10 * 1024 * 1024    // 10MB
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 5 * 1024 * 1024 // 5MB

	logId := int64(24)
	segmentId := int64(2400)
	baseDir := fmt.Sprintf("test-large-entry-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)

	// Test different large entry sizes
	testCases := []struct {
		name string
		size int
		data []byte // Store the generated data for later verification
	}{
		{"1MB", 1024 * 1024, nil},
		{"2MB", 2 * 1024 * 1024, nil},
		{"4MB", 4 * 1024 * 1024, nil},
	}

	// Generate test data once and store it
	for i := range testCases {
		testCases[i].data = generateTestData(testCases[i].size)
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			entryId := int64(i)

			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-large-%d", entryId))

			start := time.Now()
			returnedId, err := writer.WriteDataAsync(ctx, entryId, tc.data, resultCh)
			require.NoError(t, err)
			assert.Equal(t, entryId, returnedId)

			// Wait for result with longer timeout for large entries
			ctxWithTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
			result, err := resultCh.ReadResult(ctxWithTimeout)
			cancel()
			require.NoError(t, err)
			require.NoError(t, result.Err)
			assert.Equal(t, entryId, result.SyncedId)

			duration := time.Since(start)
			t.Logf("Large entry %s (%d bytes) took %v to write", tc.name, tc.size, duration)
		})
	}

	// Finalize
	_, err = writer.Finalize(ctx)
	require.NoError(t, err)
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Verify reading large entries
	reader, err := objectstorage.NewMinioFileReader(ctx, testBucket, baseDir, logId, segmentId, minioHdl)
	require.NoError(t, err)

	entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
		StartEntryID: 0,
		BatchSize:    int64(len(testCases)),
	})
	require.NoError(t, err)
	assert.Equal(t, len(testCases), len(entries))

	// Verify each large entry
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Verify_%s", tc.name), func(t *testing.T) {
			assert.Equal(t, int64(i), entries[i].EntryId)
			assert.Equal(t, tc.size, len(entries[i].Values))

			// Verify data integrity for first and last few bytes using the stored data
			assert.Equal(t, tc.data[:100], entries[i].Values[:100], "First 100 bytes mismatch")
			assert.Equal(t, tc.data[len(tc.data)-100:], entries[i].Values[len(entries[i].Values)-100:], "Last 100 bytes mismatch")
		})
	}

	err = reader.Close(ctx)
	require.NoError(t, err)
}

// TestMinioFileWriter_MetadataConsistency tests metadata consistency across operations
func TestMinioFileWriter_MetadataConsistency(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(25)
	segmentId := int64(2500)
	baseDir := fmt.Sprintf("test-metadata-consistency-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)

	// Write entries with metadata tracking
	totalEntries := 15
	expectedTotalRecords := uint32(totalEntries)

	for i := 0; i < totalEntries; i++ {
		data := []byte(fmt.Sprintf("Metadata test entry %d with some data", i))
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-metadata-%d", i))

		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)

		// Verify writer state during writing
		assert.Equal(t, int64(0), writer.GetFirstEntryId(ctx))
		assert.Equal(t, int64(i), writer.GetLastEntryId(ctx))
	}

	// Finalize
	lastEntryId, err := writer.Finalize(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(totalEntries-1), lastEntryId)

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Verify metadata consistency in reader
	reader, err := objectstorage.NewMinioFileReader(ctx, testBucket, baseDir, logId, segmentId, minioHdl)
	require.NoError(t, err)

	// Check footer metadata
	footer := reader.GetFooter()
	require.NotNil(t, footer)
	assert.Greater(t, footer.TotalBlocks, int32(0))
	assert.Equal(t, expectedTotalRecords, footer.TotalRecords)
	assert.Equal(t, codec.FormatVersion, int(footer.Version))

	// Check block indexes
	blockIndexes := reader.GetBlockIndexes()
	assert.Greater(t, len(blockIndexes), 0)

	// Verify block index consistency
	var totalEntriesFromBlocks int64
	for i, blockIndex := range blockIndexes {
		assert.Equal(t, int32(i), blockIndex.BlockNumber)
		assert.GreaterOrEqual(t, blockIndex.LastEntryID, blockIndex.FirstEntryID)
		totalEntriesFromBlocks += blockIndex.LastEntryID - blockIndex.FirstEntryID + 1
	}
	assert.Equal(t, int64(totalEntries), totalEntriesFromBlocks)

	// Check reader methods
	readerLastEntryId, err := reader.GetLastEntryID(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(totalEntries-1), readerLastEntryId)

	assert.Equal(t, expectedTotalRecords, reader.GetTotalRecords())
	assert.Equal(t, footer.TotalBlocks, reader.GetTotalBlocks())

	err = reader.Close(ctx)
	require.NoError(t, err)
}

// BenchmarkMinioFileWriter_ThroughputTest benchmarks write throughput
func BenchmarkMinioFileWriter_ThroughputTest(b *testing.B) {
	minioHdl, cfg := setupMinioFileWriterTest(&testing.T{})
	ctx := context.Background()

	baseDir := fmt.Sprintf("bench-throughput-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(&testing.T{}, minioHdl, baseDir)

	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, 1, 1, minioHdl, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer writer.Close(ctx)

	data := generateTestData(1024) // 1KB per entry
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("bench-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		if err != nil {
			b.Fatal(err)
		}

		result, err := resultCh.ReadResult(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if result.Err != nil {
			b.Fatal(result.Err)
		}
	}
}

// BenchmarkMinioFileReader_ThroughputTest benchmarks read throughput
func BenchmarkMinioFileReader_ThroughputTest(b *testing.B) {
	minioHdl, cfg := setupMinioFileWriterTest(&testing.T{})
	ctx := context.Background()

	baseDir := fmt.Sprintf("bench-read-throughput-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(&testing.T{}, minioHdl, baseDir)

	// Prepare test data
	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, 1, 1, minioHdl, cfg)
	if err != nil {
		b.Fatal(err)
	}

	// Write 1000 entries
	data := generateTestData(1024)
	for i := 0; i < 1000; i++ {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("bench-prep-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		if err != nil {
			b.Fatal(err)
		}
		result, err := resultCh.ReadResult(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if result.Err != nil {
			b.Fatal(result.Err)
		}
	}

	_, err = writer.Finalize(ctx)
	if err != nil {
		b.Fatal(err)
	}
	err = writer.Close(ctx)
	if err != nil {
		b.Fatal(err)
	}

	// Benchmark reading
	reader, err := objectstorage.NewMinioFileReader(ctx, testBucket, baseDir, 1, 1, minioHdl)
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		startId := int64(i % 900) // Ensure we don't go beyond available entries
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: startId,
			BatchSize:    10,
		})
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) == 0 {
			b.Fatal("No entries returned")
		}
	}
}

// TestEmptyPayloadValidation tests that empty payloads are rejected at the client level
func TestEmptyPayloadValidation(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(30)
	segmentId := int64(3000)
	baseDir := fmt.Sprintf("test-empty-payload-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// Create writer
	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Test empty payload validation at the storage layer
	t.Run("EmptyPayloadAtStorageLayer", func(t *testing.T) {
		// Try to write empty data directly to storage layer
		// This should now be rejected immediately by WriteDataAsync
		_, err := writer.WriteDataAsync(ctx, 0, []byte{}, channel.NewLocalResultChannel("test-empty-payload"))
		require.Error(t, err)
		assert.True(t, werr.ErrEmptyPayload.Is(err), "Error should be ErrEmptyPayload")

		t.Logf("Storage layer empty payload error: %v", err)
	})

	// Test empty payload validation at the client level (LogWriter)
	t.Run("EmptyPayloadAtClientLevel", func(t *testing.T) {
		// This test requires creating a LogWriter, which needs more setup
		// For now, we'll test the MarshalMessage function directly

		emptyMsg := &log.WriterMessage{
			Payload:    []byte{},
			Properties: map[string]string{"test": "value"},
		}

		_, err := log.MarshalMessage(emptyMsg)
		require.Error(t, err)
		assert.True(t, werr.ErrEmptyPayload.Is(err), "Error should be ErrEmptyPayload")

		t.Logf("Client level empty payload error: %v", err)
	})

	// Test nil payload validation
	t.Run("NilPayloadAtClientLevel", func(t *testing.T) {
		nilMsg := &log.WriterMessage{
			Payload:    nil,
			Properties: map[string]string{"test": "value"},
		}

		_, err := log.MarshalMessage(nilMsg)
		require.Error(t, err)
		assert.True(t, werr.ErrEmptyPayload.Is(err), "Error should be ErrEmptyPayload")

		t.Logf("Client level nil payload error: %v", err)
	})

	// Test valid payload for comparison
	t.Run("ValidPayload", func(t *testing.T) {
		validMsg := &log.WriterMessage{
			Payload:    []byte("valid data"),
			Properties: map[string]string{"test": "value"},
		}

		data, err := log.MarshalMessage(validMsg)
		require.NoError(t, err)
		assert.Greater(t, len(data), 0)

		t.Logf("Valid payload marshaled successfully, size: %d bytes", len(data))
	})

	err = writer.Close(ctx)
	require.NoError(t, err)
}

// TestMinioFileWriter_HeaderRecordVerification tests that the first record is always HeaderRecord
func TestMinioFileWriter_HeaderRecordVerification(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(30)
	segmentId := int64(3000)
	baseDir := fmt.Sprintf("test-header-record-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// Create writer
	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Write some test data
	testData := [][]byte{
		[]byte("First entry data"),
		[]byte("Second entry data"),
		[]byte("Third entry data"),
	}

	for i, data := range testData {
		entryId := int64(i)
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-header-%d", entryId))
		_, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)
		require.NoError(t, err)

		// Wait for result
		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	// Force sync to ensure data is written
	err = writer.Sync(ctx)
	require.NoError(t, err)

	// Close writer
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Now read the first object and verify it starts with HeaderRecord
	objectCh := minioHdl.ListObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, minio.ListObjectsOptions{})

	var firstDataObject string
	for object := range objectCh {
		require.NoError(t, object.Err)
		if strings.HasSuffix(object.Key, ".blk") && !strings.HasSuffix(object.Key, "footer.blk") {
			// This should be the first data object (0.blk)
			if strings.HasSuffix(object.Key, "/0.blk") {
				firstDataObject = object.Key
				break
			}
		}
	}

	require.NotEmpty(t, firstDataObject, "Should find the first data object (0.blk)")
	t.Logf("Found first data object: %s", firstDataObject)

	// Read the first object content
	obj, err := minioHdl.GetObject(ctx, testBucket, firstDataObject, minio.GetObjectOptions{})
	require.NoError(t, err)

	// Get object size
	objInfo, err := minioHdl.StatObject(ctx, testBucket, firstDataObject, minio.StatObjectOptions{})
	require.NoError(t, err)

	data, err := minioHandler.ReadObjectFull(ctx, obj, objInfo.Size)
	require.NoError(t, err)
	obj.Close()

	t.Logf("First object data size: %d bytes", len(data))

	// Decode all records from the first object
	records, err := codec.DecodeRecordList(data)
	require.NoError(t, err)
	require.Greater(t, len(records), 0, "Should have at least one record")

	t.Logf("Found %d records in first object", len(records))

	// Print all record types for debugging
	for i, record := range records {
		t.Logf("Record %d: type=%T, recordType=%d", i, record, record.Type())
	}

	// The first record should be HeaderRecord
	firstRecord := records[0]
	assert.Equal(t, codec.HeaderRecordType, firstRecord.Type(),
		"First record should be HeaderRecord (type %d), but got type %d",
		codec.HeaderRecordType, firstRecord.Type())

	if firstRecord.Type() == codec.HeaderRecordType {
		headerRecord := firstRecord.(*codec.HeaderRecord)
		t.Logf("HeaderRecord found: Version=%d, Flags=%d, FirstEntryID=%d",
			headerRecord.Version, headerRecord.Flags, headerRecord.FirstEntryID)

		// Verify header content
		assert.Equal(t, uint16(codec.FormatVersion), headerRecord.Version)
		assert.Equal(t, int64(0), headerRecord.FirstEntryID, "First entry ID should be 0")
	}
}

func TestMinioFileWriter_RecoveryDebug(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(50)       // 修改为不同的 logId 避免冲突
	segmentId := int64(5000) // 修改为不同的 segmentId 避免冲突
	baseDir := fmt.Sprintf("test-recovery-debug-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	t.Run("WriteDataAndInterruptOnly", func(t *testing.T) {
		// Create first writer and write some data
		writer1, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		require.NotNil(t, writer1)

		// Write test data
		testData := [][]byte{
			[]byte("Entry 0: Initial data"),
			[]byte("Entry 1: More data"),
			generateTestData(1024), // 1KB
			[]byte("Entry 3: Final entry before interruption"),
		}

		for i, data := range testData {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-recovery-write-%d", i))
			_, err := writer1.WriteDataAsync(ctx, int64(i), data, resultCh)
			require.NoError(t, err)

			// Wait for result
			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
		}

		// Force sync to ensure data is written to object storage
		err = writer1.Sync(ctx)
		require.NoError(t, err)

		// Wait a bit for async operations to complete
		time.Sleep(100 * time.Millisecond)

		// Verify initial state
		assert.Equal(t, int64(0), writer1.GetFirstEntryId(ctx))
		assert.Equal(t, int64(3), writer1.GetLastEntryId(ctx))

		// List objects BEFORE close
		t.Log("Objects BEFORE close:")
		objectCh := minioHdl.ListObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, minio.ListObjectsOptions{})
		for object := range objectCh {
			require.NoError(t, object.Err)
			t.Logf("  Before close: %s (size: %d)", object.Key, object.Size)
		}

		// Simulate interruption by closing the writer without finalize
		// Note: Close may trigger Complete() which calls Finalize(), but this simulates a real-world scenario
		err = writer1.Close(ctx)
		if err != nil {
			t.Logf("Expected error during close (simulating interruption): %v", err)
		}

		// Wait a bit more to ensure all async operations complete
		time.Sleep(500 * time.Millisecond)

		// List objects AFTER close
		t.Log("Objects AFTER close:")
		objectCh = minioHdl.ListObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, minio.ListObjectsOptions{})
		objectCount := 0
		for object := range objectCh {
			require.NoError(t, object.Err)
			if !strings.HasSuffix(object.Key, ".lock") {
				objectCount++
				t.Logf("  After close: %s (size: %d)", object.Key, object.Size)
			}
		}
		assert.Greater(t, objectCount, 0, "Should have created at least one data object")
	})
}

// TestMinioFileRW_ConcurrentReadWrite tests concurrent reading and writing to verify real-time read capability in MinIO
func TestMinioFileRW_ConcurrentReadWrite(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(70)
	segmentId := int64(7000)
	baseDir := fmt.Sprintf("test-concurrent-rw-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// Create writer
	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Test parameters
	totalEntries := 15
	writeInterval := 200 * time.Millisecond // Wait 200ms between writes
	readInterval := 100 * time.Millisecond  // Check for new data every 100ms

	// Channels for coordination
	writerDone := make(chan struct{})
	readerDone := make(chan struct{})
	writeProgress := make(chan int64, totalEntries) // Track write progress

	// Start writer goroutine
	go func() {
		defer close(writerDone)
		defer close(writeProgress)

		for i := 0; i < totalEntries; i++ {
			entryId := int64(i)
			data := []byte(fmt.Sprintf("Entry %d: MinIO concurrent data written at %s",
				i, time.Now().Format("15:04:05.000")))

			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-minio-concurrent-write-%d", entryId))
			_, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)
			if err != nil {
				t.Errorf("Writer: Failed to write entry %d: %v", entryId, err)
				return
			}

			// Wait for write to complete
			result, err := resultCh.ReadResult(ctx)
			if err != nil {
				t.Errorf("Writer: Failed to get write result for entry %d: %v", entryId, err)
				return
			}
			if result.Err != nil {
				t.Errorf("Writer: Write failed for entry %d: %v", entryId, result.Err)
				return
			}

			// Force sync to ensure data is written to MinIO
			err = writer.Sync(ctx)
			if err != nil {
				t.Errorf("Writer: Failed to sync after entry %d: %v", entryId, err)
				return
			}

			// Notify reader about new data
			writeProgress <- entryId

			t.Logf("Writer: Successfully wrote entry %d", entryId)

			// Wait before writing next entry (except for the last one)
			if i < totalEntries-1 {
				time.Sleep(writeInterval)
			}
		}

		t.Logf("Writer: Finished writing all %d entries", totalEntries)
	}()

	// Start reader goroutine
	go func() {
		defer close(readerDone)

		// Wait a bit for writer to create the first block
		time.Sleep(writeInterval + 100*time.Millisecond)

		// Create reader for the incomplete file
		reader, err := objectstorage.NewMinioFileReader(ctx, testBucket, baseDir, logId, segmentId, minioHdl)
		if err != nil {
			t.Errorf("Reader: Failed to create reader: %v", err)
			return
		}
		defer reader.Close(ctx)

		var lastReadEntryId int64 = -1
		readEntries := make(map[int64][]byte) // Track read entries

		// Keep reading until we've read all expected entries
		for {
			select {
			case <-ctx.Done():
				t.Logf("Reader: Context cancelled")
				return
			case latestWrittenId, ok := <-writeProgress:
				if !ok {
					// Writer is done, do final read
					t.Logf("Reader: Writer finished, doing final read")
					goto finalRead
				}

				// Try to read up to the latest written entry
				if latestWrittenId > lastReadEntryId {
					startId := lastReadEntryId + 1
					batchSize := latestWrittenId - lastReadEntryId

					t.Logf("Reader: Attempting to read entries %d to %d", startId, latestWrittenId)

					entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
						StartEntryID: startId,
						BatchSize:    batchSize,
					})

					if err != nil {
						t.Logf("Reader: Failed to read entries %d to %d: %v", startId, latestWrittenId, err)
						// Continue trying - the data might not be fully written yet
						time.Sleep(readInterval)
						continue
					}

					// Process read entries
					for _, entry := range entries {
						readEntries[entry.EntryId] = entry.Values
						if entry.EntryId > lastReadEntryId {
							lastReadEntryId = entry.EntryId
						}
						t.Logf("Reader: Successfully read entry %d: %s",
							entry.EntryId, string(entry.Values))
					}

					t.Logf("Reader: Read %d entries, lastReadEntryId now %d",
						len(entries), lastReadEntryId)
				}

				// Check if we've read all expected entries
				if lastReadEntryId >= int64(totalEntries-1) {
					t.Logf("Reader: Read all expected entries, finishing")
					return
				}

			default:
				// No new write notification, wait a bit
				time.Sleep(readInterval)
			}
		}

	finalRead:
		// Final verification read after writer is done
		t.Logf("Reader: Doing final verification read")

		// Wait a bit for any pending writes to complete
		time.Sleep(300 * time.Millisecond)

		// Try to read any remaining entries
		if lastReadEntryId < int64(totalEntries-1) {
			startId := lastReadEntryId + 1
			remainingEntries := int64(totalEntries) - startId

			finalEntries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
				StartEntryID: startId,
				BatchSize:    remainingEntries,
			})

			if err != nil {
				t.Errorf("Reader: Failed final read: %v", err)
			} else {
				t.Logf("Reader: Final read got %d entries", len(finalEntries))

				for _, entry := range finalEntries {
					readEntries[entry.EntryId] = entry.Values
					if entry.EntryId > lastReadEntryId {
						lastReadEntryId = entry.EntryId
					}
				}
			}
		}

		// Verify we read all expected entries
		for i := 0; i < totalEntries; i++ {
			if _, exists := readEntries[int64(i)]; !exists {
				t.Errorf("Reader: Missing entry %d", i)
			}
		}

		t.Logf("Reader: Successfully verified all %d entries", len(readEntries))
	}()

	// Wait for both goroutines to complete with timeout
	timeout := time.Duration(totalEntries)*writeInterval + 15*time.Second

	select {
	case <-writerDone:
		t.Logf("Writer completed successfully")
	case <-time.After(timeout):
		t.Fatalf("Writer timed out after %v", timeout)
	}

	select {
	case <-readerDone:
		t.Logf("Reader completed successfully")
	case <-time.After(10 * time.Second):
		t.Fatalf("Reader timed out after writer completion")
	}

	// Close writer without finalizing to keep it as incomplete file
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Final verification: Create a new reader and verify data is accessible
	t.Run("FinalVerification", func(t *testing.T) {
		finalReader, err := objectstorage.NewMinioFileReader(ctx, testBucket, baseDir, logId, segmentId, minioHdl)
		require.NoError(t, err)
		defer finalReader.Close(ctx)

		// Verify file is incomplete (no footer)
		footer := finalReader.GetFooter()
		assert.Nil(t, footer, "File should be incomplete (no footer)")

		// Verify we can read available entries (may be less than total due to MinIO block boundaries)
		lastEntryId, err := finalReader.GetLastEntryID(ctx)
		if err == nil && lastEntryId >= 0 {
			// Try to read from the beginning
			allEntries, err := finalReader.ReadNextBatch(ctx, storage.ReaderOpt{
				StartEntryID: 0,
				BatchSize:    lastEntryId + 1,
			})
			if err == nil {
				assert.Greater(t, len(allEntries), 0, "Should read at least some entries")

				// Verify entry content and order
				for i, entry := range allEntries {
					assert.Equal(t, int64(i), entry.EntryId, "Entry ID should match")
					assert.Contains(t, string(entry.Values), fmt.Sprintf("Entry %d:", i),
						"Entry content should contain expected prefix")
				}

				t.Logf("Final verification: Read %d entries successfully", len(allEntries))
			}
		}
	})
}

// TestMinioFileRW_ConcurrentOneWriteMultipleReads tests one writer with multiple concurrent readers for MinIO
func TestMinioFileRW_ConcurrentOneWriteMultipleReads(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(80)
	segmentId := int64(8000)
	baseDir := fmt.Sprintf("test-multi-reader-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// Create writer
	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Test parameters
	totalEntries := 20
	numReaders := 3                         // Number of concurrent readers
	writeInterval := 250 * time.Millisecond // Wait 250ms between writes
	readInterval := 150 * time.Millisecond  // Check for new data every 150ms

	// Channels for coordination
	writerDone := make(chan struct{})
	readersDone := make(chan int, numReaders)                // Track which reader finished
	writeProgress := make(chan int64, totalEntries)          // Track write progress
	readerResults := make(chan map[int64][]byte, numReaders) // Collect results from each reader

	// Start writer goroutine
	go func() {
		defer close(writerDone)
		defer close(writeProgress)

		for i := 0; i < totalEntries; i++ {
			entryId := int64(i)
			data := []byte(fmt.Sprintf("Entry %d: MinIO multi-reader data written at %s",
				i, time.Now().Format("15:04:05.000")))

			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-minio-multi-reader-write-%d", entryId))
			_, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)
			if err != nil {
				t.Errorf("Writer: Failed to write entry %d: %v", entryId, err)
				return
			}

			// Wait for write to complete
			result, err := resultCh.ReadResult(ctx)
			if err != nil {
				t.Errorf("Writer: Failed to get write result for entry %d: %v", entryId, err)
				return
			}
			if result.Err != nil {
				t.Errorf("Writer: Write failed for entry %d: %v", entryId, result.Err)
				return
			}

			// Force sync to ensure data is written to MinIO
			err = writer.Sync(ctx)
			if err != nil {
				t.Errorf("Writer: Failed to sync after entry %d: %v", entryId, err)
				return
			}

			// Notify all readers about new data
			writeProgress <- entryId

			t.Logf("Writer: Successfully wrote entry %d", entryId)

			// Wait before writing next entry (except for the last one)
			if i < totalEntries-1 {
				time.Sleep(writeInterval)
			}
		}

		t.Logf("Writer: Finished writing all %d entries", totalEntries)
	}()

	// Start multiple reader goroutines
	for readerId := 0; readerId < numReaders; readerId++ {
		go func(readerNum int) {
			defer func() {
				readersDone <- readerNum
			}()

			t.Logf("Reader %d: Starting", readerNum)

			// Wait a bit for writer to create the first block
			time.Sleep(writeInterval + time.Duration(readerNum*50)*time.Millisecond)

			// Create reader for the incomplete file
			reader, err := objectstorage.NewMinioFileReader(ctx, testBucket, baseDir, logId, segmentId, minioHdl)
			if err != nil {
				t.Errorf("Reader %d: Failed to create reader: %v", readerNum, err)
				return
			}
			defer reader.Close(ctx)

			var lastReadEntryId int64 = -1
			readEntries := make(map[int64][]byte) // Track read entries

			// Each reader has different reading patterns
			var readStrategy string
			var batchSize int64
			switch readerNum {
			case 0:
				readStrategy = "single-entry"
				batchSize = 1 // Read one entry at a time
			case 1:
				readStrategy = "small-batch"
				batchSize = 2 // Read 2 entries at a time
			case 2:
				readStrategy = "large-batch"
				batchSize = 4 // Read 4 entries at a time
			}

			t.Logf("Reader %d: Using strategy '%s' with batch size %d", readerNum, readStrategy, batchSize)

			// Keep reading until we've read all expected entries
			for {
				select {
				case <-ctx.Done():
					t.Logf("Reader %d: Context cancelled", readerNum)
					return
				case latestWrittenId, ok := <-writeProgress:
					if !ok {
						// Writer is done, do final read
						t.Logf("Reader %d: Writer finished, doing final read", readerNum)
						goto finalRead
					}

					// Try to read up to the latest written entry based on strategy
					if latestWrittenId > lastReadEntryId {
						startId := lastReadEntryId + 1

						// Calculate how many entries to read based on strategy
						var entriesToRead int64
						switch readStrategy {
						case "single-entry":
							entriesToRead = 1 // Always read just one
						case "small-batch":
							entriesToRead = min(batchSize, latestWrittenId-lastReadEntryId)
						case "large-batch":
							entriesToRead = min(batchSize, latestWrittenId-lastReadEntryId)
						}

						t.Logf("Reader %d: Attempting to read %d entries starting from %d (latest written: %d)",
							readerNum, entriesToRead, startId, latestWrittenId)

						entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
							StartEntryID: startId,
							BatchSize:    entriesToRead,
						})

						if err != nil {
							t.Logf("Reader %d: Failed to read entries %d to %d: %v",
								readerNum, startId, startId+entriesToRead-1, err)
							// Continue trying - the data might not be fully written yet
							time.Sleep(readInterval)
							continue
						}

						// Process read entries
						for _, entry := range entries {
							readEntries[entry.EntryId] = entry.Values
							if entry.EntryId > lastReadEntryId {
								lastReadEntryId = entry.EntryId
							}
							t.Logf("Reader %d: Successfully read entry %d: %s",
								readerNum, entry.EntryId, string(entry.Values))
						}

						t.Logf("Reader %d: Read %d entries, lastReadEntryId now %d",
							readerNum, len(entries), lastReadEntryId)
					}

					// Check if we've read all expected entries
					if lastReadEntryId >= int64(totalEntries-1) {
						t.Logf("Reader %d: Read all expected entries, finishing", readerNum)
						readerResults <- readEntries
						return
					}

				default:
					// No new write notification, wait a bit
					time.Sleep(readInterval)
				}
			}

		finalRead:
			// Final verification read after writer is done
			t.Logf("Reader %d: Doing final verification read", readerNum)

			// Wait a bit for any pending writes to complete
			time.Sleep(400 * time.Millisecond)

			// Try to read any remaining entries
			if lastReadEntryId < int64(totalEntries-1) {
				startId := lastReadEntryId + 1
				remainingEntries := int64(totalEntries) - startId

				finalEntries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
					StartEntryID: startId,
					BatchSize:    remainingEntries,
				})

				if err != nil {
					t.Errorf("Reader %d: Failed final read: %v", readerNum, err)
				} else {
					t.Logf("Reader %d: Final read got %d entries", readerNum, len(finalEntries))

					for _, entry := range finalEntries {
						readEntries[entry.EntryId] = entry.Values
						if entry.EntryId > lastReadEntryId {
							lastReadEntryId = entry.EntryId
						}
					}
				}
			}

			// Verify we read all expected entries that are available
			for i := 0; i <= int(lastReadEntryId); i++ {
				if _, exists := readEntries[int64(i)]; !exists {
					t.Errorf("Reader %d: Missing entry %d", readerNum, i)
				}
			}

			t.Logf("Reader %d: Successfully verified %d entries", readerNum, len(readEntries))
			readerResults <- readEntries
		}(readerId)
	}

	// Wait for writer to complete with timeout
	timeout := time.Duration(totalEntries)*writeInterval + 15*time.Second

	select {
	case <-writerDone:
		t.Logf("Writer completed successfully")
	case <-time.After(timeout):
		t.Fatalf("Writer timed out after %v", timeout)
	}

	// Wait for all readers to complete
	completedReaders := 0
	readerTimeout := 15 * time.Second

	for completedReaders < numReaders {
		select {
		case readerNum := <-readersDone:
			t.Logf("Reader %d completed successfully", readerNum)
			completedReaders++
		case <-time.After(readerTimeout):
			t.Fatalf("Readers timed out after writer completion, only %d/%d completed", completedReaders, numReaders)
		}
	}

	// Collect and verify results from all readers
	allReaderResults := make([]map[int64][]byte, 0, numReaders)
	for i := 0; i < numReaders; i++ {
		select {
		case result := <-readerResults:
			allReaderResults = append(allReaderResults, result)
		case <-time.After(2 * time.Second):
			t.Errorf("Failed to collect results from reader %d", i)
		}
	}

	// Close writer without finalizing to keep it as incomplete file
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Verify all readers got consistent data (allowing for MinIO block boundaries)
	t.Run("VerifyReaderConsistency", func(t *testing.T) {
		require.Equal(t, numReaders, len(allReaderResults), "Should have results from all readers")

		// Find the minimum number of entries read by any reader
		minEntries := int(^uint(0) >> 1) // Max int
		for readerIdx, readerData := range allReaderResults {
			if len(readerData) < minEntries {
				minEntries = len(readerData)
			}
			t.Logf("Reader %d read %d entries", readerIdx, len(readerData))
		}

		if minEntries > 0 {
			// Verify data consistency for the entries that all readers managed to read
			for entryId := 0; entryId < minEntries; entryId++ {
				// Get data from first reader as reference
				referenceData, exists := allReaderResults[0][int64(entryId)]
				if !exists {
					continue
				}

				// Compare with all other readers
				for readerIdx := 1; readerIdx < numReaders; readerIdx++ {
					readerData, exists := allReaderResults[readerIdx][int64(entryId)]
					if exists {
						assert.Equal(t, referenceData, readerData,
							"Entry %d should be identical across all readers (reader 0 vs reader %d)",
							entryId, readerIdx)
					}
				}
			}

			t.Logf("All %d readers successfully read consistent data for %d entries",
				numReaders, minEntries)
		}
	})

	// Final verification: Create a new reader and verify data is accessible
	t.Run("FinalFileVerification", func(t *testing.T) {
		finalReader, err := objectstorage.NewMinioFileReader(ctx, testBucket, baseDir, logId, segmentId, minioHdl)
		require.NoError(t, err)
		defer finalReader.Close(ctx)

		// Verify file is incomplete (no footer)
		footer := finalReader.GetFooter()
		assert.Nil(t, footer, "File should be incomplete (no footer)")

		// Verify we can read available entries
		lastEntryId, err := finalReader.GetLastEntryID(ctx)
		if err == nil && lastEntryId >= 0 {
			allEntries, err := finalReader.ReadNextBatch(ctx, storage.ReaderOpt{
				StartEntryID: 0,
				BatchSize:    lastEntryId + 1,
			})
			if err == nil {
				assert.Greater(t, len(allEntries), 0, "Should read at least some entries")

				// Verify entry content and order
				for i, entry := range allEntries {
					assert.Equal(t, int64(i), entry.EntryId, "Entry ID should match")
					assert.Contains(t, string(entry.Values), fmt.Sprintf("Entry %d:", i),
						"Entry content should contain expected prefix")
				}

				t.Logf("Final verification: File contains %d readable entries", len(allEntries))
			}
		}
	})
}

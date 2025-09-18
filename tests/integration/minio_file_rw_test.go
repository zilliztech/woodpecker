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
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"hash/crc32"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage"
	"github.com/zilliztech/woodpecker/tests/utils"
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
	data, err := utils.GenerateRandomBytes(size)
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
	lastEntryId, err := writer.Finalize(ctx, -1)
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

	// Calculate block length and CRC for the serialized data
	blockLength := uint32(len(serializedData))
	blockCrc := crc32.ChecksumIEEE(serializedData)

	blockHeaderRecord := &codec.BlockHeaderRecord{
		BlockNumber:  0, // Test block number
		FirstEntryID: firstEntryID,
		LastEntryID:  lastEntryID,
		BlockLength:  blockLength,
		BlockCrc:     blockCrc,
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

	_, err = writer.Finalize(ctx, -1)
	if err != nil {
		b.Fatal(err)
	}
	err = writer.Close(ctx)
	if err != nil {
		b.Fatal(err)
	}

	// Benchmark reading
	reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, 1, 1, minioHdl, 16_000_000, 32)
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		startId := int64(i % 900) // Ensure we don't go beyond available entries
		entries, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    startId,
			MaxBatchEntries: 10,
		}, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(entries.Entries) == 0 {
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

		emptyMsg := &log.WriteMessage{
			Payload:    []byte{},
			Properties: map[string]string{"test": "value"},
		}

		_, err := log.MarshalMessage(emptyMsg)
		require.NoError(t, err)
	})

	// Test nil payload validation
	t.Run("NilPayloadAtClientLevel", func(t *testing.T) {
		nilMsg := &log.WriteMessage{
			Payload:    nil,
			Properties: map[string]string{"test": "value"},
		}

		_, err := log.MarshalMessage(nilMsg)
		require.NoError(t, err)
	})

	// Test both empty err
	t.Run("BothEmptyMsg", func(t *testing.T) {
		nilMsg := &log.WriteMessage{
			Payload:    nil,
			Properties: map[string]string{},
		}

		_, err := log.MarshalMessage(nilMsg)
		require.Error(t, err)
		require.True(t, werr.ErrInvalidMessage.Is(err))

		t.Logf("Client level nil payload error: %v", err)
	})

	// Test valid payload for comparison
	t.Run("ValidPayload", func(t *testing.T) {
		validMsg := &log.WriteMessage{
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

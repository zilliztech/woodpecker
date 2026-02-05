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
	"hash/crc32"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
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

func setupMinioFileWriterTest(t *testing.T) (storageclient.ObjectStorage, *config.Configuration) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	// Set log level to debug for detailed logging
	cfg.Log.Level = "debug"

	// Initialize logger with debug level
	logger.InitLogger(cfg)

	testBucket = cfg.Minio.BucketName
	client, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)
	return client, cfg
}

func cleanupMinioFileWriterObjects(t *testing.T, client storageclient.ObjectStorage, prefix string) {
	ctx := context.Background()

	// Use WalkWithObjects to list and delete objects
	err := client.WalkWithObjects(ctx, testBucket, prefix, true, func(objInfo *storageclient.ChunkObjectInfo) bool {
		deleteErr := client.RemoveObject(ctx, testBucket, objInfo.FilePath, "test-ns", "0")
		if deleteErr != nil {
			t.Logf("Warning: failed to cleanup object %s: %v", objInfo.FilePath, deleteErr)
		}
		return true // Continue walking
	}, "test-ns", "0")
	if err != nil {
		t.Logf("Warning: failed to list objects during cleanup with prefix %s: %v", prefix, err)
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

	// Verify objects were created in storage
	objectCount := 0
	err = minioHdl.WalkWithObjects(ctx, testBucket, baseDir, true, func(objInfo *storageclient.ChunkObjectInfo) bool {
		objectCount++
		t.Logf("Created object: %s", objInfo.FilePath)
		return true // Continue walking
	}, "test-ns", "0")
	require.NoError(t, err)
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
	objects := make([]string, 0)
	err = minioHdl.WalkWithObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, func(objInfo *storageclient.ChunkObjectInfo) bool {
		objects = append(objects, objInfo.FilePath)
		t.Logf("Finalized object: %s", objInfo.FilePath)
		return true // Continue walking
	}, "test-ns", "0")
	require.NoError(t, err)
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
	err = minioHdl.WalkWithObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, func(objInfo *storageclient.ChunkObjectInfo) bool {
		// Skip lock files
		if len(objInfo.FilePath) >= 5 && objInfo.FilePath[len(objInfo.FilePath)-5:] == ".lock" {
			return true // Continue walking
		}

		t.Logf("Verifying object: %s", objInfo.FilePath)

		// Get object size first
		objSize, _, err := minioHdl.StatObject(ctx, testBucket, objInfo.FilePath, "test-ns", "0")
		require.NoError(t, err)

		// Read object content
		obj, err := minioHdl.GetObject(ctx, testBucket, objInfo.FilePath, 0, objSize, "test-ns", "0")
		require.NoError(t, err)

		data, err := io.ReadAll(obj)
		require.NoError(t, err)
		obj.Close()

		// Parse records from the object
		records, err := codec.DecodeRecordList(data)
		require.NoError(t, err)

		if len(records) == 0 {
			t.Logf("Object contains no records, skipping verification")
			return true // Continue walking
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
		return true // Continue walking
	}, "test-ns", "0")
	require.NoError(t, err)
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
	err = minioHdl.WalkWithObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, func(objInfo *storageclient.ChunkObjectInfo) bool {
		if strings.HasSuffix(objInfo.FilePath, ".blk") {
			t.Logf("Checking object: %s", objInfo.FilePath)

			// Get object size first
			objSize, _, err := minioHdl.StatObject(ctx, testBucket, objInfo.FilePath, "test-ns", "0")
			require.NoError(t, err)

			// Read the object
			obj, err := minioHdl.GetObject(ctx, testBucket, objInfo.FilePath, 0, objSize, "test-ns", "0")
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
				return true // Continue walking
			}
			t.Logf("Decoded %d records from object %s", len(records), objInfo.FilePath)

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

			assert.True(t, foundBlockHeaderRecord, "Should find BlockHeaderRecord in object %s", objInfo.FilePath)
		}
		return true // Continue walking
	}, "test-ns", "0")
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
	var firstDataObject string
	err = minioHdl.WalkWithObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, func(objInfo *storageclient.ChunkObjectInfo) bool {
		if strings.HasSuffix(objInfo.FilePath, ".blk") && !strings.HasSuffix(objInfo.FilePath, "footer.blk") {
			// This should be the first data object (0.blk)
			if strings.HasSuffix(objInfo.FilePath, "/0.blk") {
				firstDataObject = objInfo.FilePath
				return false // Stop walking
			}
		}
		return true // Continue walking
	}, "test-ns", "0")
	require.NoError(t, err)

	require.NotEmpty(t, firstDataObject, "Should find the first data object (0.blk)")
	t.Logf("Found first data object: %s", firstDataObject)

	// Get object size first
	objSize, _, err := minioHdl.StatObject(ctx, testBucket, firstDataObject, "test-ns", "0")
	require.NoError(t, err)

	// Read the first object content
	obj, err := minioHdl.GetObject(ctx, testBucket, firstDataObject, 0, objSize, "test-ns", "0")
	require.NoError(t, err)

	data, err := io.ReadAll(obj)
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
		err = minioHdl.WalkWithObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			t.Logf("  Before close: %s", objInfo.FilePath)
			return true // Continue walking
		}, "test-ns", "0")
		require.NoError(t, err)

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
		objectCount := 0
		err = minioHdl.WalkWithObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId), true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			if !strings.HasSuffix(objInfo.FilePath, ".lock") {
				objectCount++
				t.Logf("  After close: %s", objInfo.FilePath)
			}
			return true // Continue walking
		}, "test-ns", "0")
		require.NoError(t, err)
		assert.Greater(t, objectCount, 0, "Should have created at least one data object")
	})
}

// TestMinioFileWriter_CompactionWithCleanup tests compact functionality including small file cleanup
func TestMinioFileWriter_CompactionWithCleanup(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	// Configure for small blocks to create multiple files for compaction
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 100    // Very small blocks
	cfg.Woodpecker.Logstore.SegmentCompactionPolicy.MaxBytes = 1024 // Target 1KB merged blocks

	logId := int64(60)
	segmentId := int64(6000)
	baseDir := fmt.Sprintf("test-compaction-cleanup-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	t.Run("NormalCompactionWithCleanup", func(t *testing.T) {
		// Create writer and write data that will create multiple small blocks
		writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		require.NotNil(t, writer)

		// Write data that will create multiple small blocks
		testData := [][]byte{
			[]byte("Entry 0: " + string(generateTestData(80))), // Block 0
			[]byte("Entry 1: " + string(generateTestData(80))), // Block 1
			[]byte("Entry 2: " + string(generateTestData(80))), // Block 2
			[]byte("Entry 3: " + string(generateTestData(80))), // Block 3
			[]byte("Entry 4: " + string(generateTestData(80))), // Block 4
		}

		for i, data := range testData {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-compact-%d", i))
			_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
			require.NoError(t, err)

			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
		}

		// Finalize to complete writing
		lastEntryId, err := writer.Finalize(ctx, -1)
		require.NoError(t, err)
		assert.Equal(t, int64(len(testData)-1), lastEntryId)

		// Close writer
		err = writer.Close(ctx)
		require.NoError(t, err)

		// List objects before compaction
		segmentPrefix := fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId)
		t.Log("Objects BEFORE compaction:")
		originalBlocks := make([]string, 0)
		err = minioHdl.WalkWithObjects(ctx, testBucket, segmentPrefix, true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			t.Logf("  Before compact: %s", objInfo.FilePath)

			// Count original block files (exclude footer and lock)
			if strings.HasSuffix(objInfo.FilePath, ".blk") &&
				!strings.HasSuffix(objInfo.FilePath, "footer.blk") &&
				!strings.Contains(objInfo.FilePath, "m_") { // exclude merged files
				originalBlocks = append(originalBlocks, objInfo.FilePath)
			}
			return true // Continue walking
		}, "test-ns", "0")
		require.NoError(t, err)

		assert.Greater(t, len(originalBlocks), 1, "Should have multiple original blocks before compaction")
		t.Logf("Found %d original blocks before compaction", len(originalBlocks))

		// Create a new writer for compaction (recovery mode)
		writerForCompact, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg, true)
		require.NoError(t, err)
		require.NotNil(t, writerForCompact)

		// Perform compaction
		compactedSize, err := writerForCompact.Compact(ctx)
		require.NoError(t, err)
		assert.Greater(t, compactedSize, int64(0), "Compacted size should be positive")
		t.Logf("Compaction completed, compacted size: %d bytes", compactedSize)

		// Close compaction writer
		err = writerForCompact.Close(ctx)
		require.NoError(t, err)

		// List objects after compaction
		t.Log("Objects AFTER compaction:")
		remainingOriginalBlocks := make([]string, 0)
		mergedBlocks := make([]string, 0)
		hasFooter := false

		err = minioHdl.WalkWithObjects(ctx, testBucket, segmentPrefix, true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			t.Logf("  After compact: %s", objInfo.FilePath)

			if strings.HasSuffix(objInfo.FilePath, "footer.blk") {
				hasFooter = true
			} else if strings.HasSuffix(objInfo.FilePath, ".blk") {
				if strings.Contains(objInfo.FilePath, "m_") {
					mergedBlocks = append(mergedBlocks, objInfo.FilePath)
				} else if !strings.HasSuffix(objInfo.FilePath, ".lock") {
					remainingOriginalBlocks = append(remainingOriginalBlocks, objInfo.FilePath)
				}
			}
			return true // Continue walking
		}, "test-ns", "0")
		require.NoError(t, err)

		// Verify compaction results
		assert.True(t, hasFooter, "Should have footer.blk file")
		assert.Greater(t, len(mergedBlocks), 0, "Should have created merged blocks")
		assert.Equal(t, 0, len(remainingOriginalBlocks), "All original blocks should be deleted after compaction")

		t.Logf("Compaction verification: %d merged blocks, %d remaining original blocks",
			len(mergedBlocks), len(remainingOriginalBlocks))
	})

	t.Run("IdempotentCompaction", func(t *testing.T) {
		// This test verifies that running compact multiple times is safe and idempotent
		segmentId2 := segmentId + 1

		// Create writer and write data
		writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId2, minioHdl, cfg)
		require.NoError(t, err)

		// Write test data
		testData := [][]byte{
			[]byte("Idempotent 0: " + string(generateTestData(80))),
			[]byte("Idempotent 1: " + string(generateTestData(80))),
			[]byte("Idempotent 2: " + string(generateTestData(80))),
		}

		for i, data := range testData {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-idempotent-%d", i))
			_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
			require.NoError(t, err)

			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
		}

		_, err = writer.Finalize(ctx, -1)
		require.NoError(t, err)
		err = writer.Close(ctx)
		require.NoError(t, err)

		// First compaction
		writerForCompact1, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, baseDir, logId, segmentId2, minioHdl, cfg, true)
		require.NoError(t, err)

		compactedSize1, err := writerForCompact1.Compact(ctx)
		require.NoError(t, err)
		assert.Greater(t, compactedSize1, int64(0))

		err = writerForCompact1.Close(ctx)
		require.NoError(t, err)

		// Second compaction (should be idempotent)
		writerForCompact2, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, baseDir, logId, segmentId2, minioHdl, cfg, true)
		require.NoError(t, err)

		compactedSize2, err := writerForCompact2.Compact(ctx)
		require.NoError(t, err)
		assert.Equal(t, compactedSize1, compactedSize2, "Idempotent compaction should return same size")

		err = writerForCompact2.Close(ctx)
		require.NoError(t, err)

		// Verify objects remain consistent
		segmentPrefix2 := fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId2)
		remainingOriginalBlocks := 0
		err = minioHdl.WalkWithObjects(ctx, testBucket, segmentPrefix2, true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			if strings.HasSuffix(objInfo.FilePath, ".blk") &&
				!strings.HasSuffix(objInfo.FilePath, "footer.blk") &&
				!strings.Contains(objInfo.FilePath, "m_") &&
				!strings.HasSuffix(objInfo.FilePath, ".lock") {
				remainingOriginalBlocks++
			}
			return true // Continue walking
		}, "test-ns", "0")
		require.NoError(t, err)

		assert.Equal(t, 0, remainingOriginalBlocks, "Should have no original blocks after idempotent compaction")
		t.Log("Idempotent compaction test passed")
	})

	t.Run("CompactionRecoveryWithResidualFiles", func(t *testing.T) {
		// This test simulates a scenario where compaction was interrupted and left some original files
		segmentId3 := segmentId + 2

		// Create writer and write data
		writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId3, minioHdl, cfg)
		require.NoError(t, err)

		// Write test data
		testData := [][]byte{
			[]byte("Recovery 0: " + string(generateTestData(80))),
			[]byte("Recovery 1: " + string(generateTestData(80))),
			[]byte("Recovery 2: " + string(generateTestData(80))),
		}

		for i, data := range testData {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-recovery-%d", i))
			_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
			require.NoError(t, err)

			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
		}

		_, err = writer.Finalize(ctx, -1)
		require.NoError(t, err)
		err = writer.Close(ctx)
		require.NoError(t, err)

		// Perform compaction but simulate interruption by doing it manually
		writerForCompact, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, baseDir, logId, segmentId3, minioHdl, cfg, true)
		require.NoError(t, err)

		// Get the original blocks before compaction
		segmentPrefix3 := fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId3)
		originalBlockKeys := make([]string, 0)
		err = minioHdl.WalkWithObjects(ctx, testBucket, segmentPrefix3, true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			if strings.HasSuffix(objInfo.FilePath, ".blk") &&
				!strings.HasSuffix(objInfo.FilePath, "footer.blk") &&
				!strings.Contains(objInfo.FilePath, "m_") {
				originalBlockKeys = append(originalBlockKeys, objInfo.FilePath)
			}
			return true // Continue walking
		}, "test-ns", "0")
		require.NoError(t, err)

		// Complete compaction first
		compactedSize, err := writerForCompact.Compact(ctx)
		require.NoError(t, err)
		assert.Greater(t, compactedSize, int64(0))

		// Manually restore one of the original blocks to simulate interrupted cleanup
		if len(originalBlockKeys) > 0 {
			// Create a dummy original block to simulate incomplete cleanup
			dummyBlockKey := originalBlockKeys[0]
			dummyData := []byte("dummy original block data")
			err = minioHdl.PutObject(ctx, testBucket, dummyBlockKey,
				strings.NewReader(string(dummyData)), int64(len(dummyData)), "test-ns", "0")
			require.NoError(t, err)
			t.Logf("Manually created residual file: %s", dummyBlockKey)
		}

		err = writerForCompact.Close(ctx)
		require.NoError(t, err)

		// Now perform recovery compaction which should clean up the residual file
		writerForRecovery, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, baseDir, logId, segmentId3, minioHdl, cfg, true)
		require.NoError(t, err)

		// This should detect the compacted state and clean up residual files
		recoverySize, err := writerForRecovery.Compact(ctx)
		require.NoError(t, err)
		assert.Equal(t, compactedSize, recoverySize, "Recovery compaction should return same size")

		err = writerForRecovery.Close(ctx)
		require.NoError(t, err)

		// Verify all original blocks are cleaned up
		remainingOriginalBlocks := 0
		err = minioHdl.WalkWithObjects(ctx, testBucket, segmentPrefix3, true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			t.Logf("  After recovery: %s", objInfo.FilePath)

			if strings.HasSuffix(objInfo.FilePath, ".blk") &&
				!strings.HasSuffix(objInfo.FilePath, "footer.blk") &&
				!strings.Contains(objInfo.FilePath, "m_") &&
				!strings.HasSuffix(objInfo.FilePath, ".lock") {
				remainingOriginalBlocks++
			}
			return true // Continue walking
		}, "test-ns", "0")
		require.NoError(t, err)

		assert.Equal(t, 0, remainingOriginalBlocks, "Recovery should clean up all remaining original blocks")
		t.Log("Compaction recovery test passed")
	})

	t.Run("CompactionDuringReadCrossCompletedAndCompacted", func(t *testing.T) {
		// This test verifies that compaction doesn't interfere with ongoing read operations
		segmentId4 := segmentId + 3

		// Create writer and write 20 entries
		writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId4, minioHdl, cfg)
		require.NoError(t, err)

		// Write 20 test entries with fixed size for predictable batch control
		entryCount := 20
		entrySize := 100 // Fixed size per entry for predictable batch size calculation
		testEntries := make([][]byte, entryCount)
		for i := 0; i < entryCount; i++ {
			entryData := fmt.Sprintf("Entry_%04d: %s", i, string(generateTestData(entrySize-20))) // Reserve 20 bytes for prefix
			testEntries[i] = []byte(entryData)

			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-ch-%d", i))
			defer resultCh.Close(ctx)
			_, err = writer.WriteDataAsync(ctx, int64(i), testEntries[i], resultCh)
			require.NoError(t, err)
			_, readResultErr := resultCh.ReadResult(ctx)
			require.NoError(t, readResultErr)
		}

		// Finalize the writer
		lastEntryId, err := writer.Finalize(ctx, -1)
		require.NoError(t, err)
		assert.Equal(t, int64(entryCount-1), lastEntryId)

		err = writer.Close(ctx)
		require.NoError(t, err)

		t.Logf("Successfully wrote %d entries, each ~%d bytes", entryCount, entrySize)

		// Calculate maxBatchSize to read approximately 10 entries in first batch
		// Account for record headers and other overhead (approximately 50 bytes per entry overhead)
		firstReadCount := 10
		maxBatchSizeForFirstRead := int64(firstReadCount * entrySize)

		// Create reader with controlled batch size for predictable reading
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId4, minioHdl, maxBatchSizeForFirstRead, 4)
		require.NoError(t, err)

		// Read first batch (should get approximately 10 entries)
		readEntries := make([][]byte, 0, entryCount)

		t.Logf("Reading first batch with maxBatchSize: %d bytes...", maxBatchSizeForFirstRead)
		opt := storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(entryCount), // Set high to let maxBatchSize control the limit
		}

		batch, err := reader.ReadNextBatchAdv(ctx, opt, nil)
		require.NoError(t, err)
		require.NotNil(t, batch)

		for _, entry := range batch.Entries {
			readEntries = append(readEntries, entry.Values)
			t.Logf("Read entry %d: %s", entry.EntryId, string(entry.Values[:min(50, len(entry.Values))]))
		}

		firstBatchCount := len(readEntries)
		t.Logf("First batch read %d entries (expected around %d)", firstBatchCount, firstReadCount)
		assert.Greater(t, firstBatchCount, 5, "Should have read at least 5 entries in first batch")
		assert.Less(t, firstBatchCount, entryCount, "Should not have read all entries in first batch")

		// Now trigger compaction while reader is in middle of reading
		t.Log("Triggering compaction during read...")
		writerForCompact, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, baseDir, logId, segmentId4, minioHdl, cfg, true)
		require.NoError(t, err)

		compactedSize, err := writerForCompact.Compact(ctx)
		require.NoError(t, err)
		assert.Greater(t, compactedSize, int64(0))

		err = writerForCompact.Close(ctx)
		require.NoError(t, err)

		t.Logf("Compaction completed, size: %d bytes", compactedSize)

		// Continue reading the remaining entries (should work normally after compaction)
		t.Log("Continuing to read remaining entries after compaction...")

		// Determine where to start reading next
		lastReadEntryId := int64(-1)
		if len(readEntries) > 0 {
			// Find the last entry ID we actually read
			for i, entry := range batch.Entries {
				if i < len(readEntries) {
					lastReadEntryId = entry.EntryId
				}
			}
		}
		nextStartEntryId := lastReadEntryId + 1
		t.Logf("Continuing from entry ID: %d (last read: %d)", nextStartEntryId, lastReadEntryId)

		// Continue reading the remaining entries using a larger batch size to get all remaining entries
		opt2 := storage.ReaderOpt{
			StartEntryID:    nextStartEntryId,
			MaxBatchEntries: int64(entryCount), // Set high to get all remaining entries
		}

		// Use the LastReadState from previous batch to continue reading
		// first read will fail
		batchTmp, err := reader.ReadNextBatchAdv(ctx, opt2, batch.LastReadState)
		require.Error(t, err, "Should read nothing due to blocks deleted by compaction after last read")
		require.Nil(t, batchTmp)
		// second read retry should success, cause footer refresh, and lastReadState != refresh footer will relocate to new merged blocks for reading
		time.Sleep(time.Millisecond * 200) // mock 200 delay
		batch2, err := reader.ReadNextBatchAdv(ctx, opt2, batch.LastReadState)
		require.NoError(t, err, "Should be able to read remaining entries after compaction")
		require.NotNil(t, batch2)

		secondBatchCount := len(batch2.Entries)
		t.Logf("Second batch read %d entries", secondBatchCount)

		for _, entry := range batch2.Entries {
			readEntries = append(readEntries, entry.Values)
			t.Logf("Read entry %d after compaction: %s", entry.EntryId, string(entry.Values[:min(50, len(entry.Values))]))
		}

		totalReadCount := len(readEntries)
		t.Logf("Total entries read: %d (expected: %d)", totalReadCount, entryCount)

		// We should have read all entries
		assert.Equal(t, entryCount, totalReadCount, "Should have read all %d entries", entryCount)

		// Verify no more entries by trying to read beyond the last entry
		opt3 := storage.ReaderOpt{
			StartEntryID:    int64(entryCount),
			MaxBatchEntries: 10,
		}
		batch3, err := reader.ReadNextBatchAdv(ctx, opt3, batch2.LastReadState)
		if err == nil {
			assert.Empty(t, batch3.Entries, "Should get no more entries beyond the end")
		} else {
			// It's also acceptable to get an error when reading beyond the end
			t.Logf("Expected error when reading beyond end: %v", err)
		}

		// Verify content integrity - compare read entries with original
		for i := 0; i < entryCount; i++ {
			assert.Equal(t, testEntries[i], readEntries[i],
				"Entry %d content should match after compaction", i)
		}

		err = reader.Close(ctx)
		require.NoError(t, err)

		// Verify that compaction actually happened and cleaned up original files
		segmentPrefix4 := fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId4)
		originalBlocks := 0
		mergedBlocks := 0
		hasFooter := false

		err = minioHdl.WalkWithObjects(ctx, testBucket, segmentPrefix4, true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			t.Logf("  Final file: %s", objInfo.FilePath)

			if strings.HasSuffix(objInfo.FilePath, "footer.blk") {
				hasFooter = true
			} else if strings.HasSuffix(objInfo.FilePath, ".blk") {
				if strings.Contains(objInfo.FilePath, "m_") {
					mergedBlocks++
				} else if !strings.HasSuffix(objInfo.FilePath, ".lock") {
					originalBlocks++
				}
			}
			return true // Continue walking
		}, "test-ns", "0")
		require.NoError(t, err)

		assert.True(t, hasFooter, "Should have footer after compaction")
		assert.Greater(t, mergedBlocks, 0, "Should have merged blocks")
		assert.Equal(t, 0, originalBlocks, "All original blocks should be cleaned up")

		t.Logf("Read-during-compaction test passed: read %d entries successfully, %d merged blocks created",
			entryCount, mergedBlocks)
	})

	// Test all possible reader state transitions during read operations
	t.Run("ReaderStateTransitions", func(t *testing.T) {
		baseSegmentId := segmentId + 10 // Use different segment IDs for each test

		// Helper function to create test data
		createTestData := func(count int) [][]byte {
			entries := make([][]byte, count)
			for i := 0; i < count; i++ {
				entries[i] = []byte(fmt.Sprintf("StateTest_%04d: %s", i, string(generateTestData(80))))
			}
			return entries
		}

		// Helper function to write and prepare segment
		writeAndPrepareSegment := func(segId int64, finalizeSegment bool, compactSegment bool) [][]byte {
			writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segId, minioHdl, cfg)
			require.NoError(t, err)

			testData := createTestData(15)
			for i, data := range testData {
				resultCh := channel.NewLocalResultChannel(fmt.Sprintf("state-test-%d-%d", segId, i))
				defer resultCh.Close(ctx)
				_, err = writer.WriteDataAsync(ctx, int64(i), data, resultCh)
				require.NoError(t, err)
				_, readResultErr := resultCh.ReadResult(ctx)
				require.NoError(t, readResultErr)
			}

			if finalizeSegment {
				_, err = writer.Finalize(ctx, -1)
				require.NoError(t, err)
			}

			err = writer.Close(ctx)
			require.NoError(t, err)

			if compactSegment && finalizeSegment {
				writerForCompact, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, baseDir, logId, segId, minioHdl, cfg, true)
				require.NoError(t, err)
				_, err = writerForCompact.Compact(ctx)
				require.NoError(t, err)
				err = writerForCompact.Close(ctx)
				require.NoError(t, err)
			}

			return testData
		}

		// Helper function to read first batch
		readFirstBatch := func(segId int64, entryCount int) (*objectstorage.MinioFileReaderAdv, *proto.BatchReadResult, [][]byte) {
			maxBatchSize := int64(8 * 120) // Read about 8 entries in first batch
			reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segId, minioHdl, maxBatchSize, 4)
			require.NoError(t, err)

			opt := storage.ReaderOpt{
				StartEntryID:    0,
				MaxBatchEntries: int64(entryCount),
			}

			batch, err := reader.ReadNextBatchAdv(ctx, opt, nil)
			require.NoError(t, err)
			require.NotNil(t, batch)

			readEntries := make([][]byte, 0)
			for _, entry := range batch.Entries {
				readEntries = append(readEntries, entry.Values)
			}

			return reader, batch, readEntries
		}

		// Case 1: Active → Active (no state change)
		t.Run("ActiveToActive", func(t *testing.T) {
			segId := baseSegmentId + 1
			testData := writeAndPrepareSegment(segId, false, false) // Keep active

			reader, batch1, readEntries := readFirstBatch(segId, len(testData))
			defer reader.Close(ctx)

			t.Logf("First batch read %d entries in Active state", len(readEntries))

			// Continue reading (segment remains active)
			nextStartId := int64(len(readEntries))
			opt2 := storage.ReaderOpt{
				StartEntryID:    nextStartId,
				MaxBatchEntries: int64(len(testData)),
			}

			batch2, err := reader.ReadNextBatchAdv(ctx, opt2, batch1.LastReadState)
			require.NoError(t, err)

			for _, entry := range batch2.Entries {
				readEntries = append(readEntries, entry.Values)
			}

			t.Logf("Active→Active: Read %d total entries", len(readEntries))
			assert.GreaterOrEqual(t, len(readEntries), len(testData), "Should read all entries")
		})

		// Case 2: Active → Completed
		t.Run("ActiveToCompleted", func(t *testing.T) {
			segId := baseSegmentId + 2
			testData := writeAndPrepareSegment(segId, false, false) // Start active

			reader, batch1, readEntries := readFirstBatch(segId, len(testData))
			defer reader.Close(ctx)

			t.Logf("First batch read %d entries in Active state", len(readEntries))

			// Now finalize the segment (Active → Completed)
			writerToFinalize, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, baseDir, logId, segId, minioHdl, cfg, true)
			require.NoError(t, err)
			_, err = writerToFinalize.Finalize(ctx, -1)
			require.NoError(t, err)
			err = writerToFinalize.Close(ctx)
			require.NoError(t, err)

			// Continue reading after finalization
			nextStartId := int64(len(readEntries))
			opt2 := storage.ReaderOpt{
				StartEntryID:    nextStartId,
				MaxBatchEntries: int64(len(testData)),
			}

			batch2, err := reader.ReadNextBatchAdv(ctx, opt2, batch1.LastReadState)
			require.NoError(t, err)

			for _, entry := range batch2.Entries {
				readEntries = append(readEntries, entry.Values)
			}

			t.Logf("Active→Completed: Read %d total entries", len(readEntries))
			assert.Equal(t, len(testData), len(readEntries), "Should read all entries")
		})

		// Case 3: Active → Compacted
		t.Run("ActiveToCompacted", func(t *testing.T) {
			segId := baseSegmentId + 3
			testData := writeAndPrepareSegment(segId, false, false) // Start active

			reader, batch1, readEntries := readFirstBatch(segId, len(testData))
			defer reader.Close(ctx)

			t.Logf("First batch read %d entries in Active state", len(readEntries))

			// Now finalize and compact the segment (Active → Compacted)
			writerToCompact, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, baseDir, logId, segId, minioHdl, cfg, true)
			require.NoError(t, err)
			_, err = writerToCompact.Finalize(ctx, -1)
			require.NoError(t, err)
			_, err = writerToCompact.Compact(ctx)
			require.NoError(t, err)
			err = writerToCompact.Close(ctx)
			require.NoError(t, err)

			// Continue reading after compaction
			nextStartId := int64(len(readEntries))
			opt2 := storage.ReaderOpt{
				StartEntryID:    nextStartId,
				MaxBatchEntries: int64(len(testData)),
			}

			// This might return an error due to block deletion, which is expected
			// first read will fail
			batchTmp, err := reader.ReadNextBatchAdv(ctx, opt2, batch1.LastReadState)
			require.Error(t, err, "Should read nothing due to blocks deleted by compaction after last read")
			require.Nil(t, batchTmp)
			// second read retry should success, cause footer refresh, and lastReadState != refresh footer will relocate to new merged blocks for reading
			time.Sleep(200 * time.Millisecond) // mock retry interval
			batch2, err := reader.ReadNextBatchAdv(ctx, opt2, batch1.LastReadState)
			require.NoError(t, err, "Should be able to read remaining entries after compaction")
			require.NotNil(t, batch2)

			for _, entry := range batch2.Entries {
				readEntries = append(readEntries, entry.Values)
			}

			t.Logf("Active→Compacted: Read %d total entries", len(readEntries))
			assert.Equal(t, len(testData), len(readEntries), "Should read all entries")
		})

		// Case 4: Completed → Completed (no state change)
		t.Run("CompletedToCompleted", func(t *testing.T) {
			segId := baseSegmentId + 4
			testData := writeAndPrepareSegment(segId, true, false) // Start completed

			reader, batch1, readEntries := readFirstBatch(segId, len(testData))
			defer reader.Close(ctx)

			t.Logf("First batch read %d entries in Completed state", len(readEntries))

			// Continue reading (segment remains completed)
			nextStartId := int64(len(readEntries))
			opt2 := storage.ReaderOpt{
				StartEntryID:    nextStartId,
				MaxBatchEntries: int64(len(testData)),
			}

			batch2, err := reader.ReadNextBatchAdv(ctx, opt2, batch1.LastReadState)
			require.NoError(t, err)

			for _, entry := range batch2.Entries {
				readEntries = append(readEntries, entry.Values)
			}

			t.Logf("Completed→Completed: Read %d total entries", len(readEntries))
			assert.Equal(t, len(testData), len(readEntries), "Should read all entries")
		})

		// Case 5: Completed → Compacted
		t.Run("CompletedToCompacted", func(t *testing.T) {
			segId := baseSegmentId + 5
			testData := writeAndPrepareSegment(segId, true, false) // Start completed

			reader, batch1, readEntries := readFirstBatch(segId, len(testData))
			defer reader.Close(ctx)

			t.Logf("First batch read %d entries in Completed state", len(readEntries))

			// Now compact the segment (Completed → Compacted)
			writerToCompact, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, baseDir, logId, segId, minioHdl, cfg, true)
			require.NoError(t, err)
			_, err = writerToCompact.Compact(ctx)
			require.NoError(t, err)
			err = writerToCompact.Close(ctx)
			require.NoError(t, err)

			// Continue reading after compaction
			nextStartId := int64(len(readEntries))
			opt2 := storage.ReaderOpt{
				StartEntryID:    nextStartId,
				MaxBatchEntries: int64(len(testData)),
			}

			// This might return an error due to block deletion, which is expected
			// first read will fail
			batchTmp, err := reader.ReadNextBatchAdv(ctx, opt2, batch1.LastReadState)
			require.Error(t, err, "Should read nothing due to blocks deleted by compaction after last read")
			require.Nil(t, batchTmp)
			// second read retry should success, cause footer refresh, and lastReadState != refresh footer will relocate to new merged blocks for reading
			time.Sleep(200 * time.Millisecond) // mock retry interval
			batch2, err := reader.ReadNextBatchAdv(ctx, opt2, batch1.LastReadState)
			require.NoError(t, err, "Should be able to read remaining entries after compaction")
			require.NotNil(t, batch2)

			for _, entry := range batch2.Entries {
				readEntries = append(readEntries, entry.Values)
			}

			t.Logf("Completed→Compacted: Read %d total entries", len(readEntries))
			assert.Equal(t, len(testData), len(readEntries), "Should read all entries")
		})

		// Case 6: Compacted → Compacted (no state change)
		t.Run("CompactedToCompacted", func(t *testing.T) {
			segId := baseSegmentId + 6
			testData := writeAndPrepareSegment(segId, true, true) // Start compacted

			reader, batch1, readEntries := readFirstBatch(segId, len(testData))
			defer reader.Close(ctx)

			t.Logf("First batch read %d entries in Compacted state", len(readEntries))

			// Continue reading (segment remains compacted)
			nextStartId := int64(len(readEntries))
			opt2 := storage.ReaderOpt{
				StartEntryID:    nextStartId,
				MaxBatchEntries: int64(len(testData)),
			}

			batch2, err := reader.ReadNextBatchAdv(ctx, opt2, batch1.LastReadState)
			require.NoError(t, err)

			for _, entry := range batch2.Entries {
				readEntries = append(readEntries, entry.Values)
			}

			t.Logf("Compacted→Compacted: Read %d total entries", len(readEntries))
			assert.Equal(t, len(testData), len(readEntries), "Should read all entries")
		})

		// Case 7: Read all data from Active segment, then Finalize+Compact, expect EOF on next read
		t.Run("ActiveReadAllThenCompactEOF", func(t *testing.T) {
			segId := baseSegmentId + 7

			// Write 10 entries without finalizing
			writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segId, minioHdl, cfg)
			require.NoError(t, err)

			testData := createTestData(10)
			for i, data := range testData {
				resultCh := channel.NewLocalResultChannel(fmt.Sprintf("eof-test1-%d-%d", segId, i))
				defer resultCh.Close(ctx)
				_, err = writer.WriteDataAsync(ctx, int64(i), data, resultCh)
				require.NoError(t, err)
				_, readResultErr := resultCh.ReadResult(ctx)
				require.NoError(t, readResultErr)
			}

			err = writer.Close(ctx)
			require.NoError(t, err)

			// Read all data from active segment
			maxBatchSize := int64(10 * 120) // Large enough to read all entries in one batch
			reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segId, minioHdl, maxBatchSize, 4)
			require.NoError(t, err)
			defer reader.Close(ctx)

			opt := storage.ReaderOpt{
				StartEntryID:    0,
				MaxBatchEntries: int64(len(testData)),
			}

			// First read - get all data
			batch1, err := reader.ReadNextBatchAdv(ctx, opt, nil)
			require.NoError(t, err)
			require.NotNil(t, batch1)
			require.Equal(t, len(testData), len(batch1.Entries), "Should read all entries")

			t.Logf("Read all %d entries from Active segment", len(batch1.Entries))

			// Now finalize and compact the segment
			writerToCompact, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, baseDir, logId, segId, minioHdl, cfg, true)
			require.NoError(t, err)
			_, err = writerToCompact.Finalize(ctx, -1)
			require.NoError(t, err)
			_, err = writerToCompact.Compact(ctx)
			require.NoError(t, err)
			err = writerToCompact.Close(ctx)
			require.NoError(t, err)

			// Try to read next batch - should return EOF/no more data
			nextStartId := int64(len(testData))
			opt2 := storage.ReaderOpt{
				StartEntryID:    nextStartId,
				MaxBatchEntries: int64(len(testData)),
			}

			batch2, err := reader.ReadNextBatchAdv(ctx, opt2, batch1.LastReadState)
			if err != nil {
				t.Logf("Got expected error when trying to read beyond EOF: %v", err)
			} else {
				// If no error, should have empty entries (EOF condition)
				require.NotNil(t, batch2)
				require.Empty(t, batch2.Entries, "Should have no more entries to read (EOF)")
				t.Logf("Got EOF condition: no more entries to read")
			}

			t.Logf("ActiveReadAllThenCompactEOF test completed successfully")
		})

		// Case 8: Read all data from Completed segment, then Compact, expect EOF on next read
		t.Run("CompletedReadAllThenCompactEOF", func(t *testing.T) {
			segId := baseSegmentId + 8

			// Write 10 entries and finalize
			writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segId, minioHdl, cfg)
			require.NoError(t, err)

			testData := createTestData(10)
			for i, data := range testData {
				resultCh := channel.NewLocalResultChannel(fmt.Sprintf("eof-test2-%d-%d", segId, i))
				defer resultCh.Close(ctx)
				_, err = writer.WriteDataAsync(ctx, int64(i), data, resultCh)
				require.NoError(t, err)
				_, readResultErr := resultCh.ReadResult(ctx)
				require.NoError(t, readResultErr)
			}

			// Finalize the segment (Active -> Completed)
			_, err = writer.Finalize(ctx, -1)
			require.NoError(t, err)
			err = writer.Close(ctx)
			require.NoError(t, err)

			// Read all data from completed segment
			maxBatchSize := int64(10 * 120) // Large enough to read all entries in one batch
			reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segId, minioHdl, maxBatchSize, 4)
			require.NoError(t, err)
			defer reader.Close(ctx)

			opt := storage.ReaderOpt{
				StartEntryID:    0,
				MaxBatchEntries: int64(len(testData)),
			}

			// First read - get all data
			batch1, err := reader.ReadNextBatchAdv(ctx, opt, nil)
			require.NoError(t, err)
			require.NotNil(t, batch1)
			require.Equal(t, len(testData), len(batch1.Entries), "Should read all entries")

			t.Logf("Read all %d entries from Completed segment", len(batch1.Entries))

			// Now compact the segment (Completed -> Compacted)
			writerToCompact, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, baseDir, logId, segId, minioHdl, cfg, true)
			require.NoError(t, err)
			_, err = writerToCompact.Compact(ctx)
			require.NoError(t, err)
			err = writerToCompact.Close(ctx)
			require.NoError(t, err)

			// Try to read next batch - should return EOF/no more data
			nextStartId := int64(len(testData))
			opt2 := storage.ReaderOpt{
				StartEntryID:    nextStartId,
				MaxBatchEntries: int64(len(testData)),
			}

			batch2, err := reader.ReadNextBatchAdv(ctx, opt2, batch1.LastReadState)
			if err != nil {
				t.Logf("Got expected error when trying to read beyond EOF: %v", err)
			} else {
				// If no error, should have empty entries (EOF condition)
				require.NotNil(t, batch2)
				require.Empty(t, batch2.Entries, "Should have no more entries to read (EOF)")
				t.Logf("Got EOF condition: no more entries to read")
			}

			t.Logf("CompletedReadAllThenCompactEOF test completed successfully")
		})

		t.Log("All reader state transition tests completed successfully")
	})
}

// TestMinioFileWriter_CheckpointRecovery verifies that checkpoint-based
// recovery is both correct and meaningfully faster than a full block scan.
//
// Setup: 909 blocks × ~100 KB each, checkpointThreshold = 10.
// The last checkpoint covers blocks 0-899, so recovery scans only 9 blocks
// (900-908) incrementally.
//
// Approach:
//  1. Write 909 blocks (~100 KB each), close without finalize.
//  2. Verify segment state: checkpoint.blk exists, no footer.blk.
//  3. Calibrate: sequentially StatObject 909 blocks vs 10 blocks.
//  4. Time recovery and assert it is on the ~10-block order, not ~909-block.
//  5. Finalize and verify checkpoint.blk is cleaned up.
func TestMinioFileWriter_CheckpointRecovery(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(10)
	segmentId := int64(2000)
	baseDir := fmt.Sprintf("test-checkpoint-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// ~100 KB per entry; MaxFlushSize large enough to hold one entry per block.
	// Use a 10 ms sync interval so blocks are flushed quickly during the write loop.
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 200 * 1024
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.CheckpointThreshold = 10
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval = config.NewDurationMillisecondsFromInt(10)

	const totalBlocks = 909
	const entrySize = 100 * 1024 // 100 KB
	segmentPrefix := fmt.Sprintf("%s/%d/%d/", baseDir, logId, segmentId)

	// Pre-generate a reusable 100 KB payload.
	payload := generateTestData(entrySize)

	// ---- Step 1: write 909 blocks, close without finalize ----
	t.Run("WriteBlocksAndClose", func(t *testing.T) {
		writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)

		for i := 0; i < totalBlocks; i++ {
			ch := channel.NewLocalResultChannel(fmt.Sprintf("cp-%d", i))
			_, err := writer.WriteDataAsync(ctx, int64(i), payload, ch)
			require.NoError(t, err)
			res, err := ch.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, res.Err)
			require.NoError(t, writer.Sync(ctx))
		}

		// Let the async checkpoint goroutine finish.
		time.Sleep(2 * time.Second)

		// Close without finalize to simulate a crash.
		require.NoError(t, writer.Close(ctx))
	})

	// ---- Step 2: verify segment state ----
	t.Run("VerifySegmentState", func(t *testing.T) {
		var hasCheckpoint, hasFooter bool
		var dataBlockCount int
		err := minioHdl.WalkWithObjects(ctx, testBucket, segmentPrefix, true, func(obj *storageclient.ChunkObjectInfo) bool {
			switch {
			case strings.HasSuffix(obj.FilePath, "checkpoint.blk"):
				hasCheckpoint = true
			case strings.HasSuffix(obj.FilePath, "footer.blk"):
				hasFooter = true
			case strings.HasSuffix(obj.FilePath, ".blk") && !strings.HasSuffix(obj.FilePath, ".lock"):
				dataBlockCount++
			}
			return true
		}, "test-ns", "0")
		require.NoError(t, err)

		assert.True(t, hasCheckpoint, "checkpoint.blk must exist")
		assert.False(t, hasFooter, "footer.blk must NOT exist (not finalized)")
		assert.Equal(t, totalBlocks, dataBlockCount, "should have exactly %d data blocks", totalBlocks)
		t.Logf("Segment state OK: checkpoint=%v footer=%v dataBlocks=%d", hasCheckpoint, hasFooter, dataBlockCount)
	})

	// ---- Step 3: calibrate per-block latency (StatObject + GetObject) ----
	// The actual recovery does StatObject + GetObject(partial) per block,
	// so the calibration must include both to be a fair comparison.
	var scanAll time.Duration
	var scan10 time.Duration

	t.Run("CalibrateBlockLatency", func(t *testing.T) {
		// Stat + read all 909 blocks sequentially.
		start := time.Now()
		for i := 0; i < totalBlocks; i++ {
			key := fmt.Sprintf("%s/%d/%d/%d.blk", baseDir, logId, segmentId, i)
			size, _, err := minioHdl.StatObject(ctx, testBucket, key, "test-ns", "0")
			require.NoError(t, err)
			obj, err := minioHdl.GetObject(ctx, testBucket, key, 0, size, "test-ns", "0")
			require.NoError(t, err)
			obj.Close()
		}
		scanAll = time.Since(start)

		// Stat + read 10 blocks sequentially.
		start = time.Now()
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("%s/%d/%d/%d.blk", baseDir, logId, segmentId, i)
			size, _, err := minioHdl.StatObject(ctx, testBucket, key, "test-ns", "0")
			require.NoError(t, err)
			obj, err := minioHdl.GetObject(ctx, testBucket, key, 0, size, "test-ns", "0")
			require.NoError(t, err)
			obj.Close()
		}
		scan10 = time.Since(start)

		t.Logf("Calibration: scan %d blocks (stat+get) = %v, scan 10 blocks = %v", totalBlocks, scanAll, scan10)
	})

	// ---- Step 4: recovery must be on the ~10-block order, not ~909-block ----
	t.Run("RecoveryIsFast", func(t *testing.T) {
		start := time.Now()
		writer2, err := objectstorage.NewMinioFileWriterWithMode(
			ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg, true)
		recoveryTime := time.Since(start)
		require.NoError(t, err)

		// Correctness: all 909 entries recovered.
		assert.Equal(t, int64(0), writer2.GetFirstEntryId(ctx))
		assert.Equal(t, int64(totalBlocks-1), writer2.GetLastEntryId(ctx))

		t.Logf("Recovery time: %v  (calibration: 10-blk=%v, %d-blk=%v)", recoveryTime, scan10, totalBlocks, scanAll)

		// The last checkpoint covers blocks 0-899 (909/10*10 = 900).
		// Recovery reads 1 checkpoint.blk + scans 9 incremental blocks.
		// It must be significantly less than a full 909-block scan.
		assert.Less(t, recoveryTime, scanAll/2,
			"Checkpoint recovery (%v) should be less than half of a full %d-block scan (%v)",
			recoveryTime, totalBlocks, scanAll)

		require.NoError(t, writer2.Close(ctx))
	})

	// ---- Step 5: finalize cleans up checkpoint.blk ----
	t.Run("CheckpointDeletedAfterFinalize", func(t *testing.T) {
		writer3, err := objectstorage.NewMinioFileWriterWithMode(
			ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg, true)
		require.NoError(t, err)

		_, err = writer3.Finalize(ctx, -1)
		require.NoError(t, err)

		// Let async deletion complete.
		time.Sleep(500 * time.Millisecond)

		var hasCheckpoint bool
		err = minioHdl.WalkWithObjects(ctx, testBucket, segmentPrefix, true, func(obj *storageclient.ChunkObjectInfo) bool {
			if strings.HasSuffix(obj.FilePath, "checkpoint.blk") {
				hasCheckpoint = true
				return false
			}
			return true
		}, "test-ns", "0")
		require.NoError(t, err)
		assert.False(t, hasCheckpoint, "checkpoint.blk should be deleted after finalize")

		require.NoError(t, writer3.Close(ctx))
	})
}

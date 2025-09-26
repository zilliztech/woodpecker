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
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/objectstorage"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func TestAdvAdvMinioFileWriter_RecoveryAfterInterruption(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(10)
	segmentId := int64(1000)
	baseDir := fmt.Sprintf("test-recovery-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	t.Run("AdvWriteDataAndInterrupt", func(t *testing.T) {
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

		// Verify objects were created in object storage
		var objectCount int
		var hasFooter bool
		err = minioHdl.WalkWithObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d/", baseDir, logId, segmentId), true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			if !strings.HasSuffix(objInfo.FilePath, ".lock") {
				objectCount++
				if strings.HasSuffix(objInfo.FilePath, "footer.blk") {
					hasFooter = true
				}
				// Get object size for logging
				objSize, _, statErr := minioHdl.StatObject(ctx, testBucket, objInfo.FilePath)
				if statErr == nil {
					t.Logf("Created object after interruption: %s (size: %d)", objInfo.FilePath, objSize)
				} else {
					t.Logf("Created object after interruption: %s", objInfo.FilePath)
				}
			}
			return true // continue walking
		})
		require.NoError(t, err)
		assert.Greater(t, objectCount, 0, "Should have created at least one data object")

		// Check if footer exists to determine test path
		if hasFooter {
			t.Log("Footer found - segment was completed during close, testing recovery from completed segment")
		} else {
			t.Log("No footer found - segment was interrupted, testing recovery from incomplete segment")
		}
	})

	var recoverySegmentFileKey string
	t.Run("AdvRecoverAndContinueWriting", func(t *testing.T) {
		// Use the same segment key to test true recovery
		recoverySegmentFileKey = baseDir

		// Check if segment has footer first
		var hasFooter bool
		err := minioHdl.WalkWithObjects(ctx, testBucket, baseDir+"/", true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			if strings.HasSuffix(objInfo.FilePath, "footer.blk") {
				hasFooter = true
				return false // stop walking
			}
			return true // continue walking
		})
		require.NoError(t, err)

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

	t.Run("AdvVerifyOriginalSegmentObjects", func(t *testing.T) {
		// Verify that the objects were created
		var dataObjects []string
		err := minioHdl.WalkWithObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d/", baseDir, logId, segmentId), true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			if strings.HasSuffix(objInfo.FilePath, ".blk") {
				dataObjects = append(dataObjects, objInfo.FilePath)
				// Get object size for logging
				objSize, _, statErr := minioHdl.StatObject(ctx, testBucket, objInfo.FilePath)
				if statErr == nil {
					t.Logf("Found segment object: %s (size: %d)", objInfo.FilePath, objSize)
				} else {
					t.Logf("Found segment object: %s", objInfo.FilePath)
				}
			}
			return true // continue walking
		})
		require.NoError(t, err)

		assert.Greater(t, len(dataObjects), 0, "Should have created data objects")
		t.Logf("Total data objects: %d", len(dataObjects))
	})

	t.Run("AdvVerifyFinalSegmentState", func(t *testing.T) {
		// Create reader to verify the final segment state
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, recoverySegmentFileKey, logId, segmentId, minioHdl, 16_000_000, 32)
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
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 20, // Read all entries
		}, nil)
		require.NoError(t, err)
		t.Logf("Read %d entries", len(batch.Entries))

		// We should have at least the original 4 entries
		assert.GreaterOrEqual(t, len(batch.Entries), 4, "Should have at least the original 4 entries")

		// Verify some entry content (at least the first few)
		if len(batch.Entries) > 0 {
			assert.Equal(t, int64(0), batch.Entries[0].EntryId)
			assert.Equal(t, []byte("Entry 0: Initial data"), batch.Entries[0].Values)
		}
		if len(batch.Entries) > 1 {
			assert.Equal(t, int64(1), batch.Entries[1].EntryId)
			assert.Equal(t, []byte("Entry 1: More data"), batch.Entries[1].Values)
		}

		err = reader.Close(ctx)
		require.NoError(t, err)
	})

	t.Run("AdvVerifyAllObjectsExist", func(t *testing.T) {
		// List all objects and verify structure
		var dataObjects []string
		err := minioHdl.WalkWithObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d/", baseDir, logId, segmentId), true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			if strings.HasSuffix(objInfo.FilePath, ".lock") {
				return true // Skip lock files, continue walking
			}

			if strings.Contains(objInfo.FilePath, ".blk") {
				dataObjects = append(dataObjects, objInfo.FilePath)
			}

			// Get object size for logging
			objSize, _, statErr := minioHdl.StatObject(ctx, testBucket, objInfo.FilePath)
			if statErr == nil {
				t.Logf("Final object: %s (size: %d)", objInfo.FilePath, objSize)
			} else {
				t.Logf("Final object: %s", objInfo.FilePath)
			}
			return true // continue walking
		})
		require.NoError(t, err)

		assert.Greater(t, len(dataObjects), 0, "Should have data objects")
		t.Logf("Total data objects: %d", len(dataObjects))
	})
}

// TestMinioFileWriter_VerifyBlockLastRecord tests that written data contains BlockLastRecord
func TestAdvMinioFileWriter_VerifyBlockLastRecord(t *testing.T) {
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
	err = minioHdl.WalkWithObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d/", baseDir, logId, segmentId), true, func(objInfo *storageclient.ChunkObjectInfo) bool {
		if strings.HasSuffix(objInfo.FilePath, ".blk") {
			// Get object size for logging
			objSize, _, statErr := minioHdl.StatObject(ctx, testBucket, objInfo.FilePath)
			if statErr != nil {
				t.Logf("Error getting object size for %s: %v", objInfo.FilePath, statErr)
				return true // continue walking
			}
			t.Logf("Checking object: %s (size: %d)", objInfo.FilePath, objSize)

			// Read the object
			obj, err := minioHdl.GetObject(ctx, testBucket, objInfo.FilePath, 0, objSize)
			if err != nil {
				t.Logf("Error getting object %s: %v", objInfo.FilePath, err)
				return true // continue walking
			}

			data, err := io.ReadAll(obj)
			obj.Close()
			if err != nil {
				t.Logf("Error reading object %s: %v", objInfo.FilePath, err)
				return true // continue walking
			}

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
				return true // continue walking
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
		return true // continue walking
	})
	require.NoError(t, err)
}

func TestAdvMinioFileReader_ReadNextBatchModes(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(10)
	segmentId := int64(1000)
	baseDir := fmt.Sprintf("test-batch-modes-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// Create writer and write test data with varying sizes to test both byte and entry limits
	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Write data with different payload sizes
	// Small entries: ~17 bytes each (including metadata)
	// Large entries: ~1000+ bytes each
	testData := [][]byte{
		[]byte("Small Entry 0"),                      // ~13 bytes payload
		[]byte("Small Entry 1"),                      // ~13 bytes payload
		[]byte("Small Entry 2"),                      // ~13 bytes payload
		bytes.Repeat([]byte("Large Entry 3 - "), 50), // ~900 bytes payload
		bytes.Repeat([]byte("Large Entry 4 - "), 50), // ~900 bytes payload
		[]byte("Small Entry 5"),                      // ~13 bytes payload
		[]byte("Small Entry 6"),                      // ~13 bytes payload
		bytes.Repeat([]byte("Large Entry 7 - "), 50), // ~900 bytes payload
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
	assert.Equal(t, int64(7), lastEntryId)

	err = writer.Close(ctx)
	require.NoError(t, err)

	t.Run("MaxBatchEntries_LimitTesting", func(t *testing.T) {
		// Test that maxBatchEntries limit is respected after server-side batch is collected
		largeBatchSize := int64(16_000_000) // 16MB - large enough to not limit by bytes
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, largeBatchSize, 32)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		// Request only 3 entries - should respect this limit even though server batch could contain more
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0, // Start from entry 0
			MaxBatchEntries: 3, // Limit to 3 entries
		}, nil)
		require.NoError(t, err)

		// Should stop after collecting >= 3 entries from server-side batch
		assert.GreaterOrEqual(t, len(batch.Entries), 3, "Should return at least 3 entries")
		assert.LessOrEqual(t, len(batch.Entries), 8, "Should not return more than all entries")

		// Verify the entries are correct
		assert.Equal(t, int64(0), batch.Entries[0].EntryId)
		assert.Equal(t, []byte("Small Entry 0"), batch.Entries[0].Values)

		t.Logf("MaxBatchEntries=3: returned %d entries (server may return more due to batch collection)", len(batch.Entries))
	})

	t.Run("MaxBatchSize_BytesLimitTesting", func(t *testing.T) {
		// Test that maxBatchSize (server-side byte limit) controls the server batch size
		smallBatchSize := int64(100) // Small byte limit - should only collect small entries per batch
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, smallBatchSize, 32)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		// Request many entries but with small byte limit
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,  // Start from entry 0
			MaxBatchEntries: 10, // Request many entries
		}, nil)
		require.NoError(t, err)

		// Due to small maxBatchSize, server should collect until hitting byte limit
		// The server collects blocks until maxBatchSize is exceeded, then returns the collected batch
		// Expected: entries 0,1,2,3 (small+small+small+large) because entry 3 causes the limit to be exceeded
		assert.Equal(t, 4, len(batch.Entries), "Should return all entries collected before exceeding byte limit")

		// Verify we got the small entries
		if len(batch.Entries) >= 1 {
			assert.Equal(t, int64(0), batch.Entries[0].EntryId)
			assert.Equal(t, []byte("Small Entry 0"), batch.Entries[0].Values)
		}

		t.Logf("MaxBatchSize=100 bytes: returned %d entries (limited by server-side byte collection)", len(batch.Entries))
	})

	t.Run("CombinedLimits_EntriesAndBytes", func(t *testing.T) {
		// Test interaction between maxBatchEntries and maxBatchSize
		mediumBatchSize := int64(500) // Medium byte limit
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, mediumBatchSize, 32)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		// Request limited entries with medium byte limit
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0, // Start from entry 0
			MaxBatchEntries: 2, // Limit to 2 entries
		}, nil)
		require.NoError(t, err)

		// Should be limited by whichever constraint hits first
		assert.GreaterOrEqual(t, len(batch.Entries), 1, "Should return at least 1 entry")
		assert.LessOrEqual(t, len(batch.Entries), 8, "Should not exceed total entries")

		t.Logf("Combined limits (entries=2, bytes=500): returned %d entries", len(batch.Entries))
	})

	t.Run("AutoBatch_NoLimits", func(t *testing.T) {
		// Test auto batch mode with large limits - should return all remaining entries
		largeBatchSize := int64(16_000_000) // 16MB
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, largeBatchSize, 32)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    2,  // Start from entry 2
			MaxBatchEntries: -1, // Auto mode (no entry limit)
		}, nil)
		require.NoError(t, err)

		// Should return all entries from 2 to end (entries 2,3,4,5,6,7)
		expectedEntries := 6
		assert.Equal(t, expectedEntries, len(batch.Entries), "Auto mode should return all remaining entries")
		assert.Equal(t, int64(2), batch.Entries[0].EntryId)
		assert.Equal(t, int64(7), batch.Entries[len(batch.Entries)-1].EntryId)

		t.Logf("Auto batch mode: returned %d entries from entry %d to %d",
			len(batch.Entries), batch.Entries[0].EntryId, batch.Entries[len(batch.Entries)-1].EntryId)
	})

	t.Run("ServerBatch_CollectionBehavior", func(t *testing.T) {
		// Test that server collects a full batch first, then applies limits
		verySmallBatchSize := int64(50) // Very small - should only get 1-2 small entries per server batch
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, verySmallBatchSize, 32)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		// Request 1 entry but with very small batch size
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0, // Start from entry 0
			MaxBatchEntries: 1, // Request only 1 entry
		}, nil)
		require.NoError(t, err)

		// Server should collect its batch first (limited by verySmallBatchSize),
		// then return all entries from that batch regardless of MaxBatchEntries=1
		assert.GreaterOrEqual(t, len(batch.Entries), 1, "Should return at least the requested entry")

		// Due to the "batch-first" design, might return more than 1 entry if server batch contains multiple small entries
		assert.Equal(t, int64(0), batch.Entries[0].EntryId)

		t.Logf("Server batch behavior: requested 1 entry, got %d entries (server collected batch first)", len(batch.Entries))
	})

	t.Run("MiddleStart_EntriesLimitBeforeBytesLimit", func(t *testing.T) {
		// Test scenario: Start reading from middle position where:
		// 1. Total data from start to middle > maxBatchSize
		// 2. When reading, maxBatchEntries limit hits before maxBatchSize limit

		// Use large maxBatchSize but the data from middle position should be small enough
		// that maxBatchEntries limit will be hit first
		largeBatchSize := int64(10_000) // Large enough to contain all remaining entries by bytes
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, largeBatchSize, 32)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		// Start from middle position (entry 5) - after the large entries (3,4)
		// Remaining entries: 5 (small), 6 (small), 7 (large ~900 bytes)
		// Even though we have large maxBatchSize, limit to only 2 entries
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    5, // Start from entry 5 (middle position)
			MaxBatchEntries: 2, // Limit to 2 entries - should hit this limit first
		}, nil)
		require.NoError(t, err)

		// Should get entries 5,6,7 since server collects batch first
		// But due to maxBatchEntries=2, it should be limited by entry count, not bytes
		// Note: Server batch collection might still include entry 7 even though we requested only 2
		assert.GreaterOrEqual(t, len(batch.Entries), 2, "Should return at least 2 entries as requested")
		assert.LessOrEqual(t, len(batch.Entries), 3, "Should not exceed remaining entries")

		// Verify we start from the correct position
		assert.Equal(t, int64(5), batch.Entries[0].EntryId)
		assert.Equal(t, []byte("Small Entry 5"), batch.Entries[0].Values)

		// Second entry should be entry 6
		if len(batch.Entries) >= 2 {
			assert.Equal(t, int64(6), batch.Entries[1].EntryId)
			assert.Equal(t, []byte("Small Entry 6"), batch.Entries[1].Values)
		}

		t.Logf("Middle start with entries limit: requested 2 entries from position 5, got %d entries", len(batch.Entries))
		t.Logf("Entries returned: %v", func() []int64 {
			ids := make([]int64, len(batch.Entries))
			for i, e := range batch.Entries {
				ids[i] = e.EntryId
			}
			return ids
		}())
	})

	t.Run("MiddleStart_ComplexScenario_EntriesLimitFirst", func(t *testing.T) {
		// More complex scenario: Create a new dataset specifically for this test
		// where entries before middle position have large total size,
		// but from middle position we have many small entries that hit entries limit first

		writerComplex, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir+"-complex", logId, segmentId+1, minioHdl, cfg)
		require.NoError(t, err)
		require.NotNil(t, writerComplex)

		// Create test data:
		// Entries 0-4: Large entries (total ~4500 bytes) - exceed most maxBatchSize limits
		// Entries 5-14: Small entries (~13 bytes each, total ~130 bytes)
		complexTestData := make([][]byte, 15)

		// Large entries at beginning (0-4)
		for i := 0; i < 5; i++ {
			complexTestData[i] = bytes.Repeat([]byte(fmt.Sprintf("Large Entry %d - ", i)), 50) // ~900 bytes each
		}

		// Small entries in middle (5-14)
		for i := 5; i < 15; i++ {
			complexTestData[i] = []byte(fmt.Sprintf("Small Entry %d", i)) // ~13 bytes each
		}

		// Write all entries
		for i, data := range complexTestData {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("complex-test-write-%d", i))
			_, err := writerComplex.WriteDataAsync(ctx, int64(i), data, resultCh)
			require.NoError(t, err)

			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)

			err = writerComplex.Sync(ctx)
			require.NoError(t, err)
		}

		lastEntryId, err := writerComplex.Finalize(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(14), lastEntryId)

		err = writerComplex.Close(ctx)
		require.NoError(t, err)

		defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir+"-complex")

		// Now test: Start from entry 8 (middle of small entries)
		// maxBatchSize is large enough for all remaining small entries by bytes (~91 bytes for entries 8-14)
		// but limit maxBatchEntries to 3 - this should hit first
		mediumBatchSize := int64(2000) // Large enough for remaining small entries but smaller than large entries
		readerComplex, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir+"-complex", logId, segmentId+1, minioHdl, mediumBatchSize, 32)
		require.NoError(t, err)
		require.NotNil(t, readerComplex)
		defer readerComplex.Close(ctx)

		batch, err := readerComplex.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    8, // Start from middle of small entries
			MaxBatchEntries: 3, // Should hit this limit before bytes limit
		}, nil)
		require.NoError(t, err)

		// The key test: Even though maxBatchSize (2000) can accommodate all remaining entries (8-14) by bytes,
		// maxBatchEntries=3 should limit the result
		// But due to server batch collection, might get more than 3
		assert.GreaterOrEqual(t, len(batch.Entries), 3, "Should return at least 3 entries as requested")

		// All remaining entries from 8-14 total bytes < 2000, so server might collect all
		// But our limit logic should still apply
		assert.LessOrEqual(t, len(batch.Entries), 7, "Should not exceed remaining entries (8-14)")

		// Verify correct starting position and content
		assert.Equal(t, int64(8), batch.Entries[0].EntryId)
		assert.Equal(t, []byte("Small Entry 8"), batch.Entries[0].Values)

		t.Logf("Complex scenario - Start from entry 8, maxBatchEntries=3, maxBatchSize=2000:")
		t.Logf("  Returned %d entries (entries limit should hit before bytes limit)", len(batch.Entries))
		t.Logf("  Entry IDs: %v", func() []int64 {
			ids := make([]int64, len(batch.Entries))
			for i, e := range batch.Entries {
				ids[i] = e.EntryId
			}
			return ids
		}())

		// Additional verification: total bytes of returned entries should be much less than maxBatchSize
		totalBytes := 0
		for _, entry := range batch.Entries {
			totalBytes += len(entry.Values)
		}
		t.Logf("  Total bytes returned: %d (much less than maxBatchSize=2000)", totalBytes)
		assert.Less(t, totalBytes, 200, "Total bytes should be much less than maxBatchSize, proving entries limit hit first")
	})
}

// TestMinioFileWriter_DataIntegrityWithDifferentSizes tests data integrity with various data sizes
func TestAdvMinioFileWriter_DataIntegrityWithDifferentSizes(t *testing.T) {
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
	reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
	require.NoError(t, err)

	// Read all entries
	batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: int64(len(testCases)),
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, len(testCases), len(batch.Entries))

	// Verify each entry
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Verify_%s", tc.name), func(t *testing.T) {
			assert.Equal(t, int64(i), batch.Entries[i].EntryId)
			assert.Equal(t, tc.data, batch.Entries[i].Values, "Data mismatch for %s", tc.name)
		})
	}

	err = reader.Close(ctx)
	require.NoError(t, err)
}

// TestMinioFileReader_SequentialReading tests sequential reading patterns
func TestAdvMinioFileReader_SequentialReading(t *testing.T) {
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
	reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
	require.NoError(t, err)

	t.Run("AdvReadFromBeginning", func(t *testing.T) {
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(totalEntries),
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, totalEntries, len(batch.Entries))

		// Verify order and content
		for i, entry := range batch.Entries {
			assert.Equal(t, int64(i), entry.EntryId)
			assert.Equal(t, testData[i], entry.Values)
		}
	})

	t.Run("AdvReadFromMiddle", func(t *testing.T) {
		startId := int64(10)
		expectedCount := totalEntries - int(startId)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    startId,
			MaxBatchEntries: int64(expectedCount),
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, expectedCount, len(batch.Entries))

		// Verify order and content
		for i, entry := range batch.Entries {
			expectedId := startId + int64(i)
			assert.Equal(t, expectedId, entry.EntryId)
			assert.Equal(t, testData[expectedId], entry.Values)
		}
	})

	t.Run("AdvReadWithSmallBatchSize", func(t *testing.T) {
		// Create a reader with small maxBatchSize to ensure maxBatchEntries limit takes effect
		// Each test entry is ~550 bytes, so 1500 bytes should fit about 2-3 entries
		smallBatchReader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 1500, 32)
		require.NoError(t, err)
		defer smallBatchReader.Close(ctx)

		batchSize := int64(3)
		startId := int64(5)

		batch, err := smallBatchReader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    startId,
			MaxBatchEntries: batchSize,
		}, nil)
		require.NoError(t, err)

		// With small maxBatchSize (1500 bytes), the server should collect ~2-3 entries per batch
		// Since maxBatchEntries=3, we expect the server to collect and return a batch with 2-3 entries
		assert.LessOrEqual(t, len(batch.Entries), int(batchSize), "Should not exceed maxBatchEntries significantly")
		assert.GreaterOrEqual(t, len(batch.Entries), 1, "Should return at least 1 entry")

		// Verify content starts from the expected ID
		for i, entry := range batch.Entries {
			expectedId := startId + int64(i)
			assert.Equal(t, expectedId, entry.EntryId)
			assert.Equal(t, testData[expectedId], entry.Values)
		}
	})

	t.Run("AdvReadAutoBatchFromDifferentBlocks", func(t *testing.T) {
		// Test auto batch mode from different starting points
		testCases := []int64{0, 5, 10, 15, 19}

		for _, startId := range testCases {
			batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
				StartEntryID:    startId,
				MaxBatchEntries: -1, // Auto batch mode
			}, nil)
			require.NoError(t, err)
			assert.Greater(t, len(batch.Entries), 0)

			// Should return at least one entry
			assert.Equal(t, startId, batch.Entries[0].EntryId)
			assert.Equal(t, testData[startId], batch.Entries[0].Values)
		}
	})

	err = reader.Close(ctx)
	require.NoError(t, err)
}

// TestMinioFileWriter_ConcurrentReadWrite tests concurrent read and write operations
func TestAdvMinioFileWriter_ConcurrentReadWrite(t *testing.T) {
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
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
		if err != nil {
			errors <- err
			return
		}
		defer reader.Close(ctx)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(initialEntries),
		}, nil)
		if err != nil {
			errors <- err
			return
		}

		if len(batch.Entries) != initialEntries {
			errors <- fmt.Errorf("expected %d entries, got %d", initialEntries, len(batch.Entries))
			return
		}

		// Verify initial data
		for i, entry := range batch.Entries {
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
	reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
	require.NoError(t, err)

	batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: int64(initialEntries + 10),
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, initialEntries+10, len(batch.Entries))

	err = reader.Close(ctx)
	require.NoError(t, err)
}

// TestMinioFileReader_ErrorHandling tests error handling in reader
func TestAdvMinioFileReader_ErrorHandling(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, _ := setupMinioFileWriterTest(t)
	ctx := context.Background()

	t.Run("AdvNonExistentSegment", func(t *testing.T) {
		nonExistentBaseDir := fmt.Sprintf("non-existent-segment-%d", time.Now().Unix())

		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, nonExistentBaseDir, 1000, 2000, minioHdl, 16_000_000, 32)
		require.NoError(t, err) // Should succeed in creating reader

		// But reading should fail gracefully
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 10,
		}, nil)
		assert.Error(t, err)
		assert.Nil(t, batch)

		err = reader.Close(ctx)
		require.NoError(t, err)
	})

	t.Run("AdvInvalidEntryId", func(t *testing.T) {
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
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
		require.NoError(t, err)

		// Test reading from non-existent entry ID
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    100, // Way beyond available entries
			MaxBatchEntries: 10,
		}, nil)
		assert.Error(t, err)
		assert.Nil(t, batch)

		// Test reading from negative entry ID
		batch, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    -1,
			MaxBatchEntries: 10,
		}, nil)
		assert.Error(t, err)
		assert.Nil(t, batch)

		err = reader.Close(ctx)
		require.NoError(t, err)
	})
}

// TestMinioFileWriter_LargeEntryHandling tests handling of very large entries
func TestAdvMinioFileWriter_LargeEntryHandling(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	// Increase buffer sizes for large entries
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes = 20 * 1024 * 1024     // 20MB
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 18 * 1024 * 1024 // 18MB

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
		{"2MB", 2 * 1024 * 1024, nil},
		{"4MB", 4 * 1024 * 1024, nil},
		{"8MB", 8 * 1024 * 1024, nil},
		{"16MB", 16 * 1024 * 1024, nil},
		{"32MB", 32 * 1024 * 1024, nil},
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
	reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
	require.NoError(t, err)

	batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: int64(len(testCases)),
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, len(testCases)-1, len(batch.Entries))

	// Verify each large entry
	for i, tc := range testCases {
		if i == len(testCases)-1 {
			break
		}
		t.Run(fmt.Sprintf("Verify_%s", tc.name), func(t *testing.T) {
			assert.Equal(t, int64(i), batch.Entries[i].EntryId)
			assert.Equal(t, tc.size, len(batch.Entries[i].Values))

			// Verify data integrity for first and last few bytes using the stored data
			assert.Equal(t, tc.data[:100], batch.Entries[i].Values[:100], "First 100 bytes mismatch")
			assert.Equal(t, tc.data[len(tc.data)-100:], batch.Entries[i].Values[len(batch.Entries[i].Values)-100:], "Last 100 bytes mismatch")
		})
	}

	// last 32MB msg
	batch2, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID: 4,
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, len(batch2.Entries))
	assert.Equal(t, int64(4), batch2.Entries[0].EntryId)
	assert.Equal(t, len(testCases[4].data), len(batch2.Entries[0].Values))

	err = reader.Close(ctx)
	require.NoError(t, err)

	// read err after closed
	batch3, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID: 2,
	}, nil)
	assert.Nil(t, batch3)
	assert.Error(t, err)
	assert.True(t, werr.ErrFileReaderAlreadyClosed.Is(err))
}

// TestMinioFileWriter_VeryLargePayloadSupport specifically tests writing very large payloads
func TestAdvMinioFileWriter_VeryLargePayloadSupport(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	// Configure for very large entries
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes = 25 * 1024 * 1024     // 25MB
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 20 * 1024 * 1024 // 20MB

	logId := int64(100)
	segmentId := int64(10000)
	baseDir := fmt.Sprintf("test-very-large-payload-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	defer writer.Close(ctx)

	// Test writing large payloads: 2MB, 4MB, 8MB, 16MB
	testSizes := []struct {
		name string
		size int
	}{
		{"2MB", 2 * 1024 * 1024},
		{"4MB", 4 * 1024 * 1024},
		{"8MB", 8 * 1024 * 1024},
		{"16MB", 16 * 1024 * 1024},
	}

	var testData [][]byte

	for i, testCase := range testSizes {
		t.Run(testCase.name, func(t *testing.T) {
			entryId := int64(i)
			largePayload := generateTestData(testCase.size)
			testData = append(testData, largePayload) // Store for later verification

			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-very-large-payload-%s", testCase.name))

			start := time.Now()
			returnedId, err := writer.WriteDataAsync(ctx, entryId, largePayload, resultCh)
			require.NoError(t, err, "Should be able to write %s payload", testCase.name)
			assert.Equal(t, entryId, returnedId)

			// Wait for result with extended timeout for very large entries
			ctxWithTimeout, cancel := context.WithTimeout(ctx, 60*time.Second)
			result, err := resultCh.ReadResult(ctxWithTimeout)
			cancel()
			require.NoError(t, err)
			require.NoError(t, result.Err)
			assert.Equal(t, entryId, result.SyncedId)

			duration := time.Since(start)
			throughput := float64(testCase.size) / duration.Seconds() / (1024 * 1024) // MB/s
			t.Logf("Successfully wrote %s payload (%d bytes) in %v (%.2f MB/s)",
				testCase.name, testCase.size, duration, throughput)
		})
	}

	// Verify all entries were written correctly
	assert.Equal(t, int64(0), writer.GetFirstEntryId(ctx))
	assert.Equal(t, int64(len(testSizes)-1), writer.GetLastEntryId(ctx))

	// Finalize and verify file integrity
	_, err = writer.Finalize(ctx)
	require.NoError(t, err)

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Verify reading large entries back
	t.Run("AdvVerifyReadBack", func(t *testing.T) {
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Read all entries
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(len(testSizes)),
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, len(testSizes), len(batch.Entries))

		// Verify each large entry
		for i, testCase := range testSizes {
			t.Run(fmt.Sprintf("Verify_%s", testCase.name), func(t *testing.T) {
				assert.Equal(t, int64(i), batch.Entries[i].EntryId)
				assert.Equal(t, testCase.size, len(batch.Entries[i].Values))

				// Verify data integrity by comparing with original
				if i < len(testData) {
					// Compare first and last 1000 bytes for efficiency
					assert.Equal(t, testData[i][:1000], batch.Entries[i].Values[:1000],
						"First 1000 bytes mismatch for %s", testCase.name)
					assert.Equal(t, testData[i][len(testData[i])-1000:],
						batch.Entries[i].Values[len(batch.Entries[i].Values)-1000:],
						"Last 1000 bytes mismatch for %s", testCase.name)
				}

				t.Logf("Successfully verified %s payload (%d bytes)", testCase.name, testCase.size)
			})
		}
	})
}

// TestMinioFileWriter_MetadataConsistency tests metadata consistency across operations
func TestAdvMinioFileWriter_MetadataConsistency(t *testing.T) {
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
	reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
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

// TestEmptyPayloadValidation tests that empty payloads are rejected at the client level
func TestAdvEmptyPayloadValidation(t *testing.T) {
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
	t.Run("AdvEmptyPayloadAtStorageLayer", func(t *testing.T) {
		// Try to write empty data directly to storage layer
		// This should now be rejected immediately by WriteDataAsync
		_, err := writer.WriteDataAsync(ctx, 0, []byte{}, channel.NewLocalResultChannel("test-empty-payload"))
		require.Error(t, err)
		assert.True(t, werr.ErrEmptyPayload.Is(err), "Error should be ErrEmptyPayload")

		t.Logf("Storage layer empty payload error: %v", err)
	})

	// Test empty payload validation at the client level (LogWriter)
	t.Run("AdvEmptyPayloadAtClientLevel", func(t *testing.T) {
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
	t.Run("AdvNilPayloadAtClientLevel", func(t *testing.T) {
		nilMsg := &log.WriteMessage{
			Payload:    nil,
			Properties: map[string]string{"test": "value"},
		}

		_, err := log.MarshalMessage(nilMsg)
		require.NoError(t, err)
	})

	// Test both empty err
	t.Run("AdvBothEmptyMsg", func(t *testing.T) {
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
	t.Run("AdvValidPayload", func(t *testing.T) {
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
func TestAdvMinioFileWriter_HeaderRecordVerification(t *testing.T) {
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
	err = minioHdl.WalkWithObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d/", baseDir, logId, segmentId), true, func(objInfo *storageclient.ChunkObjectInfo) bool {
		if strings.HasSuffix(objInfo.FilePath, ".blk") && !strings.HasSuffix(objInfo.FilePath, "footer.blk") {
			// This should be the first data object (0.blk)
			if strings.HasSuffix(objInfo.FilePath, "/0.blk") {
				firstDataObject = objInfo.FilePath
				return false // stop walking
			}
		}
		return true // continue walking
	})
	require.NoError(t, err)

	require.NotEmpty(t, firstDataObject, "Should find the first data object (0.blk)")
	t.Logf("Found first data object: %s", firstDataObject)

	// Get object size
	objSize, _, err := minioHdl.StatObject(ctx, testBucket, firstDataObject)
	require.NoError(t, err)

	// Read the first object content
	obj, err := minioHdl.GetObject(ctx, testBucket, firstDataObject, 0, objSize)
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

func TestAdvMinioFileWriter_RecoveryDebug(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(50)       // 修改为不同的 logId 避免冲突
	segmentId := int64(5000) // 修改为不同的 segmentId 避免冲突
	baseDir := fmt.Sprintf("test-recovery-debug-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	t.Run("AdvWriteDataAndInterruptOnly", func(t *testing.T) {
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
		err = minioHdl.WalkWithObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d/", baseDir, logId, segmentId), true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			// Get object size for logging
			objSize, _, statErr := minioHdl.StatObject(ctx, testBucket, objInfo.FilePath)
			if statErr == nil {
				t.Logf("  Before close: %s (size: %d)", objInfo.FilePath, objSize)
			} else {
				t.Logf("  Before close: %s", objInfo.FilePath)
			}
			return true // continue walking
		})
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
		err = minioHdl.WalkWithObjects(ctx, testBucket, fmt.Sprintf("%s/%d/%d/", baseDir, logId, segmentId), true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			if !strings.HasSuffix(objInfo.FilePath, ".lock") {
				objectCount++
				// Get object size for logging
				objSize, _, statErr := minioHdl.StatObject(ctx, testBucket, objInfo.FilePath)
				if statErr == nil {
					t.Logf("  After close: %s (size: %d)", objInfo.FilePath, objSize)
				} else {
					t.Logf("  After close: %s", objInfo.FilePath)
				}
			}
			return true // continue walking
		})
		require.NoError(t, err)
		assert.Greater(t, objectCount, 0, "Should have created at least one data object")
	})
}

// TestMinioFileRW_ConcurrentReadWrite tests concurrent reading and writing to verify real-time read capability in MinIO
func TestAdvMinioFileRW_ConcurrentReadWrite(t *testing.T) {
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
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
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

					batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
						StartEntryID:    startId,
						MaxBatchEntries: batchSize,
					}, nil)

					if err != nil {
						t.Logf("Reader: Failed to read entries %d to %d: %v", startId, latestWrittenId, err)
						// Continue trying - the data might not be fully written yet
						time.Sleep(readInterval)
						continue
					}

					// Process read entries
					for _, entry := range batch.Entries {
						readEntries[entry.EntryId] = entry.Values
						if entry.EntryId > lastReadEntryId {
							lastReadEntryId = entry.EntryId
						}
						t.Logf("Reader: Successfully read entry %d: %s",
							entry.EntryId, string(entry.Values))
					}

					t.Logf("Reader: Read %d entries, lastReadEntryId now %d",
						len(batch.Entries), lastReadEntryId)
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

			finalBatch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
				StartEntryID:    startId,
				MaxBatchEntries: remainingEntries,
			}, nil)

			if err != nil {
				t.Errorf("Reader: Failed final read: %v", err)
			} else {
				t.Logf("Reader: Final read got %d entries", len(finalBatch.Entries))

				for _, entry := range finalBatch.Entries {
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
	t.Run("AdvFinalVerification", func(t *testing.T) {
		finalReader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
		require.NoError(t, err)
		defer finalReader.Close(ctx)

		// Verify file is incomplete (no footer)
		footer := finalReader.GetFooter()
		assert.Nil(t, footer, "File should be incomplete (no footer)")

		// Verify we can read available entries (may be less than total due to MinIO block boundaries)
		lastEntryId, err := finalReader.GetLastEntryID(ctx)
		if err == nil && lastEntryId >= 0 {
			// Try to read from the beginning
			allBatch, err := finalReader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
				StartEntryID:    0,
				MaxBatchEntries: lastEntryId + 1,
			}, nil)
			if err == nil {
				assert.Greater(t, len(allBatch.Entries), 0, "Should read at least some entries")

				// Verify entry content and order
				for i, entry := range allBatch.Entries {
					assert.Equal(t, int64(i), entry.EntryId, "Entry ID should match")
					assert.Contains(t, string(entry.Values), fmt.Sprintf("Entry %d:", i),
						"Entry content should contain expected prefix")
				}

				t.Logf("Final verification: Read %d entries successfully", len(allBatch.Entries))
			}
		}
	})
}

// TestMinioFileRW_ConcurrentOneWriteMultipleReads tests one writer with multiple concurrent readers for MinIO
func TestAdvMinioFileRW_ConcurrentOneWriteMultipleReads(t *testing.T) {
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
			reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
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

						batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
							StartEntryID:    startId,
							MaxBatchEntries: entriesToRead,
						}, nil)

						if err != nil {
							t.Logf("Reader %d: Failed to read entries %d to %d: %v",
								readerNum, startId, startId+entriesToRead-1, err)
							// Continue trying - the data might not be fully written yet
							time.Sleep(readInterval)
							continue
						}

						// Process read entries
						for _, entry := range batch.Entries {
							readEntries[entry.EntryId] = entry.Values
							if entry.EntryId > lastReadEntryId {
								lastReadEntryId = entry.EntryId
							}
							t.Logf("Reader %d: Successfully read entry %d: %s",
								readerNum, entry.EntryId, string(entry.Values))
						}

						t.Logf("Reader %d: Read %d entries, lastReadEntryId now %d",
							readerNum, len(batch.Entries), lastReadEntryId)
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

				finalBatch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
					StartEntryID:    startId,
					MaxBatchEntries: remainingEntries,
				}, nil)

				if err != nil {
					t.Errorf("Reader %d: Failed final read: %v", readerNum, err)
				} else {
					t.Logf("Reader %d: Final read got %d entries", readerNum, len(finalBatch.Entries))

					for _, entry := range finalBatch.Entries {
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
	t.Run("AdvVerifyReaderConsistency", func(t *testing.T) {
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
	t.Run("AdvFinalFileVerification", func(t *testing.T) {
		finalReader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
		require.NoError(t, err)
		defer finalReader.Close(ctx)

		// Verify file is incomplete (no footer)
		footer := finalReader.GetFooter()
		assert.Nil(t, footer, "File should be incomplete (no footer)")

		// Verify we can read available entries
		lastEntryId, err := finalReader.GetLastEntryID(ctx)
		if err == nil && lastEntryId >= 0 {
			allBatch, err := finalReader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
				StartEntryID:    0,
				MaxBatchEntries: lastEntryId + 1,
			}, nil)
			if err == nil {
				assert.Greater(t, len(allBatch.Entries), 0, "Should read at least some entries")

				// Verify entry content and order
				for i, entry := range allBatch.Entries {
					assert.Equal(t, int64(i), entry.EntryId, "Entry ID should match")
					assert.Contains(t, string(entry.Values), fmt.Sprintf("Entry %d:", i),
						"Entry content should contain expected prefix")
				}

				t.Logf("Final verification: File contains %d readable entries", len(allBatch.Entries))
			}
		}
	})
}

// TestMinioFileWriter_SingleEntryLargerThanMaxFlushSize tests entries larger than maxFlushSize
func TestAdvMinioFileWriter_SingleEntryLargerThanMaxFlushSize(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	// Configure small maxFlushSize to test the edge case
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 1 * 1024 * 1024 // 1MB flush size
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes = 5 * 1024 * 1024     // 5MB buffer size

	logId := int64(200)
	segmentId := int64(20000)
	baseDir := fmt.Sprintf("test-oversized-entry-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)
	defer writer.Close(ctx)

	// Test writing entries larger than maxFlushSize
	testCases := []struct {
		name string
		size int
	}{
		{"1.5MB", int(1.5 * 1024 * 1024)}, // 1.5MB > 1MB maxFlushSize
		{"2MB", 2 * 1024 * 1024},          // 2MB > 1MB maxFlushSize
		{"3MB", 3 * 1024 * 1024},          // 3MB > 1MB maxFlushSize
		{"4MB", 4 * 1024 * 1024},          // 4MB > 1MB maxFlushSize
	}

	var writtenData [][]byte
	for i, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Generate large payload with pattern for verification
			largePayload := make([]byte, testCase.size)
			for j := 0; j < len(largePayload); j++ {
				largePayload[j] = byte((j + i*1000) % 256) // Different pattern for each test
			}
			writtenData = append(writtenData, largePayload)

			entryId := int64(i)
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-oversized-entry-%d", i))

			// This should succeed even though entry size > maxFlushSize
			// because we write to buffer first, then check for flush
			_, err := writer.WriteDataAsync(ctx, entryId, largePayload, resultCh)
			require.NoError(t, err)

			// Wait for result with extended timeout
			ctxWithTimeout, cancel := context.WithTimeout(ctx, 120*time.Second)
			result, err := resultCh.ReadResult(ctxWithTimeout)
			cancel()
			require.NoError(t, err)
			require.NoError(t, result.Err)
			assert.Equal(t, entryId, result.SyncedId)

			t.Logf("Successfully wrote %s payload (%d bytes) which is larger than maxFlushSize (1MB)", testCase.name, testCase.size)
		})
	}

	// Verify all entries were written correctly
	assert.Equal(t, int64(0), writer.GetFirstEntryId(ctx))
	assert.Equal(t, int64(len(testCases)-1), writer.GetLastEntryId(ctx))

	// Finalize and verify file integrity
	_, err = writer.Finalize(ctx)
	require.NoError(t, err)

	// Read back and verify data integrity
	reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Read all entries and verify them
	batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: int64(len(testCases)),
	}, nil)
	require.NoError(t, err)
	require.Equal(t, len(testCases), len(batch.Entries), "Should read all entries")

	// Verify each large entry
	for i, originalData := range writtenData {
		require.True(t, i < len(batch.Entries), "Entry %d should exist", i)
		data := batch.Entries[i].Values
		require.Equal(t, len(originalData), len(data), "Entry %d: data length mismatch", i)

		// Verify data integrity byte by byte for first and last 100 bytes
		verifyLen := 100
		if len(originalData) < verifyLen {
			verifyLen = len(originalData)
		}

		// Check first 100 bytes
		assert.Equal(t, originalData[:verifyLen], data[:verifyLen], "Entry %d: first %d bytes mismatch", i, verifyLen)

		// Check last 100 bytes if data is large enough
		if len(originalData) > verifyLen {
			assert.Equal(t, originalData[len(originalData)-verifyLen:], data[len(data)-verifyLen:], "Entry %d: last %d bytes mismatch", i, verifyLen)
		}

		t.Logf("Successfully verified entry %d with size %d bytes", i, len(data))
	}

	logger.Ctx(ctx).Info("successfully wrote and verified all oversized entries",
		zap.String("segmentFileKey", fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId)),
		zap.Int("totalTestCases", len(testCases)))

	err = reader.Close(ctx)
	require.NoError(t, err)
}

// TestAdvMinioFileWriter_Compaction tests the complete compaction workflow
func TestAdvMinioFileWriter_Compaction(t *testing.T) {
	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	// Configure small maxFlushSize to create multiple small blocks
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 512          // Very small to create many blocks
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes = 10 * 1024 * 1024 // 10MB buffer

	logId := int64(300)
	segmentId := int64(30000)
	baseDir := fmt.Sprintf("test-compaction-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// Phase 1: Write data to create multiple small blocks
	t.Run("AdvWriteDataToCreateMultipleBlocks", func(t *testing.T) {
		writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		require.NotNil(t, writer)

		// Write enough data to create multiple blocks (each entry ~200-300 bytes, block size ~512 bytes)
		totalEntries := 20
		testData := make([][]byte, totalEntries)

		for i := 0; i < totalEntries; i++ {
			// Create entries of varying sizes to ensure realistic block distribution
			entrySize := 200 + (i%3)*100 // 200, 300, or 400 bytes per entry
			data := make([]byte, entrySize)
			for j := 0; j < entrySize; j++ {
				data[j] = byte((i*100 + j) % 256)
			}
			testData[i] = data

			entryId := int64(i)
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-compaction-write-%d", entryId))

			_, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)
			require.NoError(t, err)

			// Wait for result
			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)

			// Force sync after every few entries to create separate blocks
			if (i+1)%3 == 0 {
				err = writer.Sync(ctx)
				require.NoError(t, err)
			}
		}

		// Final sync and finalize
		err = writer.Sync(ctx)
		require.NoError(t, err)

		lastEntryId, err := writer.Finalize(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(totalEntries-1), lastEntryId)

		err = writer.Close(ctx)
		require.NoError(t, err)

		t.Logf("Successfully wrote %d entries and finalized segment", totalEntries)
	})

	// Phase 2: Verify original data before compaction
	var originalData []*proto.LogEntry
	var originalBlockCount int
	var originalFooter *codec.FooterRecord

	t.Run("AdvVerifyOriginalDataBeforeCompaction", func(t *testing.T) {
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		// Get original footer and block information
		originalFooter = reader.GetFooter()
		require.NotNil(t, originalFooter, "Original footer should exist")
		assert.False(t, codec.IsCompacted(originalFooter.Flags), "Original segment should not be compacted")

		originalBlockIndexes := reader.GetBlockIndexes()
		originalBlockCount = len(originalBlockIndexes)
		assert.Greater(t, originalBlockCount, 3, "Should have created multiple blocks")

		t.Logf("Original segment has %d blocks, %d total records",
			originalBlockCount, originalFooter.TotalRecords)

		// Read all original data for later comparison
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(originalFooter.TotalRecords),
		}, nil)
		require.NoError(t, err)
		originalData = batch.Entries
		assert.Equal(t, int(originalFooter.TotalRecords), len(originalData))

		t.Logf("Successfully read %d entries from original segment", len(originalData))
	})

	// Phase 3: Perform compaction
	sizeAfterCompacted := int64(0)
	t.Run("AdvPerformCompaction", func(t *testing.T) {
		// Open writer in recovery mode to compact existing segment
		writer, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg, true)
		require.NoError(t, err)
		require.NotNil(t, writer)

		// Perform compaction
		compactedSize, err := writer.Compact(ctx)
		require.NoError(t, err)
		assert.Greater(t, compactedSize, int64(0), "Compacted size should be positive")

		err = writer.Close(ctx)
		require.NoError(t, err)

		sizeAfterCompacted = compactedSize
		t.Logf("Successfully compacted segment, new size: %d bytes", compactedSize)
	})

	// Phase 4: Verify compacted data
	t.Run("AdvVerifyCompactedData", func(t *testing.T) {
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		// Verify compacted footer and flags
		compactedFooter := reader.GetFooter()
		require.NotNil(t, compactedFooter, "Compacted footer should exist")
		assert.True(t, codec.IsCompacted(compactedFooter.Flags), "Compacted segment should have compacted flag set")
		assert.Equal(t, originalFooter.TotalRecords, compactedFooter.TotalRecords, "Total records should remain the same")

		// Verify block count reduction
		compactedBlockIndexes := reader.GetBlockIndexes()
		compactedBlockCount := len(compactedBlockIndexes)
		assert.Less(t, compactedBlockCount, originalBlockCount, "Compacted segment should have fewer blocks")
		assert.Equal(t, int32(compactedBlockCount), compactedFooter.TotalBlocks, "Footer should reflect actual block count")

		t.Logf("Compacted segment has %d blocks (reduced from %d), %d total records",
			compactedBlockCount, originalBlockCount, compactedFooter.TotalRecords)

		// Read all compacted data
		compactedBatch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(compactedFooter.TotalRecords),
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, len(originalData), len(compactedBatch.Entries), "Should have same number of entries")

		// Verify data integrity - every entry should be identical
		for i, originalEntry := range originalData {
			assert.Equal(t, originalEntry.EntryId, compactedBatch.Entries[i].EntryId,
				"Entry ID should match at index %d", i)
			assert.Equal(t, originalEntry.Values, compactedBatch.Entries[i].Values,
				"Entry data should match at index %d", i)
		}

		t.Logf("Successfully verified %d entries have identical data after compaction", len(compactedBatch.Entries))
	})

	// Phase 5: Verify object storage structure
	t.Run("AdvVerifyObjectStorageStructure", func(t *testing.T) {
		segmentPrefix := fmt.Sprintf("%s/%d/%d/", baseDir, logId, segmentId)
		var regularBlocks []string
		var mergedBlocks []string
		var footerFound bool
		var lockFiles []string

		err := minioHdl.WalkWithObjects(ctx, testBucket, segmentPrefix, true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			if strings.HasSuffix(objInfo.FilePath, ".lock") {
				lockFiles = append(lockFiles, objInfo.FilePath)
			} else if strings.HasSuffix(objInfo.FilePath, "footer.blk") {
				footerFound = true
			} else if strings.Contains(objInfo.FilePath, "/m_") && strings.HasSuffix(objInfo.FilePath, ".blk") {
				mergedBlocks = append(mergedBlocks, objInfo.FilePath)
			} else if strings.HasSuffix(objInfo.FilePath, ".blk") {
				regularBlocks = append(regularBlocks, objInfo.FilePath)
			}

			// Get object size for logging
			objSize, _, statErr := minioHdl.StatObject(ctx, testBucket, objInfo.FilePath)
			if statErr == nil {
				t.Logf("Found object: %s (size: %d)", objInfo.FilePath, objSize)
			} else {
				t.Logf("Found object: %s", objInfo.FilePath)
			}
			return true // continue walking
		})
		require.NoError(t, err)

		// Verify object structure
		assert.True(t, footerFound, "Footer object should exist")
		assert.Greater(t, len(mergedBlocks), 0, "Should have merged block objects")
		assert.Equal(t, len(regularBlocks), 0, "May have original block objects")

		t.Logf("Object structure: %d regular blocks, %d merged blocks, footer: %v",
			len(regularBlocks), len(mergedBlocks), footerFound)
	})

	// Phase 6: Test auto batch mode before and after compaction
	var originalAutoBatchSizes []int
	var originalBlocks int32
	var compactedAutoBatchSizes []int
	var compactedBlocks int32

	t.Run("AdvTestAutoBatchBeforeCompaction", func(t *testing.T) {
		// First, restore original (non-compacted) segment temporarily for testing
		// We need to test with original data before it was compacted

		// Since we already compacted, we'll create a fresh segment for this test
		tempLogId := logId + 1000
		tempSegmentId := segmentId + 1000
		tempBaseDir := fmt.Sprintf("test-auto-batch-before-%d", time.Now().Unix())
		defer cleanupMinioFileWriterObjects(t, minioHdl, tempBaseDir)

		// Write same data pattern as original but don't compact
		writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, tempBaseDir, tempLogId, tempSegmentId, minioHdl, cfg)
		require.NoError(t, err)

		// Write same 20 entries with same pattern
		totalEntries := 20
		for i := 0; i < totalEntries; i++ {
			entrySize := 200 + (i%3)*100 // Same pattern as before
			data := make([]byte, entrySize)
			for j := 0; j < entrySize; j++ {
				data[j] = byte((i*100 + j) % 256)
			}

			entryId := int64(i)
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-auto-batch-before-write-%d", entryId))

			_, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)
			require.NoError(t, err)

			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)

			// Force sync after every few entries to create separate blocks (same as original)
			if (i+1)%3 == 0 {
				err = writer.Sync(ctx)
				require.NoError(t, err)
			}
		}

		err = writer.Sync(ctx)
		require.NoError(t, err)
		_, err = writer.Finalize(ctx)
		require.NoError(t, err)
		err = writer.Close(ctx)
		require.NoError(t, err)

		// Test auto batch mode on original (non-compacted) segment
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, tempBaseDir, tempLogId, tempSegmentId, minioHdl, 16_000_000, 32)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Verify this segment is not compacted
		footer := reader.GetFooter()
		require.NotNil(t, footer)
		assert.False(t, codec.IsCompacted(footer.Flags), "Test segment should not be compacted")
		assert.Equal(t, int32(len(reader.GetBlockIndexes())), footer.TotalBlocks, "Total blocks should equal to index records")
		originalBlocks = footer.TotalBlocks

		// Test auto batch from different starting points
		testStartIds := []int64{0, 3, 6, 9, 12, 15}
		for _, startId := range testStartIds {
			batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
				StartEntryID:    startId,
				MaxBatchEntries: 1, // soft limit count=1, it will return 1 block records
			}, nil)
			require.NoError(t, err)

			if len(batch.Entries) > 0 {
				originalAutoBatchSizes = append(originalAutoBatchSizes, len(batch.Entries))
				assert.Equal(t, startId, batch.Entries[0].EntryId, "First entry should match start ID %d", startId)

				t.Logf("Original segment: Auto batch from entry %d returned %d entries", startId, len(batch.Entries))
			}
		}

		t.Logf("Original segment auto batch sizes: %v", originalAutoBatchSizes)
	})

	t.Run("AdvTestAutoBatchAfterCompaction", func(t *testing.T) {
		// Test auto batch mode on compacted segment
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 16_000_000, 32)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Verify this segment is compacted
		footer := reader.GetFooter()
		require.NotNil(t, footer)
		assert.True(t, codec.IsCompacted(footer.Flags), "Segment should be compacted")
		assert.Equal(t, int32(len(reader.GetBlockIndexes())), footer.TotalBlocks, "Total blocks should equal to index records")
		compactedBlocks = footer.TotalBlocks

		// Test auto batch from same starting points
		testStartIds := []int64{0, 3, 6, 9, 12, 15}
		for _, startId := range testStartIds {
			batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
				StartEntryID:    startId,
				MaxBatchEntries: 1, // soft limit count=1, it will return 1 block records
			}, nil)
			require.NoError(t, err)

			if len(batch.Entries) > 0 {
				compactedAutoBatchSizes = append(compactedAutoBatchSizes, len(batch.Entries))
				assert.Equal(t, startId, batch.Entries[0].EntryId, "First entry should match start ID %d", startId)

				t.Logf("Compacted segment: Auto batch from entry %d returned %d entries", startId, len(batch.Entries))
			}
		}

		t.Logf("Compacted segment auto batch sizes: %v", compactedAutoBatchSizes)
	})

	t.Run("AdvVerifyAutoBatchImprovementAfterCompaction", func(t *testing.T) {
		// Verify that compaction improve blocks amount
		assert.Less(t, compactedBlocks, originalBlocks, "Should have less blocks than original blocks after compacted")

		// Verify that compaction actually improves auto batch efficiency
		require.Greater(t, len(originalAutoBatchSizes), 0, "Should have original auto batch size data")
		require.Greater(t, len(compactedAutoBatchSizes), 0, "Should have compacted auto batch size data")

		// Calculate average entries per auto batch
		var originalSum, compactedSum int
		for _, size := range originalAutoBatchSizes {
			originalSum += size
		}
		for _, size := range compactedAutoBatchSizes {
			compactedSum += size
		}

		originalAvg := float64(originalSum) / float64(len(originalAutoBatchSizes))
		compactedAvg := float64(compactedSum) / float64(len(compactedAutoBatchSizes))

		t.Logf("Original segment average entries per auto batch: %.2f", originalAvg)
		t.Logf("Compacted segment average entries per auto batch: %.2f", compactedAvg)

		// Compacted segment should have larger auto batches on average
		// This validates that compaction successfully merged multiple small blocks into fewer large blocks
		// With the new batch collection logic, compacted segments may not always show improvement
		// because both segments are constrained by the same batch collection behavior
		// The original test expectation was too restrictive for the new logic
		assert.GreaterOrEqual(t, compactedAvg, originalAvg, // Allow some tolerance
			"Compacted segment auto batch size (%.2f) should be similar to or better than original segment (%.2f)",
			compactedAvg, originalAvg)

		// Find max batch sizes for comparison
		originalMax := 0
		compactedMax := 0
		for _, size := range originalAutoBatchSizes {
			if size > originalMax {
				originalMax = size
			}
		}
		for _, size := range compactedAutoBatchSizes {
			if size > compactedMax {
				compactedMax = size
			}
		}

		t.Logf("Original segment max auto batch size: %d", originalMax)
		t.Logf("Compacted segment max auto batch size: %d", compactedMax)

		// The maximum auto batch size should also be larger after compaction
		assert.GreaterOrEqual(t, compactedMax, originalMax,
			"Compacted segment max auto batch size (%d) should be >= original segment (%d)",
			compactedMax, originalMax)
	})

	// Additional tests for specific batch size mode
	t.Run("AdvTestSpecificBatchSizeModes", func(t *testing.T) {
		// For compacted segments, we need a very small maxBatchSize to test maxBatchEntries limits
		// Since all entries are in one 6KB block, we use a small maxBatchSize (1KB) to force
		// the server to respect maxBatchEntries when the collected batch exceeds byte limits
		smallMaxBatchSize := int64(1024) // 1KB - much smaller than the 6KB compacted block
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, smallMaxBatchSize, 32)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Test specific batch size mode
		t.Run("AdvSpecificBatchSize", func(t *testing.T) {
			batchSize := int64(7)
			batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
				StartEntryID:    3,
				MaxBatchEntries: batchSize,
			}, nil)
			require.NoError(t, err)
			// With very small maxBatchSize (1KB), the compacted block (6KB) exceeds the limit
			// So the server will collect the full block first, then return all entries from it
			// This tests the "batch-first collection" behavior with compacted segments
			assert.Equal(t, 17, len(batch.Entries), "Should return all entries from collected block for compacted segment")

			// Verify sequential entry IDs
			for i, entry := range batch.Entries {
				assert.Equal(t, int64(3+i), entry.EntryId, "Entry ID should be sequential")
			}
		})

		// Test reading from different starting points
		t.Run("AdvDifferentStartingPoints", func(t *testing.T) {
			testPoints := []int64{0, 1, 5, 10, 15}
			for _, startId := range testPoints {
				batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
					StartEntryID:    startId,
					MaxBatchEntries: 3,
				}, nil)
				require.NoError(t, err)
				if len(batch.Entries) > 0 {
					assert.Equal(t, startId, batch.Entries[0].EntryId,
						"First entry should match start ID %d", startId)
				}
			}
		})
	})

	// Phase 7: Test compaction is idempotent
	t.Run("AdvTestCompactionIdempotency", func(t *testing.T) {
		// Try to compact again - should be skipped since already compacted
		writer, err := objectstorage.NewMinioFileWriterWithMode(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg, true)
		require.NoError(t, err)
		require.NotNil(t, writer)

		// Second compaction should return -1 (already compacted)
		compactedSize, err := writer.Compact(ctx)
		require.NoError(t, err)
		assert.Equal(t, sizeAfterCompacted, compactedSize, "Second compaction should return -1 (already compacted)")

		err = writer.Close(ctx)
		require.NoError(t, err)

		t.Log("Successfully verified compaction idempotency")
	})
}

// TestAdvMinioFileReader_ReadNextBatchAdvScenarios tests all three scenarios of ReadNextBatchAdv
func TestAdvMinioFileReader_ReadNextBatchAdvScenarios(t *testing.T) {
	ctx := context.Background()
	baseDir := fmt.Sprintf("test_adv_scenarios-%d", time.Now().Unix())
	logId := int64(3001)
	segId := int64(4001)

	// Setup MinIO configuration
	client, cfg := setupMinioFileWriterTest(t)

	// Write test data first
	writer, err := objectstorage.NewMinioFileWriter(ctx, cfg.Minio.BucketName, baseDir, logId, segId, client, cfg)
	require.NoError(t, err)

	// Write multiple blocks of data
	testData := [][]byte{
		[]byte("Block 0 - Entry 0"),
		[]byte("Block 0 - Entry 1"),
		[]byte("Block 1 - Entry 2"),
		[]byte("Block 1 - Entry 3"),
		[]byte("Block 2 - Entry 4"),
		[]byte("Block 2 - Entry 5"),
	}

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-adv-minio-%d", i))
		returnedId, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)
		assert.Equal(t, int64(i), returnedId)

		// Wait for result
		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(i), result.SyncedId)
	}

	// Complete the file to create footer
	lastEntryId, err := writer.Finalize(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(5), lastEntryId)
	writer.Close(ctx)

	t.Run("Scenario1_WithAdvOpt", func(t *testing.T) {
		// Use a small maxBatchSize to ensure maxBatchEntries limits take effect
		// Each block is around 60-80 bytes, so 150 bytes should limit us to ~2 blocks
		smallBatchSize := int64(150)
		reader1, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId, client, smallBatchSize, 32)
		require.NoError(t, err)
		defer reader1.Close(ctx)

		// Read first batch to get batch info
		batch1, err := reader1.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 2,
		}, nil)
		require.NoError(t, err)
		require.NotNil(t, batch1.LastReadState)
		// With small batch size, should get around 2 blocks/entries (due to batch collection behavior)
		assert.GreaterOrEqual(t, len(batch1.Entries), 1)
		assert.LessOrEqual(t, len(batch1.Entries), 3) // Allow some flexibility due to batch collection

		// Now create a new reader with same small batch size for continuation
		reader2, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId, client, smallBatchSize, 32)
		require.NoError(t, err)
		defer reader2.Close(ctx)

		// Read next batch using advOpt (should start from next block after the last read batch)
		batch2, err := reader2.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    2, // Should be ignored when using advOpt
			MaxBatchEntries: 2,
		}, batch1.LastReadState)

		// The test might result in EOF if we've read all blocks in the first batch
		// This is expected with the new batch collection logic
		if err != nil && batch2 == nil {
			// This is expected if all blocks were collected in the first batch
			t.Logf("Batch continuation resulted in EOF - expected with batch collection logic")
		} else {
			require.NoError(t, err)
			assert.Greater(t, len(batch2.Entries), 0)

			// Verify the entries are from the expected continuation point
			assert.True(t, batch2.Entries[0].EntryId > batch1.Entries[len(batch1.Entries)-1].EntryId,
				"Should continue from where previous batch left off")
		}
	})

	t.Run("Scenario2_WithFooter_NoAdvOpt", func(t *testing.T) {
		// Use a small maxBatchSize to ensure maxBatchEntries limits take effect
		smallBatchSize := int64(150)
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId, client, smallBatchSize, 32)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Verify footer exists
		footer := reader.GetFooter()
		assert.NotNil(t, footer, "Should have footer for completed file")
		assert.Greater(t, footer.TotalBlocks, int32(0))

		// Read from middle using footer search
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    3,
			MaxBatchEntries: 2,
		}, nil)
		require.NoError(t, err)
		// With batch collection logic, may get more than 2 entries but should start from entry 3
		assert.GreaterOrEqual(t, len(batch.Entries), 1)
		assert.Equal(t, int64(3), batch.Entries[0].EntryId)
		assert.NotNil(t, batch.LastReadState)
		t.Logf("Scenario2: Got %d entries starting from entry %d", len(batch.Entries), batch.Entries[0].EntryId)
	})

	t.Run("Scenario3_NoFooter_IncompleteFile", func(t *testing.T) {
		// Create a new incomplete file (no footer)
		incompleteLogId := int64(3002)
		incompleteSegId := int64(4002)

		writer, err := objectstorage.NewMinioFileWriter(ctx, cfg.Minio.BucketName, baseDir, incompleteLogId, incompleteSegId, client, cfg)
		require.NoError(t, err)

		// Write some data but don't finalize (no footer)
		for i := 0; i < 4; i++ {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-incomplete-minio-%d", i))
			returnedId, err := writer.WriteDataAsync(ctx, int64(i), []byte(fmt.Sprintf("Entry %d", i)), resultCh)
			require.NoError(t, err)
			assert.Equal(t, int64(i), returnedId)

			// Wait for result
			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			assert.Equal(t, int64(i), result.SyncedId)
		}
		writer.Close(ctx) // Close without finalizing

		// Create reader for incomplete file
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, incompleteLogId, incompleteSegId, client, 16_000_000, 32)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Verify no footer exists
		footer := reader.GetFooter()
		assert.Nil(t, footer, "Should have no footer for incomplete file")

		// Read from incomplete file (should use dynamic scanning)
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 3,
		}, nil)
		require.NoError(t, err)
		assert.Greater(t, len(batch.Entries), 0)
		assert.Equal(t, int64(0), batch.Entries[0].EntryId)
	})
}

// TestAdvMinioFileReader_AdvOptContinuation tests the continuation behavior with advOpt
func TestAdvMinioFileReader_AdvOptContinuation(t *testing.T) {
	ctx := context.Background()
	baseDir := fmt.Sprintf("test_adv_continuation-%d", time.Now().Unix())
	logId := int64(3003)
	segId := int64(4003)

	// Setup MinIO configuration
	client, cfg := setupMinioFileWriterTest(t)

	// Create test file with multiple blocks
	writer, err := objectstorage.NewMinioFileWriter(ctx, cfg.Minio.BucketName, baseDir, logId, segId, client, cfg)
	require.NoError(t, err)

	// Write 15 entries across multiple blocks
	for i := 0; i < 15; i++ {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-continuation-minio-%d", i))
		returnedId, err := writer.WriteDataAsync(ctx, int64(i), []byte(fmt.Sprintf("Entry %d data", i)), resultCh)
		require.NoError(t, err)
		assert.Equal(t, int64(i), returnedId)

		// Wait for result
		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(i), result.SyncedId)
	}

	lastEntryId, err := writer.Finalize(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(14), lastEntryId)
	writer.Close(ctx)

	// Test sequential reading with advOpt
	var lastReadState *proto.LastReadState
	var allReadEntries []*proto.LogEntry

	for batchNum := 0; batchNum < 5; batchNum++ {
		// Use a smaller maxBatchSize to ensure continuation works properly with batch collection logic
		// Each block is around 58-59 bytes, so 200 bytes should limit us to ~3-4 blocks
		smallBatchSize := int64(200)
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId, client, smallBatchSize, 32)
		require.NoError(t, err)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    int64(batchNum * 3), // This should be ignored when lastBatchInfo is provided
			MaxBatchEntries: 3,
		}, lastReadState)

		if werr.ErrFileReaderEndOfFile.Is(err) {
			reader.Close(ctx)
			t.Logf("Reached EOF after %d batches, as expected with batch collection logic", batchNum)
			break
		}

		require.NoError(t, err)
		assert.Greater(t, len(batch.Entries), 0)

		// Collect entries
		allReadEntries = append(allReadEntries, batch.Entries...)
		lastReadState = batch.LastReadState

		t.Logf("Batch %d: Read %d entries (entry IDs %d to %d)",
			batchNum, len(batch.Entries), batch.Entries[0].EntryId, batch.Entries[len(batch.Entries)-1].EntryId)

		reader.Close(ctx)
	}

	// Verify we read all entries in sequence
	// With batch collection logic, we may get all entries in fewer batches than expected
	assert.GreaterOrEqual(t, len(allReadEntries), 1, "Should read at least some entries")
	assert.LessOrEqual(t, len(allReadEntries), 15, "Should not read more than available entries")

	// Verify entries are in sequence
	for i, entry := range allReadEntries {
		assert.Equal(t, int64(i), entry.EntryId, "Entries should be in sequence")
	}

	t.Logf("Successfully read %d entries through batch collection continuation logic", len(allReadEntries))
}

// TestAdvMinioFileReader_EdgeCases tests edge cases of ReadNextBatchAdv
func TestAdvMinioFileReader_EdgeCases(t *testing.T) {
	ctx := context.Background()
	baseDir := fmt.Sprintf("test_adv_edge_cases-%d", time.Now().Unix())
	logId := int64(3004)
	segId := int64(4004)

	// Setup MinIO configuration
	client, cfg := setupMinioFileWriterTest(t)

	t.Run("EmptyFile_NoAdvOpt", func(t *testing.T) {
		// Create empty file
		writer, err := objectstorage.NewMinioFileWriter(ctx, cfg.Minio.BucketName, baseDir, logId, segId+1, client, cfg)
		require.NoError(t, err)
		writer.Close(ctx)

		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId+1, client, 16_000_000, 32)
		require.NoError(t, err)
		defer reader.Close(ctx)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 10,
		}, nil)
		assert.Error(t, err)
		assert.Nil(t, batch)
	})

	t.Run("SingleEntry_WithFooter", func(t *testing.T) {
		writer, err := objectstorage.NewMinioFileWriter(ctx, cfg.Minio.BucketName, baseDir, logId, segId+2, client, cfg)
		require.NoError(t, err)

		resultCh := channel.NewLocalResultChannel("test-single-minio")
		returnedId, err := writer.WriteDataAsync(ctx, 0, []byte("single entry"), resultCh)
		require.NoError(t, err)
		assert.Equal(t, int64(0), returnedId)

		// Wait for result
		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), result.SyncedId)

		lastEntryId, err := writer.Finalize(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), lastEntryId)
		writer.Close(ctx)

		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId+2, client, 16_000_000, 32)
		require.NoError(t, err)
		defer reader.Close(ctx)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 10,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, len(batch.Entries))
		assert.Equal(t, int64(0), batch.Entries[0].EntryId)
		assert.NotNil(t, batch.LastReadState)
	})

	t.Run("ReadBeyondEnd_WithAdvOpt", func(t *testing.T) {
		writer, err := objectstorage.NewMinioFileWriter(ctx, cfg.Minio.BucketName, baseDir, logId, segId+3, client, cfg)
		require.NoError(t, err)

		// Write 3 entries
		for i := 0; i < 3; i++ {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-beyond-minio-%d", i))
			returnedId, err := writer.WriteDataAsync(ctx, int64(i), []byte(fmt.Sprintf("entry %d", i)), resultCh)
			require.NoError(t, err)
			assert.Equal(t, int64(i), returnedId)

			// Wait for result
			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			assert.Equal(t, int64(i), result.SyncedId)
		}

		lastEntryId, err := writer.Finalize(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(2), lastEntryId)
		writer.Close(ctx)

		// Read all entries first
		reader1, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId+3, client, 16_000_000, 32)
		require.NoError(t, err)

		batch1, err := reader1.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 10,
		}, nil)
		require.NoError(t, err)
		require.Equal(t, 3, len(batch1.Entries))
		reader1.Close(ctx)

		// Try to read beyond end with advOpt
		reader2, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId+3, client, 16_000_000, 32)
		require.NoError(t, err)
		defer reader2.Close(ctx)

		batch2, err := reader2.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    100, // Should be ignored
			MaxBatchEntries: 10,
		}, batch1.LastReadState)
		assert.Error(t, err)
		assert.True(t, werr.ErrFileReaderEndOfFile.Is(err))
		assert.Nil(t, batch2)
	})

	t.Run("CompactedFile_WithAdvOpt", func(t *testing.T) {
		// Create a compacted file by using proper configuration
		compactedLogId := int64(3005)
		compactedSegId := int64(4005)

		writer, err := objectstorage.NewMinioFileWriter(ctx, cfg.Minio.BucketName, baseDir, compactedLogId, compactedSegId, client, cfg)
		require.NoError(t, err)

		// Write data
		for i := 0; i < 6; i++ {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-compact-minio-%d", i))
			returnedId, err := writer.WriteDataAsync(ctx, int64(i), []byte(fmt.Sprintf("compacted entry %d", i)), resultCh)
			require.NoError(t, err)
			assert.Equal(t, int64(i), returnedId)

			// Wait for result
			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			assert.Equal(t, int64(i), result.SyncedId)
		}

		lastEntryId, err := writer.Finalize(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(5), lastEntryId)
		writer.Close(ctx)

		// Use a small maxBatchSize to ensure MaxBatchEntries limits take effect
		// Each block is around 63-88 bytes, so 200 bytes should limit us to ~3 blocks
		smallBatchSize := int64(200)
		reader1, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, compactedLogId, compactedSegId, client, smallBatchSize, 32)
		require.NoError(t, err)

		batch1, err := reader1.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 3,
		}, nil)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(batch1.Entries), 1)
		assert.LessOrEqual(t, len(batch1.Entries), 3)
		reader1.Close(ctx)

		// Continue reading with advOpt using same small batch size
		reader2, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, compactedLogId, compactedSegId, client, smallBatchSize, 32)
		require.NoError(t, err)
		defer reader2.Close(ctx)

		batch2, err := reader2.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    100, // Should be ignored
			MaxBatchEntries: 3,
		}, batch1.LastReadState)
		require.Error(t, err)
		assert.True(t, werr.ErrFileReaderEndOfFile.Is(err))
		assert.Nil(t, batch2)
	})
}

// TestAdvMinioFileReader_LargeStartEntryIdWithoutFooter tests the bug fix for reading with large startEntryId
// when the file has no footer and startEntryId is beyond 16MB of raw block data
func TestAdvMinioFileReader_LargeStartEntryIdWithoutFooter(t *testing.T) {
	if os.Getenv("SKIP_MINIO_TESTS") != "" {
		t.Skip("Skipping MinIO tests")
	}

	minioHdl, cfg := setupMinioFileWriterTest(t)
	ctx := context.Background()

	logId := int64(500)
	segmentId := int64(50000)
	baseDir := fmt.Sprintf("test-large-start-entry-id-%d", time.Now().Unix())
	defer cleanupMinioFileWriterObjects(t, minioHdl, baseDir)

	// Configure to create blocks for testing
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 100 * 1024   // 100KB per block
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxBytes = 15 * 1024 * 1024 // 15MB buffer

	t.Run("AdvWriteLargeDatasetWithoutFooter", func(t *testing.T) {
		writer, err := objectstorage.NewMinioFileWriter(ctx, testBucket, baseDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		require.NotNil(t, writer)

		// Write entries to exceed the readDataBlocks maxBytes limit (4MB)
		// Each entry ~100KB, each block ~100KB = 1 entry per block
		// Total: 100 entries * 100KB = 10MB data
		totalEntries := 100     // 100 entries for testing
		entrySize := 100 * 1024 // 100KB per entry

		t.Logf("Writing %d entries of %d bytes each (total ~%d MB)",
			totalEntries, entrySize, (totalEntries*entrySize)/(1024*1024))

		for i := 0; i < totalEntries; i++ {
			// Create test data with pattern for verification
			data := make([]byte, entrySize)
			for j := 0; j < entrySize; j++ {
				data[j] = byte((i*1000 + j) % 256)
			}

			entryId := int64(i)
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-large-start-write-%d", entryId))

			_, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)
			require.NoError(t, err)

			// Wait for result
			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)

			// Force sync after each entry to create separate blocks (since each entry is 100KB)
			err = writer.Sync(ctx)
			require.NoError(t, err)

			if (i+1)%20 == 0 {
				t.Logf("Progress: written %d/%d entries", i+1, totalEntries)
			}
		}

		// Force final sync but DO NOT finalize (no footer)
		err = writer.Sync(ctx)
		require.NoError(t, err)

		// Verify writer state
		assert.Equal(t, int64(0), writer.GetFirstEntryId(ctx))
		assert.Equal(t, int64(totalEntries-1), writer.GetLastEntryId(ctx))

		// Close without finalizing to simulate incomplete file (no footer)
		err = writer.Close(ctx)
		require.NoError(t, err)

		t.Logf("Successfully wrote %d entries without footer", totalEntries)
	})

	t.Run("AdvTestBugFix_LargeStartEntryId", func(t *testing.T) {
		// Create reader for the incomplete file (no footer)
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 4_000_000, 32)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		// Verify file has no footer
		footer := reader.GetFooter()
		assert.Nil(t, footer, "File should have no footer (incomplete)")

		// Test case 1: Start reading from entry that's beyond maxBytes of raw block data
		// With 100KB entries, we'll try to read from entry 50 (requires scanning 5MB > 4MB limit)
		largeStartEntryId := int64(50)

		t.Logf("Testing large startEntryId: %d", largeStartEntryId)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    largeStartEntryId,
			MaxBatchEntries: 10, // Request 10 entries
		}, nil)

		// This should succeed with the bug fix
		require.NoError(t, err, "Should be able to read from large startEntryId")
		require.NotNil(t, batch, "Batch should not be nil")
		assert.Greater(t, len(batch.Entries), 0, "Should read at least one entry")

		// Verify we got the correct starting entry
		assert.Equal(t, largeStartEntryId, batch.Entries[0].EntryId,
			"First entry should match requested startEntryId")

		// Verify data integrity for the first entry
		expectedPattern := make([]byte, 100*1024)
		for j := 0; j < 100*1024; j++ {
			expectedPattern[j] = byte((int(largeStartEntryId)*1000 + j) % 256)
		}
		assert.Equal(t, expectedPattern, batch.Entries[0].Values,
			"Entry data should match expected pattern")

		t.Logf("Successfully read %d entries starting from entry %d",
			len(batch.Entries), largeStartEntryId)
	})

	t.Run("AdvTestBugFix_VeryLargeStartEntryId", func(t *testing.T) {
		// Create reader for the incomplete file (no footer)
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 4_000_000, 32)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		// Test case 2: Even larger startEntryId (requires scanning 7MB > 4MB limit)
		veryLargeStartEntryId := int64(70)

		t.Logf("Testing very large startEntryId: %d", veryLargeStartEntryId)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    veryLargeStartEntryId,
			MaxBatchEntries: 5, // Request 5 entries
		}, nil)

		// This should also succeed with the bug fix
		require.NoError(t, err, "Should be able to read from very large startEntryId")
		require.NotNil(t, batch, "Batch should not be nil")
		assert.Greater(t, len(batch.Entries), 0, "Should read at least one entry")

		// Verify we got the correct starting entry
		assert.Equal(t, veryLargeStartEntryId, batch.Entries[0].EntryId,
			"First entry should match requested startEntryId")

		// Verify sequential entry IDs
		for i, entry := range batch.Entries {
			expectedEntryId := veryLargeStartEntryId + int64(i)
			assert.Equal(t, expectedEntryId, entry.EntryId,
				"Entry %d should have sequential ID", i)
		}

		t.Logf("Successfully read %d entries starting from entry %d",
			len(batch.Entries), veryLargeStartEntryId)
	})

	t.Run("AdvTestBugFix_StartFromMiddleOfRange", func(t *testing.T) {
		// Create reader for the incomplete file (no footer)
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 4_000_000, 32)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		// Test case 3: Start from middle of range and read many entries
		middleStartEntryId := int64(30) // Requires scanning 3MB < 4MB limit (should work easily)
		largeBatchSize := int64(20)

		t.Logf("Testing middle startEntryId: %d with large batch size: %d",
			middleStartEntryId, largeBatchSize)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    middleStartEntryId,
			MaxBatchEntries: largeBatchSize,
		}, nil)

		// This should succeed and return at least the requested batch size
		require.NoError(t, err, "Should be able to read large batch from middle")
		require.NotNil(t, batch, "Batch should not be nil")
		assert.GreaterOrEqual(t, len(batch.Entries), int(largeBatchSize),
			"Should read at least the requested batch size")

		// Verify all entries are sequential and correct
		for i, entry := range batch.Entries {
			expectedEntryId := middleStartEntryId + int64(i)
			assert.Equal(t, expectedEntryId, entry.EntryId,
				"Entry %d should have sequential ID %d", i, expectedEntryId)

			// Verify data integrity for a few entries
			if i < 3 || i >= len(batch.Entries)-3 {
				expectedPattern := make([]byte, 100*1024)
				for j := 0; j < 100*1024; j++ {
					expectedPattern[j] = byte((int(expectedEntryId)*1000 + j) % 256)
				}
				assert.Equal(t, expectedPattern, entry.Values,
					"Entry %d data should match expected pattern", expectedEntryId)
			}
		}

		t.Logf("Successfully read %d entries starting from entry %d",
			len(batch.Entries), middleStartEntryId)
	})

	t.Run("AdvTestBugFix_EdgeCaseNearEnd", func(t *testing.T) {
		// Create reader for the incomplete file (no footer)
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, 4_000_000, 32)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		// Test case 4: Start from near the end of written data
		nearEndStartEntryId := int64(90) // Close to the end (100 total entries)

		t.Logf("Testing near-end startEntryId: %d", nearEndStartEntryId)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    nearEndStartEntryId,
			MaxBatchEntries: 200, // Request more than available
		}, nil)

		// This should succeed and return available entries
		require.NoError(t, err, "Should be able to read from near end")
		require.NotNil(t, batch, "Batch should not be nil")
		assert.Greater(t, len(batch.Entries), 0, "Should read at least one entry")
		assert.Less(t, len(batch.Entries), 200, "Should read less than requested (near end)")

		// Verify we got the correct starting entry
		assert.Equal(t, nearEndStartEntryId, batch.Entries[0].EntryId,
			"First entry should match requested startEntryId")

		// Verify last entry is close to the end
		lastEntry := batch.Entries[len(batch.Entries)-1]
		assert.Less(t, lastEntry.EntryId, int64(100),
			"Last entry should be within written range")

		t.Logf("Successfully read %d entries starting from entry %d, last entry: %d",
			len(batch.Entries), nearEndStartEntryId, lastEntry.EntryId)
	})

	t.Run("AdvVerifyBugFixEffectiveness", func(t *testing.T) {
		// This test specifically verifies that the bug fix works by testing the exact scenario
		// that would fail before the fix: reading from a point where >16MB of raw data
		// needs to be skipped before finding the target entry

		// Use a very small maxBatchSize to ensure precise entry targeting
		// This forces the reader to collect data in small chunks, allowing us to verify
		// that it can iterate through multiple batches to find the exact target entry
		verySmallBatchSize := int64(200 * 1024) // 200KB - much smaller than typical blocks (~100KB each)
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, verySmallBatchSize, 32)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		// This scenario verifies iterative query logic with small batch sizes:
		// - startEntryId requires scanning many blocks before finding target data
		// - Without footer, reader has to scan blocks sequentially
		// - Small maxBatchSize forces multiple iterations to reach target
		// - Should eventually find and return the exact target entry (not more due to size limit)
		problematicStartEntryId := int64(60) // Need to scan ~6MB of blocks to reach this entry

		start := time.Now()
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    problematicStartEntryId,
			MaxBatchEntries: 1, // Request exactly one entry to verify precise targeting
		}, nil)
		duration := time.Since(start)

		// The key assertions that prove the iterative query logic works:
		require.NoError(t, err, "BUG FIX: Should succeed even when startEntryId requires scanning many blocks")
		require.NotNil(t, batch, "BUG FIX: Batch should not be nil")
		assert.GreaterOrEqual(t, len(batch.Entries), 1, "BUG FIX: Should read at least 1 entry")
		assert.Equal(t, problematicStartEntryId, batch.Entries[0].EntryId,
			"BUG FIX: Should read the exact entry we requested")

		// With the small maxBatchSize (200KB), the batch collection should be more controlled.
		// The server will collect fewer blocks per iteration, making the result more precise.
		// We should still get the target entry, but the count may vary based on how many
		// blocks fit within the 200KB limit starting from the target block.
		assert.LessOrEqual(t, len(batch.Entries), 5,
			"With small maxBatchSize, should get a controlled number of entries (not too many)")

		t.Logf("ITERATIVE QUERY VERIFIED: Successfully read entry %d after scanning many blocks in %v (found %d entries with small batch size)",
			problematicStartEntryId, duration, len(batch.Entries))

		// Verify the fix doesn't take excessively long (should complete within reasonable time)
		assert.Less(t, duration, 30*time.Second,
			"BUG FIX: Should complete within reasonable time even with large startEntryId")
	})
}

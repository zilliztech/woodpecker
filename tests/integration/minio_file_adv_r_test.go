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

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
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
	t.Run("AdvRecoverAndContinueWriting", func(t *testing.T) {
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

	t.Run("AdvVerifyOriginalSegmentObjects", func(t *testing.T) {
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

	t.Run("AdvVerifyFinalSegmentState", func(t *testing.T) {
		// Create reader to verify the final segment state
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, recoverySegmentFileKey, logId, segmentId, minioHdl, nil)
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
			StartEntryID: 0,
			BatchSize:    20, // Read all entries
		})
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

func TestAdvMinioFileReader_ReadNextBatchModes(t *testing.T) {
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
	reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
	require.NoError(t, err)
	require.NotNil(t, reader)

	t.Run("AdvAutoBatchMode_BatchSizeNegativeOne", func(t *testing.T) {
		// Test auto batch mode (BatchSize = -1): should return only entries from one block
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 2,  // Start from entry 2 (which should be in block 2)
			BatchSize:    -1, // Auto batch mode
		})
		require.NoError(t, err)

		// Should return all entries from start point to end (entries 2, 3, 4)
		assert.Equal(t, 3, len(batch.Entries), "Auto batch mode should return all entries from start point")
		assert.Equal(t, int64(2), batch.Entries[0].EntryId)
		assert.Equal(t, int64(3), batch.Entries[1].EntryId)
		assert.Equal(t, int64(4), batch.Entries[2].EntryId)
		assert.Equal(t, []byte("Block 2 - Entry 2"), batch.Entries[0].Values)

		t.Logf("Auto batch mode returned %d entries starting from entry %d", len(batch.Entries), batch.Entries[0].EntryId)
	})

	t.Run("AdvSpecifiedBatchMode_BatchSize3", func(t *testing.T) {
		// Test specified batch size mode: should return exactly 3 entries across multiple blocks
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 1, // Start from entry 1
			BatchSize:    3, // Request exactly 3 entries
		})
		require.NoError(t, err)

		// Should return exactly 3 entries (entries 1, 2, 3) across multiple blocks
		assert.Equal(t, 3, len(batch.Entries), "Specified batch mode should return exactly 3 entries")
		assert.Equal(t, int64(1), batch.Entries[0].EntryId)
		assert.Equal(t, int64(2), batch.Entries[1].EntryId)
		assert.Equal(t, int64(3), batch.Entries[2].EntryId)
		assert.Equal(t, []byte("Block 1 - Entry 1"), batch.Entries[0].Values)
		assert.Equal(t, []byte("Block 2 - Entry 2"), batch.Entries[1].Values)
		assert.Equal(t, []byte("Block 3 - Entry 3"), batch.Entries[2].Values)

		t.Logf("Specified batch mode returned %d entries from entry %d to %d",
			len(batch.Entries), batch.Entries[0].EntryId, batch.Entries[len(batch.Entries)-1].EntryId)
	})

	t.Run("AdvSpecifiedBatchMode_BatchSize2", func(t *testing.T) {
		// Test specified batch size mode: should return exactly 2 entries
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 3, // Start from entry 3
			BatchSize:    2, // Request exactly 2 entries
		})
		require.NoError(t, err)

		// Should return exactly 2 entries (entries 3, 4)
		assert.Equal(t, 2, len(batch.Entries), "Specified batch mode should return exactly 2 entries")
		assert.Equal(t, int64(3), batch.Entries[0].EntryId)
		assert.Equal(t, int64(4), batch.Entries[1].EntryId)
		assert.Equal(t, []byte("Block 3 - Entry 3"), batch.Entries[0].Values)
		assert.Equal(t, []byte("Block 4 - Entry 4"), batch.Entries[1].Values)

		t.Logf("Specified batch mode returned %d entries from entry %d to %d",
			len(batch.Entries), batch.Entries[0].EntryId, batch.Entries[len(batch.Entries)-1].EntryId)
	})

	t.Run("AdvAutoBatchMode_StartFromFirstEntry", func(t *testing.T) {
		// Test auto batch mode starting from first entry
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 0,  // Start from entry 0 (first entry)
			BatchSize:    -1, // Auto batch mode
		})
		require.NoError(t, err)

		// Should return all entries from start point (entries 0, 1, 2, 3, 4)
		assert.Equal(t, 5, len(batch.Entries), "Auto batch mode should return all entries from start point")
		assert.Equal(t, int64(0), batch.Entries[0].EntryId)
		assert.Equal(t, []byte("Block 0 - Entry 0"), batch.Entries[0].Values)

		t.Logf("Auto batch mode from first entry returned %d entries", len(batch.Entries))
	})

	err = reader.Close(ctx)
	require.NoError(t, err)
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
	reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
	require.NoError(t, err)

	// Read all entries
	batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID: 0,
		BatchSize:    int64(len(testCases)),
	})
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
	reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
	require.NoError(t, err)

	t.Run("AdvReadFromBeginning", func(t *testing.T) {
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    int64(totalEntries),
		})
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
			StartEntryID: startId,
			BatchSize:    int64(expectedCount),
		})
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
		batchSize := int64(3)
		startId := int64(5)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: startId,
			BatchSize:    batchSize,
		})
		require.NoError(t, err)
		assert.Equal(t, int(batchSize), len(batch.Entries))

		// Verify content
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
				StartEntryID: startId,
				BatchSize:    -1, // Auto batch mode
			})
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
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
		if err != nil {
			errors <- err
			return
		}
		defer reader.Close(ctx)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    int64(initialEntries),
		})
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
	reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
	require.NoError(t, err)

	batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID: 0,
		BatchSize:    int64(initialEntries + 10),
	})
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

		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, nonExistentBaseDir, 1000, 2000, minioHdl, nil)
		require.NoError(t, err) // Should succeed in creating reader

		// But reading should fail gracefully
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    10,
		})
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
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
		require.NoError(t, err)

		// Test reading from non-existent entry ID
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 100, // Way beyond available entries
			BatchSize:    10,
		})
		assert.Error(t, err)
		assert.Nil(t, batch)

		// Test reading from negative entry ID
		batch, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: -1,
			BatchSize:    10,
		})
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
	reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
	require.NoError(t, err)

	batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID: 0,
		BatchSize:    int64(len(testCases)),
	})
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
	err = reader.Close(ctx)
	require.NoError(t, err)

	// last 32MB msg
	batch2, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID: 4,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, len(batch2.Entries))
	assert.Equal(t, int64(4), batch2.Entries[0].EntryId)
	assert.Equal(t, len(testCases[4].data), len(batch2.Entries[0].Values))
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
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Read all entries
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    int64(len(testSizes)),
		})
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
	reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
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

		emptyMsg := &log.WriterMessage{
			Payload:    []byte{},
			Properties: map[string]string{"test": "value"},
		}

		_, err := log.MarshalMessage(emptyMsg)
		require.NoError(t, err)
	})

	// Test nil payload validation
	t.Run("AdvNilPayloadAtClientLevel", func(t *testing.T) {
		nilMsg := &log.WriterMessage{
			Payload:    nil,
			Properties: map[string]string{"test": "value"},
		}

		_, err := log.MarshalMessage(nilMsg)
		require.NoError(t, err)
	})

	// Test both empty err
	t.Run("AdvBothEmptyMsg", func(t *testing.T) {
		nilMsg := &log.WriterMessage{
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
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
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
				StartEntryID: startId,
				BatchSize:    remainingEntries,
			})

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
		finalReader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
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
				StartEntryID: 0,
				BatchSize:    lastEntryId + 1,
			})
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
			reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
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
					StartEntryID: startId,
					BatchSize:    remainingEntries,
				})

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
		finalReader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
		require.NoError(t, err)
		defer finalReader.Close(ctx)

		// Verify file is incomplete (no footer)
		footer := finalReader.GetFooter()
		assert.Nil(t, footer, "File should be incomplete (no footer)")

		// Verify we can read available entries
		lastEntryId, err := finalReader.GetLastEntryID(ctx)
		if err == nil && lastEntryId >= 0 {
			allBatch, err := finalReader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
				StartEntryID: 0,
				BatchSize:    lastEntryId + 1,
			})
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
	reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Read all entries and verify them
	batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID: 0,
		BatchSize:    int64(len(testCases)),
	})
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
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
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
			StartEntryID: 0,
			BatchSize:    int64(originalFooter.TotalRecords),
		})
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
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
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
			StartEntryID: 0,
			BatchSize:    int64(compactedFooter.TotalRecords),
		})
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
		segmentPrefix := fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId)
		objectCh := minioHdl.ListObjects(ctx, testBucket, segmentPrefix, true, minio.ListObjectsOptions{})

		var regularBlocks []string
		var mergedBlocks []string
		var footerFound bool
		var lockFiles []string

		for object := range objectCh {
			require.NoError(t, object.Err)

			if strings.HasSuffix(object.Key, ".lock") {
				lockFiles = append(lockFiles, object.Key)
			} else if strings.HasSuffix(object.Key, "footer.blk") {
				footerFound = true
			} else if strings.Contains(object.Key, "/m_") && strings.HasSuffix(object.Key, ".blk") {
				mergedBlocks = append(mergedBlocks, object.Key)
			} else if strings.HasSuffix(object.Key, ".blk") {
				regularBlocks = append(regularBlocks, object.Key)
			}

			t.Logf("Found object: %s (size: %d)", object.Key, object.Size)
		}

		// Verify object structure
		assert.True(t, footerFound, "Footer object should exist")
		assert.Greater(t, len(mergedBlocks), 0, "Should have merged block objects")
		assert.GreaterOrEqual(t, len(regularBlocks), 0, "May have original block objects")

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
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, tempBaseDir, tempLogId, tempSegmentId, minioHdl, nil)
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
				StartEntryID: startId,
				BatchSize:    1, // soft limit count=1, it will return 1 block records
			})
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
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
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
				StartEntryID: startId,
				BatchSize:    1, // soft limit count=1, it will return 1 block records
			})
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
		assert.Greater(t, compactedAvg, originalAvg,
			"Compacted segment should have larger auto batches (%.2f) than original segment (%.2f)",
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
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, testBucket, baseDir, logId, segmentId, minioHdl, nil)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Test specific batch size mode
		t.Run("AdvSpecificBatchSize", func(t *testing.T) {
			batchSize := int64(7)
			batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
				StartEntryID: 3,
				BatchSize:    batchSize,
			})
			require.NoError(t, err)
			assert.Equal(t, int(batchSize), len(batch.Entries), "Should read exact batch size")

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
					StartEntryID: startId,
					BatchSize:    3,
				})
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
		// First, create a reader without advOpt to get some data
		reader1, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId, client, nil)
		require.NoError(t, err)
		defer reader1.Close(ctx)

		// Read first batch to get batch info
		batch1, err := reader1.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    2,
		})
		require.NoError(t, err)
		require.NotNil(t, batch1.LastBatchInfo)
		assert.Equal(t, 2, len(batch1.Entries))

		// Now create a new reader with advOpt from the previous batch
		reader2, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId, client, batch1.LastBatchInfo)
		require.NoError(t, err)
		defer reader2.Close(ctx)

		// Read next batch using advOpt (should start from next block)
		batch2, err := reader2.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 2, // Should be ignored when using advOpt
			BatchSize:    2,
		})
		require.NoError(t, err)
		assert.Greater(t, len(batch2.Entries), 0)

		// Verify the entries are from the expected continuation point
		assert.True(t, batch2.Entries[0].EntryId >= 2, "Should continue from where previous batch left off")
	})

	t.Run("Scenario2_WithFooter_NoAdvOpt", func(t *testing.T) {
		// Create reader without advOpt (should use footer)
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId, client, nil)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Verify footer exists
		footer := reader.GetFooter()
		assert.NotNil(t, footer, "Should have footer for completed file")
		assert.Greater(t, footer.TotalBlocks, int32(0))

		// Read from middle using footer search
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 3,
			BatchSize:    2,
		})
		require.NoError(t, err)
		assert.Equal(t, int64(3), batch.Entries[0].EntryId)
		assert.NotNil(t, batch.LastBatchInfo)
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
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, incompleteLogId, incompleteSegId, client, nil)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Verify no footer exists
		footer := reader.GetFooter()
		assert.Nil(t, footer, "Should have no footer for incomplete file")

		// Read from incomplete file (should use dynamic scanning)
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    3,
		})
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
	var lastBatchInfo *storage.BatchInfo
	var allReadEntries []*proto.LogEntry

	for batchNum := 0; batchNum < 5; batchNum++ {
		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId, client, lastBatchInfo)
		require.NoError(t, err)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: int64(batchNum * 3), // This should be ignored when lastBatchInfo is provided
			BatchSize:    3,
		})

		if err == werr.ErrFileReaderEndOfFile {
			reader.Close(ctx)
			break
		}

		require.NoError(t, err)
		assert.Greater(t, len(batch.Entries), 0)

		// Collect entries
		allReadEntries = append(allReadEntries, batch.Entries...)
		lastBatchInfo = batch.LastBatchInfo

		reader.Close(ctx)
	}

	// Verify we read all entries in sequence
	assert.Equal(t, 15, len(allReadEntries))
	for i, entry := range allReadEntries {
		assert.Equal(t, int64(i), entry.EntryId)
	}
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

		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId+1, client, nil)
		require.NoError(t, err)
		defer reader.Close(ctx)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    10,
		})
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

		reader, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId+2, client, nil)
		require.NoError(t, err)
		defer reader.Close(ctx)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    10,
		})
		require.NoError(t, err)
		assert.Equal(t, 1, len(batch.Entries))
		assert.Equal(t, int64(0), batch.Entries[0].EntryId)
		assert.NotNil(t, batch.LastBatchInfo)
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
		reader1, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId+3, client, nil)
		require.NoError(t, err)

		batch1, err := reader1.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    10,
		})
		require.NoError(t, err)
		require.Equal(t, 3, len(batch1.Entries))
		reader1.Close(ctx)

		// Try to read beyond end with advOpt
		reader2, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, logId, segId+3, client, batch1.LastBatchInfo)
		require.NoError(t, err)
		defer reader2.Close(ctx)

		batch2, err := reader2.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 100, // Should be ignored
			BatchSize:    10,
		})
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

		// Read first batch
		reader1, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, compactedLogId, compactedSegId, client, nil)
		require.NoError(t, err)

		batch1, err := reader1.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    3,
		})
		require.NoError(t, err)
		assert.Equal(t, 3, len(batch1.Entries))
		reader1.Close(ctx)

		// Continue reading with advOpt
		reader2, err := objectstorage.NewMinioFileReaderAdv(ctx, cfg.Minio.BucketName, baseDir, compactedLogId, compactedSegId, client, batch1.LastBatchInfo)
		require.NoError(t, err)
		defer reader2.Close(ctx)

		batch2, err := reader2.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID: 100, // Should be ignored
			BatchSize:    3,
		})
		require.Error(t, err)
		assert.True(t, werr.ErrFileReaderEndOfFile.Is(err))
		assert.Nil(t, batch2)
	})
}

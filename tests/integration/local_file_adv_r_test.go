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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/disk"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func TestReadAndPrint(t *testing.T) {
	t.Skipf("for debug only")

	cfg, _ := config.NewConfiguration("../../config/woodpecker.yaml")
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	var LastBatchInfo *storage.BatchInfo
	for {
		reader, err := disk.NewLocalFileReaderAdv(context.TODO(), "/tmp/wp/", 3926, 1, 16_000_000)
		require.NoError(t, err)
		batch, readErr := reader.ReadNextBatchAdv(context.TODO(), storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 100,
		}, LastBatchInfo)
		if readErr != nil {
			break
		}
		t.Logf("Read %d entries", len(batch.Entries))
		for i, entry := range batch.Entries {
			t.Logf("Entry %d: %d/%d", i, entry.SegId, entry.EntryId)
		}
		LastBatchInfo = batch.LastBatchInfo
	}
}

func TestAdvLocalFileReader_BasicRead(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(256 * 1024) // 256KB per block
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	// First, create a file with test data
	testData := [][]byte{
		[]byte("Entry 0: First entry"),
		[]byte("Entry 1: Second entry"),
		[]byte("Entry 2: Third entry"),
		generateTestData(1024), // Entry 3: 1KB data
		[]byte("Entry 4: Final entry"),
	}

	// Write test data
	logId := int64(11)
	segmentId := int64(1100)
	writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
	require.NoError(t, err)

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-write-%d", i))
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

	// Now test reading
	reader, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId, segmentId, 16_000_000)
	require.NoError(t, err)
	require.NotNil(t, reader)

	t.Run("AdvGetLastEntryID", func(t *testing.T) {
		lastEntryId, err := reader.GetLastEntryID(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(4), lastEntryId)
	})

	t.Run("AdvReadAllEntries", func(t *testing.T) {
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(len(testData)),
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, len(testData), len(batch.Entries))

		// Verify content
		for i, entry := range batch.Entries {
			assert.Equal(t, int64(i), entry.EntryId)
			assert.Equal(t, testData[i], entry.Values)
		}
	})

	t.Run("AdvReadFromMiddle", func(t *testing.T) {
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    2,
			MaxBatchEntries: 3,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 3, len(batch.Entries))

		// Verify content
		for i, entry := range batch.Entries {
			expectedId := int64(2 + i)
			assert.Equal(t, expectedId, entry.EntryId)
			assert.Equal(t, testData[expectedId], entry.Values)
		}
	})

	t.Run("AdvReadAutoBatchMode", func(t *testing.T) {
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    1,
			MaxBatchEntries: -1, // Auto batch mode
		}, nil)
		require.NoError(t, err)
		assert.Greater(t, len(batch.Entries), 0)

		// Should start from entry 1
		assert.Equal(t, int64(1), batch.Entries[0].EntryId)
		assert.Equal(t, testData[1], batch.Entries[0].Values)
	})

	t.Run("AdvGetMetadata", func(t *testing.T) {
		footer := reader.GetFooter()
		require.NotNil(t, footer)
		assert.Greater(t, footer.TotalBlocks, int32(0))
		assert.Greater(t, footer.TotalRecords, uint32(0))

		blockIndexes := reader.GetBlockIndexes()
		assert.Greater(t, len(blockIndexes), 0)

		totalRecords := reader.GetTotalRecords()
		assert.Greater(t, totalRecords, uint32(0))

		totalBlocks := reader.GetTotalBlocks()
		assert.Greater(t, totalBlocks, int32(0))
	})

	err = reader.Close(ctx)
	require.NoError(t, err)
}

func TestAdvLocalFileReader_MultipleBlocks(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	blockSize := int64(100 * 1024) // Small 100KB blocks to force multiple blocks
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	// Create test data that will span multiple blocks
	totalEntries := 20
	testData := make([][]byte, totalEntries)
	for i := 0; i < totalEntries; i++ {
		testData[i] = []byte(fmt.Sprintf("Entry %d: %s", i, generateTestData(8*1024))) // 8KB each
	}

	// Write test data
	logId := int64(12)
	segmentId := int64(1200)
	writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
	require.NoError(t, err)

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-write-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)

		// Force sync every few entries to create separate blocks
		if (i+1)%3 == 0 {
			err = writer.Sync(ctx)
			require.NoError(t, err)
		}
	}

	_, err = writer.Finalize(ctx)
	require.NoError(t, err)
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Test reading with different batch modes
	reader, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId, segmentId, 16_000_000)
	require.NoError(t, err)
	defer reader.Close(ctx)

	t.Run("AdvReadAcrossMultipleBlocks", func(t *testing.T) {
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    5,
			MaxBatchEntries: 10, // Read across multiple blocks
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 10, len(batch.Entries))

		// Verify order and content
		for i, entry := range batch.Entries {
			expectedId := int64(5 + i)
			assert.Equal(t, expectedId, entry.EntryId)
			assert.Equal(t, testData[expectedId], entry.Values)
		}
	})

	t.Run("AdvReadSingleBlockMode", func(t *testing.T) {
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    8,
			MaxBatchEntries: -1, // Auto batch mode - single block
		}, nil)
		require.NoError(t, err)
		assert.Greater(t, len(batch.Entries), 0)

		// Should start from entry 8
		assert.Equal(t, int64(8), batch.Entries[0].EntryId)
		assert.Equal(t, testData[8], batch.Entries[0].Values)
	})

	t.Run("AdvVerifyBlockStructure", func(t *testing.T) {
		blockIndexes := reader.GetBlockIndexes()
		assert.Greater(t, len(blockIndexes), 1, "Should have multiple blocks")

		// Verify block index consistency
		for i, blockIndex := range blockIndexes {
			assert.Equal(t, int32(i), blockIndex.BlockNumber)
			assert.GreaterOrEqual(t, blockIndex.LastEntryID, blockIndex.FirstEntryID)
		}
	})
}

func TestAdvLocalFileReader_ErrorHandling(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	blockSize := int64(256 * 1024)
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	t.Run("AdvNonExistentFile", func(t *testing.T) {
		nonExistentPath := filepath.Join(tempDir, "non-existent.log")
		reader, err := disk.NewLocalFileReaderAdv(context.TODO(), nonExistentPath, 1000, 2000, 16_000_000)
		assert.Error(t, err)
		assert.Nil(t, reader)
	})

	t.Run("AdvInvalidEntryId", func(t *testing.T) {
		// Create a valid file first
		logId := int64(13)
		segmentId := int64(1300)
		writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
		require.NoError(t, err)

		// Write a few entries
		for i := 0; i < 5; i++ {
			data := []byte(fmt.Sprintf("Entry %d", i))
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-write-%d", i))

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

		// Test reading invalid entry IDs
		reader, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId, segmentId, 16_000_000)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Test reading from non-existent entry ID
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    100, // Way beyond available entries
			MaxBatchEntries: 10,
		}, nil)
		assert.Error(t, err)
		assert.True(t, werr.ErrFileReaderEndOfFile.Is(err), err.Error())
		assert.Nil(t, batch)
	})

	t.Run("AdvReadAfterClose", func(t *testing.T) {
		logId := int64(14)
		segmentId := int64(1400)
		writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
		require.NoError(t, err)

		// Write and finalize
		resultCh := channel.NewLocalResultChannel("test-write")
		_, err = writer.WriteDataAsync(ctx, 0, []byte("test data"), resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)

		_, err = writer.Finalize(ctx)
		require.NoError(t, err)
		err = writer.Close(ctx)
		require.NoError(t, err)

		// Create reader and close it
		reader, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId, segmentId, 16_000_000)
		require.NoError(t, err)

		err = reader.Close(ctx)
		require.NoError(t, err)

		// Try to read after close
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 1,
		}, nil)
		assert.Error(t, err)
		assert.True(t, werr.ErrFileReaderAlreadyClosed.Is(err))
		assert.Nil(t, batch)
	})
}

func TestAdvLocalFileRW_DataIntegrityWithDifferentSizes(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	blockSize := int64(512 * 1024) // 512KB blocks
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	// Test various data sizes
	testCases := []struct {
		name string
		data []byte
	}{
		{"SingleByte", []byte("a")},
		{"Small", []byte("Hello, World!")},
		{"Medium", generateTestData(1024)},           // 1KB
		{"Large", generateTestData(64 * 1024)},       // 64KB
		{"ExtraLarge", generateTestData(256 * 1024)}, // 256KB
	}

	// Write data
	logId := int64(15)
	segmentId := int64(1500)
	writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
	require.NoError(t, err)

	for i, tc := range testCases {
		t.Run(tc.name+"_Write", func(t *testing.T) {
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

	// Finalize and close writer
	lastEntryId, err := writer.Finalize(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testCases)-1), lastEntryId)

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Read back and verify data integrity
	reader, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId, segmentId, 16_000_000)
	require.NoError(t, err)
	defer reader.Close(ctx)

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
}

func TestAdvLocalFileRW_EmptyPayloadValidation(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	blockSize := int64(256 * 1024)
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	logId := int64(16)
	segmentId := int64(1600)
	writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Test empty payload validation at the storage layer
	t.Run("AdvEmptyPayloadAtStorageLayer", func(t *testing.T) {
		// Try to write empty data directly to storage layer
		_, err := writer.WriteDataAsync(ctx, 0, []byte{}, channel.NewLocalResultChannel("test-empty-payload"))
		require.Error(t, err)
		assert.True(t, werr.ErrEmptyPayload.Is(err), "Error should be ErrEmptyPayload")

		t.Logf("Storage layer empty payload error: %v", err)
	})

	// Test empty payload validation at the client level (LogWriter)
	t.Run("AdvEmptyPayloadAtClientLevel", func(t *testing.T) {
		emptyMsg := &log.WriterMessage{
			Payload:    []byte{},
			Properties: map[string]string{"test": "value"},
		}

		_, err := log.MarshalMessage(emptyMsg)
		require.NoError(t, err)
		t.Logf("Client level empty payload error: %v", err)
	})

	// Test nil payload validation
	t.Run("AdvNilPayloadAtClientLevel", func(t *testing.T) {
		nilMsg := &log.WriterMessage{
			Payload:    nil,
			Properties: map[string]string{"test": "value"},
		}

		_, err := log.MarshalMessage(nilMsg)
		require.NoError(t, err)

		t.Logf("Client level nil payload error: %v", err)
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

func TestAdvLocalFileRW_BlockHeaderRecordVerification(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	blockSize := int64(200) // Very small to force block creation
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	// Create writer
	logId := int64(19)
	segmentId := int64(1900)
	writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

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

	// Finalize the segment
	lastEntryId, err := writer.Finalize(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), lastEntryId)

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Create reader and verify block structure
	reader, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId, segmentId, 16_000_000)
	require.NoError(t, err)
	require.NotNil(t, reader)

	// Verify block indexes
	blocks := reader.GetBlockIndexes()
	require.Greater(t, len(blocks), 0, "Should have at least one block")

	for i, block := range blocks {
		t.Logf("Block %d: FirstEntryID=%d, LastEntryID=%d", i, block.FirstEntryID, block.LastEntryID)
		assert.GreaterOrEqual(t, block.LastEntryID, block.FirstEntryID)
	}

	err = reader.Close(ctx)
	require.NoError(t, err)
}

// TestLocalFileRW_WriteInterruptionAndRecovery tests write interruption and recovery scenarios
func TestAdvLocalFileRW_WriteInterruptionAndRecovery(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(256 * 1024) // 256KB per block
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	// Phase 1: Write some data and simulate interruption
	t.Run("AdvWriteDataAndInterrupt", func(t *testing.T) {
		// Create first writer and write some data
		logId := int64(20)
		segmentId := int64(2000)
		writer1, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
		require.NoError(t, err)
		require.NotNil(t, writer1)

		// Write test data
		testData := [][]byte{
			[]byte("Entry 0: Initial data before interruption"),
			[]byte("Entry 1: More data before interruption"),
			generateTestData(1024), // 1KB
			[]byte("Entry 3: Final entry before interruption"),
		}

		for i, data := range testData {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-interruption-write-%d", i))
			_, err := writer1.WriteDataAsync(ctx, int64(i), data, resultCh)
			require.NoError(t, err)

			// Wait for result
			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
		}

		// Force sync to ensure data is written to disk
		err = writer1.Sync(ctx)
		require.NoError(t, err)

		// Verify initial state
		assert.Equal(t, int64(0), writer1.GetFirstEntryId(ctx))
		assert.Equal(t, int64(3), writer1.GetLastEntryId(ctx))

		// Simulate interruption by closing the writer WITHOUT finalize
		// This leaves the file in an incomplete state
		err = writer1.Close(ctx)
		require.NoError(t, err)

		// Verify file exists but is incomplete (no footer)
		stat, err := os.Stat(filepath.Join(tempDir, fmt.Sprintf("%d/%d/data.log", logId, segmentId)))
		require.NoError(t, err)
		assert.Greater(t, stat.Size(), int64(0), "File should contain some data")

		t.Logf("Interrupted file size: %d bytes", stat.Size())
	})

	// Phase 2: Recover from the interrupted file and continue writing
	t.Run("AdvRecoverAndContinueWriting", func(t *testing.T) {
		// Create a new writer in recovery mode
		logId := int64(20)
		segmentId := int64(2000)
		writer2, err := disk.NewLocalFileWriterWithMode(context.TODO(), tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)
		require.NotNil(t, writer2)

		// Check recovered state
		recoveredFirstEntryId := writer2.GetFirstEntryId(ctx)
		recoveredLastEntryId := writer2.GetLastEntryId(ctx)
		t.Logf("Recovered state: firstEntryId=%d, lastEntryId=%d", recoveredFirstEntryId, recoveredLastEntryId)

		// Verify recovered state matches what we wrote before interruption
		assert.Equal(t, int64(0), recoveredFirstEntryId)
		assert.Equal(t, int64(3), recoveredLastEntryId)

		// Continue writing additional data from where we left off
		nextEntryId := recoveredLastEntryId + 1
		additionalData := [][]byte{
			[]byte("Entry 4: Recovery data 1"),
			[]byte("Entry 5: Recovery data 2"),
			generateTestData(512), // 512B
			[]byte("Entry 7: Final recovery data"),
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

		// Verify final state before finalize
		expectedLastEntryId := nextEntryId + int64(len(additionalData)) - 1
		assert.Equal(t, recoveredFirstEntryId, writer2.GetFirstEntryId(ctx))
		assert.Equal(t, expectedLastEntryId, writer2.GetLastEntryId(ctx))

		// Finalize the segment to make it complete
		lastEntryId, err := writer2.Finalize(ctx)
		require.NoError(t, err)
		assert.Equal(t, expectedLastEntryId, lastEntryId)

		// Close the writer
		err = writer2.Close(ctx)
		require.NoError(t, err)

		t.Logf("Final file state: firstEntryId=%d, lastEntryId=%d", recoveredFirstEntryId, lastEntryId)
	})

	// Phase 3: Verify the recovered and finalized file is complete and readable
	t.Run("AdvVerifyRecoveredFileComplete", func(t *testing.T) {
		// Create reader to verify the file is properly finalized
		// Use the logId and segmentId from the RecoverAndContinueWriting test (21, 2100)
		reader, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, 20, 2000, 16_000_000)
		require.NoError(t, err)
		require.NotNil(t, reader)

		// Verify file is completed with footer
		footer := reader.GetFooter()
		require.NotNil(t, footer, "Footer should exist after finalization")
		assert.Greater(t, footer.TotalBlocks, int32(0), "Should have at least one block")
		assert.Greater(t, footer.TotalRecords, uint32(0), "Should have at least one record")

		// Verify block indexes
		blocks := reader.GetBlockIndexes()
		require.Greater(t, len(blocks), 0, "Should have at least one block index")

		// Verify we can read all entries
		lastEntryId, err := reader.GetLastEntryID(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(7), lastEntryId) // 4 original + 4 recovery entries - 1 (0-based)

		// Verify we can read data from the beginning
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 10, // Read all entries
		}, nil)
		require.NoError(t, err)
		require.Equal(t, 8, len(batch.Entries), "Should have 8 total entries (4 original + 4 recovery)")

		// Verify entry content and order
		expectedEntries := []string{
			"Entry 0: Initial data before interruption",
			"Entry 1: More data before interruption",
			"", // Entry 2 is generated data, skip content check
			"Entry 3: Final entry before interruption",
			"Entry 4: Recovery data 1",
			"Entry 5: Recovery data 2",
			"", // Entry 6 is generated data, skip content check
			"Entry 7: Final recovery data",
		}

		for i, entry := range batch.Entries {
			assert.Equal(t, int64(i), entry.EntryId, "Entry ID should match index")
			if expectedEntries[i] != "" {
				assert.Equal(t, expectedEntries[i], string(entry.Values), "Entry content should match")
			}
		}

		err = reader.Close(ctx)
		require.NoError(t, err)
	})

	// Phase 4: Test that trying to recover from a finalized file success
	t.Run("AdvCanRecoverFromFinalizedFile", func(t *testing.T) {
		// Try to create a recovery writer from the finalized file
		logId := int64(20)
		segmentId := int64(2000)
		_, err := disk.NewLocalFileWriterWithMode(context.TODO(), tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err, "Should be able to recover from a finalized file")
	})

	// Phase 5: Test recovery from empty file
	t.Run("AdvRecoveryFromEmptyFile", func(t *testing.T) {
		// Try to recover from empty file
		logId := int64(23)
		segmentId := int64(2300)
		emptyFilePath := filepath.Join(tempDir, fmt.Sprintf("%d_%d.log", logId, segmentId))

		// Create empty file
		emptyFile, err := os.Create(emptyFilePath)
		require.NoError(t, err)
		emptyFile.Close()

		writer, err := disk.NewLocalFileWriterWithMode(context.TODO(), tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)
		require.NotNil(t, writer)

		// Should start with clean state
		assert.Equal(t, int64(-1), writer.GetFirstEntryId(ctx))
		assert.Equal(t, int64(-1), writer.GetLastEntryId(ctx))

		// Write some data
		resultCh := channel.NewLocalResultChannel("test-empty-recovery")
		_, err = writer.WriteDataAsync(ctx, 0, []byte("Recovery from empty file"), resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)

		// Finalize and close
		_, err = writer.Finalize(ctx)
		require.NoError(t, err)
		err = writer.Close(ctx)
		require.NoError(t, err)
	})

	// Phase 6: Test recovery from non-existent file
	t.Run("AdvRecoveryFromNonExistentFile", func(t *testing.T) {
		// Try to recover from non-existent file
		logId := int64(24)
		segmentId := int64(2400)
		writer, err := disk.NewLocalFileWriterWithMode(context.TODO(), tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)
		require.NotNil(t, writer)

		// Should start with clean state
		assert.Equal(t, int64(-1), writer.GetFirstEntryId(ctx))
		assert.Equal(t, int64(-1), writer.GetLastEntryId(ctx))

		// Write some data
		resultCh := channel.NewLocalResultChannel("test-non-existent-recovery")
		_, err = writer.WriteDataAsync(ctx, 0, []byte("Recovery from non-existent file"), resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)

		// Finalize and close
		_, err = writer.Finalize(ctx)
		require.NoError(t, err)
		err = writer.Close(ctx)
		require.NoError(t, err)
	})

	// Phase 7: Test recovery from corrupted file (truncated in middle of record)
	t.Run("AdvRecoveryFromCorruptedFile", func(t *testing.T) {
		// Create a file with valid data first
		logId := int64(25)
		segmentId := int64(2500)
		corruptedFilePath := filepath.Join(tempDir, fmt.Sprintf("%d/%d/data.log", logId, segmentId))

		writer1, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
		require.NoError(t, err)

		// Write some data
		for i := 0; i < 3; i++ {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-corrupted-setup-%d", i))
			_, err := writer1.WriteDataAsync(ctx, int64(i), []byte(fmt.Sprintf("Entry %d", i)), resultCh)
			require.NoError(t, err)

			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
		}

		err = writer1.Sync(ctx)
		require.NoError(t, err)
		err = writer1.Close(ctx)
		require.NoError(t, err)

		// Corrupt the file by truncating it in the middle
		stat, err := os.Stat(corruptedFilePath)
		require.NoError(t, err)

		// Truncate to 80% of original size to simulate corruption
		truncatedSize := stat.Size() * 8 / 10
		err = os.Truncate(corruptedFilePath, truncatedSize)
		require.NoError(t, err)

		t.Logf("Corrupted file by truncating from %d to %d bytes", stat.Size(), truncatedSize)

		// Try to recover from corrupted file using the same logId and segmentId
		writer2, err := disk.NewLocalFileWriterWithMode(context.TODO(), tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)
		require.NotNil(t, writer2)

		// Should have recovered some entries (before corruption point)
		recoveredFirstEntryId := writer2.GetFirstEntryId(ctx)
		recoveredLastEntryId := writer2.GetLastEntryId(ctx)
		t.Logf("Recovered from corrupted file: firstEntryId=%d, lastEntryId=%d", recoveredFirstEntryId, recoveredLastEntryId)

		// Continue writing from where recovery left off
		nextEntryId := recoveredLastEntryId + 1
		if recoveredLastEntryId == -1 {
			nextEntryId = 0
		}

		resultCh := channel.NewLocalResultChannel("test-corrupted-recovery")
		_, err = writer2.WriteDataAsync(ctx, nextEntryId, []byte("Recovery from corruption"), resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)

		// Finalize and close
		_, err = writer2.Finalize(ctx)
		require.NoError(t, err)
		err = writer2.Close(ctx)
		require.NoError(t, err)
	})
}

// TestLocalFileReader_ReadIncompleteFile tests reading a file that is still being written (no footer)
func TestAdvLocalFileReader_ReadIncompleteFile(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(256 * 1024) // 256KB per block
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	logId := int64(30)
	segmentId := int64(3000)

	// Create a writer and write some data without finalizing
	writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
	require.NoError(t, err)

	// Write test data but don't finalize (so no footer will be written)
	testData := [][]byte{
		[]byte("Entry 0: First entry in incomplete file"),
		[]byte("Entry 1: Second entry in incomplete file"),
		generateTestData(1024), // Entry 2: 1KB data
		[]byte("Entry 3: Final entry in incomplete file"),
	}

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-incomplete-write-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	// Force sync to ensure data is written to disk
	err = writer.Sync(ctx)
	require.NoError(t, err)

	// Close writer WITHOUT finalizing (this simulates an incomplete file)
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Now test reading the incomplete file
	t.Run("AdvReadIncompleteFileWithFullScan", func(t *testing.T) {
		reader, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId, segmentId, 16_000_000)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		// Verify that footer is nil (file is incomplete)
		footer := reader.GetFooter()
		assert.Nil(t, footer, "Footer should be nil for incomplete file")

		// Verify that block indexes were built from full scan
		blockIndexes := reader.GetBlockIndexes()
		assert.Equal(t, len(blockIndexes), 0, "Should have 0 block indexes from due to advReader lazy scan")

		// Verify we can get the last entry ID
		lastEntryId, err := reader.GetLastEntryID(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(3), lastEntryId)

		// Verify we can read all entries
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(len(testData)),
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, len(testData), len(batch.Entries))

		// Verify content
		for i, entry := range batch.Entries {
			assert.Equal(t, int64(i), entry.EntryId)
			assert.Equal(t, testData[i], entry.Values)
		}

		// Verify we can read from middle
		batch, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    1,
			MaxBatchEntries: 2,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, len(batch.Entries))
		assert.Equal(t, int64(1), batch.Entries[0].EntryId)
		assert.Equal(t, testData[1], batch.Entries[0].Values)
		assert.Equal(t, int64(2), batch.Entries[1].EntryId)
		assert.Equal(t, testData[2], batch.Entries[1].Values)

		// Verify auto batch mode works
		batch, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    2,
			MaxBatchEntries: -1, // Auto batch mode
		}, nil)
		require.NoError(t, err)
		assert.Greater(t, len(batch.Entries), 0)
		assert.Equal(t, int64(2), batch.Entries[0].EntryId)
		assert.Equal(t, testData[2], batch.Entries[0].Values)
	})

	t.Run("AdvCompareWithFinalizedFile", func(t *testing.T) {
		// Create another writer and write the same data, but finalize it
		logId2 := int64(31)
		segmentId2 := int64(3100)
		writer2, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId2, segmentId2, cfg)
		require.NoError(t, err)

		for i, data := range testData {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-finalized-write-%d", i))
			_, err := writer2.WriteDataAsync(ctx, int64(i), data, resultCh)
			require.NoError(t, err)

			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
		}

		// Finalize this file
		_, err = writer2.Finalize(ctx)
		require.NoError(t, err)
		err = writer2.Close(ctx)
		require.NoError(t, err)

		// Read finalized file
		reader2, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId2, segmentId2, 16_000_000)
		require.NoError(t, err)
		defer reader2.Close(ctx)

		// Verify that footer exists (file is complete)
		footer2 := reader2.GetFooter()
		assert.NotNil(t, footer2, "Footer should exist for finalized file")

		// Read incomplete file
		reader1, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId, segmentId, 16_000_000)
		require.NoError(t, err)
		defer reader1.Close(ctx)

		// Compare data from both files
		batch1, err := reader1.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(len(testData)),
		}, nil)
		require.NoError(t, err)

		batch2, err := reader2.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(len(testData)),
		}, nil)
		require.NoError(t, err)

		// Data should be identical
		assert.Equal(t, len(batch1.Entries), len(batch2.Entries))
		for i := range batch1.Entries {
			assert.Equal(t, batch1.Entries[i].EntryId, batch2.Entries[i].EntryId)
			assert.Equal(t, batch1.Entries[i].Values, batch2.Entries[i].Values)
		}
	})
}

// TestLocalFileReader_DynamicScanning tests dynamic scanning of incomplete files
func TestAdvLocalFileReader_DynamicScanning(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(256 * 1024) // 256KB per block
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	logId := int64(40)
	segmentId := int64(4000)

	// Phase 1: Create a writer and write some initial data
	writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
	require.NoError(t, err)

	// Write initial data
	initialData := [][]byte{
		[]byte("Entry 0: Initial data"),
		[]byte("Entry 1: More initial data"),
	}

	for i, data := range initialData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-dynamic-initial-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	// Force sync to ensure data is written to disk
	err = writer.Sync(ctx)
	require.NoError(t, err)

	// Phase 2: Create reader for incomplete file
	reader, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId, segmentId, 16_000_000)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close(ctx)

	t.Run("AdvReadInitialData", func(t *testing.T) {
		// Read initial data
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 2,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, len(batch.Entries))

		// Verify content
		for i, entry := range batch.Entries {
			assert.Equal(t, int64(i), entry.EntryId)
			assert.Equal(t, initialData[i], entry.Values)
		}
	})

	t.Run("AdvTryReadBeyondAvailableData", func(t *testing.T) {
		// Try to read beyond available data - should return what's available
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    1,
			MaxBatchEntries: 5, // Request more than available
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, len(batch.Entries)) // Should only get entry 1

		assert.Equal(t, int64(1), batch.Entries[0].EntryId)
		assert.Equal(t, initialData[1], batch.Entries[0].Values)
	})

	// Phase 3: Write more data to the same file
	moreData := [][]byte{
		[]byte("Entry 2: Additional data after reader creation"),
		[]byte("Entry 3: Even more data"),
		generateTestData(1024), // Entry 4: 1KB data
	}

	for i, data := range moreData {
		entryId := int64(len(initialData) + i)
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-dynamic-additional-%d", entryId))
		_, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	// Force sync to ensure new data is written to disk
	err = writer.Sync(ctx)
	require.NoError(t, err)

	t.Run("AdvReadNewDataAfterDynamicScan", func(t *testing.T) {
		// Now try to read the new data - should trigger dynamic scanning
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    2,
			MaxBatchEntries: 3, // Request the 3 new entries
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 3, len(batch.Entries))

		// Verify new content
		for i, entry := range batch.Entries {
			expectedId := int64(2 + i)
			assert.Equal(t, expectedId, entry.EntryId)
			assert.Equal(t, moreData[i], entry.Values)
		}
	})

	t.Run("AdvReadAllDataAfterDynamicScan", func(t *testing.T) {
		// Read all data from the beginning
		allData := append(initialData, moreData...)
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(len(allData)),
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, len(allData), len(batch.Entries))

		// Verify all content
		for i, entry := range batch.Entries {
			assert.Equal(t, int64(i), entry.EntryId)
			assert.Equal(t, allData[i], entry.Values)
		}
	})

	// Phase 4: Write even more data
	finalData := [][]byte{
		[]byte("Entry 5: Final batch data 1"),
		[]byte("Entry 6: Final batch data 2"),
	}

	for i, data := range finalData {
		entryId := int64(len(initialData) + len(moreData) + i)
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-dynamic-final-%d", entryId))
		_, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	err = writer.Sync(ctx)
	require.NoError(t, err)

	t.Run("AdvReadFinalDataWithDynamicScan", func(t *testing.T) {
		// Read the final data
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    5,
			MaxBatchEntries: 2,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, len(batch.Entries))

		// Verify final content
		for i, entry := range batch.Entries {
			expectedId := int64(5 + i)
			assert.Equal(t, expectedId, entry.EntryId)
			assert.Equal(t, finalData[i], entry.Values)
		}
	})

	t.Run("AdvReadFromMiddleWithDynamicScan", func(t *testing.T) {
		// Read from middle, spanning across dynamically scanned blocks
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    3,
			MaxBatchEntries: 4, // Should get entries 3, 4, 5, 6
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 4, len(batch.Entries))

		expectedData := append(moreData[1:], finalData...)
		for i, entry := range batch.Entries {
			expectedId := int64(3 + i)
			assert.Equal(t, expectedId, entry.EntryId)
			assert.Equal(t, expectedData[i], entry.Values)
		}
	})

	// Close writer without finalizing to keep file incomplete
	err = writer.Close(ctx)
	require.NoError(t, err)
}

// TestLocalFileRW_ConcurrentReadWrite tests concurrent reading and writing to verify real-time read capability
func TestAdvLocalFileRW_ConcurrentReadWrite(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(256 * 1024) // 256KB per block
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	logId := int64(50)
	segmentId := int64(5000)

	// Create writer
	writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Test parameters
	totalEntries := 20
	writeInterval := 100 * time.Millisecond // Wait 100ms between writes
	readInterval := 50 * time.Millisecond   // Check for new data every 50ms

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
			data := []byte(fmt.Sprintf("Entry %d: Real-time data written at %s",
				i, time.Now().Format("15:04:05.000")))

			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-concurrent-write-%d", entryId))
			_, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)
			if err != nil {
				t.Errorf("Failed to write entry %d: %v", entryId, err)
				return
			}

			// Wait for write to complete
			result, err := resultCh.ReadResult(ctx)
			if err != nil {
				t.Errorf("Failed to get write result for entry %d: %v", entryId, err)
				return
			}
			if result.Err != nil {
				t.Errorf("Write failed for entry %d: %v", entryId, result.Err)
				return
			}

			// Force sync to ensure data is written to disk
			err = writer.Sync(ctx)
			if err != nil {
				t.Errorf("Failed to sync after entry %d: %v", entryId, err)
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

		// Create reader for the incomplete file
		reader, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId, segmentId, 16_000_000)
		if err != nil {
			t.Errorf("Failed to create reader: %v", err)
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
					break
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

		// Final verification read after writer is done
		t.Logf("Reader: Doing final verification read")

		// Wait a bit for any pending writes to complete
		time.Sleep(200 * time.Millisecond)

		finalBatch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(totalEntries),
		}, nil)

		if err != nil {
			t.Errorf("Reader: Failed final read: %v", err)
			return
		}

		t.Logf("Reader: Final read got %d entries", len(finalBatch.Entries))

		// Verify all entries were read
		for i := 0; i < totalEntries; i++ {
			if i < len(finalBatch.Entries) {
				readEntries[int64(i)] = finalBatch.Entries[i].Values
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
	timeout := time.Duration(totalEntries)*writeInterval + 10*time.Second

	select {
	case <-writerDone:
		t.Logf("Writer completed successfully")
	case <-time.After(timeout):
		t.Fatalf("Writer timed out after %v", timeout)
	}

	select {
	case <-readerDone:
		t.Logf("Reader completed successfully")
	case <-time.After(5 * time.Second):
		t.Fatalf("Reader timed out after writer completion")
	}

	// Close writer without finalizing to keep it as incomplete file
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Final verification: Create a new reader and verify all data is accessible
	t.Run("AdvFinalVerification", func(t *testing.T) {
		finalReader, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId, segmentId, 16_000_000)
		require.NoError(t, err)
		defer finalReader.Close(ctx)

		// Verify file is incomplete (no footer)
		footer := finalReader.GetFooter()
		assert.Nil(t, footer, "File should be incomplete (no footer)")

		// Verify we can read all entries
		allBatch, err := finalReader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(totalEntries),
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, totalEntries, len(allBatch.Entries), "Should read all entries")

		// Verify entry content and order
		for i, entry := range allBatch.Entries {
			assert.Equal(t, int64(i), entry.EntryId, "Entry ID should match")
			assert.Contains(t, string(entry.Values), fmt.Sprintf("Entry %d:", i),
				"Entry content should contain expected prefix")
		}

		// Verify dynamic scanning works by reading from different positions
		midBatch, err := finalReader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    int64(totalEntries / 2),
			MaxBatchEntries: 5,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 5, len(midBatch.Entries), "Should read 5 entries from middle")

		lastEntryId, err := finalReader.GetLastEntryID(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(totalEntries-1), lastEntryId, "Last entry ID should match")
	})
}

// TestAdvLocalFileRW_ConcurrentOneWriteMultipleReads tests one writer with multiple concurrent readers
func TestAdvLocalFileRW_ConcurrentOneWriteMultipleReads(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(256 * 1024) // 256KB per block
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	logId := int64(60)
	segmentId := int64(6000)

	// Create writer
	writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	// Test parameters
	totalEntries := 25
	numReaders := 3                         // Number of concurrent readers
	writeInterval := 120 * time.Millisecond // Wait 120ms between writes
	readInterval := 60 * time.Millisecond   // Check for new data every 60ms

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
			data := []byte(fmt.Sprintf("Entry %d: Multi-reader data written at %s",
				i, time.Now().Format("15:04:05.000")))

			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-multi-reader-write-%d", entryId))
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

			// Force sync to ensure data is written to disk
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

			// Create reader for the incomplete file
			reader, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId, segmentId, 16_000_000)
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
				batchSize = 3 // Read 3 entries at a time
			case 2:
				readStrategy = "large-batch"
				batchSize = 5 // Read 5 entries at a time
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
			time.Sleep(200 * time.Millisecond)

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

			// Verify we read all expected entries
			for i := 0; i < totalEntries; i++ {
				if _, exists := readEntries[int64(i)]; !exists {
					t.Errorf("Reader %d: Missing entry %d", readerNum, i)
				}
			}

			t.Logf("Reader %d: Successfully verified all %d entries", readerNum, len(readEntries))
			readerResults <- readEntries
		}(readerId)
	}

	// Wait for writer to complete with timeout
	timeout := time.Duration(totalEntries)*writeInterval + 10*time.Second

	select {
	case <-writerDone:
		t.Logf("Writer completed successfully")
	case <-time.After(timeout):
		t.Fatalf("Writer timed out after %v", timeout)
	}

	// Wait for all readers to complete
	completedReaders := 0
	readerTimeout := 10 * time.Second

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
		case <-time.After(1 * time.Second):
			t.Errorf("Failed to collect results from reader %d", i)
		}
	}

	// Close writer without finalizing to keep it as incomplete file
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Verify all readers got the same data
	t.Run("AdvVerifyReaderConsistency", func(t *testing.T) {
		require.Equal(t, numReaders, len(allReaderResults), "Should have results from all readers")

		// Check that all readers read all entries
		for readerIdx, readerData := range allReaderResults {
			assert.Equal(t, totalEntries, len(readerData),
				"Reader %d should have read all %d entries", readerIdx, totalEntries)

			// Verify each entry exists and has correct content
			for entryId := 0; entryId < totalEntries; entryId++ {
				data, exists := readerData[int64(entryId)]
				assert.True(t, exists, "Reader %d should have entry %d", readerIdx, entryId)
				assert.Contains(t, string(data), fmt.Sprintf("Entry %d:", entryId),
					"Reader %d entry %d should have correct content", readerIdx, entryId)
			}
		}

		// Verify data consistency across all readers
		for entryId := 0; entryId < totalEntries; entryId++ {
			// Get data from first reader as reference
			referenceData := allReaderResults[0][int64(entryId)]

			// Compare with all other readers
			for readerIdx := 1; readerIdx < numReaders; readerIdx++ {
				readerData := allReaderResults[readerIdx][int64(entryId)]
				assert.Equal(t, referenceData, readerData,
					"Entry %d should be identical across all readers (reader 0 vs reader %d)",
					entryId, readerIdx)
			}
		}

		t.Logf("All %d readers successfully read identical data for all %d entries",
			numReaders, totalEntries)
	})

	// Final verification: Create a new reader and verify all data is accessible
	t.Run("AdvFinalFileVerification", func(t *testing.T) {
		finalReader, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId, segmentId, 16_000_000)
		require.NoError(t, err)
		defer finalReader.Close(ctx)

		// Verify file is incomplete (no footer)
		footer := finalReader.GetFooter()
		assert.Nil(t, footer, "File should be incomplete (no footer)")

		// Verify we can read all entries
		allBatch, err := finalReader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(totalEntries),
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, totalEntries, len(allBatch.Entries), "Should read all entries")

		// Verify entry content and order
		for i, entry := range allBatch.Entries {
			assert.Equal(t, int64(i), entry.EntryId, "Entry ID should match")
			assert.Contains(t, string(entry.Values), fmt.Sprintf("Entry %d:", i),
				"Entry content should contain expected prefix")
		}

		lastEntryId, err := finalReader.GetLastEntryID(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(totalEntries-1), lastEntryId, "Last entry ID should match")

		t.Logf("Final verification: File contains all %d entries and is readable", totalEntries)
	})
}

// TestAdvLocalFileReader_ReadNextBatchAdvScenarios tests all three scenarios of ReadNextBatchAdv
func TestAdvLocalFileReader_ReadNextBatchAdvScenarios(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	logId := int64(1001)
	segId := int64(2001)

	// Setup configuration
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	// Write test data first
	writer, err := disk.NewLocalFileWriter(ctx, baseDir, logId, segId, cfg)
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
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-adv-%d", i))
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
		reader1, err := disk.NewLocalFileReaderAdv(ctx, baseDir, logId, segId, 16_000_000)
		require.NoError(t, err)
		defer reader1.Close(ctx)

		// Read first batch to get batch info
		batch1, err := reader1.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 2,
		}, nil)
		require.NoError(t, err)
		require.NotNil(t, batch1.LastBatchInfo)
		assert.Equal(t, 2, len(batch1.Entries))

		// Now create a new reader with advOpt from the previous batch
		reader2, err := disk.NewLocalFileReaderAdv(ctx, baseDir, logId, segId, 16_000_000)
		require.NoError(t, err)
		defer reader2.Close(ctx)

		// Read next batch using advOpt (should start from next block)
		batch2, err := reader2.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    2, // Should be ignored when using advOpt
			MaxBatchEntries: 2,
		}, batch1.LastBatchInfo)
		require.NoError(t, err)
		assert.Greater(t, len(batch2.Entries), 0)

		// Verify the entries are from the expected continuation point
		assert.True(t, batch2.Entries[0].EntryId >= 2, "Should continue from where previous batch left off")
	})

	t.Run("Scenario2_WithFooter_NoAdvOpt", func(t *testing.T) {
		// Create reader without advOpt (should use footer)
		reader, err := disk.NewLocalFileReaderAdv(ctx, baseDir, logId, segId, 16_000_000)
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
		assert.Equal(t, int64(3), batch.Entries[0].EntryId)
		assert.NotNil(t, batch.LastBatchInfo)
	})

	t.Run("Scenario3_NoFooter_IncompleteFile", func(t *testing.T) {
		// Create a new incomplete file (no footer)
		incompleteBaseDir := t.TempDir()
		incompleteLogId := int64(1002)
		incompleteSegId := int64(2002)

		writer, err := disk.NewLocalFileWriter(ctx, incompleteBaseDir, incompleteLogId, incompleteSegId, cfg)
		require.NoError(t, err)

		// Write some data but don't finalize (no footer)
		for i := 0; i < 4; i++ {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-incomplete-%d", i))
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
		reader, err := disk.NewLocalFileReaderAdv(ctx, incompleteBaseDir, incompleteLogId, incompleteSegId, 16_000_000)
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

// TestAdvLocalFileReader_AdvOptContinuation tests the continuation behavior with advOpt
func TestAdvLocalFileReader_AdvOptContinuation(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	logId := int64(1003)
	segId := int64(2003)

	// Setup configuration
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	// Create test file with multiple blocks
	writer, err := disk.NewLocalFileWriter(ctx, baseDir, logId, segId, cfg)
	require.NoError(t, err)

	// Write 15 entries across multiple blocks
	for i := 0; i < 15; i++ {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-continuation-%d", i))
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
		reader, err := disk.NewLocalFileReaderAdv(ctx, baseDir, logId, segId, 16_000_000)
		require.NoError(t, err)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    int64(batchNum * 3), // This should be ignored when lastBatchInfo is provided
			MaxBatchEntries: 3,
		}, lastBatchInfo)

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

// TestAdvLocalFileReader_EdgeCases tests edge cases of ReadNextBatchAdv
func TestAdvLocalFileReader_EdgeCases(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	logId := int64(1004)
	segId := int64(2004)

	// Setup configuration
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	t.Run("EmptyFile_NoAdvOpt", func(t *testing.T) {
		// Create empty file
		writer, err := disk.NewLocalFileWriter(ctx, baseDir, logId, segId+1, cfg)
		require.NoError(t, err)
		writer.Close(ctx)

		reader, err := disk.NewLocalFileReaderAdv(ctx, baseDir, logId, segId+1, 16_000_000)
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
		writer, err := disk.NewLocalFileWriter(ctx, baseDir, logId, segId+2, cfg)
		require.NoError(t, err)

		resultCh := channel.NewLocalResultChannel("test-single")
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

		reader, err := disk.NewLocalFileReaderAdv(ctx, baseDir, logId, segId+2, 16_000_000)
		require.NoError(t, err)
		defer reader.Close(ctx)

		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 10,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 1, len(batch.Entries))
		assert.Equal(t, int64(0), batch.Entries[0].EntryId)
		assert.NotNil(t, batch.LastBatchInfo)
	})

	t.Run("ReadBeyondEnd_WithAdvOpt", func(t *testing.T) {
		writer, err := disk.NewLocalFileWriter(ctx, baseDir, logId, segId+3, cfg)
		require.NoError(t, err)

		// Write 3 entries
		for i := 0; i < 3; i++ {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-beyond-%d", i))
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
		reader1, err := disk.NewLocalFileReaderAdv(ctx, baseDir, logId, segId+3, 16_000_000)
		require.NoError(t, err)

		batch1, err := reader1.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 10,
		}, nil)
		require.NoError(t, err)
		reader1.Close(ctx)

		// Try to read beyond end with advOpt
		reader2, err := disk.NewLocalFileReaderAdv(ctx, baseDir, logId, segId+3, 16_000_000)
		require.NoError(t, err)
		defer reader2.Close(ctx)

		batch2, err := reader2.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    100, // Should be ignored
			MaxBatchEntries: 10,
		}, batch1.LastBatchInfo)
		assert.Error(t, err)
		assert.True(t, werr.ErrFileReaderEndOfFile.Is(err))
		assert.Nil(t, batch2)
	})
}

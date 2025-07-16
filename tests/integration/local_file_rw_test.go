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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/disk"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func setupLocalFileTest(t *testing.T) string {
	// Load configuration and initialize logger for debugging
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	// Set log level to debug for detailed logging
	cfg.Log.Level = "debug"

	// Initialize logger with debug level
	logger.InitLogger(cfg)

	// Create a temporary directory for test files
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("woodpecker-local-test-%d", time.Now().Unix()))
	err = os.MkdirAll(tempDir, 0755)
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	return tempDir
}

func TestLocalFileWriter_BasicWriteAndFinalize(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(256 * 1024) // 256KB per block
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	// Create LocalFileWriter
	logId := int64(1)
	segmentId := int64(100)
	writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	t.Run("WriteDataAsync", func(t *testing.T) {
		// Test writing data
		testData := [][]byte{
			[]byte("Hello, Local FS!"),
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

	t.Run("Finalize", func(t *testing.T) {
		// Test finalize
		lastEntryId, err := writer.Finalize(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(3), lastEntryId)
	})

	// Close writer
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Verify file exists (construct expected file path)
	expectedFilePath := filepath.Join(tempDir, fmt.Sprintf("%d/%d/data.log", logId, segmentId))
	_, err = os.Stat(expectedFilePath)
	require.NoError(t, err)
}

func TestLocalFileWriter_LargeDataAndMultipleBlocks(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(128 * 1024) // 128KB per block to test multi-block scenario
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	logId := int64(2)
	segmentId := int64(200)
	writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
	require.NoError(t, err)
	defer writer.Close(ctx)

	// Write data that will span multiple blocks
	largeData := [][]byte{
		generateTestData(100 * 1024), // 100KB
		generateTestData(100 * 1024), // 100KB - should trigger new block
		generateTestData(50 * 1024),  // 50KB
		generateTestData(80 * 1024),  // 80KB - should trigger another block
		[]byte("Final small entry"),
	}

	// Write entries sequentially to avoid out-of-order issues
	for i, data := range largeData {
		entryId := int64(i)
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-large-%d", entryId))
		returnedId, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)
		require.NoError(t, err)
		assert.Equal(t, entryId, returnedId)

		// Wait for result
		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		assert.Equal(t, entryId, result.SyncedId)
	}

	// Verify final state
	assert.Equal(t, int64(0), writer.GetFirstEntryId(ctx))
	assert.Equal(t, int64(len(largeData)-1), writer.GetLastEntryId(ctx))

	// Finalize and close
	_, err = writer.Finalize(ctx)
	require.NoError(t, err)
}

func TestLocalFileWriter_ConcurrentWrites(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(256 * 1024)
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	logId := int64(3)
	segmentId := int64(300)
	writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
	require.NoError(t, err)
	defer writer.Close(ctx)

	// Test sequential writes with concurrent result handling
	// Note: LocalFileWriter expects entries to be written in order
	const totalEntries = 50

	var wg sync.WaitGroup
	results := make(chan error, totalEntries)

	// Write entries sequentially but handle results concurrently
	for i := 0; i < totalEntries; i++ {
		entryId := int64(i)
		data := []byte(fmt.Sprintf("Entry %d: %s", i, generateTestData(512)))

		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-concurrent-%d", entryId))
		_, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)
		if err != nil {
			results <- err
			continue
		}

		// Handle result in a separate goroutine
		wg.Add(1)
		go func(ch channel.ResultChannel) {
			defer wg.Done()
			result, err := ch.ReadResult(ctx)
			if err != nil {
				results <- err
			} else {
				results <- result.Err
			}
		}(resultCh)
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

	assert.Equal(t, totalEntries, successCount, "All sequential writes should succeed")

	// Verify final state
	assert.Equal(t, int64(0), writer.GetFirstEntryId(ctx))
	assert.Equal(t, int64(totalEntries-1), writer.GetLastEntryId(ctx))

	// Finalize
	_, err = writer.Finalize(ctx)
	require.NoError(t, err)
}

func TestLocalFileWriter_ErrorHandling(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(256 * 1024)
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	t.Run("EmptyPayloadValidation", func(t *testing.T) {
		logId := int64(4)
		segmentId := int64(400)
		writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
		require.NoError(t, err)
		defer writer.Close(ctx)

		// Try to write empty data
		resultCh := channel.NewLocalResultChannel("test-empty")
		_, err = writer.WriteDataAsync(ctx, 0, []byte{}, resultCh)
		require.Error(t, err)
		assert.True(t, werr.ErrEmptyPayload.Is(err))
	})

	t.Run("WriteAfterFinalize", func(t *testing.T) {
		logId := int64(5)
		segmentId := int64(500)
		writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
		require.NoError(t, err)
		defer writer.Close(ctx)

		// Write some data and finalize
		resultCh1 := channel.NewLocalResultChannel("test-initial")
		_, err = writer.WriteDataAsync(ctx, 0, []byte("initial data"), resultCh1)
		require.NoError(t, err)

		result, err := resultCh1.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)

		_, err = writer.Finalize(ctx)
		require.NoError(t, err)

		// Try to write after finalize
		resultCh2 := channel.NewLocalResultChannel("test-after-finalize")
		_, err = writer.WriteDataAsync(ctx, 1, []byte("should fail"), resultCh2)
		assert.Error(t, err)
		assert.True(t, werr.ErrFileWriterFinalized.Is(err))
	})

	t.Run("WriteAfterClose", func(t *testing.T) {
		logId := int64(6)
		segmentId := int64(600)
		writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
		require.NoError(t, err)

		// Close writer
		err = writer.Close(ctx)
		require.NoError(t, err)

		// Try to write after close
		resultCh := channel.NewLocalResultChannel("test-write-after-close")
		_, err = writer.WriteDataAsync(ctx, 1, []byte("should fail"), resultCh)
		assert.Error(t, err)
		assert.True(t, werr.ErrFileWriterAlreadyClosed.Is(err))
	})

	t.Run("DuplicateEntryIdWithWrittenID", func(t *testing.T) {
		logId := int64(7)
		segmentId := int64(700)
		writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
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
		logId := int64(8)
		segmentId := int64(800)
		writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
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

	t.Run("DuplicateEntryIdInFlushing", func(t *testing.T) {
		t.Skipf("sync run too fast, should mock another case instead")

		logId := int64(9)
		segmentId := int64(900)
		writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
		require.NoError(t, err)
		defer writer.Close(ctx)

		// Write first entry
		resultCh1 := channel.NewLocalResultChannel("test-duplicate-1")
		_, err = writer.WriteDataAsync(ctx, 0, []byte("first"), resultCh1)
		require.NoError(t, err)

		// Force sync to ensure first entry is processed
		err = writer.Sync(ctx) // TODO run too fast, should mock another testcase
		require.NoError(t, err)

		// Try to write same entry ID again (should be handled gracefully)
		resultCh2 := channel.NewLocalResultChannel("test-duplicate-2")
		_, err = writer.WriteDataAsync(ctx, 0, []byte("duplicate"), resultCh2)
		//require.NoError(t, err) // TODO maybe handle this notify gracefully
		require.Error(t, err)
		assert.True(t, werr.ErrFileWriterInvalidEntryId.Is(err))

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

	t.Run("LargePayloadSupport", func(t *testing.T) {
		logId := int64(10)
		segmentId := int64(1000)

		// Configure for large entries
		cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 20 * 1024 * 1024 // 20MB

		writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
		require.NoError(t, err)
		defer writer.Close(ctx)

		// Test writing large payloads: 2MB, 4MB, 8MB, 16MB
		testCases := []struct {
			name string
			size int
		}{
			{"2MB", 2 * 1024 * 1024},
			{"4MB", 4 * 1024 * 1024},
			{"8MB", 8 * 1024 * 1024},
			{"16MB", 16 * 1024 * 1024},
		}

		for i, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				// Generate large payload with pattern for verification
				largePayload := make([]byte, testCase.size)
				for j := 0; j < len(largePayload); j++ {
					largePayload[j] = byte(j % 256)
				}

				entryId := int64(i)
				resultCh := channel.NewLocalResultChannel("test-large-payload")

				// Write large payload
				_, err := writer.WriteDataAsync(ctx, entryId, largePayload, resultCh)
				require.NoError(t, err)

				// Wait for result with extended timeout for large entries
				ctxWithTimeout, cancel := context.WithTimeout(ctx, 60*time.Second)
				result, err := resultCh.ReadResult(ctxWithTimeout)
				cancel()
				require.NoError(t, err)
				require.NoError(t, result.Err)
				assert.Equal(t, entryId, result.SyncedId)

				t.Logf("Successfully wrote %s payload (%d bytes)", testCase.name, testCase.size)
			})
		}
	})

	t.Run("SingleEntryLargerThanMaxFlushSize", func(t *testing.T) {
		logId := int64(11)
		segmentId := int64(1001)

		// Configure small maxFlushSize to test the edge case
		cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 1 * 1024 * 1024 // 1MB flush size

		writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
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
		}

		for i, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				// Generate large payload
				largePayload := make([]byte, testCase.size)
				for j := 0; j < len(largePayload); j++ {
					largePayload[j] = byte((j + i*1000) % 256) // Different pattern for each test
				}

				entryId := int64(i)
				resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-oversized-entry-%d", i))

				// This should succeed even though entry size > maxFlushSize
				// because we write to buffer first, then check for flush
				_, err := writer.WriteDataAsync(ctx, entryId, largePayload, resultCh)
				require.NoError(t, err)

				// Wait for result
				ctxWithTimeout, cancel := context.WithTimeout(ctx, 60*time.Second)
				result, err := resultCh.ReadResult(ctxWithTimeout)
				cancel()
				require.NoError(t, err)
				require.NoError(t, result.Err)
				assert.Equal(t, entryId, result.SyncedId)

				t.Logf("Successfully wrote %s payload (%d bytes) which is larger than maxFlushSize (1MB)", testCase.name, testCase.size)
			})
		}
	})
}

func TestLocalFileReader_BasicRead(t *testing.T) {
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
	reader, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId, segmentId)
	require.NoError(t, err)
	require.NotNil(t, reader)

	t.Run("GetLastEntryID", func(t *testing.T) {
		lastEntryId, err := reader.GetLastEntryID(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(4), lastEntryId)
	})

	t.Run("ReadAllEntries", func(t *testing.T) {
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    int64(len(testData)),
		})
		require.NoError(t, err)
		assert.Equal(t, len(testData), len(entries))

		// Verify content
		for i, entry := range entries {
			assert.Equal(t, int64(i), entry.EntryId)
			assert.Equal(t, testData[i], entry.Values)
		}
	})

	t.Run("ReadFromMiddle", func(t *testing.T) {
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 2,
			BatchSize:    3,
		})
		require.NoError(t, err)
		assert.Equal(t, 3, len(entries))

		// Verify content
		for i, entry := range entries {
			expectedId := int64(2 + i)
			assert.Equal(t, expectedId, entry.EntryId)
			assert.Equal(t, testData[expectedId], entry.Values)
		}
	})

	t.Run("ReadAutoBatchMode", func(t *testing.T) {
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 1,
			BatchSize:    -1, // Auto batch mode
		})
		require.NoError(t, err)
		assert.Greater(t, len(entries), 0)

		// Should start from entry 1
		assert.Equal(t, int64(1), entries[0].EntryId)
		assert.Equal(t, testData[1], entries[0].Values)
	})

	t.Run("GetMetadata", func(t *testing.T) {
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

func TestLocalFileReader_MultipleBlocks(t *testing.T) {
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
	reader, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId, segmentId)
	require.NoError(t, err)
	defer reader.Close(ctx)

	t.Run("ReadAcrossMultipleBlocks", func(t *testing.T) {
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 5,
			BatchSize:    10, // Read across multiple blocks
		})
		require.NoError(t, err)
		assert.Equal(t, 10, len(entries))

		// Verify order and content
		for i, entry := range entries {
			expectedId := int64(5 + i)
			assert.Equal(t, expectedId, entry.EntryId)
			assert.Equal(t, testData[expectedId], entry.Values)
		}
	})

	t.Run("ReadSingleBlockMode", func(t *testing.T) {
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 8,
			BatchSize:    -1, // Auto batch mode - single block
		})
		require.NoError(t, err)
		assert.Greater(t, len(entries), 0)

		// Should start from entry 8
		assert.Equal(t, int64(8), entries[0].EntryId)
		assert.Equal(t, testData[8], entries[0].Values)
	})

	t.Run("VerifyBlockStructure", func(t *testing.T) {
		blockIndexes := reader.GetBlockIndexes()
		assert.Greater(t, len(blockIndexes), 1, "Should have multiple blocks")

		// Verify block index consistency
		for i, blockIndex := range blockIndexes {
			assert.Equal(t, int32(i), blockIndex.BlockNumber)
			assert.GreaterOrEqual(t, blockIndex.LastEntryID, blockIndex.FirstEntryID)
		}
	})
}

func TestLocalFileReader_ErrorHandling(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	blockSize := int64(256 * 1024)
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	t.Run("NonExistentFile", func(t *testing.T) {
		nonExistentPath := filepath.Join(tempDir, "non-existent.log")
		reader, err := disk.NewLocalFileReader(context.TODO(), nonExistentPath, 1000, 2000)
		assert.Error(t, err)
		assert.Nil(t, reader)
	})

	t.Run("InvalidEntryId", func(t *testing.T) {
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
		reader, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId, segmentId)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Test reading from non-existent entry ID
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 100, // Way beyond available entries
			BatchSize:    10,
		})
		assert.Error(t, err)
		assert.True(t, werr.ErrEntryNotFound.Is(err))
		assert.Nil(t, entries)
	})

	t.Run("ReadAfterClose", func(t *testing.T) {
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
		reader, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId, segmentId)
		require.NoError(t, err)

		err = reader.Close(ctx)
		require.NoError(t, err)

		// Try to read after close
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    1,
		})
		assert.Error(t, err)
		assert.True(t, werr.ErrFileReaderAlreadyClosed.Is(err))
		assert.Nil(t, entries)
	})
}

func TestLocalFileRW_DataIntegrityWithDifferentSizes(t *testing.T) {
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
	reader, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId, segmentId)
	require.NoError(t, err)
	defer reader.Close(ctx)

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
}

func TestLocalFileRW_EmptyPayloadValidation(t *testing.T) {
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
	t.Run("EmptyPayloadAtStorageLayer", func(t *testing.T) {
		// Try to write empty data directly to storage layer
		_, err := writer.WriteDataAsync(ctx, 0, []byte{}, channel.NewLocalResultChannel("test-empty-payload"))
		require.Error(t, err)
		assert.True(t, werr.ErrEmptyPayload.Is(err), "Error should be ErrEmptyPayload")

		t.Logf("Storage layer empty payload error: %v", err)
	})

	// Test empty payload validation at the client level (LogWriter)
	t.Run("EmptyPayloadAtClientLevel", func(t *testing.T) {
		emptyMsg := &log.WriterMessage{
			Payload:    []byte{},
			Properties: map[string]string{"test": "value"},
		}

		_, err := log.MarshalMessage(emptyMsg)
		require.NoError(t, err)
		t.Logf("Client level empty payload error: %v", err)
	})

	// Test nil payload validation
	t.Run("NilPayloadAtClientLevel", func(t *testing.T) {
		nilMsg := &log.WriterMessage{
			Payload:    nil,
			Properties: map[string]string{"test": "value"},
		}

		_, err := log.MarshalMessage(nilMsg)
		require.NoError(t, err)

		t.Logf("Client level nil payload error: %v", err)
	})

	// Test both empty err
	t.Run("BothEmptyMsg", func(t *testing.T) {
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

// BenchmarkLocalFileWriter_WriteDataAsync benchmarks write performance
func BenchmarkLocalFileWriter_WriteDataAsync(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "woodpecker-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	ctx := context.Background()
	blockSize := int64(2 * 1024 * 1024) // 2MB
	cfg, _ := config.NewConfiguration("../../config/woodpecker.yaml")
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	logId := int64(17)
	segmentId := int64(1700)
	writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
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

// BenchmarkLocalFileReader_ReadNextBatch benchmarks read performance
func BenchmarkLocalFileReader_ReadNextBatch(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "woodpecker-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	ctx := context.Background()
	blockSize := int64(2 * 1024 * 1024) // 2MB
	cfg, _ := config.NewConfiguration("../../config/woodpecker.yaml")
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	// Prepare test data
	logId := int64(18)
	segmentId := int64(1800)
	writer, err := disk.NewLocalFileWriter(context.TODO(), tempDir, logId, segmentId, cfg)
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
	reader, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId, segmentId)
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

func TestLocalFileRW_BlockHeaderRecordVerification(t *testing.T) {
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
	reader, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId, segmentId)
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
func TestLocalFileRW_WriteInterruptionAndRecovery(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(256 * 1024) // 256KB per block
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = blockSize

	// Phase 1: Write some data and simulate interruption
	t.Run("WriteDataAndInterrupt", func(t *testing.T) {
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
	t.Run("RecoverAndContinueWriting", func(t *testing.T) {
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
	t.Run("VerifyRecoveredFileComplete", func(t *testing.T) {
		// Create reader to verify the file is properly finalized
		// Use the logId and segmentId from the RecoverAndContinueWriting test (21, 2100)
		reader, err := disk.NewLocalFileReader(context.TODO(), tempDir, 20, 2000)
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
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    10, // Read all entries
		})
		require.NoError(t, err)
		require.Equal(t, 8, len(entries), "Should have 8 total entries (4 original + 4 recovery)")

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

		for i, entry := range entries {
			assert.Equal(t, int64(i), entry.EntryId, "Entry ID should match index")
			if expectedEntries[i] != "" {
				assert.Equal(t, expectedEntries[i], string(entry.Values), "Entry content should match")
			}
		}

		err = reader.Close(ctx)
		require.NoError(t, err)
	})

	// Phase 4: Test that trying to recover from a finalized file success
	t.Run("CanRecoverFromFinalizedFile", func(t *testing.T) {
		// Try to create a recovery writer from the finalized file
		logId := int64(20)
		segmentId := int64(2000)
		_, err := disk.NewLocalFileWriterWithMode(context.TODO(), tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err, "Should be able to recover from a finalized file")
	})

	// Phase 5: Test recovery from empty file
	t.Run("RecoveryFromEmptyFile", func(t *testing.T) {
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
	t.Run("RecoveryFromNonExistentFile", func(t *testing.T) {
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
	t.Run("RecoveryFromCorruptedFile", func(t *testing.T) {
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
func TestLocalFileReader_ReadIncompleteFile(t *testing.T) {
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
	t.Run("ReadIncompleteFileWithFullScan", func(t *testing.T) {
		reader, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId, segmentId)
		require.NoError(t, err)
		require.NotNil(t, reader)
		defer reader.Close(ctx)

		// Verify that footer is nil (file is incomplete)
		footer := reader.GetFooter()
		assert.Nil(t, footer, "Footer should be nil for incomplete file")

		// Verify that block indexes were built from full scan
		blockIndexes := reader.GetBlockIndexes()
		assert.Greater(t, len(blockIndexes), 0, "Should have block indexes from full scan")

		// Verify we can get the last entry ID
		lastEntryId, err := reader.GetLastEntryID(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(3), lastEntryId)

		// Verify we can read all entries
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    int64(len(testData)),
		})
		require.NoError(t, err)
		assert.Equal(t, len(testData), len(entries))

		// Verify content
		for i, entry := range entries {
			assert.Equal(t, int64(i), entry.EntryId)
			assert.Equal(t, testData[i], entry.Values)
		}

		// Verify we can read from middle
		entries, err = reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 1,
			BatchSize:    2,
		})
		require.NoError(t, err)
		assert.Equal(t, 2, len(entries))
		assert.Equal(t, int64(1), entries[0].EntryId)
		assert.Equal(t, testData[1], entries[0].Values)
		assert.Equal(t, int64(2), entries[1].EntryId)
		assert.Equal(t, testData[2], entries[1].Values)

		// Verify auto batch mode works
		entries, err = reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 2,
			BatchSize:    -1, // Auto batch mode
		})
		require.NoError(t, err)
		assert.Greater(t, len(entries), 0)
		assert.Equal(t, int64(2), entries[0].EntryId)
		assert.Equal(t, testData[2], entries[0].Values)
	})

	t.Run("CompareWithFinalizedFile", func(t *testing.T) {
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
		reader2, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId2, segmentId2)
		require.NoError(t, err)
		defer reader2.Close(ctx)

		// Verify that footer exists (file is complete)
		footer2 := reader2.GetFooter()
		assert.NotNil(t, footer2, "Footer should exist for finalized file")

		// Read incomplete file
		reader1, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId, segmentId)
		require.NoError(t, err)
		defer reader1.Close(ctx)

		// Compare data from both files
		entries1, err := reader1.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    int64(len(testData)),
		})
		require.NoError(t, err)

		entries2, err := reader2.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    int64(len(testData)),
		})
		require.NoError(t, err)

		// Data should be identical
		assert.Equal(t, len(entries1), len(entries2))
		for i := range entries1 {
			assert.Equal(t, entries1[i].EntryId, entries2[i].EntryId)
			assert.Equal(t, entries1[i].Values, entries2[i].Values)
		}
	})
}

// TestLocalFileReader_DynamicScanning tests dynamic scanning of incomplete files
func TestLocalFileReader_DynamicScanning(t *testing.T) {
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
	reader, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId, segmentId)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close(ctx)

	t.Run("ReadInitialData", func(t *testing.T) {
		// Read initial data
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    2,
		})
		require.NoError(t, err)
		assert.Equal(t, 2, len(entries))

		// Verify content
		for i, entry := range entries {
			assert.Equal(t, int64(i), entry.EntryId)
			assert.Equal(t, initialData[i], entry.Values)
		}
	})

	t.Run("TryReadBeyondAvailableData", func(t *testing.T) {
		// Try to read beyond available data - should return what's available
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 1,
			BatchSize:    5, // Request more than available
		})
		require.NoError(t, err)
		assert.Equal(t, 1, len(entries)) // Should only get entry 1

		assert.Equal(t, int64(1), entries[0].EntryId)
		assert.Equal(t, initialData[1], entries[0].Values)
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

	t.Run("ReadNewDataAfterDynamicScan", func(t *testing.T) {
		// Now try to read the new data - should trigger dynamic scanning
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 2,
			BatchSize:    3, // Request the 3 new entries
		})
		require.NoError(t, err)
		assert.Equal(t, 3, len(entries))

		// Verify new content
		for i, entry := range entries {
			expectedId := int64(2 + i)
			assert.Equal(t, expectedId, entry.EntryId)
			assert.Equal(t, moreData[i], entry.Values)
		}
	})

	t.Run("ReadAllDataAfterDynamicScan", func(t *testing.T) {
		// Read all data from the beginning
		allData := append(initialData, moreData...)
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    int64(len(allData)),
		})
		require.NoError(t, err)
		assert.Equal(t, len(allData), len(entries))

		// Verify all content
		for i, entry := range entries {
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

	t.Run("ReadFinalDataWithDynamicScan", func(t *testing.T) {
		// Read the final data
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 5,
			BatchSize:    2,
		})
		require.NoError(t, err)
		assert.Equal(t, 2, len(entries))

		// Verify final content
		for i, entry := range entries {
			expectedId := int64(5 + i)
			assert.Equal(t, expectedId, entry.EntryId)
			assert.Equal(t, finalData[i], entry.Values)
		}
	})

	t.Run("ReadFromMiddleWithDynamicScan", func(t *testing.T) {
		// Read from middle, spanning across dynamically scanned blocks
		entries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 3,
			BatchSize:    4, // Should get entries 3, 4, 5, 6
		})
		require.NoError(t, err)
		assert.Equal(t, 4, len(entries))

		expectedData := append(moreData[1:], finalData...)
		for i, entry := range entries {
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
func TestLocalFileRW_ConcurrentReadWrite(t *testing.T) {
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
		reader, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId, segmentId)
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

		// Final verification read after writer is done
		t.Logf("Reader: Doing final verification read")

		// Wait a bit for any pending writes to complete
		time.Sleep(200 * time.Millisecond)

		finalEntries, err := reader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    int64(totalEntries),
		})

		if err != nil {
			t.Errorf("Reader: Failed final read: %v", err)
			return
		}

		t.Logf("Reader: Final read got %d entries", len(finalEntries))

		// Verify all entries were read
		for i := 0; i < totalEntries; i++ {
			if i < len(finalEntries) {
				readEntries[int64(i)] = finalEntries[i].Values
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
	t.Run("FinalVerification", func(t *testing.T) {
		finalReader, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId, segmentId)
		require.NoError(t, err)
		defer finalReader.Close(ctx)

		// Verify file is incomplete (no footer)
		footer := finalReader.GetFooter()
		assert.Nil(t, footer, "File should be incomplete (no footer)")

		// Verify we can read all entries
		allEntries, err := finalReader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    int64(totalEntries),
		})
		require.NoError(t, err)
		assert.Equal(t, totalEntries, len(allEntries), "Should read all entries")

		// Verify entry content and order
		for i, entry := range allEntries {
			assert.Equal(t, int64(i), entry.EntryId, "Entry ID should match")
			assert.Contains(t, string(entry.Values), fmt.Sprintf("Entry %d:", i),
				"Entry content should contain expected prefix")
		}

		// Verify dynamic scanning works by reading from different positions
		midEntries, err := finalReader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: int64(totalEntries / 2),
			BatchSize:    5,
		})
		require.NoError(t, err)
		assert.Equal(t, 5, len(midEntries), "Should read 5 entries from middle")

		lastEntryId, err := finalReader.GetLastEntryID(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(totalEntries-1), lastEntryId, "Last entry ID should match")
	})
}

// TestLocalFileRW_ConcurrentOneWriteMultipleReads tests one writer with multiple concurrent readers
func TestLocalFileRW_ConcurrentOneWriteMultipleReads(t *testing.T) {
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
			reader, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId, segmentId)
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
			time.Sleep(200 * time.Millisecond)

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
	t.Run("VerifyReaderConsistency", func(t *testing.T) {
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
	t.Run("FinalFileVerification", func(t *testing.T) {
		finalReader, err := disk.NewLocalFileReader(context.TODO(), tempDir, logId, segmentId)
		require.NoError(t, err)
		defer finalReader.Close(ctx)

		// Verify file is incomplete (no footer)
		footer := finalReader.GetFooter()
		assert.Nil(t, footer, "File should be incomplete (no footer)")

		// Verify we can read all entries
		allEntries, err := finalReader.ReadNextBatch(ctx, storage.ReaderOpt{
			StartEntryID: 0,
			BatchSize:    int64(totalEntries),
		})
		require.NoError(t, err)
		assert.Equal(t, totalEntries, len(allEntries), "Should read all entries")

		// Verify entry content and order
		for i, entry := range allEntries {
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

// Helper function for min calculation
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

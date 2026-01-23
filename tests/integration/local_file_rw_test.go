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
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = config.NewByteSize(blockSize)

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
		lastEntryId, err := writer.Finalize(ctx, -1)
		require.NoError(t, err)
		assert.Equal(t, int64(3), lastEntryId)
	})

	// Close writer
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Verify per-block files exist (footer.blk should exist since we finalized)
	segmentDir := filepath.Join(tempDir, fmt.Sprintf("%d/%d", logId, segmentId))
	footerPath := filepath.Join(segmentDir, "footer.blk")
	_, err = os.Stat(footerPath)
	require.NoError(t, err, "footer.blk should exist after finalize")
}

func TestLocalFileWriter_LargeDataAndMultipleBlocks(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(128 * 1024) // 128KB per block to test multi-block scenario
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = config.NewByteSize(blockSize)

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
	_, err = writer.Finalize(ctx, -1)
	require.NoError(t, err)
}

func TestLocalFileWriter_ConcurrentWrites(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(256 * 1024)
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = config.NewByteSize(blockSize)

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
	_, err = writer.Finalize(ctx, -1)
	require.NoError(t, err)
}

func TestLocalFileWriter_ErrorHandling(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(256 * 1024)
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = config.NewByteSize(blockSize)

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

		_, err = writer.Finalize(ctx, -1)
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

func TestLocalFileRW_EmptyPayloadValidation(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()

	blockSize := int64(256 * 1024)
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = config.NewByteSize(blockSize)

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
		emptyMsg := &log.WriteMessage{
			Payload:    []byte{},
			Properties: map[string]string{"test": "value"},
		}

		_, err := log.MarshalMessage(emptyMsg)
		require.NoError(t, err)
		t.Logf("Client level empty payload error: %v", err)
	})

	// Test nil payload validation
	t.Run("NilPayloadAtClientLevel", func(t *testing.T) {
		nilMsg := &log.WriteMessage{
			Payload:    nil,
			Properties: map[string]string{"test": "value"},
		}

		_, err := log.MarshalMessage(nilMsg)
		require.NoError(t, err)

		t.Logf("Client level nil payload error: %v", err)
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
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = config.NewByteSize(blockSize)

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
func BenchmarkLocalFileReader_ReadNextBatchAdv(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "woodpecker-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	ctx := context.Background()
	blockSize := int64(2 * 1024 * 1024) // 2MB
	cfg, _ := config.NewConfiguration("../../config/woodpecker.yaml")
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = config.NewByteSize(blockSize)

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

	_, err = writer.Finalize(ctx, -1)
	if err != nil {
		b.Fatal(err)
	}
	err = writer.Close(ctx)
	if err != nil {
		b.Fatal(err)
	}

	// Benchmark reading
	reader, err := disk.NewLocalFileReaderAdv(context.TODO(), tempDir, logId, segmentId, 16_000_000)
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		startId := int64(i % 900) // Ensure we don't go beyond available entries
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    startId,
			MaxBatchEntries: 10,
		}, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(batch.Entries) == 0 {
			b.Fatal("No entries returned")
		}
	}
}

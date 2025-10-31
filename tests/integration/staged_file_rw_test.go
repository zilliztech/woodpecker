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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/stagedstorage"
	"github.com/zilliztech/woodpecker/tests/utils"
)

var (
	StagedTestBucket = "a-bucket"
)

func setupStagedFileTest(t *testing.T, rootDir string) (storageclient.ObjectStorage, *config.Configuration, string) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	// Set log level to debug for detailed logging
	cfg.Log.Level = "debug"

	// Initialize logger with debug level
	logger.InitLogger(cfg)

	StagedTestBucket = cfg.Minio.BucketName
	currentTime := time.Now().Unix()
	cfg.Minio.RootPath = rootDir
	storageCli, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)

	// Create a temporary directory for local files
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("%s-%d", rootDir, currentTime))
	err = os.MkdirAll(tempDir, 0755)
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	return storageCli, cfg, tempDir
}

func cleanupStagedTestObjects(t *testing.T, client storageclient.ObjectStorage, prefix string) {
	ctx := context.Background()
	err := client.WalkWithObjects(ctx, StagedTestBucket, prefix, true, func(objInfo *storageclient.ChunkObjectInfo) bool {
		err := client.RemoveObject(ctx, StagedTestBucket, objInfo.FilePath)
		if err != nil {
			t.Logf("Warning: failed to cleanup object %s: %v", objInfo.FilePath, err)
		}
		return true // Continue walking
	})
	if err != nil {
		t.Logf("Warning: failed to walk objects for cleanup: %v", err)
	}
}

func generateStagedTestData(size int) []byte {
	data, err := utils.GenerateRandomBytes(size)
	if err != nil {
		panic(err)
	}
	return data
}

func TestStagedFileWriter_BasicWriteAndSync(t *testing.T) {
	rootPath := fmt.Sprintf("test-staged-basic-write-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()

	logId := int64(1)
	segmentId := int64(100)
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	// Create QuorumFileWriter
	writer, err := stagedstorage.NewStagedFileWriter(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
	require.NoError(t, err)
	require.NotNil(t, writer)

	t.Run("WriteDataAsync", func(t *testing.T) {
		// Test writing data
		testData := [][]byte{
			[]byte("Hello, Quorum!"),
			[]byte("This is test data"),
			generateStagedTestData(1024), // 1KB
			generateStagedTestData(2048), // 2KB
		}

		for i, data := range testData {
			entryId := int64(i)
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-staged-basic-%d", entryId))

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

	// Check that files exist locally (not compacted yet)
	segmentDir := filepath.Join(tempDir, fmt.Sprintf("%d/%d", logId, segmentId))
	assert.DirExists(t, segmentDir)

	// Close writer
	err = writer.Close(ctx)
	require.NoError(t, err)
}

func TestStagedFileWriter_CompactOperation(t *testing.T) {
	rootDir := fmt.Sprintf("test-staged-compact-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootDir)
	ctx := context.Background()

	// Configure for smaller blocks to test compaction
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 256 * 1024 // 256KB per block

	logId := int64(2)
	segmentId := int64(200)
	defer cleanupStagedTestObjects(t, storageCli, rootDir)

	writer, err := stagedstorage.NewStagedFileWriter(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
	require.NoError(t, err)
	defer writer.Close(ctx)

	// Write data that will span multiple blocks
	largeData := [][]byte{
		generateStagedTestData(200 * 1024), // 200KB
		generateStagedTestData(200 * 1024), // 200KB - should trigger new block
		generateStagedTestData(100 * 1024), // 100KB
		generateStagedTestData(150 * 1024), // 150KB - should trigger another block
		[]byte("Final small entry"),
	}

	for i, data := range largeData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-compact-%d", i))
		returnedId, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)
		assert.Equal(t, int64(i), returnedId)

		// Wait for result
		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(i), result.SyncedId)
	}

	// Finalize first to complete the local segment
	lastEntryId, err := writer.Finalize(ctx, int64(len(largeData))-1)
	require.NoError(t, err)
	assert.Equal(t, int64(len(largeData)-1), lastEntryId)

	// Now test compact operation
	t.Run("CompactOperation", func(t *testing.T) {
		compactedSize, err := writer.Compact(ctx)
		require.NoError(t, err)
		assert.Greater(t, compactedSize, int64(0), "Should have compacted some data")

		t.Logf("Compacted size: %d bytes", compactedSize)

		// Verify objects were created in MinIO
		prefix := fmt.Sprintf("%s/%d/%d", rootDir, logId, segmentId)
		var objectCount int
		var hasFooter bool
		var hasMergedBlocks bool

		walkErr := storageCli.WalkWithObjects(ctx, StagedTestBucket, prefix, true, func(objInfo *storageclient.ChunkObjectInfo) bool {
			objectCount++
			t.Logf("Compacted object: %s", objInfo.FilePath)

			if strings.HasSuffix(objInfo.FilePath, "footer.blk") {
				hasFooter = true
			}
			if strings.Contains(objInfo.FilePath, "/m_") && strings.HasSuffix(objInfo.FilePath, ".blk") {
				hasMergedBlocks = true
			}
			return true // Continue walking
		})
		require.NoError(t, walkErr)

		assert.Greater(t, objectCount, 0, "Should have created objects in MinIO")
		assert.True(t, hasFooter, "Should have footer object")
		assert.True(t, hasMergedBlocks, "Should have merged block objects")
	})
}

func TestStagedFileReader_ReadLocalAndCompacted(t *testing.T) {
	rootDir := fmt.Sprintf("test-staged-reader-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootDir)
	ctx := context.Background()

	// Configure for smaller blocks
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 256 * 1024 // 256KB per block
	cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize = 16_000_000
	cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxFetchThreads = 32

	logId := int64(3)
	segmentId := int64(300)

	defer cleanupStagedTestObjects(t, storageCli, rootDir)

	// Step 1: Write and finalize data
	writer, err := stagedstorage.NewStagedFileWriter(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
	require.NoError(t, err)

	testData := [][]byte{
		[]byte("Entry 0: Local read test"),
		generateStagedTestData(100 * 1024), // 100KB
		[]byte("Entry 2: Another test"),
		generateStagedTestData(200 * 1024), // 200KB - should trigger new block
		[]byte("Entry 4: Final entry"),
	}

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-reader-write-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	_, err = writer.Finalize(ctx, int64(len(testData))-1)
	require.NoError(t, err)

	// Step 2: Test reading from local files (before compaction)
	t.Run("ReadFromLocal", func(t *testing.T) {
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)
		_ = reader.UpdateLastAddConfirmed(ctx, int64(len(testData))-1) // set lac to simulate that the test data has been persisted

		// Read all entries
		result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 10,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, len(testData), len(result.Entries))

		for i, entry := range result.Entries {
			assert.Equal(t, int64(i), entry.EntryId)
			assert.Equal(t, testData[i], entry.Values)
		}
	})

	// Step 3: Compact the data
	compactedSize, err := writer.Compact(ctx)
	require.NoError(t, err)
	assert.Greater(t, compactedSize, int64(0))

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Step 4: Test reading from compacted data (MinIO)
	t.Run("ReadFromCompacted", func(t *testing.T) {
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Read all entries from compacted data
		result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 10,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, len(testData), len(result.Entries))

		for i, entry := range result.Entries {
			assert.Equal(t, int64(i), entry.EntryId)
			assert.Equal(t, testData[i], entry.Values)
		}
	})
}

func TestStagedFileWriter_ConcurrentWrites(t *testing.T) {
	rootDir := fmt.Sprintf("test-staged-concurrent-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootDir)
	ctx := context.Background()

	logId := int64(4)
	segmentId := int64(400)

	defer cleanupStagedTestObjects(t, storageCli, rootDir)

	writer, err := stagedstorage.NewStagedFileWriter(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
	require.NoError(t, err)
	defer writer.Close(ctx)

	// Test sequential writes (writer requires entryId to be sequential starting from 0)
	const totalEntries = 6

	var wg sync.WaitGroup
	errors := make([]error, totalEntries)
	var errorMutex sync.Mutex

	// Write entries sequentially from 0 to totalEntries-1
	for i := 0; i < totalEntries; i++ {
		wg.Add(1)
		go func(entryId int64) {
			defer wg.Done()

			data := []byte(fmt.Sprintf("Entry %d: %s", entryId, generateStagedTestData(256)))

			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-staged-concurrent-%d", entryId))
			_, err := writer.WriteDataAsync(ctx, entryId, data, resultCh)

			if err != nil {
				errorMutex.Lock()
				errors[entryId] = err
				errorMutex.Unlock()
				return
			}

			// Wait for result with timeout to avoid hang
			timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			result, err := resultCh.ReadResult(timeoutCtx)
			cancel()

			errorMutex.Lock()
			if err != nil {
				errors[entryId] = err
			} else {
				errors[entryId] = result.Err
			}
			errorMutex.Unlock()
		}(int64(i))
	}

	wg.Wait()

	// Check results
	successCount := 0
	for i, err := range errors {
		if err == nil {
			successCount++
		} else {
			t.Logf("Write error %d: %v", i, err)
		}
	}

	expectedEntries := totalEntries
	assert.Equal(t, expectedEntries, successCount, "All sequential writes should succeed")
	t.Logf("Successful sequential writes: %d out of %d", successCount, expectedEntries)

	// Verify final state
	assert.Equal(t, int64(0), writer.GetFirstEntryId(ctx))
	assert.Equal(t, int64(expectedEntries-1), writer.GetLastEntryId(ctx))
}

func TestStagedFileWriter_ErrorHandling(t *testing.T) {
	t.Run("EmptyPayload", func(t *testing.T) {
		rootDir := fmt.Sprintf("test-staged-empty-payload-%d", time.Now().Unix())
		storageCli, cfg, tempDir := setupStagedFileTest(t, rootDir)
		ctx := context.Background()
		logId := int64(5)
		segmentId := int64(500)

		defer cleanupStagedTestObjects(t, storageCli, rootDir)

		writer, err := stagedstorage.NewStagedFileWriter(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
		require.NoError(t, err)
		defer writer.Close(ctx)

		// Try to write empty data
		_, err = writer.WriteDataAsync(ctx, 0, []byte{}, channel.NewLocalResultChannel("test-empty-payload"))
		require.Error(t, err)
		assert.True(t, werr.ErrEmptyPayload.Is(err), "Error should be ErrEmptyPayload")
	})

	t.Run("WriteAfterFinalize", func(t *testing.T) {
		rootDir := fmt.Sprintf("test-staged-write-after-finalize-%d", time.Now().Unix())
		storageCli, cfg, tempDir := setupStagedFileTest(t, rootDir)
		ctx := context.Background()

		logId := int64(6)
		segmentId := int64(600)
		defer cleanupStagedTestObjects(t, storageCli, rootDir)

		writer, err := stagedstorage.NewStagedFileWriter(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
		require.NoError(t, err)
		defer writer.Close(ctx)

		// Write some data first
		resultCh := channel.NewLocalResultChannel("test-before-finalize")
		_, err = writer.WriteDataAsync(ctx, 0, []byte("test data"), resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)

		// Finalize
		_, err = writer.Finalize(ctx, 0)
		require.NoError(t, err)

		// Try to write after finalize
		resultCh2 := channel.NewLocalResultChannel("test-after-finalize")
		_, err = writer.WriteDataAsync(ctx, 1, []byte("should fail"), resultCh2)
		assert.Error(t, err, "Should not be able to write after finalize")
	})

	t.Run("CompactBeforeFinalize", func(t *testing.T) {
		rootDir := fmt.Sprintf("test-staged-compact-before-finalize-%d", time.Now().Unix())
		storageCli, cfg, tempDir := setupStagedFileTest(t, rootDir)
		ctx := context.Background()

		logId := int64(7)
		segmentId := int64(700)
		defer cleanupStagedTestObjects(t, storageCli, rootDir)

		writer, err := stagedstorage.NewStagedFileWriter(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
		require.NoError(t, err)
		defer writer.Close(ctx)

		// Write some data
		resultCh := channel.NewLocalResultChannel("test-compact-before-finalize")
		_, err = writer.WriteDataAsync(ctx, 0, []byte("test data"), resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)

		// Try to compact before finalize
		_, err = writer.Compact(ctx)
		assert.Error(t, err, "Should not be able to compact before finalize")
	})
}

func TestStagedFileReader_ErrorHandling(t *testing.T) {
	rootDir := fmt.Sprintf("test-staged-reader-errorhandling-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootDir)
	ctx := context.Background()

	t.Run("ReadNonExistentSegment", func(t *testing.T) {
		logId := int64(8)
		segmentId := int64(800)

		// Try to read from non-existent segment
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
		require.Error(t, err)
		assert.True(t, werr.ErrEntryNotFound.Is(err))
		assert.Nil(t, reader)
	})
}

func TestStagedFileRW_DataIntegrityAcrossStates(t *testing.T) {
	rootDir := fmt.Sprintf("test-staged-integrity-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootDir)
	ctx := context.Background()

	// Configure for smaller blocks to ensure we test both local and compacted reading
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 128 * 1024 // 128KB per block
	cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize = 16_000_000
	cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxFetchThreads = 32

	logId := int64(9)
	segmentId := int64(900)
	defer cleanupStagedTestObjects(t, storageCli, rootDir)

	// Generate test data with different sizes
	testData := [][]byte{
		[]byte("Small entry 0"),
		generateStagedTestData(50 * 1024), // 50KB
		[]byte("Small entry 2"),
		generateStagedTestData(100 * 1024), // 100KB - should trigger new block
		[]byte("Small entry 4"),
		generateStagedTestData(80 * 1024), // 80KB
		[]byte("Final entry"),
	}

	// Step 1: Write all data
	writer, err := stagedstorage.NewStagedFileWriter(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
	require.NoError(t, err)

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-integrity-write-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	_, err = writer.Finalize(ctx, int64(len(testData))-1)
	require.NoError(t, err)

	// Step 2: Read from local files and verify data integrity
	t.Run("LocalDataIntegrity", func(t *testing.T) {
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)
		_ = reader.UpdateLastAddConfirmed(ctx, int64(len(testData))-1) // set lac to simulate that the test data has been persisted

		// Read all entries
		result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 20,
		}, nil)
		require.NoError(t, err)
		require.Equal(t, len(testData), len(result.Entries))

		// Verify data integrity
		for i, entry := range result.Entries {
			assert.Equal(t, int64(i), entry.EntryId, "Entry ID mismatch at index %d", i)
			assert.Equal(t, testData[i], entry.Values, "Data mismatch at index %d", i)
		}
	})

	// Step 3: Compact the data
	compactedSize, err := writer.Compact(ctx)
	require.NoError(t, err)
	assert.Greater(t, compactedSize, int64(0))

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Step 4: Read from compacted data and verify data integrity
	t.Run("CompactedDataIntegrity", func(t *testing.T) {
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Read all entries from compacted data
		result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 20,
		}, nil)
		require.NoError(t, err)
		require.Equal(t, len(testData), len(result.Entries))

		// Verify data integrity after compaction
		for i, entry := range result.Entries {
			assert.Equal(t, int64(i), entry.EntryId, "Entry ID mismatch at index %d after compaction", i)
			assert.Equal(t, testData[i], entry.Values, "Data mismatch at index %d after compaction", i)
		}
	})

	// Step 5: Test partial reads from different starting points
	t.Run("PartialReadsFromCompacted", func(t *testing.T) {
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Test reading from different start points
		testCases := []struct {
			startEntryId int64
			maxEntries   int64
		}{
			{0, 3},  // Read first 3 entries
			{2, 3},  // Read middle 3 entries
			{5, 10}, // Read last entries
		}

		for _, tc := range testCases {
			result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
				StartEntryID:    tc.startEntryId,
				MaxBatchEntries: tc.maxEntries,
			}, nil)
			require.NoError(t, err)

			// Verify each returned entry
			for i, entry := range result.Entries {
				expectedEntryId := tc.startEntryId + int64(i)
				if expectedEntryId < int64(len(testData)) {
					assert.Equal(t, expectedEntryId, entry.EntryId, "Entry ID mismatch in partial read")
					assert.Equal(t, testData[expectedEntryId], entry.Values, "Data mismatch in partial read")
				}
			}
		}
	})
}

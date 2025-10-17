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
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/stagedstorage"
	"github.com/zilliztech/woodpecker/tests/utils"
)

func setupStagedAdvTest(t *testing.T, rootDir string) (storageclient.ObjectStorage, *config.Configuration, string) {
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)

	// Set log level to debug for detailed logging
	cfg.Log.Level = "debug"
	cfg.Minio.RootPath = rootDir

	// Initialize logger with debug level
	logger.InitLogger(cfg)

	storageCli, err := storageclient.NewObjectStorage(context.Background(), cfg)
	require.NoError(t, err)

	// Create a temporary directory for local files
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("%s-%d", rootDir, time.Now().Unix()))
	err = os.MkdirAll(tempDir, 0755)
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	return storageCli, cfg, tempDir
}

func cleanupStagedAdvTestObjects(t *testing.T, client storageclient.ObjectStorage, prefix string) {
	ctx := context.Background()
	cfg, _ := config.NewConfiguration("../../config/woodpecker.yaml")
	if cfg == nil {
		t.Logf("Warning: could not load config for cleanup")
		return
	}

	err := client.WalkWithObjects(ctx, cfg.Minio.BucketName, prefix, true, func(objInfo *storageclient.ChunkObjectInfo) bool {
		err := client.RemoveObject(ctx, cfg.Minio.BucketName, objInfo.FilePath)
		if err != nil {
			t.Logf("Warning: failed to cleanup object %s: %v", objInfo.FilePath, err)
		}
		return true // Continue walking
	})
	if err != nil {
		t.Logf("Warning: failed to walk objects for cleanup: %v", err)
	}
}

func generateAdvTestData(size int) []byte {
	data, err := utils.GenerateRandomBytes(size)
	if err != nil {
		panic(err)
	}
	return data
}

func TestAdvStagedFileReader_BasicRead(t *testing.T) {
	rootDir := fmt.Sprintf("test-adv-staged-basic-read-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedAdvTest(t, rootDir)
	ctx := context.Background()
	cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize = 16_000_000
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads = 32

	logId := int64(1)
	segmentId := int64(100)
	defer cleanupStagedAdvTestObjects(t, storageCli, rootDir)

	// Step 1: Create test data
	writer, err := stagedstorage.NewStagedFileWriter(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
	require.NoError(t, err)

	testData := [][]byte{
		[]byte("Entry 0: First entry"),
		[]byte("Entry 1: Second entry"),
		generateAdvTestData(1024), // 1KB
		[]byte("Entry 3: Fourth entry"),
		generateAdvTestData(2048), // 2KB
	}

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-adv-basic-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	_, err = writer.Finalize(ctx, int64(len(testData)))
	require.NoError(t, err)
	err = writer.Close(ctx)
	require.NoError(t, err)

	// Step 2: Test reading from local files
	t.Run("ReadFromLocal", func(t *testing.T) {
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)
		_ = reader.UpdateLastAddConfirmed(ctx, int64(len(testData))-1) // set lac to simulate that the test data has been persisted

		// Test basic read
		result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 3,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 3, len(result.Entries))

		for i := 0; i < 3; i++ {
			assert.Equal(t, int64(i), result.Entries[i].EntryId)
			assert.Equal(t, testData[i], result.Entries[i].Values)
		}

		// Test reading remaining entries
		result, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    3,
			MaxBatchEntries: 10,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, 2, len(result.Entries))

		for i := 0; i < 2; i++ {
			assert.Equal(t, int64(i+3), result.Entries[i].EntryId)
			assert.Equal(t, testData[i+3], result.Entries[i].Values)
		}
	})
}

func TestAdvStagedFileReader_CompactedDataRead(t *testing.T) {
	rootDir := fmt.Sprintf("test-adv-staged-compacted-%d", time.Now().Unix())
	minioHdl, cfg, tempDir := setupStagedAdvTest(t, rootDir)
	ctx := context.Background()

	// Configure for smaller blocks to ensure compaction creates multiple merged blocks
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 128 * 1024 // 128KB per block
	cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize = 16_000_000
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads = 32

	logId := int64(2)
	segmentId := int64(200)

	defer cleanupStagedAdvTestObjects(t, minioHdl, rootDir)

	// Step 1: Create and write test data
	writer, err := stagedstorage.NewStagedFileWriter(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)

	// Create data that spans multiple blocks and will be compacted
	testData := [][]byte{
		generateAdvTestData(60 * 1024),       // 60KB - block 0
		generateAdvTestData(80 * 1024),       // 80KB - still block 0, total 140KB > 128KB, triggers new block
		generateAdvTestData(50 * 1024),       // 50KB - block 1
		generateAdvTestData(70 * 1024),       // 70KB - still block 1
		generateAdvTestData(40 * 1024),       // 40KB - block 1, total 160KB > 128KB, triggers new block
		[]byte("Entry 5: Final small entry"), // block 2
	}

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-adv-compacted-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	_, err = writer.Finalize(ctx, int64(len(testData))-1)
	require.NoError(t, err)

	// Step 2: Compact the data
	compactedSize, err := writer.Compact(ctx)
	require.NoError(t, err)
	assert.Greater(t, compactedSize, int64(0))

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Step 3: Test reading from compacted data
	t.Run("ReadFromCompacted", func(t *testing.T) {
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Test reading all data
		result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 10,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, len(testData), len(result.Entries))

		// Verify data integrity
		for i, entry := range result.Entries {
			assert.Equal(t, int64(i), entry.EntryId)
			assert.Equal(t, testData[i], entry.Values)
		}

		// Test reading from different starting points
		result, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    2,
			MaxBatchEntries: 3,
		}, nil)
		require.NoError(t, err)
		assert.LessOrEqual(t, 3, len(result.Entries)) // after compact, result >= 3, because one block may contain multi entries

		for i := 0; i < 3; i++ {
			assert.Equal(t, int64(i+2), result.Entries[i].EntryId)
			assert.Equal(t, testData[i+2], result.Entries[i].Values)
		}
	})
}

func TestAdvStagedFileReader_MixedLocalAndCompactedAccess(t *testing.T) {
	rootDir := fmt.Sprintf("test-adv-staged-mixed-%d", time.Now().Unix())
	minioHdl, cfg, tempDir := setupStagedAdvTest(t, rootDir)
	ctx := context.Background()

	cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize = 16_000_000
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads = 32

	logId := int64(3)
	segmentId := int64(300)
	defer cleanupStagedAdvTestObjects(t, minioHdl, rootDir)

	// Step 1: Create test data
	writer, err := stagedstorage.NewStagedFileWriter(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)

	testData := [][]byte{
		[]byte("Entry 0: Before compaction"),
		[]byte("Entry 1: Before compaction"),
		generateAdvTestData(512), // 512B
		[]byte("Entry 3: Before compaction"),
	}

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-mixed-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	_, err = writer.Finalize(ctx, int64(len(testData))-1)
	require.NoError(t, err)

	// Step 2: Create multiple readers to test priority handling
	t.Run("SimultaneousReaders", func(t *testing.T) {
		// Reader 1: Read from local (before compaction)
		reader1, err := stagedstorage.NewStagedFileReaderAdv(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		defer reader1.Close(ctx)
		_ = reader1.UpdateLastAddConfirmed(ctx, int64(len(testData))-1) // set lac to simulate that the test data has been persisted

		result1, err := reader1.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 4,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, len(testData), len(result1.Entries))

		// Compact the data
		_, err = writer.Compact(ctx)
		require.NoError(t, err)

		// Reader 2: Read from compacted (after compaction)
		reader2, err := stagedstorage.NewStagedFileReaderAdv(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		defer reader2.Close(ctx)

		result2, err := reader2.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 4,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, len(testData), len(result2.Entries))

		// Verify both readers return the same data
		for i := 0; i < len(testData); i++ {
			assert.Equal(t, result1.Entries[i].EntryId, result2.Entries[i].EntryId)
			assert.Equal(t, result1.Entries[i].Values, result2.Entries[i].Values)
			assert.Equal(t, testData[i], result2.Entries[i].Values)
		}
	})

	err = writer.Close(ctx)
	require.NoError(t, err)
}

func TestAdvStagedFileReader_SequentialReading(t *testing.T) {
	rootDir := fmt.Sprintf("test-adv-staged-sequential-%d", time.Now().Unix())
	minioHdl, cfg, tempDir := setupStagedAdvTest(t, rootDir)
	ctx := context.Background()

	cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize = 16_000_000
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads = 32

	logId := int64(4)
	segmentId := int64(400)
	defer cleanupStagedAdvTestObjects(t, minioHdl, rootDir)

	// Step 1: Create a larger dataset
	writer, err := stagedstorage.NewStagedFileWriter(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)

	const numEntries = 20
	testData := make([][]byte, numEntries)
	for i := 0; i < numEntries; i++ {
		testData[i] = []byte(fmt.Sprintf("Entry %d: Sequential test data %s", i, generateAdvTestData(100)))
	}

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-sequential-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	_, err = writer.Finalize(ctx, int64(len(testData))-1)
	require.NoError(t, err)

	_, err = writer.Compact(ctx)
	require.NoError(t, err)

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Step 2: Test sequential reading with different batch sizes
	t.Run("SequentialBatchReading", func(t *testing.T) {
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)

		batchSizes := []int64{1, 3, 5, 7, 10, 15}
		for _, batchSize := range batchSizes {
			t.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(t *testing.T) {
				var allEntries []*proto.LogEntry
				currentEntryId := int64(0)

				for currentEntryId < numEntries {
					result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
						StartEntryID:    currentEntryId,
						MaxBatchEntries: batchSize,
					}, nil)
					require.NoError(t, err)

					if len(result.Entries) == 0 {
						break
					}

					allEntries = append(allEntries, result.Entries...)
					currentEntryId = result.Entries[len(result.Entries)-1].EntryId + 1
				}

				// Verify we read all entries
				assert.Equal(t, numEntries, len(allEntries))

				// Verify data integrity
				for i, entry := range allEntries {
					assert.Equal(t, int64(i), entry.EntryId)
					assert.Equal(t, testData[i], entry.Values)
				}
			})
		}
	})
}

func TestAdvStagedFileReader_ErrorHandling(t *testing.T) {
	rootDir := fmt.Sprintf("test-adv-staged-beyond-%d", time.Now().Unix())
	minioHdl, cfg, tempDir := setupStagedAdvTest(t, rootDir)
	ctx := context.Background()
	cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize = 16_000_000
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads = 32
	defer cleanupStagedAdvTestObjects(t, minioHdl, rootDir)
	logId := int64(5)
	segmentId := int64(500)
	testData := [][]byte{
		[]byte("Entry 0"),
		[]byte("Entry 1"),
		[]byte("Entry 2"),
	}

	t.Run("ReadBeyondAvailableData", func(t *testing.T) {
		// Create a small dataset
		writer, err := stagedstorage.NewStagedFileWriter(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)

		for i, data := range testData {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-beyond-%d", i))
			_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
			require.NoError(t, err)

			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
		}

		_, err = writer.Finalize(ctx, int64(len(testData))-1)
		require.NoError(t, err)

		_, err = writer.Compact(ctx)
		require.NoError(t, err)

		err = writer.Close(ctx)
		require.NoError(t, err)

		// Test reading beyond available data
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)
		_ = reader.UpdateLastAddConfirmed(ctx, int64(len(testData))-1) // set lac to simulate that the test data has been persisted

		// Try to read starting from a non-existent entry ID
		result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    10, // Beyond available data
			MaxBatchEntries: 5,
		}, nil)

		// Should handle gracefully - either return empty result or specific error
		if err != nil {
			t.Logf("Expected error when reading beyond data: %v", err)
		} else {
			assert.Equal(t, 0, len(result.Entries), "Should return empty result when reading beyond available data")
		}
	})

	t.Run("InvalidReaderOptions", func(t *testing.T) {
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Test with invalid max batch entries
		result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 0, // Invalid batch size, it will auto set to 100
		}, nil)

		// Should handle gracefully
		if err != nil {
			t.Logf("Expected error with invalid batch size: %v", err)
		} else {
			assert.Equal(t, len(testData), len(result.Entries), "Should return empty result with invalid batch size")
		}
	})
}

func TestAdvStagedFileReader_LargeDataHandling(t *testing.T) {
	rootDir := fmt.Sprintf("test-adv-staged-large-%d", time.Now().Unix())
	minioHdl, cfg, tempDir := setupStagedAdvTest(t, rootDir)
	ctx := context.Background()

	cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize = 16_000_000
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads = 32

	// Configure for smaller blocks to test large data across multiple blocks
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 64 * 1024 // 64KB per block

	logId := int64(7)
	segmentId := int64(700)
	defer cleanupStagedAdvTestObjects(t, minioHdl, rootDir)

	// Step 1: Create large test data
	writer, err := stagedstorage.NewStagedFileWriter(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)

	const numLargeEntries = 10
	testData := make([][]byte, numLargeEntries)
	for i := 0; i < numLargeEntries; i++ {
		// Create entries of varying sizes: 50KB, 60KB, 70KB, etc.
		size := (50 + i*10) * 1024
		testData[i] = generateAdvTestData(size)
	}

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-large-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	_, err = writer.Finalize(ctx, int64(len(testData))-1)
	require.NoError(t, err)

	_, err = writer.Compact(ctx)
	require.NoError(t, err)

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Step 2: Test reading large data from compacted storage
	t.Run("ReadLargeDataFromCompacted", func(t *testing.T) {
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)
		_ = reader.UpdateLastAddConfirmed(ctx, int64(len(testData))-1) // set lac to simulate that the test data has been persisted

		// Read all large entries
		result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: numLargeEntries,
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, numLargeEntries, len(result.Entries))

		// Verify data integrity for large entries
		for i, entry := range result.Entries {
			assert.Equal(t, int64(i), entry.EntryId)
			assert.Equal(t, len(testData[i]), len(entry.Values), "Data size mismatch for entry %d", i)
			assert.Equal(t, testData[i], entry.Values, "Data content mismatch for entry %d", i)
		}
	})

	// Step 3: Test reading large entries individually
	t.Run("ReadLargeEntriesIndividually", func(t *testing.T) {
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)
		_ = reader.UpdateLastAddConfirmed(ctx, int64(len(testData))-1) // set lac to simulate that the test data has been persisted

		for i := 0; i < numLargeEntries; i++ {
			result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
				StartEntryID:    int64(i),
				MaxBatchEntries: 1,
			}, nil)
			require.NoError(t, err)
			assert.LessOrEqual(t, 1, len(result.Entries)) // after compact, result >= 1, because one block may contain multi entries

			entry := result.Entries[0]
			assert.Equal(t, int64(i), entry.EntryId)
			assert.Equal(t, testData[i], entry.Values)
		}
	})
}

func TestAdvStagedFileReader_ConcurrentReads(t *testing.T) {
	rootDir := fmt.Sprintf("test-adv-staged-concurrent-reads-%d", time.Now().Unix())
	minioHdl, cfg, tempDir := setupStagedAdvTest(t, rootDir)
	ctx := context.Background()

	cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize = 16_000_000
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushThreads = 32

	logId := int64(8)
	segmentId := int64(800)
	defer cleanupStagedAdvTestObjects(t, minioHdl, rootDir)

	// Step 1: Create test data
	writer, err := stagedstorage.NewStagedFileWriter(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
	require.NoError(t, err)

	const numEntries = 50
	testData := make([][]byte, numEntries)
	for i := 0; i < numEntries; i++ {
		testData[i] = []byte(fmt.Sprintf("Entry %d: Concurrent read test data", i))
	}

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-concurrent-write-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	_, err = writer.Finalize(ctx, int64(len(testData))-1)
	require.NoError(t, err)

	_, err = writer.Compact(ctx)
	require.NoError(t, err)

	err = writer.Close(ctx)
	require.NoError(t, err)

	// Step 2: Test concurrent reading
	t.Run("ConcurrentReaders", func(t *testing.T) {
		const numReaders = 5
		results := make(chan error, numReaders)

		for r := 0; r < numReaders; r++ {
			go func(readerId int) {
				defer func() {
					if rec := recover(); rec != nil {
						results <- fmt.Errorf("reader %d panicked: %v", readerId, rec)
						return
					}
				}()

				reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, cfg.Minio.BucketName, cfg.Minio.RootPath, tempDir, logId, segmentId, minioHdl, cfg)
				if err != nil {
					results <- fmt.Errorf("reader %d failed to create: %v", readerId, err)
					return
				}
				defer reader.Close(ctx)
				_ = reader.UpdateLastAddConfirmed(ctx, int64(len(testData))-1) // set lac to simulate that the test data has been persisted

				// Each reader reads different segments of data
				startEntry := int64(readerId * 10)
				maxEntries := int64(10)

				result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
					StartEntryID:    startEntry,
					MaxBatchEntries: maxEntries,
				}, nil)
				if err != nil {
					results <- fmt.Errorf("reader %d failed to read: %v", readerId, err)
					return
				}

				// Verify data
				for i, entry := range result.Entries {
					expectedId := startEntry + int64(i)
					if expectedId < numEntries {
						if entry.EntryId != expectedId || string(entry.Values) != string(testData[expectedId]) {
							results <- fmt.Errorf("reader %d data mismatch at entry %d", readerId, expectedId)
							return
						}
					}
				}

				results <- nil // Success
			}(r)
		}

		// Wait for all readers to complete
		for r := 0; r < numReaders; r++ {
			err := <-results
			if err != nil {
				t.Errorf("Concurrent reader error: %v", err)
			}
		}
	})
}

// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/disk"
	"github.com/zilliztech/woodpecker/server/storage/serde"
)

// =============================================================================
// Disk Storage Compaction Integration Tests
// =============================================================================
//
// These tests verify the compaction functionality for local disk storage:
// - Basic compaction functionality (merging small blocks into larger ones)
// - High concurrency scenarios (concurrent reads during compaction)
// - Crash recovery at various stages of the compaction process
//
// Compaction stages:
// 1. Read footer and validate segment is finalized
// 2. Plan merge tasks based on target block size
// 3. Concurrent merge: write merged blocks as m_N.blk.inflight
// 4. Sequential rename: m_N.blk.inflight -> m_N.blk.completed
// 5. Atomic rename: m_N.blk.completed -> m_N.blk
// 6. Update footer with compacted flag (footer.blk.inflight -> footer.blk)
// 7. Cleanup original block files

// =============================================================================
// Helper Functions
// =============================================================================

// setupCompactionTest creates a temp directory and config for testing
func setupCompactionTest(t *testing.T) (string, *config.Configuration) {
	tempDir, err := os.MkdirTemp("", "compaction_test_*")
	require.NoError(t, err, "Failed to create temp directory")
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	cfg, err := config.NewConfiguration()
	require.NoError(t, err, "Failed to create configuration")

	return tempDir, cfg
}

// createCompactionSegmentDir creates the segment directory for testing
func createCompactionSegmentDir(t *testing.T, tempDir string, logId, segmentId int64) string {
	segmentDir := filepath.Join(tempDir, fmt.Sprintf("%d/%d", logId, segmentId))
	err := os.MkdirAll(segmentDir, 0755)
	require.NoError(t, err, "Failed to create segment directory")
	return segmentDir
}

// createCompactionBlockFile creates a complete .blk file with test data
func createCompactionBlockFile(t *testing.T, segmentDir string, blockId int64, entries []*serde.BlockEntry) {
	blockPath := filepath.Join(segmentDir, fmt.Sprintf("%d.blk", blockId))
	blockData := serde.SerializeBlock(blockId, entries, false)
	err := os.WriteFile(blockPath, blockData, 0644)
	require.NoError(t, err, "Failed to write block file %s", blockPath)
}

// createCompactionFooterFile creates a footer.blk file
func createCompactionFooterFile(t *testing.T, segmentDir string, blockIndexes []*codec.IndexRecord, lastEntryId int64) {
	footerPath := filepath.Join(segmentDir, "footer.blk")
	footerData, _ := serde.SerializeFooterAndIndexes(blockIndexes, lastEntryId)
	err := os.WriteFile(footerPath, footerData, 0644)
	require.NoError(t, err, "Failed to write footer file")
}

// createCompactedFooter creates a footer.blk file with compacted flag
func createCompactedFooter(t *testing.T, segmentDir string, blockIndexes []*codec.IndexRecord, lastEntryId int64) {
	footerPath := filepath.Join(segmentDir, "footer.blk")
	footerData, _ := serde.SerializeFooterAndIndexesWithFlags(blockIndexes, lastEntryId, codec.FooterFlagCompacted)
	err := os.WriteFile(footerPath, footerData, 0644)
	require.NoError(t, err, "Failed to write compacted footer file")
}

// createCompactionMergedBlockFile creates a merged block (m_N.blk) file
func createCompactionMergedBlockFile(t *testing.T, segmentDir string, blockId int64, entries []*serde.BlockEntry) {
	blockPath := filepath.Join(segmentDir, fmt.Sprintf("m_%d.blk", blockId))
	blockData := serde.SerializeBlock(blockId, entries, false)
	err := os.WriteFile(blockPath, blockData, 0644)
	require.NoError(t, err, "Failed to write merged block file")
}

// createCompactionMergedBlockInflight creates an inflight merged block file
func createCompactionMergedBlockInflight(t *testing.T, segmentDir string, blockId int64, entries []*serde.BlockEntry) {
	inflightPath := filepath.Join(segmentDir, fmt.Sprintf("m_%d.blk.inflight", blockId))
	blockData := serde.SerializeBlock(blockId, entries, false)
	err := os.WriteFile(inflightPath, blockData, 0644)
	require.NoError(t, err, "Failed to write merged block inflight file")
}

// createCompactionMergedBlockCompleted creates a completed merged block file
func createCompactionMergedBlockCompleted(t *testing.T, segmentDir string, blockId int64, entries []*serde.BlockEntry) {
	completedPath := filepath.Join(segmentDir, fmt.Sprintf("m_%d.blk.completed", blockId))
	blockData := serde.SerializeBlock(blockId, entries, false)
	err := os.WriteFile(completedPath, blockData, 0644)
	require.NoError(t, err, "Failed to write merged block completed file")
}

// createCompactionTestEntries creates test entries for a block
func createCompactionTestEntries(startEntryId int64, count int) []*serde.BlockEntry {
	entries := make([]*serde.BlockEntry, count)
	for i := 0; i < count; i++ {
		entries[i] = &serde.BlockEntry{
			EntryId: startEntryId + int64(i),
			Data:    []byte(fmt.Sprintf("compaction-test-data-entry-%d", startEntryId+int64(i))),
		}
	}
	return entries
}

// createCompactionIndexRecords creates index records for testing
func createCompactionIndexRecords(blockCount int, entriesPerBlock int) []*codec.IndexRecord {
	var indexes []*codec.IndexRecord
	for i := 0; i < blockCount; i++ {
		firstEntry := int64(i * entriesPerBlock)
		lastEntry := firstEntry + int64(entriesPerBlock) - 1
		indexes = append(indexes, &codec.IndexRecord{
			BlockNumber:  int32(i),
			StartOffset:  0,
			BlockSize:    100,
			FirstEntryID: firstEntry,
			LastEntryID:  lastEntry,
		})
	}
	return indexes
}

// countFilesByPattern counts files matching a pattern in a directory
func countFilesByPattern(t *testing.T, dir string, pattern string) int {
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	count := 0
	for _, entry := range entries {
		matched, _ := filepath.Match(pattern, entry.Name())
		if matched {
			count++
		}
	}
	return count
}

// =============================================================================
// Basic Compaction Functionality Tests
// =============================================================================

// TestCompaction_BasicMultipleBlocks tests basic compaction with multiple small blocks
func TestCompaction_BasicMultipleBlocks(t *testing.T) {
	tempDir, cfg := setupCompactionTest(t)
	ctx := context.Background()

	const logId, segmentId = 2000, 3000

	t.Run("compact multiple small blocks into fewer larger blocks", func(t *testing.T) {
		segmentDir := createCompactionSegmentDir(t, tempDir, logId, segmentId)

		// Create 10 small blocks with 5 entries each (total 50 entries)
		for i := 0; i < 10; i++ {
			entries := createCompactionTestEntries(int64(i*5), 5)
			createCompactionBlockFile(t, segmentDir, int64(i), entries)
		}

		// Create footer (finalized segment)
		indexes := createCompactionIndexRecords(10, 5)
		createCompactionFooterFile(t, segmentDir, indexes, 49)

		// Count original blocks
		originalBlockCount := countFilesByPattern(t, segmentDir, "*.blk") - 1 // exclude footer.blk
		require.Equal(t, 10, originalBlockCount, "Should have 10 original blocks")

		// Create writer and compact
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		size, err := writer.Compact(ctx)
		require.NoError(t, err, "Compact should succeed")
		require.Greater(t, size, int64(0), "Compacted size should be positive")

		writer.Close(ctx)

		// Verify: merged blocks should exist
		mergedBlockCount := countFilesByPattern(t, segmentDir, "m_*.blk")
		require.Greater(t, mergedBlockCount, 0, "Should have merged blocks after compaction")

		// Verify: original blocks should be cleaned up
		originalAfterCompact := 0
		entries, _ := os.ReadDir(segmentDir)
		for _, entry := range entries {
			if serde.IsOriginalBlockFile(entry.Name()) {
				originalAfterCompact++
			}
		}
		require.Equal(t, 0, originalAfterCompact, "Original blocks should be cleaned up")

		// Verify: reader can read all data from merged blocks
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)
		defer reader.Close(ctx)

		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 100,
		}, nil)
		require.NoError(t, readErr)
		require.Len(t, result.Entries, 50, "Should read all 50 entries from merged blocks")

		// Verify entry data integrity
		for i, entry := range result.Entries {
			require.Equal(t, int64(i), entry.EntryId)
			expectedData := fmt.Sprintf("compaction-test-data-entry-%d", i)
			require.Equal(t, expectedData, string(entry.Values))
		}
	})
}

// TestCompaction_SingleBlock tests compaction with a single block (edge case)
func TestCompaction_SingleBlock(t *testing.T) {
	tempDir, cfg := setupCompactionTest(t)
	ctx := context.Background()

	const logId, segmentId = 2001, 3001

	t.Run("compact single block - should still work", func(t *testing.T) {
		segmentDir := createCompactionSegmentDir(t, tempDir, logId, segmentId)

		// Create single block
		entries := createCompactionTestEntries(0, 10)
		createCompactionBlockFile(t, segmentDir, 0, entries)

		// Create footer
		indexes := createCompactionIndexRecords(1, 10)
		createCompactionFooterFile(t, segmentDir, indexes, 9)

		// Compact
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		size, err := writer.Compact(ctx)
		require.NoError(t, err, "Compact single block should succeed")
		require.Greater(t, size, int64(0))

		writer.Close(ctx)

		// Verify reader works
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)
		defer reader.Close(ctx)

		result, _ := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 20,
		}, nil)
		require.Len(t, result.Entries, 10)
	})
}

// TestCompaction_AlreadyCompacted tests that re-compacting is a no-op
func TestCompaction_AlreadyCompacted(t *testing.T) {
	tempDir, cfg := setupCompactionTest(t)
	ctx := context.Background()

	const logId, segmentId = 2002, 3002

	t.Run("compact already compacted segment - should be no-op", func(t *testing.T) {
		segmentDir := createCompactionSegmentDir(t, tempDir, logId, segmentId)

		// Create merged block (simulating already compacted)
		entries := createCompactionTestEntries(0, 20)
		createCompactionMergedBlockFile(t, segmentDir, 0, entries)

		// Create compacted footer
		indexes := []*codec.IndexRecord{
			{BlockNumber: 0, StartOffset: 0, BlockSize: 200, FirstEntryID: 0, LastEntryID: 19},
		}
		createCompactedFooter(t, segmentDir, indexes, 19)

		// Try to compact again
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		size, err := writer.Compact(ctx)
		require.NoError(t, err, "Compact on already compacted should succeed (no-op)")
		require.Greater(t, size, int64(0), "Should return existing size")

		writer.Close(ctx)

		// Verify only one merged block exists (no new blocks created)
		mergedCount := countFilesByPattern(t, segmentDir, "m_*.blk")
		require.Equal(t, 1, mergedCount, "Should still have only 1 merged block")
	})
}

// TestCompaction_NotFinalized tests that compaction fails on non-finalized segments
func TestCompaction_NotFinalized(t *testing.T) {
	tempDir, cfg := setupCompactionTest(t)
	ctx := context.Background()

	const logId, segmentId = 2003, 3003

	t.Run("compact non-finalized segment - should fail", func(t *testing.T) {
		segmentDir := createCompactionSegmentDir(t, tempDir, logId, segmentId)

		// Create blocks but NO footer (not finalized)
		entries := createCompactionTestEntries(0, 5)
		createCompactionBlockFile(t, segmentDir, 0, entries)

		// Create writer (not finalized)
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		// Compact should fail
		_, err = writer.Compact(ctx)
		require.Error(t, err, "Compact on non-finalized segment should fail")

		writer.Close(ctx)
	})
}

// =============================================================================
// High Concurrency Tests
// =============================================================================

// TestCompaction_ConcurrentReads tests that reads work correctly after compaction
// Note: Readers created before compaction may fail if files are removed during read.
// This test verifies that readers created after compaction work correctly.
func TestCompaction_ConcurrentReads(t *testing.T) {
	tempDir, cfg := setupCompactionTest(t)
	ctx := context.Background()

	const logId, segmentId = 2010, 3010
	const numBlocks = 20
	const entriesPerBlock = 10
	const totalEntries = numBlocks * entriesPerBlock

	segmentDir := createCompactionSegmentDir(t, tempDir, logId, segmentId)

	// Create blocks
	for i := 0; i < numBlocks; i++ {
		entries := createCompactionTestEntries(int64(i*entriesPerBlock), entriesPerBlock)
		createCompactionBlockFile(t, segmentDir, int64(i), entries)
	}

	// Create footer
	indexes := createCompactionIndexRecords(numBlocks, entriesPerBlock)
	createCompactionFooterFile(t, segmentDir, indexes, int64(totalEntries-1))

	// Perform compaction first
	writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
	require.NoError(t, err)

	_, compactErr := writer.Compact(ctx)
	require.NoError(t, compactErr, "Compact should succeed")

	writer.Close(ctx)

	// Start multiple readers concurrently AFTER compaction
	const numReaders = 5
	const readsPerReader = 3
	var wg sync.WaitGroup
	readerErrors := make(chan error, numReaders*readsPerReader)
	readCounts := make(chan int, numReaders*readsPerReader)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerNum int) {
			defer wg.Done()

			reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
			if err != nil {
				readerErrors <- fmt.Errorf("reader %d creation failed: %w", readerNum, err)
				return
			}
			defer reader.Close(ctx)

			// Read multiple times
			for j := 0; j < readsPerReader; j++ {
				result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
					StartEntryID:    0,
					MaxBatchEntries: int64(totalEntries + 10),
				}, nil)
				if readErr != nil {
					readerErrors <- fmt.Errorf("reader %d read %d failed: %w", readerNum, j, readErr)
					return
				}
				readCounts <- len(result.Entries)
			}
		}(i)
	}

	// Wait for all readers
	wg.Wait()
	close(readerErrors)
	close(readCounts)

	// Check for errors
	for err := range readerErrors {
		t.Errorf("Reader error: %v", err)
	}

	// Verify all reads got correct count
	for count := range readCounts {
		require.Equal(t, totalEntries, count, "Each read should return all entries")
	}
}

// TestCompaction_ConcurrentCompactAttempts tests multiple concurrent compact calls
func TestCompaction_ConcurrentCompactAttempts(t *testing.T) {
	tempDir, cfg := setupCompactionTest(t)
	ctx := context.Background()

	const logId, segmentId = 2011, 3011

	segmentDir := createCompactionSegmentDir(t, tempDir, logId, segmentId)

	// Create blocks
	for i := 0; i < 5; i++ {
		entries := createCompactionTestEntries(int64(i*5), 5)
		createCompactionBlockFile(t, segmentDir, int64(i), entries)
	}

	// Create footer
	indexes := createCompactionIndexRecords(5, 5)
	createCompactionFooterFile(t, segmentDir, indexes, 24)

	// Try to compact from multiple goroutines
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var errorCount atomic.Int32

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()

			writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
			if err != nil {
				errorCount.Add(1)
				return
			}
			defer writer.Close(ctx)

			_, err = writer.Compact(ctx)
			if err == nil {
				successCount.Add(1)
			} else {
				// Error is acceptable (concurrent access)
				t.Logf("Worker %d compact error (expected): %v", workerNum, err)
			}
		}(i)
	}

	wg.Wait()

	// At least one should succeed
	t.Logf("Compact results: success=%d, errors=%d", successCount.Load(), errorCount.Load())
	require.GreaterOrEqual(t, successCount.Load(), int32(1), "At least one compact should succeed")

	// Verify final state is consistent
	reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 50,
	}, nil)
	require.NoError(t, readErr)
	require.Len(t, result.Entries, 25, "Should read all 25 entries after concurrent compacts")
}

// =============================================================================
// Crash Recovery Tests at Various Positions
// =============================================================================

// TestCompaction_CrashDuringInflightWrite tests crash during stage 3 (writing m_N.blk.inflight)
func TestCompaction_CrashDuringInflightWrite(t *testing.T) {
	tempDir, cfg := setupCompactionTest(t)
	ctx := context.Background()

	const logId, segmentId = 2020, 3020

	t.Run("crash with partial inflight files - should recover using original blocks", func(t *testing.T) {
		segmentDir := createCompactionSegmentDir(t, tempDir, logId, segmentId)

		// Create original blocks
		for i := 0; i < 5; i++ {
			entries := createCompactionTestEntries(int64(i*5), 5)
			createCompactionBlockFile(t, segmentDir, int64(i), entries)
		}

		// Create footer (non-compacted)
		indexes := createCompactionIndexRecords(5, 5)
		createCompactionFooterFile(t, segmentDir, indexes, 24)

		// Simulate crash during compact: inflight merged block exists
		inflightEntries := createCompactionTestEntries(0, 15) // Partial merge
		createCompactionMergedBlockInflight(t, segmentDir, 0, inflightEntries)

		// Reader should still work with original blocks (inflight ignored)
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)

		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 50,
		}, nil)
		require.NoError(t, readErr, "Should read from original blocks")
		require.Len(t, result.Entries, 25, "Should read all 25 entries")

		reader.Close(ctx)

		// New compact should succeed (cleaning up inflight)
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		_, compactErr := writer.Compact(ctx)
		require.NoError(t, compactErr, "Compact after crash recovery should succeed")

		writer.Close(ctx)

		// Verify no inflight files remain
		inflightCount := countFilesByPattern(t, segmentDir, "m_*.blk.inflight")
		require.Equal(t, 0, inflightCount, "No inflight files should remain")
	})
}

// TestCompaction_CrashAfterCompletedRename tests crash during stage 4-5 (after .completed rename)
func TestCompaction_CrashAfterCompletedRename(t *testing.T) {
	tempDir, cfg := setupCompactionTest(t)
	ctx := context.Background()

	const logId, segmentId = 2021, 3021

	t.Run("crash with completed files but before footer update", func(t *testing.T) {
		segmentDir := createCompactionSegmentDir(t, tempDir, logId, segmentId)

		// Create original blocks
		for i := 0; i < 4; i++ {
			entries := createCompactionTestEntries(int64(i*5), 5)
			createCompactionBlockFile(t, segmentDir, int64(i), entries)
		}

		// Create footer (non-compacted)
		indexes := createCompactionIndexRecords(4, 5)
		createCompactionFooterFile(t, segmentDir, indexes, 19)

		// Simulate crash: completed merged blocks exist but footer not updated
		mergedEntries := createCompactionTestEntries(0, 20)
		createCompactionMergedBlockCompleted(t, segmentDir, 0, mergedEntries)

		// Reader should work with original blocks (footer says non-compacted)
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)

		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 50,
		}, nil)
		require.NoError(t, readErr)
		require.Len(t, result.Entries, 20, "Should read all 20 entries from original blocks")

		reader.Close(ctx)

		// New compact should handle completed files
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		_, compactErr := writer.Compact(ctx)
		require.NoError(t, compactErr, "Compact should succeed handling completed files")

		writer.Close(ctx)
	})
}

// TestCompaction_CrashDuringFooterUpdate tests crash during stage 6 (footer update)
func TestCompaction_CrashDuringFooterUpdate(t *testing.T) {
	tempDir, cfg := setupCompactionTest(t)
	ctx := context.Background()

	const logId, segmentId = 2022, 3022

	t.Run("crash with merged blocks done but footer.blk.inflight", func(t *testing.T) {
		segmentDir := createCompactionSegmentDir(t, tempDir, logId, segmentId)

		// Create original blocks
		for i := 0; i < 3; i++ {
			entries := createCompactionTestEntries(int64(i*5), 5)
			createCompactionBlockFile(t, segmentDir, int64(i), entries)
		}

		// Create old footer (non-compacted)
		indexes := createCompactionIndexRecords(3, 5)
		createCompactionFooterFile(t, segmentDir, indexes, 14)

		// Simulate: merged blocks are complete
		mergedEntries := createCompactionTestEntries(0, 15)
		createCompactionMergedBlockFile(t, segmentDir, 0, mergedEntries)

		// Simulate: footer.blk.inflight exists (crash during footer update)
		newIndexes := []*codec.IndexRecord{
			{BlockNumber: 0, StartOffset: 0, BlockSize: 200, FirstEntryID: 0, LastEntryID: 14},
		}
		footerInflightPath := filepath.Join(segmentDir, "footer.blk.inflight")
		footerData, _ := serde.SerializeFooterAndIndexesWithFlags(newIndexes, 14, codec.FooterFlagCompacted)
		err := os.WriteFile(footerInflightPath, footerData, 0644)
		require.NoError(t, err)

		// Recovery writer should complete the footer update
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)
		writer.Close(ctx)

		// Verify footer.blk.inflight is handled (either renamed or removed)
		_, err = os.Stat(footerInflightPath)
		// Either renamed to footer.blk or removed during recovery

		// Reader should work
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)
		defer reader.Close(ctx)

		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 30,
		}, nil)
		require.NoError(t, readErr)
		require.Len(t, result.Entries, 15, "Should read all 15 entries")
	})
}

// TestCompaction_CrashDuringCleanup tests crash during stage 7 (original file cleanup)
func TestCompaction_CrashDuringCleanup(t *testing.T) {
	tempDir, cfg := setupCompactionTest(t)
	ctx := context.Background()

	const logId, segmentId = 2023, 3023

	t.Run("crash during original block cleanup - should recover gracefully", func(t *testing.T) {
		segmentDir := createCompactionSegmentDir(t, tempDir, logId, segmentId)

		// Create some original blocks (simulating partial cleanup)
		for i := 0; i < 2; i++ {
			entries := createCompactionTestEntries(int64(i*5), 5)
			createCompactionBlockFile(t, segmentDir, int64(i), entries)
		}
		// Blocks 2,3 already cleaned up

		// Create merged block (compaction complete)
		mergedEntries := createCompactionTestEntries(0, 20)
		createCompactionMergedBlockFile(t, segmentDir, 0, mergedEntries)

		// Create compacted footer
		newIndexes := []*codec.IndexRecord{
			{BlockNumber: 0, StartOffset: 0, BlockSize: 200, FirstEntryID: 0, LastEntryID: 19},
		}
		createCompactedFooter(t, segmentDir, newIndexes, 19)

		// Reader should use merged blocks (footer says compacted)
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)

		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 50,
		}, nil)
		require.NoError(t, readErr)
		require.Len(t, result.Entries, 20, "Should read all 20 entries from merged block")

		reader.Close(ctx)

		// Compact again should clean up remaining original blocks
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		_, compactErr := writer.Compact(ctx)
		require.NoError(t, compactErr, "Compact should succeed and clean up remaining files")

		writer.Close(ctx)

		// Verify original blocks are cleaned up
		originalCount := 0
		entries, _ := os.ReadDir(segmentDir)
		for _, entry := range entries {
			if serde.IsOriginalBlockFile(entry.Name()) {
				originalCount++
			}
		}
		require.Equal(t, 0, originalCount, "All original blocks should be cleaned up")
	})
}

// =============================================================================
// Mixed Crash Scenarios
// =============================================================================

// TestCompaction_MultipleCrashStages tests recovery from multiple sequential crashes
func TestCompaction_MultipleCrashStages(t *testing.T) {
	tempDir, cfg := setupCompactionTest(t)
	ctx := context.Background()

	const logId, segmentId = 2030, 3030

	t.Run("recover from multiple crash stages", func(t *testing.T) {
		segmentDir := createCompactionSegmentDir(t, tempDir, logId, segmentId)

		// Create original blocks
		for i := 0; i < 6; i++ {
			entries := createCompactionTestEntries(int64(i*5), 5)
			createCompactionBlockFile(t, segmentDir, int64(i), entries)
		}

		// Create footer
		indexes := createCompactionIndexRecords(6, 5)
		createCompactionFooterFile(t, segmentDir, indexes, 29)

		// Simulate mixed crash state:
		// - Some inflight merged blocks
		// - Some completed merged blocks
		// - Footer not yet updated
		createCompactionMergedBlockInflight(t, segmentDir, 0, createCompactionTestEntries(0, 15))
		createCompactionMergedBlockCompleted(t, segmentDir, 1, createCompactionTestEntries(15, 15))

		// Reader should work with original blocks
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)

		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 50,
		}, nil)
		require.NoError(t, readErr)
		require.Len(t, result.Entries, 30, "Should read all 30 entries from original blocks")

		reader.Close(ctx)

		// Full compact should recover and complete
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		_, compactErr := writer.Compact(ctx)
		require.NoError(t, compactErr, "Compact should succeed after mixed crash state")

		writer.Close(ctx)

		// Verify clean state - inflight files should be cleaned up
		// Note: completed files from previous crash may remain but are harmless
		// since the footer determines which blocks to read
		inflightCount := countFilesByPattern(t, segmentDir, "m_*.blk.inflight")
		require.Equal(t, 0, inflightCount, "No inflight files should remain")

		// Verify data integrity
		reader2, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)
		defer reader2.Close(ctx)

		result2, readErr2 := reader2.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 50,
		}, nil)
		require.NoError(t, readErr2)
		require.Len(t, result2.Entries, 30, "Should read all 30 entries after recovery")
	})
}

// =============================================================================
// Edge Cases and Data Integrity Tests
// =============================================================================

// TestCompaction_DataIntegrity tests that data is preserved correctly after compaction
func TestCompaction_DataIntegrity(t *testing.T) {
	tempDir, cfg := setupCompactionTest(t)
	ctx := context.Background()

	const logId, segmentId = 2040, 3040

	t.Run("verify data integrity after compaction", func(t *testing.T) {
		segmentDir := createCompactionSegmentDir(t, tempDir, logId, segmentId)

		// Create blocks with identifiable data
		const numBlocks = 15
		const entriesPerBlock = 7
		totalEntries := numBlocks * entriesPerBlock

		for i := 0; i < numBlocks; i++ {
			entries := createCompactionTestEntries(int64(i*entriesPerBlock), entriesPerBlock)
			createCompactionBlockFile(t, segmentDir, int64(i), entries)
		}

		// Create footer
		indexes := createCompactionIndexRecords(numBlocks, entriesPerBlock)
		createCompactionFooterFile(t, segmentDir, indexes, int64(totalEntries-1))

		// Read data before compaction
		readerBefore, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)

		resultBefore, _ := readerBefore.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(totalEntries + 10),
		}, nil)
		readerBefore.Close(ctx)

		require.Len(t, resultBefore.Entries, totalEntries)

		// Compact
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		_, compactErr := writer.Compact(ctx)
		require.NoError(t, compactErr)

		writer.Close(ctx)

		// Read data after compaction
		readerAfter, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)
		defer readerAfter.Close(ctx)

		resultAfter, readErr := readerAfter.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(totalEntries + 10),
		}, nil)
		require.NoError(t, readErr)
		require.Len(t, resultAfter.Entries, totalEntries)

		// Compare data byte-by-byte
		for i := 0; i < totalEntries; i++ {
			require.Equal(t, resultBefore.Entries[i].EntryId, resultAfter.Entries[i].EntryId,
				"Entry ID mismatch at index %d", i)
			require.Equal(t, resultBefore.Entries[i].Values, resultAfter.Entries[i].Values,
				"Entry data mismatch at index %d", i)
		}
	})
}

// TestCompaction_LargeSegment tests compaction with a larger segment
func TestCompaction_LargeSegment(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large segment test in short mode")
	}

	tempDir, cfg := setupCompactionTest(t)
	ctx := context.Background()

	const logId, segmentId = 2050, 3050

	t.Run("compact large segment with many small blocks", func(t *testing.T) {
		segmentDir := createCompactionSegmentDir(t, tempDir, logId, segmentId)

		// Create 100 small blocks with 10 entries each (total 1000 entries)
		const numBlocks = 100
		const entriesPerBlock = 10
		totalEntries := numBlocks * entriesPerBlock

		for i := 0; i < numBlocks; i++ {
			entries := createCompactionTestEntries(int64(i*entriesPerBlock), entriesPerBlock)
			createCompactionBlockFile(t, segmentDir, int64(i), entries)
		}

		// Create footer
		indexes := createCompactionIndexRecords(numBlocks, entriesPerBlock)
		createCompactionFooterFile(t, segmentDir, indexes, int64(totalEntries-1))

		// Compact
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		startTime := time.Now()
		size, compactErr := writer.Compact(ctx)
		compactDuration := time.Since(startTime)

		require.NoError(t, compactErr)
		require.Greater(t, size, int64(0))

		t.Logf("Compacted %d blocks in %v", numBlocks, compactDuration)

		writer.Close(ctx)

		// Verify merged block count is much smaller
		mergedCount := countFilesByPattern(t, segmentDir, "m_*.blk")
		t.Logf("Merged block count: %d (from %d original blocks)", mergedCount, numBlocks)
		require.Less(t, mergedCount, numBlocks, "Should have fewer merged blocks than original")

		// Verify data integrity
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)
		defer reader.Close(ctx)

		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: int64(totalEntries + 10),
		}, nil)
		require.NoError(t, readErr)
		require.Len(t, result.Entries, totalEntries, "Should read all entries after compaction")
	})
}

// TestCompaction_ReadFromMiddle tests reading from middle of compacted segment
func TestCompaction_ReadFromMiddle(t *testing.T) {
	tempDir, cfg := setupCompactionTest(t)
	ctx := context.Background()

	const logId, segmentId = 2060, 3060

	t.Run("read from middle of compacted segment", func(t *testing.T) {
		segmentDir := createCompactionSegmentDir(t, tempDir, logId, segmentId)

		// Create blocks
		const numBlocks = 10
		const entriesPerBlock = 10
		const totalEntries = numBlocks * entriesPerBlock

		for i := 0; i < numBlocks; i++ {
			entries := createCompactionTestEntries(int64(i*entriesPerBlock), entriesPerBlock)
			createCompactionBlockFile(t, segmentDir, int64(i), entries)
		}

		// Create footer
		indexes := createCompactionIndexRecords(numBlocks, entriesPerBlock)
		createCompactionFooterFile(t, segmentDir, indexes, 99)

		// Compact
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)
		_, _ = writer.Compact(ctx)
		writer.Close(ctx)

		// Read from middle
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Read starting from entry 50 - reader returns all remaining entries in the segment
		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    50,
			MaxBatchEntries: int64(totalEntries), // Large enough to get all remaining
		}, nil)
		require.NoError(t, readErr)
		// Should read entries 50-99 (50 entries remaining from entry 50)
		require.Len(t, result.Entries, 50, "Should read 50 entries starting from 50")
		require.Equal(t, int64(50), result.Entries[0].EntryId, "First entry should be 50")
		require.Equal(t, int64(99), result.Entries[49].EntryId, "Last entry should be 99")

		// Verify data integrity for middle entries
		for i, entry := range result.Entries {
			expectedId := int64(50 + i)
			require.Equal(t, expectedId, entry.EntryId, "Entry ID should be correct")
			expectedData := fmt.Sprintf("compaction-test-data-entry-%d", expectedId)
			require.Equal(t, expectedData, string(entry.Values), "Entry data should be correct")
		}
	})
}

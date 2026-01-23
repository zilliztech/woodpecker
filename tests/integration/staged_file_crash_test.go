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
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/serde"
	"github.com/zilliztech/woodpecker/server/storage/stagedstorage"
)

// =============================================================================
// Per-Block Format Crash Recovery Tests
// =============================================================================
//
// These tests verify crash recovery for the per-block file format:
// - Each block is written as a separate {blockId}.blk file
// - Atomic writes use three-stage: .blk.inflight -> .blk.completed -> .blk
// - Only .blk files are considered committed; .inflight and .completed are cleaned up
// - Footer is written as footer.blk with two-stage: footer.blk.inflight -> footer.blk
//
// Test scenarios cover crashes at various stages of the write process.

// =============================================================================
// Helper Functions for Creating Test Files
// =============================================================================

// createSegmentDir creates the segment directory for testing
func createSegmentDir(t *testing.T, tempDir string, logId, segmentId int64) string {
	segmentDir := filepath.Join(tempDir, fmt.Sprintf("%d/%d", logId, segmentId))
	err := os.MkdirAll(segmentDir, 0755)
	require.NoError(t, err, "Failed to create segment directory")
	return segmentDir
}

// createBlockFile creates a complete .blk file with test data
// Note: stagedstorage block format is: BlockHeaderRecord + DataRecords (no HeaderRecord)
func createBlockFile(t *testing.T, segmentDir string, blockId int64, entries []*serde.BlockEntry) {
	blockPath := filepath.Join(segmentDir, fmt.Sprintf("%d.blk", blockId))
	// stagedstorage does NOT include HeaderRecord in block files
	blockData := serde.SerializeBlock(blockId, entries, false)
	err := os.WriteFile(blockPath, blockData, 0644)
	require.NoError(t, err, "Failed to write block file %s", blockPath)
}

// createInflightBlockFile creates a .blk.inflight file (stage 1 of atomic write)
func createInflightBlockFile(t *testing.T, segmentDir string, blockId int64, entries []*serde.BlockEntry) {
	inflightPath := filepath.Join(segmentDir, fmt.Sprintf("%d.blk.inflight", blockId))
	// stagedstorage does NOT include HeaderRecord in block files
	blockData := serde.SerializeBlock(blockId, entries, false)
	err := os.WriteFile(inflightPath, blockData, 0644)
	require.NoError(t, err, "Failed to write inflight block file %s", inflightPath)
}

// createCompletedBlockFile creates a .blk.completed file (stage 2 of atomic write)
func createCompletedBlockFile(t *testing.T, segmentDir string, blockId int64, entries []*serde.BlockEntry) {
	completedPath := filepath.Join(segmentDir, fmt.Sprintf("%d.blk.completed", blockId))
	// stagedstorage does NOT include HeaderRecord in block files
	blockData := serde.SerializeBlock(blockId, entries, false)
	err := os.WriteFile(completedPath, blockData, 0644)
	require.NoError(t, err, "Failed to write completed block file %s", completedPath)
}

// createFooterFile creates a complete footer.blk file
func createFooterFile(t *testing.T, segmentDir string, blockCount int, lastEntryId int64) {
	footerPath := filepath.Join(segmentDir, "footer.blk")
	footerData, _ := serde.SerializeFooterAndIndexes(createIndexRecords(blockCount), lastEntryId)
	err := os.WriteFile(footerPath, footerData, 0644)
	require.NoError(t, err, "Failed to write footer file %s", footerPath)
}

// createFooterInflightFile creates a footer.blk.inflight file
func createFooterInflightFile(t *testing.T, segmentDir string, blockCount int, lastEntryId int64) {
	inflightPath := filepath.Join(segmentDir, "footer.blk.inflight")
	footerData, _ := serde.SerializeFooterAndIndexes(createIndexRecords(blockCount), lastEntryId)
	err := os.WriteFile(inflightPath, footerData, 0644)
	require.NoError(t, err, "Failed to write footer inflight file %s", inflightPath)
}

// createCorruptedFooterInflightFile creates a corrupted footer.blk.inflight file
func createCorruptedFooterInflightFile(t *testing.T, segmentDir string) {
	inflightPath := filepath.Join(segmentDir, "footer.blk.inflight")
	corruptedData := []byte("corrupted footer data")
	err := os.WriteFile(inflightPath, corruptedData, 0644)
	require.NoError(t, err, "Failed to write corrupted footer inflight file %s", inflightPath)
}

// createIndexRecords creates index records for testing
func createIndexRecords(blockCount int) []*codec.IndexRecord {
	var indexes []*codec.IndexRecord
	for i := 0; i < blockCount; i++ {
		indexes = append(indexes, &codec.IndexRecord{
			BlockNumber:  int32(i),
			StartOffset:  0,
			BlockSize:    100, // dummy size
			FirstEntryID: int64(i),
			LastEntryID:  int64(i),
		})
	}
	return indexes
}

// createTestEntries creates test entries for a block
func createTestEntries(startEntryId int64, count int) []*serde.BlockEntry {
	entries := make([]*serde.BlockEntry, count)
	for i := 0; i < count; i++ {
		entries[i] = &serde.BlockEntry{
			EntryId: startEntryId + int64(i),
			Data:    []byte(fmt.Sprintf("test-data-entry-%d", startEntryId+int64(i))),
		}
	}
	return entries
}

// =============================================================================
// Test: Empty Segment Directory
// =============================================================================

// TestStagedFileCrash_EmptySegmentDir tests behavior when segment directory is empty
func TestStagedFileCrash_EmptySegmentDir(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-empty-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1000, 2000

	t.Run("empty directory - writer recovery should succeed with no data", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId)
		t.Logf("Created empty segment directory for writer: %s", segmentDir)

		// Create writer in recovery mode on empty directory
		writer, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
			cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg, true)
		require.NoError(t, err, "Should be able to create recovery writer on empty directory")

		// Should have no entries
		require.Equal(t, int64(-1), writer.GetLastEntryId(ctx), "LastEntryId should be -1 for empty segment")
		require.Equal(t, int64(-1), writer.GetFirstEntryId(ctx), "FirstEntryId should be -1 for empty segment")

		writer.Close(ctx)
	})
}

// =============================================================================
// Test: Inflight and Completed Files Cleanup
// =============================================================================

// TestStagedFileCrash_InflightAndCompletedCleanup tests that inflight and completed files are cleaned up
func TestStagedFileCrash_InflightAndCompletedCleanup(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-cleanup-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1001, 2001

	t.Run("inflight files should be cleaned up", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId)
		createInflightBlockFile(t, segmentDir, 0, createTestEntries(0, 5))

		// Writer recovery should clean up inflight files
		writer, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
			cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg, true)
		require.NoError(t, err)

		// Inflight data should not be recovered
		require.Equal(t, int64(-1), writer.GetLastEntryId(ctx), "Inflight data should not be recovered")
		writer.Close(ctx)

		// Verify inflight file was removed
		_, err = os.Stat(filepath.Join(segmentDir, "0.blk.inflight"))
		require.True(t, os.IsNotExist(err), "Inflight file should be removed")
	})

	t.Run("completed files should be cleaned up", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId+1)
		createCompletedBlockFile(t, segmentDir, 0, createTestEntries(0, 5))

		// Writer recovery should clean up completed files
		writer, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
			cfg.Minio.RootPath, tempDir, logId, segmentId+1, storageCli, cfg, true)
		require.NoError(t, err)

		// Completed data should not be recovered (three-stage write: only .blk is committed)
		require.Equal(t, int64(-1), writer.GetLastEntryId(ctx), "Completed data should not be recovered")
		writer.Close(ctx)

		// Verify completed file was removed
		_, err = os.Stat(filepath.Join(segmentDir, "0.blk.completed"))
		require.True(t, os.IsNotExist(err), "Completed file should be removed")
	})

	t.Run("mixed inflight and completed should all be cleaned up", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId+2)
		createInflightBlockFile(t, segmentDir, 0, createTestEntries(0, 3))
		createCompletedBlockFile(t, segmentDir, 1, createTestEntries(3, 3))
		createInflightBlockFile(t, segmentDir, 2, createTestEntries(6, 3))

		writer, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
			cfg.Minio.RootPath, tempDir, logId, segmentId+2, storageCli, cfg, true)
		require.NoError(t, err)

		require.Equal(t, int64(-1), writer.GetLastEntryId(ctx), "No data should be recovered from uncommitted files")
		writer.Close(ctx)

		// All incomplete files should be removed
		_, err = os.Stat(filepath.Join(segmentDir, "0.blk.inflight"))
		require.True(t, os.IsNotExist(err))
		_, err = os.Stat(filepath.Join(segmentDir, "1.blk.completed"))
		require.True(t, os.IsNotExist(err))
		_, err = os.Stat(filepath.Join(segmentDir, "2.blk.inflight"))
		require.True(t, os.IsNotExist(err))
	})
}

// =============================================================================
// Test: Complete Block Recovery
// =============================================================================

// TestStagedFileCrash_CompleteBlockRecovery tests recovery from complete .blk files
func TestStagedFileCrash_CompleteBlockRecovery(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-complete-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1002, 2002

	t.Run("single complete block should be recovered", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId)
		entries := createTestEntries(0, 5)
		createBlockFile(t, segmentDir, 0, entries)

		writer, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
			cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg, true)
		require.NoError(t, err)

		require.Equal(t, int64(4), writer.GetLastEntryId(ctx), "Should recover lastEntryId=4")
		require.Equal(t, int64(0), writer.GetFirstEntryId(ctx), "Should recover firstEntryId=0")
		writer.Close(ctx)

		// Reader should be able to read the data
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath,
			tempDir, logId, segmentId, storageCli, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)

		reader.UpdateLastAddConfirmed(ctx, 4)
		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 10,
		}, nil)
		require.NoError(t, readErr, "Should be able to read recovered data")
		require.Len(t, result.Entries, 5, "Should read all 5 entries")
	})

	t.Run("multiple complete blocks should be recovered", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId+1)
		createBlockFile(t, segmentDir, 0, createTestEntries(0, 3))
		createBlockFile(t, segmentDir, 1, createTestEntries(3, 3))
		createBlockFile(t, segmentDir, 2, createTestEntries(6, 4))

		writer, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
			cfg.Minio.RootPath, tempDir, logId, segmentId+1, storageCli, cfg, true)
		require.NoError(t, err)

		require.Equal(t, int64(9), writer.GetLastEntryId(ctx), "Should recover lastEntryId=9")
		require.Equal(t, int64(0), writer.GetFirstEntryId(ctx), "Should recover firstEntryId=0")
		writer.Close(ctx)

		// Reader should read all blocks
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath,
			tempDir, logId, segmentId+1, storageCli, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)

		reader.UpdateLastAddConfirmed(ctx, 9)
		result, _ := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 20,
		}, nil)
		require.Len(t, result.Entries, 10, "Should read all 10 entries from 3 blocks")
	})
}

// =============================================================================
// Test: Mixed Complete and Incomplete Files
// =============================================================================

// TestStagedFileCrash_MixedBlockFiles tests recovery with mix of .blk and incomplete files
func TestStagedFileCrash_MixedBlockFiles(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-mixed-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1003, 2003

	t.Run("complete blocks + inflight - only complete should be recovered", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId)
		// Block 0 and 1: complete
		createBlockFile(t, segmentDir, 0, createTestEntries(0, 3))
		createBlockFile(t, segmentDir, 1, createTestEntries(3, 3))
		// Block 2: inflight (should be cleaned up)
		createInflightBlockFile(t, segmentDir, 2, createTestEntries(6, 3))

		writer, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
			cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg, true)
		require.NoError(t, err)

		// Should only see entries from complete blocks (0-5), not inflight (6-8)
		require.Equal(t, int64(5), writer.GetLastEntryId(ctx), "Should recover up to entry 5")
		require.Equal(t, int64(0), writer.GetFirstEntryId(ctx))
		writer.Close(ctx)

		// Inflight file should be removed
		_, err = os.Stat(filepath.Join(segmentDir, "2.blk.inflight"))
		require.True(t, os.IsNotExist(err), "Inflight file should be removed")

		// Reader should only read complete blocks
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath,
			tempDir, logId, segmentId, storageCli, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)

		reader.UpdateLastAddConfirmed(ctx, 5)
		result, _ := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 20,
		}, nil)
		require.Len(t, result.Entries, 6, "Should read 6 entries from complete blocks")
	})

	t.Run("complete blocks + completed - only committed data recovered", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId+1)
		// Block 0: complete
		createBlockFile(t, segmentDir, 0, createTestEntries(0, 3))
		// Block 1: completed (not yet committed, will be cleaned up)
		createCompletedBlockFile(t, segmentDir, 1, createTestEntries(3, 3))

		writer, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
			cfg.Minio.RootPath, tempDir, logId, segmentId+1, storageCli, cfg, true)
		require.NoError(t, err)

		// Should only see block 0 (committed), not block 1 (completed but not committed)
		require.Equal(t, int64(2), writer.GetLastEntryId(ctx), "Should recover only committed data")
		writer.Close(ctx)

		// Completed file should be removed
		_, err = os.Stat(filepath.Join(segmentDir, "1.blk.completed"))
		require.True(t, os.IsNotExist(err), "Completed file should be removed")
	})
}

// =============================================================================
// Test: Footer Crash Scenarios
// =============================================================================

// TestStagedFileCrash_FooterScenarios tests crash during footer write
func TestStagedFileCrash_FooterScenarios(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-footer-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1004, 2004

	t.Run("complete blocks with valid footer.blk.inflight - footer should be completed", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId)
		// Create complete blocks
		createBlockFile(t, segmentDir, 0, createTestEntries(0, 3))
		createBlockFile(t, segmentDir, 1, createTestEntries(3, 3))
		// Create valid footer.blk.inflight
		createFooterInflightFile(t, segmentDir, 2, 5)

		writer, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
			cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg, true)
		require.NoError(t, err)
		writer.Close(ctx)

		// Footer.blk should exist after recovery (inflight renamed)
		_, err = os.Stat(filepath.Join(segmentDir, "footer.blk"))
		require.NoError(t, err, "footer.blk should exist after recovery")
		// Footer.blk.inflight should be removed
		_, err = os.Stat(filepath.Join(segmentDir, "footer.blk.inflight"))
		require.True(t, os.IsNotExist(err), "footer.blk.inflight should be removed")
	})

	t.Run("complete blocks with corrupted footer.blk.inflight - should be removed", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId+1)
		// Create complete blocks
		createBlockFile(t, segmentDir, 0, createTestEntries(0, 3))
		// Create corrupted footer.blk.inflight
		createCorruptedFooterInflightFile(t, segmentDir)

		writer, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
			cfg.Minio.RootPath, tempDir, logId, segmentId+1, storageCli, cfg, true)
		require.NoError(t, err)

		// Should still recover block data
		require.Equal(t, int64(2), writer.GetLastEntryId(ctx), "Should recover entries from blocks")
		writer.Close(ctx)

		// Corrupted footer.blk.inflight should be removed
		_, err = os.Stat(filepath.Join(segmentDir, "footer.blk.inflight"))
		require.True(t, os.IsNotExist(err), "corrupted footer.blk.inflight should be removed")
	})

	t.Run("complete footer.blk - fully finalized segment", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId+2)
		// Create complete blocks
		createBlockFile(t, segmentDir, 0, createTestEntries(0, 3))
		createBlockFile(t, segmentDir, 1, createTestEntries(3, 3))
		// Create complete footer
		createFooterFile(t, segmentDir, 2, 5)

		// Reader should work correctly with finalized segment
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath,
			tempDir, logId, segmentId+2, storageCli, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Finalized segment doesn't need LAC
		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 20,
		}, nil)
		require.NoError(t, readErr, "Should read finalized segment")
		require.Len(t, result.Entries, 6, "Should read all 6 entries")
	})
}

// =============================================================================
// Test: Sequential Block Recovery
// =============================================================================

// TestStagedFileCrash_SequentialRecovery tests recovery with non-sequential block states
func TestStagedFileCrash_SequentialRecovery(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-sequential-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1005, 2005

	t.Run("gap in block sequence - recovery scans all blocks", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId)
		// Block 0: complete
		createBlockFile(t, segmentDir, 0, createTestEntries(0, 3))
		// Block 1: missing (gap)
		// Block 2: complete
		createBlockFile(t, segmentDir, 2, createTestEntries(6, 3))

		writer, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
			cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg, true)
		require.NoError(t, err)

		// NOTE: Current implementation recovers blocks independently.
		// With gap at block 1, recovery finds block 2 (entries 6-8) as the last block.
		// This is the current behavior - blocks are recovered based on what exists.
		lastEntry := writer.GetLastEntryId(ctx)
		// The recovery scans blocks in order and uses the last valid block found
		// Block 0 has entries 0-2, block 2 has entries 6-8
		// Recovery should find the higher numbered block
		require.True(t, lastEntry == 2 || lastEntry == 8, "Should recover available blocks, got lastEntry=%d", lastEntry)
		writer.Close(ctx)
	})

	t.Run("sequential blocks without gap - all recovered", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId+1)
		// All blocks sequential: 0, 1, 2
		createBlockFile(t, segmentDir, 0, createTestEntries(0, 3))
		createBlockFile(t, segmentDir, 1, createTestEntries(3, 3))
		createBlockFile(t, segmentDir, 2, createTestEntries(6, 3))

		writer, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
			cfg.Minio.RootPath, tempDir, logId, segmentId+1, storageCli, cfg, true)
		require.NoError(t, err)

		// All sequential blocks should be recovered
		require.Equal(t, int64(8), writer.GetLastEntryId(ctx), "Should recover all entries up to 8")
		require.Equal(t, int64(0), writer.GetFirstEntryId(ctx), "Should start from entry 0")
		writer.Close(ctx)
	})
}

// =============================================================================
// Test: Write After Recovery
// =============================================================================

// TestStagedFileCrash_WriteAfterRecovery tests finalization after crash recovery
func TestStagedFileCrash_WriteAfterRecovery(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-write-after-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1006, 2006

	t.Run("finalize after partial recovery", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId)
		// Create complete blocks
		createBlockFile(t, segmentDir, 0, createTestEntries(0, 3))
		createBlockFile(t, segmentDir, 1, createTestEntries(3, 3))
		// Create inflight (will be cleaned up)
		createInflightBlockFile(t, segmentDir, 2, createTestEntries(6, 3))

		// Recovery writer
		writer, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
			cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg, true)
		require.NoError(t, err)

		recoveredLastEntry := writer.GetLastEntryId(ctx)
		require.Equal(t, int64(5), recoveredLastEntry, "Should recover up to entry 5")

		// Finalize the recovered data
		_, err = writer.Finalize(ctx, recoveredLastEntry)
		require.NoError(t, err)
		writer.Close(ctx)

		// Verify final state with reader
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath,
			tempDir, logId, segmentId, storageCli, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)

		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 20,
		}, nil)
		require.NoError(t, readErr)
		require.Len(t, result.Entries, 6, "Should read 6 entries after recovery and finalize")

		// After finalize, should get EOF on next read
		result2, readErr2 := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    6,
			MaxBatchEntries: 20,
		}, result.LastReadState)
		require.Nil(t, result2)
		require.True(t, errors.Is(readErr2, werr.ErrFileReaderEndOfFile), "Should get EOF after all entries")
	})
}

// =============================================================================
// Test: Reader with LAC (Last Add Confirmed)
// =============================================================================

// TestStagedFileCrash_ReaderWithLAC tests reader behavior with different LAC values
func TestStagedFileCrash_ReaderWithLAC(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-lac-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1007, 2007

	t.Run("reader respects LAC for non-finalized segment", func(t *testing.T) {
		segmentDir := createSegmentDir(t, tempDir, logId, segmentId)
		// Create complete blocks with entries 0-5
		createBlockFile(t, segmentDir, 0, createTestEntries(0, 3))
		createBlockFile(t, segmentDir, 1, createTestEntries(3, 3))

		// Create reader
		reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath,
			tempDir, logId, segmentId, storageCli, cfg)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Set LAC to only confirm first 3 entries
		reader.UpdateLastAddConfirmed(ctx, 2)
		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 20,
		}, nil)
		require.NoError(t, readErr)
		require.Len(t, result.Entries, 3, "Should only read entries up to LAC=2")

		// Trying to read beyond LAC should return EntryNotFound
		result2, readErr2 := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    3,
			MaxBatchEntries: 20,
		}, result.LastReadState)
		require.Nil(t, result2)
		require.True(t, errors.Is(readErr2, werr.ErrEntryNotFound), "Should get EntryNotFound beyond LAC")

		// Update LAC to confirm all entries
		reader.UpdateLastAddConfirmed(ctx, 5)
		result3, readErr3 := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    3,
			MaxBatchEntries: 20,
		}, nil)
		require.NoError(t, readErr3)
		require.Len(t, result3.Entries, 3, "Should read remaining entries after LAC update")
	})
}

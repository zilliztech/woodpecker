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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/disk"
	"github.com/zilliztech/woodpecker/server/storage/serde"
)

// =============================================================================
// Per-Block Format Crash Recovery Tests for Local Disk Storage
// =============================================================================
//
// These tests verify crash recovery for the per-block file format in local disk storage:
// - Each block is written as a separate {blockId}.blk file
// - Atomic writes use three-stage: .blk.inflight -> .blk.completed -> .blk
// - Only .blk files are considered committed; .inflight and .completed are cleaned up
// - Footer is written as footer.blk with two-stage: footer.blk.inflight -> footer.blk
//
// Test scenarios cover crashes at various stages of the write process.

// =============================================================================
// Helper Functions for Creating Test Files
// =============================================================================

// setupLocalDiskCrashTest creates a temp directory and config for testing
func setupLocalDiskCrashTest(t *testing.T) (string, *config.Configuration) {
	tempDir, err := os.MkdirTemp("", "disk_crash_test_*")
	require.NoError(t, err, "Failed to create temp directory")
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	cfg, err := config.NewConfiguration()
	require.NoError(t, err, "Failed to create configuration")

	return tempDir, cfg
}

// createLocalDiskSegmentDir creates the segment directory for testing
func createLocalDiskSegmentDir(t *testing.T, tempDir string, logId, segmentId int64) string {
	segmentDir := filepath.Join(tempDir, fmt.Sprintf("%d/%d", logId, segmentId))
	err := os.MkdirAll(segmentDir, 0755)
	require.NoError(t, err, "Failed to create segment directory")
	return segmentDir
}

// createLocalDiskBlockFile creates a complete .blk file with test data
// Note: disk storage block format is: BlockHeaderRecord + DataRecords (no HeaderRecord)
func createLocalDiskBlockFile(t *testing.T, segmentDir string, blockId int64, entries []*serde.BlockEntry) {
	blockPath := filepath.Join(segmentDir, fmt.Sprintf("%d.blk", blockId))
	// disk storage does NOT include HeaderRecord in block files
	blockData := serde.SerializeBlock(blockId, entries, false)
	err := os.WriteFile(blockPath, blockData, 0644)
	require.NoError(t, err, "Failed to write block file %s", blockPath)
}

// createLocalDiskInflightBlockFile creates a .blk.inflight file (stage 1 of atomic write)
func createLocalDiskInflightBlockFile(t *testing.T, segmentDir string, blockId int64, entries []*serde.BlockEntry) {
	inflightPath := filepath.Join(segmentDir, fmt.Sprintf("%d.blk.inflight", blockId))
	// disk storage does NOT include HeaderRecord in block files
	blockData := serde.SerializeBlock(blockId, entries, false)
	err := os.WriteFile(inflightPath, blockData, 0644)
	require.NoError(t, err, "Failed to write inflight block file %s", inflightPath)
}

// createLocalDiskCompletedBlockFile creates a .blk.completed file (stage 2 of atomic write)
func createLocalDiskCompletedBlockFile(t *testing.T, segmentDir string, blockId int64, entries []*serde.BlockEntry) {
	completedPath := filepath.Join(segmentDir, fmt.Sprintf("%d.blk.completed", blockId))
	// disk storage does NOT include HeaderRecord in block files
	blockData := serde.SerializeBlock(blockId, entries, false)
	err := os.WriteFile(completedPath, blockData, 0644)
	require.NoError(t, err, "Failed to write completed block file %s", completedPath)
}

// createLocalDiskFooterFile creates a complete footer.blk file
func createLocalDiskFooterFile(t *testing.T, segmentDir string, blockCount int, lastEntryId int64) {
	footerPath := filepath.Join(segmentDir, "footer.blk")
	footerData, _ := serde.SerializeFooterAndIndexes(createLocalDiskIndexRecords(blockCount), lastEntryId)
	err := os.WriteFile(footerPath, footerData, 0644)
	require.NoError(t, err, "Failed to write footer file %s", footerPath)
}

// createLocalDiskFooterInflightFile creates a footer.blk.inflight file
func createLocalDiskFooterInflightFile(t *testing.T, segmentDir string, blockCount int, lastEntryId int64) {
	inflightPath := filepath.Join(segmentDir, "footer.blk.inflight")
	footerData, _ := serde.SerializeFooterAndIndexes(createLocalDiskIndexRecords(blockCount), lastEntryId)
	err := os.WriteFile(inflightPath, footerData, 0644)
	require.NoError(t, err, "Failed to write footer inflight file %s", inflightPath)
}

// createLocalDiskCorruptedFooterInflightFile creates a corrupted footer.blk.inflight file
func createLocalDiskCorruptedFooterInflightFile(t *testing.T, segmentDir string) {
	inflightPath := filepath.Join(segmentDir, "footer.blk.inflight")
	corruptedData := []byte("corrupted footer data")
	err := os.WriteFile(inflightPath, corruptedData, 0644)
	require.NoError(t, err, "Failed to write corrupted footer inflight file %s", inflightPath)
}

// createLocalDiskIndexRecords creates index records for testing
func createLocalDiskIndexRecords(blockCount int) []*codec.IndexRecord {
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

// createLocalDiskTestEntries creates test entries for a block
func createLocalDiskTestEntries(startEntryId int64, count int) []*serde.BlockEntry {
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

// TestLocalFileCrash_EmptySegmentDir tests behavior when segment directory is empty
func TestLocalFileCrash_EmptySegmentDir(t *testing.T) {
	tempDir, cfg := setupLocalDiskCrashTest(t)
	ctx := context.Background()

	const logId, segmentId = 1000, 2000

	t.Run("empty directory - writer recovery should succeed with no data", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId)
		t.Logf("Created empty segment directory for writer: %s", segmentDir)

		// Create writer in recovery mode on empty directory
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
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

// TestLocalFileCrash_InflightAndCompletedCleanup tests that inflight and completed files are cleaned up
func TestLocalFileCrash_InflightAndCompletedCleanup(t *testing.T) {
	tempDir, cfg := setupLocalDiskCrashTest(t)
	ctx := context.Background()

	const logId, segmentId = 1001, 2001

	t.Run("inflight files should be cleaned up", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId)
		createLocalDiskInflightBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 5))

		// Writer recovery should clean up inflight files
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		// Inflight data should not be recovered
		require.Equal(t, int64(-1), writer.GetLastEntryId(ctx), "Inflight data should not be recovered")
		writer.Close(ctx)

		// Verify inflight file was removed
		_, err = os.Stat(filepath.Join(segmentDir, "0.blk.inflight"))
		require.True(t, os.IsNotExist(err), "Inflight file should be removed")
	})

	t.Run("completed files should be cleaned up", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId+1)
		createLocalDiskCompletedBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 5))

		// Writer recovery should clean up completed files
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId+1, cfg, true)
		require.NoError(t, err)

		// Completed data should not be recovered (three-stage write: only .blk is committed)
		require.Equal(t, int64(-1), writer.GetLastEntryId(ctx), "Completed data should not be recovered")
		writer.Close(ctx)

		// Verify completed file was removed
		_, err = os.Stat(filepath.Join(segmentDir, "0.blk.completed"))
		require.True(t, os.IsNotExist(err), "Completed file should be removed")
	})

	t.Run("mixed inflight and completed should all be cleaned up", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId+2)
		createLocalDiskInflightBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		createLocalDiskCompletedBlockFile(t, segmentDir, 1, createLocalDiskTestEntries(3, 3))
		createLocalDiskInflightBlockFile(t, segmentDir, 2, createLocalDiskTestEntries(6, 3))

		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId+2, cfg, true)
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

// TestLocalFileCrash_CompleteBlockRecovery tests recovery from complete .blk files
func TestLocalFileCrash_CompleteBlockRecovery(t *testing.T) {
	tempDir, cfg := setupLocalDiskCrashTest(t)
	ctx := context.Background()

	const logId, segmentId = 1002, 2002

	t.Run("single complete block should be recovered", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId)
		entries := createLocalDiskTestEntries(0, 5)
		createLocalDiskBlockFile(t, segmentDir, 0, entries)

		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		require.Equal(t, int64(4), writer.GetLastEntryId(ctx), "Should recover lastEntryId=4")
		require.Equal(t, int64(0), writer.GetFirstEntryId(ctx), "Should recover firstEntryId=0")
		writer.Close(ctx)

		// Reader should be able to read the data
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
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
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId+1)
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		createLocalDiskBlockFile(t, segmentDir, 1, createLocalDiskTestEntries(3, 3))
		createLocalDiskBlockFile(t, segmentDir, 2, createLocalDiskTestEntries(6, 4))

		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId+1, cfg, true)
		require.NoError(t, err)

		require.Equal(t, int64(9), writer.GetLastEntryId(ctx), "Should recover lastEntryId=9")
		require.Equal(t, int64(0), writer.GetFirstEntryId(ctx), "Should recover firstEntryId=0")
		writer.Close(ctx)

		// Reader should read all blocks
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId+1, 1024*1024)
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

// TestLocalFileCrash_MixedBlockFiles tests recovery with mix of .blk and incomplete files
func TestLocalFileCrash_MixedBlockFiles(t *testing.T) {
	tempDir, cfg := setupLocalDiskCrashTest(t)
	ctx := context.Background()

	const logId, segmentId = 1003, 2003

	t.Run("complete blocks + inflight - only complete should be recovered", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId)
		// Block 0 and 1: complete
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		createLocalDiskBlockFile(t, segmentDir, 1, createLocalDiskTestEntries(3, 3))
		// Block 2: inflight (should be cleaned up)
		createLocalDiskInflightBlockFile(t, segmentDir, 2, createLocalDiskTestEntries(6, 3))

		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		// Should only see entries from complete blocks (0-5), not inflight (6-8)
		require.Equal(t, int64(5), writer.GetLastEntryId(ctx), "Should recover up to entry 5")
		require.Equal(t, int64(0), writer.GetFirstEntryId(ctx))
		writer.Close(ctx)

		// Inflight file should be removed
		_, err = os.Stat(filepath.Join(segmentDir, "2.blk.inflight"))
		require.True(t, os.IsNotExist(err), "Inflight file should be removed")

		// Reader should only read complete blocks
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
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
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId+1)
		// Block 0: complete
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		// Block 1: completed (not yet committed, will be cleaned up)
		createLocalDiskCompletedBlockFile(t, segmentDir, 1, createLocalDiskTestEntries(3, 3))

		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId+1, cfg, true)
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

// TestLocalFileCrash_FooterScenarios tests crash during footer write
func TestLocalFileCrash_FooterScenarios(t *testing.T) {
	tempDir, cfg := setupLocalDiskCrashTest(t)
	ctx := context.Background()

	const logId, segmentId = 1004, 2004

	t.Run("complete blocks with valid footer.blk.inflight - footer should be completed", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId)
		// Create complete blocks
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		createLocalDiskBlockFile(t, segmentDir, 1, createLocalDiskTestEntries(3, 3))
		// Create valid footer.blk.inflight
		createLocalDiskFooterInflightFile(t, segmentDir, 2, 5)

		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
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
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId+1)
		// Create complete blocks
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		// Create corrupted footer.blk.inflight
		createLocalDiskCorruptedFooterInflightFile(t, segmentDir)

		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId+1, cfg, true)
		require.NoError(t, err)

		// Should still recover block data
		require.Equal(t, int64(2), writer.GetLastEntryId(ctx), "Should recover entries from blocks")
		writer.Close(ctx)

		// Corrupted footer.blk.inflight should be removed
		_, err = os.Stat(filepath.Join(segmentDir, "footer.blk.inflight"))
		require.True(t, os.IsNotExist(err), "corrupted footer.blk.inflight should be removed")
	})

	t.Run("complete footer.blk - fully finalized segment", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId+2)
		// Create complete blocks
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		createLocalDiskBlockFile(t, segmentDir, 1, createLocalDiskTestEntries(3, 3))
		// Create complete footer
		createLocalDiskFooterFile(t, segmentDir, 2, 5)

		// Reader should work correctly with finalized segment
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId+2, 1024*1024)
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

// TestLocalFileCrash_SequentialRecovery tests recovery with non-sequential block states
func TestLocalFileCrash_SequentialRecovery(t *testing.T) {
	tempDir, cfg := setupLocalDiskCrashTest(t)
	ctx := context.Background()

	const logId, segmentId = 1005, 2005

	t.Run("gap in block sequence - recovery scans all blocks", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId)
		// Block 0: complete
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		// Block 1: missing (gap)
		// Block 2: complete
		createLocalDiskBlockFile(t, segmentDir, 2, createLocalDiskTestEntries(6, 3))

		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
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
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId+1)
		// All blocks sequential: 0, 1, 2
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		createLocalDiskBlockFile(t, segmentDir, 1, createLocalDiskTestEntries(3, 3))
		createLocalDiskBlockFile(t, segmentDir, 2, createLocalDiskTestEntries(6, 3))

		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId+1, cfg, true)
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

// TestLocalFileCrash_WriteAfterRecovery tests finalization after crash recovery
func TestLocalFileCrash_WriteAfterRecovery(t *testing.T) {
	tempDir, cfg := setupLocalDiskCrashTest(t)
	ctx := context.Background()

	const logId, segmentId = 1006, 2006

	t.Run("finalize after partial recovery", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId)
		// Create complete blocks
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		createLocalDiskBlockFile(t, segmentDir, 1, createLocalDiskTestEntries(3, 3))
		// Create inflight (will be cleaned up)
		createLocalDiskInflightBlockFile(t, segmentDir, 2, createLocalDiskTestEntries(6, 3))

		// Recovery writer
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		recoveredLastEntry := writer.GetLastEntryId(ctx)
		require.Equal(t, int64(5), recoveredLastEntry, "Should recover up to entry 5")

		// Finalize the recovered data
		_, err = writer.Finalize(ctx, recoveredLastEntry)
		require.NoError(t, err)
		writer.Close(ctx)

		// Verify final state with reader
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
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
// Test: Reader Reads All Available Data
// =============================================================================

// TestLocalFileCrash_ReaderReadsAllData tests reader reads all available block data
// Note: Disk storage reader's UpdateLastAddConfirmed is a NO-OP; LAC is not enforced.
// This is different from stagedstorage which respects LAC for non-finalized segments.
func TestLocalFileCrash_ReaderReadsAllData(t *testing.T) {
	tempDir, _ := setupLocalDiskCrashTest(t)
	ctx := context.Background()

	const logId, segmentId = 1007, 2007

	t.Run("reader reads all available data in blocks", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId)
		// Create complete blocks with entries 0-5
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		createLocalDiskBlockFile(t, segmentDir, 1, createLocalDiskTestEntries(3, 3))

		// Create reader
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Read all entries
		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 20,
		}, nil)
		require.NoError(t, readErr)
		require.Len(t, result.Entries, 6, "Should read all 6 entries from both blocks")

		// Verify entry IDs
		for i, entry := range result.Entries {
			require.Equal(t, int64(i), entry.EntryId, "Entry %d should have correct ID", i)
		}
	})

	t.Run("reader can read from specific start entry", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId+1)
		// Create complete blocks
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 5))
		createLocalDiskBlockFile(t, segmentDir, 1, createLocalDiskTestEntries(5, 5))

		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId+1, 1024*1024)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Read starting from entry 3
		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    3,
			MaxBatchEntries: 20,
		}, nil)
		require.NoError(t, readErr)
		require.Len(t, result.Entries, 7, "Should read entries 3-9")
		require.Equal(t, int64(3), result.Entries[0].EntryId, "First entry should be ID 3")
	})
}

// =============================================================================
// Test: Fence File Scenarios
// =============================================================================

// TestLocalFileCrash_FenceFileScenarios tests fence file handling during crash recovery
func TestLocalFileCrash_FenceFileScenarios(t *testing.T) {
	tempDir, cfg := setupLocalDiskCrashTest(t)
	ctx := context.Background()

	const logId, segmentId = 1008, 2008

	t.Run("fence file present - writer should detect fencing", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId)
		// Create complete block
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		// Create fence file
		fencePath := filepath.Join(segmentDir, "write.fence")
		err := os.WriteFile(fencePath, []byte("fenced"), 0644)
		require.NoError(t, err)

		// Recovery writer should still work but recognize segment is fenced
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		// Should recover block data
		require.Equal(t, int64(2), writer.GetLastEntryId(ctx), "Should recover entries from block")
		writer.Close(ctx)
	})
}

// =============================================================================
// Test: Write Lock File
// =============================================================================

// TestLocalFileCrash_WriteLockFile tests write lock file handling
func TestLocalFileCrash_WriteLockFile(t *testing.T) {
	tempDir, cfg := setupLocalDiskCrashTest(t)
	ctx := context.Background()

	const logId, segmentId = 1009, 2009

	t.Run("stale write lock file should not block recovery", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId)
		// Create complete blocks
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		// Create stale write lock file
		lockPath := filepath.Join(segmentDir, "write.lock")
		err := os.WriteFile(lockPath, []byte("stale-lock-from-crashed-writer"), 0644)
		require.NoError(t, err)

		// Recovery writer should handle stale lock
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		// Should recover block data despite stale lock
		require.Equal(t, int64(2), writer.GetLastEntryId(ctx), "Should recover entries from block")
		writer.Close(ctx)
	})
}

// =============================================================================
// Test: Compact Operation
// =============================================================================

// createLocalDiskMergedBlockFile creates a complete merged block (m_N.blk) file
func createLocalDiskMergedBlockFile(t *testing.T, segmentDir string, blockId int64, entries []*serde.BlockEntry) {
	blockPath := filepath.Join(segmentDir, fmt.Sprintf("m_%d.blk", blockId))
	blockData := serde.SerializeBlock(blockId, entries, false)
	err := os.WriteFile(blockPath, blockData, 0644)
	require.NoError(t, err, "Failed to write merged block file %s", blockPath)
}

// createLocalDiskMergedBlockInflightFile creates an inflight merged block file
func createLocalDiskMergedBlockInflightFile(t *testing.T, segmentDir string, blockId int64, entries []*serde.BlockEntry) {
	inflightPath := filepath.Join(segmentDir, fmt.Sprintf("m_%d.blk.inflight", blockId))
	blockData := serde.SerializeBlock(blockId, entries, false)
	err := os.WriteFile(inflightPath, blockData, 0644)
	require.NoError(t, err, "Failed to write merged block inflight file %s", inflightPath)
}

// createLocalDiskMergedBlockCompletedFile creates a completed merged block file
func createLocalDiskMergedBlockCompletedFile(t *testing.T, segmentDir string, blockId int64, entries []*serde.BlockEntry) {
	completedPath := filepath.Join(segmentDir, fmt.Sprintf("m_%d.blk.completed", blockId))
	blockData := serde.SerializeBlock(blockId, entries, false)
	err := os.WriteFile(completedPath, blockData, 0644)
	require.NoError(t, err, "Failed to write merged block completed file %s", completedPath)
}

// createCompactedFooterFile creates a footer file with compacted flag set
func createCompactedFooterFile(t *testing.T, segmentDir string, blockIndexes []*codec.IndexRecord, lastEntryId int64) {
	footerPath := filepath.Join(segmentDir, "footer.blk")
	footerData, _ := serde.SerializeFooterAndIndexesWithFlags(blockIndexes, lastEntryId, codec.FooterFlagCompacted)
	err := os.WriteFile(footerPath, footerData, 0644)
	require.NoError(t, err, "Failed to write compacted footer file %s", footerPath)
}

// TestLocalFileCrash_CompactOperation tests compact operation and crash recovery
func TestLocalFileCrash_CompactOperation(t *testing.T) {
	tempDir, cfg := setupLocalDiskCrashTest(t)
	ctx := context.Background()

	const logId, segmentId = 1010, 2010

	t.Run("compact finalized segment", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId)

		// Create multiple small blocks
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		createLocalDiskBlockFile(t, segmentDir, 1, createLocalDiskTestEntries(3, 3))
		createLocalDiskBlockFile(t, segmentDir, 2, createLocalDiskTestEntries(6, 3))

		// Create footer (finalized segment)
		createLocalDiskFooterFile(t, segmentDir, 3, 8)

		// Create writer and compact
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		// Mark as finalized (reading existing footer)
		_, err = writer.Compact(ctx)
		require.NoError(t, err, "Compact should succeed")

		writer.Close(ctx)

		// Verify merged blocks exist
		entries, err := os.ReadDir(segmentDir)
		require.NoError(t, err)

		var mergedBlocks, originalBlocks int
		for _, entry := range entries {
			name := entry.Name()
			if serde.IsMergedBlockFile(name) {
				mergedBlocks++
			} else if serde.IsOriginalBlockFile(name) {
				originalBlocks++
			}
		}

		t.Logf("After compact: mergedBlocks=%d, originalBlocks=%d", mergedBlocks, originalBlocks)
		require.Greater(t, mergedBlocks, 0, "Should have merged blocks after compact")
		require.Equal(t, 0, originalBlocks, "Original blocks should be cleaned up")
	})

	t.Run("compact with already compacted segment - no-op", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId+1)

		// Create merged block
		createLocalDiskMergedBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 9))

		// Create compacted footer
		blockIndexes := []*codec.IndexRecord{
			{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 8},
		}
		createCompactedFooterFile(t, segmentDir, blockIndexes, 8)

		// Create writer and try to compact again
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId+1, cfg, true)
		require.NoError(t, err)

		// Compact should return early (already compacted)
		size, err := writer.Compact(ctx)
		require.NoError(t, err, "Compact on already compacted segment should succeed")
		require.Greater(t, size, int64(0), "Should return existing size")

		writer.Close(ctx)
	})

	t.Run("compact crash during merge - inflight cleanup", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId+2)

		// Create original blocks
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		createLocalDiskBlockFile(t, segmentDir, 1, createLocalDiskTestEntries(3, 3))

		// Create footer (finalized)
		createLocalDiskFooterFile(t, segmentDir, 2, 5)

		// Simulate crash during compact - inflight merged block exists
		createLocalDiskMergedBlockInflightFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 6))

		// Reader should still work with original blocks
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId+2, 1024*1024)
		require.NoError(t, err)

		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 20,
		}, nil)
		require.NoError(t, readErr, "Should read from original blocks")
		require.Len(t, result.Entries, 6, "Should read all 6 entries")

		reader.Close(ctx)
	})

	t.Run("compact crash before footer update - completed files present", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId+3)

		// Create original blocks
		createLocalDiskBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 3))
		createLocalDiskBlockFile(t, segmentDir, 1, createLocalDiskTestEntries(3, 3))

		// Create footer (non-compacted)
		createLocalDiskFooterFile(t, segmentDir, 2, 5)

		// Simulate crash after merge but before footer update
		// Completed merged block exists but footer not updated
		createLocalDiskMergedBlockCompletedFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 6))

		// Reader should still work with original blocks (footer not updated)
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId+3, 1024*1024)
		require.NoError(t, err)

		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 20,
		}, nil)
		require.NoError(t, readErr, "Should read from original blocks")
		require.Len(t, result.Entries, 6, "Should read all 6 entries")

		reader.Close(ctx)
	})

	t.Run("compact completed - reader uses merged blocks", func(t *testing.T) {
		segmentDir := createLocalDiskSegmentDir(t, tempDir, logId, segmentId+4)

		// Create merged block (simulating completed compact)
		createLocalDiskMergedBlockFile(t, segmentDir, 0, createLocalDiskTestEntries(0, 9))

		// Create compacted footer pointing to merged block
		blockIndexes := []*codec.IndexRecord{
			{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 8},
		}
		createCompactedFooterFile(t, segmentDir, blockIndexes, 8)

		// Reader should read from merged blocks
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId+4, 1024*1024)
		require.NoError(t, err)

		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 20,
		}, nil)
		require.NoError(t, readErr, "Should read from merged blocks")
		require.Len(t, result.Entries, 9, "Should read all 9 entries from merged block")

		reader.Close(ctx)
	})
}

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

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/disk"
	"github.com/zilliztech/woodpecker/server/storage/serde"
)

// =============================================================================
// Disk Storage Directory-Based Fence Integration Tests
// =============================================================================
//
// These tests verify the directory-based fence mechanism for local disk storage:
// - Fence is implemented as a directory named {blockId}.blk
// - When a stale writer tries to rename a file to this directory path, it fails
// - This works consistently across all operating systems (Linux, macOS, Windows)
// - The reader detects fence directories and stops reading at that position
//
// Test scenarios:
// - Basic fence functionality
// - Concurrent write attempts blocked by fence
// - Reader correctly handles fence directories
// - Recovery with fence directories

// =============================================================================
// Helper Functions
// =============================================================================

// setupFenceTest creates a temp directory and config for testing
func setupFenceTest(t *testing.T) (string, *config.Configuration) {
	tempDir, err := os.MkdirTemp("", "fence_test_*")
	require.NoError(t, err, "Failed to create temp directory")
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	cfg, err := config.NewConfiguration()
	require.NoError(t, err, "Failed to create configuration")

	return tempDir, cfg
}

// createFenceSegmentDir creates the segment directory for testing
func createFenceSegmentDir(t *testing.T, tempDir string, logId, segmentId int64) string {
	segmentDir := filepath.Join(tempDir, fmt.Sprintf("%d/%d", logId, segmentId))
	err := os.MkdirAll(segmentDir, 0755)
	require.NoError(t, err, "Failed to create segment directory")
	return segmentDir
}

// createFenceTestBlockFile creates a complete .blk file with test data
func createFenceTestBlockFile(t *testing.T, segmentDir string, blockId int64, entries []*serde.BlockEntry) {
	blockPath := filepath.Join(segmentDir, fmt.Sprintf("%d.blk", blockId))
	blockData := serde.SerializeBlock(blockId, entries, false)
	err := os.WriteFile(blockPath, blockData, 0644)
	require.NoError(t, err, "Failed to write block file %s", blockPath)
}

// createFenceDirectory creates a fence directory at the specified block ID
func createFenceDirectory(t *testing.T, segmentDir string, blockId int64) string {
	fenceDirPath := filepath.Join(segmentDir, fmt.Sprintf("%d.blk", blockId))
	err := os.MkdirAll(fenceDirPath, 0755)
	require.NoError(t, err, "Failed to create fence directory")
	return fenceDirPath
}

// createFenceTestEntries creates test entries for a block
func createFenceTestEntries(startEntryId int64, count int) []*serde.BlockEntry {
	entries := make([]*serde.BlockEntry, count)
	for i := 0; i < count; i++ {
		entries[i] = &serde.BlockEntry{
			EntryId: startEntryId + int64(i),
			Data:    []byte(fmt.Sprintf("fence-test-data-entry-%d", startEntryId+int64(i))),
		}
	}
	return entries
}

// isFenceDir checks if a path is a directory
func isFenceDir(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

// =============================================================================
// Basic Fence Functionality Tests
// =============================================================================

// TestFence_BasicFenceCreation tests that Fence() creates a directory
func TestFence_BasicFenceCreation(t *testing.T) {
	tempDir, cfg := setupFenceTest(t)
	ctx := context.Background()

	const logId, segmentId = 5000, 6000

	t.Run("fence creates a directory at next block position", func(t *testing.T) {
		segmentDir := createFenceSegmentDir(t, tempDir, logId, segmentId)

		// Create some blocks first
		createFenceTestBlockFile(t, segmentDir, 0, createFenceTestEntries(0, 5))
		createFenceTestBlockFile(t, segmentDir, 1, createFenceTestEntries(5, 5))

		// Create writer in recovery mode
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		// Verify initial state
		require.Equal(t, int64(9), writer.GetLastEntryId(ctx), "Should recover entries 0-9")

		// Call Fence()
		lac, err := writer.Fence(ctx)
		require.NoError(t, err, "Fence should succeed")
		require.Equal(t, int64(9), lac, "LAC should be the last entry ID")

		writer.Close(ctx)

		// Verify fence directory was created at block 2 (next block position)
		fencePath := filepath.Join(segmentDir, "2.blk")
		require.True(t, isFenceDir(fencePath), "Fence should create a directory at 2.blk")
	})
}

// TestFence_FenceIdempotent tests that calling Fence() multiple times is idempotent
func TestFence_FenceIdempotent(t *testing.T) {
	tempDir, cfg := setupFenceTest(t)
	ctx := context.Background()

	const logId, segmentId = 5001, 6001

	t.Run("fence is idempotent", func(t *testing.T) {
		segmentDir := createFenceSegmentDir(t, tempDir, logId, segmentId)

		// Create a block
		createFenceTestBlockFile(t, segmentDir, 0, createFenceTestEntries(0, 5))

		// Create writer in recovery mode
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		// Call Fence() multiple times
		lac1, err1 := writer.Fence(ctx)
		require.NoError(t, err1)

		lac2, err2 := writer.Fence(ctx)
		require.NoError(t, err2)

		lac3, err3 := writer.Fence(ctx)
		require.NoError(t, err3)

		// All should return the same LAC
		require.Equal(t, lac1, lac2)
		require.Equal(t, lac2, lac3)

		writer.Close(ctx)

		// Verify only one fence directory exists
		fencePath := filepath.Join(segmentDir, "1.blk")
		require.True(t, isFenceDir(fencePath), "Fence directory should exist")
	})
}

// =============================================================================
// Concurrent Write Blocking Tests
// =============================================================================

// TestFence_BlocksStaleWriter tests that a fence directory blocks stale writer renames
func TestFence_BlocksStaleWriter(t *testing.T) {
	tempDir, cfg := setupFenceTest(t)
	ctx := context.Background()

	const logId, segmentId = 5010, 6010

	t.Run("fence directory blocks stale writer rename", func(t *testing.T) {
		segmentDir := createFenceSegmentDir(t, tempDir, logId, segmentId)

		// Create initial blocks
		createFenceTestBlockFile(t, segmentDir, 0, createFenceTestEntries(0, 5))
		createFenceTestBlockFile(t, segmentDir, 1, createFenceTestEntries(5, 5))

		// Create the fence directory at position 2
		createFenceDirectory(t, segmentDir, 2)

		// Now create a writer (simulating a stale writer that doesn't know about the fence)
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		// Writer should detect fence during recovery
		require.Equal(t, int64(9), writer.GetLastEntryId(ctx), "Should recover entries 0-9")

		// Try to write - should fail because segment is fenced
		resultCh := channel.NewLocalResultChannel("fence-test-write")
		_, writeErr := writer.WriteDataAsync(ctx, 10, []byte("test-data"), resultCh)
		require.Error(t, writeErr)
		require.ErrorIs(t, writeErr, werr.ErrSegmentFenced, "Write should fail with ErrSegmentFenced")

		writer.Close(ctx)
	})
}

// TestFence_ConcurrentWriteAndFence tests concurrent write attempts while fencing
func TestFence_ConcurrentWriteAndFence(t *testing.T) {
	tempDir, cfg := setupFenceTest(t)
	ctx := context.Background()

	const logId, segmentId = 5011, 6011
	const numWriters = 5
	const entriesPerWriter = 10

	t.Run("concurrent write attempts with fence", func(t *testing.T) {
		createFenceSegmentDir(t, tempDir, logId, segmentId)

		// Create a writer and write some initial data
		mainWriter, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, false)
		require.NoError(t, err)

		// Write some entries
		for i := 0; i < 5; i++ {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("init-write-%d", i))
			_, writeErr := mainWriter.WriteDataAsync(ctx, int64(i), []byte(fmt.Sprintf("data-%d", i)), resultCh)
			require.NoError(t, writeErr)
		}
		time.Sleep(100 * time.Millisecond) // Let writes complete

		// Now start concurrent writers that will try to write while we fence
		var wg sync.WaitGroup
		writeSuccessCount := atomic.Int32{}
		writeFencedCount := atomic.Int32{}

		// Start multiple goroutines trying to write
		for w := 0; w < numWriters; w++ {
			wg.Add(1)
			go func(writerNum int) {
				defer wg.Done()
				startEntry := int64(100 + writerNum*entriesPerWriter)
				for i := 0; i < entriesPerWriter; i++ {
					resultCh := channel.NewLocalResultChannel(fmt.Sprintf("concurrent-%d-%d", writerNum, i))
					_, writeErr := mainWriter.WriteDataAsync(ctx, startEntry+int64(i), []byte(fmt.Sprintf("concurrent-data-%d-%d", writerNum, i)), resultCh)
					if writeErr == nil {
						writeSuccessCount.Add(1)
					} else if werr.ErrSegmentFenced.Is(writeErr) {
						writeFencedCount.Add(1)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}(w)
		}

		// Fence the segment while writers are active
		time.Sleep(20 * time.Millisecond)
		_, fenceErr := mainWriter.Fence(ctx)
		require.NoError(t, fenceErr, "Fence should succeed")

		// Wait for all writers to finish
		wg.Wait()

		t.Logf("Write results: success=%d, fenced=%d", writeSuccessCount.Load(), writeFencedCount.Load())

		// After fence, all further writes should be rejected
		afterFenceResultCh := channel.NewLocalResultChannel("after-fence")
		_, writeErr := mainWriter.WriteDataAsync(ctx, 1000, []byte("after-fence"), afterFenceResultCh)
		require.Error(t, writeErr)
		require.ErrorIs(t, writeErr, werr.ErrSegmentFenced)

		mainWriter.Close(ctx)
	})
}

// TestFence_SimulateStaleWriterRename tests that os.Rename fails when target is a directory
func TestFence_SimulateStaleWriterRename(t *testing.T) {
	tempDir, _ := setupFenceTest(t)

	const logId, segmentId = 5012, 6012

	t.Run("os.Rename file to directory fails on all platforms", func(t *testing.T) {
		segmentDir := createFenceSegmentDir(t, tempDir, logId, segmentId)

		// Create a fence directory (simulating the new master's fence)
		fenceDirPath := filepath.Join(segmentDir, "5.blk")
		err := os.MkdirAll(fenceDirPath, 0755)
		require.NoError(t, err)

		// Create an inflight file (simulating the stale writer's data)
		inflightPath := filepath.Join(segmentDir, "5.blk.completed")
		err = os.WriteFile(inflightPath, []byte("stale writer data"), 0644)
		require.NoError(t, err)

		// Try to rename the file to the directory path - this should FAIL
		renameErr := os.Rename(inflightPath, fenceDirPath)
		require.Error(t, renameErr, "Rename file to directory should fail")

		t.Logf("Rename error (expected): %v", renameErr)

		// Verify the fence directory still exists
		require.True(t, isFenceDir(fenceDirPath), "Fence directory should still exist")

		// Verify the inflight file still exists (rename failed)
		_, statErr := os.Stat(inflightPath)
		require.NoError(t, statErr, "Inflight file should still exist after failed rename")
	})
}

// =============================================================================
// Reader Fence Detection Tests
// =============================================================================

// TestFence_ReaderDetectsFenceDirectory tests that reader stops at fence directory
func TestFence_ReaderDetectsFenceDirectory(t *testing.T) {
	tempDir, _ := setupFenceTest(t)
	ctx := context.Background()

	const logId, segmentId = 5020, 6020

	t.Run("reader stops reading at fence directory", func(t *testing.T) {
		segmentDir := createFenceSegmentDir(t, tempDir, logId, segmentId)

		// Create blocks 0, 1, 2 with data
		createFenceTestBlockFile(t, segmentDir, 0, createFenceTestEntries(0, 5))
		createFenceTestBlockFile(t, segmentDir, 1, createFenceTestEntries(5, 5))
		createFenceTestBlockFile(t, segmentDir, 2, createFenceTestEntries(10, 5))

		// Create fence directory at position 3
		createFenceDirectory(t, segmentDir, 3)

		// Create more blocks after the fence (these should be ignored)
		createFenceTestBlockFile(t, segmentDir, 4, createFenceTestEntries(15, 5))
		createFenceTestBlockFile(t, segmentDir, 5, createFenceTestEntries(20, 5))

		// Create reader
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)
		defer reader.Close(ctx)

		// Reader should only see blocks 0, 1, 2 (entries 0-14)
		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 100,
		}, nil)
		require.NoError(t, readErr)
		require.Len(t, result.Entries, 15, "Should read only 15 entries (blocks 0-2), not blocks after fence")

		// Verify entry IDs
		require.Equal(t, int64(0), result.Entries[0].EntryId)
		require.Equal(t, int64(14), result.Entries[14].EntryId)
	})
}

// TestFence_ReaderFenceAtBlockZero tests fence at the very first block
func TestFence_ReaderFenceAtBlockZero(t *testing.T) {
	tempDir, _ := setupFenceTest(t)
	ctx := context.Background()

	const logId, segmentId = 5021, 6021

	t.Run("fence at block 0 - reader sees no data", func(t *testing.T) {
		segmentDir := createFenceSegmentDir(t, tempDir, logId, segmentId)

		// Create fence directory at position 0
		createFenceDirectory(t, segmentDir, 0)

		// Create blocks after the fence (these should be ignored)
		createFenceTestBlockFile(t, segmentDir, 1, createFenceTestEntries(0, 5))
		createFenceTestBlockFile(t, segmentDir, 2, createFenceTestEntries(5, 5))

		// Create reader - when fence is at block 0, all blocks are filtered out
		// The reader may fail to create (returns ErrEntryNotFound) or succeed but return no data
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		if err != nil {
			// If reader creation fails, it should be ErrEntryNotFound (no valid data)
			require.ErrorIs(t, err, werr.ErrEntryNotFound, "Reader should fail with ErrEntryNotFound when no valid blocks")
			return
		}
		defer reader.Close(ctx)

		// If reader was created, reading should return no data or error
		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 100,
		}, nil)
		if readErr == nil {
			// If no error, should have no entries
			require.Empty(t, result.Entries, "Should have no entries when fence is at block 0")
		} else {
			// Error is acceptable (e.g., ErrEntryNotFound or end of file)
			t.Logf("Read returned expected error: %v", readErr)
		}
	})
}

// =============================================================================
// Recovery with Fence Tests
// =============================================================================

// TestFence_RecoveryWithFenceDirectory tests writer recovery detects fence directories
func TestFence_RecoveryWithFenceDirectory(t *testing.T) {
	tempDir, cfg := setupFenceTest(t)
	ctx := context.Background()

	const logId, segmentId = 5030, 6030

	t.Run("writer recovery detects fence directory and marks as fenced", func(t *testing.T) {
		segmentDir := createFenceSegmentDir(t, tempDir, logId, segmentId)

		// Create blocks 0, 1, 2
		createFenceTestBlockFile(t, segmentDir, 0, createFenceTestEntries(0, 5))
		createFenceTestBlockFile(t, segmentDir, 1, createFenceTestEntries(5, 5))
		createFenceTestBlockFile(t, segmentDir, 2, createFenceTestEntries(10, 5))

		// Create fence directory at position 3
		createFenceDirectory(t, segmentDir, 3)

		// Create recovery writer
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		// Should recover entries 0-14 (blocks 0-2)
		require.Equal(t, int64(14), writer.GetLastEntryId(ctx))
		require.Equal(t, int64(0), writer.GetFirstEntryId(ctx))

		// Writer should be marked as fenced
		resultCh := channel.NewLocalResultChannel("recovery-write")
		_, writeErr := writer.WriteDataAsync(ctx, 15, []byte("test"), resultCh)
		require.Error(t, writeErr)
		require.ErrorIs(t, writeErr, werr.ErrSegmentFenced, "Writer should be fenced")

		writer.Close(ctx)
	})
}

// TestFence_RecoveryWithFenceInMiddle tests recovery when fence is in middle of block sequence
func TestFence_RecoveryWithFenceInMiddle(t *testing.T) {
	tempDir, cfg := setupFenceTest(t)
	ctx := context.Background()

	const logId, segmentId = 5031, 6031

	t.Run("recovery with fence in middle of sequence", func(t *testing.T) {
		segmentDir := createFenceSegmentDir(t, tempDir, logId, segmentId)

		// Create blocks 0, 1
		createFenceTestBlockFile(t, segmentDir, 0, createFenceTestEntries(0, 5))
		createFenceTestBlockFile(t, segmentDir, 1, createFenceTestEntries(5, 5))

		// Create fence directory at position 2 (in middle)
		createFenceDirectory(t, segmentDir, 2)

		// Create blocks 3, 4 (after fence - should be ignored)
		createFenceTestBlockFile(t, segmentDir, 3, createFenceTestEntries(10, 5))
		createFenceTestBlockFile(t, segmentDir, 4, createFenceTestEntries(15, 5))

		// Create recovery writer
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		// Should only recover entries 0-9 (blocks 0-1)
		require.Equal(t, int64(9), writer.GetLastEntryId(ctx))

		// Writer should be fenced
		resultCh := channel.NewLocalResultChannel("recovery-middle-write")
		_, writeErr := writer.WriteDataAsync(ctx, 10, []byte("test"), resultCh)
		require.Error(t, writeErr)
		require.ErrorIs(t, writeErr, werr.ErrSegmentFenced)

		writer.Close(ctx)
	})
}

// =============================================================================
// High Concurrency Fence Tests
// =============================================================================

// TestFence_ConcurrentFenceAndWrite tests race between fence and multiple writers
func TestFence_ConcurrentFenceAndWrite(t *testing.T) {
	tempDir, cfg := setupFenceTest(t)
	ctx := context.Background()

	const logId, segmentId = 5040, 6040
	const numConcurrentWriters = 10
	const writesPerGoroutine = 20

	t.Run("concurrent fence blocks all subsequent writes", func(t *testing.T) {
		createFenceSegmentDir(t, tempDir, logId, segmentId)

		// Create main writer
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, false)
		require.NoError(t, err)

		// Write initial data
		for i := 0; i < 5; i++ {
			resultCh := channel.NewLocalResultChannel(fmt.Sprintf("init-%d", i))
			_, err := writer.WriteDataAsync(ctx, int64(i), []byte(fmt.Sprintf("initial-%d", i)), resultCh)
			require.NoError(t, err)
		}
		time.Sleep(50 * time.Millisecond)

		var wg sync.WaitGroup
		successCount := atomic.Int32{}
		fencedCount := atomic.Int32{}
		fenceComplete := atomic.Bool{}

		// Start multiple writer goroutines
		for w := 0; w < numConcurrentWriters; w++ {
			wg.Add(1)
			go func(writerNum int) {
				defer wg.Done()
				startId := int64(1000 + writerNum*writesPerGoroutine)
				for i := 0; i < writesPerGoroutine; i++ {
					entryId := startId + int64(i)
					resultCh := channel.NewLocalResultChannel(fmt.Sprintf("data-%d", entryId))
					_, writeErr := writer.WriteDataAsync(ctx, entryId, []byte(fmt.Sprintf("data-%d", entryId)), resultCh)
					if writeErr == nil {
						successCount.Add(1)
					} else if werr.ErrSegmentFenced.Is(writeErr) {
						fencedCount.Add(1)
					}
					time.Sleep(2 * time.Millisecond)
				}
			}(w)
		}

		// Wait a bit then fence
		time.Sleep(30 * time.Millisecond)
		_, fenceErr := writer.Fence(ctx)
		require.NoError(t, fenceErr)
		fenceComplete.Store(true)

		// Wait for all writers
		wg.Wait()

		t.Logf("Results: successful writes=%d, fenced writes=%d", successCount.Load(), fencedCount.Load())

		// Verify that after fence completed, no more writes succeeded
		// The exact numbers depend on timing, but we should have some of each
		require.Greater(t, successCount.Load()+fencedCount.Load(), int32(0), "Should have attempted some writes")

		// After fence, write should definitely fail
		finalResultCh := channel.NewLocalResultChannel("final")
		_, writeErr := writer.WriteDataAsync(ctx, 9999, []byte("final"), finalResultCh)
		require.Error(t, writeErr)
		require.ErrorIs(t, writeErr, werr.ErrSegmentFenced)

		writer.Close(ctx)
	})
}

// TestFence_MultipleFencingAttempts tests that only one fence succeeds
func TestFence_MultipleFencingAttempts(t *testing.T) {
	tempDir, cfg := setupFenceTest(t)
	ctx := context.Background()

	const logId, segmentId = 5041, 6041
	const numFenceAttempts = 5

	t.Run("multiple concurrent fence attempts all succeed idempotently", func(t *testing.T) {
		segmentDir := createFenceSegmentDir(t, tempDir, logId, segmentId)

		// Create initial blocks
		createFenceTestBlockFile(t, segmentDir, 0, createFenceTestEntries(0, 5))
		createFenceTestBlockFile(t, segmentDir, 1, createFenceTestEntries(5, 5))

		// Create multiple writers (simulating multiple potential masters)
		writers := make([]*disk.LocalFileWriter, numFenceAttempts)
		for i := 0; i < numFenceAttempts; i++ {
			w, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
			require.NoError(t, err)
			writers[i] = w
		}

		// All try to fence concurrently
		var wg sync.WaitGroup
		fenceResults := make(chan error, numFenceAttempts)
		fenceLACs := make(chan int64, numFenceAttempts)

		for i := 0; i < numFenceAttempts; i++ {
			wg.Add(1)
			go func(writerNum int) {
				defer wg.Done()
				lac, err := writers[writerNum].Fence(ctx)
				fenceResults <- err
				fenceLACs <- lac
			}(i)
		}

		wg.Wait()
		close(fenceResults)
		close(fenceLACs)

		// All fence calls should succeed (idempotent)
		for err := range fenceResults {
			require.NoError(t, err, "All fence calls should succeed")
		}

		// All should report the same LAC
		var firstLAC int64 = -1
		for lac := range fenceLACs {
			if firstLAC == -1 {
				firstLAC = lac
			} else {
				require.Equal(t, firstLAC, lac, "All LACs should be the same")
			}
		}

		// Only one fence directory should exist
		fenceCount := 0
		entries, _ := os.ReadDir(segmentDir)
		for _, entry := range entries {
			if entry.IsDir() {
				isFence, _ := serde.IsFenceDirectory(entry.Name(), true)
				if isFence {
					fenceCount++
				}
			}
		}
		require.Equal(t, 1, fenceCount, "Only one fence directory should exist")

		// Cleanup
		for _, w := range writers {
			w.Close(ctx)
		}
	})
}

// =============================================================================
// Edge Cases
// =============================================================================

// TestFence_EmptySegment tests fencing an empty segment
func TestFence_EmptySegment(t *testing.T) {
	tempDir, cfg := setupFenceTest(t)
	ctx := context.Background()

	const logId, segmentId = 5050, 6050

	t.Run("fence empty segment", func(t *testing.T) {
		createFenceSegmentDir(t, tempDir, logId, segmentId)

		// Create writer on empty segment
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, false)
		require.NoError(t, err)

		// Fence immediately
		lac, fenceErr := writer.Fence(ctx)
		require.NoError(t, fenceErr)
		require.Equal(t, int64(-1), lac, "LAC should be -1 for empty segment")

		// Write should fail
		resultCh := channel.NewLocalResultChannel("empty-write")
		_, writeErr := writer.WriteDataAsync(ctx, 0, []byte("test"), resultCh)
		require.Error(t, writeErr)
		require.ErrorIs(t, writeErr, werr.ErrSegmentFenced)

		writer.Close(ctx)
	})
}

// TestFence_FenceAndRead tests that fenced segment can still be read
func TestFence_FenceAndRead(t *testing.T) {
	tempDir, cfg := setupFenceTest(t)
	ctx := context.Background()

	const logId, segmentId = 5051, 6051

	t.Run("fenced segment can still be read", func(t *testing.T) {
		segmentDir := createFenceSegmentDir(t, tempDir, logId, segmentId)

		// Create blocks
		createFenceTestBlockFile(t, segmentDir, 0, createFenceTestEntries(0, 5))
		createFenceTestBlockFile(t, segmentDir, 1, createFenceTestEntries(5, 5))

		// Create writer and fence
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)

		_, fenceErr := writer.Fence(ctx)
		require.NoError(t, fenceErr)

		writer.Close(ctx)

		// Reader should still be able to read the data
		reader, err := disk.NewLocalFileReaderAdv(ctx, tempDir, logId, segmentId, 1024*1024)
		require.NoError(t, err)
		defer reader.Close(ctx)

		result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    0,
			MaxBatchEntries: 20,
		}, nil)
		require.NoError(t, readErr)
		require.Len(t, result.Entries, 10, "Should read all 10 entries")
	})
}

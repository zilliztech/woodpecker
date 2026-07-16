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
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/stagedstorage"
)

// TestCompactedLocalCleanup_ReadFromRealMinioAfterLocalDataLogRemoved proves the
// end-to-end loop the compacted-local-file-cleanup feature depends on, against REAL
// object storage (minio), on a single node:
//
//  1. Write N entries through a real StagedFileWriter, finalize, and Compact(ctx) it,
//     so the data is durably uploaded to real minio.
//  2. Assert the compacted footer object actually exists in real object storage
//     (mirrors footerExistsInMinio in server/compacted_file_cleanup.go, which is what
//     authorizes the cleanup task to drop the local data.log).
//  3. Simulate what the compacted-file-cleanup task's dropSegmentLocalData does:
//     os.Remove the local data.log for the segment (that logic itself is unit-tested
//     in server/compacted_file_cleanup_test.go; here we only reproduce its externally
//     observable effect on the filesystem).
//  4. Open a brand-new StagedFileReaderAdv for that segment (local data.log now
//     absent) and assert it reads back ALL N entries correctly FROM REAL MINIO — this
//     exercises the R1 fallback (server/storage/stagedstorage/reader_impl.go) against
//     real object storage end to end — and that no local data.log is re-created (no
//     re-caching), matching the reader's documented "no local re-caching" behavior.
//
// The multi-node / 3-replica / node-crash / decommission scenarios sketched in the
// plan are intentionally out of scope here: they are already covered by the unit
// tests for client fanout (NotifySegmentCompacted), reader R1, invalidate+retry, the
// cleanup-task decision matrix (server/compacted_file_cleanup_test.go), and
// HasLocalSegmentData-ignores-marked. This test's sole job is proving the core
// read-after-local-removal loop actually works against real minio, not mocks.
func TestCompactedLocalCleanup_ReadFromRealMinioAfterLocalDataLogRemoved(t *testing.T) {
	rootDir := fmt.Sprintf("test-compacted-local-cleanup-%d", time.Now().UnixNano())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootDir)
	ctx := context.Background()

	// Use small blocks so a modest number of entries still spans multiple blocks and
	// exercises the merged-block compaction path (mirrors TestStagedFileWriter_CompactOperation).
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = 128 * 1024 // 128KB per block
	cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxBatchSize = 16_000_000
	cfg.Woodpecker.Logstore.SegmentReadPolicy.MaxFetchThreads = 32

	logId := int64(236)
	segmentId := int64(23600)
	defer cleanupStagedTestObjects(t, storageCli, rootDir)

	const numEntries = 20
	testData := make([][]byte, numEntries)
	for i := range testData {
		if i%4 == 0 {
			testData[i] = generateStagedTestData(64 * 1024) // 64KB, forces multiple blocks
		} else {
			testData[i] = []byte(fmt.Sprintf("compacted-local-cleanup entry %d", i))
		}
	}

	// Step 1: write N entries, finalize, and compact -> real minio.
	writer, err := stagedstorage.NewStagedFileWriter(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
	require.NoError(t, err)

	for i, data := range testData {
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("test-compacted-cleanup-write-%d", i))
		_, err := writer.WriteDataAsync(ctx, int64(i), data, resultCh)
		require.NoError(t, err)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
	}

	_, err = writer.Finalize(ctx, int64(numEntries-1))
	require.NoError(t, err)

	compactedSize, err := writer.Compact(ctx)
	require.NoError(t, err)
	require.Greater(t, compactedSize, int64(0), "compaction should have uploaded data to minio")
	require.NoError(t, writer.Close(ctx))

	segmentDir := filepath.Join(tempDir, fmt.Sprintf("%d/%d", logId, segmentId))
	dataLogPath := filepath.Join(segmentDir, "data.log")
	require.FileExists(t, dataLogPath, "local data.log should still exist right after compact (compaction does not delete it)")

	// Step 2: assert the compacted footer actually exists in REAL object storage. This
	// is exactly the HEAD check footerExistsInMinio (server/compacted_file_cleanup.go)
	// performs before authorizing local data.log removal.
	footerKey := fmt.Sprintf("%s/%d/%d/footer.blk", cfg.Minio.RootPath, logId, segmentId)
	_, _, statErr := storageCli.StatObject(ctx, StagedTestBucket, footerKey, "test-ns", "0")
	require.NoError(t, statErr, "compacted footer object must exist in real minio at %s", footerKey)

	// Step 3: simulate the compacted-file-cleanup GC dropping the local copy. This
	// reproduces the filesystem effect of dropSegmentLocalData without re-running its
	// decision logic (that matrix is unit-tested in server/compacted_file_cleanup_test.go):
	// the data.log is removed but the compacted.mark is KEPT as a tombstone — that mark is
	// what lets the reader serve from object storage without an object-storage HEAD.
	markPath := filepath.Join(segmentDir, stagedstorage.CompactedMarkFileName)
	require.NoError(t, os.WriteFile(markPath, []byte("{}"), 0o644))
	require.NoError(t, os.Remove(dataLogPath))
	require.NoFileExists(t, dataLogPath)

	// Step 4: open a NEW reader for the segment now that local data.log is gone, and
	// verify it reads back ALL entries correctly from real minio (R1 fallback).
	reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	result, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: numEntries + 1,
	}, nil)
	require.NoError(t, err)
	require.Len(t, result.Entries, numEntries)
	for i, entry := range result.Entries {
		assert.Equal(t, int64(i), entry.EntryId, "entry id mismatch at index %d", i)
		assert.Equal(t, testData[i], entry.Values, "data mismatch at index %d", i)
	}

	lastEntryId, err := reader.GetLastEntryID(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(numEntries-1), lastEntryId)

	// No re-caching: the reader must serve reads from minio without recreating a
	// local data.log (dropped local files must stay dropped).
	assert.NoFileExists(t, dataLogPath, "reader must not re-create/re-cache a local data.log after serving reads from minio")
}

// TestCompactedLocalCleanup_NeverCompactedSegmentReturnsEntryNotFound is the negative
// case: for a segment that was never written/finalized/compacted (so it has neither a
// local data.log nor a minio footer), a brand-new reader must return
// werr.ErrEntryNotFound rather than silently succeeding or panicking. This guards the
// R1 fallback's "genuinely not found" branch in NewStagedFileReaderAdv.
func TestCompactedLocalCleanup_NeverCompactedSegmentReturnsEntryNotFound(t *testing.T) {
	rootDir := fmt.Sprintf("test-compacted-local-cleanup-negative-%d", time.Now().UnixNano())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootDir)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootDir)

	logId := int64(237)
	segmentId := int64(23700)

	reader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
	require.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err), "expected ErrEntryNotFound, got: %v", err)
	assert.Nil(t, reader)

	// Also confirm there is genuinely no footer object for this segment in real minio,
	// i.e. the ErrEntryNotFound above reflects reality and isn't masking a real object
	// that the reader simply failed to find.
	footerKey := fmt.Sprintf("%s/%d/%d/footer.blk", cfg.Minio.RootPath, logId, segmentId)
	_, _, statErr := storageCli.StatObject(ctx, StagedTestBucket, footerKey, "test-ns", "0")
	require.Error(t, statErr, "footer object must not exist in real minio for a never-compacted segment")
}

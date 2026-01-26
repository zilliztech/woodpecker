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
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/disk"
	"github.com/zilliztech/woodpecker/server/storage/serde"
)

// legacyBlock holds the entries for a single block in the legacy data.log format.
type legacyBlock struct {
	blockId int64
	entries []*serde.BlockEntry
}

// buildLegacyDataLog constructs a data.log byte slice from blocks.
// If addFooter is true, index records and a footer record are appended.
func buildLegacyDataLog(blocks []legacyBlock, addFooter bool) []byte {
	var buf bytes.Buffer

	// Track block indexes for the footer
	var blockIndexes []*codec.IndexRecord

	for i, blk := range blocks {
		includeFileHeader := (i == 0)
		blockBytes := serde.SerializeBlock(blk.blockId, blk.entries, includeFileHeader)

		startOffset := int64(buf.Len())
		buf.Write(blockBytes)
		blockSize := uint32(len(blockBytes))

		firstEntryId := blk.entries[0].EntryId
		lastEntryId := blk.entries[len(blk.entries)-1].EntryId

		blockIndexes = append(blockIndexes, &codec.IndexRecord{
			BlockNumber:  int32(blk.blockId),
			StartOffset:  startOffset,
			BlockSize:    blockSize,
			FirstEntryID: firstEntryId,
			LastEntryID:  lastEntryId,
		})
	}

	if addFooter && len(blockIndexes) > 0 {
		indexOffset := uint64(buf.Len())
		lac := blockIndexes[len(blockIndexes)-1].LastEntryID
		footerBytes, _ := serde.SerializeLegacyFooterAndIndexes(blockIndexes, indexOffset, lac)
		buf.Write(footerBytes)
	}

	return buf.Bytes()
}

// makeEntries creates a slice of BlockEntry starting at firstEntryId with count entries.
func makeEntries(firstEntryId int64, count int) []*serde.BlockEntry {
	entries := make([]*serde.BlockEntry, count)
	for i := 0; i < count; i++ {
		eid := firstEntryId + int64(i)
		entries[i] = &serde.BlockEntry{
			EntryId: eid,
			Data:    []byte(fmt.Sprintf("entry-%d", eid)),
		}
	}
	return entries
}

// writeLegacyDataLog writes raw bytes to the segment's data.log file.
func writeLegacyDataLog(t *testing.T, baseDir string, logId, segmentId int64, data []byte) {
	t.Helper()
	segDir := fmt.Sprintf("%s/%d/%d", baseDir, logId, segmentId)
	require.NoError(t, os.MkdirAll(segDir, 0755))
	dataLogPath := fmt.Sprintf("%s/data.log", segDir)
	require.NoError(t, os.WriteFile(dataLogPath, data, 0644))
}

// loadTestConfig returns a test configuration with block size set to 256KB.
func loadTestConfig(t *testing.T) *config.Configuration {
	t.Helper()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	blockSize := int64(256 * 1024)
	cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxFlushSize = config.NewByteSize(blockSize)
	return cfg
}

// readAllEntries reads all entries from the reader starting at startEntry until EOF or error.
// Returns the entries collected.
func readAllEntries(t *testing.T, ctx context.Context, baseDir string, logId, segmentId int64, startEntry int64) []*proto.LogEntry {
	t.Helper()
	reader, err := disk.NewLocalFileReaderAdv(ctx, baseDir, logId, segmentId, 16_000_000)
	require.NoError(t, err)
	defer reader.Close(ctx)

	var allEntries []*proto.LogEntry
	var lastState *proto.LastReadState

	for {
		batch, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    startEntry,
			MaxBatchEntries: 1000,
		}, lastState)
		if readErr != nil {
			// EOF or entry not found means we've read everything available
			if werr.ErrFileReaderEndOfFile.Is(readErr) || werr.ErrEntryNotFound.Is(readErr) {
				break
			}
			// Unexpected error
			t.Fatalf("unexpected read error: %v", readErr)
		}
		if batch == nil || len(batch.Entries) == 0 {
			break
		}
		allEntries = append(allEntries, batch.Entries...)
		lastState = batch.LastReadState
	}
	return allEntries
}

// verifyEntries checks count and content of entries against expected entry IDs and data.
func verifyEntries(t *testing.T, entries []*proto.LogEntry, expectedFirstEntry int64, expectedCount int) {
	t.Helper()
	require.Equal(t, expectedCount, len(entries), "entry count mismatch")
	for i, entry := range entries {
		expectedId := expectedFirstEntry + int64(i)
		assert.Equal(t, expectedId, entry.EntryId, "entry ID mismatch at index %d", i)
		expectedData := []byte(fmt.Sprintf("entry-%d", expectedId))
		assert.Equal(t, expectedData, entry.Values, "entry data mismatch at index %d", i)
	}
}

// recoverAndFinalize creates a recovery writer, verifies it detected legacy mode
// with the expected entry range, then finalizes and closes the writer.
func recoverAndFinalize(t *testing.T, ctx context.Context, baseDir string, logId, segmentId int64, cfg *config.Configuration, expectedFirstEntry, expectedLAC int64) {
	t.Helper()
	writer, err := disk.NewLocalFileWriterWithMode(ctx, baseDir, logId, segmentId, cfg, true)
	require.NoError(t, err)

	assert.True(t, writer.IsLegacyMode(), "writer should detect legacy mode")
	assert.Equal(t, expectedFirstEntry, writer.GetFirstEntryId(ctx), "recovered first entry ID")
	assert.Equal(t, expectedLAC, writer.GetLastEntryId(ctx), "recovered LAC")

	_, err = writer.Finalize(ctx, writer.GetLastEntryId(ctx))
	require.NoError(t, err)

	err = writer.Close(ctx)
	require.NoError(t, err)
}

// TestLegacyUpgrade_CompleteFinalized tests reading a fully finalized legacy data.log
// that contains header + 3 blocks (10 entries each) + indexes + footer.
func TestLegacyUpgrade_CompleteFinalized(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()
	cfg := loadTestConfig(t)

	logId := int64(100)
	segmentId := int64(1000)

	// Build legacy data.log: 3 blocks, 10 entries each, with footer
	blocks := []legacyBlock{
		{blockId: 0, entries: makeEntries(0, 10)},
		{blockId: 1, entries: makeEntries(10, 10)},
		{blockId: 2, entries: makeEntries(20, 10)},
	}
	data := buildLegacyDataLog(blocks, true)
	writeLegacyDataLog(t, tempDir, logId, segmentId, data)

	// Phase 1: Pre-recovery read - reader detects footer, reads all 30 entries
	t.Run("PreRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 30)
	})

	// Phase 2: Recovery writer detects finalized file (footer already present)
	t.Run("Recovery", func(t *testing.T) {
		recoverAndFinalize(t, ctx, tempDir, logId, segmentId, cfg, 0, 29)
	})

	// Phase 3: Post-recovery read - still reads 30 entries
	t.Run("PostRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 30)
	})
}

// TestLegacyUpgrade_IncompleteMultiBlock tests a legacy data.log with 3 blocks but no footer.
func TestLegacyUpgrade_IncompleteMultiBlock(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()
	cfg := loadTestConfig(t)

	logId := int64(101)
	segmentId := int64(1001)

	// Build legacy data.log: 3 blocks, 10 entries each, NO footer
	blocks := []legacyBlock{
		{blockId: 0, entries: makeEntries(0, 10)},
		{blockId: 1, entries: makeEntries(10, 10)},
		{blockId: 2, entries: makeEntries(20, 10)},
	}
	data := buildLegacyDataLog(blocks, false)
	writeLegacyDataLog(t, tempDir, logId, segmentId, data)

	// Phase 1: Pre-recovery read - reader scans file, reads all 30 entries
	t.Run("PreRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 30)
	})

	// Phase 2: Recovery writer full-scans, recovers 3 blocks, LAC=29, finalize appends footer
	t.Run("Recovery", func(t *testing.T) {
		recoverAndFinalize(t, ctx, tempDir, logId, segmentId, cfg, 0, 29)
	})

	// Phase 3: Post-recovery read - reader finds footer, reads all 30 entries
	t.Run("PostRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 30)
	})
}

// TestLegacyUpgrade_IncompleteSingleBlock tests a legacy data.log with 1 block and no footer.
func TestLegacyUpgrade_IncompleteSingleBlock(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()
	cfg := loadTestConfig(t)

	logId := int64(102)
	segmentId := int64(1002)

	// Build legacy data.log: 1 block, 5 entries, no footer
	blocks := []legacyBlock{
		{blockId: 0, entries: makeEntries(0, 5)},
	}
	data := buildLegacyDataLog(blocks, false)
	writeLegacyDataLog(t, tempDir, logId, segmentId, data)

	// Phase 1: Pre-recovery read
	t.Run("PreRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 5)
	})

	// Phase 2: Recovery
	t.Run("Recovery", func(t *testing.T) {
		recoverAndFinalize(t, ctx, tempDir, logId, segmentId, cfg, 0, 4)
	})

	// Phase 3: Post-recovery read
	t.Run("PostRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 5)
	})
}

// TestLegacyUpgrade_EmptyFile tests a 0-byte legacy data.log file.
// NOTE: A 0-byte data.log does NOT trigger legacy recovery in the writer because
// recoverFromBlockFilesUnsafe checks stat.Size() > 0 before entering legacy path.
// The writer falls through to per-block scan (finds nothing). The reader detects the
// file exists and enters legacy mode but finds no data to read.
// This test verifies the system handles leftover empty data.log files gracefully.
func TestLegacyUpgrade_EmptyFile(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()
	cfg := loadTestConfig(t)

	logId := int64(103)
	segmentId := int64(1003)

	// Write 0-byte data.log
	writeLegacyDataLog(t, tempDir, logId, segmentId, []byte{})

	// Phase 1: Pre-recovery read - reader returns no data
	t.Run("PreRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		assert.Empty(t, entries)
	})

	// Phase 2: Recovery - writer does NOT enter legacy mode (size=0 skips legacy check)
	t.Run("Recovery", func(t *testing.T) {
		writer, err := disk.NewLocalFileWriterWithMode(ctx, tempDir, logId, segmentId, cfg, true)
		require.NoError(t, err)
		assert.False(t, writer.IsLegacyMode(), "0-byte file should not trigger legacy mode")
		assert.Equal(t, int64(-1), writer.GetLastEntryId(ctx))
		err = writer.Close(ctx)
		require.NoError(t, err)
	})

	// Phase 3: Post-recovery read - no data
	t.Run("PostRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		assert.Empty(t, entries)
	})
}

// TestLegacyUpgrade_HeaderOnly tests a legacy data.log with only a HeaderRecord (25 bytes).
func TestLegacyUpgrade_HeaderOnly(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()
	cfg := loadTestConfig(t)

	logId := int64(104)
	segmentId := int64(1004)

	// Build data.log with only a HeaderRecord (no blocks)
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	headerBytes := codec.EncodeRecord(headerRecord)
	writeLegacyDataLog(t, tempDir, logId, segmentId, headerBytes)

	// Phase 1: Pre-recovery read - reader finds no blocks
	t.Run("PreRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		assert.Empty(t, entries)
	})

	// Phase 2: Recovery - writer enters legacy mode, scans header but no blocks, LAC=-1.
	// Finalize appends a footer to mark the segment as completed (empty).
	t.Run("Recovery", func(t *testing.T) {
		recoverAndFinalize(t, ctx, tempDir, logId, segmentId, cfg, 0, -1)
	})

	// Phase 3: Post-recovery read - reader finds footer, no blocks → EOF
	t.Run("PostRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		assert.Empty(t, entries)
	})
}

// TestLegacyUpgrade_TruncatedBlock tests a legacy data.log with 1 complete block
// and a partial 2nd block (BlockHeader only, data truncated).
func TestLegacyUpgrade_TruncatedBlock(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()
	cfg := loadTestConfig(t)

	logId := int64(105)
	segmentId := int64(1005)

	// Build block 0 normally (5 entries)
	block0 := serde.SerializeBlock(0, makeEntries(0, 5), true)

	// Build block 1 but truncate it: serialize a valid block, then take only the
	// BlockHeader portion (no actual data records)
	block1Full := serde.SerializeBlock(1, makeEntries(5, 5), false)
	// BlockHeader is RecordHeaderSize + BlockHeaderRecordSize = 9 + 28 = 37 bytes
	blockHeaderOnly := block1Full[:codec.RecordHeaderSize+codec.BlockHeaderRecordSize]

	var buf bytes.Buffer
	buf.Write(block0)
	buf.Write(blockHeaderOnly)
	writeLegacyDataLog(t, tempDir, logId, segmentId, buf.Bytes())

	// Phase 1: Pre-recovery read - reader scans, reads 5 entries from block 0 (block 1 fails)
	t.Run("PreRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 5)
	})

	// Phase 2: Recovery - writer scans, recovers block 0 only, LAC=4
	t.Run("Recovery", func(t *testing.T) {
		recoverAndFinalize(t, ctx, tempDir, logId, segmentId, cfg, 0, 4)
	})

	// Phase 3: Post-recovery read - reads 5 entries
	t.Run("PostRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 5)
	})
}

// TestLegacyUpgrade_LargeMultiBlock tests a legacy data.log with 10 blocks (100 entries total).
func TestLegacyUpgrade_LargeMultiBlock(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()
	cfg := loadTestConfig(t)

	logId := int64(106)
	segmentId := int64(1006)

	// Build 10 blocks, 10 entries each = 100 total, no footer
	blocks := make([]legacyBlock, 10)
	for i := 0; i < 10; i++ {
		blocks[i] = legacyBlock{
			blockId: int64(i),
			entries: makeEntries(int64(i*10), 10),
		}
	}
	data := buildLegacyDataLog(blocks, false)
	writeLegacyDataLog(t, tempDir, logId, segmentId, data)

	// Phase 1: Pre-recovery read
	t.Run("PreRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 100)
	})

	// Phase 2: Recovery
	t.Run("Recovery", func(t *testing.T) {
		recoverAndFinalize(t, ctx, tempDir, logId, segmentId, cfg, 0, 99)
	})

	// Phase 3: Post-recovery read
	t.Run("PostRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 100)
	})
}

// TestLegacyUpgrade_CorruptedMiddleBlock tests a legacy data.log with block 0 (5 entries),
// then a corrupted block 1 (bad CRC), then block 2 (5 entries).
// The reader and recovery writer should stop at the corruption.
func TestLegacyUpgrade_CorruptedMiddleBlock(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()
	cfg := loadTestConfig(t)

	logId := int64(107)
	segmentId := int64(1007)

	// Build block 0 normally
	block0 := serde.SerializeBlock(0, makeEntries(0, 5), true)

	// Build block 1 but corrupt the data portion
	block1Full := serde.SerializeBlock(1, makeEntries(5, 5), false)
	// Corrupt some data bytes in the block (after the BlockHeader record)
	corruptedBlock1 := make([]byte, len(block1Full))
	copy(corruptedBlock1, block1Full)
	headerSize := codec.RecordHeaderSize + codec.BlockHeaderRecordSize
	if len(corruptedBlock1) > headerSize+4 {
		// Flip some data bytes to break CRC
		corruptedBlock1[headerSize+0] ^= 0xFF
		corruptedBlock1[headerSize+1] ^= 0xFF
		corruptedBlock1[headerSize+2] ^= 0xFF
		corruptedBlock1[headerSize+3] ^= 0xFF
	}

	// Build block 2 normally
	block2 := serde.SerializeBlock(2, makeEntries(10, 5), false)

	var buf bytes.Buffer
	buf.Write(block0)
	buf.Write(corruptedBlock1)
	buf.Write(block2)
	writeLegacyDataLog(t, tempDir, logId, segmentId, buf.Bytes())

	// Phase 1: Pre-recovery read - reader stops at block 0 (block 1 CRC failure stops scan)
	t.Run("PreRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 5)
	})

	// Phase 2: Recovery - writer scans, stops at corruption, recovers block 0 only, LAC=4
	t.Run("Recovery", func(t *testing.T) {
		recoverAndFinalize(t, ctx, tempDir, logId, segmentId, cfg, 0, 4)
	})

	// Phase 3: Post-recovery read - reads 5 entries from block 0
	t.Run("PostRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 5)
	})
}

// TestLegacyUpgrade_TrailingGarbage tests a legacy data.log with valid blocks followed
// by random garbage bytes that don't form a valid RecordHeader.
// This simulates a crash during a write where partial bytes were flushed to disk.
func TestLegacyUpgrade_TrailingGarbage(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()
	cfg := loadTestConfig(t)

	logId := int64(108)
	segmentId := int64(1008)

	// Build 1 valid block (5 entries) + trailing garbage bytes
	block0 := serde.SerializeBlock(0, makeEntries(0, 5), true)
	garbage := make([]byte, 64)
	for i := range garbage {
		garbage[i] = byte(i*7 + 13) // arbitrary non-record bytes
	}

	var buf bytes.Buffer
	buf.Write(block0)
	buf.Write(garbage)
	writeLegacyDataLog(t, tempDir, logId, segmentId, buf.Bytes())

	// Phase 1: Pre-recovery read - reader reads block 0, stops at garbage
	t.Run("PreRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 5)
	})

	// Phase 2: Recovery - writer full-scans, recovers block 0, ignores garbage
	t.Run("Recovery", func(t *testing.T) {
		recoverAndFinalize(t, ctx, tempDir, logId, segmentId, cfg, 0, 4)
	})

	// Phase 3: Post-recovery read
	t.Run("PostRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 5)
	})
}

// TestLegacyUpgrade_GarbageContent tests a legacy data.log that contains non-zero random
// bytes with no valid HeaderRecord. The writer should detect !headerFound and reset all
// state to empty. The reader should produce no entries.
func TestLegacyUpgrade_GarbageContent(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()
	cfg := loadTestConfig(t)

	logId := int64(109)
	segmentId := int64(1009)

	// Write garbage bytes (no valid header record)
	garbage := make([]byte, 128)
	for i := range garbage {
		garbage[i] = byte(i*13 + 7)
	}
	writeLegacyDataLog(t, tempDir, logId, segmentId, garbage)

	// Phase 1: Pre-recovery read - reader enters legacy mode but can't decode any blocks
	t.Run("PreRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		assert.Empty(t, entries)
	})

	// Phase 2: Recovery - writer enters legacy mode (size > 0), full scan finds no valid
	// header, resets state. Finalize writes a footer marking segment as completed (empty).
	t.Run("Recovery", func(t *testing.T) {
		recoverAndFinalize(t, ctx, tempDir, logId, segmentId, cfg, -1, -1)
	})

	// Phase 3: Post-recovery read - no data
	t.Run("PostRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		assert.Empty(t, entries)
	})
}

// TestLegacyUpgrade_SingleEntry tests a legacy data.log with the minimum viable data:
// one block containing exactly one entry (firstEntryId == lastEntryId == 0).
func TestLegacyUpgrade_SingleEntry(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()
	cfg := loadTestConfig(t)

	logId := int64(110)
	segmentId := int64(1010)

	// Build 1 block with 1 entry, no footer
	blocks := []legacyBlock{
		{blockId: 0, entries: makeEntries(0, 1)},
	}
	data := buildLegacyDataLog(blocks, false)
	writeLegacyDataLog(t, tempDir, logId, segmentId, data)

	// Phase 1: Pre-recovery read
	t.Run("PreRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 1)
	})

	// Phase 2: Recovery - firstEntry=0, LAC=0 (single entry)
	t.Run("Recovery", func(t *testing.T) {
		recoverAndFinalize(t, ctx, tempDir, logId, segmentId, cfg, 0, 0)
	})

	// Phase 3: Post-recovery read
	t.Run("PostRecoveryRead", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 1)
	})
}

// TestLegacyUpgrade_RecoverIdempotent tests that recovering and finalizing a legacy data.log
// twice produces correct results. This simulates: first upgrade finalize succeeds but the
// process crashes before metadata update, then the second startup recovers again.
func TestLegacyUpgrade_RecoverIdempotent(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()
	cfg := loadTestConfig(t)

	logId := int64(111)
	segmentId := int64(1011)

	// Build legacy data.log: 3 blocks, 10 entries each, NO footer
	blocks := []legacyBlock{
		{blockId: 0, entries: makeEntries(0, 10)},
		{blockId: 1, entries: makeEntries(10, 10)},
		{blockId: 2, entries: makeEntries(20, 10)},
	}
	data := buildLegacyDataLog(blocks, false)
	writeLegacyDataLog(t, tempDir, logId, segmentId, data)

	// First recovery + finalize: appends indexes + footer to data.log
	t.Run("FirstRecovery", func(t *testing.T) {
		recoverAndFinalize(t, ctx, tempDir, logId, segmentId, cfg, 0, 29)
	})

	// Verify data is readable after first finalize
	t.Run("AfterFirstFinalize", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 30)
	})

	// Second recovery: detects footer at end of data.log → already finalized.
	// Finalize() returns early (w.finalized.Load() == true).
	t.Run("SecondRecovery", func(t *testing.T) {
		recoverAndFinalize(t, ctx, tempDir, logId, segmentId, cfg, 0, 29)
	})

	// Data still readable after second recovery
	t.Run("AfterSecondFinalize", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 0)
		verifyEntries(t, entries, 0, 30)
	})
}

// TestLegacyUpgrade_ReadFromMiddle tests reading from a non-zero startEntryID on a
// finalized legacy data.log. This exercises the SearchBlock path in ReadNextBatchAdv
// which uses legacy IndexRecord.StartOffset values (absolute file offsets).
func TestLegacyUpgrade_ReadFromMiddle(t *testing.T) {
	tempDir := setupLocalFileTest(t)
	ctx := context.Background()
	cfg := loadTestConfig(t)

	logId := int64(112)
	segmentId := int64(1012)

	// Build legacy data.log: 3 blocks, 10 entries each, NO footer
	blocks := []legacyBlock{
		{blockId: 0, entries: makeEntries(0, 10)},  // entries 0-9
		{blockId: 1, entries: makeEntries(10, 10)}, // entries 10-19
		{blockId: 2, entries: makeEntries(20, 10)}, // entries 20-29
	}
	data := buildLegacyDataLog(blocks, false)
	writeLegacyDataLog(t, tempDir, logId, segmentId, data)

	// Recovery + finalize to get proper footer with block indexes
	t.Run("Recovery", func(t *testing.T) {
		recoverAndFinalize(t, ctx, tempDir, logId, segmentId, cfg, 0, 29)
	})

	// Read from entry 15 (middle of block 1) → should get entries 15-29
	t.Run("ReadFromEntry15", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 15)
		verifyEntries(t, entries, 15, 15)
	})

	// Read from entry 20 (start of block 2) → should get entries 20-29
	t.Run("ReadFromEntry20", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 20)
		verifyEntries(t, entries, 20, 10)
	})

	// Read from entry 25 (middle of block 2) → should get entries 25-29
	t.Run("ReadFromEntry25", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 25)
		verifyEntries(t, entries, 25, 5)
	})

	// Read from entry 30 (beyond LAC) → should get EOF (no entries)
	t.Run("ReadBeyondLAC", func(t *testing.T) {
		entries := readAllEntries(t, ctx, tempDir, logId, segmentId, 30)
		assert.Empty(t, entries)
	})
}

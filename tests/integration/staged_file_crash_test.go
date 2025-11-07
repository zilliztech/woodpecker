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
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/stagedstorage"
)

const (
	// test File records
	// {HeaderRecord,BlockHeaderRecord,DataRecord,BlockHeaderRecord,DataRecord,IndexRecord,IndexRecord,FooterRecord}
	headerRecordIndex            = 0
	firstBlockHeaderRecordIndex  = 1
	firstDataRecordIndex         = 2
	secondBlockHeaderRecordIndex = 3
	secondDataRecordIndex        = 4
	firstIndexRecordIndex        = 5
	secondIndexRecordIndex       = 6
	footerRecordIndex            = 7
)

// RecordPosition represents the byte range of a record in the file
type RecordPosition struct {
	RecordType string // "header", "data", "block_header", "index", "footer"
	EntryID    int64  // For data records, -1 for metadata records
	StartPos   int64
	EndPos     int64
}

// PreparedTestData contains information about a prepared test file
type PreparedTestData struct {
	FilePath        string
	TotalSize       int64
	EntryIDs        []int64
	RecordPositions []RecordPosition
}

// prepareCompleteTestFile prepares a complete finalized file with fixed 5 entries
// Returns PreparedTestData with file path and record positions for crash simulation
func prepareCompleteTestFile(t *testing.T, ctx context.Context, cfg *config.Configuration, tempDir string,
	logId, segmentId int64, storageCli objectstorage.ObjectStorage) *PreparedTestData {

	const numEntries = 2
	segmentFilePath := filepath.Join(tempDir, fmt.Sprintf("%d/%d/data.log", logId, segmentId))

	writer, err := stagedstorage.NewStagedFileWriter(ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg)
	require.NoError(t, err)

	entryIds := make([]int64, numEntries)
	testDataList := make([][]byte, numEntries)

	// Write fixed 5 entries
	for i := 0; i < numEntries; i++ {
		testData := []byte(fmt.Sprintf("TestData-%d-%s", i, generateStagedTestData(50)))
		testDataList[i] = testData
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("prepare-complete-%d", i))
		returnedId, err := writer.WriteDataAsync(ctx, int64(i), testData, resultCh)
		require.NoError(t, err)
		entryIds[i] = returnedId

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(i), result.SyncedId)
	}

	// Finalize to create complete file structure: header | datablk | index session | footer
	lastEntryId, err := writer.Finalize(ctx, int64(numEntries-1))
	require.NoError(t, err)
	require.Equal(t, int64(numEntries-1), lastEntryId)

	writer.Close(ctx)

	// Parse file to extract record positions
	recordPositions := parseFileRecordPositions(t, segmentFilePath)

	stat, err := os.Stat(segmentFilePath)
	require.NoError(t, err)

	result := &PreparedTestData{
		FilePath:        segmentFilePath,
		TotalSize:       stat.Size(),
		EntryIDs:        entryIds,
		RecordPositions: recordPositions,
	}

	t.Logf("Prepared complete test file: %s, size=%d bytes, %d entries, %d records",
		segmentFilePath, result.TotalSize, numEntries, len(recordPositions))

	return result
}

// parseFileRecordPositions parses the file and returns positions of all records
func parseFileRecordPositions(t *testing.T, filePath string) []RecordPosition {
	file, err := os.Open(filePath)
	require.NoError(t, err)
	defer file.Close()

	stat, err := file.Stat()
	require.NoError(t, err)
	fileSize := stat.Size()

	data := make([]byte, fileSize)
	_, err = io.ReadFull(file, data)
	require.NoError(t, err)

	positions := make([]RecordPosition, 0)
	offset := int64(0)

	// Parse records sequentially
	for offset < fileSize {
		if offset+codec.RecordHeaderSize > fileSize {
			break
		}

		startPos := offset

		// Read record header to get payload length
		// Record header: CRC32(4) + Type(1) + Length(4)
		recordType := data[offset+4]
		payloadLength := binary.LittleEndian.Uint32(data[offset+5 : offset+9])
		recordSize := int64(codec.RecordHeaderSize) + int64(payloadLength)

		// Check if we have complete record
		if offset+recordSize > fileSize {
			t.Logf("Incomplete record at offset %d: need %d bytes, only %d available", offset, recordSize, fileSize-offset)
			break
		}

		endPos := startPos + recordSize

		var recordTypeStr string
		var entryID int64 = -1

		switch recordType {
		case codec.HeaderRecordType:
			recordTypeStr = "header"
		case codec.DataRecordType:
			recordTypeStr = "data"
		case codec.BlockHeaderRecordType:
			recordTypeStr = "block_header"
		case codec.IndexRecordType:
			recordTypeStr = "index"
		case codec.FooterRecordType:
			recordTypeStr = "footer"
		default:
			recordTypeStr = fmt.Sprintf("unknown_%d", recordType)
		}

		positions = append(positions, RecordPosition{
			RecordType: recordTypeStr,
			EntryID:    entryID,
			StartPos:   startPos,
			EndPos:     endPos,
		})

		offset = endPos
	}

	return positions
}

func TestStagedFileWriter_CrashRecovery_Empty(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-after-header-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1000, 2000

	// prepare test file
	preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId, storageCli)
	crashPos := int64(0)

	t.Run("Testing crash at empty file", func(t *testing.T) {
		// test crash empty file & recover the crash file
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId,
			storageCli, preparedData.FilePath, crashPos,
			int64(-1), 0, werr.ErrEntryNotFound, // Pre-read: 0 entries, no specific error expected
			int64(-1), 0, werr.ErrFileReaderEndOfFile) // Post-read: 0 entries after recovery from empty file

		// test recover the completed empty file
		recoveryWriter, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
			cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg, true)
		require.NoError(t, err, "Recovery empty completed file failed for log=%d segment=%d (expected recovery to always succeed)", logId, segmentId)
		require.NotNil(t, recoveryWriter)
		firstEntryID := recoveryWriter.GetFirstEntryId(context.TODO())
		require.Equal(t, firstEntryID, int64(-1))
		lastEntryID := recoveryWriter.GetLastEntryId(context.TODO())
		require.Equal(t, lastEntryID, int64(-1))
		footer := recoveryWriter.GetRecoveredFooter()
		require.NotNil(t, footer)
		require.Equal(t, footer.LAC, int64(-1))
	})
}

// TestStagedFileWriter_CrashRecovery_HeaderRecord tests crash right after header record
func TestStagedFileWriter_CrashRecovery_HeaderRecord(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-after-header-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1001, 2001

	t.Run("crash after file headerRecord, before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+100, storageCli)
		crashPos := preparedData.RecordPositions[headerRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+100,
			storageCli, preparedData.FilePath, crashPos,
			int64(-1), 0, werr.ErrEntryNotFound,
			int64(-1), 0, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash after file headerRecord, after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+200, storageCli)
		crashPos := preparedData.RecordPositions[headerRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+200,
			storageCli, preparedData.FilePath, crashPos,
			int64(0), 0, werr.ErrEntryNotFound,
			int64(0), 0, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within file headerRecord", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+300, storageCli)
		rec := preparedData.RecordPositions[headerRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+300,
			storageCli, preparedData.FilePath, crashPos,
			int64(-1), 0, werr.ErrEntryNotFound,
			int64(-1), 0, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within file headerRecord", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+400, storageCli)
		rec := preparedData.RecordPositions[headerRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+400,
			storageCli, preparedData.FilePath, crashPos,
			int64(0), 0, werr.ErrEntryNotFound,
			int64(0), 0, werr.ErrFileReaderEndOfFile)
	})
}

// TestStagedFileWriter_CrashRecovery_FirstBlockHeaderRecord tests crash scenarios for first block header record
func TestStagedFileWriter_CrashRecovery_FirstBlockHeaderRecord(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-first-blkhdr-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1002, 2002

	t.Run("crash after first block header record, before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+100, storageCli)
		crashPos := preparedData.RecordPositions[firstBlockHeaderRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+100,
			storageCli, preparedData.FilePath, crashPos,
			int64(-1), 0, werr.ErrEntryNotFound,
			int64(-1), 0, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash after first block header record, after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+200, storageCli)
		crashPos := preparedData.RecordPositions[firstBlockHeaderRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+200,
			storageCli, preparedData.FilePath, crashPos,
			int64(0), 0, werr.ErrEntryNotFound,
			int64(0), 0, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within first block header record, before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+300, storageCli)
		rec := preparedData.RecordPositions[firstBlockHeaderRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+300,
			storageCli, preparedData.FilePath, crashPos,
			int64(-1), 0, werr.ErrEntryNotFound,
			int64(-1), 0, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within first block header record, after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+400, storageCli)
		rec := preparedData.RecordPositions[firstBlockHeaderRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+400,
			storageCli, preparedData.FilePath, crashPos,
			int64(0), 0, werr.ErrEntryNotFound,
			int64(0), 0, werr.ErrFileReaderEndOfFile)
	})
}

// TestStagedFileWriter_CrashRecovery_FirstDataRecord tests crash scenarios for first data record
func TestStagedFileWriter_CrashRecovery_FirstDataRecord(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-first-data-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1003, 2003

	t.Run("crash after first data record, before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+100, storageCli)
		crashPos := preparedData.RecordPositions[firstDataRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+100,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, no records found
			int64(-1), 0, werr.ErrEntryNotFound,
			// if finalized, reader's LAC will be ignore, using footer's LAC instead, so it should read 1 entry at first batchRead, and get EOF at second read
			int64(-1), 1, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash after first data record, after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+200, storageCli)
		crashPos := preparedData.RecordPositions[firstDataRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+200,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, found 1 record
			int64(0), 1, werr.ErrEntryNotFound,
			// if finalized, reader's LAC will be ignore, using footer's LAC instead, so it should read 1 entry at first batchRead, and get EOF at second read
			int64(0), 1, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within first data record, before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+300, storageCli)
		rec := preparedData.RecordPositions[firstDataRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+300,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, incomplete data record cannot be read
			int64(-1), 0, werr.ErrEntryNotFound,
			// if finalized, incomplete entry is discarded during recovery, so no data to read
			int64(-1), 0, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within first data record, after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+400, storageCli)
		rec := preparedData.RecordPositions[firstDataRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+400,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, incomplete data record cannot be read
			int64(0), 0, werr.ErrEntryNotFound,
			// if finalized, incomplete entry is discarded during recovery, so no data to read
			int64(0), 0, werr.ErrFileReaderEndOfFile)
	})
}

// TestStagedFileWriter_CrashRecovery_SecondBlockHeaderRecord tests crash scenarios for second block header record
func TestStagedFileWriter_CrashRecovery_SecondBlockHeaderRecord(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-second-blkhdr-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1004, 2004

	t.Run("crash after second block header record, before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+100, storageCli)
		crashPos := preparedData.RecordPositions[secondBlockHeaderRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+100,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, first block's entry can be read
			int64(0), 1, werr.ErrEntryNotFound,
			// if finalized, using footer's LAC, first block's entry is available
			int64(0), 1, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash after second block header record, after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+200, storageCli)
		crashPos := preparedData.RecordPositions[secondBlockHeaderRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+200,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, first block's entry can be read
			int64(1), 1, werr.ErrEntryNotFound,
			// if finalized, using footer's LAC, first block's entry is available
			int64(1), 1, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within second block header record, before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+300, storageCli)
		rec := preparedData.RecordPositions[secondBlockHeaderRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+300,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, first block's entry can be read
			int64(0), 1, werr.ErrEntryNotFound,
			// if finalized, second block header incomplete, only first block data recovered
			int64(0), 1, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within second block header record, after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+400, storageCli)
		rec := preparedData.RecordPositions[secondBlockHeaderRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+400,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, first block's entry can be read
			int64(1), 1, werr.ErrEntryNotFound,
			// if finalized, second block header incomplete, only first block data recovered
			int64(1), 1, werr.ErrFileReaderEndOfFile)
	})
}

// TestStagedFileWriter_CrashRecovery_SecondDataRecord tests crash scenarios for second data record
func TestStagedFileWriter_CrashRecovery_SecondDataRecord(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-second-data-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1005, 2005

	t.Run("crash after second data record, before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+100, storageCli)
		crashPos := preparedData.RecordPositions[secondDataRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+100,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, only first entry can be read
			int64(0), 1, werr.ErrEntryNotFound,
			// if finalized, using footer's LAC, both entries (0 and 1) recovered
			int64(0), 2, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash after second data record, after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+200, storageCli)
		crashPos := preparedData.RecordPositions[secondDataRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+200,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, both entries can be read
			int64(1), 2, werr.ErrEntryNotFound,
			// if finalized, using footer's LAC, both entries (0 and 1) recovered
			int64(1), 2, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within second data record, before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+300, storageCli)
		rec := preparedData.RecordPositions[secondDataRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+300,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, only first entry can be read
			int64(0), 1, werr.ErrEntryNotFound,
			// if finalized, second data incomplete and discarded, only entry 0 recovered
			int64(0), 1, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within second data record, after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+400, storageCli)
		rec := preparedData.RecordPositions[secondDataRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+400,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, only first entry can be read
			int64(1), 1, werr.ErrEntryNotFound,
			// if finalized, second data incomplete and discarded, only entry 0 recovered
			int64(1), 1, werr.ErrFileReaderEndOfFile)
	})
}

// TestStagedFileWriter_CrashRecovery_FirstIndexRecord tests crash scenarios for first index record
func TestStagedFileWriter_CrashRecovery_FirstIndexRecord(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-first-index-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1006, 2006

	t.Run("crash after first index record, before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+100, storageCli)
		crashPos := preparedData.RecordPositions[firstIndexRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+100,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, only first entry can be read
			int64(0), 1, werr.ErrEntryNotFound,
			// if finalized, recovery uses full scan to find both entries
			int64(0), 2, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash after first index record, after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+200, storageCli)
		crashPos := preparedData.RecordPositions[firstIndexRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+200,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, both entries can be read
			int64(1), 2, werr.ErrEntryNotFound,
			// if finalized, recovery uses full scan to find both entries
			int64(1), 2, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within first index record, before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+300, storageCli)
		rec := preparedData.RecordPositions[firstIndexRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+300,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, only first entry can be read
			int64(0), 1, werr.ErrEntryNotFound,
			// if finalized, incomplete index discarded, recovery uses full scan to find both entries
			int64(0), 2, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within first index record, after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+400, storageCli)
		rec := preparedData.RecordPositions[firstIndexRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+400,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, both entries can be read
			int64(1), 2, werr.ErrEntryNotFound,
			// if finalized, incomplete index discarded, recovery uses full scan to find both entries
			int64(1), 2, werr.ErrFileReaderEndOfFile)
	})
}

// TestStagedFileWriter_CrashRecovery_SecondIndexRecord tests crash scenarios for second index record
func TestStagedFileWriter_CrashRecovery_SecondIndexRecord(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-second-index-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1007, 2007

	t.Run("crash after second index record, before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+100, storageCli)
		crashPos := preparedData.RecordPositions[secondIndexRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+100,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, only first entry can be read
			int64(0), 1, werr.ErrEntryNotFound,
			// if finalized, all index records present, recovery uses full scan to find both entries
			int64(0), 2, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash after second index record, after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+200, storageCli)
		crashPos := preparedData.RecordPositions[secondIndexRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+200,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, both entries can be read
			int64(1), 2, werr.ErrEntryNotFound,
			// if finalized, all index records present, recovery uses full scan to find both entries
			int64(1), 2, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within second index record, before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+300, storageCli)
		rec := preparedData.RecordPositions[secondIndexRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+300,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, only first entry can be read
			int64(0), 1, werr.ErrEntryNotFound,
			// if finalized, second index incomplete, recovery uses full scan to find both entries
			int64(0), 2, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within second index record, after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+400, storageCli)
		rec := preparedData.RecordPositions[secondIndexRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+400,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, both entries can be read
			int64(1), 2, werr.ErrEntryNotFound,
			// if finalized, second index incomplete, recovery uses full scan to find both entries
			int64(1), 2, werr.ErrFileReaderEndOfFile)
	})
}

// TestStagedFileWriter_CrashRecovery_FooterRecord tests crash scenarios for footer record
func TestStagedFileWriter_CrashRecovery_FooterRecord(t *testing.T) {
	rootPath := fmt.Sprintf("test-crash-footer-%d", time.Now().Unix())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	const logId, segmentId = 1008, 2008

	t.Run("crash after footer record (complete file), before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+100, storageCli)
		crashPos := preparedData.RecordPositions[footerRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+100,
			storageCli, preparedData.FilePath, crashPos,
			// file is finalized, reader uses footer's LAC, both entries can be read
			int64(0), 2, werr.ErrFileReaderEndOfFile,
			// file already finalized and complete, both entries still available after recovery
			int64(0), 2, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash after footer record (complete file), after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+200, storageCli)
		crashPos := preparedData.RecordPositions[footerRecordIndex].EndPos
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+200,
			storageCli, preparedData.FilePath, crashPos,
			// file is finalized, reader uses footer's LAC, both entries can be read
			int64(1), 2, werr.ErrFileReaderEndOfFile,
			// file already finalized and complete, both entries still available after recovery
			int64(1), 2, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within footer record, before lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+300, storageCli)
		rec := preparedData.RecordPositions[footerRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+300,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, only first entry can be read
			int64(0), 1, werr.ErrEntryNotFound,
			// if finalized, incomplete footer, recovery uses full scan to find both entries
			int64(0), 2, werr.ErrFileReaderEndOfFile)
	})

	t.Run("crash within footer record, after lac", func(t *testing.T) {
		preparedData := prepareCompleteTestFile(t, ctx, cfg, tempDir, logId, segmentId+400, storageCli)
		rec := preparedData.RecordPositions[footerRecordIndex]
		crashPos := rec.StartPos + (rec.EndPos-rec.StartPos)/2
		testCrashRecoveryAtPosition(t, ctx, cfg, tempDir, logId, segmentId+400,
			storageCli, preparedData.FilePath, crashPos,
			// if not finalized, reader's LAC will be active, both entries can be read
			int64(1), 2, werr.ErrEntryNotFound,
			// if finalized, incomplete footer, recovery uses full scan to find both entries
			int64(1), 2, werr.ErrFileReaderEndOfFile)
	})
}

// testCrashRecoveryAtPosition tests the four-step crash recovery process
func testCrashRecoveryAtPosition(t *testing.T, ctx context.Context, cfg *config.Configuration,
	tempDir string, logId, segmentId int64, storageCli objectstorage.ObjectStorage,
	sourcePath string, truncatePosition int64,
	preReaderLAC int64,
	expectPreReadEntries int, expectPreReadError error,
	postReaderLAC int64,
	expectPostReadEntries int, expectPostReadError error) {

	// Step1: prepare crash data
	// Prepare target path for this test
	targetDir := filepath.Join(tempDir, fmt.Sprintf("%d/%d", logId, segmentId))
	err := os.MkdirAll(targetDir, 0755)
	require.NoError(t, err, "Failed to create target directory %s for log=%d segment=%d", targetDir, logId, segmentId)

	targetPath := filepath.Join(targetDir, "data.log")

	// Copy source file to target
	sourceData, err := os.ReadFile(sourcePath)
	require.NoError(t, err, "Failed to read source file %s for log=%d segment=%d", sourcePath, logId, segmentId)
	err = os.WriteFile(targetPath, sourceData, 0644)
	require.NoError(t, err, "Failed to write target file %s for log=%d segment=%d", targetPath, logId, segmentId)

	// Simulate crash by truncating
	if truncatePosition >= 0 {
		err = os.Truncate(targetPath, truncatePosition)
		require.NoError(t, err,
			"Failed to truncate file %s to position %d for log=%d segment=%d (file size was %d)",
			targetPath, truncatePosition, logId, segmentId, len(sourceData))
		t.Logf("Simulated crash: truncated to position %d (original size: %d)", truncatePosition, len(sourceData))
	}

	// Step 2: Pre-Recovery Read Verification
	t.Log("Step 2: Pre-Recovery Read - attempting to read from crashed file")
	preReader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath,
		tempDir, logId, segmentId, storageCli, cfg)
	require.NoError(t, err, "Failed to create pre-recovery reader for log=%d segment=%d at truncate position %d",
		logId, segmentId, truncatePosition)
	_ = preReader.UpdateLastAddConfirmed(ctx, preReaderLAC)

	// preRecovery first read
	preReadResult, preReadErr := preReader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}, nil)
	if expectPreReadEntries == 0 {
		require.True(t, errors.Is(preReadErr, expectPreReadError),
			"Pre-recovery first read error mismatch: expected error type %v, got %v (truncate at %d, LAC=%d)",
			expectPreReadError, preReadErr, truncatePosition, preReaderLAC)
	} else {
		require.NoError(t, preReadErr,
			"Pre-recovery first read failed unexpectedly (truncate at %d, LAC=%d, expected %d entries)",
			truncatePosition, preReaderLAC, expectPreReadEntries)
		actualEntries := len(preReadResult.Entries)
		require.Equal(t, expectPreReadEntries, actualEntries,
			"Pre-recovery read entries count mismatch at truncate position %d with LAC=%d: expected %d entries, got %d",
			truncatePosition, preReaderLAC, expectPreReadEntries, actualEntries)

		// preRecovery last read
		preReadResult2, preReadErr2 := preReader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    preReadResult.Entries[len(preReadResult.Entries)-1].EntryId + 1,
			MaxBatchEntries: 10,
		}, preReadResult.LastReadState)
		require.Error(t, preReadErr2,
			"Pre-recovery last read should fail after reading %d entries (truncate at %d, LAC=%d)",
			expectPreReadEntries, truncatePosition, preReaderLAC)
		require.Nil(t, preReadResult2,
			"Pre-recovery last read result should be nil (truncate at %d, LAC=%d)",
			truncatePosition, preReaderLAC)
		require.True(t, errors.Is(preReadErr2, expectPreReadError),
			"Pre-recovery last read error mismatch: expected error type %v, got %v (truncate at %d, LAC=%d)",
			expectPreReadError, preReadErr2, truncatePosition, preReaderLAC)
	}
	_ = preReader.Close(ctx)

	// Step 3: Recovery and Finalization
	t.Log("Step 3: Recovery - starting writer in recovery mode")
	recoveryWriter, err := stagedstorage.NewStagedFileWriterWithMode(ctx, StagedTestBucket,
		cfg.Minio.RootPath, tempDir, logId, segmentId, storageCli, cfg, true)
	require.NoError(t, err,
		"Recovery failed at truncate position %d for log=%d segment=%d (expected recovery to always succeed)",
		truncatePosition, logId, segmentId)

	// Get recovered last entry
	lastEntryId := recoveryWriter.GetLastEntryId(ctx)
	t.Logf("Recovered lastEntryId: %d", lastEntryId)

	// Finalize with recovered last entry
	finalizedId, finalizeErr := recoveryWriter.Finalize(ctx, lastEntryId)
	require.NoError(t, finalizeErr,
		"Finalize failed after recovery at truncate position %d with recovered lastEntryId=%d (log=%d segment=%d)",
		truncatePosition, lastEntryId, logId, segmentId)
	require.Equal(t, lastEntryId, finalizedId,
		"Finalize returned unexpected lastEntryId at truncate position %d: expected=%d, got=%d (log=%d segment=%d)",
		truncatePosition, lastEntryId, finalizedId, logId, segmentId)
	recoveryWriter.Close(ctx)

	// Step 4: Post-Recovery Read Verification
	t.Log("Step 4: Post-Recovery Read - attempting to read after recovery")
	postReader, err := stagedstorage.NewStagedFileReaderAdv(ctx, StagedTestBucket, cfg.Minio.RootPath,
		tempDir, logId, segmentId, storageCli, cfg)
	require.NoError(t, err,
		"Failed to create post-recovery reader for log=%d segment=%d after recovery (truncate was at %d, recovered lastEntryId=%d)",
		logId, segmentId, truncatePosition, lastEntryId)
	_ = postReader.UpdateLastAddConfirmed(ctx, postReaderLAC)

	// postRecovery first read
	postReadResult, postReadErr := postReader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}, nil)
	if expectPostReadEntries == 0 {
		require.True(t, errors.Is(postReadErr, expectPostReadError),
			"Post-recovery first read error mismatch: expected error type %v, got %v (truncate at %d, recovered lastEntryId=%d, LAC=%d)",
			expectPostReadError, postReadErr, truncatePosition, lastEntryId, postReaderLAC)
	} else {
		require.NoError(t, postReadErr,
			"Post-recovery first read failed unexpectedly (truncate at %d, recovered lastEntryId=%d, LAC=%d, expected %d entries)",
			truncatePosition, lastEntryId, postReaderLAC, expectPostReadEntries)
		actualEntries := len(postReadResult.Entries)
		require.Equal(t, expectPostReadEntries, actualEntries,
			"Post-recovery read entries count mismatch at truncate position %d with LAC=%d: expected %d entries, got %d (recovered lastEntryId=%d)",
			truncatePosition, postReaderLAC, expectPostReadEntries, actualEntries, lastEntryId)

		// postRecovery last read
		postReadResult2, postReadErr2 := postReader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
			StartEntryID:    postReadResult.Entries[len(postReadResult.Entries)-1].EntryId + 1,
			MaxBatchEntries: 10,
		}, postReadResult.LastReadState)
		require.Error(t, postReadErr2,
			"Post-recovery last read should fail after reading %d entries (truncate at %d, recovered lastEntryId=%d, LAC=%d)",
			expectPostReadEntries, truncatePosition, lastEntryId, postReaderLAC)
		require.Nil(t, postReadResult2,
			"Post-recovery last read result should be nil (truncate at %d, recovered lastEntryId=%d, LAC=%d)",
			truncatePosition, lastEntryId, postReaderLAC)
		require.True(t, errors.Is(postReadErr2, expectPostReadError),
			"Post-recovery last read error mismatch: expected error type %v, got %v (truncate at %d, recovered lastEntryId=%d, LAC=%d)",
			expectPostReadError, postReadErr2, truncatePosition, lastEntryId, postReaderLAC)
	}
	_ = postReader.Close(ctx)
}

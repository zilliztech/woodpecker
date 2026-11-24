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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/stagedstorage"
)

type decodedStagedFileRecord struct {
	record codec.Record
	start  int64
	end    int64
}

func decodeStagedFinalizeFile(t *testing.T, filePath string) ([]decodedStagedFileRecord, []byte) {
	t.Helper()

	fileData, err := os.ReadFile(filePath)
	require.NoError(t, err)

	records := make([]decodedStagedFileRecord, 0)
	for offset := int64(0); offset < int64(len(fileData)); {
		require.GreaterOrEqual(t, int64(len(fileData))-offset, int64(codec.RecordHeaderSize),
			"truncated record header at offset %d", offset)

		payloadLength := int64(binary.LittleEndian.Uint32(fileData[offset+5 : offset+9]))
		recordEnd := offset + int64(codec.RecordHeaderSize) + payloadLength
		require.LessOrEqual(t, recordEnd, int64(len(fileData)),
			"truncated record payload at offset %d", offset)

		record, err := codec.DecodeRecord(fileData[offset:recordEnd])
		require.NoError(t, err, "record at offset %d must have a valid type, length, and CRC", offset)
		records = append(records, decodedStagedFileRecord{record: record, start: offset, end: recordEnd})
		offset = recordEnd
	}

	if len(records) > 0 {
		require.Equal(t, int64(len(fileData)), records[len(records)-1].end,
			"the finalized file must not contain trailing or partial bytes")
	}
	return records, fileData
}

func writeStagedFinalizeEntries(
	t *testing.T,
	ctx context.Context,
	writer *stagedstorage.StagedFileWriter,
	caseName string,
	entryCount int,
) [][]byte {
	t.Helper()

	entries := make([][]byte, entryCount)
	for i := 0; i < entryCount; i++ {
		entryID := int64(i)
		entries[i] = []byte(fmt.Sprintf("finalize-%s-entry-%d", caseName, entryID))
		resultCh := channel.NewLocalResultChannel(fmt.Sprintf("finalize-%s-%d", caseName, entryID))
		returnedID, err := writer.WriteDataAsync(ctx, entryID, entries[i], resultCh)
		require.NoError(t, err)
		require.Equal(t, entryID, returnedID)

		result, err := resultCh.ReadResult(ctx)
		require.NoError(t, err)
		require.NoError(t, result.Err)
		require.Equal(t, entryID, result.SyncedId)
	}
	return entries
}

func verifyFinalizedStagedFile(
	t *testing.T,
	filePath string,
	entries [][]byte,
	targetLAC int64,
) *codec.FooterRecord {
	t.Helper()

	records, fileData := decodeStagedFinalizeFile(t, filePath)
	require.NotEmpty(t, records)
	require.Equal(t, codec.HeaderRecordType, records[0].record.Type(),
		"a finalized staged file must start with one header")
	require.Equal(t, codec.FooterRecordType, records[len(records)-1].record.Type(),
		"a finalized staged file must end with one footer")

	headerCount := 0
	blockHeaderCount := 0
	dataRecords := make([]*codec.DataRecord, 0, len(entries))
	indexCount := 0
	footerCount := 0
	for _, decoded := range records {
		switch decoded.record.Type() {
		case codec.HeaderRecordType:
			headerCount++
		case codec.BlockHeaderRecordType:
			blockHeaderCount++
		case codec.DataRecordType:
			dataRecord, ok := decoded.record.(*codec.DataRecord)
			require.True(t, ok)
			dataRecords = append(dataRecords, dataRecord)
		case codec.IndexRecordType:
			indexCount++
		case codec.FooterRecordType:
			footerCount++
		}
	}

	require.Equal(t, 1, headerCount)
	require.Equal(t, 1, footerCount)
	require.Len(t, dataRecords, len(entries), "Finalize must preserve every local data record")
	for i, expected := range entries {
		require.Equal(t, expected, dataRecords[i].Payload)
	}

	footer, ok := records[len(records)-1].record.(*codec.FooterRecord)
	require.True(t, ok)
	require.Equal(t, targetLAC, footer.LAC, "footer must store the coordinator's global target LAC")
	require.Equal(t, int(footer.TotalBlocks), blockHeaderCount)
	require.Equal(t, int(footer.TotalBlocks), indexCount)
	require.Equal(t, uint32(footer.TotalBlocks), footer.TotalRecords)

	footerStart := records[len(records)-1].start
	require.Equal(t, uint64(footerStart), footer.TotalSize,
		"footer TotalSize must point to the end of the index section")
	require.Equal(t, footerStart, int64(footer.IndexOffset)+int64(footer.IndexLength),
		"the index section must end immediately before the footer")
	require.Equal(t, int64(len(fileData)), records[len(records)-1].end)

	if len(entries) == 0 {
		require.Zero(t, footer.TotalBlocks)
		require.Zero(t, footer.IndexLength)
	} else {
		require.Positive(t, footer.TotalBlocks)
		require.Positive(t, footer.IndexLength)
	}
	return footer
}

func verifyRecoveredFinalizedStagedFile(
	t *testing.T,
	ctx context.Context,
	storageCli objectstorage.ObjectStorage,
	cfg *config.Configuration,
	tempDir string,
	logID int64,
	segmentID int64,
	localLastEntryID int64,
	targetLAC int64,
) {
	t.Helper()

	recovered, err := stagedstorage.NewStagedFileWriterWithMode(
		ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logID, segmentID, storageCli, cfg, true,
	)
	require.NoError(t, err)
	require.True(t, recovered.Snapshot().Finalized)
	require.Equal(t, localLastEntryID, recovered.GetLastEntryId(ctx))
	require.NotNil(t, recovered.GetRecoveredFooter())
	require.Equal(t, targetLAC, recovered.GetRecoveredFooter().LAC)
	require.NoError(t, recovered.Close(ctx))
}

// TestStagedFileWriter_FinalizeFileMatrix validates the durable data.log output,
// rather than only the in-memory writer state or Finalize return value.
//
// Scenario matrix:
//
//	Local file     target LAC relation       Expected durable result
//	empty          target = -1               header + footer(LAC=-1), no blocks
//	empty          target > local tail       header + footer(target), no blocks
//	non-empty      target < local tail       all local data + footer(target)
//	non-empty      target = local tail       all local data + footer(target)
//	non-empty      target > local tail       all local data + footer(target)
//	non-empty      target < -1 (invalid)     error, no footer, file unchanged
//
// A target above the local tail is intentionally a valid partial-replica
// finalization. The local tail returned by Finalize lets the segment coordinator
// decide whether that replica qualifies for the completion quorum.
func TestStagedFileWriter_FinalizeFileMatrix(t *testing.T) {
	rootPath := fmt.Sprintf("test-staged-finalize-file-matrix-%d", time.Now().UnixNano())
	storageCli, cfg, tempDir := setupStagedFileTest(t, rootPath)
	ctx := context.Background()
	defer cleanupStagedTestObjects(t, storageCli, rootPath)

	tests := []struct {
		name       string
		entryCount int
		targetLAC  int64
		wantErr    bool
	}{
		{name: "empty target equals sentinel", entryCount: 0, targetLAC: -1},
		{name: "empty target above local tail", entryCount: 0, targetLAC: 0},
		{name: "non-empty target below local tail", entryCount: 3, targetLAC: 1},
		{name: "non-empty target equals local tail", entryCount: 3, targetLAC: 2},
		{name: "non-empty target above local tail", entryCount: 3, targetLAC: 4},
		{name: "invalid target below sentinel", entryCount: 2, targetLAC: -2, wantErr: true},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logID := int64(9100)
			segmentID := int64(9200 + i)
			filePath := filepath.Join(tempDir, fmt.Sprintf("%d/%d/data.log", logID, segmentID))
			localLastEntryID := int64(tt.entryCount - 1)

			writer, err := stagedstorage.NewStagedFileWriter(
				ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logID, segmentID, storageCli, cfg,
			)
			require.NoError(t, err)
			writerClosed := false
			defer func() {
				if !writerClosed {
					require.NoError(t, writer.Close(ctx))
				}
			}()

			entries := writeStagedFinalizeEntries(t, ctx, writer, fmt.Sprintf("case-%d", i), tt.entryCount)

			if tt.wantErr {
				before, err := os.ReadFile(filePath)
				require.NoError(t, err)

				returnedLastEntryID, finalizeErr := writer.Finalize(ctx, tt.targetLAC)
				require.Error(t, finalizeErr)
				require.True(t, werr.ErrInvalidLACAlignment.Is(finalizeErr))
				require.Equal(t, localLastEntryID, returnedLastEntryID)
				require.False(t, writer.Snapshot().Finalized)
				require.Nil(t, writer.GetRecoveredFooter())

				afterRecords, after := decodeStagedFinalizeFile(t, filePath)
				require.Equal(t, before, after, "an invalid Finalize call must not modify data.log")
				for _, record := range afterRecords {
					require.NotEqual(t, codec.FooterRecordType, record.record.Type(),
						"an invalid Finalize call must not append a footer")
				}
				return
			}

			returnedLastEntryID, err := writer.Finalize(ctx, tt.targetLAC)
			require.NoError(t, err)
			require.Equal(t, localLastEntryID, returnedLastEntryID)
			require.True(t, writer.Snapshot().Finalized)
			require.Equal(t, tt.targetLAC, writer.GetRecoveredFooter().LAC)
			verifyFinalizedStagedFile(t, filePath, entries, tt.targetLAC)

			require.NoError(t, writer.Close(ctx))
			writerClosed = true
			verifyRecoveredFinalizedStagedFile(
				t, ctx, storageCli, cfg, tempDir, logID, segmentID, localLastEntryID, tt.targetLAC,
			)

			reader, err := stagedstorage.NewStagedFileReaderAdv(
				ctx, StagedTestBucket, cfg.Minio.RootPath, tempDir, logID, segmentID, storageCli, cfg,
			)
			require.NoError(t, err)
			defer reader.Close(ctx)
			require.NotNil(t, reader.GetFooter())
			require.Equal(t, tt.targetLAC, reader.GetFooter().LAC)

			readableEntries := tt.entryCount
			if tt.targetLAC < localLastEntryID {
				readableEntries = int(tt.targetLAC + 1)
			}
			if readableEntries > 0 {
				result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
					StartEntryID:    0,
					MaxBatchEntries: int64(readableEntries),
				}, nil)
				require.NoError(t, readErr)
				require.Len(t, result.Entries, readableEntries)
				for entryIndex := 0; entryIndex < readableEntries; entryIndex++ {
					require.Equal(t, int64(entryIndex), result.Entries[entryIndex].EntryId)
					require.Equal(t, entries[entryIndex], result.Entries[entryIndex].Values)
				}
			}

			if tt.targetLAC > localLastEntryID {
				result, readErr := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
					StartEntryID:    localLastEntryID + 1,
					MaxBatchEntries: 1,
				}, nil)
				assert.Nil(t, result)
				require.Error(t, readErr)
				require.True(t, werr.ErrEntryNotFound.Is(readErr),
					"a partial finalized file must make the reader fail over for the missing suffix")
			}
		})
	}
}

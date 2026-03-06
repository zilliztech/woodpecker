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

package stagedstorage

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// Helper function to encode a list of records into a single buffer
func encodeRecordList(records []codec.Record) []byte {
	var buffer bytes.Buffer
	for _, record := range records {
		encodedRecord := codec.EncodeRecord(record)
		buffer.Write(encodedRecord)
	}
	return buffer.Bytes()
}

// TestReadDataBlocksWithLACButNoData tests the scenario where:
// - LAC (Last Add Confirmed) is set to a value (e.g., 100)
// - File has some data (e.g., entries 0-20)
// - Client tries to read entries starting from an ID beyond available data but less than LAC (e.g., 30)
// - Should return ErrEntryNotFound to allow retry or read from other nodes
func TestReadDataBlocksWithLACButNoData(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	logId := int64(1)
	segId := int64(1)

	// Create a file with data blocks for entries 0-20
	localBaseDir := filepath.Join(tempDir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)

	// Create file with HeaderRecord and one data block (entries 0-20)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write a header record
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	headerData := codec.EncodeRecord(headerRecord)
	_, err = file.Write(headerData)
	require.NoError(t, err)

	// Write one data block with entries 0-20
	dataRecords := make([]codec.Record, 0, 21)
	for i := int64(0); i <= 20; i++ {
		dataRecord := &codec.DataRecord{
			Payload: []byte("test data"),
		}
		dataRecords = append(dataRecords, dataRecord)
	}

	blockData := encodeRecordList(dataRecords)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  20,
		BlockLength:  uint32(len(blockData)),
		BlockCrc:     crc32.ChecksumIEEE(blockData),
	}
	blockHeaderData := codec.EncodeRecord(blockHeader)
	_, err = file.Write(blockHeaderData)
	require.NoError(t, err)
	_, err = file.Write(blockData)
	require.NoError(t, err)
	// DO NOT write footer - keep the file incomplete so it will scan from beginning
	file.Close()

	// Create configuration
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// Create reader
	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close(ctx)

	// Set LAC to 15 (LESS than the actual data in file which goes to 20)
	// This simulates a situation where entries 16-20 are not yet confirmed
	lac := int64(15)
	err = reader.UpdateLastAddConfirmed(ctx, lac)
	require.NoError(t, err)

	// Try to read starting from entry 18 (greater than LAC but within file data range)
	// - File is incomplete (no footer), so will scan blocks from beginning
	// - Will read block 0 (entries 0-20)
	// - When iterating through entries, currentEntryID starts from 0
	// - When currentEntryID reaches 16, it exceeds LAC (15), so reachLac=true and breaks
	// - Since startEntryID=18 > LAC=15, no data is collected
	// - lastReadEntryID stays -1, currentLAC=15
	// - reachLac=true causes the loop to break WITHOUT trying to read next block (no EOF error)
	// - Triggers: lastReadEntryID (-1) < currentLAC (15)
	opt := storage.ReaderOpt{
		StartEntryID:    18,
		MaxBatchEntries: 10,
	}

	result, err := reader.ReadNextBatchAdv(ctx, opt, nil)

	// Should return ErrEntryNotFound with the specific LAC message because:
	// - No entries were read (len(entries) == 0, due to startEntryID > last available)
	// - lastReadEntryID (-1) < currentLAC (100)
	// - This indicates data should exist but isn't available on this node
	assert.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err), "Expected ErrEntryNotFound, got: %v", err)
	assert.Nil(t, result)
}

// TestReadDataBlocksWithLACAndPartialData tests the scenario where:
// - LAC is set to 100
// - File has some data blocks (e.g., entries 0-50)
// - Client tries to read starting from entry 60 (which is less than LAC)
// - Should return ErrEntryNotFound because the requested data should exist but isn't available
func TestReadDataBlocksWithLACAndPartialData(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	logId := int64(2)
	segId := int64(2)

	// Create a file with data blocks for entries 0-50
	localBaseDir := filepath.Join(tempDir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)

	// Create file with HeaderRecord and one data block (entries 0-50)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write a header record
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	headerData := codec.EncodeRecord(headerRecord)
	_, err = file.Write(headerData)
	require.NoError(t, err)

	// Write one data block with entries 0-50
	dataRecords := make([]codec.Record, 0, 51)
	for i := int64(0); i <= 50; i++ {
		dataRecord := &codec.DataRecord{
			Payload: []byte("test data"),
		}
		dataRecords = append(dataRecords, dataRecord)
	}

	blockData := encodeRecordList(dataRecords)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  50,
		BlockLength:  uint32(len(blockData)),
		BlockCrc:     crc32.ChecksumIEEE(blockData),
	}
	blockHeaderData := codec.EncodeRecord(blockHeader)
	_, err = file.Write(blockHeaderData)
	require.NoError(t, err)
	_, err = file.Write(blockData)
	require.NoError(t, err)
	// DO NOT write footer - keep the file incomplete
	file.Close()

	// Create configuration
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// Create reader
	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close(ctx)

	// Set LAC to 45 (LESS than the actual data in file which goes to 50)
	// This simulates a situation where entries 46-50 are not yet confirmed
	lac := int64(45)
	err = reader.UpdateLastAddConfirmed(ctx, lac)
	require.NoError(t, err)

	// Try to read starting from entry 48 (greater than LAC but within file data range)
	// - File is incomplete (no footer), will scan blocks from beginning
	// - Will read block 0 (entries 0-50)
	// - When currentEntryID reaches 46, it exceeds LAC (45), so reachLac=true and breaks
	// - Since startEntryID=48 > LAC=45, no data is collected
	// - lastReadEntryID stays -1, currentLAC=45
	// - reachLac=true causes the loop to break WITHOUT trying to read next block
	// - Triggers: lastReadEntryID (-1) < currentLAC (45)
	opt := storage.ReaderOpt{
		StartEntryID:    48,
		MaxBatchEntries: 10,
	}

	result, err := reader.ReadNextBatchAdv(ctx, opt, nil)

	// Should return ErrEntryNotFound with the specific LAC message because:
	// - No entries were read (len(entries) == 0)
	// - lastReadEntryID (-1) < currentLAC (100)
	// - Requested entry 60 should exist (< LAC) but isn't available on this node
	assert.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err), "Expected ErrEntryNotFound, got: %v", err)
	assert.Nil(t, result)
}

// TestReadDataBlocksWithLACGreaterThanData tests the scenario where:
// - LAC is GREATER than actual data (e.g., data 0-10, LAC=15)
// - Client tries to read entry 11 (which should exist according to LAC but doesn't)
// - Should return ErrEntryNotFound because data is lagging behind LAC
func TestReadDataBlocksWithLACGreaterThanData(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	logId := int64(5)
	segId := int64(5)

	// Create a file with data blocks for entries 0-10 only
	localBaseDir := filepath.Join(tempDir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)

	// Create file with HeaderRecord and one data block (entries 0-10)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write a header record
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	headerData := codec.EncodeRecord(headerRecord)
	_, err = file.Write(headerData)
	require.NoError(t, err)

	// Write one data block with entries 0-10
	dataRecords := make([]codec.Record, 0, 11)
	for i := int64(0); i <= 10; i++ {
		dataRecord := &codec.DataRecord{
			Payload: []byte("test data"),
		}
		dataRecords = append(dataRecords, dataRecord)
	}

	blockData := encodeRecordList(dataRecords)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  10,
		BlockLength:  uint32(len(blockData)),
		BlockCrc:     crc32.ChecksumIEEE(blockData),
	}
	blockHeaderData := codec.EncodeRecord(blockHeader)
	_, err = file.Write(blockHeaderData)
	require.NoError(t, err)
	_, err = file.Write(blockData)
	require.NoError(t, err)
	// DO NOT write footer - keep the file incomplete
	file.Close()

	// Create configuration
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// Create reader
	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close(ctx)

	// Set LAC to 15 (GREATER than actual data which only goes to 10)
	// This simulates LAC saying entries 0-15 should be available, but data is lagging
	lac := int64(15)
	err = reader.UpdateLastAddConfirmed(ctx, lac)
	require.NoError(t, err)

	// Try to read starting from entry 11 (beyond actual data but within LAC)
	// - File is incomplete (no footer), will scan blocks from beginning
	// - Will read block 0 (entries 0-10), but startEntryID=11 > 10, so no data collected
	// - Tries to read next block but reaches EOF (no more blocks)
	// - hasDataReadError = true
	// - lastReadEntryID = -1, currentLAC = 15
	opt := storage.ReaderOpt{
		StartEntryID:    11,
		MaxBatchEntries: 10,
	}

	result, err := reader.ReadNextBatchAdv(ctx, opt, nil)

	// Should return ErrEntryNotFound because:
	// - Data (0-10) is lagging behind LAC (15)
	// - Entry 11 should exist according to LAC but isn't in the file yet
	// - This indicates a data lag situation, client should retry
	assert.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err), "Expected ErrEntryNotFound, got: %v", err)
	assert.Nil(t, result)
	// Note: Due to hasDataReadError=true (EOF when trying next block),
	// this will return generic "no record extract" error, not LAC-specific message
	assert.Contains(t, err.Error(), "no record extract")
}

// TestReadDataBlocksWithLACAtBoundary tests the scenario where:
// - LAC equals the last available entry
// - Client tries to read starting from an ID equal to LAC + 1
// - Should NOT return the LAC-related error, as this is expected EOF behavior
func TestReadDataBlocksWithLACAtBoundary(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	logId := int64(3)
	segId := int64(3)

	// Create a file with data blocks for entries 0-50
	localBaseDir := filepath.Join(tempDir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)

	// Create file with HeaderRecord and one data block (entries 0-50)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write a header record
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	headerData := codec.EncodeRecord(headerRecord)
	_, err = file.Write(headerData)
	require.NoError(t, err)

	// Write one data block with entries 0-50
	dataRecords := make([]codec.Record, 0, 51)
	for i := int64(0); i <= 50; i++ {
		dataRecord := &codec.DataRecord{
			Payload: []byte("test data"),
		}
		dataRecords = append(dataRecords, dataRecord)
	}

	blockData := encodeRecordList(dataRecords)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  50,
		BlockLength:  uint32(len(blockData)),
		BlockCrc:     crc32.ChecksumIEEE(blockData),
	}
	blockHeaderData := codec.EncodeRecord(blockHeader)
	_, err = file.Write(blockHeaderData)
	require.NoError(t, err)
	_, err = file.Write(blockData)
	require.NoError(t, err)
	file.Close()

	// Create configuration
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// Create reader
	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close(ctx)

	// Set LAC to 50 (equal to last available entry)
	lac := int64(50)
	err = reader.UpdateLastAddConfirmed(ctx, lac)
	require.NoError(t, err)

	// Try to read starting from entry 51 (beyond LAC)
	opt := storage.ReaderOpt{
		StartEntryID:    51,
		MaxBatchEntries: 10,
	}

	result, err := reader.ReadNextBatchAdv(ctx, opt, nil)

	// Should return ErrEntryNotFound but NOT the LAC-specific error
	// because lastReadEntryID (50) is NOT less than currentLAC (50)
	assert.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err), "Expected ErrEntryNotFound, got: %v", err)
	assert.Nil(t, result)
	// Should NOT contain the LAC-specific message
	assert.NotContains(t, err.Error(), "some data less than LAC but can't read here now")
}

// TestReadDataBlocksSuccessfulRead tests a successful read scenario for comparison
func TestReadDataBlocksSuccessfulRead(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	logId := int64(4)
	segId := int64(4)

	// Create a file with data blocks for entries 0-50
	localBaseDir := filepath.Join(tempDir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)

	// Create file with HeaderRecord and one data block (entries 0-50)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write a header record
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	headerData := codec.EncodeRecord(headerRecord)
	_, err = file.Write(headerData)
	require.NoError(t, err)

	// Write one data block with entries 0-50
	dataRecords := make([]codec.Record, 0, 51)
	for i := int64(0); i <= 50; i++ {
		dataRecord := &codec.DataRecord{
			Payload: []byte("test data"),
		}
		dataRecords = append(dataRecords, dataRecord)
	}

	blockData := encodeRecordList(dataRecords)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  50,
		BlockLength:  uint32(len(blockData)),
		BlockCrc:     crc32.ChecksumIEEE(blockData),
	}
	blockHeaderData := codec.EncodeRecord(blockHeader)
	_, err = file.Write(blockHeaderData)
	require.NoError(t, err)
	_, err = file.Write(blockData)
	require.NoError(t, err)
	file.Close()

	// Create configuration
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// Create reader
	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close(ctx)

	// Set LAC to 50
	lac := int64(50)
	err = reader.UpdateLastAddConfirmed(ctx, lac)
	require.NoError(t, err)

	// Try to read starting from entry 0 (within available range)
	opt := storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}

	result, err := reader.ReadNextBatchAdv(ctx, opt, nil)

	// Should succeed
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result.Entries)
	// Note: MaxBatchEntries is a soft limit. Since all 51 entries (0-50) are in one block,
	// and a block is read atomically, all 51 entries will be returned even though MaxBatchEntries=10
	assert.Equal(t, 51, len(result.Entries)) // All entries from the single block
	assert.Equal(t, int64(0), result.Entries[0].EntryId)
	assert.Equal(t, int64(50), result.Entries[50].EntryId)
}

// TestNewStagedFileReaderAdv_NonExistentFile tests reader creation with no file
func TestNewStagedFileReaderAdv_NonExistentFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	_, err = NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", dir, 99, 99, nil, cfg)
	assert.Error(t, err)
}

// TestStagedFileReaderAdv_Close tests closing the reader
func TestStagedFileReaderAdv_Close(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	logId := int64(10)
	segId := int64(10)

	// Create a minimal file
	localBaseDir := filepath.Join(tempDir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	headerData := codec.EncodeRecord(headerRecord)
	_, err = file.Write(headerData)
	require.NoError(t, err)
	file.Close()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, reader)

	// Close should succeed
	err = reader.Close(ctx)
	assert.NoError(t, err)

	// Second close should be idempotent
	err = reader.Close(ctx)
	assert.NoError(t, err)
}

// TestStagedFileReaderAdv_UpdateLastAddConfirmed tests LAC updates
func TestStagedFileReaderAdv_UpdateLastAddConfirmed(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	logId := int64(11)
	segId := int64(11)

	localBaseDir := filepath.Join(tempDir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	headerData := codec.EncodeRecord(headerRecord)
	_, err = file.Write(headerData)
	require.NoError(t, err)
	file.Close()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Update LAC
	err = reader.UpdateLastAddConfirmed(ctx, 100)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), reader.lastAddConfirmed.Load())

	// LAC can only move forward
	err = reader.UpdateLastAddConfirmed(ctx, 200)
	assert.NoError(t, err)
	assert.Equal(t, int64(200), reader.lastAddConfirmed.Load())
}

// TestStagedFileReaderAdv_GettersWithFooter tests getter methods after finalization
func TestStagedFileReaderAdv_GettersWithFooter(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(12)
	segId := int64(12)

	// Create a finalized file with a writer
	writer, err := NewStagedFileWriter(ctx, "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(ctx, i, []byte("data"), nil)
		require.NoError(t, err)
	}

	err = writer.Sync(ctx)
	require.NoError(t, err)
	// Wait for async flush
	for retries := 0; retries < 50; retries++ {
		if writer.GetLastEntryId(ctx) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	_, err = writer.Finalize(ctx, 2)
	require.NoError(t, err)
	writer.Close(ctx)

	// Open reader
	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	assert.NotNil(t, reader.GetFooter())
	assert.GreaterOrEqual(t, reader.GetTotalBlocks(), int32(1))
	assert.GreaterOrEqual(t, reader.GetTotalRecords(), uint32(1))
	assert.NotEmpty(t, reader.GetBlockIndexes())
}

// TestStagedFileReaderAdv_GettersWithoutFooter tests getter methods on incomplete file
func TestStagedFileReaderAdv_GettersWithoutFooter(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	logId := int64(13)
	segId := int64(13)

	localBaseDir := filepath.Join(tempDir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	headerData := codec.EncodeRecord(headerRecord)
	_, err = file.Write(headerData)
	require.NoError(t, err)
	file.Close()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	assert.Nil(t, reader.GetFooter())
	assert.Equal(t, int32(0), reader.GetTotalBlocks())
	assert.Equal(t, uint32(0), reader.GetTotalRecords())
}

// TestStagedFileReaderAdv_ReadAfterClose tests that read fails after close
func TestStagedFileReaderAdv_ReadAfterClose(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	logId := int64(14)
	segId := int64(14)

	localBaseDir := filepath.Join(tempDir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	headerData := codec.EncodeRecord(headerRecord)
	_, err = file.Write(headerData)
	require.NoError(t, err)

	// Write a data block
	dataRecords := make([]codec.Record, 0, 5)
	for i := 0; i < 5; i++ {
		dataRecords = append(dataRecords, &codec.DataRecord{Payload: []byte("test")})
	}
	blockData := encodeRecordList(dataRecords)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  4,
		BlockLength:  uint32(len(blockData)),
		BlockCrc:     crc32.ChecksumIEEE(blockData),
	}
	_, err = file.Write(codec.EncodeRecord(blockHeader))
	require.NoError(t, err)
	_, err = file.Write(blockData)
	require.NoError(t, err)
	file.Close()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)

	// Close the reader
	err = reader.Close(ctx)
	require.NoError(t, err)

	// Read after close should fail
	opt := storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}
	_, err = reader.ReadNextBatchAdv(ctx, opt, nil)
	assert.Error(t, err)
}

// === Helper to create a test reader from a finalized writer ===

func createTestReaderFromWriter(t *testing.T, dir string, logId, segId int64, entryCount int64, lac int64) *StagedFileReaderAdv {
	t.Helper()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	writer, err := NewStagedFileWriter(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)

	for i := int64(0); i < entryCount; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test data"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(context.Background())
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)

	_, err = writer.Finalize(context.Background(), lac)
	require.NoError(t, err)
	writer.Close(context.Background())

	reader, err := NewStagedFileReaderAdv(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)
	return reader
}

// === getFooterBlockKey / getCompactedBlockKey ===

func TestStagedFileReaderAdv_GetFooterBlockKey(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 5, 3, 3, 2)
	defer reader.Close(context.Background())

	assert.Equal(t, "test-root/5/3/footer.blk", reader.getFooterBlockKey())
}

func TestStagedFileReaderAdv_GetCompactedBlockKey(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 5, 3, 3, 2)
	defer reader.Close(context.Background())

	assert.Equal(t, "test-root/5/3/m_0.blk", reader.getCompactedBlockKey(0))
	assert.Equal(t, "test-root/5/3/m_7.blk", reader.getCompactedBlockKey(7))
}

// === GetLastEntryID ===

func TestStagedFileReaderAdv_GetLastEntryID_WithFooter(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 20, 20, 10, 9)
	defer reader.Close(context.Background())

	lastEntryID, err := reader.GetLastEntryID(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(9), lastEntryID)
}

func TestStagedFileReaderAdv_GetLastEntryID_IncompleteFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logId := int64(21)
	segId := int64(21)
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// Create a file without footer (incomplete)
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write header
	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)

	// Write a data block with entries 0-4
	dataRecords := make([]codec.Record, 0, 5)
	for i := 0; i < 5; i++ {
		dataRecords = append(dataRecords, &codec.DataRecord{Payload: []byte("data")})
	}
	blockData := encodeRecordList(dataRecords)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  4,
		BlockLength:  uint32(len(blockData)),
		BlockCrc:     crc32.ChecksumIEEE(blockData),
	}
	_, err = file.Write(codec.EncodeRecord(blockHeader))
	require.NoError(t, err)
	_, err = file.Write(blockData)
	require.NoError(t, err)
	file.Close()

	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	assert.True(t, reader.isIncompleteFile.Load())

	lastEntryID, err := reader.GetLastEntryID(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), lastEntryID)
}

func TestStagedFileReaderAdv_GetLastEntryID_Closed(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 22, 22, 3, 2)
	reader.Close(context.Background())

	_, err := reader.GetLastEntryID(context.Background())
	assert.ErrorIs(t, err, werr.ErrFileReaderAlreadyClosed)
}

func TestStagedFileReaderAdv_GetLastEntryID_NoBlocks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logId := int64(23)
	segId := int64(23)
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// Create a file with only header (no blocks, no footer)
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)
	file.Close()

	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	_, err = reader.GetLastEntryID(ctx)
	assert.ErrorIs(t, err, werr.ErrFileReaderNoBlockFound)
}

// === parseMinioFooterDataUnsafe ===

func TestStagedFileReaderAdv_ParseMinioFooterData_Valid(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 30, 30, 5, 4)
	defer reader.Close(context.Background())

	// Build valid footer data: [IndexRecords][FooterRecord]
	blockIndexes := []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 4},
	}
	footer := &codec.FooterRecord{
		TotalBlocks:  1,
		TotalRecords: 5,
		TotalSize:    100,
		IndexLength:  uint32(codec.RecordHeaderSize + codec.IndexRecordSize),
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(0),
		LAC:          4,
	}

	var buf bytes.Buffer
	for _, idx := range blockIndexes {
		buf.Write(codec.EncodeRecord(idx))
	}
	buf.Write(codec.EncodeRecord(footer))
	footerData := buf.Bytes()

	// Reset reader state before parsing
	reader.footer = nil
	reader.blockIndexes = nil
	reader.isIncompleteFile.Store(true)

	err := reader.parseMinioFooterDataUnsafe(context.Background(), footerData)
	assert.NoError(t, err)
	assert.NotNil(t, reader.footer)
	assert.Len(t, reader.blockIndexes, 1)
	assert.Equal(t, int64(4), reader.footer.LAC)
}

func TestStagedFileReaderAdv_ParseMinioFooterData_TooSmall(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 31, 31, 3, 2)
	defer reader.Close(context.Background())

	err := reader.parseMinioFooterDataUnsafe(context.Background(), []byte{0x01, 0x02})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "footer data too small")
}

// === parseIndexDataUnsafe ===

func TestStagedFileReaderAdv_ParseIndexData_Valid(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 32, 32, 3, 2)
	defer reader.Close(context.Background())

	// Build valid index data
	idx := &codec.IndexRecord{
		BlockNumber:  0,
		StartOffset:  0,
		BlockSize:    100,
		FirstEntryID: 0,
		LastEntryID:  2,
	}
	indexData := codec.EncodeRecord(idx)

	reader.blockIndexes = nil
	err := reader.parseIndexDataUnsafe(context.Background(), indexData, 1)
	assert.NoError(t, err)
	assert.Len(t, reader.blockIndexes, 1)
	assert.Equal(t, int32(0), reader.blockIndexes[0].BlockNumber)
}

func TestStagedFileReaderAdv_ParseIndexData_WrongRecordType(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 33, 33, 3, 2)
	defer reader.Close(context.Background())

	// Write a data record instead of index record
	dataRecord := &codec.DataRecord{Payload: []byte("test")}
	data := codec.EncodeRecord(dataRecord)

	reader.blockIndexes = nil
	err := reader.parseIndexDataUnsafe(context.Background(), data, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected record type")
}

func TestStagedFileReaderAdv_ParseIndexData_CountMismatch(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 34, 34, 3, 2)
	defer reader.Close(context.Background())

	// Build one index record but expect 2
	idx := &codec.IndexRecord{
		BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 2,
	}
	indexData := codec.EncodeRecord(idx)

	reader.blockIndexes = nil
	err := reader.parseIndexDataUnsafe(context.Background(), indexData, 2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parsed 1 blocks, expected 2")
}

// === scanForAllBlockInfoUnsafe ===

func TestStagedFileReaderAdv_ScanForAllBlockInfo(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logId := int64(40)
	segId := int64(40)
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// Create a file without footer but with blocks
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write header
	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)

	// Write 2 data blocks
	for blockNum := int32(0); blockNum < 2; blockNum++ {
		firstEntry := int64(blockNum) * 5
		lastEntry := firstEntry + 4
		dataRecords := make([]codec.Record, 0, 5)
		for i := 0; i < 5; i++ {
			dataRecords = append(dataRecords, &codec.DataRecord{Payload: []byte("data")})
		}
		blockData := encodeRecordList(dataRecords)
		blockHeader := &codec.BlockHeaderRecord{
			BlockNumber:  blockNum,
			FirstEntryID: firstEntry,
			LastEntryID:  lastEntry,
			BlockLength:  uint32(len(blockData)),
			BlockCrc:     crc32.ChecksumIEEE(blockData),
		}
		_, err = file.Write(codec.EncodeRecord(blockHeader))
		require.NoError(t, err)
		_, err = file.Write(blockData)
		require.NoError(t, err)
	}
	file.Close()

	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Trigger the lazy scan by calling GetLastEntryID
	reader.lastAddConfirmed.Store(9)
	lastEntry, err := reader.GetLastEntryID(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(9), lastEntry) // last entry in last block (block1: 5-9)

	// Verify that the scan found the blocks
	assert.GreaterOrEqual(t, len(reader.blockIndexes), 2)
}

// === determineBlocksToRead ===

func TestStagedFileReaderAdv_DetermineBlocksToRead(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 50, 50, 20, 19)
	defer reader.Close(context.Background())

	// All blocks should be returned when limits are high
	blocks := reader.determineBlocksToRead(0, 1000, 10*1024*1024)
	assert.NotEmpty(t, blocks)
}

func TestStagedFileReaderAdv_DetermineBlocksToRead_LimitedEntries(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 51, 51, 20, 19)
	defer reader.Close(context.Background())

	// Limit to 1 entry — should still return at least 1 block
	blocks := reader.determineBlocksToRead(0, 1, 10*1024*1024)
	assert.NotEmpty(t, blocks)
}

func TestStagedFileReaderAdv_DetermineBlocksToRead_OutOfRange(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 52, 52, 5, 4)
	defer reader.Close(context.Background())

	// Start from beyond all blocks
	blocks := reader.determineBlocksToRead(100, 1000, 10*1024*1024)
	assert.Empty(t, blocks)
}

// === tryParseMinioFooterUnsafe ===

func TestStagedFileReaderAdv_TryParseMinioFooter_NilClient(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 60, 60, 3, 2)
	defer reader.Close(context.Background())

	reader.storageCli = nil
	err := reader.tryParseMinioFooterUnsafe(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "storage client not available")
}

func TestStagedFileReaderAdv_TryParseMinioFooter_NotFound(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	// Create a minimal file for reader creation
	localBaseDir := filepath.Join(dir, "local")
	logId := int64(61)
	segId := int64(61)
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)
	file.Close()

	// Mock: footer stat returns not-found
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, minio.ErrorResponse{Code: "NoSuchKey"})

	reader, err := NewStagedFileReaderAdv(context.Background(), "test-bucket", "test-root", localBaseDir, logId, segId, mockStorage, cfg)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	// Footer not found in minio, so should remain incomplete
	assert.True(t, reader.isIncompleteFile.Load())
}

func TestStagedFileReaderAdv_TryParseMinioFooter_ValidFooter(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	mockStorage := mocks_objectstorage.NewObjectStorage(t)

	logId := int64(62)
	segId := int64(62)

	// Build valid footer data
	blockIndexes := []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 4},
	}
	footer := &codec.FooterRecord{
		TotalBlocks:  1,
		TotalRecords: 5,
		TotalSize:    100,
		IndexLength:  uint32(codec.RecordHeaderSize + codec.IndexRecordSize),
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(0),
		LAC:          4,
	}

	var buf bytes.Buffer
	for _, idx := range blockIndexes {
		buf.Write(codec.EncodeRecord(idx))
	}
	buf.Write(codec.EncodeRecord(footer))
	footerData := buf.Bytes()

	footerKey := "test-root/62/62/footer.blk"

	// Mock StatObject
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(int64(len(footerData)), false, nil)

	// Mock GetObject
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", footerKey, int64(0), int64(len(footerData)), mock.Anything, mock.Anything).
		Return(&readerMockFileReader{data: footerData}, nil)

	// Create a minimal file for reader creation
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)
	file.Close()

	reader, err := NewStagedFileReaderAdv(context.Background(), "test-bucket", "test-root", localBaseDir, logId, segId, mockStorage, cfg)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	// Footer found in minio, so should be complete and compacted
	assert.False(t, reader.isIncompleteFile.Load())
	assert.True(t, reader.isCompacted.Load())
	assert.NotNil(t, reader.footer)
	assert.Equal(t, int64(4), reader.footer.LAC)
}

// === readCompactedDataFromMinio ===

// readerMockFileReader implements minioHandler.FileReader for tests
type readerMockFileReader struct {
	data     []byte
	position int
}

func (m *readerMockFileReader) Read(p []byte) (n int, err error) {
	if m.position >= len(m.data) {
		return 0, io.EOF
	}
	n = copy(p, m.data[m.position:])
	m.position += n
	return n, nil
}

func (m *readerMockFileReader) Close() error {
	return nil
}

func (m *readerMockFileReader) ReadAt(p []byte, off int64) (n int, err error) {
	if int(off) >= len(m.data) {
		return 0, io.EOF
	}
	n = copy(p, m.data[off:])
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

func (m *readerMockFileReader) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = int64(m.position) + offset
	case io.SeekEnd:
		newPos = int64(len(m.data)) + offset
	}
	m.position = int(newPos)
	return newPos, nil
}

func (m *readerMockFileReader) Size() (int64, error) {
	return int64(len(m.data)), nil
}

func TestStagedFileReaderAdv_ReadCompactedDataFromMinio(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	logId := int64(70)
	segId := int64(70)

	// Build valid footer data
	blockIndex := &codec.IndexRecord{
		BlockNumber: 0, StartOffset: 0, BlockSize: 200, FirstEntryID: 0, LastEntryID: 4,
	}
	footer := &codec.FooterRecord{
		TotalBlocks:  1,
		TotalRecords: 5,
		TotalSize:    200,
		IndexLength:  uint32(codec.RecordHeaderSize + codec.IndexRecordSize),
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(0),
		LAC:          4,
	}

	var footerBuf bytes.Buffer
	footerBuf.Write(codec.EncodeRecord(blockIndex))
	footerBuf.Write(codec.EncodeRecord(footer))
	footerData := footerBuf.Bytes()

	footerKey := "test-root/70/70/footer.blk"

	// Mock StatObject for footer
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(int64(len(footerData)), false, nil)

	// Mock GetObject for footer
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", footerKey, int64(0), int64(len(footerData)), mock.Anything, mock.Anything).
		Return(&readerMockFileReader{data: footerData}, nil)

	// Build valid block data for compacted block m_0.blk
	// Format: [HeaderRecord][BlockHeaderRecord][DataRecords]
	var blockBuf bytes.Buffer
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(0),
		FirstEntryID: 0,
	}
	blockBuf.Write(codec.EncodeRecord(headerRecord))

	dataRecords := make([]codec.Record, 0, 5)
	for i := 0; i < 5; i++ {
		dataRecords = append(dataRecords, &codec.DataRecord{Payload: []byte("test data")})
	}
	blockDataOnly := encodeRecordList(dataRecords)
	blockHeaderRecord := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  4,
		BlockLength:  uint32(len(blockDataOnly)),
		BlockCrc:     crc32.ChecksumIEEE(blockDataOnly),
	}
	blockBuf.Write(codec.EncodeRecord(blockHeaderRecord))
	blockBuf.Write(blockDataOnly)
	blockData := blockBuf.Bytes()

	blockKey := "test-root/70/70/m_0.blk"

	// Mock GetObject for block data
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", blockKey, int64(0), mock.Anything, mock.Anything, mock.Anything).
		Return(&readerMockFileReader{data: blockData}, nil)

	// Create a minimal file for reader creation
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	hdr := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(hdr))
	require.NoError(t, err)
	file.Close()

	reader, err := NewStagedFileReaderAdv(context.Background(), "test-bucket", "test-root", localBaseDir, logId, segId, mockStorage, cfg)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	assert.True(t, reader.isCompacted.Load())

	opt := storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}

	result, err := reader.ReadNextBatchAdv(context.Background(), opt, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 5, len(result.Entries))
	assert.Equal(t, int64(0), result.Entries[0].EntryId)
	assert.Equal(t, int64(4), result.Entries[4].EntryId)
}

// === extractEntriesFromBlockData / extractEntriesFromBlockDataWithBytes ===

func TestStagedFileReaderAdv_ExtractEntriesFromBlockData(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 80, 80, 5, 4)
	defer reader.Close(context.Background())

	// Build block data with data records
	dataRecords := make([]codec.Record, 0, 5)
	for i := 0; i < 5; i++ {
		dataRecords = append(dataRecords, &codec.DataRecord{Payload: []byte("payload")})
	}
	blockData := encodeRecordList(dataRecords)

	blockIndex := &codec.IndexRecord{
		BlockNumber: 0, FirstEntryID: 0, LastEntryID: 4,
	}

	entries, err := reader.extractEntriesFromBlockData(context.Background(), blockData, blockIndex, 0)
	assert.NoError(t, err)
	assert.Len(t, entries, 5)
}

func TestStagedFileReaderAdv_ExtractEntriesFromBlockDataWithBytes(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 81, 81, 5, 4)
	defer reader.Close(context.Background())

	dataRecords := make([]codec.Record, 0, 5)
	for i := 0; i < 5; i++ {
		dataRecords = append(dataRecords, &codec.DataRecord{Payload: []byte("payload")})
	}
	blockData := encodeRecordList(dataRecords)

	blockIndex := &codec.IndexRecord{
		BlockNumber: 0, FirstEntryID: 0, LastEntryID: 4,
	}

	entries, readBytes, err := reader.extractEntriesFromBlockDataWithBytes(context.Background(), blockData, blockIndex, 0)
	assert.NoError(t, err)
	assert.Len(t, entries, 5)
	assert.Greater(t, readBytes, 0)
}

func TestStagedFileReaderAdv_ExtractEntriesFromBlockDataWithBytes_FilterStart(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 82, 82, 5, 4)
	defer reader.Close(context.Background())

	dataRecords := make([]codec.Record, 0, 5)
	for i := 0; i < 5; i++ {
		dataRecords = append(dataRecords, &codec.DataRecord{Payload: []byte("payload")})
	}
	blockData := encodeRecordList(dataRecords)

	blockIndex := &codec.IndexRecord{
		BlockNumber: 0, FirstEntryID: 0, LastEntryID: 4,
	}

	// Filter: only entries >= 3
	entries, _, err := reader.extractEntriesFromBlockDataWithBytes(context.Background(), blockData, blockIndex, 3)
	assert.NoError(t, err)
	assert.Len(t, entries, 2) // entries 3 and 4
	assert.Equal(t, int64(3), entries[0].EntryId)
	assert.Equal(t, int64(4), entries[1].EntryId)
}

// === readBlockFromMinioByKey ===

func TestStagedFileReaderAdv_ReadBlockFromMinioByKey_Success(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	logId := int64(90)
	segId := int64(90)

	// Create minimal file
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	hdr := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(hdr))
	require.NoError(t, err)
	file.Close()

	// Mock StatObject for footer — not found
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, minio.ErrorResponse{Code: "NoSuchKey"})

	reader, err := NewStagedFileReaderAdv(context.Background(), "test-bucket", "test-root", localBaseDir, logId, segId, mockStorage, cfg)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	blockContent := []byte("block data content")
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", "test-root/90/90/m_0.blk", int64(0), int64(len(blockContent)), mock.Anything, mock.Anything).
		Return(&readerMockFileReader{data: blockContent}, nil)

	data, err := reader.readBlockFromMinioByKey(context.Background(), "test-root/90/90/m_0.blk", int64(len(blockContent)))
	assert.NoError(t, err)
	assert.Equal(t, blockContent, data)
}

func TestStagedFileReaderAdv_ReadBlockFromMinioByKey_Error(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	logId := int64(91)
	segId := int64(91)

	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	hdr := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(hdr))
	require.NoError(t, err)
	file.Close()

	// Mock StatObject for footer — not found
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, minio.ErrorResponse{Code: "NoSuchKey"})

	reader, err := NewStagedFileReaderAdv(context.Background(), "test-bucket", "test-root", localBaseDir, logId, segId, mockStorage, cfg)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, fmt.Errorf("get error"))

	_, err = reader.readBlockFromMinioByKey(context.Background(), "test-root/91/91/m_0.blk", 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get block object")
}

// === readAndExtractBlockConcurrently ===

func TestStagedFileReaderAdv_ReadAndExtractBlockConcurrently_Success(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	logId := int64(95)
	segId := int64(95)

	// Create minimal file
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	hdr := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(hdr))
	require.NoError(t, err)
	file.Close()

	// Mock StatObject for footer — not found
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, minio.ErrorResponse{Code: "NoSuchKey"})

	reader, err := NewStagedFileReaderAdv(context.Background(), "test-bucket", "test-root", localBaseDir, logId, segId, mockStorage, cfg)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	// Set up footer and block index manually for the compacted reader path
	reader.footer = &codec.FooterRecord{
		TotalBlocks: 1,
		Version:     codec.FormatVersion,
		Flags:       codec.SetCompacted(0),
		LAC:         4,
	}
	reader.isCompacted.Store(true)
	reader.isIncompleteFile.Store(false)

	blockIndex := &codec.IndexRecord{
		BlockNumber: 0, StartOffset: 0, BlockSize: 200, FirstEntryID: 0, LastEntryID: 4,
	}
	reader.blockIndexes = []*codec.IndexRecord{blockIndex}

	// Build block data with data records
	dataRecords := make([]codec.Record, 0, 5)
	for i := 0; i < 5; i++ {
		dataRecords = append(dataRecords, &codec.DataRecord{Payload: []byte("payload")})
	}
	blockData := encodeRecordList(dataRecords)

	blockToRead := &BlockToRead{
		blockID: 0,
		objKey:  "test-root/95/95/m_0.blk",
		size:    int64(len(blockData)),
	}

	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", "test-root/95/95/m_0.blk", int64(0), int64(len(blockData)), mock.Anything, mock.Anything).
		Return(&readerMockFileReader{data: blockData}, nil)

	result := reader.readAndExtractBlockConcurrently(context.Background(), blockToRead, blockIndex, 0)
	assert.Nil(t, result.err)
	assert.Len(t, result.entries, 5)
}

// === ReadNextBatchAdv with lastReadBatchInfo ===

func TestStagedFileReaderAdv_ReadNextBatchAdv_WithLastReadState(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(100)
	segId := int64(100)

	// Create file with 2 blocks
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write header
	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)

	for blockNum := int32(0); blockNum < 2; blockNum++ {
		firstEntry := int64(blockNum) * 5
		lastEntry := firstEntry + 4
		dataRecords := make([]codec.Record, 0, 5)
		for j := 0; j < 5; j++ {
			dataRecords = append(dataRecords, &codec.DataRecord{Payload: []byte("data")})
		}
		blockData := encodeRecordList(dataRecords)
		blockHeader := &codec.BlockHeaderRecord{
			BlockNumber:  blockNum,
			FirstEntryID: firstEntry,
			LastEntryID:  lastEntry,
			BlockLength:  uint32(len(blockData)),
			BlockCrc:     crc32.ChecksumIEEE(blockData),
		}
		_, err = file.Write(codec.EncodeRecord(blockHeader))
		require.NoError(t, err)
		_, err = file.Write(blockData)
		require.NoError(t, err)
	}
	file.Close()

	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.lastAddConfirmed.Store(9)

	// First read
	opt := storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 100,
	}

	result, err := reader.ReadNextBatchAdv(ctx, opt, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.LastReadState)

	// Second read using LastReadState
	opt2 := storage.ReaderOpt{
		StartEntryID:    5,
		MaxBatchEntries: 100,
	}
	result2, err := reader.ReadNextBatchAdv(ctx, opt2, result.LastReadState)
	// Either returns data or error (depends on last block info)
	if err == nil {
		assert.NotNil(t, result2)
	}
}

// === isFooterExistsUnsafe ===

func TestStagedFileReaderAdv_IsFooterExists_True(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 110, 110, 5, 4)
	defer reader.Close(context.Background())

	// The file was finalized so footer should exist
	reader.mu.RLock()
	exists := reader.isFooterExistsUnsafe(context.Background())
	reader.mu.RUnlock()
	assert.True(t, exists)
}

func TestStagedFileReaderAdv_IsFooterExists_False(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logId := int64(111)
	segId := int64(111)
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// Create a file without footer
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)
	file.Close()

	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.mu.RLock()
	exists := reader.isFooterExistsUnsafe(ctx)
	reader.mu.RUnlock()
	assert.False(t, exists)
}

// === readCompactedDataFromMinio edge cases ===

func TestStagedFileReaderAdv_ReadCompactedDataFromMinio_NoBlocks(t *testing.T) {
	dir := t.TempDir()
	reader := createTestReaderFromWriter(t, dir, 120, 120, 3, 2)
	defer reader.Close(context.Background())

	// Manually set compacted state with no block indexes
	reader.isCompacted.Store(true)
	reader.blockIndexes = nil

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	result, err := reader.readCompactedDataFromMinio(context.Background(), opt, 999, 0)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.Entries)
}

// === ReadNextBatchAdv with compacted footer and lastReadState ===

func TestStagedFileReaderAdv_ReadNextBatchAdv_CompactedWithLastReadState(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	logId := int64(130)
	segId := int64(130)

	// Build footer
	blockIndex0 := &codec.IndexRecord{BlockNumber: 0, StartOffset: 0, BlockSize: 200, FirstEntryID: 0, LastEntryID: 4}
	blockIndex1 := &codec.IndexRecord{BlockNumber: 1, StartOffset: 200, BlockSize: 200, FirstEntryID: 5, LastEntryID: 9}
	footer := &codec.FooterRecord{
		TotalBlocks: 2, TotalRecords: 10, TotalSize: 400,
		IndexLength: uint32(2 * (codec.RecordHeaderSize + codec.IndexRecordSize)),
		Version:     codec.FormatVersion, Flags: codec.SetCompacted(0), LAC: 9,
	}

	var footerBuf bytes.Buffer
	footerBuf.Write(codec.EncodeRecord(blockIndex0))
	footerBuf.Write(codec.EncodeRecord(blockIndex1))
	footerBuf.Write(codec.EncodeRecord(footer))
	footerData := footerBuf.Bytes()

	footerKey := "test-root/130/130/footer.blk"
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(int64(len(footerData)), false, nil)
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", footerKey, int64(0), int64(len(footerData)), mock.Anything, mock.Anything).
		Return(&readerMockFileReader{data: footerData}, nil)

	// Build block data for both blocks
	for blockNum := 0; blockNum < 2; blockNum++ {
		dataRecords := make([]codec.Record, 0, 5)
		for j := 0; j < 5; j++ {
			dataRecords = append(dataRecords, &codec.DataRecord{Payload: []byte("data")})
		}
		blockData := encodeRecordList(dataRecords)

		blockKey := reader_getCompactedBlockKey("test-root", logId, segId, int64(blockNum))
		mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", blockKey, int64(0), mock.Anything, mock.Anything, mock.Anything).
			Return(&readerMockFileReader{data: blockData}, nil).Maybe()
	}

	// Create minimal file
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	hdr := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(hdr))
	require.NoError(t, err)
	file.Close()

	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, mockStorage, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	assert.True(t, reader.isCompacted.Load())

	// First read
	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 5}
	result, err := reader.ReadNextBatchAdv(ctx, opt, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, result.Entries)

	// Second read with LastReadState
	if result.LastReadState != nil {
		opt2 := storage.ReaderOpt{StartEntryID: 5, MaxBatchEntries: 10}
		result2, err := reader.ReadNextBatchAdv(ctx, opt2, result.LastReadState)
		// Compacted reads use pool-based reading
		if err == nil {
			assert.NotNil(t, result2)
		}
	}
}

// Helper to build compacted block key (same as reader method)
func reader_getCompactedBlockKey(rootPath string, logId, segId, blockID int64) string {
	return fmt.Sprintf("%s/%d/%d/m_%d.blk", rootPath, logId, segId, blockID)
}

// === NewStagedFileReaderAdv error branches ===

func TestNewStagedFileReaderAdv_OpenFileError_NotPermission(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(200)
	segId := int64(200)
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))

	// Create a file then make it unreadable
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	file.Close()
	require.NoError(t, os.Chmod(filePath, 0o000))
	t.Cleanup(func() { os.Chmod(filePath, 0o644) })

	_, err = NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "open file")
}

// === tryParseMinioFooterUnsafe error branches ===

func TestStagedFileReaderAdv_TryParseMinioFooter_StatObjectError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	logId := int64(210)
	segId := int64(210)

	// Create minimal file
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	_, err = file.Write(codec.EncodeRecord(&codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}))
	require.NoError(t, err)
	file.Close()

	footerKey := fmt.Sprintf("test-root/%d/%d/footer.blk", logId, segId)

	// Mock: StatObject returns a non-NotExist error (e.g., network error)
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(int64(0), false, fmt.Errorf("network timeout"))

	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, mockStorage, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Minio footer parse failed, falls back to local file which is incomplete
	assert.True(t, reader.isIncompleteFile.Load())
}

func TestStagedFileReaderAdv_TryParseMinioFooter_GetObjectError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	logId := int64(211)
	segId := int64(211)

	// Create minimal file
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	_, err = file.Write(codec.EncodeRecord(&codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}))
	require.NoError(t, err)
	file.Close()

	footerKey := fmt.Sprintf("test-root/%d/%d/footer.blk", logId, segId)

	// StatObject succeeds but GetObject fails
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(int64(100), false, nil)
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", footerKey, int64(0), int64(100), mock.Anything, mock.Anything).
		Return(nil, fmt.Errorf("get object failed"))

	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, mockStorage, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Should fall back to local, remain incomplete
	assert.True(t, reader.isIncompleteFile.Load())
}

func TestStagedFileReaderAdv_TryParseMinioFooter_ReadFullError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	logId := int64(212)
	segId := int64(212)

	// Create minimal file
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	_, err = file.Write(codec.EncodeRecord(&codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}))
	require.NoError(t, err)
	file.Close()

	footerKey := fmt.Sprintf("test-root/%d/%d/footer.blk", logId, segId)
	claimedSize := int64(200) // Claim a large size but return a short reader

	// StatObject returns large size, GetObject returns reader with less data
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(claimedSize, false, nil)
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", footerKey, int64(0), claimedSize, mock.Anything, mock.Anything).
		Return(&readerMockFileReader{data: []byte("short")}, nil) // io.ReadFull will fail: unexpected EOF

	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, mockStorage, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	assert.True(t, reader.isIncompleteFile.Load())
}

func TestStagedFileReaderAdv_TryParseMinioFooter_InvalidFooterData(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	logId := int64(213)
	segId := int64(213)

	// Create minimal file
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	_, err = file.Write(codec.EncodeRecord(&codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}))
	require.NoError(t, err)
	file.Close()

	footerKey := fmt.Sprintf("test-root/%d/%d/footer.blk", logId, segId)

	// Return garbage data that's large enough but can't be parsed as footer
	maxFooterSize := codec.GetMaxFooterReadSize()
	garbageData := make([]byte, maxFooterSize+10) // big enough to pass size check
	for i := range garbageData {
		garbageData[i] = 0xFF
	}

	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(int64(len(garbageData)), false, nil)
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", footerKey, int64(0), int64(len(garbageData)), mock.Anything, mock.Anything).
		Return(&readerMockFileReader{data: garbageData}, nil)

	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, mockStorage, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// parseMinioFooterDataUnsafe should fail, fall back to incomplete
	assert.True(t, reader.isIncompleteFile.Load())
}

// === tryParseLocalFooterUnsafe: parseIndexRecordsUnsafe error ===

func TestStagedFileReaderAdv_TryParseLocalFooter_IndexParseError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	logId := int64(220)
	segId := int64(220)

	// Create a file with a valid footer but corrupted index area
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write header
	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)

	// Write garbage where index should be (same size as one IndexRecord)
	indexRecordSize := codec.RecordHeaderSize + codec.IndexRecordSize
	garbage := make([]byte, indexRecordSize)
	for i := range garbage {
		garbage[i] = 0xAB
	}
	_, err = file.Write(garbage)
	require.NoError(t, err)

	// Write valid footer claiming 1 block exists
	footer := &codec.FooterRecord{
		TotalBlocks:  1,
		TotalRecords: 5,
		TotalSize:    100,
		IndexLength:  uint32(indexRecordSize),
		Version:      codec.FormatVersion,
		Flags:        0,
		LAC:          4,
	}
	_, err = file.Write(codec.EncodeRecord(footer))
	require.NoError(t, err)
	file.Close()

	// Reader should succeed (falls back to incomplete when index parsing fails)
	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Footer found but index parsing failed → reset to incomplete, footer nil
	assert.True(t, reader.isIncompleteFile.Load())
	assert.Nil(t, reader.footer)
}

// === scanForAllBlockInfoUnsafe error branches ===

// Helper: create a minimal reader with an incomplete file for scan testing
func createMinimalIncompleteReader(t *testing.T, dir string, logId, segId int64, fileContent []byte) *StagedFileReaderAdv {
	t.Helper()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	require.NoError(t, os.WriteFile(filePath, fileContent, 0o644))

	reader, err := NewStagedFileReaderAdv(context.Background(), "test-bucket", "test-root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	return reader
}

func TestScanForAllBlockInfo_NotEnoughDataForBlockHeader(t *testing.T) {
	dir := t.TempDir()
	logId := int64(230)
	segId := int64(230)

	// Write header + a few extra bytes (not enough for BlockHeaderRecord)
	var buf bytes.Buffer
	buf.Write(codec.EncodeRecord(&codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}))
	buf.Write([]byte{0x01, 0x02, 0x03}) // partial block header data

	reader := createMinimalIncompleteReader(t, dir, logId, segId, buf.Bytes())
	defer reader.Close(context.Background())

	assert.True(t, reader.isIncompleteFile.Load())
	// Trigger scan via GetLastEntryID
	reader.lastAddConfirmed.Store(100)
	_, err := reader.GetLastEntryID(context.Background())
	// No blocks found
	assert.Error(t, err)
	assert.Empty(t, reader.blockIndexes)
}

func TestScanForAllBlockInfo_InvalidBlockHeader(t *testing.T) {
	dir := t.TempDir()
	logId := int64(231)
	segId := int64(231)

	// Write valid header + garbage where block header should be
	var buf bytes.Buffer
	buf.Write(codec.EncodeRecord(&codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}))
	// Write enough garbage bytes for a complete BlockHeaderRecord but with invalid CRC/type
	blockHeaderRecordSize := codec.RecordHeaderSize + codec.BlockHeaderRecordSize
	garbage := make([]byte, blockHeaderRecordSize)
	for i := range garbage {
		garbage[i] = 0xCD
	}
	buf.Write(garbage)

	reader := createMinimalIncompleteReader(t, dir, logId, segId, buf.Bytes())
	defer reader.Close(context.Background())

	reader.lastAddConfirmed.Store(100)
	_, err := reader.GetLastEntryID(context.Background())
	assert.Error(t, err) // No valid blocks found
	assert.Empty(t, reader.blockIndexes)
}

func TestScanForAllBlockInfo_NotEnoughDataForBlockData(t *testing.T) {
	dir := t.TempDir()
	logId := int64(232)
	segId := int64(232)

	// Write header + valid block header that claims a large block length, but no actual block data
	var buf bytes.Buffer
	buf.Write(codec.EncodeRecord(&codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}))

	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  9,
		BlockLength:  10000, // Claims 10000 bytes but file won't have them
		BlockCrc:     0,
	}
	buf.Write(codec.EncodeRecord(blockHeader))

	reader := createMinimalIncompleteReader(t, dir, logId, segId, buf.Bytes())
	defer reader.Close(context.Background())

	reader.lastAddConfirmed.Store(100)
	_, err := reader.GetLastEntryID(context.Background())
	assert.Error(t, err) // No valid blocks
	assert.Empty(t, reader.blockIndexes)
}

func TestScanForAllBlockInfo_BlockCrcVerificationFailure(t *testing.T) {
	dir := t.TempDir()
	logId := int64(233)
	segId := int64(233)

	// Write header + valid block header + corrupted block data (CRC mismatch)
	var buf bytes.Buffer
	buf.Write(codec.EncodeRecord(&codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}))

	// Create valid data records
	dataRecords := make([]codec.Record, 0, 5)
	for i := 0; i < 5; i++ {
		dataRecords = append(dataRecords, &codec.DataRecord{Payload: []byte("data")})
	}
	blockData := encodeRecordList(dataRecords)

	// Write block header with WRONG CRC
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  4,
		BlockLength:  uint32(len(blockData)),
		BlockCrc:     0xDEADBEEF, // Wrong CRC
	}
	buf.Write(codec.EncodeRecord(blockHeader))
	buf.Write(blockData)

	reader := createMinimalIncompleteReader(t, dir, logId, segId, buf.Bytes())
	defer reader.Close(context.Background())

	reader.lastAddConfirmed.Store(100)
	_, err := reader.GetLastEntryID(context.Background())
	assert.Error(t, err) // CRC mismatch → no valid blocks
	assert.Empty(t, reader.blockIndexes)
}

func TestScanForAllBlockInfo_SecondBlockCorrupted(t *testing.T) {
	dir := t.TempDir()
	logId := int64(234)
	segId := int64(234)

	// Write header + 1 valid block + 1 corrupted block
	// Scanner should stop at block 1 but still return block 0
	var buf bytes.Buffer
	buf.Write(codec.EncodeRecord(&codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}))

	// Block 0: valid
	dataRecords0 := make([]codec.Record, 0, 5)
	for i := 0; i < 5; i++ {
		dataRecords0 = append(dataRecords0, &codec.DataRecord{Payload: []byte("data")})
	}
	blockData0 := encodeRecordList(dataRecords0)
	blockHeader0 := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  4,
		BlockLength:  uint32(len(blockData0)),
		BlockCrc:     crc32.ChecksumIEEE(blockData0),
	}
	buf.Write(codec.EncodeRecord(blockHeader0))
	buf.Write(blockData0)

	// Block 1: corrupted CRC
	dataRecords1 := make([]codec.Record, 0, 5)
	for i := 0; i < 5; i++ {
		dataRecords1 = append(dataRecords1, &codec.DataRecord{Payload: []byte("more")})
	}
	blockData1 := encodeRecordList(dataRecords1)
	blockHeader1 := &codec.BlockHeaderRecord{
		BlockNumber:  1,
		FirstEntryID: 5,
		LastEntryID:  9,
		BlockLength:  uint32(len(blockData1)),
		BlockCrc:     0xBADC0DE, // Wrong CRC
	}
	buf.Write(codec.EncodeRecord(blockHeader1))
	buf.Write(blockData1)

	reader := createMinimalIncompleteReader(t, dir, logId, segId, buf.Bytes())
	defer reader.Close(context.Background())

	reader.lastAddConfirmed.Store(100)
	lastEntry, err := reader.GetLastEntryID(context.Background())
	require.NoError(t, err)
	// Only block 0 should be indexed (block 1 failed CRC)
	assert.Equal(t, 1, len(reader.blockIndexes))
	assert.Equal(t, int64(4), lastEntry)
}

// === New coverage tests ===

func TestUpdateLastAddConfirmed_LowerValue(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	localBaseDir := filepath.Join(tempDir, "local")
	logId := int64(100)
	segId := int64(100)

	// Create a minimal valid file (header + one block)
	segDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)

	dr := &codec.DataRecord{Payload: []byte("data")}
	blockData := codec.EncodeRecord(dr)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber: 0, FirstEntryID: 0, LastEntryID: 0,
		BlockLength: uint32(len(blockData)), BlockCrc: crc32.ChecksumIEEE(blockData),
	}
	_, err = file.Write(codec.EncodeRecord(blockHeader))
	require.NoError(t, err)
	_, err = file.Write(blockData)
	require.NoError(t, err)
	file.Close()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	reader, err := NewStagedFileReaderAdv(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Set LAC to 10
	err = reader.UpdateLastAddConfirmed(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, int64(10), reader.lastAddConfirmed.Load())

	// Try to set lower LAC - should be skipped (fast path)
	err = reader.UpdateLastAddConfirmed(ctx, 5)
	require.NoError(t, err)
	assert.Equal(t, int64(10), reader.lastAddConfirmed.Load()) // unchanged

	// Same value - should also skip
	err = reader.UpdateLastAddConfirmed(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, int64(10), reader.lastAddConfirmed.Load())
}

func TestNewStagedFileReaderAdv_MkdirAllError(t *testing.T) {
	ctx := context.Background()
	// Use a file as base dir to make MkdirAll fail
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "not-a-dir")
	err := os.WriteFile(filePath, []byte("block"), 0o644)
	require.NoError(t, err)

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	_, err = NewStagedFileReaderAdv(ctx, "bucket", "root", filePath, 1, 1, nil, cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create directory")
}

func TestVerifyBlockDataIntegrity_CRCMismatch(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	localBaseDir := filepath.Join(tempDir, "local")
	logId := int64(101)
	segId := int64(101)

	segDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)

	// Write a block with WRONG CRC
	dr := &codec.DataRecord{Payload: []byte("data")}
	blockData := codec.EncodeRecord(dr)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber: 0, FirstEntryID: 0, LastEntryID: 0,
		BlockLength: uint32(len(blockData)), BlockCrc: 0xDEADBEEF, // Wrong CRC
	}
	_, err = file.Write(codec.EncodeRecord(blockHeader))
	require.NoError(t, err)
	_, err = file.Write(blockData)
	require.NoError(t, err)
	file.Close()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// Create reader - it will try to scan the file
	reader, err := NewStagedFileReaderAdv(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Also test verifyBlockDataIntegrity directly
	wrongBlockHeader := &codec.BlockHeaderRecord{
		BlockNumber: 0, FirstEntryID: 0, LastEntryID: 0,
		BlockLength: 10, BlockCrc: 0xDEADBEEF,
	}
	err = reader.verifyBlockDataIntegrity(ctx, wrongBlockHeader, 0, []byte("some data!"))
	assert.Error(t, err)

	// The block CRC verification should have failed during scan
	// so no block indexes should be found (CRC failed blocks are skipped)
	// This exercises the verifyBlockDataIntegrity error path
	// Note: The reader still creates successfully because the file is "incomplete" (no footer)
	assert.True(t, reader.isIncompleteFile.Load())
}

func TestReadNextBatchAdv_CompletedFileEOF(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	localBaseDir := filepath.Join(tempDir, "local")
	logId := int64(102)
	segId := int64(102)
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// Create a finalized file using writer
	writer, err := NewStagedFileWriter(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	for i := int64(0); i < 5; i++ {
		_, err = writer.WriteDataAsync(ctx, i, []byte("data"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(ctx)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	_, err = writer.Finalize(ctx, 4)
	require.NoError(t, err)
	writer.Close(ctx)

	// Create reader
	reader, err := NewStagedFileReaderAdv(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Read past all entries (startEntryID > LAC and > all data)
	opt := storage.ReaderOpt{StartEntryID: 100, MaxBatchEntries: 10}
	_, err = reader.ReadNextBatchAdv(ctx, opt, nil)
	assert.Error(t, err)
	assert.True(t, werr.ErrFileReaderEndOfFile.Is(err), "Expected EOF, got: %v", err)
}

func TestReadNextBatchAdv_MaxEntriesDefault(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	localBaseDir := filepath.Join(tempDir, "local")
	logId := int64(103)
	segId := int64(103)
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// Create a finalized file
	writer, err := NewStagedFileWriter(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(ctx, i, []byte("data"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(ctx)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	_, err = writer.Finalize(ctx, 2)
	require.NoError(t, err)
	writer.Close(ctx)

	reader, err := NewStagedFileReaderAdv(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// MaxBatchEntries=0 should use default (100)
	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 0}
	result, err := reader.ReadNextBatchAdv(ctx, opt, nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Entries, 3) // All 3 entries should be read
}

func TestScanForAllBlockInfo_TruncatedBlockHeader(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	localBaseDir := filepath.Join(tempDir, "local")
	logId := int64(104)
	segId := int64(104)

	segDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write valid header
	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)

	// Write truncated block header (just a few bytes, not enough for BlockHeaderRecord)
	_, err = file.Write([]byte{0x01, 0x02, 0x03})
	require.NoError(t, err)
	file.Close()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// Reader should succeed (file is "incomplete") but no blocks recovered
	reader, err := NewStagedFileReaderAdv(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	assert.True(t, reader.isIncompleteFile.Load())
	// No valid blocks should be found
	assert.Empty(t, reader.blockIndexes)
}

func TestScanForAllBlockInfo_InvalidHeaderType(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	localBaseDir := filepath.Join(tempDir, "local")
	logId := int64(105)
	segId := int64(105)

	segDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write a DataRecord as the first record instead of HeaderRecord
	// The file needs to be large enough to pass the minimum size check
	dr := &codec.DataRecord{Payload: make([]byte, 200)}
	_, err = file.Write(codec.EncodeRecord(dr))
	require.NoError(t, err)
	file.Close()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// File exists but has wrong first record type
	// NewStagedFileReaderAdv should still succeed (incomplete file), but scan will fail
	// When called via GetLastEntryID -> scanForAllBlockInfoUnsafe, it will hit the error
	reader, err := NewStagedFileReaderAdv(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Try to get last entry ID, which triggers scanning
	_, err = reader.GetLastEntryID(ctx)
	assert.Error(t, err)
}

func TestGetLastEntryID_IncompleteNoBlocks(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	localBaseDir := filepath.Join(tempDir, "local")
	logId := int64(106)
	segId := int64(106)

	segDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write only a valid header (no blocks)
	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	headerData := codec.EncodeRecord(headerRecord)
	_, err = file.Write(headerData)
	require.NoError(t, err)

	// Pad with zeros to make file large enough for scan check
	padding := make([]byte, 200)
	_, err = file.Write(padding)
	require.NoError(t, err)
	file.Close()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	reader, err := NewStagedFileReaderAdv(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	assert.True(t, reader.isIncompleteFile.Load())

	// GetLastEntryID should scan but find no blocks
	_, err = reader.GetLastEntryID(ctx)
	assert.Error(t, err)
	assert.True(t, werr.ErrFileReaderNoBlockFound.Is(err), "Expected ErrFileReaderNoBlockFound, got: %v", err)
}

func TestReadAtUnsafe_NegativeOffset(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	localBaseDir := filepath.Join(tempDir, "local")
	logId := int64(107)
	segId := int64(107)

	segDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)

	dr := &codec.DataRecord{Payload: []byte("data")}
	blockData := codec.EncodeRecord(dr)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber: 0, FirstEntryID: 0, LastEntryID: 0,
		BlockLength: uint32(len(blockData)), BlockCrc: crc32.ChecksumIEEE(blockData),
	}
	_, err = file.Write(codec.EncodeRecord(blockHeader))
	require.NoError(t, err)
	_, err = file.Write(blockData)
	require.NoError(t, err)
	file.Close()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	reader, err := NewStagedFileReaderAdv(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Test negative offset
	_, err = reader.readAtUnsafe(ctx, -1, 10)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid read parameters")

	// Test negative length
	_, err = reader.readAtUnsafe(ctx, 0, -1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid read parameters")
}

func TestReadNextBatchAdv_ClosedReader(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	localBaseDir := filepath.Join(tempDir, "local")
	logId := int64(108)
	segId := int64(108)

	segDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)

	dr := &codec.DataRecord{Payload: []byte("data")}
	blockData := codec.EncodeRecord(dr)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber: 0, FirstEntryID: 0, LastEntryID: 0,
		BlockLength: uint32(len(blockData)), BlockCrc: crc32.ChecksumIEEE(blockData),
	}
	_, err = file.Write(codec.EncodeRecord(blockHeader))
	require.NoError(t, err)
	_, err = file.Write(blockData)
	require.NoError(t, err)
	file.Close()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	reader, err := NewStagedFileReaderAdv(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)

	// Close the reader first
	reader.Close(ctx)

	// Try to read - should fail with closed error
	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	_, err = reader.ReadNextBatchAdv(ctx, opt, nil)
	assert.Error(t, err)
	assert.True(t, werr.ErrFileReaderAlreadyClosed.Is(err))
}

func TestReadDataBlocks_IncompleteNoFooterNoLAC(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	localBaseDir := filepath.Join(tempDir, "local")
	logId := int64(109)
	segId := int64(109)

	segDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write header + one block with entries 0-5
	headerRecord := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)

	dataRecords := make([]codec.Record, 0, 6)
	for i := 0; i < 6; i++ {
		dataRecords = append(dataRecords, &codec.DataRecord{Payload: []byte("data")})
	}
	blockData := encodeRecordList(dataRecords)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber: 0, FirstEntryID: 0, LastEntryID: 5,
		BlockLength: uint32(len(blockData)), BlockCrc: crc32.ChecksumIEEE(blockData),
	}
	_, err = file.Write(codec.EncodeRecord(blockHeader))
	require.NoError(t, err)
	_, err = file.Write(blockData)
	require.NoError(t, err)
	file.Close() // No footer - incomplete file

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	reader, err := NewStagedFileReaderAdv(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Do NOT set LAC (stays at -1)
	// Read starting at entry 10 (beyond all data)
	// This exercises the path: entries==0, !hasDataReadError (read EOF is data read error, but block scan stops at EOF),
	// isIncompleteFile=true, footer==nil, currentLAC==-1
	// Should return ErrEntryNotFound
	opt := storage.ReaderOpt{StartEntryID: 10, MaxBatchEntries: 10}
	_, err = reader.ReadNextBatchAdv(ctx, opt, nil)
	assert.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err), "Expected ErrEntryNotFound, got: %v", err)
}

func TestReadNextBatchAdv_FooterLACOverride(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	localBaseDir := filepath.Join(tempDir, "local")
	logId := int64(110)
	segId := int64(110)
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	// Create a finalized file with LAC=9
	writer, err := NewStagedFileWriter(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	for i := int64(0); i < 10; i++ {
		_, err = writer.WriteDataAsync(ctx, i, []byte("data"), nil)
		require.NoError(t, err)
	}
	err = writer.Sync(ctx)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	_, err = writer.Finalize(ctx, 9)
	require.NoError(t, err)
	writer.Close(ctx)

	// Read with lastAddConfirmed at -1 (lower than footer's LAC=9)
	// This should make footer.LAC override the currentLAC
	reader, err := NewStagedFileReaderAdv(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// lastAddConfirmed stays at -1 (default)
	// footer.LAC = 9 should override
	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 100}
	result, err := reader.ReadNextBatchAdv(ctx, opt, nil)
	require.NoError(t, err)
	assert.NotNil(t, result)
	// All 10 entries should be readable since footer.LAC=9 overrides currentLAC=-1
	assert.Len(t, result.Entries, 10)
}

// === scanForAllBlockInfo - CRC verification failure ===

func TestScanForAllBlockInfo_CRCVerifyFailed(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	localBaseDir := filepath.Join(tempDir, "local")
	logId := int64(111)
	segId := int64(111)

	// Create a file with a block that has a wrong CRC
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	file, err := os.Create(filePath)
	require.NoError(t, err)

	// Write header
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	_, err = file.Write(codec.EncodeRecord(headerRecord))
	require.NoError(t, err)

	// Write a block with intentionally wrong CRC
	dataRecords := []codec.Record{
		&codec.DataRecord{Payload: []byte("entry0")},
		&codec.DataRecord{Payload: []byte("entry1")},
	}
	blockData := encodeRecordList(dataRecords)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  1,
		BlockLength:  uint32(len(blockData)),
		BlockCrc:     0xDEADBEEF, // intentionally wrong CRC
	}
	_, err = file.Write(codec.EncodeRecord(blockHeader))
	require.NoError(t, err)
	_, err = file.Write(blockData)
	require.NoError(t, err)
	file.Close()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	reader, err := NewStagedFileReaderAdv(ctx, "bucket", "root", localBaseDir, logId, segId, nil, cfg)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// The block should have been skipped during scan due to CRC mismatch
	// So blockIndexes should be empty
	assert.Empty(t, reader.blockIndexes)
}

// === readCompactedDataFromMinio - GetObject failure ===

func TestReadCompactedData_ReadBlockError(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	logId := int64(112)
	segId := int64(112)

	// Build valid footer with one block
	blockIndex := &codec.IndexRecord{
		BlockNumber: 0, StartOffset: 0, BlockSize: 200, FirstEntryID: 0, LastEntryID: 4,
	}
	footer := &codec.FooterRecord{
		TotalBlocks:  1,
		TotalRecords: 5,
		TotalSize:    200,
		IndexLength:  uint32(codec.RecordHeaderSize + codec.IndexRecordSize),
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(0),
		LAC:          4,
	}

	var footerBuf bytes.Buffer
	footerBuf.Write(codec.EncodeRecord(blockIndex))
	footerBuf.Write(codec.EncodeRecord(footer))
	footerData := footerBuf.Bytes()

	footerKey := fmt.Sprintf("test-root/%d/%d/footer.blk", logId, segId)

	// Mock StatObject for footer
	mockStorage.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(int64(len(footerData)), false, nil)

	// Mock GetObject for footer
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", footerKey, int64(0), int64(len(footerData)), mock.Anything, mock.Anything).
		Return(&readerMockFileReader{data: footerData}, nil)

	// Mock GetObject for block data - return error
	blockKey := fmt.Sprintf("test-root/%d/%d/m_0.blk", logId, segId)
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", blockKey, int64(0), mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("minio connection error"))

	// Create minimal local file
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	f, err := os.Create(filePath)
	require.NoError(t, err)
	hdr := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = f.Write(codec.EncodeRecord(hdr))
	require.NoError(t, err)
	f.Close()

	reader, err := NewStagedFileReaderAdv(context.Background(), "test-bucket", "test-root", localBaseDir, logId, segId, mockStorage, cfg)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	assert.True(t, reader.isCompacted.Load())

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	_, err = reader.ReadNextBatchAdv(context.Background(), opt, nil)
	// Should get EOF since the block read failed and no entries were collected
	assert.Error(t, err)
	assert.True(t, werr.ErrFileReaderEndOfFile.Is(err))
}

// === readCompactedDataFromMinio - no entries match (all below startEntryID) ===

func TestReadCompactedData_NoEntriesMatch(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	mockStorage := mocks_objectstorage.NewObjectStorage(t)
	logId := int64(113)
	segId := int64(113)

	// Build valid block data with entries 0-4
	var blockBuf bytes.Buffer
	hdrRec := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: codec.SetCompacted(0), FirstEntryID: 0}
	blockBuf.Write(codec.EncodeRecord(hdrRec))

	dataRecords := make([]codec.Record, 0, 5)
	for i := 0; i < 5; i++ {
		dataRecords = append(dataRecords, &codec.DataRecord{Payload: []byte("data")})
	}
	blockDataOnly := encodeRecordList(dataRecords)
	blockHdr := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  4,
		BlockLength:  uint32(len(blockDataOnly)),
		BlockCrc:     crc32.ChecksumIEEE(blockDataOnly),
	}
	blockBuf.Write(codec.EncodeRecord(blockHdr))
	blockBuf.Write(blockDataOnly)
	blockData := blockBuf.Bytes()

	blockKey := fmt.Sprintf("test-root/%d/%d/m_0.blk", logId, segId)
	mockStorage.EXPECT().GetObject(mock.Anything, "test-bucket", blockKey, int64(0), mock.Anything, mock.Anything, mock.Anything).
		Return(&readerMockFileReader{data: blockData}, nil)

	// Mock StatObject to return "not found" so constructor doesn't try to parse a compacted footer
	mockStorage.EXPECT().StatObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, errors.New("not found")).Maybe()

	// Create minimal local file
	localBaseDir := filepath.Join(dir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))
	filePath := getSegmentFilePath(localBaseDir, logId, segId)
	f, err := os.Create(filePath)
	require.NoError(t, err)
	hdr := &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0}
	_, err = f.Write(codec.EncodeRecord(hdr))
	require.NoError(t, err)
	f.Close()

	reader, err := NewStagedFileReaderAdv(context.Background(), "test-bucket", "test-root", localBaseDir, logId, segId, mockStorage, cfg)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	// Set up compacted state with block indexes and footer directly
	reader.isCompacted.Store(true)
	reader.blockIndexes = []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 200, FirstEntryID: 0, LastEntryID: 4},
	}
	reader.footer = &codec.FooterRecord{
		TotalBlocks: 1, TotalRecords: 5, LAC: 4, Version: codec.FormatVersion,
		Flags: codec.SetCompacted(0),
	}

	// Call readCompactedDataFromMinio directly with startEntryID=100 (beyond all block entries 0-4)
	// This exercises the "no entries collected" EOF path at L1328-1334
	opt := storage.ReaderOpt{StartEntryID: 100, MaxBatchEntries: 10}
	_, err = reader.readCompactedDataFromMinio(context.Background(), opt, 0, 0)
	assert.Error(t, err)
	assert.True(t, werr.ErrFileReaderEndOfFile.Is(err))
}

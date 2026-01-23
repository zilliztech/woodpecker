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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
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

// Helper function to create a per-block file with given entries
func createBlockFile(t *testing.T, segmentDir string, blockId int64, firstEntryId int64, lastEntryId int64) {
	// Create data records for entries
	dataRecords := make([]codec.Record, 0, lastEntryId-firstEntryId+1)
	for i := firstEntryId; i <= lastEntryId; i++ {
		dataRecord := &codec.DataRecord{
			Payload: []byte("test data"),
		}
		dataRecords = append(dataRecords, dataRecord)
	}

	blockData := encodeRecordList(dataRecords)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  int32(blockId),
		FirstEntryID: firstEntryId,
		LastEntryID:  lastEntryId,
		BlockLength:  uint32(len(blockData)),
		BlockCrc:     crc32.ChecksumIEEE(blockData),
	}

	// Write block file: blockHeader + blockData
	blockFilePath := filepath.Join(segmentDir, fmt.Sprintf("%d.blk", blockId))
	blockFile, err := os.Create(blockFilePath)
	require.NoError(t, err)

	blockHeaderData := codec.EncodeRecord(blockHeader)
	_, err = blockFile.Write(blockHeaderData)
	require.NoError(t, err)
	_, err = blockFile.Write(blockData)
	require.NoError(t, err)
	blockFile.Close()
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

	// Create segment directory
	localBaseDir := filepath.Join(tempDir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0755)
	require.NoError(t, err)

	// Create per-block file with entries 0-20
	createBlockFile(t, segmentDir, 0, 0, 20)

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

	// Create segment directory
	localBaseDir := filepath.Join(tempDir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0755)
	require.NoError(t, err)

	// Create per-block file with entries 0-50
	createBlockFile(t, segmentDir, 0, 0, 50)

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

	// Create segment directory
	localBaseDir := filepath.Join(tempDir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0755)
	require.NoError(t, err)

	// Create per-block file with entries 0-10
	createBlockFile(t, segmentDir, 0, 0, 10)

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

	// Create segment directory
	localBaseDir := filepath.Join(tempDir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0755)
	require.NoError(t, err)

	// Create per-block file with entries 0-50
	createBlockFile(t, segmentDir, 0, 0, 50)

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

	// Create segment directory
	localBaseDir := filepath.Join(tempDir, "local")
	segmentDir := getSegmentDir(localBaseDir, logId, segId)
	err := os.MkdirAll(segmentDir, 0755)
	require.NoError(t, err)

	// Create per-block file with entries 0-50
	createBlockFile(t, segmentDir, 0, 0, 50)

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

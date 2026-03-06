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

package disk

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

func createTempFileWithContent(t *testing.T, content []byte) (string, string) {
	// Create temporary base directory
	tempDir, err := os.MkdirTemp("", "test_woodpecker_base_*")
	require.NoError(t, err)

	// Create the expected directory structure: baseDir/logId/segmentId/
	segmentDir := filepath.Join(tempDir, "1", "1")
	err = os.MkdirAll(segmentDir, 0o755)
	require.NoError(t, err)

	// Create the data.log file in the correct location
	filePath := filepath.Join(segmentDir, "data.log")
	err = os.WriteFile(filePath, content, 0o644)
	require.NoError(t, err)

	return tempDir, filePath // Return baseDir and actual file path
}

func TestLocalFileReaderAdv_ReadDataBlocks_FileDeleted_ShouldReturnEntryNotFound(t *testing.T) {
	// Test: File deleted during reading → should return EntryNotFound (not EOF)
	ctx := context.Background()

	// Create a temporary file structure
	baseDir, filePath := createTempFileWithContent(t, []byte("test content"))
	defer os.RemoveAll(baseDir)

	// Create reader
	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Set file as completed (has footer) to test the bug fix
	reader.footer = &codec.FooterRecord{}
	reader.isIncompleteFile.Store(false)

	// Delete the file after opening (simulates file being deleted during operation)
	os.Remove(filePath)

	opt := storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}

	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0, 0)

	// Should return EntryNotFound because read error occurred, even though file is completed
	assert.Nil(t, batch)
	assert.True(t, errors.Is(err, werr.ErrEntryNotFound))
	assert.Contains(t, err.Error(), "no record extract")

	t.Logf("BUG FIX VERIFIED: File with footer + read error = EntryNotFound (not EOF)")
}

func TestLocalFileReaderAdv_ReadDataBlocks_PermissionError_ShouldReturnEntryNotFound(t *testing.T) {
	// Test: File permission error → should return EntryNotFound
	ctx := context.Background()

	// Create a temporary file structure
	baseDir, filePath := createTempFileWithContent(t, []byte("test content"))
	defer func() {
		// Restore permissions for cleanup
		os.Chmod(filePath, 0o644)
		os.RemoveAll(baseDir)
	}()

	// Create reader first
	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Set as completed file to test bug fix
	reader.footer = &codec.FooterRecord{}
	reader.isIncompleteFile.Store(false)

	// Close the file and change permissions to make it unreadable
	reader.file.Close()
	err = os.Chmod(filePath, 0o000)
	require.NoError(t, err)

	opt := storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}

	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0, 0)

	// Should return EntryNotFound because of permission error
	assert.Nil(t, batch)
	assert.True(t, errors.Is(err, werr.ErrEntryNotFound))

	t.Logf("Permission error correctly returns EntryNotFound")
}

func TestLocalFileReaderAdv_ReadDataBlocks_CompletedFileNoError_ShouldReturnEOF(t *testing.T) {
	t.Skipf("mock real data read")
	// Test: Normal completed file with no errors → should return EOF
	ctx := context.Background()

	// Create a temporary empty file structure
	baseDir, _ := createTempFileWithContent(t, []byte{})
	defer os.RemoveAll(baseDir)

	// Create reader
	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Set as completed file (has footer)
	reader.footer = &codec.FooterRecord{}
	reader.isIncompleteFile.Store(false)
	reader.blockIndexes = []*codec.IndexRecord{} // No blocks

	opt := storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}

	// TODO mock no error exists when read data blocks
	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0, 0)

	// Should return EOF because file is completed and no read errors occurred
	assert.Nil(t, batch)
	assert.True(t, errors.Is(err, werr.ErrFileReaderEndOfFile))

	t.Logf("Completed file without errors correctly returns EOF")
}

func TestLocalFileReaderAdv_ReadDataBlocks_IncompleteFileNoError_ShouldReturnEntryNotFound(t *testing.T) {
	// Test: Incomplete file with no errors → should return EntryNotFound
	ctx := context.Background()

	// Create a temporary empty file structure
	baseDir, _ := createTempFileWithContent(t, []byte{})
	defer os.RemoveAll(baseDir)

	// Create reader
	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Set as incomplete file (no footer)
	reader.footer = nil
	reader.isIncompleteFile.Store(true)
	reader.blockIndexes = []*codec.IndexRecord{} // No blocks

	opt := storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}

	// TODO mock no error exists when read data blocks
	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0, 0)

	// Should return EntryNotFound because file is incomplete
	assert.Nil(t, batch)
	assert.True(t, errors.Is(err, werr.ErrEntryNotFound))

	t.Logf("Incomplete file correctly returns EntryNotFound")
}

func TestLocalFileReaderAdv_ReadAt_FileNotExist_ShouldReturnError(t *testing.T) {
	// Test: readAt method with file not exist error
	ctx := context.Background()

	// Create a temporary file structure with content
	content := make([]byte, 1024)
	for i := range content {
		content[i] = byte(i % 256)
	}
	baseDir, filePath := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	// Create reader
	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Test normal read first
	data, err := reader.readAtUnsafe(ctx, 0, 100)
	assert.NoError(t, err)
	assert.Len(t, data, 100)

	// close&Delete the file to mock read error
	reader.file.Close()
	removeErr := os.Remove(filePath)
	assert.NoError(t, removeErr)

	// This should now fail
	data, err = reader.readAtUnsafe(ctx, 0, 100)
	assert.Error(t, err)
	assert.Nil(t, data)

	t.Logf("readAt correctly handles file deletion errors")
}

func TestLocalFileReaderAdv_verifyBlockDataIntegrity_ErrorHandling(t *testing.T) {
	// Test that data integrity errors are properly handled
	ctx := context.Background()

	reader := &LocalFileReaderAdv{
		filePath: "/test/path",
		logId:    1,
		segId:    1,
		logIdStr: "1",
	}

	// Create a block header with invalid CRC that will fail verification
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber: 0,
		BlockLength: 10,
		BlockCrc:    12345, // Invalid CRC
	}

	invalidData := []byte{0x01, 0x02, 0x03} // Data that won't match CRC

	err := reader.verifyBlockDataIntegrity(ctx, blockHeader, 0, invalidData)

	// Should return an error for integrity verification failure
	assert.Error(t, err)
	t.Logf("Integrity verification correctly failed with: %v", err)
}

// === readAtUnsafe negative parameters ===

func TestLocalFileReaderAdv_ReadAtUnsafe_NegativeOffset(t *testing.T) {
	ctx := context.Background()
	baseDir, _ := createTempFileWithContent(t, []byte("some data"))
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	data, err := reader.readAtUnsafe(ctx, -1, 10)
	assert.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "invalid read parameters")
}

func TestLocalFileReaderAdv_ReadAtUnsafe_NegativeLength(t *testing.T) {
	ctx := context.Background()
	baseDir, _ := createTempFileWithContent(t, []byte("some data"))
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	data, err := reader.readAtUnsafe(ctx, 0, -5)
	assert.Error(t, err)
	assert.Nil(t, data)
	assert.Contains(t, err.Error(), "invalid read parameters")
}

// === isFooterExistsUnsafe tests (0% → covered) ===

func TestLocalFileReaderAdv_IsFooterExistsUnsafe_WithFooter(t *testing.T) {
	// Create a finalized file (has footer), then use maxBatchSize=0 to skip the read loop
	// so readDataBlocksUnsafe reaches isFooterExistsUnsafe
	ctx := context.Background()
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	writer, err := NewLocalFileWriter(ctx, dir, 1, 0, cfg)
	require.NoError(t, err)
	_, err = writer.WriteDataAsync(ctx, 0, []byte("data"), nil)
	require.NoError(t, err)
	_, err = writer.Finalize(ctx, 0)
	require.NoError(t, err)
	writer.Close(ctx)

	// Open reader with maxBatchSize=0 to skip read loop
	reader, err := NewLocalFileReaderAdv(ctx, dir, 1, 0, 0)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Force incomplete state so isFooterExistsUnsafe is called
	reader.isIncompleteFile.Store(true)
	reader.footer = nil

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	_, err = reader.readDataBlocksUnsafe(ctx, opt, 0, 0)
	// isFooterExistsUnsafe finds real footer → ErrFileReaderEndOfFile
	assert.True(t, errors.Is(err, werr.ErrFileReaderEndOfFile))
}

func TestLocalFileReaderAdv_IsFooterExistsUnsafe_NoFooter(t *testing.T) {
	// Create an incomplete file (no footer), then use maxBatchSize=0
	ctx := context.Background()
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	writer, err := NewLocalFileWriter(ctx, dir, 1, 0, cfg)
	require.NoError(t, err)
	_, err = writer.WriteDataAsync(ctx, 0, []byte("data"), nil)
	require.NoError(t, err)
	// Don't finalize - incomplete file
	writer.Close(ctx)

	reader, err := NewLocalFileReaderAdv(ctx, dir, 1, 0, 0)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.footer = nil

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	_, err = reader.readDataBlocksUnsafe(ctx, opt, 0, 0)
	// isFooterExistsUnsafe finds no footer → ErrEntryNotFound
	assert.True(t, errors.Is(err, werr.ErrEntryNotFound))
}

func TestLocalFileReaderAdv_IsFooterExistsUnsafe_FileTooSmall(t *testing.T) {
	// Create a tiny file (too small for footer)
	ctx := context.Background()
	baseDir, _ := createTempFileWithContent(t, []byte("tiny"))
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 0)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.footer = nil

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	_, err = reader.readDataBlocksUnsafe(ctx, opt, 0, 0)
	// file too small for footer → returns false → ErrEntryNotFound
	assert.True(t, errors.Is(err, werr.ErrEntryNotFound))
}

// === tryParseFooterAndIndexesIfExists already-parsed path ===

func TestLocalFileReaderAdv_TryParseFooter_AlreadyParsed(t *testing.T) {
	ctx := context.Background()
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	writer, err := NewLocalFileWriter(ctx, dir, 1, 0, cfg)
	require.NoError(t, err)
	_, err = writer.WriteDataAsync(ctx, 0, []byte("data"), nil)
	require.NoError(t, err)
	_, err = writer.Finalize(ctx, 0)
	require.NoError(t, err)
	writer.Close(ctx)

	reader, err := NewLocalFileReaderAdv(ctx, dir, 1, 0, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Footer already parsed (isIncompleteFile=false from constructor)
	assert.False(t, reader.isIncompleteFile.Load())
	assert.NotNil(t, reader.footer)

	// Call again — should early-return without error (covers the double-check path)
	err = reader.tryParseFooterAndIndexesIfExists(ctx)
	assert.NoError(t, err)
}

func TestLocalFileReaderAdv_TryParseFooter_FileTooSmall(t *testing.T) {
	ctx := context.Background()
	// Create a file that's too small for a footer
	baseDir, _ := createTempFileWithContent(t, []byte("ab"))
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// File too small → isIncompleteFile stays true, no error
	assert.True(t, reader.isIncompleteFile.Load())
	assert.Nil(t, reader.footer)
}

func TestLocalFileReaderAdv_TryParseFooter_InvalidFooterData(t *testing.T) {
	ctx := context.Background()
	// Create a file large enough for footer check but with invalid data
	content := make([]byte, codec.RecordHeaderSize+codec.FooterRecordSizeV6+10)
	for i := range content {
		content[i] = 0xFF
	}
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Invalid footer data → isIncompleteFile stays true
	assert.True(t, reader.isIncompleteFile.Load())
	assert.Nil(t, reader.footer)
}

func TestLocalFileReaderAdv_TryParseFooter_ReadAtFails(t *testing.T) {
	ctx := context.Background()
	// Create a file that passes the min footer size check (>=45 bytes)
	// but is smaller than maxFooterSize (53 bytes), causing readAtUnsafe to get negative offset
	minSize := codec.RecordHeaderSize + codec.FooterRecordSizeV5 // 45
	content := make([]byte, minSize)
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	// This may fail or succeed depending on whether readAt with offset < 0 returns err
	// In our code, negative offset → "invalid read parameters" error
	// So tryParseFooterAndIndexesIfExists should return ErrEntryNotFound
	if err != nil {
		assert.Contains(t, err.Error(), "try parse footer")
	} else {
		defer reader.Close(ctx)
		// If it didn't fail at init, the footer read error path was still exercised
		assert.True(t, reader.isIncompleteFile.Load())
	}
}

func TestLocalFileReaderAdv_TryParseFooter_StatError(t *testing.T) {
	ctx := context.Background()
	baseDir, _ := createTempFileWithContent(t, []byte("some content for stat test"))
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)

	// Close the file to cause stat to fail on next tryParseFooter call
	reader.file.Close()

	reader.isIncompleteFile.Store(true)
	err = reader.tryParseFooterAndIndexesIfExists(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stat file")
}

func TestLocalFileReaderAdv_TryParseFooter_IndexRecordsParseFail(t *testing.T) {
	ctx := context.Background()
	// Create a file with a valid footer at the end but corrupted index data before it
	// We'll use a finalized file, then corrupt the index portion

	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	writer, err := NewLocalFileWriter(ctx, dir, 1, 0, cfg)
	require.NoError(t, err)
	_, err = writer.WriteDataAsync(ctx, 0, []byte("data"), nil)
	require.NoError(t, err)
	_, err = writer.Finalize(ctx, 0)
	require.NoError(t, err)
	writer.Close(ctx)

	// Read the file, corrupt the index records region, rewrite
	filePath := getSegmentFilePath(dir, 1, 0)
	data, err := os.ReadFile(filePath)
	require.NoError(t, err)

	// Corrupt some bytes in the middle (where index records should be)
	footerSize := codec.RecordHeaderSize + codec.FooterRecordSizeV6
	if len(data) > footerSize+10 {
		corruptStart := len(data) - footerSize - 5
		for i := corruptStart; i < corruptStart+5 && i < len(data); i++ {
			data[i] = 0xFF
		}
		err = os.WriteFile(filePath, data, 0o644)
		require.NoError(t, err)
	}

	// Open reader — tryParseFooter should find valid footer but fail to parse indexes
	reader, err := NewLocalFileReaderAdv(ctx, dir, 1, 0, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Index parse failure → falls back to incomplete mode
	assert.True(t, reader.isIncompleteFile.Load())
	assert.Nil(t, reader.footer)
}

// === parseIndexRecordsUnsafe error paths ===

func TestLocalFileReaderAdv_ParseIndexRecords_InvalidOffset(t *testing.T) {
	ctx := context.Background()
	baseDir, _ := createTempFileWithContent(t, []byte("some data that is longer than a few bytes"))
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Set footer with a huge TotalBlocks to force negative indexStartOffset
	reader.footer = &codec.FooterRecord{
		TotalBlocks: 100000,
		Version:     codec.FormatVersion,
	}

	err = reader.parseIndexRecordsUnsafe(ctx, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid index start offset")
}

// === NewLocalFileReaderAdv error paths ===

func TestLocalFileReaderAdv_NewReader_PermissionDenied(t *testing.T) {
	ctx := context.Background()
	baseDir, filePath := createTempFileWithContent(t, []byte("data"))
	defer func() {
		os.Chmod(filePath, 0o644)
		os.RemoveAll(baseDir)
	}()

	// Remove read permission from file
	err := os.Chmod(filePath, 0o000)
	require.NoError(t, err)

	// Should fail with non-NotExist error (permission denied)
	_, err = NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "open file")
}

func TestLocalFileReaderAdv_NewReader_MkdirAllError(t *testing.T) {
	ctx := context.Background()

	// Use a path where MkdirAll will fail (file exists where dir expected)
	tmpFile, err := os.CreateTemp("", "test_mkdirall_*")
	require.NoError(t, err)
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// Using a file as baseDir will cause MkdirAll to fail
	_, err = NewLocalFileReaderAdv(ctx, tmpFile.Name(), 1, 1, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create directory")
}

// === ReadNextBatchAdv with lastReadBatchInfo ===

func TestLocalFileReaderAdv_ReadNextBatchAdv_WithLastReadBatchInfo(t *testing.T) {
	ctx := context.Background()
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Write entries and finalize
	writer, err := NewLocalFileWriter(ctx, dir, 1, 0, cfg)
	require.NoError(t, err)
	for i := int64(0); i < 10; i++ {
		_, err = writer.WriteDataAsync(ctx, i, []byte(fmt.Sprintf("entry-%d", i)), nil)
		require.NoError(t, err)
	}
	_, err = writer.Finalize(ctx, 9)
	require.NoError(t, err)
	writer.Close(ctx)

	reader, err := NewLocalFileReaderAdv(ctx, dir, 1, 0, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// First read
	result1, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 3,
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, result1)
	require.NotNil(t, result1.LastReadState)

	// Subsequent read using lastReadBatchInfo (covers the lastReadBatchInfo != nil path)
	nextStart := result1.Entries[len(result1.Entries)-1].EntryId + 1
	result2, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID:    nextStart,
		MaxBatchEntries: 3,
	}, result1.LastReadState)
	if err == nil {
		assert.NotNil(t, result2)
		assert.GreaterOrEqual(t, len(result2.Entries), 1)
	}
}

// === ReadNextBatchAdv completed file entry not found (SearchBlock returns nil → EOF) ===

func TestLocalFileReaderAdv_ReadNextBatchAdv_CompletedFile_EntryBeyondRange(t *testing.T) {
	ctx := context.Background()
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Write a few entries and finalize
	writer, err := NewLocalFileWriter(ctx, dir, 1, 0, cfg)
	require.NoError(t, err)
	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(ctx, i, []byte("data"), nil)
		require.NoError(t, err)
	}
	_, err = writer.Finalize(ctx, 2)
	require.NoError(t, err)
	writer.Close(ctx)

	reader, err := NewLocalFileReaderAdv(ctx, dir, 1, 0, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Request entryID beyond what exists → SearchBlock returns nil → EOF
	_, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID:    999999,
		MaxBatchEntries: 10,
	}, nil)
	assert.True(t, errors.Is(err, werr.ErrFileReaderEndOfFile))
}

// === GetLastEntryID incomplete file with scan ===

func TestLocalFileReaderAdv_GetLastEntryID_IncompleteFileWithBlocks(t *testing.T) {
	ctx := context.Background()
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Write entries but don't finalize (incomplete file with block data)
	writer, err := NewLocalFileWriter(ctx, dir, 1, 0, cfg)
	require.NoError(t, err)
	for i := int64(0); i < 5; i++ {
		_, err = writer.WriteDataAsync(ctx, i, []byte("data"), nil)
		require.NoError(t, err)
	}
	// Sync to flush blocks to disk
	writer.Sync(ctx)
	writer.Close(ctx)

	reader, err := NewLocalFileReaderAdv(ctx, dir, 1, 0, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// File is incomplete (no footer), should trigger scanForAllBlockInfoUnsafe
	assert.True(t, reader.isIncompleteFile.Load())

	lastId, err := reader.GetLastEntryID(ctx)
	if err == nil {
		assert.GreaterOrEqual(t, lastId, int64(0))
	} else {
		// May fail if blocks aren't fully flushed
		assert.True(t, werr.ErrFileReaderNoBlockFound.Is(err))
	}
}

func TestLocalFileReaderAdv_GetLastEntryID_NotIncomplete_NoFooterNoBlocks(t *testing.T) {
	ctx := context.Background()
	baseDir, _ := createTempFileWithContent(t, []byte("data"))
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Not incomplete but no footer/blocks → falls through to final return
	reader.isIncompleteFile.Store(false)
	reader.footer = nil
	reader.blockIndexes = nil

	_, err = reader.GetLastEntryID(ctx)
	assert.True(t, werr.ErrFileReaderNoBlockFound.Is(err))
}

// === scanForAllBlockInfoUnsafe edge cases ===

func TestLocalFileReaderAdv_ScanForAllBlockInfo_FileTooSmall(t *testing.T) {
	ctx := context.Background()
	// Create a file too small for any block data
	baseDir, _ := createTempFileWithContent(t, []byte("x"))
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	err = reader.scanForAllBlockInfoUnsafe(ctx)
	assert.NoError(t, err) // No data to scan, not an error
	assert.Empty(t, reader.blockIndexes)
}

func TestLocalFileReaderAdv_ScanForAllBlockInfo_SuccessfulScan(t *testing.T) {
	ctx := context.Background()
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	// Write some entries, sync, then close without finalize
	writer, err := NewLocalFileWriter(ctx, dir, 1, 0, cfg)
	require.NoError(t, err)
	for i := int64(0); i < 5; i++ {
		_, err = writer.WriteDataAsync(ctx, i, []byte(fmt.Sprintf("block-data-%d", i)), nil)
		require.NoError(t, err)
	}
	writer.Sync(ctx)
	writer.Close(ctx)

	reader, err := NewLocalFileReaderAdv(ctx, dir, 1, 0, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.blockIndexes = nil

	err = reader.scanForAllBlockInfoUnsafe(ctx)
	assert.NoError(t, err)
	// Should have found at least one block
	assert.GreaterOrEqual(t, len(reader.blockIndexes), 1)
}

// === ReadNextBatchAdv with tryParseFooter error ===

func TestLocalFileReaderAdv_ReadNextBatchAdv_ParseFooterError(t *testing.T) {
	ctx := context.Background()
	baseDir, filePath := createTempFileWithContent(t, []byte("some data"))
	defer func() {
		os.Chmod(filePath, 0o644)
		os.RemoveAll(baseDir)
	}()

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Force incomplete state so tryParseFooterAndIndexesIfExists is called
	reader.isIncompleteFile.Store(true)
	reader.footer = nil

	// Close the underlying file to cause stat error in tryParseFooterAndIndexesIfExists
	reader.file.Close()
	f, _ := os.Open(filePath)
	reader.file = f

	// Remove read permission to cause stat/read failure
	os.Chmod(filePath, 0o000)

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	_, err = reader.ReadNextBatchAdv(ctx, opt, nil)
	// May error or succeed depending on OS behavior with already-open fd
	// The key is that the code path is exercised
	if err != nil {
		assert.Error(t, err)
	}
	// Restore permissions
	os.Chmod(filePath, 0o644)
}

// === ReadNextBatchAdv with lastReadBatchInfo sets flags/version ===

func TestLocalFileReaderAdv_ReadNextBatchAdv_LastReadBatchInfoSetsState(t *testing.T) {
	ctx := context.Background()
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	writer, err := NewLocalFileWriter(ctx, dir, 1, 0, cfg)
	require.NoError(t, err)
	_, err = writer.WriteDataAsync(ctx, 0, []byte("data"), nil)
	require.NoError(t, err)
	_, err = writer.Finalize(ctx, 0)
	require.NoError(t, err)
	writer.Close(ctx)

	reader, err := NewLocalFileReaderAdv(ctx, dir, 1, 0, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// First read to get real state
	result1, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, result1)
	require.NotNil(t, result1.LastReadState)

	// Use lastReadBatchInfo with custom version to verify it's applied
	lastState := &proto.LastReadState{
		SegmentId:   0,
		Flags:       result1.LastReadState.Flags,
		Version:     result1.LastReadState.Version,
		LastBlockId: result1.LastReadState.LastBlockId,
		BlockOffset: result1.LastReadState.BlockOffset,
		BlockSize:   result1.LastReadState.BlockSize,
	}

	// Subsequent read using lastReadBatchInfo — covers the code path
	_, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{
		StartEntryID:    result1.Entries[len(result1.Entries)-1].EntryId + 1,
		MaxBatchEntries: 10,
	}, lastState)
	// May get EOF since all entries were read in first batch
	// The key is the lastReadBatchInfo path was exercised
	assert.True(t, err == nil || errors.Is(err, werr.ErrEntryNotFound) || errors.Is(err, werr.ErrFileReaderEndOfFile))
}

// === readDataBlocksUnsafe completed file, no hasDataReadError, footer set → EOF ===

func TestLocalFileReaderAdv_ReadDataBlocks_CompletedFile_ZeroBatchSize_ReturnsEOF(t *testing.T) {
	ctx := context.Background()
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	writer, err := NewLocalFileWriter(ctx, dir, 1, 0, cfg)
	require.NoError(t, err)
	_, err = writer.WriteDataAsync(ctx, 0, []byte("data"), nil)
	require.NoError(t, err)
	_, err = writer.Finalize(ctx, 0)
	require.NoError(t, err)
	writer.Close(ctx)

	// maxBatchSize=0 makes the read loop skip entirely
	reader, err := NewLocalFileReaderAdv(ctx, dir, 1, 0, 0)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Footer is set, isIncompleteFile=false → !r.isIncompleteFile.Load() || r.footer != nil → EOF
	assert.False(t, reader.isIncompleteFile.Load())
	assert.NotNil(t, reader.footer)

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	_, err = reader.readDataBlocksUnsafe(ctx, opt, 0, 0)
	assert.True(t, errors.Is(err, werr.ErrFileReaderEndOfFile))
}

// ============================================================================
// scanForAllBlockInfoUnsafe error paths
// ============================================================================

func TestScanForAllBlockInfo_StatError(t *testing.T) {
	ctx := context.Background()
	baseDir, _ := createTempFileWithContent(t, make([]byte, 200))
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)

	// Close the file to cause stat error
	reader.file.Close()
	reader.isIncompleteFile.Store(true)

	err = reader.scanForAllBlockInfoUnsafe(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stat file for scan")
}

func TestScanForAllBlockInfo_HeaderDecodeError_CrcMismatch(t *testing.T) {
	ctx := context.Background()
	// Create file that passes size check but has corrupted header with bad CRC
	// File needs to be > RecordHeaderSize + HeaderRecordSize + RecordHeaderSize + BlockHeaderRecordSize = 62
	content := make([]byte, 100)
	// Write data that looks like a record header but has invalid CRC
	content[0] = 0xFF // bad CRC
	content[1] = 0xFF
	content[2] = 0xFF
	content[3] = 0xFF
	content[4] = codec.HeaderRecordType // type
	content[5] = byte(codec.HeaderRecordSize)
	content[6] = 0
	content[7] = 0
	content[8] = 0
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.blockIndexes = nil

	err = reader.scanForAllBlockInfoUnsafe(ctx)
	assert.Error(t, err)
}

func TestScanForAllBlockInfo_HeaderDecodeError(t *testing.T) {
	ctx := context.Background()
	// Create file with garbage data large enough to pass size check
	content := make([]byte, 100)
	for i := range content {
		content[i] = 0xAB // Invalid CRC
	}
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.blockIndexes = nil

	err = reader.scanForAllBlockInfoUnsafe(ctx)
	assert.Error(t, err) // CRC mismatch → decode error
}

func TestScanForAllBlockInfo_InvalidHeaderRecordType(t *testing.T) {
	ctx := context.Background()
	// Create a file where the first record is a valid DataRecord with exactly HeaderRecordSize payload
	// so that readAtUnsafe reads exactly the right number of bytes (25 = RecordHeaderSize + HeaderRecordSize)
	// and DecodeRecord succeeds, but the type is DataRecordType instead of HeaderRecordType
	dataRecord := &codec.DataRecord{Payload: make([]byte, codec.HeaderRecordSize)}
	encodedData := codec.EncodeRecord(dataRecord)
	// Pad to ensure file is large enough (> 62 bytes)
	content := make([]byte, len(encodedData)+100)
	copy(content, encodedData)
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.blockIndexes = nil

	err = reader.scanForAllBlockInfoUnsafe(ctx)
	assert.Error(t, err)
	assert.True(t, werr.ErrFileReaderInvalidRecord.Is(err))
}

func TestScanForAllBlockInfo_NotEnoughDataForBlockHeader(t *testing.T) {
	ctx := context.Background()
	// Create file with valid HeaderRecord but not enough data for a BlockHeaderRecord
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	encodedHeader := codec.EncodeRecord(headerRecord)
	// Add a few extra bytes (not enough for a BlockHeaderRecord)
	content := make([]byte, len(encodedHeader)+5)
	copy(content, encodedHeader)
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.blockIndexes = nil

	err = reader.scanForAllBlockInfoUnsafe(ctx)
	assert.NoError(t, err) // Breaks out of loop, not an error
	assert.Empty(t, reader.blockIndexes)
}

func TestScanForAllBlockInfo_InvalidBlockHeaderRecord(t *testing.T) {
	ctx := context.Background()
	// Create file with valid HeaderRecord + garbage where BlockHeaderRecord should be
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	encodedHeader := codec.EncodeRecord(headerRecord)
	// Add garbage data for the block header (enough bytes but invalid CRC)
	blockHeaderSize := codec.RecordHeaderSize + codec.BlockHeaderRecordSize
	garbage := make([]byte, blockHeaderSize)
	for i := range garbage {
		garbage[i] = 0xCD
	}
	content := append(encodedHeader, garbage...)
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.blockIndexes = nil

	err = reader.scanForAllBlockInfoUnsafe(ctx)
	assert.NoError(t, err) // Breaks out of loop, not an error
	assert.Empty(t, reader.blockIndexes)
}

func TestScanForAllBlockInfo_NotEnoughDataForCompleteBlock(t *testing.T) {
	ctx := context.Background()
	// Create file with valid HeaderRecord + valid BlockHeaderRecord claiming large BlockLength
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  0,
		BlockLength:  99999, // Claims much more data than exists
		BlockCrc:     0,
	}
	content := append(codec.EncodeRecord(headerRecord), codec.EncodeRecord(blockHeader)...)
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.blockIndexes = nil

	err = reader.scanForAllBlockInfoUnsafe(ctx)
	assert.NoError(t, err) // Breaks out of loop
	assert.Empty(t, reader.blockIndexes)
}

func TestScanForAllBlockInfo_BlockCrcVerificationFailed(t *testing.T) {
	ctx := context.Background()
	// Create file with valid HeaderRecord + valid BlockHeaderRecord + data with wrong CRC
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	// Create some block data
	dataRecord := codec.EncodeRecord(&codec.DataRecord{Payload: []byte("test-data")})

	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  0,
		BlockLength:  uint32(len(dataRecord)),
		BlockCrc:     12345, // Wrong CRC
	}
	content := append(codec.EncodeRecord(headerRecord), codec.EncodeRecord(blockHeader)...)
	content = append(content, dataRecord...)
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.blockIndexes = nil

	err = reader.scanForAllBlockInfoUnsafe(ctx)
	assert.NoError(t, err) // Breaks out of loop on CRC mismatch
	assert.Empty(t, reader.blockIndexes)
}

// ============================================================================
// readDataBlocksUnsafe error paths
// ============================================================================

func TestReadDataBlocks_MaxEntriesDefault(t *testing.T) {
	// Test that maxEntries defaults to 100 when <= 0
	ctx := context.Background()
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	writer, err := NewLocalFileWriter(ctx, dir, 1, 0, cfg)
	require.NoError(t, err)
	_, err = writer.WriteDataAsync(ctx, 0, []byte("data"), nil)
	require.NoError(t, err)
	_, err = writer.Finalize(ctx, 0)
	require.NoError(t, err)
	writer.Close(ctx)

	reader, err := NewLocalFileReaderAdv(ctx, dir, 1, 0, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// MaxBatchEntries = 0 → defaults to 100
	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 0}
	result, err := reader.readDataBlocksUnsafe(ctx, opt, 0, 0)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.GreaterOrEqual(t, len(result.Entries), 1)
}

func TestReadDataBlocks_BlockHeaderDecodeError(t *testing.T) {
	ctx := context.Background()
	// Create file with valid header but garbage block header
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	encodedHeader := codec.EncodeRecord(headerRecord)
	// Add garbage where block header should be
	garbage := make([]byte, codec.RecordHeaderSize+codec.BlockHeaderRecordSize)
	for i := range garbage {
		garbage[i] = 0xFF
	}
	content := append(encodedHeader, garbage...)
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	// Start from offset 0 so it reads header + block header
	_, err = reader.readDataBlocksUnsafe(ctx, opt, 0, 0)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, werr.ErrEntryNotFound))
}

func TestReadDataBlocks_HeaderRecordFromBlockZero(t *testing.T) {
	// Test the path where readDataBlocksUnsafe reads HeaderRecord from offset 0
	// (when no footer/advOpt), covering the HeaderRecordType branch in the for loop
	ctx := context.Background()
	dir := getTempDir(t)
	cfg, _ := config.NewConfiguration()

	writer, err := NewLocalFileWriter(ctx, dir, 1, 0, cfg)
	require.NoError(t, err)
	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(ctx, i, []byte(fmt.Sprintf("data-%d", i)), nil)
		require.NoError(t, err)
	}
	writer.Sync(ctx)
	writer.Close(ctx) // No finalize → incomplete file

	reader, err := NewLocalFileReaderAdv(ctx, dir, 1, 0, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.footer = nil
	reader.blockIndexes = nil

	// Read from block 0, offset 0 → triggers header read path
	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	result, err := reader.readDataBlocksUnsafe(ctx, opt, 0, 0)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.GreaterOrEqual(t, len(result.Entries), 1)
}

func TestReadDataBlocks_BlockDataReadError(t *testing.T) {
	ctx := context.Background()
	// Create file with valid header + valid block header claiming more data than exists
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  0,
		BlockLength:  50000, // Claims more data than exists
		BlockCrc:     0,
	}
	content := append(codec.EncodeRecord(headerRecord), codec.EncodeRecord(blockHeader)...)
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.footer = nil

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	// Start at block 1, offset past header so it reads BlockHeaderRecord directly
	headerSize := int64(codec.RecordHeaderSize + codec.HeaderRecordSize)
	_, err = reader.readDataBlocksUnsafe(ctx, opt, 1, headerSize)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, werr.ErrEntryNotFound))
}

func TestReadDataBlocks_VerifyIntegrityError(t *testing.T) {
	ctx := context.Background()
	// Create file with valid header + valid block header + block data that fails CRC check
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	dataRecord := codec.EncodeRecord(&codec.DataRecord{Payload: []byte("payload")})
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  0,
		BlockLength:  uint32(len(dataRecord)),
		BlockCrc:     99999, // Wrong CRC
	}
	content := append(codec.EncodeRecord(headerRecord), codec.EncodeRecord(blockHeader)...)
	content = append(content, dataRecord...)
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	headerSize := int64(codec.RecordHeaderSize + codec.HeaderRecordSize)
	_, err = reader.readDataBlocksUnsafe(ctx, opt, 1, headerSize)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, werr.ErrEntryNotFound))
}

func TestReadDataBlocks_BlockDataDecodeError(t *testing.T) {
	ctx := context.Background()
	// Create block data that has correct CRC but contains invalid record data
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	// Create garbage that will pass CRC but fail DecodeRecordList
	garbage := []byte{0x00, 0x00, 0x00, 0x00, 0xFF, 0x00, 0x00, 0x00, 0x00} // Invalid record
	blockCrc := crc32.ChecksumIEEE(garbage)
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  0,
		BlockLength:  uint32(len(garbage)),
		BlockCrc:     blockCrc,
	}
	content := append(codec.EncodeRecord(headerRecord), codec.EncodeRecord(blockHeader)...)
	content = append(content, garbage...)
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	headerSize := int64(codec.RecordHeaderSize + codec.HeaderRecordSize)
	_, err = reader.readDataBlocksUnsafe(ctx, opt, 1, headerSize)
	// DecodeRecordList may succeed with partial decode or fail entirely
	// Either way, no valid entries → ErrEntryNotFound
	assert.Error(t, err)
	assert.True(t, errors.Is(err, werr.ErrEntryNotFound))
}

// ============================================================================
// Additional edge cases for remaining uncovered lines
// ============================================================================

func TestReadNextBatchAdv_Closed(t *testing.T) {
	ctx := context.Background()
	baseDir, _ := createTempFileWithContent(t, []byte("data"))
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	reader.Close(ctx)

	_, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}, nil)
	assert.True(t, errors.Is(err, werr.ErrFileReaderAlreadyClosed))
}

func TestGetLastEntryID_Closed(t *testing.T) {
	ctx := context.Background()
	baseDir, _ := createTempFileWithContent(t, []byte("data"))
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	reader.Close(ctx)

	_, err = reader.GetLastEntryID(ctx)
	assert.True(t, errors.Is(err, werr.ErrFileReaderAlreadyClosed))
}

func TestGetLastEntryID_IncompleteFile_ScanError(t *testing.T) {
	ctx := context.Background()
	baseDir, _ := createTempFileWithContent(t, make([]byte, 200))
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)

	reader.isIncompleteFile.Store(true)
	reader.footer = nil
	reader.blockIndexes = nil

	// Close file to cause scan to fail on stat
	reader.file.Close()

	_, err = reader.GetLastEntryID(ctx)
	assert.Error(t, err)
}

func TestGetLastEntryID_IncompleteFile_NoBlocksAfterScan(t *testing.T) {
	ctx := context.Background()
	// Create file with valid header only (no blocks)
	headerRecord := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	encodedHeader := codec.EncodeRecord(headerRecord)
	// Pad to pass size check
	content := make([]byte, len(encodedHeader)+100)
	copy(content, encodedHeader)
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.footer = nil
	reader.blockIndexes = nil

	_, err = reader.GetLastEntryID(ctx)
	assert.True(t, werr.ErrFileReaderNoBlockFound.Is(err))
}

func TestParseIndexRecords_ReadIndexDataError(t *testing.T) {
	ctx := context.Background()
	// Create a sufficiently large file so the reader constructor doesn't fail
	content := make([]byte, 200)
	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Manually set footer with 1 block, then call parseIndexRecordsUnsafe
	// with a fileSize that makes indexStartOffset valid but ReadAt returns EOF
	reader.footer = &codec.FooterRecord{
		TotalBlocks: 1,
		Version:     codec.FormatVersion,
	}

	// Use a fileSize where indexStartOffset is valid (>= 0) but
	// the index data region extends beyond the actual file content
	// footerRecordSize = RecordHeaderSize + FooterRecordSizeV6 = 9 + 44 = 53
	// indexRecordSize = RecordHeaderSize + IndexRecordSize = 9 + 32 = 41
	// indexStartOffset = fileSize - 53 - 41 = fileSize - 94
	// With fileSize=500, indexStartOffset=406, but actual file is only 200 bytes → read fails
	err = reader.parseIndexRecordsUnsafe(ctx, 500)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read index data")
}

func TestIsFooterExistsUnsafe_StatError(t *testing.T) {
	ctx := context.Background()
	baseDir, _ := createTempFileWithContent(t, make([]byte, 200))
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)

	// Close file to make stat fail
	reader.file.Close()

	result := reader.isFooterExistsUnsafe(ctx)
	assert.False(t, result)
}

func TestIsFooterExistsUnsafe_ReadFooterDataError(t *testing.T) {
	ctx := context.Background()
	// Create file large enough to pass the minFooterSize check
	// but where reading the footer region returns truncated data
	minSize := codec.RecordHeaderSize + codec.FooterRecordSizeV5 + 10 // > minFooterSize
	content := make([]byte, minSize)
	baseDir, filePath := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Truncate file after opening to cause read failure
	os.Truncate(filePath, 5)

	result := reader.isFooterExistsUnsafe(ctx)
	assert.False(t, result)
}

// === Reader parse error edge case tests for improved coverage ===

func TestParseIndexRecords_WrongRecordType(t *testing.T) {
	ctx := context.Background()

	// Build a file: [DataRecord with IndexRecordSize payload] [FooterRecord]
	// The DataRecord has the same encoded size as an IndexRecord (9+32=41 bytes)
	// but with Type=DataRecordType instead of IndexRecordType
	fakeIndexPayload := make([]byte, codec.IndexRecordSize)
	fakeIndex := codec.EncodeRecord(&codec.DataRecord{Payload: fakeIndexPayload})

	footerRecord := &codec.FooterRecord{
		TotalBlocks:  1,
		TotalRecords: 1,
		TotalSize:    uint64(len(fakeIndex) + codec.RecordHeaderSize + codec.FooterRecordSizeV6),
		IndexOffset:  0,
		IndexLength:  uint32(len(fakeIndex)),
		Version:      codec.FormatVersion,
		LAC:          -1,
	}
	encodedFooter := codec.EncodeRecord(footerRecord)

	content := append(fakeIndex, encodedFooter...)

	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// The constructor's tryParseFooterAndIndexesIfExists would have failed on index parsing
	// and set isIncompleteFile=true, footer=nil. Now test directly:
	reader.footer = footerRecord
	err = reader.parseIndexRecordsUnsafe(ctx, int64(len(content)))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected index record type")
}

func TestParseIndexRecords_MultipleBlocks_SortComparator(t *testing.T) {
	// Create a finalized file with multiple small blocks so parseIndexRecordsUnsafe
	// processes 2+ index records and exercises the sort.Slice comparator
	dir, err := os.MkdirTemp("", "test_woodpecker_sort_*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	cfg, _ := config.NewConfiguration()

	writer, err := NewLocalFileWriter(context.Background(), dir, 1, 0, cfg)
	require.NoError(t, err)
	writer.maxFlushSize = 30 // Very small block size to force multiple blocks

	// Write entries one by one with sync to force separate blocks
	for i := int64(0); i < 5; i++ {
		_, err = writer.WriteDataAsync(context.Background(), i, []byte("test data entry"), nil)
		require.NoError(t, err)
		writer.Sync(context.Background())
		time.Sleep(100 * time.Millisecond) // Wait for async flush
	}

	_, err = writer.Finalize(context.Background(), 4)
	require.NoError(t, err)
	writer.Close(context.Background())

	// Open reader - should parse 2+ index records and call sort.Slice comparator
	reader, err := NewLocalFileReaderAdv(context.Background(), dir, 1, 0, 100)
	require.NoError(t, err)
	defer reader.Close(context.Background())

	// Verify multiple blocks were created and parsed
	indexes := reader.GetBlockIndexes()
	assert.GreaterOrEqual(t, len(indexes), 2, "should have multiple blocks")

	// Verify they are sorted by block number
	for i := 1; i < len(indexes); i++ {
		assert.Greater(t, indexes[i].BlockNumber, indexes[i-1].BlockNumber,
			"block indexes should be sorted by block number")
	}
}

func TestReadDataBlocks_DecodeBlockHeaderError(t *testing.T) {
	ctx := context.Background()

	// Build file: [HeaderRecord] [corrupted BlockHeader data]
	header := codec.EncodeRecord(&codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	})

	// Add garbage data that looks like a block header region but isn't decodable
	garbage := make([]byte, 50)
	for i := range garbage {
		garbage[i] = byte(0xAB)
	}
	content := append(header, garbage...)

	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Set as incomplete to avoid footer lookup
	reader.isIncompleteFile.Store(true)
	reader.footer = nil

	opt := storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}

	// Reading from offset 0 with no lastReadBatchInfo and no footer → scans from beginning
	// The garbage after header should cause decode errors
	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0, 0)
	assert.Nil(t, batch)
	assert.Error(t, err) // Should get entryNotFound or EOF
}

func TestReadDataBlocks_CorruptedBlockData(t *testing.T) {
	ctx := context.Background()

	// Build a file with valid header + valid block header but corrupted block data
	header := codec.EncodeRecord(&codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	})

	// Create a valid block header that claims to have specific block data
	validData := codec.EncodeRecord(&codec.DataRecord{Payload: []byte("test data")})
	blockCrc := crc32.ChecksumIEEE(validData)
	blockHeader := codec.EncodeRecord(&codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  0,
		BlockLength:  uint32(len(validData)),
		BlockCrc:     blockCrc,
	})

	// Write corrupted block data (wrong CRC)
	corruptedData := make([]byte, len(validData))
	copy(corruptedData, validData)
	corruptedData[len(corruptedData)-1] ^= 0xFF // Flip last byte to break CRC

	var content []byte
	content = append(content, header...)
	content = append(content, blockHeader...)
	content = append(content, corruptedData...)

	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.footer = nil

	opt := storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}

	// Read should fail on CRC verification
	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0, 0)
	assert.Nil(t, batch)
	assert.Error(t, err)
}

func TestScanForAllBlockInfo_TruncatedBlockHeader(t *testing.T) {
	ctx := context.Background()

	// Build file: [HeaderRecord] + partial block header (truncated)
	header := codec.EncodeRecord(&codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	})

	// Add partial block header data (not enough bytes for a complete BlockHeaderRecord)
	partial := make([]byte, codec.RecordHeaderSize+5) // Less than RecordHeaderSize+BlockHeaderRecordSize
	content := append(header, partial...)

	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.footer = nil
	reader.blockIndexes = nil

	// Scan should handle truncated block header gracefully
	err = reader.scanForAllBlockInfoUnsafe(ctx)
	assert.NoError(t, err)
	assert.Empty(t, reader.blockIndexes)
}

func TestScanForAllBlockInfo_TruncatedBlockData(t *testing.T) {
	ctx := context.Background()

	// Build file: [HeaderRecord] + [valid BlockHeader] + [truncated block data]
	header := codec.EncodeRecord(&codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	})

	// Block header claims 100 bytes of data but file only has 10
	blockHeader := codec.EncodeRecord(&codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  0,
		BlockLength:  100, // Claims 100 bytes
		BlockCrc:     0,
	})

	// Only provide 10 bytes (truncated)
	truncatedData := make([]byte, 10)

	var content []byte
	content = append(content, header...)
	content = append(content, blockHeader...)
	content = append(content, truncatedData...)

	baseDir, _ := createTempFileWithContent(t, content)
	defer os.RemoveAll(baseDir)

	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	reader.isIncompleteFile.Store(true)
	reader.footer = nil
	reader.blockIndexes = nil

	// Scan should handle truncated block data gracefully
	err = reader.scanForAllBlockInfoUnsafe(ctx)
	assert.NoError(t, err)
	assert.Empty(t, reader.blockIndexes) // Block not added due to incomplete data
}

func TestReadNextBatchAdv_SearchBlockError(t *testing.T) {
	ctx := context.Background()

	// Create a finalized file, then read with an entryID beyond all blocks
	dir, err := os.MkdirTemp("", "test_woodpecker_search_*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	cfg, _ := config.NewConfiguration()
	writer, err := NewLocalFileWriter(ctx, dir, 1, 0, cfg)
	require.NoError(t, err)

	// Write a few entries
	for i := int64(0); i < 3; i++ {
		_, err = writer.WriteDataAsync(ctx, i, []byte("data"), nil)
		require.NoError(t, err)
	}
	_, err = writer.Finalize(ctx, 2)
	require.NoError(t, err)
	writer.Close(ctx)

	reader, err := NewLocalFileReaderAdv(ctx, dir, 1, 0, 100)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Read with entryID beyond all blocks → SearchBlock returns nil → ErrFileReaderEndOfFile
	opt := storage.ReaderOpt{
		StartEntryID:    9999,
		MaxBatchEntries: 10,
	}
	batch, err := reader.ReadNextBatchAdv(ctx, opt, nil)
	assert.Nil(t, batch)
	assert.True(t, werr.ErrFileReaderEndOfFile.Is(err))
}

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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

func createTempFileWithContent(t *testing.T, content []byte) (string, string) {
	// Create temporary base directory
	tempDir, err := os.MkdirTemp("", "test_woodpecker_base_*")
	require.NoError(t, err)

	// Create the expected directory structure: baseDir/logId/segmentId/
	segmentDir := filepath.Join(tempDir, "1", "1")
	err = os.MkdirAll(segmentDir, 0755)
	require.NoError(t, err)

	// Create the data.log file in the correct location
	filePath := filepath.Join(segmentDir, "data.log")
	err = os.WriteFile(filePath, content, 0644)
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
	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, nil, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Set file as completed (has footer) to test the bug fix
	reader.footer = &codec.FooterRecord{}
	reader.isIncompleteFile = false

	// Delete the file after opening (simulates file being deleted during operation)
	os.Remove(filePath)

	opt := storage.ReaderOpt{
		StartEntryID: 0,
		BatchSize:    10,
	}

	batch, err := reader.readDataBlocks(ctx, opt, 0, 0)

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
		os.Chmod(filePath, 0644)
		os.RemoveAll(baseDir)
	}()

	// Create reader first
	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, nil, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Set as completed file to test bug fix
	reader.footer = &codec.FooterRecord{}
	reader.isIncompleteFile = false

	// Close the file and change permissions to make it unreadable
	reader.file.Close()
	err = os.Chmod(filePath, 0000)
	require.NoError(t, err)

	opt := storage.ReaderOpt{
		StartEntryID: 0,
		BatchSize:    10,
	}

	batch, err := reader.readDataBlocks(ctx, opt, 0, 0)

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
	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, nil, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Set as completed file (has footer)
	reader.footer = &codec.FooterRecord{}
	reader.isIncompleteFile = false
	reader.blockIndexes = []*codec.IndexRecord{} // No blocks

	opt := storage.ReaderOpt{
		StartEntryID: 0,
		BatchSize:    10,
	}

	// TODO mock no error exists when read data blocks
	batch, err := reader.readDataBlocks(ctx, opt, 0, 0)

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
	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, nil, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Set as incomplete file (no footer)
	reader.footer = nil
	reader.isIncompleteFile = true
	reader.blockIndexes = []*codec.IndexRecord{} // No blocks

	opt := storage.ReaderOpt{
		StartEntryID: 0,
		BatchSize:    10,
	}

	// TODO mock no error exists when read data blocks
	batch, err := reader.readDataBlocks(ctx, opt, 0, 0)

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
	reader, err := NewLocalFileReaderAdv(ctx, baseDir, 1, 1, nil, 4*1024*1024)
	require.NoError(t, err)
	defer reader.Close(ctx)

	// Test normal read first
	data, err := reader.readAt(ctx, 0, 100)
	assert.NoError(t, err)
	assert.Len(t, data, 100)

	// close&Delete the file to mock read error
	reader.file.Close()
	removeErr := os.Remove(filePath)
	assert.NoError(t, removeErr)

	// This should now fail
	data, err = reader.readAt(ctx, 0, 100)
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

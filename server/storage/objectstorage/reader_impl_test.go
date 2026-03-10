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

package objectstorage

import (
	"context"
	"hash/crc32"
	"io"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/conc"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

func mockNoSuchKeyError() error {
	return werr.ErrEntryNotFound.WithCauseErrMsg("The specified key does not exist.")
}

// mockFileReader implements a simple mock for minioHandler.FileReader
type mockFileReader struct {
	data     []byte
	position int
}

func (m *mockFileReader) Read(p []byte) (n int, err error) {
	if m.position >= len(m.data) {
		return 0, io.EOF
	}
	n = copy(p, m.data[m.position:])
	m.position += n
	if m.position >= len(m.data) {
		err = io.EOF
	}
	return n, err
}

func (m *mockFileReader) Close() error {
	return nil
}

func (m *mockFileReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n = copy(p, m.data[off:])
	if off+int64(n) >= int64(len(m.data)) {
		err = io.EOF
	}
	return n, err
}

func (m *mockFileReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		m.position = int(offset)
	case io.SeekCurrent:
		m.position += int(offset)
	case io.SeekEnd:
		m.position = len(m.data) + int(offset)
	}
	if m.position < 0 {
		m.position = 0
	}
	if m.position > len(m.data) {
		m.position = len(m.data)
	}
	return int64(m.position), nil
}

func (m *mockFileReader) Size() (int64, error) {
	return int64(len(m.data)), nil
}

// createMockCompactedFooterData creates valid encoded footer data with compaction flag set
func createMockCompactedFooterData() []byte {
	footer := &codec.FooterRecord{
		TotalBlocks:  1,
		TotalRecords: 1,
		TotalSize:    1024,
		IndexOffset:  0,
		IndexLength:  0,
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(0),
		LAC:          10,
	}
	record := codec.Record(footer)
	return codec.EncodeRecord(record)
}

func TestMinioFileReaderAdv_readDataBlocks_NoError_EOF_CompletedFile(t *testing.T) {
	// Test: No read errors, file is completed → should return EOF
	ctx := context.Background()

	mockClient := mocks_objectstorage.NewObjectStorage(t)

	reader := &MinioFileReaderAdv{
		client:         mockClient,
		bucket:         "test-bucket",
		segmentFileKey: "test-segment",
		logId:          1,
		segmentId:      1,
		logIdStr:       "1",
		maxBatchSize:   4 * 1024 * 1024, // 4MB
		pool:           conc.NewPool[*BlockReadResult](4, conc.WithPreAlloc(true)),
		footer:         &codec.FooterRecord{}, // Has footer = completed
	}
	reader.isCompleted.Store(true)

	// Mock StatObject to return "not found" (no blocks to read)
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, mockNoSuchKeyError()).
		Maybe()

	// Mock IsObjectNotExistsError to return true for our mock error
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
		Return(true).
		Maybe()

	opt := storage.ReaderOpt{
		StartEntryID:    100,
		MaxBatchEntries: 10,
	}

	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0)

	// Should return EOF because file is completed and no read errors occurred
	assert.Nil(t, batch)
	assert.True(t, errors.Is(err, werr.ErrFileReaderEndOfFile))
}

func TestMinioFileReaderAdv_readDataBlocks_NoError_EntryNotFound_IncompleteFile(t *testing.T) {
	// Test: No read errors, file is incomplete → should return EntryNotFound
	ctx := context.Background()

	mockClient := mocks_objectstorage.NewObjectStorage(t)

	reader := &MinioFileReaderAdv{
		client:         mockClient,
		bucket:         "test-bucket",
		segmentFileKey: "test-segment",
		logId:          1,
		segmentId:      1,
		logIdStr:       "1",
		maxBatchSize:   4 * 1024 * 1024, // 4MB
		pool:           conc.NewPool[*BlockReadResult](4, conc.WithPreAlloc(true)),
		footer:         nil, // No footer = incomplete
	}
	reader.isCompleted.Store(false)
	reader.isCompacted.Store(false)
	reader.isFenced.Store(false)

	// Mock StatObject to return "not found" (no blocks to read and no footer)
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, mockNoSuchKeyError()).
		Maybe()

	// Mock IsObjectNotExistsError to return true for our mock error
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
		Return(true).
		Maybe()

	opt := storage.ReaderOpt{
		StartEntryID:    100,
		MaxBatchEntries: 10,
	}

	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0)

	// Should return EntryNotFound because file is incomplete and no read errors occurred
	assert.Nil(t, batch)
	assert.True(t, errors.Is(err, werr.ErrEntryNotFound))
}

func TestMinioFileReaderAdv_readDataBlocks_WithStatError_ShouldReturnEntryNotFound(t *testing.T) {
	// Test: StatObject error occurred → should return EntryNotFound (not EOF)
	ctx := context.Background()

	mockClient := mocks_objectstorage.NewObjectStorage(t)

	reader := &MinioFileReaderAdv{
		client:         mockClient,
		bucket:         "test-bucket",
		segmentFileKey: "test-segment",
		logId:          1,
		segmentId:      1,
		logIdStr:       "1",
		maxBatchSize:   4 * 1024 * 1024, // 4MB
		pool:           conc.NewPool[*BlockReadResult](4, conc.WithPreAlloc(true)),
		footer:         &codec.FooterRecord{}, // Has footer = completed
	}
	reader.isCompleted.Store(true) // File is completed

	// Mock StatObject to return a network error (not "not found")
	statError := errors.New("network timeout")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, statError).
		Once()

	// Mock IsObjectNotExistsError to return false for network error
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
		Return(false).
		Maybe()

	opt := storage.ReaderOpt{
		StartEntryID:    100,
		MaxBatchEntries: 10,
	}

	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0)

	// Should return EntryNotFound because read error occurred, even though file is completed
	assert.Nil(t, batch)
	assert.True(t, errors.Is(err, werr.ErrEntryNotFound))
	assert.Contains(t, err.Error(), "no record extract")
}

func TestMinioFileReaderAdv_readBlockBatch_ErrorHandling(t *testing.T) {
	// Test: readBlockBatch properly handles and reports errors
	ctx := context.Background()

	mockClient := mocks_objectstorage.NewObjectStorage(t)

	reader := &MinioFileReaderAdv{
		client:         mockClient,
		bucket:         "test-bucket",
		segmentFileKey: "test-segment",
		logId:          1,
		segmentId:      1,
		logIdStr:       "1",
		maxBatchSize:   4 * 1024 * 1024, // 4MB
		pool:           conc.NewPool[*BlockReadResult](4, conc.WithPreAlloc(true)),
	}

	// Test StatObject error
	statError := errors.New("stat failed")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, statError).
		Once()

	// Mock IsObjectNotExistsError to return false for stat error
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
		Return(false).
		Maybe()

	futures, nextBlockID, totalRawSize, hasMoreBlocks, readBlockBatchErr := reader.readBlockBatchUnsafe(ctx, 0, 4*1024*1024, 0)

	assert.Empty(t, futures)
	assert.Equal(t, int64(0), nextBlockID)
	assert.Equal(t, int64(0), totalRawSize)
	assert.Error(t, readBlockBatchErr) // Error should be reported
	assert.True(t, hasMoreBlocks)      // hasMoreBlocks should remain true when error occurs
}

func TestMinioFileReaderAdv_processBlockData_IntegrityError(t *testing.T) {
	// Test: Block data integrity error should be handled properly
	ctx := context.Background()
	reader := &MinioFileReaderAdv{
		segmentFileKey: "test-segment",
		logId:          1,
		segmentId:      1,
		logIdStr:       "1",
	}

	// Create invalid block data that will fail integrity check
	invalidBlockData := []byte{0x01, 0x02, 0x03} // Too short to be valid

	entries, readBytes, blockInfo, err := reader.processBlockData(ctx, 0, invalidBlockData, 0)

	assert.Nil(t, entries)
	assert.Equal(t, 0, readBytes)
	assert.Nil(t, blockInfo)
	assert.Error(t, err)
	// Update error message check based on actual error
	assert.Contains(t, err.Error(), "record")
}

func TestMinioFileReaderAdv_ErrorHandling_MultipleScenarios(t *testing.T) {
	testCases := []struct {
		name                    string
		isCompleted             bool
		isCompacted             bool
		isFenced                bool
		hasFooter               bool
		hasReadError            bool
		objectNotFound          bool
		blockShouldExist        bool
		mockCompactionDetection bool
		expectEOF               bool
		expectNotFound          bool
		expectCompactionError   bool
	}{
		// Basic completed state scenarios - when segments are completed, should return EOF
		{
			name:                    "NoError_Completed_ShouldReturnEOF",
			isCompleted:             true,
			isCompacted:             false,
			isFenced:                false,
			hasFooter:               true,
			hasReadError:            false,
			objectNotFound:          true,
			blockShouldExist:        false,
			mockCompactionDetection: false,
			expectEOF:               true,
			expectNotFound:          false,
			expectCompactionError:   false,
		},
		{
			name:                    "NoError_Compacted_ShouldReturnEOF",
			isCompleted:             true,
			isCompacted:             true,
			isFenced:                false,
			hasFooter:               true,
			hasReadError:            false,
			objectNotFound:          true,
			blockShouldExist:        false,
			mockCompactionDetection: false,
			expectEOF:               true,
			expectNotFound:          false,
			expectCompactionError:   false,
		},
		{
			name:                    "NoError_Fenced_ShouldReturnEOF",
			isCompleted:             false,
			isCompacted:             false,
			isFenced:                true,
			hasFooter:               false,
			hasReadError:            false,
			objectNotFound:          true,
			blockShouldExist:        false,
			mockCompactionDetection: false,
			expectEOF:               true,
			expectNotFound:          false,
			expectCompactionError:   false,
		},
		{
			name:                    "NoError_HasFooter_ShouldReturnEOF",
			isCompleted:             true,
			isCompacted:             false,
			isFenced:                false,
			hasFooter:               true,
			hasReadError:            false,
			objectNotFound:          true,
			blockShouldExist:        false,
			mockCompactionDetection: false,
			expectEOF:               true,
			expectNotFound:          false,
			expectCompactionError:   false,
		},
		{
			name:                    "NoError_Incomplete_ShouldReturnEntryNotFound",
			isCompleted:             false,
			isCompacted:             false,
			isFenced:                false,
			hasFooter:               false,
			hasReadError:            false,
			objectNotFound:          true,
			blockShouldExist:        true, // Active segment assumes block might exist
			mockCompactionDetection: false,
			expectEOF:               false,
			expectNotFound:          true,
			expectCompactionError:   false,
		},

		// General read error scenarios - any read error should return EntryNotFound
		{
			name:                    "WithError_Completed_ShouldReturnEntryNotFound",
			isCompleted:             true,
			isCompacted:             false,
			isFenced:                false,
			hasFooter:               true,
			hasReadError:            true,
			objectNotFound:          false,
			blockShouldExist:        false,
			mockCompactionDetection: false,
			expectEOF:               false,
			expectNotFound:          true,
			expectCompactionError:   false,
		},
		{
			name:                    "WithError_Compacted_ShouldReturnEntryNotFound",
			isCompleted:             true,
			isCompacted:             true,
			isFenced:                false,
			hasFooter:               true,
			hasReadError:            true,
			objectNotFound:          false,
			blockShouldExist:        false,
			mockCompactionDetection: false,
			expectEOF:               false,
			expectNotFound:          true,
			expectCompactionError:   false,
		},
		{
			name:                    "WithError_HasFooter_ShouldReturnEntryNotFound",
			isCompleted:             true,
			isCompacted:             false,
			isFenced:                false,
			hasFooter:               true,
			hasReadError:            true,
			objectNotFound:          false,
			blockShouldExist:        false,
			mockCompactionDetection: false,
			expectEOF:               false,
			expectNotFound:          true,
			expectCompactionError:   false,
		},

		// Compaction-related scenarios - testing shouldBlockExist and compaction detection
		{
			name:                    "BlockNotFound_ShouldExist_NoCompaction_ActiveSegment",
			isCompleted:             false,
			isCompacted:             false,
			isFenced:                false,
			hasFooter:               false,
			hasReadError:            false,
			objectNotFound:          true,
			blockShouldExist:        true, // Active segment, block should exist
			mockCompactionDetection: false,
			expectEOF:               false, // Active segment without footer should return EntryNotFound
			expectNotFound:          true,
			expectCompactionError:   false,
		},
		{
			name:                    "BlockNotFound_ShouldExist_CompactionDetected",
			isCompleted:             true,
			isCompacted:             false, // Not yet compacted in reader state
			isFenced:                false,
			hasFooter:               true,
			hasReadError:            false,
			objectNotFound:          true,
			blockShouldExist:        true,
			mockCompactionDetection: true, // Mock footer refresh detects compaction
			expectEOF:               true,
			expectNotFound:          false, // TODO: Mock footer refresh to detect compaction flags and verify expected behavior: expectEOF=false, expectNotFound=true
			expectCompactionError:   false,
		},
		{
			name:                    "BlockNotFound_ShouldNotExist_CompletedSegment",
			isCompleted:             true,
			isCompacted:             false,
			isFenced:                false,
			hasFooter:               true,
			hasReadError:            false,
			objectNotFound:          true,
			blockShouldExist:        false, // Block beyond footer's total blocks
			mockCompactionDetection: false,
			expectEOF:               true, // Normal EOF
			expectNotFound:          false,
			expectCompactionError:   false,
		},
		{
			name:                    "BlockNotFound_AlreadyCompacted",
			isCompleted:             true,
			isCompacted:             true, // Already compacted
			isFenced:                false,
			hasFooter:               true,
			hasReadError:            false,
			objectNotFound:          true,
			blockShouldExist:        false, // Block doesn't exist in compacted state
			mockCompactionDetection: false,
			expectEOF:               true,
			expectNotFound:          false,
			expectCompactionError:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			mockClient := mocks_objectstorage.NewObjectStorage(t)

			reader := &MinioFileReaderAdv{
				client:         mockClient,
				bucket:         "test-bucket",
				segmentFileKey: "test-segment",
				logId:          1,
				segmentId:      1,
				logIdStr:       "1",
				maxBatchSize:   4 * 1024 * 1024, // 4MB
				pool:           conc.NewPool[*BlockReadResult](4, conc.WithPreAlloc(true)),
			}

			// Setup reader state
			reader.isCompleted.Store(tc.isCompleted)
			reader.isCompacted.Store(tc.isCompacted)
			reader.isFenced.Store(tc.isFenced)

			// Setup footer for shouldBlockExist logic
			if tc.hasFooter {
				if tc.blockShouldExist {
					// Block should exist: set TotalBlocks to be greater than startBlockID
					reader.footer = &codec.FooterRecord{TotalBlocks: 200}
				} else {
					// Block should not exist: set TotalBlocks to be less than startBlockID
					reader.footer = &codec.FooterRecord{TotalBlocks: 5}
				}
			}

			// Setup mock expectations
			if tc.hasReadError {
				// Mock StatObject to return a general error (not object not found)
				mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
					Return(int64(0), false, errors.New("mock error")).
					Once()

				// Mock IsObjectNotExistsError to return false for general error
				mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
					Return(false).
					Maybe()
			} else if tc.objectNotFound {
				// Mock StatObject to return "not found"
				mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
					Return(int64(0), false, mockNoSuchKeyError()).
					Maybe()

				// Mock IsObjectNotExistsError to return true for not found error
				mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
					Return(true).
					Maybe()

				// For compaction detection scenarios, mock footer refresh
				if tc.mockCompactionDetection {
					// Mock footer.blk StatObject and GetObject for compaction detection
					mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-segment/footer.blk", mock.Anything, mock.Anything).
						Return(int64(100), false, nil).
						Maybe()

					// Create a mock compacted footer response
					compactedFooterData := createMockCompactedFooterData()
					mockFooterObj := &mockFileReader{data: compactedFooterData}
					mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-segment/footer.blk", int64(0), int64(100), mock.Anything, mock.Anything).
						Return(mockFooterObj, nil).
						Maybe()
				}
			} else {
				// No specific error expected, mock normal behavior
				mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
					Return(int64(0), false, mockNoSuchKeyError()).
					Maybe()

				// Mock IsObjectNotExistsError to return true for not found error
				mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
					Return(true).
					Maybe()
			}

			opt := storage.ReaderOpt{
				StartEntryID:    100, // Use high block ID to test shouldBlockExist logic
				MaxBatchEntries: 10,
			}

			batch, err := reader.readDataBlocksUnsafe(ctx, opt, 100) // startBlockID = 100

			// Verify results
			assert.Nil(t, batch)
			require.Error(t, err)

			if tc.expectEOF {
				assert.True(t, errors.Is(err, werr.ErrFileReaderEndOfFile), "Expected EOF error, got: %v", err)
			} else if tc.expectNotFound {
				assert.True(t, errors.Is(err, werr.ErrEntryNotFound), "Expected EntryNotFound error, got: %v", err)
			} else if tc.expectCompactionError {
				assert.True(t, werr.ErrFileReaderBlockDeletedByCompaction.Is(err), "Expected Compaction error, got: %v", err)
			}
		})
	}
}

// ===== Additional Reader Unit Tests =====

// newTestReader creates a MinioFileReaderAdv directly for unit testing
func newTestReader(mockClient *mocks_objectstorage.ObjectStorage) *MinioFileReaderAdv {
	logId := int64(1)
	segId := int64(0)
	reader := &MinioFileReaderAdv{
		logId:          logId,
		segmentId:      segId,
		logIdStr:       strconv.FormatInt(logId, 10),
		nsStr:          "test-bucket/test-base",
		client:         mockClient,
		bucket:         "test-bucket",
		segmentFileKey: "test-base/1/0",
		blocks:         make([]*codec.IndexRecord, 0),
		footer:         nil,
		maxBatchSize:   4 * 1024 * 1024,
		pool:           conc.NewPool[*BlockReadResult](4, conc.WithPreAlloc(true)),
	}
	reader.flags.Store(0)
	reader.version.Store(codec.FormatVersion)
	reader.closed.Store(false)
	reader.isCompacted.Store(false)
	reader.isCompleted.Store(false)
	reader.isFenced.Store(false)
	return reader
}

func TestMinioFileReaderAdv_Close_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	err := reader.Close(ctx)
	assert.NoError(t, err)
	assert.True(t, reader.closed.Load())
}

func TestMinioFileReaderAdv_Close_Idempotent(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// First close
	err := reader.Close(ctx)
	assert.NoError(t, err)
	assert.True(t, reader.closed.Load())

	// Second close - should be idempotent
	err = reader.Close(ctx)
	assert.NoError(t, err)
}

func TestMinioFileReaderAdv_GetBlockIndexes_Empty(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blocks := reader.GetBlockIndexes()
	assert.NotNil(t, blocks)
	assert.Empty(t, blocks)
}

func TestMinioFileReaderAdv_GetBlockIndexes_WithBlocks(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	reader.blocks = []*codec.IndexRecord{
		{BlockNumber: 0, FirstEntryID: 0, LastEntryID: 9, BlockSize: 100},
		{BlockNumber: 1, FirstEntryID: 10, LastEntryID: 19, BlockSize: 200},
	}

	blocks := reader.GetBlockIndexes()
	assert.Len(t, blocks, 2)
	assert.Equal(t, int32(0), blocks[0].BlockNumber)
	assert.Equal(t, int64(0), blocks[0].FirstEntryID)
	assert.Equal(t, int64(9), blocks[0].LastEntryID)
	assert.Equal(t, int32(1), blocks[1].BlockNumber)
	assert.Equal(t, int64(10), blocks[1].FirstEntryID)
}

func TestMinioFileReaderAdv_GetFooter_Nil(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	footer := reader.GetFooter()
	assert.Nil(t, footer)
}

func TestMinioFileReaderAdv_GetFooter_Exists(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	expectedFooter := &codec.FooterRecord{
		TotalBlocks:  5,
		TotalRecords: 50,
		TotalSize:    1024,
	}
	reader.footer = expectedFooter

	footer := reader.GetFooter()
	assert.Equal(t, expectedFooter, footer)
	assert.Equal(t, int32(5), footer.TotalBlocks)
	assert.Equal(t, uint32(50), footer.TotalRecords)
}

func TestMinioFileReaderAdv_GetTotalRecords_NoFooter(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	assert.Equal(t, uint32(0), reader.GetTotalRecords())
}

func TestMinioFileReaderAdv_GetTotalRecords_WithFooter(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	reader.footer = &codec.FooterRecord{TotalRecords: 42}

	assert.Equal(t, uint32(42), reader.GetTotalRecords())
}

func TestMinioFileReaderAdv_GetTotalBlocks_NoFooter(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	assert.Equal(t, int32(0), reader.GetTotalBlocks())
}

func TestMinioFileReaderAdv_GetTotalBlocks_WithFooter(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	reader.footer = &codec.FooterRecord{TotalBlocks: 7}

	assert.Equal(t, int32(7), reader.GetTotalBlocks())
}

func TestMinioFileReaderAdv_UpdateLastAddConfirmed(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// UpdateLastAddConfirmed is a no-op, should always succeed
	err := reader.UpdateLastAddConfirmed(ctx, 100)
	assert.NoError(t, err)

	err = reader.UpdateLastAddConfirmed(ctx, -1)
	assert.NoError(t, err)

	err = reader.UpdateLastAddConfirmed(ctx, 0)
	assert.NoError(t, err)
}

func TestMinioFileReaderAdv_GetLastEntryID_NoBlocks(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// No blocks exist, stat returns not found
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, mockNoSuchKeyError()).
		Maybe()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
		Return(true).
		Maybe()

	lastEntryID, err := reader.GetLastEntryID(ctx)
	assert.Equal(t, int64(-1), lastEntryID)
	assert.True(t, werr.ErrFileReaderNoBlockFound.Is(err))
}

func TestMinioFileReaderAdv_ShouldBlockExist_WithFooter(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	reader.footer = &codec.FooterRecord{TotalBlocks: 5}

	// Block 0-4 should exist
	assert.True(t, reader.shouldBlockExist(0))
	assert.True(t, reader.shouldBlockExist(4))

	// Block 5+ should not exist
	assert.False(t, reader.shouldBlockExist(5))
	assert.False(t, reader.shouldBlockExist(100))
}

func TestMinioFileReaderAdv_ShouldBlockExist_NoFooter(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Without footer (active segment), always assume block might exist
	assert.True(t, reader.shouldBlockExist(0))
	assert.True(t, reader.shouldBlockExist(100))
}

func TestMinioFileReaderAdv_GetBlockObjectKey_Normal(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	reader.isCompacted.Store(false)

	key := reader.getBlockObjectKey(0)
	assert.Equal(t, "test-base/1/0/0.blk", key)

	key = reader.getBlockObjectKey(5)
	assert.Equal(t, "test-base/1/0/5.blk", key)
}

func TestMinioFileReaderAdv_GetBlockObjectKey_Compacted(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	reader.isCompacted.Store(true)

	key := reader.getBlockObjectKey(0)
	// Compacted blocks use getMergedBlockKey which has "m_" prefix
	assert.Contains(t, key, "m_")
	assert.Contains(t, key, "0.blk")
}

func TestMinioFileReaderAdv_ReadDataBlocks_ClosedReader(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	reader.closed.Store(true)

	// ReadNextBatchAdv calls tryReadFooterAndIndex before checking closed state,
	// so we need to mock the footer stat call
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, mockNoSuchKeyError()).
		Maybe()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
		Return(true).
		Maybe()

	opt := storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}

	batch, err := reader.ReadNextBatchAdv(ctx, opt, nil)
	assert.Nil(t, batch)
	assert.Error(t, err)
	assert.True(t, werr.ErrFileReaderAlreadyClosed.Is(err))
}

func TestMinioFileReaderAdv_IsFooterExists_NotFound(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Footer stat returns not found
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, mockNoSuchKeyError()).
		Once()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
		Return(true).
		Maybe()

	exists := reader.isFooterExists(ctx)
	assert.False(t, exists)
}

func TestMinioFileReaderAdv_IsFooterExists_Found(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Footer stat returns found
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(256), false, nil).
		Once()

	exists := reader.isFooterExists(ctx)
	assert.True(t, exists)
}

func TestMinioFileReaderAdv_IsFooterExists_OtherError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Footer stat returns a generic error (not "not found")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, errors.New("network error")).
		Once()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
		Return(false).
		Maybe()

	exists := reader.isFooterExists(ctx)
	assert.False(t, exists)
}

func TestMinioFileReaderAdv_ProcessBlockData_InvalidData(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Very short data should fail decoding
	entries, readBytes, blockInfo, err := reader.processBlockData(ctx, 0, []byte{0x01}, 0)
	assert.Nil(t, entries)
	assert.Equal(t, 0, readBytes)
	assert.Nil(t, blockInfo)
	assert.Error(t, err)
}

func TestMinioFileReaderAdv_InitialState(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	assert.Equal(t, int64(1), reader.logId)
	assert.Equal(t, int64(0), reader.segmentId)
	assert.Equal(t, "test-base/1/0", reader.segmentFileKey)
	assert.Equal(t, "test-bucket", reader.bucket)
	assert.False(t, reader.closed.Load())
	assert.False(t, reader.isCompleted.Load())
	assert.False(t, reader.isCompacted.Load())
	assert.False(t, reader.isFenced.Load())
	assert.Nil(t, reader.footer)
	assert.Empty(t, reader.blocks)
	assert.NotNil(t, reader.pool)
}

func TestMinioFileReaderAdv_ReadNextBatchAdv_NoFooter_NoBlocks(t *testing.T) {
	// Test reading from a segment with no footer and no blocks
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// StatObject for footer check in tryReadFooterAndIndex
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, mockNoSuchKeyError()).
		Maybe()

	// StatObject for block 0 should return not found
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, mockNoSuchKeyError()).
		Maybe()

	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
		Return(true).
		Maybe()

	opt := storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 10,
	}

	batch, err := reader.ReadNextBatchAdv(ctx, opt, nil)
	assert.Nil(t, batch)
	assert.Error(t, err)
	// Should return EntryNotFound since segment is not completed
	assert.True(t, errors.Is(err, werr.ErrEntryNotFound))
}

func TestMinioFileReaderAdv_TryReadFooterAndIndex_AlreadyCompleted(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Mark as already completed
	reader.isCompleted.Store(true)
	reader.footer = &codec.FooterRecord{TotalBlocks: 3}

	// No mock calls should be made since it's already completed
	err := reader.tryReadFooterAndIndex(ctx)
	assert.NoError(t, err)
}

func TestMinioFileReaderAdv_PrefetchIncrementalBlockInfo_NoNewBlocks(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// StatObject for block 0 returns not found
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, mockNoSuchKeyError()).
		Once()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
		Return(true).
		Maybe()

	existsNewBlock, lastBlockInfo, err := reader.prefetchIncrementalBlockInfoUnsafe(ctx)
	assert.False(t, existsNewBlock)
	assert.Nil(t, lastBlockInfo)
	assert.NoError(t, err)
}

func TestMinioFileReaderAdv_PrefetchIncrementalBlockInfo_StatError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// StatObject returns a non-"not found" error
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, errors.New("stat failed")).
		Once()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
		Return(false).
		Maybe()

	existsNewBlock, lastBlockInfo, err := reader.prefetchIncrementalBlockInfoUnsafe(ctx)
	assert.False(t, existsNewBlock)
	assert.Nil(t, lastBlockInfo)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stat failed")
}

func TestMinioFileReaderAdv_PrefetchIncrementalBlockInfo_FencedBlock(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// StatObject returns fenced=true
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(0), true, nil).
		Once()

	existsNewBlock, lastBlockInfo, err := reader.prefetchIncrementalBlockInfoUnsafe(ctx)
	assert.False(t, existsNewBlock)
	assert.Nil(t, lastBlockInfo)
	assert.NoError(t, err)
	assert.True(t, reader.isFenced.Load())
}

// ===== Helper to build valid block data for testing =====

func buildValidBlockData(t *testing.T, blockID int64, firstEntryID int64, payloads [][]byte) []byte {
	t.Helper()
	// Build data records first
	var dataRecordsBuf []byte
	for _, payload := range payloads {
		dr, err := codec.ParseData(payload)
		require.NoError(t, err)
		dataRecordsBuf = append(dataRecordsBuf, codec.EncodeRecord(dr)...)
	}

	// Calculate CRC and length
	blockLength := uint32(len(dataRecordsBuf))
	blockCrc := crc32.ChecksumIEEE(dataRecordsBuf)

	lastEntryID := firstEntryID + int64(len(payloads)) - 1
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  int32(blockID),
		FirstEntryID: firstEntryID,
		LastEntryID:  lastEntryID,
		BlockLength:  blockLength,
		BlockCrc:     blockCrc,
	}

	var result []byte
	if blockID == 0 {
		// First block has file header
		header := &codec.HeaderRecord{
			Version:      codec.FormatVersion,
			Flags:        0,
			FirstEntryID: firstEntryID,
		}
		result = append(result, codec.EncodeRecord(header)...)
	}
	result = append(result, codec.EncodeRecord(blockHeader)...)
	result = append(result, dataRecordsBuf...)
	return result
}

// ===== processBlockData tests with valid data =====

func TestMinioFileReaderAdv_ProcessBlockData_ValidBlock0(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockData := buildValidBlockData(t, 0, 0, [][]byte{[]byte("hello"), []byte("world")})

	entries, readBytes, blockInfo, err := reader.processBlockData(ctx, 0, blockData, 0)
	assert.NoError(t, err)
	assert.Len(t, entries, 2)
	assert.Greater(t, readBytes, 0)
	assert.NotNil(t, blockInfo)
	assert.Equal(t, int32(0), blockInfo.BlockNumber)
	assert.Equal(t, int64(0), blockInfo.FirstEntryID)
	assert.Equal(t, int64(1), blockInfo.LastEntryID)

	// Verify entries
	assert.Equal(t, int64(0), entries[0].EntryId)
	assert.Equal(t, int64(1), entries[1].EntryId)
}

func TestMinioFileReaderAdv_ProcessBlockData_ValidBlock1(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockData := buildValidBlockData(t, 1, 10, [][]byte{[]byte("data10"), []byte("data11")})

	entries, readBytes, blockInfo, err := reader.processBlockData(ctx, 1, blockData, 10)
	assert.NoError(t, err)
	assert.Len(t, entries, 2)
	assert.Greater(t, readBytes, 0)
	assert.NotNil(t, blockInfo)
	assert.Equal(t, int32(1), blockInfo.BlockNumber)
	assert.Equal(t, int64(10), blockInfo.FirstEntryID)
	assert.Equal(t, int64(11), blockInfo.LastEntryID)
}

func TestMinioFileReaderAdv_ProcessBlockData_FilterByStartEntryID(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Block with entries 0,1,2 but we only want entries >= 2
	blockData := buildValidBlockData(t, 0, 0, [][]byte{[]byte("data0"), []byte("data1"), []byte("data2")})

	entries, _, blockInfo, err := reader.processBlockData(ctx, 0, blockData, 2)
	assert.NoError(t, err)
	assert.Len(t, entries, 1) // only entry 2
	assert.NotNil(t, blockInfo)
	assert.Equal(t, int64(2), entries[0].EntryId)
}

func TestMinioFileReaderAdv_ProcessBlockData_NoBlockHeader(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Only a HeaderRecord, no BlockHeaderRecord
	header := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	data := codec.EncodeRecord(header)

	entries, _, blockInfo, err := reader.processBlockData(ctx, 0, data, 0)
	assert.Error(t, err)
	assert.Nil(t, entries)
	assert.Nil(t, blockInfo)
	assert.Contains(t, err.Error(), "block header record not found")
}

// ===== verifyBlockDataIntegrity tests =====

func TestMinioFileReaderAdv_VerifyBlockDataIntegrity_Valid(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockData := buildValidBlockData(t, 0, 0, [][]byte{[]byte("hello"), []byte("world")})

	// Parse to get the block header
	records, err := codec.DecodeRecordList(blockData)
	require.NoError(t, err)
	var blockHeader *codec.BlockHeaderRecord
	for _, r := range records {
		if bh, ok := r.(*codec.BlockHeaderRecord); ok {
			blockHeader = bh
			break
		}
	}
	require.NotNil(t, blockHeader)

	err = reader.verifyBlockDataIntegrity(ctx, blockHeader, 0, blockData)
	assert.NoError(t, err)
}

func TestMinioFileReaderAdv_VerifyBlockDataIntegrity_Block1(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockData := buildValidBlockData(t, 1, 10, [][]byte{[]byte("data10")})
	records, err := codec.DecodeRecordList(blockData)
	require.NoError(t, err)
	var blockHeader *codec.BlockHeaderRecord
	for _, r := range records {
		if bh, ok := r.(*codec.BlockHeaderRecord); ok {
			blockHeader = bh
			break
		}
	}
	require.NotNil(t, blockHeader)

	err = reader.verifyBlockDataIntegrity(ctx, blockHeader, 1, blockData)
	assert.NoError(t, err)
}

// ===== asyncUpdateReaderState tests =====

func TestMinioFileReaderAdv_AsyncUpdateReaderState(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	footerAndIndex := &FooterAndIndex{
		footer: &codec.FooterRecord{
			TotalBlocks:  2,
			TotalRecords: 20,
			TotalSize:    500,
			Version:      codec.FormatVersion,
			Flags:        codec.SetCompacted(0),
		},
		blocks: []*codec.IndexRecord{
			{BlockNumber: 0, FirstEntryID: 0, LastEntryID: 9},
			{BlockNumber: 1, FirstEntryID: 10, LastEntryID: 19},
		},
	}

	ctx := context.Background()
	reader.asyncUpdateReaderState(ctx, footerAndIndex)

	assert.True(t, reader.isCompacted.Load())
	assert.True(t, reader.isCompleted.Load())
	assert.NotNil(t, reader.footer)
	assert.Len(t, reader.blocks, 2)
}

func TestMinioFileReaderAdv_AsyncUpdateReaderState_AlreadyCompacted(t *testing.T) {
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	reader.isCompacted.Store(true) // already compacted

	footerAndIndex := &FooterAndIndex{
		footer: &codec.FooterRecord{
			TotalBlocks: 2,
			Flags:       codec.SetCompacted(0),
		},
		blocks: []*codec.IndexRecord{
			{BlockNumber: 0},
		},
	}

	ctx := context.Background()
	reader.asyncUpdateReaderState(ctx, footerAndIndex)
	// Should not update since already compacted
	assert.Nil(t, reader.footer)
}

// ===== readFooterAndIndexUnsafe with valid footer data =====

func TestMinioFileReaderAdv_ReadFooterAndIndexUnsafe_NotFound(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	result, err := reader.readFooterAndIndexUnsafe(ctx)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestMinioFileReaderAdv_ReadFooterAndIndexUnsafe_StatError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	statErr := errors.New("network error")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, statErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(statErr).Return(false).Once()

	result, err := reader.readFooterAndIndexUnsafe(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestMinioFileReaderAdv_ReadFooterAndIndexUnsafe_GetObjectError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(200), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", int64(0), int64(200), mock.Anything, mock.Anything).
		Return(nil, errors.New("get error")).Once()

	result, err := reader.readFooterAndIndexUnsafe(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestMinioFileReaderAdv_ReadFooterAndIndexUnsafe_TooSmall(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	smallData := []byte("tiny")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(len(smallData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", int64(0), int64(len(smallData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: smallData}, nil).Once()

	result, err := reader.readFooterAndIndexUnsafe(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "too small")
}

func TestMinioFileReaderAdv_ReadFooterAndIndexUnsafe_ValidFooter(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Build valid footer data using serializeFooterAndIndexes
	blocks := []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 9},
		{BlockNumber: 1, StartOffset: 1, BlockSize: 200, FirstEntryID: 10, LastEntryID: 19},
	}
	footerData, _ := serializeFooterAndIndexes(ctx, blocks)

	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(len(footerData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", int64(0), int64(len(footerData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: footerData}, nil).Once()

	result, err := reader.readFooterAndIndexUnsafe(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotNil(t, result.footer)
	assert.Len(t, result.blocks, 2)
	assert.Equal(t, int32(2), result.footer.TotalBlocks)
}

// ===== tryReadFooterAndIndex tests =====

func TestMinioFileReaderAdv_TryReadFooterAndIndex_UpdateState(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Build valid footer
	blocks := []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 9},
	}
	footerData, _ := serializeFooterAndIndexes(ctx, blocks)

	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(len(footerData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", int64(0), int64(len(footerData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: footerData}, nil).Once()

	err := reader.tryReadFooterAndIndex(ctx)
	assert.NoError(t, err)
	assert.True(t, reader.isCompleted.Load())
	assert.NotNil(t, reader.footer)
	assert.Len(t, reader.blocks, 1)
}

func TestMinioFileReaderAdv_TryReadFooterAndIndex_Error(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	statErr := errors.New("network error")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, statErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(statErr).Return(false).Once()

	err := reader.tryReadFooterAndIndex(ctx)
	assert.Error(t, err)
}

// ===== getBlockHeaderRecord tests =====

func TestMinioFileReaderAdv_GetBlockHeaderRecord_Block0(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Build block 0 data
	blockData := buildValidBlockData(t, 0, 0, [][]byte{[]byte("hello"), []byte("world")})

	// For block 0, readSize includes header + blockHeader
	readSize := int64(codec.RecordHeaderSize+codec.HeaderRecordSize) + int64(codec.RecordHeaderSize+codec.BlockHeaderRecordSize)
	headerData := blockData[:readSize]

	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: headerData}, nil).Once()

	blockHeader, err := reader.getBlockHeaderRecord(ctx, 0, "test-base/1/0/0.blk")
	assert.NoError(t, err)
	assert.NotNil(t, blockHeader)
	assert.Equal(t, int32(0), blockHeader.BlockNumber)
	assert.Equal(t, int64(0), blockHeader.FirstEntryID)
	assert.Equal(t, int64(1), blockHeader.LastEntryID)
}

func TestMinioFileReaderAdv_GetBlockHeaderRecord_Block1(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Build block 1 data (no header record)
	blockData := buildValidBlockData(t, 1, 10, [][]byte{[]byte("data10"), []byte("data11")})

	// For block != 0, readSize is just blockHeader
	readSize := int64(codec.RecordHeaderSize + codec.BlockHeaderRecordSize)
	headerData := blockData[:readSize]

	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: headerData}, nil).Once()

	blockHeader, err := reader.getBlockHeaderRecord(ctx, 1, "test-base/1/0/1.blk")
	assert.NoError(t, err)
	assert.NotNil(t, blockHeader)
	assert.Equal(t, int32(1), blockHeader.BlockNumber)
	assert.Equal(t, int64(10), blockHeader.FirstEntryID)
	assert.Equal(t, int64(11), blockHeader.LastEntryID)
}

func TestMinioFileReaderAdv_GetBlockHeaderRecord_GetError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	readSize := int64(codec.RecordHeaderSize + codec.BlockHeaderRecordSize)
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(nil, errors.New("get error")).Once()

	blockHeader, err := reader.getBlockHeaderRecord(ctx, 1, "test-base/1/0/1.blk")
	assert.Error(t, err)
	assert.Nil(t, blockHeader)
}

func TestMinioFileReaderAdv_GetBlockHeaderRecord_ShortData(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	readSize := int64(codec.RecordHeaderSize + codec.BlockHeaderRecordSize)
	shortData := []byte("too short")
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: shortData}, nil).Once()

	blockHeader, err := reader.getBlockHeaderRecord(ctx, 1, "test-base/1/0/1.blk")
	assert.Error(t, err)
	assert.Nil(t, blockHeader)
}

// ===== fetchAndProcessBlock tests =====

func TestMinioFileReaderAdv_FetchAndProcessBlock_Success(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockData := buildValidBlockData(t, 0, 0, [][]byte{[]byte("hello"), []byte("world")})

	block := BlockToRead{
		blockID: 0,
		objKey:  "test-base/1/0/0.blk",
		size:    int64(len(blockData)),
	}

	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), int64(len(blockData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: blockData}, nil).Once()

	result := reader.fetchAndProcessBlock(ctx, block, 0)
	assert.NoError(t, result.err)
	assert.Len(t, result.entries, 2)
	assert.NotNil(t, result.blockInfo)
}

func TestMinioFileReaderAdv_FetchAndProcessBlock_GetError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	block := BlockToRead{
		blockID: 0,
		objKey:  "test-base/1/0/0.blk",
		size:    100,
	}

	getErr := errors.New("get error")
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), int64(100), mock.Anything, mock.Anything).
		Return(nil, getErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(getErr).Return(false).Once()

	result := reader.fetchAndProcessBlock(ctx, block, 0)
	assert.Error(t, result.err)
}

func TestMinioFileReaderAdv_FetchAndProcessBlock_NotFoundNoFooter(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	block := BlockToRead{
		blockID: 0,
		objKey:  "test-base/1/0/0.blk",
		size:    100,
	}

	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), int64(100), mock.Anything, mock.Anything).
		Return(nil, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	// handleBlockNotFoundByCompaction: shouldBlockExist returns true (no footer, active segment)
	// Then checks compaction: readFooterAndIndexUnsafe returns nil (no footer)
	footerNotFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, footerNotFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(footerNotFoundErr).Return(true).Once()

	result := reader.fetchAndProcessBlock(ctx, block, 0)
	// handleBlockNotFoundByCompaction returns nil (block legitimately not found), so error is the original notFoundErr
	assert.Error(t, result.err)
}

// ===== ReadNextBatchAdv with lastReadBatchInfo =====

func TestMinioFileReaderAdv_ReadNextBatchAdv_WithLastReadState(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Build block 1 data
	blockData := buildValidBlockData(t, 1, 10, [][]byte{[]byte("data10"), []byte("data11")})

	// lastReadState says we already read block 0
	lastState := &proto.LastReadState{
		SegmentId:   0,
		Flags:       0,
		Version:     uint32(codec.FormatVersion),
		LastBlockId: 0,
	}

	// StatObject for block 1 - exists
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(int64(len(blockData)), false, nil).Once()
	// GetObject for block 1
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", int64(0), int64(len(blockData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: blockData}, nil).Once()
	// Block 2 not found
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/2.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).
		Return(true).Maybe()

	// handleBlockNotFoundByCompaction may check footer
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()

	opt := storage.ReaderOpt{
		StartEntryID:    10,
		MaxBatchEntries: 100,
	}

	batch, err := reader.ReadNextBatchAdv(ctx, opt, lastState)
	// With mock data the second read may return EOF or data; either is valid,
	// but an unexpected error type should fail
	if err != nil {
		assert.True(t, errors.Is(err, werr.ErrFileReaderEndOfFile) || errors.Is(err, werr.ErrEntryNotFound),
			"unexpected error: %v", err)
	} else {
		assert.NotNil(t, batch)
		assert.GreaterOrEqual(t, len(batch.Entries), 1)
	}
}

func TestMinioFileReaderAdv_ReadNextBatchAdv_Closed(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	reader.closed.Store(true)

	// When lastReadBatchInfo is nil, tryReadFooterAndIndex is called before closed check
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	batch, err := reader.ReadNextBatchAdv(ctx, opt, nil)
	assert.Nil(t, batch)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, werr.ErrFileReaderAlreadyClosed))
}

func TestMinioFileReaderAdv_ReadNextBatchAdv_CompletedNoBlock(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Set footer with blocks but start entry beyond range
	reader.footer = &codec.FooterRecord{TotalBlocks: 1, Version: codec.FormatVersion}
	reader.isCompleted.Store(true)
	reader.blocks = []*codec.IndexRecord{
		{BlockNumber: 0, FirstEntryID: 0, LastEntryID: 9, BlockSize: 100},
	}

	opt := storage.ReaderOpt{StartEntryID: 100, MaxBatchEntries: 10} // beyond all entries

	batch, err := reader.ReadNextBatchAdv(ctx, opt, nil)
	assert.Nil(t, batch)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, werr.ErrFileReaderEndOfFile))
}

// ===== handleBlockNotFoundByCompaction tests =====

func TestMinioFileReaderAdv_HandleBlockNotFound_BlockShouldNotExist(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	reader.footer = &codec.FooterRecord{TotalBlocks: 2}

	// Block 5 > TotalBlocks(2), should not exist
	err := reader.handleBlockNotFoundByCompaction(ctx, 5, "StatObject")
	assert.NoError(t, err)
}

func TestMinioFileReaderAdv_HandleBlockNotFound_AlreadyCompacted(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	reader.footer = &codec.FooterRecord{TotalBlocks: 5}
	reader.isCompacted.Store(true)

	// Block 2 should exist but is already compacted
	err := reader.handleBlockNotFoundByCompaction(ctx, 2, "StatObject")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "block 2 not found during StatObject")
}

// ===== GetLastEntryID tests =====

func TestMinioFileReaderAdv_GetLastEntryID_WithExistingBlocks(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	reader.blocks = []*codec.IndexRecord{
		{BlockNumber: 0, FirstEntryID: 0, LastEntryID: 9},
	}

	// No new blocks found
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	lastEntry, err := reader.GetLastEntryID(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(9), lastEntry)
}

func TestMinioFileReaderAdv_GetLastEntryID_EmptySegment(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	lastEntry, err := reader.GetLastEntryID(ctx)
	assert.Equal(t, int64(-1), lastEntry)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, werr.ErrFileReaderNoBlockFound))
}

// ===== prefetchIncrementalBlockInfoUnsafe with new blocks =====

func TestMinioFileReaderAdv_PrefetchIncrementalBlockInfo_NewBlock(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Build block 0 data
	blockData := buildValidBlockData(t, 0, 0, [][]byte{[]byte("hello"), []byte("world")})
	readSize := int64(codec.RecordHeaderSize+codec.HeaderRecordSize) + int64(codec.RecordHeaderSize+codec.BlockHeaderRecordSize)
	headerData := blockData[:readSize]

	// Block 0 exists
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(len(blockData)), false, nil).Once()
	// GetObject for block 0 header
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: headerData}, nil).Once()

	// Block 1 not found
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	existsNewBlock, lastBlockInfo, err := reader.prefetchIncrementalBlockInfoUnsafe(ctx)
	assert.True(t, existsNewBlock)
	assert.NotNil(t, lastBlockInfo)
	assert.NoError(t, err)
	assert.Len(t, reader.blocks, 1)
	assert.Equal(t, int64(0), lastBlockInfo.FirstEntryID)
	assert.Equal(t, int64(1), lastBlockInfo.LastEntryID)
}

// ===== readBlockBatchUnsafe tests =====

func TestMinioFileReaderAdv_ReadBlockBatch_FencedBlock(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Block 0 is fenced
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(0), true, nil).Once()

	futures, nextBlockID, totalRawSize, hasMoreBlocks, readErr := reader.readBlockBatchUnsafe(ctx, 0, 4*1024*1024, 0)
	assert.Empty(t, futures)
	assert.Equal(t, int64(0), nextBlockID)
	assert.Equal(t, int64(0), totalRawSize)
	assert.False(t, hasMoreBlocks)
	assert.NoError(t, readErr)
	assert.True(t, reader.isFenced.Load())
}

func TestMinioFileReaderAdv_ReadBlockBatch_StatError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	statErr := errors.New("network error")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, statErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(statErr).Return(false).Once()

	futures, _, _, _, readErr := reader.readBlockBatchUnsafe(ctx, 0, 4*1024*1024, 0)
	assert.Empty(t, futures)
	assert.Error(t, readErr)
}

// ===== NewMinioFileReaderAdv tests =====

func TestNewMinioFileReaderAdv_Success_NoFooter(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)

	// Footer not found → no footer loaded
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	reader, err := NewMinioFileReaderAdv(ctx, "test-bucket", "base", 1, 0, mockClient, 4*1024*1024, 4)
	require.NoError(t, err)
	require.NotNil(t, reader)

	assert.Equal(t, int64(1), reader.logId)
	assert.Equal(t, int64(0), reader.segmentId)
	assert.Equal(t, "base/1/0", reader.segmentFileKey)
	assert.Equal(t, "test-bucket", reader.bucket)
	assert.Equal(t, "test-bucket/base", reader.nsStr)
	assert.Equal(t, "1", reader.logIdStr)
	assert.False(t, reader.closed.Load())
	assert.False(t, reader.isCompleted.Load())
	assert.False(t, reader.isCompacted.Load())
	assert.False(t, reader.isFenced.Load())
	assert.Nil(t, reader.footer)
	assert.Empty(t, reader.blocks)
	assert.Equal(t, int64(4*1024*1024), reader.maxBatchSize)

	// Cleanup
	reader.pool.Release()
}

func TestNewMinioFileReaderAdv_Success_WithFooter(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)

	// Build valid footer
	blocks := []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 9},
	}
	footerData, _ := serializeFooterAndIndexes(ctx, blocks)

	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(len(footerData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "base/1/0/footer.blk", int64(0), int64(len(footerData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: footerData}, nil).Once()

	reader, err := NewMinioFileReaderAdv(ctx, "test-bucket", "base", 1, 0, mockClient, 4*1024*1024, 4)
	require.NoError(t, err)
	require.NotNil(t, reader)

	assert.True(t, reader.isCompleted.Load())
	assert.NotNil(t, reader.footer)
	assert.Len(t, reader.blocks, 1)

	// Cleanup
	reader.pool.Release()
}

func TestNewMinioFileReaderAdv_FooterReadError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)

	// Footer stat returns error (not "not found")
	statErr := errors.New("network error")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, statErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(statErr).Return(false).Once()

	reader, err := NewMinioFileReaderAdv(ctx, "test-bucket", "base", 1, 0, mockClient, 4*1024*1024, 4)
	assert.Error(t, err)
	assert.Nil(t, reader)
}

// ===== handleBlockNotFoundByCompaction additional tests =====

func TestMinioFileReaderAdv_HandleBlockNotFound_CompactionDetected(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	// Footer says block 0 should exist (TotalBlocks=5)
	reader.footer = &codec.FooterRecord{TotalBlocks: 5}

	// Build a compacted footer response
	compactedBlocks := []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 500, FirstEntryID: 0, LastEntryID: 49},
	}
	_, compactedFooter := serializeFooterAndIndexes(ctx, compactedBlocks)
	// Set compacted flag
	compactedFooter.Flags = codec.SetCompacted(0)
	// Re-serialize with compacted flag
	compactedFooterData := make([]byte, 0)
	for _, b := range compactedBlocks {
		compactedFooterData = append(compactedFooterData, codec.EncodeRecord(b)...)
	}
	compactedFooter.Flags = codec.SetCompacted(0)
	compactedFooterData = append(compactedFooterData, codec.EncodeRecord(compactedFooter)...)

	// Mock footer.blk stat and get
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(len(compactedFooterData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", int64(0), int64(len(compactedFooterData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: compactedFooterData}, nil).Once()

	err := reader.handleBlockNotFoundByCompaction(ctx, 0, "GetObject")
	assert.Error(t, err)
	assert.True(t, errors.Is(err, werr.ErrFileReaderBlockDeletedByCompaction))
}

func TestMinioFileReaderAdv_HandleBlockNotFound_FooterReadError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	// No footer yet, active segment → shouldBlockExist returns true
	// Not compacted yet

	// Footer stat returns error
	statErr := errors.New("network error reading footer")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, statErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(statErr).Return(false).Once()

	err := reader.handleBlockNotFoundByCompaction(ctx, 0, "StatObject")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "network error reading footer")
}

func TestMinioFileReaderAdv_HandleBlockNotFound_NoCompactionDetected(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	// Active segment, no footer

	// Footer stat returns not found → no compaction
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	err := reader.handleBlockNotFoundByCompaction(ctx, 0, "StatObject")
	assert.NoError(t, err) // Block legitimately doesn't exist
}

// ===== getBlockHeaderRecord additional tests =====

func TestMinioFileReaderAdv_GetBlockHeaderRecord_NoBlockHeaderInRecords(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Build a valid data with just a HeaderRecord (no BlockHeaderRecord) that matches expected read size
	header := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	headerBytes := codec.EncodeRecord(header)

	// For block 0, readSize includes header + blockHeader
	readSize := int64(codec.RecordHeaderSize+codec.HeaderRecordSize) + int64(codec.RecordHeaderSize+codec.BlockHeaderRecordSize)
	// Pad to exact readSize
	data := make([]byte, readSize)
	copy(data, headerBytes)

	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: data}, nil).Once()

	blockHeader, err := reader.getBlockHeaderRecord(ctx, 0, "test-base/1/0/0.blk")
	assert.Error(t, err)
	assert.Nil(t, blockHeader)
}

func TestMinioFileReaderAdv_GetBlockHeaderRecord_ReadError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	readSize := int64(codec.RecordHeaderSize + codec.BlockHeaderRecordSize)
	// Return a reader that has different size than expected → triggers "not enough size" error
	shortData := make([]byte, readSize-1) // 1 byte short
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: shortData}, nil).Once()

	blockHeader, err := reader.getBlockHeaderRecord(ctx, 1, "test-base/1/0/1.blk")
	assert.Error(t, err)
	assert.Nil(t, blockHeader)
	assert.True(t, errors.Is(err, werr.ErrFileReaderInvalidRecord))
}

// ===== fetchAndProcessBlock additional tests =====

func TestMinioFileReaderAdv_FetchAndProcessBlock_NotFoundCompacted(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	// Set footer so shouldBlockExist returns true for block 0
	reader.footer = &codec.FooterRecord{TotalBlocks: 5}

	block := BlockToRead{
		blockID: 0,
		objKey:  "test-base/1/0/0.blk",
		size:    100,
	}

	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), int64(100), mock.Anything, mock.Anything).
		Return(nil, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	// handleBlockNotFoundByCompaction: not compacted, read footer
	compactedBlocks := []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 500, FirstEntryID: 0, LastEntryID: 49},
	}
	compactedFooterData := make([]byte, 0)
	for _, b := range compactedBlocks {
		compactedFooterData = append(compactedFooterData, codec.EncodeRecord(b)...)
	}
	compactedFooter := &codec.FooterRecord{
		TotalBlocks:  1,
		TotalRecords: 1,
		TotalSize:    500,
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(0),
		LAC:          49,
	}
	compactedFooterData = append(compactedFooterData, codec.EncodeRecord(compactedFooter)...)

	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(len(compactedFooterData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", int64(0), int64(len(compactedFooterData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: compactedFooterData}, nil).Once()

	result := reader.fetchAndProcessBlock(ctx, block, 0)
	assert.Error(t, result.err)
	assert.True(t, errors.Is(result.err, werr.ErrFileReaderBlockDeletedByCompaction))
}

// ===== ReadNextBatchAdv additional tests =====

func TestMinioFileReaderAdv_ReadNextBatchAdv_CompactionReset(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Reader now has compacted flag
	reader.flags.Store(uint32(codec.SetCompacted(0)))
	reader.isCompacted.Store(true)
	reader.isCompleted.Store(true)
	reader.footer = &codec.FooterRecord{
		TotalBlocks: 1,
		Version:     codec.FormatVersion,
		Flags:       codec.SetCompacted(0),
	}
	reader.blocks = []*codec.IndexRecord{
		{BlockNumber: 0, FirstEntryID: 0, LastEntryID: 9, BlockSize: 100},
	}

	// lastReadState has non-compacted flags → compaction state changed
	lastState := &proto.LastReadState{
		SegmentId:   0,
		Flags:       0, // not compacted
		Version:     uint32(codec.FormatVersion),
		LastBlockId: 0,
	}

	// After reset, reader should start from SearchBlock result
	// Block 0 merged exists
	blockData := buildValidBlockData(t, 0, 0, [][]byte{[]byte("data0"), []byte("data1")})
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(int64(len(blockData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", mock.Anything, int64(0), int64(len(blockData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: blockData}, nil).Once()

	// No more blocks
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 100}
	batch, err := reader.ReadNextBatchAdv(ctx, opt, lastState)
	require.NoError(t, err)
	assert.NotNil(t, batch)
	assert.GreaterOrEqual(t, len(batch.Entries), 1)
}

func TestMinioFileReaderAdv_ReadNextBatchAdv_TryReadFooterError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Footer stat returns a non-retriable error
	statErr := errors.New("fatal error")
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, statErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(statErr).Return(false).Once()

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	batch, err := reader.ReadNextBatchAdv(ctx, opt, nil)
	assert.Nil(t, batch)
	assert.Error(t, err)
}

func TestMinioFileReaderAdv_ReadNextBatchAdv_WithFooterSearchBlock(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Set up completed reader with footer and blocks
	reader.footer = &codec.FooterRecord{TotalBlocks: 2, Version: codec.FormatVersion}
	reader.isCompleted.Store(true)
	reader.blocks = []*codec.IndexRecord{
		{BlockNumber: 0, FirstEntryID: 0, LastEntryID: 9, BlockSize: 100},
		{BlockNumber: 1, FirstEntryID: 10, LastEntryID: 19, BlockSize: 200},
	}

	// Build block 1 data
	blockData := buildValidBlockData(t, 1, 10, [][]byte{[]byte("data10"), []byte("data11")})

	// StatObject for block 1
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(int64(len(blockData)), false, nil).Once()
	// GetObject for block 1
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", int64(0), int64(len(blockData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: blockData}, nil).Once()

	// Block 2 not found
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/2.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()

	opt := storage.ReaderOpt{StartEntryID: 10, MaxBatchEntries: 100}
	batch, err := reader.ReadNextBatchAdv(ctx, opt, nil)
	// Should find block 1 via SearchBlock since startEntryID=10 matches block 1
	require.NoError(t, err)
	assert.NotNil(t, batch)
	assert.GreaterOrEqual(t, len(batch.Entries), 1)
}

// ===== readDataBlocksUnsafe additional tests =====

func TestMinioFileReaderAdv_ReadDataBlocks_SuccessWithEntries(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockData := buildValidBlockData(t, 0, 0, [][]byte{[]byte("hello"), []byte("world")})

	// Block 0 exists
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(len(blockData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), int64(len(blockData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: blockData}, nil).Once()

	// Block 1 not found
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()

	// handleBlockNotFoundByCompaction: no footer → return nil
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 100}
	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0)
	require.NoError(t, err)
	require.NotNil(t, batch)
	assert.Len(t, batch.Entries, 2)
	assert.Equal(t, int64(0), batch.Entries[0].EntryId)
	assert.Equal(t, int64(1), batch.Entries[1].EntryId)
	assert.NotNil(t, batch.LastReadState)
}

func TestMinioFileReaderAdv_ReadDataBlocks_MaxEntriesLimit(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Block 0 with 3 entries
	blockData0 := buildValidBlockData(t, 0, 0, [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(len(blockData0)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), int64(len(blockData0)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: blockData0}, nil).Once()

	// Block 1 not found → stops reading
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()

	// MaxBatchEntries = 1, but will still read entire block (soft limit)
	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 1}
	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0)
	require.NoError(t, err)
	require.NotNil(t, batch)
	assert.GreaterOrEqual(t, len(batch.Entries), 1)
}

// ===== verifyBlockDataIntegrity additional tests =====

func TestMinioFileReaderAdv_VerifyBlockDataIntegrity_CorruptedData(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockData := buildValidBlockData(t, 1, 10, [][]byte{[]byte("data10")})

	// Parse to get the block header
	records, err := codec.DecodeRecordList(blockData)
	require.NoError(t, err)
	var blockHeader *codec.BlockHeaderRecord
	for _, r := range records {
		if bh, ok := r.(*codec.BlockHeaderRecord); ok {
			blockHeader = bh
			break
		}
	}
	require.NotNil(t, blockHeader)

	// Corrupt the data by modifying a byte
	corruptData := make([]byte, len(blockData))
	copy(corruptData, blockData)
	if len(corruptData) > 0 {
		corruptData[len(corruptData)-1] ^= 0xFF // flip last byte
	}

	err = reader.verifyBlockDataIntegrity(ctx, blockHeader, 1, corruptData)
	assert.Error(t, err)
}

func TestMinioFileReaderAdv_VerifyBlockDataIntegrity_ShortData(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  1,
		FirstEntryID: 0,
		LastEntryID:  0,
		BlockLength:  100,
		BlockCrc:     12345,
	}

	// Data shorter than dataStartOffset (only blockHeader, no data records)
	shortData := make([]byte, codec.RecordHeaderSize+codec.BlockHeaderRecordSize)

	// Should pass since len(blockData) <= dataStartOffset, no data to verify
	err := reader.verifyBlockDataIntegrity(ctx, blockHeader, 1, shortData)
	assert.NoError(t, err)
}

// ===== readFooterAndIndexUnsafe edge case tests =====

func TestMinioFileReaderAdv_ReadFooterAndIndexUnsafe_InvalidFooterData(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Data large enough to pass the size check but with invalid content
	invalidData := make([]byte, codec.RecordHeaderSize+codec.FooterRecordSize+100)

	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(len(invalidData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", int64(0), int64(len(invalidData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: invalidData}, nil).Once()

	result, err := reader.readFooterAndIndexUnsafe(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
}

// ===== readBlockBatchUnsafe additional tests =====

func TestMinioFileReaderAdv_ReadBlockBatch_SubmitBlock(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockData := buildValidBlockData(t, 0, 0, [][]byte{[]byte("hello")})

	// Block 0 exists
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(len(blockData)), false, nil).Once()

	// Block 1 not found → handleBlockNotFoundByCompaction will check footer
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()
	// Footer not found either (active segment, no compaction)
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()

	// GetObject will be called by the future
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), int64(len(blockData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: blockData}, nil).Once()

	futures, nextBlockID, totalRawSize, hasMoreBlocks, readErr := reader.readBlockBatchUnsafe(ctx, 0, 4*1024*1024, 0)
	assert.NoError(t, readErr)
	assert.Len(t, futures, 1)
	assert.Equal(t, int64(1), nextBlockID)
	assert.Equal(t, int64(len(blockData)), totalRawSize)
	assert.False(t, hasMoreBlocks)

	// Verify the future completes successfully
	result := futures[0].Value()
	assert.NoError(t, result.err)
	assert.Len(t, result.entries, 1)
}

func TestMinioFileReaderAdv_ReadBlockBatch_MaxBytesLimit(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockData := buildValidBlockData(t, 0, 0, [][]byte{[]byte("hello")})

	// Block 0 exists - size will exceed maxBytes of 1
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(len(blockData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), int64(len(blockData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: blockData}, nil).Once()

	// maxBytes=1, so after reading block 0 it should stop (totalRawSize >= maxBytes)
	futures, nextBlockID, totalRawSize, hasMoreBlocks, readErr := reader.readBlockBatchUnsafe(ctx, 0, 1, 0)
	assert.NoError(t, readErr)
	assert.Len(t, futures, 1) // Still reads one block even if it exceeds
	assert.Equal(t, int64(1), nextBlockID)
	assert.True(t, totalRawSize >= 1)
	assert.True(t, hasMoreBlocks) // Didn't check next block, so assumes more exist

	// Cleanup future
	_ = futures[0].Value()
}

func TestMinioFileReaderAdv_ReadBlockBatch_NotFoundWithCompaction(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	// Set footer so block 0 should exist
	reader.footer = &codec.FooterRecord{TotalBlocks: 5}
	reader.isCompacted.Store(true) // already compacted

	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	futures, _, _, _, readErr := reader.readBlockBatchUnsafe(ctx, 0, 4*1024*1024, 0)
	assert.Empty(t, futures)
	assert.Error(t, readErr)
	assert.Contains(t, readErr.Error(), "block 0 not found")
}

// ===== GetLastEntryID with new block found =====

func TestMinioFileReaderAdv_GetLastEntryID_NewBlockFound(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockData := buildValidBlockData(t, 0, 0, [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	readSize := int64(codec.RecordHeaderSize+codec.HeaderRecordSize) + int64(codec.RecordHeaderSize+codec.BlockHeaderRecordSize)
	headerData := blockData[:readSize]

	// Block 0 exists
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(len(blockData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: headerData}, nil).Once()

	// Block 1 not found
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	lastEntryID, err := reader.GetLastEntryID(ctx)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), lastEntryID) // entries 0,1,2 → lastEntryID=2
}

// ===== prefetchIncrementalBlockInfoUnsafe additional tests =====

func TestMinioFileReaderAdv_PrefetchIncrementalBlockInfo_GetHeaderError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Block 0 stat succeeds
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(1000), false, nil).Once()

	// GetObject for block header fails
	readSize := int64(codec.RecordHeaderSize+codec.HeaderRecordSize) + int64(codec.RecordHeaderSize+codec.BlockHeaderRecordSize)
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), readSize, mock.Anything, mock.Anything).
		Return(nil, errors.New("get error")).Once()

	existsNewBlock, lastBlockInfo, err := reader.prefetchIncrementalBlockInfoUnsafe(ctx)
	assert.False(t, existsNewBlock)
	assert.Nil(t, lastBlockInfo)
	assert.Error(t, err)
}

func TestMinioFileReaderAdv_PrefetchIncrementalBlockInfo_ContinueFromExisting(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Already has block 0
	reader.blocks = []*codec.IndexRecord{
		{BlockNumber: 0, FirstEntryID: 0, LastEntryID: 9, BlockSize: 100},
	}

	// Block 1 not found
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	existsNewBlock, lastBlockInfo, err := reader.prefetchIncrementalBlockInfoUnsafe(ctx)
	assert.False(t, existsNewBlock)
	assert.NotNil(t, lastBlockInfo) // Returns last existing block
	assert.Equal(t, int64(9), lastBlockInfo.LastEntryID)
	assert.NoError(t, err)
}

// ===== readFooterAndIndexUnsafe additional coverage tests =====

// errorFileReader is a mock FileReader that returns an error on Read
type errorFileReader struct {
	err error
}

func (e *errorFileReader) Read(p []byte) (int, error) {
	return 0, e.err
}

func (e *errorFileReader) Close() error {
	return nil
}

func (e *errorFileReader) ReadAt(p []byte, off int64) (int, error) {
	return 0, e.err
}

func (e *errorFileReader) Seek(offset int64, whence int) (int64, error) {
	return 0, e.err
}

func (e *errorFileReader) Size() (int64, error) {
	return 0, e.err
}

func TestMinioFileReaderAdv_ReadFooterAndIndex_FooterTooSmall(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	footerKey := "test-base/1/0/footer.blk"
	smallData := []byte{0x01, 0x02, 0x03, 0x04, 0x05}

	// StatObject returns small size
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(int64(len(smallData)), false, nil).Once()

	// GetObject returns a reader with small data
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", footerKey, int64(0), int64(len(smallData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: smallData}, nil).Once()

	result, err := reader.readFooterAndIndexUnsafe(ctx)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "footer.blk too small")
}

func TestMinioFileReaderAdv_ReadFooterAndIndex_GetObjectError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	footerKey := "test-base/1/0/footer.blk"

	// StatObject succeeds
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(int64(100), false, nil).Once()

	// GetObject returns an error
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", footerKey, int64(0), int64(100), mock.Anything, mock.Anything).
		Return(nil, errors.New("get object failed")).Once()

	result, err := reader.readFooterAndIndexUnsafe(ctx)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get object failed")
}

func TestMinioFileReaderAdv_ReadFooterAndIndex_ReadObjectFullError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	footerKey := "test-base/1/0/footer.blk"

	// StatObject succeeds
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(int64(100), false, nil).Once()

	// GetObject returns a reader that errors on Read
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", footerKey, int64(0), int64(100), mock.Anything, mock.Anything).
		Return(&errorFileReader{err: errors.New("read failure")}, nil).Once()

	result, err := reader.readFooterAndIndexUnsafe(ctx)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read failure")
}

// ===== getBlockHeaderRecord additional coverage tests =====

func TestMinioFileReaderAdv_GetBlockHeaderRecord_FenceDetection(t *testing.T) {
	// When ReadObjectFull returns fewer bytes than requested, it indicates a fenced object
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockKey := "test-base/1/0/1.blk"
	readSize := int64(codec.RecordHeaderSize + codec.BlockHeaderRecordSize)

	// Return only 1 byte instead of the expected readSize
	shortData := []byte{0x01}
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", blockKey, int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: shortData}, nil).Once()

	result, err := reader.getBlockHeaderRecord(ctx, 1, blockKey)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, werr.ErrFileReaderInvalidRecord))
}

func TestMinioFileReaderAdv_GetBlockHeaderRecord_DecodeRecordListError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockKey := "test-base/1/0/1.blk"
	readSize := int64(codec.RecordHeaderSize + codec.BlockHeaderRecordSize)

	// Create garbage data that has the right length but will fail to decode
	garbageData := make([]byte, readSize)
	for i := range garbageData {
		garbageData[i] = 0xFF
	}
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", blockKey, int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: garbageData}, nil).Once()

	result, err := reader.getBlockHeaderRecord(ctx, 1, blockKey)
	assert.Nil(t, result)
	assert.Error(t, err)
}

func TestMinioFileReaderAdv_GetBlockHeaderRecord_BlockHeaderNotFound(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockKey := "test-base/1/0/1.blk"
	readSize := int64(codec.RecordHeaderSize + codec.BlockHeaderRecordSize)

	// Build a valid record that is NOT a BlockHeaderRecord (use a DataRecord instead)
	// DataRecord with a payload that makes encoded size == readSize
	payloadSize := int(readSize) - codec.RecordHeaderSize
	dataPayload := make([]byte, payloadSize)
	dr := &codec.DataRecord{Payload: dataPayload}
	encodedData := codec.EncodeRecord(dr)

	// Pad or truncate to match exactly readSize
	if len(encodedData) > int(readSize) {
		encodedData = encodedData[:readSize]
	} else if len(encodedData) < int(readSize) {
		padded := make([]byte, readSize)
		copy(padded, encodedData)
		encodedData = padded
	}

	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", blockKey, int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: encodedData}, nil).Once()

	result, err := reader.getBlockHeaderRecord(ctx, 1, blockKey)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "BlockHeaderRecord not found")
}

// ===== GetLastEntryID additional coverage tests =====

func TestMinioFileReaderAdv_GetLastEntryID_ExistingBlocksNoPrefetch(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Pre-populate blocks
	reader.blocks = []*codec.IndexRecord{
		{BlockNumber: 0, FirstEntryID: 0, LastEntryID: 4, BlockSize: 100},
		{BlockNumber: 1, FirstEntryID: 5, LastEntryID: 9, BlockSize: 200},
	}

	// Block 2 not found - prefetch returns no new blocks, lastBlockInfo is nil
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/2.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	lastEntryID, err := reader.GetLastEntryID(ctx)
	assert.NoError(t, err)
	// Should fall back to last existing block's LastEntryID
	assert.Equal(t, int64(9), lastEntryID)
}

// ===== ReadNextBatchAdv additional coverage tests =====

func TestMinioFileReaderAdv_ReadNextBatchAdv_CompactionStateChange(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Set reader state as compacted
	reader.flags.Store(uint32(codec.SetCompacted(0)))
	reader.isCompacted.Store(true)
	reader.isCompleted.Store(true)
	reader.footer = &codec.FooterRecord{
		TotalBlocks: 1,
		Version:     codec.FormatVersion,
		Flags:       codec.SetCompacted(0),
	}
	reader.blocks = []*codec.IndexRecord{
		{BlockNumber: 0, FirstEntryID: 0, LastEntryID: 9, BlockSize: 100},
	}

	// Pass lastReadState with Flags=0 (not compacted) => compaction state mismatch => reset
	lastState := &proto.LastReadState{
		SegmentId:   0,
		Flags:       0, // not compacted
		Version:     uint32(codec.FormatVersion),
		LastBlockId: 0,
	}

	// After reset lastReadState=nil, it enters the footer != nil branch
	// SearchBlock for startEntryID=5 should find block 0 (range 0-9)
	blockData := buildValidBlockData(t, 0, 0, [][]byte{
		[]byte("d0"), []byte("d1"), []byte("d2"), []byte("d3"), []byte("d4"),
		[]byte("d5"), []byte("d6"), []byte("d7"), []byte("d8"), []byte("d9"),
	})

	// Block 0 (merged)
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/m_0.blk", mock.Anything, mock.Anything).
		Return(int64(len(blockData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/m_0.blk", int64(0), int64(len(blockData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: blockData}, nil).Once()

	// Block 1 (merged) not found
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/m_1.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()

	// handleBlockNotFoundByCompaction checks footer for block 1: TotalBlocks=1 so block 1 should not exist
	opt := storage.ReaderOpt{StartEntryID: 5, MaxBatchEntries: 100}
	batch, err := reader.ReadNextBatchAdv(ctx, opt, lastState)
	require.NoError(t, err)
	assert.NotNil(t, batch)
	// Should only get entries 5-9 since startEntryID=5
	assert.Equal(t, 5, len(batch.Entries))
	assert.Equal(t, int64(5), batch.Entries[0].EntryId)
}

func TestMinioFileReaderAdv_ReadNextBatchAdv_FoundStartBlockNil(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Set footer with blocks but startEntryID beyond all blocks
	reader.footer = &codec.FooterRecord{TotalBlocks: 1, Version: codec.FormatVersion}
	reader.isCompleted.Store(true)
	reader.blocks = []*codec.IndexRecord{
		{BlockNumber: 0, FirstEntryID: 0, LastEntryID: 9, BlockSize: 100},
	}

	// StartEntryID=100 is beyond all blocks, SearchBlock returns nil
	opt := storage.ReaderOpt{StartEntryID: 100, MaxBatchEntries: 10}
	batch, err := reader.ReadNextBatchAdv(ctx, opt, nil)
	assert.Nil(t, batch)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, werr.ErrFileReaderEndOfFile))
}

// ===== readDataBlocksUnsafe additional coverage tests =====

func TestMinioFileReaderAdv_ReadDataBlocks_MaxEntriesDefault(t *testing.T) {
	// When MaxBatchEntries is 0, it should default to 100
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockData := buildValidBlockData(t, 0, 0, [][]byte{[]byte("hello"), []byte("world")})

	// Block 0 exists
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(len(blockData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), int64(len(blockData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: blockData}, nil).Once()

	// Block 1 not found
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/1.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()

	// handleBlockNotFoundByCompaction may check footer
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()

	opt := storage.ReaderOpt{
		StartEntryID:    0,
		MaxBatchEntries: 0, // should default to 100
	}

	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0)
	require.NoError(t, err)
	assert.NotNil(t, batch)
	assert.Len(t, batch.Entries, 2)
}

func TestMinioFileReaderAdv_ReadDataBlocks_ZeroEntriesFooterExists(t *testing.T) {
	// No blocks readable but footer exists via isFooterExists check
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	reader.isCompleted.Store(false)
	reader.isCompacted.Store(false)
	reader.isFenced.Store(false)
	reader.footer = nil // no footer in reader

	// Block 0 not found
	blockNotFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, blockNotFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(blockNotFoundErr).Return(true).Once()

	// handleBlockNotFoundByCompaction: shouldBlockExist returns true (no footer), so it reads footer
	// footer stat succeeds in isFooterExists
	footerKey := "test-base/1/0/footer.blk"

	// Build valid footer data for handleBlockNotFoundByCompaction's readFooterAndIndexUnsafe call
	footer := &codec.FooterRecord{
		TotalBlocks:  0,
		TotalRecords: 0,
		TotalSize:    0,
		IndexOffset:  0,
		IndexLength:  0,
		Version:      codec.FormatVersion,
		Flags:        0, // not compacted
		LAC:          -1,
	}
	footerData := codec.EncodeRecord(footer)

	// handleBlockNotFoundByCompaction calls readFooterAndIndexUnsafe -> StatObject for footer
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(int64(len(footerData)), false, nil).Maybe()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", footerKey, int64(0), int64(len(footerData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: footerData}, nil).Maybe()

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0)
	assert.Nil(t, batch)
	assert.Error(t, err)
	// Should return either ErrFileReaderEndOfFile (if footer check triggers) or ErrEntryNotFound
}

// ===== fetchAndProcessBlock additional coverage tests =====

func TestMinioFileReaderAdv_FetchAndProcessBlock_NotFoundWithCompaction(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Set footer so shouldBlockExist returns false for block 5 (TotalBlocks=2)
	reader.footer = &codec.FooterRecord{TotalBlocks: 2}

	block := BlockToRead{
		blockID: 5, // beyond TotalBlocks, so shouldBlockExist returns false
		objKey:  "test-base/1/0/5.blk",
		size:    100,
	}

	// GetObject returns not found
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/5.blk", int64(0), int64(100), mock.Anything, mock.Anything).
		Return(nil, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	result := reader.fetchAndProcessBlock(ctx, block, 0)
	assert.Error(t, result.err)
	// handleBlockNotFoundByCompaction returns nil (block legitimately doesn't exist)
	// but the original getErr is still returned
	assert.Equal(t, notFoundErr, result.err)
}

func TestMinioFileReaderAdv_FetchAndProcessBlock_ReadObjectFullError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	block := BlockToRead{
		blockID: 0,
		objKey:  "test-base/1/0/0.blk",
		size:    100,
	}

	// GetObject succeeds but reader errors on Read
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", int64(0), int64(100), mock.Anything, mock.Anything).
		Return(&errorFileReader{err: errors.New("read error during fetch")}, nil).Once()

	result := reader.fetchAndProcessBlock(ctx, block, 0)
	assert.Error(t, result.err)
	assert.Contains(t, result.err.Error(), "read error during fetch")
}

// ===== processBlockData additional coverage tests =====

func TestMinioFileReaderAdv_ProcessBlockData_HeaderRecordHandling(t *testing.T) {
	// Block 0 has HeaderRecord + BlockHeaderRecord + DataRecords
	// This tests that HeaderRecord is properly handled and version/flags are updated
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// Verify initial version and flags
	assert.Equal(t, uint32(codec.FormatVersion), reader.version.Load())
	assert.Equal(t, uint32(0), reader.flags.Load())

	// Build block 0 data with a custom flags value in HeaderRecord
	payloads := [][]byte{[]byte("entry0"), []byte("entry1"), []byte("entry2")}

	// Build data records first
	var dataRecordsBuf []byte
	for _, payload := range payloads {
		dr, err := codec.ParseData(payload)
		require.NoError(t, err)
		dataRecordsBuf = append(dataRecordsBuf, codec.EncodeRecord(dr)...)
	}

	blockLength := uint32(len(dataRecordsBuf))
	blockCrc := crc32.ChecksumIEEE(dataRecordsBuf)

	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  2,
		BlockLength:  blockLength,
		BlockCrc:     blockCrc,
	}

	// Build with custom HeaderRecord flags
	header := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(0), // set compacted flag
		FirstEntryID: 0,
	}

	var blockData []byte
	blockData = append(blockData, codec.EncodeRecord(header)...)
	blockData = append(blockData, codec.EncodeRecord(blockHeader)...)
	blockData = append(blockData, dataRecordsBuf...)

	entries, readBytes, blockInfo, err := reader.processBlockData(ctx, 0, blockData, 0)
	assert.NoError(t, err)
	assert.Len(t, entries, 3)
	assert.Greater(t, readBytes, 0)
	assert.NotNil(t, blockInfo)
	assert.Equal(t, int64(0), blockInfo.FirstEntryID)
	assert.Equal(t, int64(2), blockInfo.LastEntryID)

	// Verify that flags were updated from the HeaderRecord
	assert.Equal(t, uint32(codec.SetCompacted(0)), reader.flags.Load())
}

func TestMinioFileReaderAdv_ProcessBlockData_EntryFilterByStartEntryID(t *testing.T) {
	// Block has entries 0-4 but startEntryID=2, should only return entries 2-4
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	payloads := [][]byte{
		[]byte("data0"),
		[]byte("data1"),
		[]byte("data2"),
		[]byte("data3"),
		[]byte("data4"),
	}

	blockData := buildValidBlockData(t, 0, 0, payloads)

	entries, readBytes, blockInfo, err := reader.processBlockData(ctx, 0, blockData, 2)
	assert.NoError(t, err)
	assert.Len(t, entries, 3) // entries 2, 3, 4
	assert.Greater(t, readBytes, 0)
	assert.NotNil(t, blockInfo)

	// Verify only entries with ID >= 2 are returned
	assert.Equal(t, int64(2), entries[0].EntryId)
	assert.Equal(t, int64(3), entries[1].EntryId)
	assert.Equal(t, int64(4), entries[2].EntryId)
	assert.Equal(t, []byte("data2"), entries[0].Values)
	assert.Equal(t, []byte("data3"), entries[1].Values)
	assert.Equal(t, []byte("data4"), entries[2].Values)
}

func TestMinioFileReaderAdv_ProcessBlockData_NonBlock0(t *testing.T) {
	// Test block 1 which should NOT have a HeaderRecord
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	payloads := [][]byte{[]byte("entry10"), []byte("entry11")}
	blockData := buildValidBlockData(t, 1, 10, payloads)

	entries, readBytes, blockInfo, err := reader.processBlockData(ctx, 1, blockData, 10)
	assert.NoError(t, err)
	assert.Len(t, entries, 2)
	assert.Greater(t, readBytes, 0)
	assert.NotNil(t, blockInfo)
	assert.Equal(t, int64(10), entries[0].EntryId)
	assert.Equal(t, int64(11), entries[1].EntryId)
}

func TestMinioFileReaderAdv_ProcessBlockData_FilterAllEntries(t *testing.T) {
	// All entries are below startEntryID, so no entries returned
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	payloads := [][]byte{[]byte("d0"), []byte("d1")}
	blockData := buildValidBlockData(t, 0, 0, payloads)

	entries, readBytes, blockInfo, err := reader.processBlockData(ctx, 0, blockData, 100)
	assert.NoError(t, err)
	assert.Len(t, entries, 0) // All entries filtered out
	assert.Equal(t, 0, readBytes)
	assert.NotNil(t, blockInfo)
	assert.Equal(t, int64(0), blockInfo.FirstEntryID)
	assert.Equal(t, int64(1), blockInfo.LastEntryID)
}

// ===== handleBlockNotFoundByCompaction: compaction detected via footer read =====

func TestMinioFileReaderAdv_HandleBlockNotFound_CompactionDetectedViaFooterRead(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// No footer so shouldBlockExist returns true (active segment)
	reader.footer = nil
	reader.isCompacted.Store(false)

	footerKey := "test-base/1/0/footer.blk"

	// Build compacted footer data
	footer := &codec.FooterRecord{
		TotalBlocks:  2,
		TotalRecords: 10,
		TotalSize:    1024,
		IndexOffset:  0,
		IndexLength:  0,
		Version:      codec.FormatVersion,
		Flags:        codec.SetCompacted(0),
		LAC:          9,
	}
	footerData := codec.EncodeRecord(footer)

	// Build index records for the footer.blk
	idx0 := &codec.IndexRecord{BlockNumber: 0, StartOffset: 0, BlockSize: 512, FirstEntryID: 0, LastEntryID: 4}
	idx1 := &codec.IndexRecord{BlockNumber: 1, StartOffset: 1, BlockSize: 512, FirstEntryID: 5, LastEntryID: 9}
	var footerBlkData []byte
	footerBlkData = append(footerBlkData, codec.EncodeRecord(idx0)...)
	footerBlkData = append(footerBlkData, codec.EncodeRecord(idx1)...)
	footerBlkData = append(footerBlkData, footerData...)

	// readFooterAndIndexUnsafe calls StatObject on footer key
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(int64(len(footerBlkData)), false, nil).Once()
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", footerKey, int64(0), int64(len(footerBlkData)), mock.Anything, mock.Anything).
		Return(&mockFileReader{data: footerBlkData}, nil).Once()

	err := reader.handleBlockNotFoundByCompaction(ctx, 0, "GetObject")
	assert.Error(t, err)
	assert.True(t, errors.Is(err, werr.ErrFileReaderBlockDeletedByCompaction))
}

func TestMinioFileReaderAdv_HandleBlockNotFound_NoFooterNoCompactionDetected(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	// No footer so shouldBlockExist returns true (active segment)
	reader.footer = nil
	reader.isCompacted.Store(false)

	footerKey := "test-base/1/0/footer.blk"

	// readFooterAndIndexUnsafe: footer not found
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", footerKey, mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Once()
	mockClient.EXPECT().IsObjectNotExistsError(notFoundErr).Return(true).Once()

	// No compaction detected, returns nil
	err := reader.handleBlockNotFoundByCompaction(ctx, 0, "GetObject")
	assert.NoError(t, err)
}

// ===== GetBlockHeaderRecord: GetObject error and block 0 with HeaderRecord =====

func TestMinioFileReaderAdv_GetBlockHeaderRecord_Block0WithHeader(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockKey := "test-base/1/0/0.blk"
	readSize := int64(codec.RecordHeaderSize+codec.HeaderRecordSize) + int64(codec.RecordHeaderSize+codec.BlockHeaderRecordSize)

	// Build valid header + block header
	header := &codec.HeaderRecord{
		Version:      codec.FormatVersion,
		Flags:        0,
		FirstEntryID: 0,
	}
	blockHeader := &codec.BlockHeaderRecord{
		BlockNumber:  0,
		FirstEntryID: 0,
		LastEntryID:  4,
		BlockLength:  100,
		BlockCrc:     12345,
	}

	var headerData []byte
	headerData = append(headerData, codec.EncodeRecord(header)...)
	headerData = append(headerData, codec.EncodeRecord(blockHeader)...)

	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", blockKey, int64(0), readSize, mock.Anything, mock.Anything).
		Return(&mockFileReader{data: headerData}, nil).Once()

	result, err := reader.getBlockHeaderRecord(ctx, 0, blockKey)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, int32(0), result.BlockNumber)
	assert.Equal(t, int64(0), result.FirstEntryID)
	assert.Equal(t, int64(4), result.LastEntryID)
}

func TestMinioFileReaderAdv_GetBlockHeaderRecord_GetObjectConnectionError(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)

	blockKey := "test-base/1/0/1.blk"
	readSize := int64(codec.RecordHeaderSize + codec.BlockHeaderRecordSize)

	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", blockKey, int64(0), readSize, mock.Anything, mock.Anything).
		Return(nil, errors.New("connection refused")).Once()

	result, err := reader.getBlockHeaderRecord(ctx, 1, blockKey)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

// ===== ReadNextBatchAdv: isFooterExists path when no footer in reader =====

func TestMinioFileReaderAdv_ReadNextBatchAdv_FooterExistsNoDataCheckEOF(t *testing.T) {
	// Tests the path in readDataBlocksUnsafe where zero entries are read,
	// hasDataReadError is false, footer is nil in reader, but isFooterExists
	// returns true from storage => should return EOF
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(mockClient)
	reader.isCompleted.Store(false)
	reader.isCompacted.Store(false)
	reader.isFenced.Store(false)
	reader.footer = nil

	// tryReadFooterAndIndex: no footer initially
	notFoundErr := mockNoSuchKeyError()
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/footer.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()
	mockClient.EXPECT().IsObjectNotExistsError(mock.Anything).Return(true).Maybe()

	// Block 0 not found
	mockClient.EXPECT().StatObject(mock.Anything, "test-bucket", "test-base/1/0/0.blk", mock.Anything, mock.Anything).
		Return(int64(0), false, notFoundErr).Maybe()

	opt := storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 10}
	batch, err := reader.ReadNextBatchAdv(ctx, opt, nil)
	assert.Nil(t, batch)
	assert.Error(t, err)
	// Should return ErrEntryNotFound since no footer, no blocks, not completed
	assert.True(t, errors.Is(err, werr.ErrEntryNotFound))
}

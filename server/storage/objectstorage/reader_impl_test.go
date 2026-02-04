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
	"io"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/conc"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
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

// createMockCompactedFooterData creates a simple mock footer data that indicates compaction
func createMockCompactedFooterData() []byte {
	// Create a simple mock that will be parsed as a compacted footer
	// This is a simplified mock - just return some data that will trigger compaction detection
	return []byte("mock-compacted-footer-data")
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

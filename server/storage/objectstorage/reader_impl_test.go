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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/conc"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_minio"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

func mockNoSuchKeyError() error {
	return minio.ErrorResponse{
		Code:       "NoSuchKey",
		Message:    "The specified key does not exist.",
		BucketName: "test-bucket",
		Key:        "test-key",
		RequestID:  "mock-request-id",
		HostID:     "mock-host-id",
	}
}

func TestMinioFileReaderAdv_readDataBlocks_NoError_EOF_CompletedFile(t *testing.T) {
	// Test: No read errors, file is completed → should return EOF
	ctx := context.Background()

	mockClient := mocks_minio.NewMinioHandler(t)

	reader := &MinioFileReaderAdv{
		client:         mockClient,
		bucket:         "test-bucket",
		segmentFileKey: "test-segment",
		logId:          1,
		segmentId:      1,
		logIdStr:       "1",
		batchMaxSize:   4 * 1024 * 1024, // 4MB
		pool:           conc.NewPool[*BlockReadResult](4, conc.WithPreAlloc(true)),
		footer:         &codec.FooterRecord{}, // Has footer = completed
	}
	reader.isCompleted.Store(true)

	// Mock StatObject to return "not found" (no blocks to read)
	mockClient.On("StatObject", mock.Anything, "test-bucket", mock.Anything, mock.Anything).
		Return(minio.ObjectInfo{}, mockNoSuchKeyError()).
		Maybe()

	opt := storage.ReaderOpt{
		StartEntryID: 100,
		BatchSize:    10,
	}

	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0)

	// Should return EOF because file is completed and no read errors occurred
	assert.Nil(t, batch)
	assert.True(t, errors.Is(err, werr.ErrFileReaderEndOfFile))
}

func TestMinioFileReaderAdv_readDataBlocks_NoError_EntryNotFound_IncompleteFile(t *testing.T) {
	// Test: No read errors, file is incomplete → should return EntryNotFound
	ctx := context.Background()

	mockClient := mocks_minio.NewMinioHandler(t)

	reader := &MinioFileReaderAdv{
		client:         mockClient,
		bucket:         "test-bucket",
		segmentFileKey: "test-segment",
		logId:          1,
		segmentId:      1,
		logIdStr:       "1",
		batchMaxSize:   4 * 1024 * 1024, // 4MB
		pool:           conc.NewPool[*BlockReadResult](4, conc.WithPreAlloc(true)),
		footer:         nil, // No footer = incomplete
	}
	reader.isCompleted.Store(false)
	reader.isCompacted.Store(false)
	reader.isFenced.Store(false)

	// Mock StatObject to return "not found" (no blocks to read and no footer)
	mockClient.On("StatObject", mock.Anything, "test-bucket", mock.Anything, mock.Anything).
		Return(minio.ObjectInfo{}, mockNoSuchKeyError()).
		Maybe()

	opt := storage.ReaderOpt{
		StartEntryID: 100,
		BatchSize:    10,
	}

	batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0)

	// Should return EntryNotFound because file is incomplete and no read errors occurred
	assert.Nil(t, batch)
	assert.True(t, errors.Is(err, werr.ErrEntryNotFound))
}

func TestMinioFileReaderAdv_readDataBlocks_WithStatError_ShouldReturnEntryNotFound(t *testing.T) {
	// Test: StatObject error occurred → should return EntryNotFound (not EOF)
	ctx := context.Background()

	mockClient := mocks_minio.NewMinioHandler(t)

	reader := &MinioFileReaderAdv{
		client:         mockClient,
		bucket:         "test-bucket",
		segmentFileKey: "test-segment",
		logId:          1,
		segmentId:      1,
		logIdStr:       "1",
		batchMaxSize:   4 * 1024 * 1024, // 4MB
		pool:           conc.NewPool[*BlockReadResult](4, conc.WithPreAlloc(true)),
		footer:         &codec.FooterRecord{}, // Has footer = completed
	}
	reader.isCompleted.Store(true) // File is completed

	// Mock StatObject to return a network error (not "not found")
	statError := errors.New("network timeout")
	mockClient.On("StatObject", mock.Anything, "test-bucket", mock.Anything, mock.Anything).
		Return(minio.ObjectInfo{}, statError).
		Once()

	opt := storage.ReaderOpt{
		StartEntryID: 100,
		BatchSize:    10,
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

	mockClient := mocks_minio.NewMinioHandler(t)

	reader := &MinioFileReaderAdv{
		client:         mockClient,
		bucket:         "test-bucket",
		segmentFileKey: "test-segment",
		logId:          1,
		segmentId:      1,
		logIdStr:       "1",
		batchMaxSize:   4 * 1024 * 1024, // 4MB
		pool:           conc.NewPool[*BlockReadResult](4, conc.WithPreAlloc(true)),
	}

	// Test StatObject error
	statError := errors.New("stat failed")
	mockClient.On("StatObject", mock.Anything, "test-bucket", mock.Anything, mock.Anything).
		Return(minio.ObjectInfo{}, statError).
		Once()

	futures, nextBlockID, totalRawSize, hasMoreBlocks, hasReadBlockBatchErr := reader.readBlockBatchUnsafe(ctx, 0, 4*1024*1024, 0)

	assert.Empty(t, futures)
	assert.Equal(t, int64(0), nextBlockID)
	assert.Equal(t, int64(0), totalRawSize)
	assert.True(t, hasReadBlockBatchErr) // Error should be reported
	assert.True(t, hasMoreBlocks)        // hasMoreBlocks should remain true when error occurs
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
		name           string
		isCompleted    bool
		isCompacted    bool
		isFenced       bool
		hasFooter      bool
		hasReadError   bool
		expectEOF      bool
		expectNotFound bool
	}{
		{
			name:           "NoError_Completed_ShouldReturnEOF",
			isCompleted:    true,
			hasReadError:   false,
			expectEOF:      true,
			expectNotFound: false,
		},
		{
			name:           "NoError_Compacted_ShouldReturnEOF",
			isCompacted:    true,
			hasReadError:   false,
			expectEOF:      true,
			expectNotFound: false,
		},
		{
			name:           "NoError_Fenced_ShouldReturnEOF",
			isFenced:       true,
			hasReadError:   false,
			expectEOF:      true,
			expectNotFound: false,
		},
		{
			name:           "NoError_HasFooter_ShouldReturnEOF",
			hasFooter:      true,
			hasReadError:   false,
			expectEOF:      true,
			expectNotFound: false,
		},
		{
			name:           "WithError_Completed_ShouldReturnEntryNotFound",
			isCompleted:    true,
			hasReadError:   true,
			expectEOF:      false,
			expectNotFound: true,
		},
		{
			name:           "WithError_Compacted_ShouldReturnEntryNotFound",
			isCompacted:    true,
			hasReadError:   true,
			expectEOF:      false,
			expectNotFound: true,
		},
		{
			name:           "WithError_HasFooter_ShouldReturnEntryNotFound",
			hasFooter:      true,
			hasReadError:   true,
			expectEOF:      false,
			expectNotFound: true,
		},
		{
			name:           "NoError_Incomplete_ShouldReturnEntryNotFound",
			isCompleted:    false,
			isCompacted:    false,
			isFenced:       false,
			hasFooter:      false,
			hasReadError:   false,
			expectEOF:      false,
			expectNotFound: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			mockClient := mocks_minio.NewMinioHandler(t)

			reader := &MinioFileReaderAdv{
				client:         mockClient,
				bucket:         "test-bucket",
				segmentFileKey: "test-segment",
				logId:          1,
				segmentId:      1,
				logIdStr:       "1",
				batchMaxSize:   4 * 1024 * 1024, // 4MB
				pool:           conc.NewPool[*BlockReadResult](4, conc.WithPreAlloc(true)),
			}

			// Setup reader state
			reader.isCompleted.Store(tc.isCompleted)
			reader.isCompacted.Store(tc.isCompacted)
			reader.isFenced.Store(tc.isFenced)
			if tc.hasFooter {
				reader.footer = &codec.FooterRecord{}
			}

			// Setup mock expectations
			if tc.hasReadError {
				// Mock StatObject to return an error
				mockClient.On("StatObject", mock.Anything, "test-bucket", mock.Anything, mock.Anything).
					Return(minio.ObjectInfo{}, errors.New("mock error")).
					Once()
			} else {
				// Mock StatObject to return "not found" (no blocks to read)
				mockClient.On("StatObject", mock.Anything, "test-bucket", mock.Anything, mock.Anything).
					Return(minio.ObjectInfo{}, mockNoSuchKeyError()).
					Maybe()
			}

			opt := storage.ReaderOpt{
				StartEntryID: 100,
				BatchSize:    10,
			}

			batch, err := reader.readDataBlocksUnsafe(ctx, opt, 0)

			// Verify results
			assert.Nil(t, batch)
			require.Error(t, err)

			if tc.expectEOF {
				assert.True(t, errors.Is(err, werr.ErrFileReaderEndOfFile))
			} else if tc.expectNotFound {
				assert.True(t, errors.Is(err, werr.ErrEntryNotFound))
			}
		})
	}
}

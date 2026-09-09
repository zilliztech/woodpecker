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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// A segment finalized with zero blocks uploads a footer.blk that carries only
// the footer record: TotalBlocks=0, IndexLength=0. Recovery has to accept it,
// or the segment can never be fenced -- the failure that left a local-storage
// WAL unopenable in #306. This backend was never affected, and had no test
// saying so.
func TestMinioFileWriter_RecoverFromFooter_EmptyFinalizedSegment(t *testing.T) {
	ctx := context.Background()
	mockClient := mocks_objectstorage.NewObjectStorage(t)
	w := newTestMinioFileWriter()
	w.client = mockClient

	footerData, footer := serializeFooterAndIndexes(ctx, nil)
	require.EqualValues(t, 0, footer.TotalBlocks)
	require.EqualValues(t, 0, footer.IndexLength)
	require.Len(t, footerData, codec.RecordHeaderSize+codec.GetFooterRecordSize(footer.Version),
		"an empty finalized segment uploads the footer record and nothing else")

	footerBlockKey := "test-base/1/0/footer.blk"
	mockReader := &writerMockFileReader{data: footerData}
	mockClient.EXPECT().GetObject(mock.Anything, "test-bucket", footerBlockKey, int64(0), int64(len(footerData)), mock.Anything, mock.Anything).
		Return(mockReader, nil).Once()

	err := w.recoverFromFooter(ctx, footerBlockKey, int64(len(footerData)))
	require.NoError(t, err, "recovery of a finalized empty segment")
	assert.True(t, w.finalized.Load(), "a footer means the segment is finalized")
	assert.False(t, w.storageWritable.Load())
	assert.Empty(t, w.blockIndexes)
	assert.EqualValues(t, -1, w.lastEntryID.Load())
}

// This backend keeps its index at offset 0 of a separate footer.blk, so
// IndexOffset=0 is its normal value -- the opposite of the local-file backends,
// where it marks a corrupt footer. codec.ValidateIndexSection encodes the
// local-file rule, so object-storage footers must never be routed through it.
// If someone ever "unifies" recoverFromFooter onto the shared codec helper,
// this fails instead of every object-storage segment being declared corrupt.
func TestObjectStorageFooterIsNotValidatedAsInlineIndex(t *testing.T) {
	ctx := context.Background()

	_, empty := serializeFooterAndIndexes(ctx, nil)
	_, populated := serializeFooterAndIndexes(ctx, []*codec.IndexRecord{
		{BlockNumber: 0, StartOffset: 0, BlockSize: 100, FirstEntryID: 0, LastEntryID: 9},
	})

	for name, footer := range map[string]*codec.FooterRecord{"empty": empty, "populated": populated} {
		t.Run(name, func(t *testing.T) {
			require.EqualValues(t, 0, footer.IndexOffset,
				"index records start at offset 0 of footer.blk")
			assert.Error(t, codec.ValidateIndexSection(footer),
				"the inline-index validator rejects this backend's footers by design")
		})
	}
}

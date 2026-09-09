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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// A segment finalized with zero blocks is header + footer only, with
// IndexOffset = header size and IndexLength = 0. Reopening it in recovery
// mode (what Fence does when another writer takes over the log) must succeed
// and see an empty, finalized segment; rejecting it makes the segment
// unfenceable and the log unopenable.
func TestLocalFileWriter_RecoverFinalizedEmptySegment(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	w, err := NewLocalFileWriter(ctx, dir, 1, 7, cfg)
	require.NoError(t, err)
	last, err := w.Finalize(ctx, -1)
	require.NoError(t, err)
	assert.EqualValues(t, -1, last)
	require.NoError(t, w.Close(ctx))

	st, err := os.Stat(getSegmentFilePath(dir, 1, 7))
	require.NoError(t, err)
	assert.EqualValues(t, codec.RecordHeaderSize+codec.HeaderRecordSize+codec.RecordHeaderSize+codec.FooterRecordSize, st.Size(),
		"header + footer only")

	rw, err := NewLocalFileWriterWithMode(ctx, dir, 1, 7, cfg, true)
	require.NoError(t, err, "recovery of a finalized empty segment")
	assert.True(t, rw.finalized.Load(), "recovered as finalized")
	assert.Empty(t, rw.blockIndexes)
	assert.EqualValues(t, -1, rw.GetFirstEntryId(ctx))
	assert.EqualValues(t, -1, rw.GetLastEntryId(ctx))

	fenced, err := rw.Fence(ctx)
	require.NoError(t, err, "fence after reopen")
	assert.EqualValues(t, -1, fenced)

	_, err = rw.Finalize(ctx, -1)
	assert.NoError(t, err, "finalize is idempotent on an already finalized segment")
	require.NoError(t, rw.Close(ctx))
}

// A finalized segment with data still recovers its index through the shared
// codec path.
func TestLocalFileWriter_RecoverFinalizedSegmentWithData(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)

	w, err := NewLocalFileWriter(ctx, dir, 1, 8, cfg)
	require.NoError(t, err)
	for i := int64(0); i < 3; i++ {
		_, err := w.WriteDataAsync(ctx, i, []byte("payload"), nil)
		require.NoError(t, err)
	}
	// Finalize syncs the buffer and waits for every flush before writing the footer.
	last, err := w.Finalize(ctx, 2)
	require.NoError(t, err)
	assert.EqualValues(t, 2, last)
	require.NoError(t, w.Close(ctx))

	rw, err := NewLocalFileWriterWithMode(ctx, dir, 1, 8, cfg, true)
	require.NoError(t, err)
	assert.True(t, rw.finalized.Load())
	assert.NotEmpty(t, rw.blockIndexes)
	assert.EqualValues(t, 0, rw.GetFirstEntryId(ctx))
	assert.EqualValues(t, 2, rw.GetLastEntryId(ctx))
	require.NoError(t, rw.Close(ctx))
}

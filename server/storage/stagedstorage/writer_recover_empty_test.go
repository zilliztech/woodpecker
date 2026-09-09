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
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// The staged mirror of disk's TestLocalFileWriter_RecoverFinalizedEmptySegment.
//
// TestRecoverBlocksFromFooter_EmptyIndexLength already covers reopening a
// finalized empty segment, but it stops at inspecting the recovered state. The
// sequence that made a WAL unopenable in #306 goes one step further: Fence
// reopens the writer in recovery mode and then fences it. That combination was
// never exercised on this backend, which is how the same defect survived in
// disk until #307.
func TestStagedFileWriter_RecoverFinalizedEmptySegment_ThenFence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := newTestConfig(t)

	w, err := NewStagedFileWriter(ctx, "test-bucket", "test-root", dir, 1, 77, nil, cfg)
	require.NoError(t, err)
	last, err := w.Finalize(ctx, -1)
	require.NoError(t, err)
	assert.EqualValues(t, -1, last)
	require.NoError(t, w.Close(ctx))

	rw, err := NewStagedFileWriterWithMode(ctx, "test-bucket", "test-root", dir, 1, 77, nil, cfg, true)
	require.NoError(t, err, "recovery of a finalized empty segment")
	defer rw.Close(ctx)
	assert.True(t, rw.finalized.Load(), "recovered as finalized")
	assert.Empty(t, rw.blockIndexes)

	fenced, err := rw.Fence(ctx)
	require.NoError(t, err, "fence after reopen")
	assert.EqualValues(t, -1, fenced)

	_, err = rw.Finalize(ctx, -1)
	assert.NoError(t, err, "finalize is idempotent on an already finalized segment")
}

// writeFinalizedFileWithFooter lays down header + footer at the path the staged
// writer reads, so recovery sees a well-formed file carrying exactly this footer.
func writeFinalizedFileWithFooter(t *testing.T, dir string, logId, segId int64, footer *codec.FooterRecord) {
	t.Helper()
	require.NoError(t, os.MkdirAll(getSegmentDir(dir, logId, segId), 0o755))

	header := codec.EncodeRecord(&codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0, FirstEntryID: 0})
	data := append(append([]byte{}, header...), codec.EncodeRecord(footer)...)
	require.NoError(t, os.WriteFile(getSegmentFilePath(dir, logId, segId), data, 0o644))
}

// #307 routed this backend through codec.ValidateIndexSection, which rejects
// IndexOffset == 0. The fast path it replaced accepted any footer with
// IndexLength == 0 regardless of IndexOffset, so this is a real tightening of
// staged's contract and needs a test of its own -- the codec package tests the
// rule, not that this backend applies it.
func TestStagedFileWriter_RecoverFromFooter_RejectsZeroIndexOffset(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := newTestConfig(t)

	writeFinalizedFileWithFooter(t, dir, 2, 10, &codec.FooterRecord{
		TotalBlocks: 0,
		TotalSize:   0,
		IndexOffset: 0,
		IndexLength: 0,
		Version:     codec.FormatVersion,
		LAC:         -1,
	})

	_, err := NewStagedFileWriterWithMode(ctx, "test-bucket", "test-root", dir, 2, 10, nil, cfg, true)
	require.Error(t, err, "an inline-index footer can never start at offset 0")
	assert.ErrorContains(t, err, "IndexOffset=0")
}

// The other half of the tightening: a zero-length index cannot describe blocks.
func TestStagedFileWriter_RecoverFromFooter_RejectsIndexLengthZeroWithBlocks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := newTestConfig(t)

	writeFinalizedFileWithFooter(t, dir, 2, 11, &codec.FooterRecord{
		TotalBlocks: 3,
		TotalSize:   uint64(codec.RecordHeaderSize + codec.HeaderRecordSize),
		IndexOffset: uint64(codec.RecordHeaderSize + codec.HeaderRecordSize),
		IndexLength: 0,
		Version:     codec.FormatVersion,
		LAC:         -1,
	})

	_, err := NewStagedFileWriterWithMode(ctx, "test-bucket", "test-root", dir, 2, 11, nil, cfg, true)
	require.Error(t, err, "IndexLength=0 cannot describe 3 blocks")
	assert.ErrorContains(t, err, "TotalBlocks=3")
}

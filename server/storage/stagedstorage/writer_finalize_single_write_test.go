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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// Finalize writes the index records and the footer with one write. The file
// it leaves must be byte for byte what writing them one record at a time
// left: the index records in block order right after the last block, then a
// footer describing them.
func TestStagedFileWriter_FinalizeWritesIndexAndFooterAsBefore(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := newTestConfig(t)

	const blocks = 64
	w, err := NewStagedFileWriter(ctx, "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	for i := int64(0); i < blocks; i++ {
		_, err := w.WriteDataAsync(ctx, i, []byte(fmt.Sprintf("entry-%d", i)), nil)
		require.NoError(t, err)
		require.NoError(t, w.Sync(ctx)) // one entry per block, as at a trickle
	}
	last, err := w.Finalize(ctx, blocks-1)
	require.NoError(t, err)
	assert.EqualValues(t, blocks-1, last)
	indexes := append([]*codec.IndexRecord(nil), w.blockIndexes...)
	filePath := w.segmentFilePath
	require.NoError(t, w.Close(ctx))
	require.Len(t, indexes, blocks)

	data, err := os.ReadFile(filePath)
	require.NoError(t, err)
	footer, err := codec.ParseFooterFromBytes(data)
	require.NoError(t, err)

	lastBlock := indexes[len(indexes)-1]
	dataEnd := uint64(lastBlock.StartOffset) + uint64(lastBlock.BlockSize)
	var wantIndex bytes.Buffer
	for _, idx := range indexes {
		wantIndex.Write(codec.EncodeRecord(idx))
	}
	wantFooter := &codec.FooterRecord{
		TotalBlocks:  blocks,
		TotalRecords: blocks,
		TotalSize:    dataEnd + uint64(wantIndex.Len()),
		IndexOffset:  dataEnd,
		IndexLength:  uint32(wantIndex.Len()),
		Version:      codec.FormatVersion,
		LAC:          blocks - 1,
	}
	assert.Equal(t, wantFooter, footer)
	require.EqualValues(t, int(dataEnd)+wantIndex.Len()+len(codec.EncodeRecord(wantFooter)), len(data))
	assert.Equal(t, wantIndex.Bytes(), data[dataEnd:dataEnd+uint64(wantIndex.Len())], "index section")
	assert.Equal(t, codec.EncodeRecord(wantFooter), data[footer.TotalSize:], "footer")

	// The finalized segment reads back in full, and recovers as finalized.
	reader, err := NewStagedFileReaderAdv(ctx, "test-bucket", "test-root", dir, 1, 0, nil, cfg)
	require.NoError(t, err)
	var got []string
	for next := int64(0); next < blocks; {
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: next, MaxBatchEntries: blocks}, nil)
		require.NoError(t, err)
		require.NotEmpty(t, batch.Entries)
		for _, e := range batch.Entries {
			got = append(got, string(e.Values))
		}
		next += int64(len(batch.Entries))
	}
	require.NoError(t, reader.Close(ctx))
	require.Len(t, got, blocks)
	for i, v := range got {
		assert.Equal(t, fmt.Sprintf("entry-%d", i), v)
	}

	rw, err := NewStagedFileWriterWithMode(ctx, "test-bucket", "test-root", dir, 1, 0, nil, cfg, true)
	require.NoError(t, err)
	assert.True(t, rw.finalized.Load())
	assert.Len(t, rw.blockIndexes, blocks)
	assert.EqualValues(t, blocks-1, rw.GetLastEntryId(ctx))
	require.NoError(t, rw.Close(ctx))
}

// BenchmarkStagedFileWriter_FinalizeManyBlocks measures writing the index and
// footer of a segment with many small blocks, what completing a segment
// written at a trickle costs. One real block is written; the rest of the index
// records are synthetic, since only their encoding and writing is measured.
func BenchmarkStagedFileWriter_FinalizeManyBlocks(b *testing.B) {
	for _, blocks := range []int{1000, 5000} {
		b.Run(fmt.Sprintf("blocks=%d", blocks), func(b *testing.B) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration()
			require.NoError(b, err)
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				w, err := NewStagedFileWriter(ctx, "test-bucket", "test-root", b.TempDir(), 1, 0, nil, cfg)
				require.NoError(b, err)
				_, err = w.WriteDataAsync(ctx, 0, []byte("entry"), nil)
				require.NoError(b, err)
				require.NoError(b, w.Sync(ctx))
				require.NoError(b, w.awaitAllFlushTasks(ctx))
				first := w.blockIndexes[0]
				for n := 1; n < blocks; n++ {
					w.blockIndexes = append(w.blockIndexes, &codec.IndexRecord{
						BlockNumber:  int32(n),
						StartOffset:  first.StartOffset + int64(n)*int64(first.BlockSize),
						BlockSize:    first.BlockSize,
						FirstEntryID: int64(n),
						LastEntryID:  int64(n),
					})
				}
				b.StartTimer()

				_, err = w.Finalize(ctx, int64(blocks-1))

				b.StopTimer()
				require.NoError(b, err)
				require.NoError(b, w.Close(ctx))
				b.StartTimer()
			}
		})
	}
}

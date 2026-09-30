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

// Reader and writer on the same segment, with the writer doing what it can do
// while a tail reader reads: append, sync, advance the LAC (which reaches the
// reader late, if at all), fence, finalize, close. Whatever happens, the reader
// may answer "not found" (retry, or ask another replica), but it must answer
// end-of-file only past the segment's final boundary, the footer's LAC, and
// every entry up to that boundary must be readable (#405).

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
)

func lifecycleReader(t *testing.T, dir string, logId, segId int64) *StagedFileReaderAdv {
	t.Helper()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	reader, err := NewStagedFileReaderAdv(context.Background(), "test-bucket", "test-root", dir, logId, segId, nil, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(context.Background()) })
	return reader
}

// The reader holds a resume state from an earlier batch and its LAC lags when
// the writer finalizes further on. The next read from past the stale LAC must
// return the entries up to the footer's LAC, not end-of-file.
func TestStagedReaderLifecycle_StatefulReadAfterFinalizeBeyondStaleLAC(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logId, segId := int64(301), int64(301)
	writer := newTailingWriter(t, dir, logId, segId, 10) // entries 0..9 on disk, not finalized
	reader := lifecycleReader(t, dir, logId, segId)

	require.NoError(t, reader.UpdateLastAddConfirmed(ctx, 4)) // the last LAC this replica heard
	first, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 0, MaxBatchEntries: 100}, nil)
	require.NoError(t, err)
	require.Len(t, first.Entries, 5)
	require.NotNil(t, first.LastReadState)

	_, err = writer.Finalize(ctx, 9)
	require.NoError(t, err)
	require.NoError(t, writer.Close(ctx))

	next, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 5, MaxBatchEntries: 100}, first.LastReadState)
	require.NoError(t, err, "entries 5..9 are below the footer's LAC; end-of-file here skips them")
	require.Len(t, next.Entries, 5)
	assert.Equal(t, int64(5), next.Entries[0].EntryId)
	assert.Equal(t, int64(9), next.Entries[4].EntryId)

	_, err = reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 10, MaxBatchEntries: 100}, next.LastReadState)
	require.Error(t, err)
	assert.True(t, werr.ErrFileReaderEndOfFile.Is(err), "past the footer's LAC is the end: %v", err)
}

// drain reads from `from` until end-of-file, the way a tail consumer does: it
// keeps the resume state of the previous batch (stateful) or never passes one,
// and retries "not found" a bounded number of times. It fails the test if
// end-of-file comes at or below `final`, and returns the entry ids read.
func drain(t *testing.T, reader *StagedFileReaderAdv, from int64, state *proto.LastReadState, final int64, stateful bool) []int64 {
	t.Helper()
	ctx := context.Background()
	var ids []int64
	next := from
	for attempts := 0; attempts < 200; attempts++ {
		var passed *proto.LastReadState
		if stateful {
			passed = state
		}
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: next, MaxBatchEntries: 3}, passed)
		switch {
		case err == nil:
			for _, e := range batch.Entries {
				ids = append(ids, e.EntryId)
				next = e.EntryId + 1
			}
			state = batch.LastReadState
		case werr.ErrFileReaderEndOfFile.Is(err):
			require.Greater(t, next, final, "end-of-file at entry %d, below the segment's final boundary %d", next, final)
			return ids
		case werr.ErrEntryNotFound.Is(err):
			time.Sleep(5 * time.Millisecond)
		default:
			require.NoError(t, err)
		}
	}
	t.Fatalf("never reached end-of-file; read up to %d of %d", next-1, final)
	return nil
}

func span(from, to int64) []int64 {
	var out []int64
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

// What the writer may have done between the reader's reads, and where the
// segment ends. In every case the reader must deliver exactly the entries up to
// the footer's LAC and then report end-of-file.
func TestStagedReaderLifecycle_WriterEventsBetweenReads(t *testing.T) {
	cases := []struct {
		name      string
		written   int64 // entries 0..written-1 on disk before the finalize
		readerLAC int64 // the LAC the reader has heard when it starts
		finalLAC  int64 // the finalize target: the footer's LAC
		fence     bool  // fence before finalizing
		growBy    int64 // entries appended after the first read, before finalizing
	}{
		{name: "LAC current at finalize", written: 10, readerLAC: 9, finalLAC: 9},
		{name: "LAC lags the finalize", written: 10, readerLAC: 4, finalLAC: 9},
		{name: "LAC never heard", written: 10, readerLAC: -1, finalLAC: 9},
		{name: "finalized below the written tail", written: 10, readerLAC: 4, finalLAC: 7},
		{name: "fenced then finalized", written: 10, readerLAC: 4, finalLAC: 9, fence: true},
		{name: "grows after the first read then finalized", written: 6, readerLAC: 3, finalLAC: 11, growBy: 6},
		{name: "empty segment", written: 0, readerLAC: -1, finalLAC: -1},
	}
	for _, tc := range cases {
		for _, stateful := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/stateful=%v", tc.name, stateful), func(t *testing.T) {
				ctx := context.Background()
				dir := t.TempDir()
				logId, segId := int64(400), int64(400)
				writer := newTailingWriter(t, dir, logId, segId, tc.written)
				reader := lifecycleReader(t, dir, logId, segId)
				if tc.readerLAC >= 0 {
					require.NoError(t, reader.UpdateLastAddConfirmed(ctx, tc.readerLAC))
				}

				// While the segment is open the consumer reads what it may, up to the
				// reader's LAC, and past it gets "not found", never end-of-file.
				var got []int64
				var state *proto.LastReadState
				for next := int64(0); ; {
					var passed *proto.LastReadState
					if stateful {
						passed = state
					}
					batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: next, MaxBatchEntries: 3}, passed)
					if err != nil {
						assert.True(t, werr.ErrEntryNotFound.Is(err), "an open segment has no end yet: %v", err)
						break
					}
					for _, e := range batch.Entries {
						got = append(got, e.EntryId)
						next = e.EntryId + 1
					}
					state = batch.LastReadState
				}
				require.Equal(t, span(0, tc.readerLAC), got, "reads up to the reader's LAC while open")

				for i := tc.written; i < tc.written+tc.growBy; i++ {
					_, err := writer.WriteDataAsync(ctx, i, []byte("test data"), nil)
					require.NoError(t, err)
				}
				if tc.growBy > 0 {
					require.NoError(t, writer.Sync(ctx))
				}
				if tc.fence {
					_, err := writer.Fence(ctx)
					require.NoError(t, err)
				}
				_, err := writer.Finalize(ctx, tc.finalLAC)
				require.NoError(t, err)
				require.NoError(t, writer.Close(ctx))

				// After it, the consumer carries on from where it was, with the state it
				// has, and must get the rest up to the footer's LAC.
				got = append(got, drain(t, reader, tc.readerLAC+1, state, tc.finalLAC, stateful)...)
				assert.Equal(t, span(0, tc.finalLAC), got)
			})
		}
	}
}

// The writer appends, syncs and advances the reader's LAC late (by a random lag)
// while a consumer tails the segment, then finalizes. The consumer must read
// every entry once, in order, and reach end-of-file only after the last one.
func TestStagedReaderLifecycle_ConcurrentTailingThroughFinalize(t *testing.T) {
	for round := 0; round < 5; round++ {
		for _, stateful := range []bool{true, false} {
			t.Run(fmt.Sprintf("round%d/stateful=%v", round, stateful), func(t *testing.T) {
				ctx := context.Background()
				dir := t.TempDir()
				logId, segId := int64(500+round), int64(500)
				writer := newTailingWriter(t, dir, logId, segId, 0)
				reader := lifecycleReader(t, dir, logId, segId)
				rng := rand.New(rand.NewSource(int64(round)))
				const total = int64(60)

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					for next := int64(0); next < total; {
						n := int64(1 + rng.Intn(5))
						for i := int64(0); i < n && next < total; i++ {
							_, err := writer.WriteDataAsync(ctx, next, []byte("test data"), nil)
							assert.NoError(t, err)
							next++
						}
						assert.NoError(t, writer.Sync(ctx))
						// The LAC reaches this replica late, and not always.
						if lag := int64(rng.Intn(8)); next-1-lag >= 0 && rng.Intn(4) != 0 {
							assert.NoError(t, reader.UpdateLastAddConfirmed(ctx, next-1-lag))
						}
						time.Sleep(time.Duration(rng.Intn(5)) * time.Millisecond)
					}
					_, err := writer.Finalize(ctx, total-1)
					assert.NoError(t, err)
					assert.NoError(t, writer.Close(ctx))
				}()

				got := drain(t, reader, 0, nil, total-1, stateful)
				wg.Wait()
				assert.Equal(t, span(0, total-1), got)
			})
		}
	}
}

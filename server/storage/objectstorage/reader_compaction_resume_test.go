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
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// The #506 cursor is original block 131 / entry 160. Compaction changes 148
// original blocks into 24 merged blocks; entry 161 moves to merged block 16.
func newCompactionResumeReader(t *testing.T) (*MinioFileReaderAdv, *mocks_objectstorage.ObjectStorage, *FooterAndIndex) {
	t.Helper()
	client := mocks_objectstorage.NewObjectStorage(t)
	reader := newTestReader(client)
	t.Cleanup(reader.pool.Release)
	reader.isCompleted.Store(true)
	reader.footer = &codec.FooterRecord{TotalBlocks: 148, LAC: 184, Version: codec.FormatVersion}
	compacted := &FooterAndIndex{
		footer: &codec.FooterRecord{TotalBlocks: 24, LAC: 184, Version: codec.FormatVersion, Flags: codec.SetCompacted(0)},
	}
	ranges := [][2]int64{
		{0, 57},
		{58, 116},
		{117, 120},
		{121, 121},
		{122, 122},
		{123, 123},
		{124, 124},
		{125, 126},
		{127, 137},
		{138, 139},
		{140, 140},
		{141, 149},
		{150, 151},
		{152, 152},
		{153, 158},
		{159, 160},
		{161, 161},
		{162, 166},
		{167, 168},
		{169, 169},
		{170, 177},
		{178, 179},
		{180, 180},
		{181, 184},
	}
	for blockID, entries := range ranges {
		compacted.blocks = append(compacted.blocks, &codec.IndexRecord{
			BlockNumber: int32(blockID), StartOffset: int64(blockID), FirstEntryID: entries[0], LastEntryID: entries[1],
		})
	}
	return reader, client, compacted
}

func expectCompactedResumeTail(t *testing.T, reader *MinioFileReaderAdv, client *mocks_objectstorage.ObjectStorage, compacted *FooterAndIndex, firstBlock int) {
	t.Helper()
	for _, block := range compacted.blocks[firstBlock:] {
		payloads := make([][]byte, 0, block.LastEntryID-block.FirstEntryID+1)
		for entryID := block.FirstEntryID; entryID <= block.LastEntryID; entryID++ {
			payloads = append(payloads, []byte(fmt.Sprintf("entry-%d", entryID)))
		}
		data := buildValidBlockData(t, int64(block.BlockNumber), block.FirstEntryID, payloads)
		key := getMergedBlockKey(reader.segmentFileKey, int64(block.BlockNumber))
		client.EXPECT().StatObject(mock.Anything, reader.bucket, key, reader.logNs, reader.logIdStr).Return(int64(len(data)), false, nil).Once()
		client.EXPECT().GetObject(mock.Anything, reader.bucket, key, int64(0), int64(len(data)), reader.logNs, reader.logIdStr).Return(&mockFileReader{data: data}, nil).Once()
	}
	missing := mockNoSuchKeyError()
	client.EXPECT().StatObject(mock.Anything, reader.bucket, getMergedBlockKey(reader.segmentFileKey, 24), reader.logNs, reader.logIdStr).Return(int64(0), false, missing).Maybe()
	client.EXPECT().IsObjectNotExistsError(missing).Return(true).Maybe()
}

// Observe the blocked critical section instead of sleeping and hoping the
// scheduler has reached it. No hook is needed in the production reader.
func waitForCompactionResumeLock(t *testing.T, method, lockFrame string) {
	t.Helper()
	require.Eventually(t, func() bool {
		buf := make([]byte, 1024*1024)
		n := runtime.Stack(buf, true)
		for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(stack, "(*MinioFileReaderAdv)."+method) && strings.Contains(stack, lockFrame) {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond, "%s must block at %s", method, lockFrame)
}

func requireCompactionResumeTail(t *testing.T, batch *proto.BatchReadResult, firstEntry int64) {
	t.Helper()
	require.NotNil(t, batch)
	require.Len(t, batch.Entries, int(185-firstEntry))
	for i, entry := range batch.Entries {
		entryID := firstEntry + int64(i)
		require.Equal(t, entryID, entry.EntryId)
		require.Equal(t, fmt.Sprintf("entry-%d", entryID), string(entry.Values))
	}
	require.NotNil(t, batch.LastReadState)
	require.True(t, codec.IsCompacted(uint16(batch.LastReadState.Flags)))
	require.EqualValues(t, 23, batch.LastReadState.LastBlockId)
}

func TestMinioFileReaderAdv_ResumeAcrossCompaction(t *testing.T) {
	for _, timing := range []string{"before_call", "while_waiting_for_read_lock"} {
		t.Run(timing, func(t *testing.T) {
			reader, client, compacted := newCompactionResumeReader(t)
			expectCompactedResumeTail(t, reader, client, compacted, 16)
			// On the old implementation the stale cursor probes m_132.blk
			// and returns EOF. Model that result so the regression fails with
			// premature EOF, rather than an unexpected mock invocation.
			missing := mockNoSuchKeyError()
			client.EXPECT().StatObject(mock.Anything, reader.bucket, getMergedBlockKey(reader.segmentFileKey, 132), reader.logNs, reader.logIdStr).Return(int64(0), false, missing).Maybe()
			client.EXPECT().IsObjectNotExistsError(missing).Return(true).Maybe()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			state := &proto.LastReadState{SegmentId: reader.segmentId, Flags: 0, Version: uint32(codec.FormatVersion), LastBlockId: 131}
			opt := storage.ReaderOpt{StartEntryID: 161, MaxBatchEntries: 200}
			type readResult struct {
				batch *proto.BatchReadResult
				err   error
			}
			read := func() readResult {
				batch, err := reader.ReadNextBatchAdv(ctx, opt, state)
				return readResult{batch, err}
			}
			var result readResult
			if timing == "before_call" {
				reader.asyncUpdateReaderState(ctx, compacted)
				result = read()
			} else {
				// An existing read holds the lock while a real async state
				// update queues for the write lock. RWMutex makes later reads
				// wait for that writer, reproducing the #506 interleaving.
				reader.mu.RLock()
				held := true
				defer func() {
					if held {
						reader.mu.RUnlock()
					}
				}()
				updated := make(chan struct{})
				go func() { defer close(updated); reader.asyncUpdateReaderState(ctx, compacted) }()
				waitForCompactionResumeLock(t, "asyncUpdateReaderState", "sync.(*RWMutex).Lock")
				done := make(chan readResult, 1)
				go func() { done <- read() }()
				waitForCompactionResumeLock(t, "ReadNextBatchAdv", "sync.(*RWMutex).RLock")
				reader.mu.RUnlock()
				held = false
				select {
				case result = <-done:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case <-updated:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			require.NoError(t, result.err, "entry 161 must not become EOF while the footer ends at 184")
			requireCompactionResumeTail(t, result.batch, 161)
			// The caller's resume state remains untouched; the next state
			// describes the merged layout. EOF is correct only past the tail.
			require.EqualValues(t, 0, state.Flags)
			require.EqualValues(t, 131, state.LastBlockId)
			_, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 185, MaxBatchEntries: 200}, result.batch.LastReadState)
			require.True(t, werr.ErrFileReaderEndOfFile.Is(err))
		})
	}
}

func TestMinioFileReaderAdv_CompactedResumeKeepsPosition(t *testing.T) {
	reader, client, compacted := newCompactionResumeReader(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reader.asyncUpdateReaderState(ctx, compacted)
	expectCompactedResumeTail(t, reader, client, compacted, 17)
	state := &proto.LastReadState{SegmentId: reader.segmentId, Flags: uint32(compacted.footer.Flags), Version: uint32(codec.FormatVersion), LastBlockId: 16}
	batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 162, MaxBatchEntries: 200}, state)
	require.NoError(t, err)
	requireCompactionResumeTail(t, batch, 162)
}

func TestMinioFileReaderAdv_CompactionWaitsForRead(t *testing.T) {
	reader, client, compacted := newCompactionResumeReader(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Finish one batch from the original layout while compaction is waiting.
	reader.maxBatchSize = 1
	data := buildValidBlockData(t, 132, 161, [][]byte{[]byte("entry-161")})
	key := getBlockKey(reader.segmentFileKey, 132)
	reading := make(chan struct{})
	releaseRead := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(releaseRead) })
	t.Cleanup(unblock)
	client.EXPECT().StatObject(mock.Anything, reader.bucket, key, reader.logNs, reader.logIdStr).
		Run(func(context.Context, string, string, string, string) { close(reading); <-releaseRead }).
		Return(int64(len(data)), false, nil).Once()
	client.EXPECT().GetObject(mock.Anything, reader.bucket, key, int64(0), int64(len(data)), reader.logNs, reader.logIdStr).
		Return(&mockFileReader{data: data}, nil).Once()
	type readResult struct {
		batch *proto.BatchReadResult
		err   error
	}
	done := make(chan readResult, 1)
	go func() {
		state := &proto.LastReadState{SegmentId: reader.segmentId, Flags: 0, Version: uint32(codec.FormatVersion), LastBlockId: 131}
		batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 161, MaxBatchEntries: 200}, state)
		done <- readResult{batch, err}
	}()
	select {
	case <-reading:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	updated := make(chan struct{})
	go func() { defer close(updated); reader.asyncUpdateReaderState(ctx, compacted) }()
	waitForCompactionResumeLock(t, "asyncUpdateReaderState", "sync.(*RWMutex).Lock")
	unblock()
	var first readResult
	select {
	case first = <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-updated:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, first.err)
	require.Len(t, first.batch.Entries, 1)
	require.EqualValues(t, 161, first.batch.Entries[0].EntryId)
	require.False(t, codec.IsCompacted(uint16(first.batch.LastReadState.Flags)))
	require.EqualValues(t, 132, first.batch.LastReadState.LastBlockId)

	// Once the update completes, the next read must translate that old cursor
	// by logical entry ID and deliver every remaining entry from merged blocks.
	reader.maxBatchSize = 4 * 1024 * 1024
	expectCompactedResumeTail(t, reader, client, compacted, 17)
	batch, err := reader.ReadNextBatchAdv(ctx, storage.ReaderOpt{StartEntryID: 162, MaxBatchEntries: 200}, first.batch.LastReadState)
	require.NoError(t, err)
	requireCompactionResumeTail(t, batch, 162)
}

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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	gproto "google.golang.org/protobuf/proto"

	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	pb "github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage/cache"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/codec/checkpoint"
)

func checkpointTestIndexes(count int) []*codec.IndexRecord {
	blocks := make([]*codec.IndexRecord, count)
	for i := range blocks {
		blocks[i] = &codec.IndexRecord{BlockNumber: int32(i), StartOffset: int64(i), BlockSize: 128, FirstEntryID: int64(i), LastEntryID: int64(i)}
	}
	return blocks
}

func checkpointTestData(t *testing.T, blocks []*codec.IndexRecord) []byte {
	t.Helper()
	data, err := checkpoint.Serialize(checkpoint.New(blocks))
	require.NoError(t, err)
	return data
}

func uncheckedCheckpointTestData(t *testing.T, cp *pb.SegmentCheckpoint) []byte {
	t.Helper()
	payload, err := gproto.Marshal(cp)
	require.NoError(t, err)
	raw := append([]byte("WPCP"), payload...)
	return binary.LittleEndian.AppendUint32(raw, crc32.ChecksumIEEE(payload))
}

func checkpointTestWriter(t *testing.T, raw []byte) (*MinioFileWriter, *mocks_objectstorage.ObjectStorage) {
	t.Helper()
	w := newTestMinioFileWriter()
	t.Cleanup(w.pool.Release)
	m := mocks_objectstorage.NewObjectStorage(t)
	w.client = m
	missing := mockNoSuchKeyError()
	m.EXPECT().StatObject(mock.Anything, w.bucket, getFooterBlockKey(w.segmentFileKey), mock.Anything, mock.Anything).Return(int64(0), false, missing).Maybe()
	m.EXPECT().IsObjectNotExistsError(mock.Anything).RunAndReturn(func(err error) bool { return werr.ErrEntryNotFound.Is(err) }).Maybe()
	m.EXPECT().StatObject(mock.Anything, w.bucket, getCheckpointBlockKey(w.segmentFileKey), mock.Anything, mock.Anything).Return(int64(len(raw)), false, nil)
	m.EXPECT().GetObject(mock.Anything, w.bucket, getCheckpointBlockKey(w.segmentFileKey), int64(0), int64(len(raw)), mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, string, string, int64, int64, string, string) (minioHandler.FileReader, error) {
			return &mockFileReader{data: raw}, nil
		},
	)
	return w, m
}

func expectCheckpointTailBlock(t *testing.T, w *MinioFileWriter, m *mocks_objectstorage.ObjectStorage, blockID int64) {
	t.Helper()
	data := buildValidBlockData(t, blockID, blockID, [][]byte{[]byte("payload")})
	size := int64(codec.RecordHeaderSize + codec.BlockHeaderRecordSize)
	if blockID == 0 {
		size += int64(codec.RecordHeaderSize + codec.HeaderRecordSize)
	}
	key := getBlockKey(w.segmentFileKey, blockID)
	m.EXPECT().StatObject(mock.Anything, w.bucket, key, mock.Anything, mock.Anything).Return(int64(len(data)), false, nil)
	m.EXPECT().GetObject(mock.Anything, w.bucket, key, int64(0), size, mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, string, string, int64, int64, string, string) (minioHandler.FileReader, error) {
			return &mockFileReader{data: data[:size]}, nil
		},
	)
}

func expectCheckpointEnd(w *MinioFileWriter, m *mocks_objectstorage.ObjectStorage, blockID int64, fenced bool) {
	var err error
	if !fenced {
		err = mockNoSuchKeyError()
	}
	m.EXPECT().StatObject(mock.Anything, w.bucket, getBlockKey(w.segmentFileKey, blockID), mock.Anything, mock.Anything).Return(int64(0), fenced, err)
}

func TestCheckpointRecoveryOnlyReadsTail(t *testing.T) {
	// Check request counts, not wall-clock timing: cloud latency must multiply
	// only the tail length, even when the checkpoint covers hundreds of blocks.
	for _, prefix := range []int{0, 10, 635} {
		t.Run(fmt.Sprint(prefix), func(t *testing.T) {
			w, m := checkpointTestWriter(t, checkpointTestData(t, checkpointTestIndexes(prefix)))
			for i := int64(prefix); i < int64(prefix+5); i++ {
				expectCheckpointTailBlock(t, w, m, i)
			}
			expectCheckpointEnd(w, m, int64(prefix+5), false)
			require.NoError(t, w.recoverFromStorageUnsafe(context.Background()))
			require.Len(t, w.blockIndexes, prefix+5)
			require.Equal(t, int64(prefix+4), w.lastEntryID.Load())
			m.AssertNumberOfCalls(t, "StatObject", 8) // footer + checkpoint + 5 tail blocks + first gap
			m.AssertNumberOfCalls(t, "GetObject", 6)  // checkpoint + 5 headers
		})
	}
}

func TestCheckpointRecoveryFenceRetryReplacesIndexes(t *testing.T) {
	w, m := checkpointTestWriter(t, checkpointTestData(t, checkpointTestIndexes(1)))
	// Before the fence, block 1 is absent. The old owner then wins its PUT;
	// retry recovery sees that block and must replace, rather than append to,
	// the previous recovered index snapshot.
	key := getBlockKey(w.segmentFileKey, 1)
	m.EXPECT().StatObject(mock.Anything, w.bucket, key, mock.Anything, mock.Anything).Return(int64(0), false, mockNoSuchKeyError()).Once()
	expectCheckpointTailBlock(t, w, m, 1)
	expectCheckpointEnd(w, m, 2, false)
	m.EXPECT().PutFencedObject(mock.Anything, w.bucket, key, mock.Anything, mock.Anything).Return(werr.ErrObjectAlreadyExists).Once()
	m.EXPECT().PutFencedObject(mock.Anything, w.bucket, getBlockKey(w.segmentFileKey, 2), mock.Anything, mock.Anything).Return(nil).Once()
	require.NoError(t, w.recoverFromStorageUnsafe(context.Background()))
	lac, err := w.Fence(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(1), lac)
	require.Len(t, w.blockIndexes, 2)
	require.Equal(t, int32(0), w.blockIndexes[0].BlockNumber)
	require.Equal(t, int32(1), w.blockIndexes[1].BlockNumber)
	_, footer := serializeFooterAndIndexes(context.Background(), w.blockIndexes)
	require.Equal(t, int32(2), footer.TotalBlocks)
}

func TestCheckpointRecoveryStopsAtGapOrFence(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		t.Run(fmt.Sprint(fenced), func(t *testing.T) {
			w, m := checkpointTestWriter(t, checkpointTestData(t, checkpointTestIndexes(2)))
			// A later object may exist due to concurrent uploads. It must never be
			// read past the first missing block or fence (unexpected calls fail).
			expectCheckpointEnd(w, m, 2, fenced)
			require.NoError(t, w.recoverFromStorageUnsafe(context.Background()))
			require.Equal(t, int64(1), w.lastEntryID.Load())
			require.Equal(t, fenced, w.fenced.Load())
			m.AssertNumberOfCalls(t, "GetObject", 1)
		})
	}
}

func TestCheckpointRecoveryFailureLeavesStateUntouched(t *testing.T) {
	w, m := checkpointTestWriter(t, checkpointTestData(t, checkpointTestIndexes(1)))
	before := checkpointTestIndexes(3)
	w.blockIndexes = before
	w.lastEntryID.Store(2)
	expectCheckpointTailBlock(t, w, m, 1)
	failure := errors.New("temporary storage failure")
	m.EXPECT().StatObject(mock.Anything, w.bucket, getBlockKey(w.segmentFileKey, 2), mock.Anything, mock.Anything).Return(int64(0), false, failure)
	require.ErrorIs(t, w.recoverFromCheckpoint(context.Background()), failure)
	require.Equal(t, before, w.blockIndexes)
	require.Equal(t, int64(2), w.lastEntryID.Load())
}

func TestCheckpointRecoveryInvalidHintFallsBack(t *testing.T) {
	blocks := checkpointTestIndexes(2)
	raw := checkpointTestData(t, blocks)
	corruptCRC := append([]byte(nil), raw...)
	corruptCRC[5] ^= 0xff
	hole := checkpoint.New(blocks)
	hole.BlockIndexes[1].BlockNumber = 2
	hole.BlockIndexes[1].StartOffset = 2
	wrongLAC := checkpoint.New(blocks)
	wrongLAC.LastEntryId++
	unknownVersion := checkpoint.New(blocks)
	unknownVersion.FormatVersion++
	legacyRaw, _ := serializeFooterAndIndexes(context.Background(), blocks)
	legacyJSON := append([]byte(nil), legacyRaw...)
	meta := []byte(fmt.Sprintf(`{"version":1,"sections":[{"type":1,"offset":0,"length":%d}]}`, len(legacyRaw)))
	legacyJSON = append(legacyJSON, meta...)
	legacyJSON = binary.LittleEndian.AppendUint32(legacyJSON, uint32(len(meta)))
	legacyJSON = append(legacyJSON, []byte("CKPT")...)
	cases := map[string][]byte{
		"truncated":              {1, 2, 3},
		"invalid protobuf CRC":   corruptCRC,
		"non-contiguous indexes": uncheckedCheckpointTestData(t, hole),
		"wrong LAC":              uncheckedCheckpointTestData(t, wrongLAC),
		"unsupported version":    uncheckedCheckpointTestData(t, unknownVersion),
		"legacy raw snapshot":    legacyRaw,
		"legacy JSON snapshot":   legacyJSON,
	}

	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			w, m := checkpointTestWriter(t, data)
			expectCheckpointTailBlock(t, w, m, 0)
			expectCheckpointEnd(w, m, 1, false)
			require.NoError(t, w.recoverFromStorageUnsafe(context.Background()))
			require.Len(t, w.blockIndexes, 1)
			require.Equal(t, int64(0), w.lastEntryID.Load())
		})
	}
}

func TestCheckpointRecoveryEmptyAndCompletePrefix(t *testing.T) {
	for _, count := range []int{0, 2} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			w, m := checkpointTestWriter(t, checkpointTestData(t, checkpointTestIndexes(count)))
			expectCheckpointEnd(w, m, int64(count), true)
			require.NoError(t, w.recoverFromStorageUnsafe(context.Background()))
			require.Len(t, w.blockIndexes, count)
			require.Equal(t, int64(count-1), w.lastEntryID.Load())
			require.True(t, w.fenced.Load())
		})
	}
}

func TestCheckpointWritesCoalesce(t *testing.T) {
	w := newTestMinioFileWriter()
	t.Cleanup(w.pool.Release)
	w.checkpointThreshold = 1
	m := mocks_objectstorage.NewObjectStorage(t)
	w.client = m
	started := make(chan struct{})
	release := make(chan struct{})
	written := make(chan int32, 3)
	var calls atomic.Int32
	m.EXPECT().PutObject(mock.Anything, w.bucket, getCheckpointBlockKey(w.segmentFileKey), mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _, _ string, reader io.Reader, _ int64, _, _ string) error {
			data, err := io.ReadAll(reader)
			if err != nil {
				return err
			}
			cp, err := checkpoint.Parse(data)
			if err != nil {
				return err
			}

			if calls.Add(1) == 1 {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			written <- int32(len(cp.BlockIndexes))
			return nil
		},
	)
	w.blockIndexes = checkpointTestIndexes(1)
	w.maybeWriteCheckpointAsync(context.Background())
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint did not start")
	}
	w.blockIndexes = checkpointTestIndexes(2)
	w.maybeWriteCheckpointAsync(context.Background())
	w.blockIndexes = checkpointTestIndexes(3)
	w.maybeWriteCheckpointAsync(context.Background())
	close(release)
	for _, expected := range []int32{1, 3} {
		select {
		case count := <-written:
			require.Equal(t, expected, count)
		case <-time.After(5 * time.Second):
			t.Fatal("checkpoint did not finish")
		}
	}
	require.Eventually(t, func() bool { return w.lastCheckpointBlockCount.Load() == 3 }, time.Second, time.Millisecond)
	require.Equal(t, int32(2), calls.Load())
}

func TestCheckpointWriteFailureDoesNotFailAcknowledgment(t *testing.T) {
	w := newTestMinioFileWriter()
	t.Cleanup(w.pool.Release)
	w.checkpointThreshold = 1
	m := mocks_objectstorage.NewObjectStorage(t)
	w.client = m
	failure := errors.New("checkpoint PUT failed")
	m.EXPECT().PutObject(mock.Anything, w.bucket, getCheckpointBlockKey(w.segmentFileKey), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(failure).Once()
	results := newMockResultChannel()
	// The checkpoint goroutine can fail without affecting the successful data
	// acknowledgment; the next committed block schedules another checkpoint.
	w.fastFlushSuccessUnsafe(context.Background(), &BlockInfo{BlockID: 0, FirstEntryID: 0, LastEntryID: 0, Size: 128}, []*cache.BufferEntry{{EntryId: 0, NotifyChan: results}})
	require.Len(t, results.results, 1)
	require.NoError(t, results.results[0].Err)
	require.Eventually(t, func() bool {
		w.checkpointMu.Lock()
		defer w.checkpointMu.Unlock()
		return !w.checkpointRunning
	}, time.Second, time.Millisecond)
	require.Zero(t, w.lastCheckpointBlockCount.Load())
	m.EXPECT().PutObject(mock.Anything, w.bucket, getCheckpointBlockKey(w.segmentFileKey), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	w.fastFlushSuccessUnsafe(context.Background(), &BlockInfo{BlockID: 1, FirstEntryID: 1, LastEntryID: 1, Size: 128}, nil)
	require.Eventually(t, func() bool { return w.lastCheckpointBlockCount.Load() == 2 }, time.Second, time.Millisecond)
}

func TestCheckpointWritesDisabledOrStopped(t *testing.T) {
	for _, state := range []string{"disabled", "closed", "finalized", "recovery"} {
		t.Run(state, func(t *testing.T) {
			w := newTestMinioFileWriter()
			t.Cleanup(w.pool.Release)
			w.client = mocks_objectstorage.NewObjectStorage(t)
			w.checkpointThreshold = 1
			w.blockIndexes = checkpointTestIndexes(2)
			switch state {
			case "disabled":
				w.checkpointThreshold = 0
			case "closed":
				w.closed.Store(true)
			case "finalized":
				w.finalized.Store(true)
			case "recovery":
				w.inRecoveryMode.Store(true)
			}
			w.maybeWriteCheckpointAsync(context.Background())
			require.False(t, w.checkpointRunning)
			require.Nil(t, w.checkpointPendingSnapshot)
		})
	}
}

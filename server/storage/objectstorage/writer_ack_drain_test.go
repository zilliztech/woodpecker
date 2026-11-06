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
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/conc"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
)

// Keep workers alive well beyond every test deadline, so success proves that
// completion depends on uploads and ACKs rather than on worker retirement.
func newACKDrainTestWriter(t *testing.T) (*MinioFileWriter, *mocks_objectstorage.ObjectStorage) {
	t.Helper()
	w := newTestMinioFileWriter()
	w.pool.Release()
	w.pool = conc.NewPool[*blockUploadResult](4, conc.WithPreAlloc(true), conc.WithExpiryDuration(time.Hour))
	client := mocks_objectstorage.NewObjectStorage(t)
	w.client = client
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		w.ack()
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		assert.NoError(t, w.Close(ctx))
		select {
		case <-ackDone:
		case <-time.After(2 * time.Second):
			t.Error("ACK consumer did not exit")
		}
	})
	return w, client
}

func expectDrainTestFooter(t *testing.T, w *MinioFileWriter, client *mocks_objectstorage.ObjectStorage) {
	t.Helper()
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, w.bucket, getFooterBlockKey(w.segmentFileKey),
		mock.Anything, mock.Anything, w.logNs, w.logIdStr).Return(nil).Once()
}

func TestMinioFileWriter_CompletionDoesNotWaitForIdleWorkers(t *testing.T) {
	for _, operation := range []string{"finalize", "close"} {
		t.Run(operation, func(t *testing.T) {
			w, client := newACKDrainTestWriter(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			client.EXPECT().PutObjectIfNoneMatch(mock.Anything, w.bucket, getBlockKey(w.segmentFileKey, 0),
				mock.Anything, mock.Anything, w.logNs, w.logIdStr).Return(nil).Once()
			resultCh := channel.NewLocalResultChannel("completed-upload")
			_, err := w.WriteDataAsync(ctx, 0, []byte("already uploaded"), resultCh)
			require.NoError(t, err)
			require.NoError(t, w.Sync(ctx))
			result, err := resultCh.ReadResult(ctx)
			require.NoError(t, err)
			require.NoError(t, result.Err)
			require.Equal(t, 1, w.pool.Running(), "completed upload still has a live worker")

			if operation == "finalize" {
				expectDrainTestFooter(t, w, client)
				lastID, err := w.Finalize(ctx, -1)
				require.NoError(t, err)
				require.EqualValues(t, 0, lastID)
				require.Equal(t, 1, w.pool.Running(), "finalization must not retire idle workers")
				require.EqualValues(t, 1, w.footerRecord.TotalBlocks)
			} else {
				require.NoError(t, w.Close(ctx))
				// Recovery callers may finalize a writer that already closed.
				expectDrainTestFooter(t, w, client)
				_, err = w.Finalize(ctx, -1)
				require.NoError(t, err)
			}
			require.True(t, w.allUploadingTaskDone.Load())
			require.Len(t, w.blockIndexes, 1)
		})
	}
}

func TestMinioFileWriter_CloseSyncByRecoveryMode(t *testing.T) {
	for _, tc := range []struct {
		name     string
		recovery bool
	}{
		{name: "normal"},
		{name: "recovery", recovery: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, client := newACKDrainTestWriter(t)
			w.inRecoveryMode.Store(tc.recovery)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			resultCh := channel.NewLocalResultChannel("close-pending")
			if tc.recovery {
				// Inject unexpected buffered data to prove Close skips Sync
				// independently of the normally empty recovery buffer.
				_, err := w.buffer.Load().WriteEntryWithNotify(0, []byte("unexpected"), resultCh)
				require.NoError(t, err)
			} else {
				client.EXPECT().PutObjectIfNoneMatch(mock.Anything, w.bucket, getBlockKey(w.segmentFileKey, 0),
					mock.Anything, mock.Anything, w.logNs, w.logIdStr).Return(nil).Once()
				_, err := w.WriteDataAsync(ctx, 0, []byte("pending"), resultCh)
				require.NoError(t, err)
			}
			require.NoError(t, w.Close(ctx))
			if tc.recovery {
				client.AssertNumberOfCalls(t, "PutObjectIfNoneMatch", 0)
				require.NotNil(t, w.buffer.Load().Entries[0])
				require.EqualValues(t, 1, w.buffer.Load().GetExpectedNextEntryId())
				require.EqualValues(t, -1, w.lastEntryID.Load())
			} else {
				result, err := resultCh.ReadResult(ctx)
				require.NoError(t, err)
				require.NoError(t, result.Err)
				require.EqualValues(t, 0, w.lastEntryID.Load())
				require.Len(t, w.blockIndexes, 1)
			}
		})
	}
}

type drainTestResultChannel struct {
	channel.ResultChannel
	ackStarted chan struct{}
	releaseACK chan struct{}
}

func (c *drainTestResultChannel) SendResult(ctx context.Context, result *channel.AppendResult) error {
	close(c.ackStarted)
	<-c.releaseACK
	return c.ResultChannel.SendResult(ctx, result)
}

func TestMinioFileWriter_FinalizeWaitsForUploadAndACK(t *testing.T) {
	w, client := newACKDrainTestWriter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	uploadStarted := make(chan struct{})
	releaseUpload := make(chan struct{})
	unblockUpload := sync.OnceFunc(func() { close(releaseUpload) })
	t.Cleanup(unblockUpload)
	resultCh := &drainTestResultChannel{
		ResultChannel: channel.NewLocalResultChannel("in-flight-upload"),
		ackStarted:    make(chan struct{}),
		releaseACK:    make(chan struct{}),
	}
	unblockACK := sync.OnceFunc(func() { close(resultCh.releaseACK) })
	t.Cleanup(unblockACK)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, w.bucket, getBlockKey(w.segmentFileKey, 0),
		mock.Anything, mock.Anything, w.logNs, w.logIdStr).
		Run(func(context.Context, string, string, io.Reader, int64, string, string) {
			close(uploadStarted)
			<-releaseUpload
		}).Return(nil).Once()
	footerWritten := make(chan struct{})
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, w.bucket, getFooterBlockKey(w.segmentFileKey),
		mock.Anything, mock.Anything, w.logNs, w.logIdStr).
		Run(func(context.Context, string, string, io.Reader, int64, string, string) { close(footerWritten) }).Return(nil).Once()
	_, err := w.WriteDataAsync(ctx, 0, []byte("in flight"), resultCh)
	require.NoError(t, err)
	finalized := make(chan error, 1)
	go func() {
		_, err := w.Finalize(ctx, -1)
		finalized <- err
	}()
	select {
	case <-uploadStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.False(t, w.allUploadingTaskDone.Load())
	select {
	case <-footerWritten:
		t.Fatal("footer written before the upload completed")
	default:
	}
	unblockUpload()
	select {
	case <-resultCh.ackStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// ACK processing is blocked, so the queued item is the termination marker.
	require.Eventually(t, func() bool {
		return len(w.flushingTaskList) == 1
	}, time.Second, time.Millisecond)
	select {
	case <-footerWritten:
		t.Fatal("footer written before ACK processing completed")
	default:
	}
	require.False(t, w.allUploadingTaskDone.Load())
	unblockACK()
	select {
	case err := <-finalized:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Len(t, w.blockIndexes, 1)
	require.EqualValues(t, 1, w.footerRecord.TotalBlocks)
}

func TestMinioFileWriter_FinalizeRetryAfterCancelledSync(t *testing.T) {
	w, client := newACKDrainTestWriter(t)
	resultCh := channel.NewLocalResultChannel("sync-retry")
	_, err := w.WriteDataAsync(context.Background(), 0, []byte("pending"), resultCh)
	require.NoError(t, err)
	w.flushingBufferSize.Store(w.maxBufferSize)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = w.Finalize(ctx, -1)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, w.finalizing.Load())
	require.False(t, w.finalized.Load())
	require.True(t, w.storageWritable.Load())
	require.False(t, w.allUploadingTaskDone.Load())
	require.Empty(t, w.flushingTaskList, "failed Sync must not enqueue a termination marker")
	require.EqualValues(t, 1, w.buffer.Load().GetExpectedNextEntryId())
	_, err = w.WriteDataAsync(context.Background(), 1, []byte("too late"), resultCh)
	require.True(t, werr.ErrFileWriterFinalizing.Is(err))

	w.flushingBufferSize.Store(0)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, w.bucket, getBlockKey(w.segmentFileKey, 0),
		mock.Anything, mock.Anything, w.logNs, w.logIdStr).Return(nil).Once()
	expectDrainTestFooter(t, w, client)
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer retryCancel()
	lastID, err := w.Finalize(retryCtx, -1)
	require.NoError(t, err)
	require.EqualValues(t, 0, lastID)
	require.True(t, w.finalized.Load())
	require.False(t, w.finalizing.Load())
	require.Len(t, w.blockIndexes, 1)
	result, err := resultCh.ReadResult(retryCtx)
	require.NoError(t, err)
	require.NoError(t, result.Err)
}

func TestMinioFileWriter_FinalizeRetryAfterCancelledDrain(t *testing.T) {
	w, client := newACKDrainTestWriter(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultCh := &drainTestResultChannel{
		ResultChannel: channel.NewLocalResultChannel("retry-upload"),
		ackStarted:    make(chan struct{}),
		releaseACK:    make(chan struct{}),
	}
	unblock := sync.OnceFunc(func() { close(resultCh.releaseACK) })
	t.Cleanup(unblock)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, w.bucket, getBlockKey(w.segmentFileKey, 0),
		mock.Anything, mock.Anything, w.logNs, w.logIdStr).Return(nil).Once()
	_, err := w.WriteDataAsync(ctx, 0, []byte("retry"), resultCh)
	require.NoError(t, err)
	firstDone := make(chan error, 1)
	go func() {
		_, err := w.Finalize(ctx, -1)
		firstDone <- err
	}()
	select {
	case <-resultCh.ackStarted:
	case <-time.After(time.Second):
		t.Fatal("ACK processing did not start")
	}
	require.Eventually(t, func() bool {
		return len(w.flushingTaskList) == 1
	}, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-firstDone, context.Canceled)
	require.True(t, w.finalizing.Load())
	require.False(t, w.finalized.Load())
	_, err = w.WriteDataAsync(context.Background(), 1, []byte("too late"), resultCh)
	require.True(t, werr.ErrFileWriterFinalizing.Is(err), "cancelled drain must not reopen write admission")
	require.NoError(t, w.Sync(context.Background()))
	expectDrainTestFooter(t, w, client)
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer retryCancel()
	retryDone := make(chan error, 1)
	go func() {
		_, err := w.Finalize(retryCtx, -1)
		retryDone <- err
	}()
	// Like the staged writer, a retry may enqueue another marker while ACK
	// processing is pending. It must not repeat uploads or advance indexes twice.
	require.Eventually(t, func() bool {
		return len(w.flushingTaskList) == 2
	}, time.Second, time.Millisecond)
	unblock()
	require.NoError(t, <-retryDone)
	require.True(t, w.allUploadingTaskDone.Load())
	require.Len(t, w.blockIndexes, 1)
	require.EqualValues(t, 1, w.footerRecord.TotalBlocks)
	require.True(t, w.finalized.Load())
	require.False(t, w.finalizing.Load())
	require.NoError(t, w.Close(retryCtx))
	_, err = w.Finalize(retryCtx, -1)
	require.NoError(t, err, "already-finalized retries remain idempotent after Close")
}

func TestMinioFileWriter_DrainWaitsForTaskSubmission(t *testing.T) {
	w, client := newACKDrainTestWriter(t)
	// Model a producer holding Sync's submission lock before its last task
	// reaches the ACK queue. The drain must not overtake that producer.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w.finalizing.Store(true)
	expectDrainTestFooter(t, w, client)
	w.syncMu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			w.syncMu.Unlock()
		}
	}()
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		_, err := w.Finalize(ctx, -1)
		done <- err
	}()
	<-started
	select {
	case err := <-done:
		t.Fatalf("drain overtook an active producer: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	future := w.pool.Submit(func() (*blockUploadResult, error) {
		return &blockUploadResult{block: &BlockInfo{BlockID: 0, FirstEntryID: 0, LastEntryID: 0}}, nil
	})
	w.flushingTaskList <- &blockUploadTask{flushData: []*cache.BufferEntry{}, flushFuture: future}
	w.syncMu.Unlock()
	unlocked = true
	require.NoError(t, <-done)
	require.Len(t, w.blockIndexes, 1, "marker must follow the last producer's queued task")
	require.NoError(t, w.Sync(ctx), "periodic Sync after ACK completion is a no-op")
}

func TestMinioFileWriter_FinalizeAfterCancelledClose(t *testing.T) {
	w := newTestMinioFileWriter()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, w.Close(ctx), context.Canceled)
	_, err := w.Finalize(context.Background(), -1)
	require.True(t, werr.ErrFileWriterAlreadyClosed.Is(err), "failed Close must not send on the destroyed queue")
	require.True(t, werr.ErrFileWriterAlreadyClosed.Is(w.Sync(context.Background())), "Sync must not use the closed task queue")
}

func TestMinioFileWriter_FinalizeRetryAfterFooterFailure(t *testing.T) {
	w, client := newACKDrainTestWriter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	footerErr := errors.New("footer upload failed")
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, w.bucket, getFooterBlockKey(w.segmentFileKey),
		mock.Anything, mock.Anything, w.logNs, w.logIdStr).Return(footerErr).Once()
	_, err := w.Finalize(ctx, -1)
	require.ErrorIs(t, err, footerErr)
	require.True(t, w.allUploadingTaskDone.Load())
	require.True(t, w.finalizing.Load(), "footer failure must keep write admission closed")
	require.False(t, w.finalized.Load())
	_, err = w.WriteDataAsync(ctx, 0, []byte("too late"), channel.NewLocalResultChannel("footer-retry"))
	require.True(t, werr.ErrFileWriterFinalizing.Is(err))
	expectDrainTestFooter(t, w, client)
	_, err = w.Finalize(ctx, -1)
	require.NoError(t, err)
	require.False(t, w.finalizing.Load())
	require.True(t, w.finalized.Load())
	require.Empty(t, w.flushingTaskList)
}

func TestMinioFileWriter_UploadFailureDrainsACK(t *testing.T) {
	w, client := newACKDrainTestWriter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, w.bucket, getBlockKey(w.segmentFileKey, 0),
		mock.Anything, mock.Anything, w.logNs, w.logIdStr).Return(werr.ErrSegmentFenced).Once()
	expectDrainTestFooter(t, w, client)
	resultCh := channel.NewLocalResultChannel("failed-upload")
	_, err := w.WriteDataAsync(ctx, 0, []byte("failed"), resultCh)
	require.NoError(t, err)
	lastID, err := w.Finalize(ctx, -1)
	require.NoError(t, err)
	require.EqualValues(t, -1, lastID, "failed uploads must not advance the confirmed entry")
	result, err := resultCh.ReadResult(ctx)
	require.NoError(t, err)
	require.True(t, werr.ErrSegmentFenced.Is(result.Err))
	require.False(t, w.storageWritable.Load())
	require.True(t, w.allUploadingTaskDone.Load())
	require.Empty(t, w.blockIndexes)
	require.EqualValues(t, 0, w.footerRecord.TotalBlocks)
}

func TestMinioFileWriter_CloseWaitsForConcurrentFinalize(t *testing.T) {
	w, client := newACKDrainTestWriter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	footerStarted := make(chan struct{})
	releaseFooter := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(releaseFooter) })
	t.Cleanup(unblock)
	client.EXPECT().PutObjectIfNoneMatch(mock.Anything, w.bucket, getFooterBlockKey(w.segmentFileKey),
		mock.Anything, mock.Anything, w.logNs, w.logIdStr).
		Run(func(context.Context, string, string, io.Reader, int64, string, string) {
			close(footerStarted)
			<-releaseFooter
		}).Return(nil).Once()
	finalized := make(chan error, 1)
	go func() {
		_, err := w.Finalize(ctx, -1)
		finalized <- err
	}()
	select {
	case <-footerStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	closed := make(chan error, 1)
	closeStarted := make(chan struct{})
	go func() {
		close(closeStarted)
		closed <- w.Close(ctx)
	}()
	<-closeStarted
	select {
	case err := <-closed:
		t.Fatalf("Close released resources before Finalize returned: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	require.False(t, w.closed.Load())
	unblock()
	require.NoError(t, <-finalized)
	require.NoError(t, <-closed)
	require.Empty(t, w.flushingTaskList)
}

func TestMinioFileWriter_DrainFullQueueTimeoutAndRetry(t *testing.T) {
	w := newTestMinioFileWriter()
	t.Cleanup(w.pool.Release)
	w.finalizing.Store(true)
	// Hold off the ACK consumer until the queue is full and sending the marker
	// reaches the caller's deadline. These are valid, already completed tasks.
	for i := 0; i < cap(w.flushingTaskList); i++ {
		blockID := int64(i)
		future := w.pool.Submit(func() (*blockUploadResult, error) {
			return &blockUploadResult{block: &BlockInfo{
				BlockID: blockID, FirstEntryID: blockID, LastEntryID: blockID,
			}}, nil
		})
		_, err := future.Await()
		require.NoError(t, err)
		w.flushingTaskList <- &blockUploadTask{flushData: []*cache.BufferEntry{}, flushFuture: future}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	require.NoError(t, w.Sync(ctx))
	// The drain must honor its deadline even while another caller holds syncMu.
	// Reacquiring the lock here would wait past the caller's deadline.
	w.syncMu.Lock()
	done := make(chan error, 1)
	go func() { done <- w.awaitAllFlushTasks(ctx) }()
	select {
	case err := <-done:
		w.syncMu.Unlock()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		w.syncMu.Unlock()
		t.Fatal("ACK drain blocked on syncMu past its deadline")
	}
	require.Len(t, w.flushingTaskList, cap(w.flushingTaskList), "timed-out send must not enqueue a marker")

	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		w.ack()
	}()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cleanupCancel()
		assert.NoError(t, w.Close(cleanupCtx))
		select {
		case <-ackDone:
		case <-time.After(2 * time.Second):
			t.Error("ACK consumer did not exit")
		}
	})
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer retryCancel()
	require.NoError(t, w.awaitAllFlushTasks(retryCtx))
	require.Len(t, w.blockIndexes, cap(w.flushingTaskList))
	require.Empty(t, w.flushingTaskList)
}

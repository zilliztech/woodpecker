// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectstorage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/conc"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
)

func flushQueueHistogram(t *testing.T, w *MinioFileWriter) *dto.Histogram {
	t.Helper()
	h := metrics.WpFileFlushQueueWaitLatency.WithLabelValues(metrics.NodeID, w.logNs, w.logIdStr).(prometheus.Metric)
	m := &dto.Metric{}
	require.NoError(t, h.Write(m))
	return m.GetHistogram()
}

func TestFlushQueueWaitStopsBeforeObjectUpload(t *testing.T) {
	w := newTestMinioFileWriter()
	w.logNs = t.Name()
	metrics.WpFileFlushQueueWaitLatency.DeleteLabelValues(metrics.NodeID, w.logNs, w.logIdStr)
	w.pool.Release()
	w.pool = conc.NewPool[*blockUploadResult](1)
	workerGate, workerStarted := make(chan struct{}), make(chan struct{})
	uploadGate, uploadStarted := make(chan struct{}), make(chan struct{})
	var workerOnce, uploadOnce sync.Once
	releaseWorker := func() { workerOnce.Do(func() { close(workerGate) }) }
	releaseUpload := func() { uploadOnce.Do(func() { close(uploadGate) }) }
	t.Cleanup(func() { releaseWorker(); releaseUpload(); w.pool.Release() })
	w.pool.Submit(func() (*blockUploadResult, error) { close(workerStarted); <-workerGate; return nil, nil })
	<-workerStarted
	m := mocks_objectstorage.NewObjectStorage(t)
	w.client = m
	// Use On/Run to hold storage I/O after the pool worker begins executing.
	m.On("PutObjectIfNoneMatch", mock.Anything, w.bucket, "test-base/1/0/0.blk", mock.Anything, mock.Anything, w.logNs, w.logIdStr).
		Run(func(mock.Arguments) { close(uploadStarted); <-uploadGate }).Return(nil).Once()
	done := make(chan []*conc.Future[*blockUploadResult], 1)
	go func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		done <- w.submitBlockFlushTaskUnsafe(context.Background(), w.buffer.Load(), []*cache.BufferEntry{{EntryId: 0, Data: []byte("queued")}}, 0)
	}()
	// The batch-ready timestamp is set before publishing this block ID. Hold
	// the only worker after that point so the lower bound is deterministic,
	// even if the submitter has not reached Pool.Submit yet.
	require.Eventually(t, func() bool { return w.lastSubmittedUploadingBlockID.Load() == 0 }, time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, flushQueueHistogram(t, w).GetSampleCount(), "a task waiting for a pool worker has not started")
	releaseWorker()
	select {
	case <-uploadStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("upload did not start")
	}
	h := flushQueueHistogram(t, w)
	require.EqualValues(t, 1, h.GetSampleCount())
	queuedMS := h.GetSampleSum()
	require.GreaterOrEqual(t, queuedMS, float64(20), "waiting for pool admission must be measured")
	releaseUpload()
	var futures []*conc.Future[*blockUploadResult]
	select {
	case futures = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("flush submission did not return")
	}
	require.Len(t, futures, 1)
	_, err := futures[0].Await()
	require.NoError(t, err)
	require.Equal(t, queuedMS, flushQueueHistogram(t, w).GetSampleSum(), "upload execution must not extend queue wait")
}

func TestFlushQueueWaitIncludesObjectQuotaBackpressure(t *testing.T) {
	w := newTestMinioFileWriter()
	w.logNs = t.Name()
	metrics.WpFileFlushQueueWaitLatency.DeleteLabelValues(metrics.NodeID, w.logNs, w.logIdStr)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	w.maxBufferSize = 1
	w.flushingBufferSize.Store(1)
	uploadStarted := make(chan struct{})
	m := mocks_objectstorage.NewObjectStorage(t)
	w.client = m
	m.On("PutObjectIfNoneMatch", mock.Anything, w.bucket, "test-base/1/0/0.blk", mock.Anything, mock.Anything, w.logNs, w.logIdStr).
		Run(func(mock.Arguments) { close(uploadStarted) }).Return(nil).Once()
	t.Cleanup(func() { w.flushingBufferSize.Store(0); w.pool.Release() })
	done := make(chan []*conc.Future[*blockUploadResult], 1)
	go func() {
		done <- w.submitBlockFlushTaskUnsafe(ctx, w.buffer.Load(), []*cache.BufferEntry{{EntryId: 0, Data: []byte("queued")}}, 0)
	}()
	require.Eventually(t, func() bool { return w.lastSubmittedUploadingBlockID.Load() == 0 }, time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, flushQueueHistogram(t, w).GetSampleCount(), "quota-blocked data has not started processing")
	w.flushingBufferSize.Store(0)
	select {
	case <-uploadStarted:
	case <-ctx.Done():
		t.Fatal("quota release did not start the upload")
	}
	h := flushQueueHistogram(t, w)
	require.EqualValues(t, 1, h.GetSampleCount())
	require.GreaterOrEqual(t, h.GetSampleSum(), float64(20), "quota backpressure precedes pool submission but is still queue wait")
	select {
	case futures := <-done:
		require.Len(t, futures, 1)
		_, err := futures[0].Await()
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("flush submission did not return")
	}
}

func TestFlushQueueWaitAccumulatesAcrossObjectBatchACKWindow(t *testing.T) {
	w := newTestMinioFileWriter()
	w.logNs = t.Name()
	metrics.WpFileFlushQueueWaitLatency.DeleteLabelValues(metrics.NodeID, w.logNs, w.logIdStr)
	w.syncPolicyConfig.MaxFlushSize = config.NewByteSize(1)
	// Drive ACK-window admission explicitly, without an ACK consumer. Each send
	// must wait until this test admits it before the next block can be submitted.
	w.flushingTaskList = make(chan *blockUploadTask)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	started := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	lastUploadGate := make(chan struct{})
	var once sync.Once
	releaseLastUpload := func() { once.Do(func() { close(lastUploadGate) }) }
	m := mocks_objectstorage.NewObjectStorage(t)
	w.client = m
	for i, key := range []string{"test-base/1/0/0.blk", "test-base/1/0/1.blk", "test-base/1/0/2.blk"} {
		m.On("PutObjectIfNoneMatch", mock.Anything, w.bucket, key, mock.Anything, mock.Anything, w.logNs, w.logIdStr).
			Run(func(mock.Arguments) {
				close(started[i])
				if i == 2 {
					<-lastUploadGate
				}
			}).Return(nil).Once()
	}
	done := make(chan []*conc.Future[*blockUploadResult], 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		releaseLastUpload()
		// Also unblock the submitter if an assertion failed before all ACK slots
		// were admitted. No production queue or pool API is changed for testing.
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-finished:
				// An early assertion may leave the result unconsumed. Join its
				// uploads before mock expectations and context cleanup run.
				select {
				case futures := <-done:
					for _, future := range futures {
						_, _ = future.Await()
					}
				default:
				}
				w.pool.Release()
				return
			case <-w.flushingTaskList:
			case <-timer.C:
				w.pool.Release()
				t.Error("flush submitter did not exit")
				return
			}
		}
	})
	go func() {
		defer close(finished)
		done <- w.submitBlockFlushTaskUnsafe(ctx, w.buffer.Load(), []*cache.BufferEntry{
			{EntryId: 0, Data: []byte("a")},
			{EntryId: 1, Data: []byte("b")},
			{EntryId: 2, Data: []byte("c")},
		}, 0)
	}()
	waitStarted := func(block int) {
		t.Helper()
		select {
		case <-started[block]:
		case <-ctx.Done():
			t.Fatalf("block %d did not start", block)
		}
	}
	admitACK := func() {
		t.Helper()
		select {
		case <-w.flushingTaskList:
		case <-ctx.Done():
			t.Fatal("no ACK-window task to admit")
		}
	}
	waitStarted(0)
	firstMS := flushQueueHistogram(t, w).GetSampleSum()
	time.Sleep(20 * time.Millisecond)
	admitACK()
	waitStarted(1)
	h := flushQueueHistogram(t, w)
	require.EqualValues(t, 2, h.GetSampleCount())
	secondMS := h.GetSampleSum() - firstMS
	require.GreaterOrEqual(t, secondMS, float64(20), "second block must include the first ACK-window wait")
	time.Sleep(20 * time.Millisecond)
	admitACK()
	waitStarted(2)
	h = flushQueueHistogram(t, w)
	require.EqualValues(t, 3, h.GetSampleCount())
	thirdMS := h.GetSampleSum() - firstMS - secondMS
	require.GreaterOrEqual(t, thirdMS, float64(40), "third block must retain both earlier ACK-window waits")
	require.Greater(t, thirdMS, secondMS, "queue wait must accumulate across the prepared batch")
	queuedMS := h.GetSampleSum()
	releaseLastUpload()
	admitACK()
	select {
	case futures := <-done:
		require.Len(t, futures, 3)
		for _, future := range futures {
			_, err := future.Await()
			require.NoError(t, err)
		}
	case <-ctx.Done():
		t.Fatal("flush submission did not return")
	}
	require.Equal(t, queuedMS, flushQueueHistogram(t, w).GetSampleSum(), "the last block's upload must not extend queue wait")
}

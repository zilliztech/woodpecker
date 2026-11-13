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

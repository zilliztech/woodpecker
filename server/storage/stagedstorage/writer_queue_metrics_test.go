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

package stagedstorage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/server/storage/cache"
)

func flushQueueHistogram(t *testing.T, w *StagedFileWriter) *dto.Histogram {
	t.Helper()
	h := metrics.WpFileFlushQueueWaitLatency.WithLabelValues(metrics.NodeID, w.logNs, w.logIdStr).(prometheus.Metric)
	m := &dto.Metric{}
	require.NoError(t, h.Write(m))
	return m.GetHistogram()
}

func TestFlushQueueWaitIncludesSchedulerButExcludesExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	scheduler := NewSyncScheduler(1)
	gate, started := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(func() { release(); scheduler.Close() })
	require.True(t, scheduler.tryEnqueueJob(func(context.Context) { close(started); <-gate }))
	<-started

	w, err := NewStagedFileWriter(ctx, "queue-metrics", t.Name(), t.TempDir(), 1, 0, nil, newTestConfig(t), scheduler)
	require.NoError(t, err)
	metrics.WpFileFlushQueueWaitLatency.DeleteLabelValues(metrics.NodeID, w.logNs, w.logIdStr)
	w.flushMu.Lock() // Keep execution blocked even after the task leaves its queue.
	locked := true
	t.Cleanup(func() {
		release()
		if locked {
			w.flushMu.Unlock()
		}
		_ = w.Close(context.Background())
	})
	ch := channel.NewLocalResultChannel(t.Name())
	_, err = w.WriteDataAsync(ctx, 0, []byte("queued"), ch)
	require.NoError(t, err)
	require.NoError(t, w.Sync(ctx))
	require.Len(t, w.flushTaskChan, 1)
	require.Zero(t, flushQueueHistogram(t, w).GetSampleCount())
	time.Sleep(20 * time.Millisecond)
	release()

	require.Eventually(t, func() bool { return flushQueueHistogram(t, w).GetSampleCount() == 1 }, time.Second, time.Millisecond)
	queuedMS := flushQueueHistogram(t, w).GetSampleSum()
	require.GreaterOrEqual(t, queuedMS, float64(20), "scheduler waiting must be measured in milliseconds")
	_, ready := ch.TryReadResult()
	require.False(t, ready, "queue timing is recorded before flush execution completes")
	w.flushMu.Unlock()
	locked = false
	result, err := ch.ReadResult(ctx)
	require.NoError(t, err)
	require.NoError(t, result.Err)
	require.Equal(t, queuedMS, flushQueueHistogram(t, w).GetSampleSum(), "disk execution must not extend queue wait")
	require.EqualValues(t, 1, flushQueueHistogram(t, w).GetSampleCount())
}

func TestFlushQueueWaitSkipsControlAndUnsubmittedTasks(t *testing.T) {
	w := &StagedFileWriter{logNs: t.Name(), logIdStr: "1"}
	w.processFlushTask(context.Background(), nil)
	w.processFlushTask(context.Background(), &blockFlushTask{queuedAt: time.Now().Add(-time.Hour)})
	w.processFlushTask(context.Background(), &blockFlushTask{entries: []*cache.BufferEntry{{EntryId: 0}}})
	require.Zero(t, flushQueueHistogram(t, w).GetSampleCount())
}

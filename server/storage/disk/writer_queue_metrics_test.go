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

package disk

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/server/storage/cache"
)

func flushQueueHistogram(t *testing.T, w *LocalFileWriter) *dto.Histogram {
	t.Helper()
	h := metrics.WpFileFlushQueueWaitLatency.WithLabelValues(metrics.NodeID, w.logNs, w.logIdStr).(prometheus.Metric)
	m := &dto.Metric{}
	require.NoError(t, h.Write(m))
	return m.GetHistogram()
}

func TestFlushQueueWaitTracksTasksBehindAnExecutingFlush(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	w, err := NewLocalFileWriter(ctx, t.TempDir(), 91027, 0, cfg)
	require.NoError(t, err)
	metrics.WpFileFlushQueueWaitLatency.DeleteLabelValues(metrics.NodeID, w.logNs, w.logIdStr)
	w.flushMu.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			w.flushMu.Unlock()
		}
		_ = w.Close(context.Background())
	})
	first := channel.NewLocalResultChannel(t.Name() + "-first")
	_, err = w.WriteDataAsync(ctx, 0, []byte("first"), first)
	require.NoError(t, err)
	require.NoError(t, w.Sync(ctx))
	require.Eventually(t, func() bool { return flushQueueHistogram(t, w).GetSampleCount() == 1 }, time.Second, time.Millisecond)
	firstMS := flushQueueHistogram(t, w).GetSampleSum()
	second := channel.NewLocalResultChannel(t.Name() + "-second")
	_, err = w.WriteDataAsync(ctx, 1, []byte("second"), second)
	require.NoError(t, err)
	require.NoError(t, w.Sync(ctx))
	require.Len(t, w.flushTaskChan, 1)
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, firstMS, flushQueueHistogram(t, w).GetSampleSum(), "an executing task's timer must already have stopped")
	w.flushMu.Unlock()
	locked = false
	for _, ch := range []*channel.LocalResultChannel{first, second} {
		result, readErr := ch.ReadResult(ctx)
		require.NoError(t, readErr)
		require.NoError(t, result.Err)
	}
	h := flushQueueHistogram(t, w)
	require.EqualValues(t, 2, h.GetSampleCount())
	require.GreaterOrEqual(t, h.GetSampleSum()-firstMS, float64(20), "the second task must include waiting behind the first")
}

func TestFlushQueueWaitSkipsControlAndUnsubmittedTasks(t *testing.T) {
	w := &LocalFileWriter{logNs: t.Name(), logIdStr: "1"}
	w.processFlushTask(context.Background(), nil)
	w.processFlushTask(context.Background(), &blockFlushTask{queuedAt: time.Now().Add(-time.Hour)})
	w.processFlushTask(context.Background(), &blockFlushTask{entries: []*cache.BufferEntry{{EntryId: 0}}})
	require.Zero(t, flushQueueHistogram(t, w).GetSampleCount())
}

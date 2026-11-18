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

package segment

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

// BenchmarkGroupCommit measures single-log append throughput through the real
// segmentAppender + SequentialExecutor path with an injected per-round-trip
// latency, isolating the client send-gate that client-side group commit
// amortizes. It does NOT model the server-side flush/object-storage ceiling, so
// the numbers are an upper bound on the send-path speedup; real-cluster
// throughput is additionally bounded by the durable-write rate.
//
// Run: go test ./woodpecker/segment/ -run '^$' -bench BenchmarkGroupCommit -benchtime=2s
func BenchmarkGroupCommit(b *testing.B) {
	_ = logger.SetLevel("warn")      // keep per-op debug logging out of the measured path
	const rtt = 1 * time.Millisecond // representative per-entry buffered round-trip
	for _, k := range []int{1, 4, 8, 16, 32, 64, 128, 256} {
		b.Run(fmt.Sprintf("rtt=1ms/maxBatch=%d", k), func(b *testing.B) {
			benchGroupCommit(b, k, rtt)
		})
	}
}

func benchGroupCommit(b *testing.B, maxBatchEntries int, rtt time.Duration) {
	mockPool := mocks_logstore_client.NewLogStoreClientPool(b)
	mockClient := mocks_logstore_client.NewLogStoreClient(b)

	quorumInfo := &proto.QuorumInfo{Id: 1, Wq: 1, Aq: 1, Es: 1, Nodes: []string{"node1"}}

	deliver := func(ch channel.ResultChannel, entryId int64) {
		go func() { _ = ch.SendResult(context.Background(), &channel.AppendResult{SyncedId: entryId}) }()
	}

	mockPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil)
	mockClient.EXPECT().IsRemoteClient().Return(true).Maybe()
	// Single-entry path: one round-trip per entry (the serial gate).
	mockClient.EXPECT().
		AppendEntry(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, _ string, _ int64, e *proto.LogEntry, ch channel.ResultChannel) (int64, error) {
			time.Sleep(rtt)
			deliver(ch, e.EntryId)
			return e.EntryId, nil
		}).Maybe()
	// Batched path: one round-trip per batch, regardless of entry count.
	mockClient.EXPECT().
		AppendEntries(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, _ string, _ int64, entries []*proto.LogEntry, chs []channel.ResultChannel) ([]int64, error) {
			time.Sleep(rtt)
			ids := make([]int64, len(entries))
			for i, e := range entries {
				ids[i] = e.EntryId
				deliver(chs[i], e.EntryId)
			}
			return ids, nil
		}).Maybe()

	var wg sync.WaitGroup

	ctx := context.Background()
	ops := make([]*AppendOp, b.N)
	for i := range ops {
		ops[i] = NewAppendOp("bk", "fp", 1, 2, int64(i), []byte("v"),
			func(int64, int64, error) { wg.Done() }, quorumInfo, nil)
	}
	cfg := testAppenderConfiguration(config.SegmentAppendConfig{QueueSize: b.N + 1, MaxRetries: 1}, true)
	g := newSegmentAppender(mockPool, cfg, 1, &proto.SegmentMetadata{SegNo: 2, LastEntryId: -1, Quorum: quorumInfo}, segmentAppendCallbacks{})
	g.executor.Stop(context.Background())
	g.executor = NewBatchingSequentialExecutor(b.N+1, maxBatchEntries, 0, g.dispatch)
	defer func() { g.stop(); g.wait() }()
	wg.Add(b.N)

	b.ResetTimer()
	for _, op := range ops {
		reservation, err := g.reserve(ctx, op.Size())
		if err != nil {
			b.Fatal(err)
		}
		if !submitTestAppend(g, ctx, op, reservation) {
			b.Fatal("submit failed")
		}
	}
	g.executor.Start(ctx)
	wg.Wait()
	b.StopTimer()

	secs := b.Elapsed().Seconds()
	if secs > 0 {
		b.ReportMetric(float64(b.N)/secs, "appends/s")
	}
}

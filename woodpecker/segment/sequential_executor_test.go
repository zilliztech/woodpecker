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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func executorOp(id int64) *AppendOp { return &AppendOp{entryId: id, value: []byte("data")} }

func TestSequentialExecutorOrderAndBatching(t *testing.T) {
	for _, maxBatch := range []int{1, 3} {
		var batches [][]int64
		e := NewBatchingSequentialExecutor(8, maxBatch, 0, func(b *AppendBatch) {
			ids := []int64{}
			for _, op := range b.ops {
				ids = append(ids, op.entryId)
			}
			batches = append(batches, ids)
		})
		for i := range 8 {
			require.True(t, e.Submit(context.Background(), executorOp(int64(i))))
		}
		e.Start(context.Background())
		e.Stop(context.Background())
		var ids []int64
		for _, batch := range batches {
			require.LessOrEqual(t, len(batch), maxBatch)
			ids = append(ids, batch...)
		}
		require.Equal(t, []int64{0, 1, 2, 3, 4, 5, 6, 7}, ids)
		require.False(t, e.Submit(context.Background(), executorOp(9)))
	}
}

func TestSequentialExecutorCapacityCancellationAndStop(t *testing.T) {
	e := NewSequentialExecutor(1, func(*AppendBatch) {})
	require.True(t, e.Submit(context.Background(), executorOp(0)))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.False(t, e.Submit(ctx, executorOp(1)))
	done := make(chan bool, 1)
	go func() { done <- e.Submit(context.Background(), executorOp(2)) }()
	e.Stop(context.Background()) // starts a worker to drain previously accepted work
	require.False(t, receive(t, done))
	e.Stop(context.Background())
}

func TestSequentialExecutorConcurrentSubmitAndStop(t *testing.T) {
	for range 20 {
		var mu sync.Mutex
		accepted, dispatched := map[int64]bool{}, map[int64]bool{}
		e := NewSequentialExecutor(2, func(b *AppendBatch) {
			mu.Lock()
			defer mu.Unlock()
			for _, op := range b.ops {
				require.False(t, dispatched[op.entryId])
				dispatched[op.entryId] = true
			}
		})
		e.Start(context.Background())
		var wg sync.WaitGroup
		for i := range 20 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if e.Submit(context.Background(), executorOp(int64(i))) {
					mu.Lock()
					accepted[int64(i)] = true
					mu.Unlock()
				}
			}()
		}
		e.Stop(context.Background())
		wg.Wait()
		mu.Lock()
		require.Equal(t, accepted, dispatched)
		mu.Unlock()
	}
}

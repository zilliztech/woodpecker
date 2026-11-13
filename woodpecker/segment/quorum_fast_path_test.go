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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

type quorumFastClient struct {
	client.LogStoreClient
	fence    func(context.Context) (int64, error)
	complete func(context.Context, int64) (int64, error)
}

func (c *quorumFastClient) FenceSegment(ctx context.Context, _, _ string, _, _ int64) (int64, error) {
	return c.fence(ctx)
}

func (c *quorumFastClient) CompleteSegment(ctx context.Context, _, _ string, _, _, lac int64) (int64, error) {
	return c.complete(ctx, lac)
}

type quorumFastPool struct {
	client.LogStoreClientPool
	nodes map[string]*quorumFastClient
}

func (p *quorumFastPool) GetLogStoreClient(_ context.Context, node string) (client.LogStoreClient, error) {
	return p.nodes[node], nil
}

func newQuorumFastHandle(clients []*quorumFastClient, mode string) (*segmentHandleImpl, *proto.QuorumInfo) {
	pool := &quorumFastPool{nodes: map[string]*quorumFastClient{}}
	q := &proto.QuorumInfo{Es: int32(len(clients)), Wq: int32(len(clients)), Aq: int32(len(clients)/2 + 1)}
	for i, c := range clients {
		node := fmt.Sprintf("node%d", i)
		q.Nodes = append(q.Nodes, node)
		pool.nodes[node] = c
	}
	cfg := &config.Configuration{}
	cfg.Woodpecker.Storage.Type = mode
	h := &segmentHandleImpl{ClientPool: pool, cfg: cfg}
	h.lastAddConfirmed.Store(-1)
	return h, q
}

func waitQuorumWorker(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("surplus RPC did not exit on cancellation")
	}
}

func TestFenceQuorumEqualTailsCancelStraggler(t *testing.T) {
	for _, tail := range []int64{-1, 0, 100} {
		for slow := 0; slow < 3; slow++ {
			t.Run(fmt.Sprintf("tail_%d/slow_%d", tail, slow), func(t *testing.T) {
				entered, exited := make(chan struct{}), make(chan struct{})
				clients := make([]*quorumFastClient, 3)
				for i := range clients {
					if i == slow {
						clients[i] = &quorumFastClient{fence: func(ctx context.Context) (int64, error) {
							close(entered)
							defer close(exited)
							<-ctx.Done()
							return -1, ctx.Err()
						}}
					} else {
						clients[i] = &quorumFastClient{fence: func(context.Context) (int64, error) { <-entered; return tail, nil }}
					}
				}
				h, q := newQuorumFastHandle(clients, "service")
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				lac, err := h.fenceSegmentQuorum(ctx, q)
				require.NoError(t, err)
				require.Equal(t, tail, lac)
				require.NoError(t, ctx.Err(), "must not consume or cancel the caller budget")
				waitQuorumWorker(t, exited)
			})
		}
	}
}

func TestFenceQuorumUnequalTailsWaitForThirdResult(t *testing.T) {
	for _, tc := range []struct {
		name          string
		a, b, c, want int64
		err           error
	}{
		{"third lowers target", 90, 100, 90, 90, nil},
		{"third is higher", 90, 100, 110, 100, nil},
		{"missing data replica preserves acknowledged entry", -1, 0, -1, 0, errors.New("unreachable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{}, 1)
			defer close(release)
			var fast sync.WaitGroup
			fast.Add(2)
			clients := []*quorumFastClient{
				{fence: func(ctx context.Context) (int64, error) {
					select {
					case <-release:
						return tc.c, tc.err
					case <-ctx.Done():
						return -1, ctx.Err()
					}
				}},
				{fence: func(context.Context) (int64, error) { defer fast.Done(); return tc.a, nil }},
				{fence: func(context.Context) (int64, error) { defer fast.Done(); return tc.b, nil }},
			}
			h, q := newQuorumFastHandle(clients, "service")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			type result struct {
				lac int64
				err error
			}
			done := make(chan result, 1)
			go func() { lac, err := h.fenceSegmentQuorum(ctx, q); done <- result{lac, err} }()
			fast.Wait()
			select {
			case got := <-done:
				t.Fatalf("unequal replies returned before third result: %+v", got)
			case <-time.After(20 * time.Millisecond):
			}
			release <- struct{}{}
			got := <-done
			require.NoError(t, got.err)
			require.Equal(t, tc.want, got.lac)
		})
	}
}

func TestFenceQuorumFallbackHonorsCallerDeadline(t *testing.T) {
	for _, tc := range []struct {
		name, mode string
		tails      []int64
	}{
		{"unequal service tails", "service", []int64{90, 100}},
		{"embedded unchanged", "embed", []int64{100, 100}},
		{"five replica quorum unchanged", "service", []int64{100, 100, 100, 100}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exited := make(chan struct{})
			clients := []*quorumFastClient{{fence: func(ctx context.Context) (int64, error) { defer close(exited); <-ctx.Done(); return -1, ctx.Err() }}}
			for _, tail := range tc.tails {
				clients = append(clients, &quorumFastClient{fence: func(context.Context) (int64, error) { return tail, nil }})
			}
			h, q := newQuorumFastHandle(clients, tc.mode)
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			_, err := h.fenceSegmentQuorum(ctx, q)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			waitQuorumWorker(t, exited)
		})
	}
}

func TestCompleteQuorumDoesNotWaitForEqualLocalTails(t *testing.T) {
	for _, tails := range [][]int64{{100, 100}, {100, 90}, {90, 80}} {
		t.Run(fmt.Sprint(tails), func(t *testing.T) {
			entered, exited := make(chan struct{}), make(chan struct{})
			clients := []*quorumFastClient{{complete: func(ctx context.Context, _ int64) (int64, error) {
				close(entered)
				defer close(exited)
				<-ctx.Done()
				return -1, ctx.Err()
			}}}
			targets := make(chan int64, 2)
			for _, tail := range tails {
				clients = append(clients, &quorumFastClient{complete: func(_ context.Context, lac int64) (int64, error) { <-entered; targets <- lac; return tail, nil }})
			}
			h, q := newQuorumFastHandle(clients, "service")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.NoError(t, h.completeSegmentQuorum(ctx, q, 100))
			require.NoError(t, ctx.Err())
			require.EqualValues(t, 100, h.lastAddConfirmed.Load())
			require.EqualValues(t, 100, <-targets)
			require.EqualValues(t, 100, <-targets)
			waitQuorumWorker(t, exited)
		})
	}
}

func TestCompleteQuorumStillRequiresEnoughSuccessfulBoundaries(t *testing.T) {
	for _, mode := range []string{"service", "embed"} {
		t.Run(mode, func(t *testing.T) {
			exited := make(chan struct{})
			clients := []*quorumFastClient{
				{complete: func(ctx context.Context, _ int64) (int64, error) {
					defer close(exited)
					<-ctx.Done()
					return -1, ctx.Err()
				}},
				{complete: func(context.Context, int64) (int64, error) { return 100, nil }},
				{complete: func(context.Context, int64) (int64, error) {
					if mode == "embed" {
						return 100, nil
					}
					return -1, errors.New("failed")
				}},
			}
			h, q := newQuorumFastHandle(clients, mode)
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			require.ErrorIs(t, h.completeSegmentQuorum(ctx, q, 100), context.DeadlineExceeded)
			require.EqualValues(t, -1, h.lastAddConfirmed.Load())
			waitQuorumWorker(t, exited)
		})
	}
	// Exhausting all responses without Aq successes also remains a quorum error.
	clients := []*quorumFastClient{}
	for i := 0; i < 3; i++ {
		clients = append(clients, &quorumFastClient{complete: func(context.Context, int64) (int64, error) { return -1, errors.New("failed") }})
	}
	h, q := newQuorumFastHandle(clients, "service")
	require.True(t, werr.ErrAppendOpQuorumFailed.Is(h.completeSegmentQuorum(context.Background(), q, 100)))
}

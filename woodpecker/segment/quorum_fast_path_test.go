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
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage/stagedstorage"
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
	h.finalLAC.Store(-1)
	return h, q
}

func waitQuorumWorker(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recovery RPC did not finish")
	}
}

func TestFenceQuorumEqualTailsLetSlowReplicaFinish(t *testing.T) {
	for _, tail := range []int64{-1, 0, 100} {
		for slow := 0; slow < 3; slow++ {
			t.Run(fmt.Sprintf("tail_%d/slow_%d", tail, slow), func(t *testing.T) {
				entered, exited := make(chan struct{}), make(chan struct{})
				release := make(chan struct{})
				finished := make(chan error, 1)
				clients := make([]*quorumFastClient, 3)
				for i := range clients {
					if i == slow {
						clients[i] = &quorumFastClient{fence: func(ctx context.Context) (int64, error) {
							close(entered)
							defer close(exited)
							select {
							case <-release:
							case <-ctx.Done():
							}
							finished <- ctx.Err()
							return tail + 1, ctx.Err()
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
				cancel()
				close(release)
				waitQuorumWorker(t, exited)
				require.NoError(t, <-finished)
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
		{"single embedded replica is silent", "local", nil},
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

func TestCompleteQuorumWithCoveringReplicaLetsSlowReplicaFinish(t *testing.T) {
	for _, tails := range [][]int64{{100, 100}, {100, 90}, {90, 101}} {
		t.Run(fmt.Sprint(tails), func(t *testing.T) {
			entered, exited := make(chan struct{}), make(chan struct{})
			release := make(chan struct{})
			finished := make(chan error, 1)
			clients := []*quorumFastClient{{complete: func(ctx context.Context, _ int64) (int64, error) {
				close(entered)
				defer close(exited)
				select {
				case <-release:
				case <-ctx.Done():
				}
				finished <- ctx.Err()
				return 100, ctx.Err()
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
			require.EqualValues(t, 100, h.confirmedLAC())
			require.EqualValues(t, 100, <-targets)
			require.EqualValues(t, 100, <-targets)
			cancel()
			close(release)
			waitQuorumWorker(t, exited)
			require.NoError(t, <-finished)
		})
	}
}

func TestCompleteQuorumWaitsForOnlyReplicaThatCanCompact(t *testing.T) {
	ctx := t.Context()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	store := mocks_objectstorage.NewObjectStorage(t)
	missing := errors.New("remote footer missing")
	store.EXPECT().StatObject(mock.Anything, "bucket", "root/1/0/footer.blk", mock.Anything, mock.Anything).Return(0, false, missing)
	store.EXPECT().IsObjectNotExistsError(missing).Return(true)
	store.EXPECT().PutObject(mock.Anything, "bucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	covering, err := stagedstorage.NewStagedFileWriter(ctx, "bucket", "root", t.TempDir(), 1, 0, store, cfg)
	require.NoError(t, err)
	defer covering.Close(ctx)
	for id := int64(0); id <= 1; id++ {
		_, err := covering.WriteDataAsync(ctx, id, []byte("entry"), nil)
		require.NoError(t, err)
	}

	// Only A holds entry 1. C misses Fence, then rejoins B to finalize at
	// target 1. Both report their actual local tail, which is still 0.
	// Flush real staged I/O before the virtual-time quorum scenario.
	coveringTail, err := covering.Fence(ctx)
	require.NoError(t, err)
	partial := func(context.Context, int64) (int64, error) { return 0, nil }
	clients := []*quorumFastClient{
		{fence: func(context.Context) (int64, error) { return coveringTail, nil }, complete: func(ctx context.Context, lac int64) (int64, error) {
			select {
			case <-ctx.Done():
				return -1, ctx.Err()
			case <-time.After(time.Second):
				return covering.Finalize(ctx, lac)
			}
		}},
		{fence: func(context.Context) (int64, error) { return 0, nil }, complete: partial},
		{fence: func(context.Context) (int64, error) { return -1, errors.New("fence unavailable") }, complete: partial},
	}
	h, q := newQuorumFastHandle(clients, "service")
	var lac int64
	synctest.Test(t, func(t *testing.T) {
		lac, err = h.fenceSegmentQuorum(t.Context(), q)
		require.NoError(t, err)
		require.EqualValues(t, 1, lac)
		// Virtual time advances to A's reply after the fast replies have arrived.
		require.NoError(t, h.completeSegmentQuorum(t.Context(), q, lac))
	})
	size, err := covering.Compact(ctx, lac)
	require.NoError(t, err)
	require.Positive(t, size)
}

func TestCompleteQuorumWithoutCoverageWaitsForLastOutcome(t *testing.T) {
	for _, tc := range []struct {
		name       string
		replyAfter time.Duration
		wantErr    error
	}{
		{"last replica returns an error", time.Second, nil},
		{"caller deadline expires first", time.Minute, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				partial := func(context.Context, int64) (int64, error) { return 90, nil }
				h, q := newQuorumFastHandle([]*quorumFastClient{
					{complete: partial},
					{complete: partial},
					{complete: func(ctx context.Context, _ int64) (int64, error) {
						select {
						case <-time.After(tc.replyAfter):
							return -1, errors.New("replica unavailable")
						case <-ctx.Done():
							return -1, ctx.Err()
						}
					}},
				}, "service")
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
				require.ErrorIs(t, h.completeSegmentQuorum(ctx, q, 100), tc.wantErr)
			})
		})
	}
}

func TestCompleteQuorumStillRequiresEnoughSuccessfulBoundaries(t *testing.T) {
	t.Run("one finalized replica and one failure", func(t *testing.T) {
		exited := make(chan struct{})
		clients := []*quorumFastClient{
			{complete: func(ctx context.Context, _ int64) (int64, error) {
				defer close(exited)
				<-ctx.Done()
				return -1, ctx.Err()
			}},
			{complete: func(context.Context, int64) (int64, error) { return 100, nil }},
			{complete: func(context.Context, int64) (int64, error) { return -1, errors.New("failed") }},
		}
		h, q := newQuorumFastHandle(clients, "service")
		ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, h.completeSegmentQuorum(ctx, q, 100), context.DeadlineExceeded)
		require.EqualValues(t, -1, h.confirmedLAC())
		waitQuorumWorker(t, exited)
	})
	// Exhausting all responses without Aq successes also remains a quorum error.
	clients := []*quorumFastClient{}
	for i := 0; i < 3; i++ {
		clients = append(clients, &quorumFastClient{complete: func(context.Context, int64) (int64, error) { return -1, errors.New("failed") }})
	}
	h, q := newQuorumFastHandle(clients, "service")
	require.True(t, werr.ErrAppendOpQuorumFailed.Is(h.completeSegmentQuorum(context.Background(), q, 100)))
}

func TestCompleteQuorumSingleEmbeddedReplica(t *testing.T) {
	const target = int64(100)
	for _, storage := range []string{"local", "minio"} {
		for _, tc := range []struct {
			name string
			tail int64
			err  error
		}{
			{"covers target", target, nil},
			{"finalized below target", target - 1, nil},
			{"finalize failed", -1, errors.New("finalize failed")},
		} {
			t.Run(storage+"/"+tc.name, func(t *testing.T) {
				replica := &quorumFastClient{complete: func(context.Context, int64) (int64, error) {
					return tc.tail, tc.err
				}}
				h, q := newQuorumFastHandle([]*quorumFastClient{replica}, storage)
				err := h.completeSegmentQuorum(t.Context(), q, target)
				if tc.err != nil {
					require.True(t, werr.ErrAppendOpQuorumFailed.Is(err))
					require.EqualValues(t, -1, h.confirmedLAC())
					return
				}
				require.NoError(t, err)
				require.Equal(t, target, h.confirmedLAC())
			})
		}
	}
}

func TestRecoverySlowReplicaFencesBeforeCompleting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const tail int64 = 100
		releaseFence := make(chan struct{})
		completed := make(chan error, 1)
		healthy := &quorumFastClient{
			fence:    func(context.Context) (int64, error) { return tail, nil },
			complete: func(context.Context, int64) (int64, error) { return tail, nil },
		}
		slow := &quorumFastClient{
			fence: func(ctx context.Context) (int64, error) {
				select {
				case <-releaseFence:
					return tail, nil
				case <-ctx.Done():
					return -1, ctx.Err()
				}
			},
			complete: func(ctx context.Context, lac int64) (int64, error) {
				if lac != tail {
					t.Errorf("slow replica received target %d, want %d", lac, tail)
				}
				completed <- ctx.Err()
				return lac, ctx.Err()
			},
		}
		h, q := newQuorumFastHandle([]*quorumFastClient{slow, healthy, healthy}, "service")
		h.quorumInfo = q
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		lac, err := h.doComplete(ctx)
		require.NoError(t, err)
		require.Equal(t, tail, lac)
		cancel() // OpenWriter's caller is done.
		synctest.Wait()
		require.Empty(t, completed, "the slow replica still has an unfinished Fence")
		close(releaseFence)
		synctest.Wait()
		require.Len(t, completed, 1, "the same replica must now complete at the chosen target")
		require.NoError(t, <-completed)
	})
}

func TestRecoverySkipsExpiredCompleteQueuedBehindFence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const target = int64(100)
		releaseFence := make(chan struct{})
		slowCompletes := 0
		slow := &quorumFastClient{
			fence: func(context.Context) (int64, error) {
				<-releaseFence
				return target, nil
			},
			complete: func(context.Context, int64) (int64, error) {
				slowCompletes++
				return target, nil
			},
		}
		healthy := &quorumFastClient{
			fence:    func(context.Context) (int64, error) { return target, nil },
			complete: func(context.Context, int64) (int64, error) { return target - 1, nil },
		}
		h, q := newQuorumFastHandle([]*quorumFastClient{slow, healthy, healthy}, "service")
		_, err := h.fenceSegmentQuorum(t.Context(), q)
		require.NoError(t, err)

		// Only the slow replica covers the target. Its Complete is still queued when the caller times out.
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		require.ErrorIs(t, h.completeSegmentQuorum(ctx, q, target), context.DeadlineExceeded)
		close(releaseFence)
		synctest.Wait()
		require.Zero(t, slowCompletes)
		require.Empty(t, h.recovery.queues)

		require.NoError(t, h.completeSegmentQuorum(t.Context(), q, target))
		synctest.Wait()
		require.Equal(t, 1, slowCompletes)
	})
}

func TestRecoveryBackgroundTimeoutStartsAfterQuorum(t *testing.T) {
	for _, phase := range []string{"fence", "complete"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				stopped := make(chan error, 1)
				hang := func(ctx context.Context) (int64, error) {
					<-ctx.Done()
					stopped <- ctx.Err()
					return -1, ctx.Err()
				}
				// Quorum itself takes longer than the background grace period.
				reply := func(context.Context) (int64, error) {
					time.Sleep(2 * backgroundRecoveryTimeout)
					return 100, nil
				}
				slow := &quorumFastClient{fence: hang, complete: func(ctx context.Context, _ int64) (int64, error) {
					return hang(ctx)
				}}
				healthy := &quorumFastClient{fence: reply, complete: func(ctx context.Context, _ int64) (int64, error) {
					return reply(ctx)
				}}
				h, q := newQuorumFastHandle([]*quorumFastClient{healthy, slow, healthy}, "service")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if phase == "fence" {
					_, err := h.fenceSegmentQuorum(ctx, q)
					require.NoError(t, err)
				} else {
					require.NoError(t, h.completeSegmentQuorum(ctx, q, 100))
				}
				cancel()
				time.Sleep(backgroundRecoveryTimeout - time.Millisecond)
				synctest.Wait()
				require.Empty(t, stopped)
				time.Sleep(time.Millisecond)
				synctest.Wait()
				require.Len(t, stopped, 1)
				require.ErrorIs(t, <-stopped, context.Canceled)
			})
		})
	}
}

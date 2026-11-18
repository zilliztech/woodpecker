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
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
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

func captureQuorumLogs(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zap.InfoLevel)
	t.Cleanup(logger.ReplaceGlobals(zap.New(core)))
	return logs
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
				lac, err := h.fenceSegmentQuorum(ctx, q, returnOnSafeQuorum)
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
			go func() { lac, err := h.fenceSegmentQuorum(ctx, q, returnOnSafeQuorum); done <- result{lac, err} }()
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

func TestFenceQuorumFiveReplicas(t *testing.T) {
	for _, tc := range []struct {
		name        string
		tails       [5]int64
		firstErr    error
		wait        quorumWaitPolicy
		wantReplies int
		wantLAC     int64
	}{
		{"three matching replies", [5]int64{100, 100, 100, 90, 110}, nil, returnOnSafeQuorum, 3, 100},
		{"fourth reply forms a majority", [5]int64{90, 100, 100, 100, 110}, nil, returnOnSafeQuorum, 4, 100},
		{"split replies need the final tail", [5]int64{90, 100, 110, 90, 80}, nil, returnOnSafeQuorum, 5, 90},
		{"failed reply cannot vote for empty tail", [5]int64{-1, -1, -1, 10, 20}, errors.New("unreachable"), returnOnSafeQuorum, 5, 10},
		{"rolling waits for every reply", [5]int64{100, 100, 100, 90, 110}, nil, waitForAllReplicas, 5, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				finished := make(chan error, len(tc.tails))
				clients := make([]*quorumFastClient, len(tc.tails))
				for i, tail := range tc.tails {
					clients[i] = &quorumFastClient{fence: func(ctx context.Context) (int64, error) {
						// Deliver one reply per second of virtual time, in node order.
						select {
						case <-time.After(time.Duration(i+1) * time.Second):
						case <-ctx.Done():
						}
						finished <- ctx.Err()
						if i == 0 && tc.firstErr != nil {
							return -1, tc.firstErr
						}
						return tail, ctx.Err()
					}}
				}
				h, q := newQuorumFastHandle(clients, "service")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				start := time.Now()
				lac, err := h.fenceSegmentQuorum(ctx, q, tc.wait)
				require.NoError(t, err)
				require.Equal(t, tc.wantLAC, lac)
				require.Equal(t, time.Duration(tc.wantReplies)*time.Second, time.Since(start))
				cancel()
				for range clients {
					require.NoError(t, <-finished, "remaining replicas must finish after foreground return")
				}
			})
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
		{"five replicas split two against two", "service", []int64{90, 90, 100, 100}},
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
			_, err := h.fenceSegmentQuorum(ctx, q, returnOnSafeQuorum)
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
			require.NoError(t, h.completeSegmentQuorum(ctx, q, 100, returnOnSafeQuorum))
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

func TestCompleteQuorumMixedTailsCanCompactEmptyTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	store := mocks_objectstorage.NewObjectStorage(t)
	const footer = "root/1/0/footer.blk"
	missing := errors.New("remote footer missing")
	store.EXPECT().StatObject(mock.Anything, "bucket", footer, mock.Anything, mock.Anything).Return(0, false, missing)
	store.EXPECT().IsObjectNotExistsError(missing).Return(true)
	store.EXPECT().PutObject(mock.Anything, "bucket", footer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	empty, err := stagedstorage.NewStagedFileWriter(ctx, "bucket", "root", t.TempDir(), 1, 0, store, cfg)
	require.NoError(t, err)
	defer empty.Close(t.Context())
	nonEmpty, err := stagedstorage.NewStagedFileWriter(ctx, "bucket", "root", t.TempDir(), 1, 0, store, cfg)
	require.NoError(t, err)
	defer nonEmpty.Close(t.Context())
	_, err = nonEmpty.WriteDataAsync(ctx, 0, []byte("entry 0"), nil)
	require.NoError(t, err)

	// A and B fence with empty tails. Hold B's Complete so only A and C can finalize.
	allowFenceC := make(chan struct{})
	allowCompleteB := make(chan struct{})
	defer close(allowCompleteB)
	h, q := newQuorumFastHandle([]*quorumFastClient{
		{fence: empty.Fence, complete: empty.Finalize},
		{
			fence: func(context.Context) (int64, error) { return -1, nil },
			complete: func(context.Context, int64) (int64, error) {
				<-allowCompleteB
				return -1, nil
			},
		},
		{
			fence: func(ctx context.Context) (int64, error) {
				select {
				case <-allowFenceC:
					return nonEmpty.Fence(ctx)
				case <-ctx.Done():
					return -1, ctx.Err()
				}
			},
			complete: nonEmpty.Finalize,
		},
	}, "service")
	lac, err := h.fenceSegmentQuorum(ctx, q, returnOnSafeQuorum)
	require.NoError(t, err)
	require.EqualValues(t, -1, lac)
	close(allowFenceC)
	require.NoError(t, h.completeSegmentQuorum(ctx, q, lac, returnOnSafeQuorum))

	// Check C before A publishes the shared empty footer.
	_, err = nonEmpty.Compact(ctx, lac)
	require.True(t, werr.ErrInvalidLACAlignment.Is(err), "non-empty replica compact returned %v", err)
	size, err := empty.Compact(ctx, lac)
	require.NoError(t, err)
	require.Positive(t, size)
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
		lac, err = h.fenceSegmentQuorum(t.Context(), q, returnOnSafeQuorum)
		require.NoError(t, err)
		require.EqualValues(t, 1, lac)
		// Virtual time advances to A's reply after the fast replies have arrived.
		require.NoError(t, h.completeSegmentQuorum(t.Context(), q, lac, returnOnSafeQuorum))
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
				require.ErrorIs(t, h.completeSegmentQuorum(ctx, q, 100, returnOnSafeQuorum), tc.wantErr)
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
		require.ErrorIs(t, h.completeSegmentQuorum(ctx, q, 100, returnOnSafeQuorum), context.DeadlineExceeded)
		require.EqualValues(t, -1, h.confirmedLAC())
		waitQuorumWorker(t, exited)
	})
	// Exhausting all responses without Aq successes also remains a quorum error.
	clients := []*quorumFastClient{}
	for i := 0; i < 3; i++ {
		clients = append(clients, &quorumFastClient{complete: func(context.Context, int64) (int64, error) { return -1, errors.New("failed") }})
	}
	h, q := newQuorumFastHandle(clients, "service")
	require.True(t, werr.ErrAppendOpQuorumFailed.Is(h.completeSegmentQuorum(context.Background(), q, 100, returnOnSafeQuorum)))
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
				err := h.completeSegmentQuorum(t.Context(), q, target, returnOnSafeQuorum)
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
	logs := captureQuorumLogs(t)
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
		md := mocks_meta.NewMetadataProvider(t)
		h.metadata = md
		h.segmentMetaCache.Store(&meta.SegmentMeta{Metadata: &proto.SegmentMetadata{State: proto.SegmentState_Active, Quorum: q}})
		md.EXPECT().UpdateSegmentMetadata(mock.Anything, h.logName, h.logId, mock.Anything, proto.SegmentState_Active).Return(nil).Once()
		md.EXPECT().GetSegmentMetadata(mock.Anything, h.logName, h.segmentId).Return(
			&meta.SegmentMeta{Metadata: &proto.SegmentMetadata{State: proto.SegmentState_Completed, LastEntryId: tail, Quorum: q}}, nil).Once()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		lac, err := h.FenceAndComplete(ctx)
		require.NoError(t, err)
		require.Equal(t, tail, lac)
		cancel() // OpenWriter's caller is done.
		synctest.Wait()
		require.Empty(t, completed, "the slow replica still has an unfinished Fence")
		require.Equal(t, 4, logs.FilterMessage("Segment replica RPC succeeded").Len())
		for _, message := range []string{"Calculated LAC from quorum fence responses", "Quorum complete responses collected"} {
			decision := logs.FilterMessage(message).All()
			require.Len(t, decision, 1)
			require.EqualValues(t, 2, decision[0].ContextMap()["receivedCount"])
			require.EqualValues(t, 3, decision[0].ContextMap()["nodeCount"])
		}
		time.Sleep(50 * time.Millisecond)
		close(releaseFence)
		synctest.Wait()
		require.Len(t, completed, 1, "the same replica must now complete at the chosen target")
		require.NoError(t, <-completed)
		results := logs.FilterMessage("Segment replica RPC succeeded")
		require.Equal(t, 6, results.Len())
		late := results.FilterField(zap.String("node", q.Nodes[0])).All()
		require.Len(t, late, 2)
		require.Equal(t, "fence", late[0].ContextMap()["operation"])
		require.Equal(t, "complete", late[1].ContextMap()["operation"])
		require.Equal(t, tail, late[1].ContextMap()["targetLAC"])
		for _, entry := range late {
			require.Equal(t, zap.InfoLevel, entry.Level)
			require.Equal(t, tail, entry.ContextMap()["lastEntryId"])
			require.Equal(t, 50*time.Millisecond, entry.ContextMap()["duration"])
		}
	})
}

func TestRecoverySkipsExpiredCompleteQueuedBehindFence(t *testing.T) {
	logs := captureQuorumLogs(t)
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
		_, err := h.fenceSegmentQuorum(t.Context(), q, returnOnSafeQuorum)
		require.NoError(t, err)

		synctest.Wait()
		pool := h.quorumRPCPool
		require.NotNil(t, pool)
		require.Equal(t, len(q.Nodes), pool.Cap())

		// Only the slow replica covers the target. Its Complete is still queued when the caller times out.
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		require.ErrorIs(t, h.completeSegmentQuorum(ctx, q, target, returnOnSafeQuorum), context.DeadlineExceeded)
		require.Same(t, pool, h.quorumRPCPool)
		close(releaseFence)
		synctest.Wait()
		require.Zero(t, slowCompletes)
		require.Empty(t, h.pendingReplicaTasks)
		require.Nil(t, h.quorumRPCPool)
		require.True(t, pool.IsClosed())
		failed := logs.FilterMessage("Segment replica RPC failed").All()
		require.Len(t, failed, 1)
		require.Equal(t, zap.WarnLevel, failed[0].Level)
		require.Equal(t, q.Nodes[0], failed[0].ContextMap()["node"])
		require.Equal(t, true, failed[0].ContextMap()["skippedInQueue"])
		require.Equal(t, context.DeadlineExceeded.Error(), failed[0].ContextMap()["cancelCause"])
		require.Equal(t, time.Second, failed[0].ContextMap()["duration"])
		require.Equal(t, 2, logs.FilterMessage("Replica finalized but does not locally cover target LAC").Len())

		require.NoError(t, h.completeSegmentQuorum(t.Context(), q, target, returnOnSafeQuorum))
		synctest.Wait()
		require.Equal(t, 1, slowCompletes)
	})
}

func TestRecoveryBackgroundTimeoutStartsAfterQuorum(t *testing.T) {
	for _, phase := range []string{"fence", "complete"} {
		t.Run(phase, func(t *testing.T) {
			logs := captureQuorumLogs(t)
			synctest.Test(t, func(t *testing.T) {
				stopped := make(chan error, 1)
				hang := func(ctx context.Context) (int64, error) {
					<-ctx.Done()
					stopped <- ctx.Err()
					return -1, ctx.Err()
				}
				// Quorum itself takes longer than the background grace period.
				reply := func(context.Context) (int64, error) {
					time.Sleep(2 * openWriterReplicaGracePeriod)
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
					_, err := h.fenceSegmentQuorum(ctx, q, returnOnSafeQuorum)
					require.NoError(t, err)
				} else {
					require.NoError(t, h.completeSegmentQuorum(ctx, q, 100, returnOnSafeQuorum))
				}
				cancel()
				time.Sleep(openWriterReplicaGracePeriod - time.Millisecond)
				synctest.Wait()
				require.Empty(t, stopped)
				time.Sleep(time.Millisecond)
				synctest.Wait()
				require.Len(t, stopped, 1)
				require.ErrorIs(t, <-stopped, context.Canceled)
				failed := logs.FilterMessage("Segment replica RPC failed").All()
				require.Len(t, failed, 1)
				require.Equal(t, zap.WarnLevel, failed[0].Level)
				fields := failed[0].ContextMap()
				require.Equal(t, phase, fields["operation"])
				require.Equal(t, q.Nodes[1], fields["node"])
				require.Equal(t, false, fields["skippedInQueue"])
				require.Equal(t, errReplicaGracePeriodExpired.Error(), fields["cancelCause"])
				require.Equal(t, 3*openWriterReplicaGracePeriod, fields["duration"])
			})
		})
	}
}

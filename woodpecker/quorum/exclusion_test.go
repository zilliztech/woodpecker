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

package quorum

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

func exclusionTestConfig(seeds ...string) *config.QuorumConfig {
	return &config.QuorumConfig{
		BufferPools: config.NewDynamic([]config.QuorumBufferPool{{Name: "region-a", Seeds: seeds}}),
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     config.NewDynamic("random"),
			AffinityMode: config.NewDynamic("soft"),
			Replicas:     config.NewDynamic(3),
		},
	}
}

func metas(endpoints ...string) []*proto.NodeMeta {
	out := make([]*proto.NodeMeta, len(endpoints))
	for i, e := range endpoints {
		out[i] = &proto.NodeMeta{Endpoint: e}
	}
	return out
}

// The excluded replicas travel to the seed in every filter, and a seed that is
// itself excluded is asked last.
func TestSelectQuorum_ExclusionReachesSeedAndExcludedSeedGoesLast(t *testing.T) {
	ctx := WithExcludedEndpoints(context.Background(), []string{"s1:1", "n9:1"})
	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	cli := mocks_logstore_client.NewLogStoreClient(t)
	// Only s2 is asked: s1 is excluded and goes last, and s2 answers.
	pool.EXPECT().GetLogStoreClient(mock.Anything, "s2:1").Return(cli, nil)
	cli.EXPECT().SelectNodes(mock.Anything, proto.StrategyType_RANDOM, proto.AffinityMode_SOFT, mock.Anything).
		RunAndReturn(func(_ context.Context, _ proto.StrategyType, _ proto.AffinityMode, filters []*proto.NodeFilter) ([]*proto.NodeMeta, error) {
			require.Len(t, filters, 1)
			assert.ElementsMatch(t, []string{"s1:1", "n9:1"}, filters[0].ExcludeEndpoints)
			return metas("a:1", "b:1", "c:1"), nil
		})

	d, err := NewQuorumDiscovery(ctx, exclusionTestConfig("s1:1", "s2:1"), pool)
	require.NoError(t, err)
	for i := 0; i < 10; i++ { // seed order is shuffled; s1 must never be asked first
		result, err := d.SelectQuorum(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"a:1", "b:1", "c:1"}, result.Nodes)
	}
}

// A seed that predates ExcludeEndpoints ignores it. The client drops the
// excluded nodes itself, and asks once more for enough extra nodes.
func TestSelectQuorum_OldSeedIgnoringExclusion(t *testing.T) {
	ctx := WithExcludedEndpoints(context.Background(), []string{"n9:1"})
	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	cli := mocks_logstore_client.NewLogStoreClient(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "s1:1").Return(cli, nil)
	var limits []int32
	cli.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ proto.StrategyType, _ proto.AffinityMode, filters []*proto.NodeFilter) ([]*proto.NodeMeta, error) {
			limits = append(limits, filters[0].Limit)
			if len(limits) == 1 {
				return metas("n9:1", "a:1", "b:1"), nil
			}
			return metas("a:1", "n9:1", "b:1", "c:1"), nil
		})

	d, err := NewQuorumDiscovery(ctx, exclusionTestConfig("s1:1"), pool)
	require.NoError(t, err)
	result, err := d.SelectQuorum(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a:1", "b:1", "c:1"}, result.Nodes)
	assert.Equal(t, []int32{3, 4}, limits, "the second request asks for one extra node per excluded endpoint")
}

// Too few nodes without the excluded ones: the exclusion is dropped rather
// than allowed to hold the log up.
func TestSelectQuorum_ExclusionDroppedWhenTooFewNodes(t *testing.T) {
	ctx := WithExcludedEndpoints(context.Background(), []string{"c:1"})
	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	cli := mocks_logstore_client.NewLogStoreClient(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "s1:1").Return(cli, nil)
	cli.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ proto.StrategyType, _ proto.AffinityMode, filters []*proto.NodeFilter) ([]*proto.NodeMeta, error) {
			if len(filters[0].ExcludeEndpoints) > 0 {
				return metas("a:1", "b:1"), nil // a three-node cluster, one excluded
			}
			return metas("a:1", "b:1", "c:1"), nil
		})

	d, err := NewQuorumDiscovery(ctx, exclusionTestConfig("s1:1"), pool)
	require.NoError(t, err)
	result, err := d.SelectQuorum(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a:1", "b:1", "c:1"}, result.Nodes)
}

// The excluded replica is also a seed and cannot be reached. It is asked last,
// after the healthy seeds found too few nodes without it; its connection error
// must not hide their answer, or the exclusion is never dropped and no segment
// can be created until it is back.
func TestSelectQuorum_UnreachableExcludedSeedDoesNotHideTooFewNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = WithExcludedEndpoints(ctx, []string{"c:1"})
	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	healthy := func(_ context.Context, _ proto.StrategyType, _ proto.AffinityMode, filters []*proto.NodeFilter) ([]*proto.NodeMeta, error) {
		if len(filters[0].ExcludeEndpoints) > 0 {
			return metas("a:1", "b:1"), nil // a three-node cluster, one excluded
		}
		return metas("a:1", "b:1", "c:1"), nil
	}
	for _, seed := range []string{"a:1", "b:1"} {
		cli := mocks_logstore_client.NewLogStoreClient(t)
		cli.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(healthy).Maybe()
		pool.EXPECT().GetLogStoreClient(mock.Anything, seed).Return(cli, nil).Maybe()
	}
	pool.EXPECT().GetLogStoreClient(mock.Anything, "c:1").Return(nil, errors.New("connection refused")).Maybe()

	d, err := NewQuorumDiscovery(ctx, exclusionTestConfig("a:1", "b:1", "c:1"), pool)
	require.NoError(t, err)
	result, err := d.SelectQuorum(ctx)
	require.NoError(t, err, "the selection must fall back to all nodes, not retry with the exclusion")
	assert.ElementsMatch(t, []string{"a:1", "b:1", "c:1"}, result.Nodes)
}

// A pool whose seeds all failed reports "too few nodes" if any seed said so,
// whichever seed was asked last.
func TestRequestNodesFromPool_PrefersInsufficientQuorum(t *testing.T) {
	ctx := context.Background()
	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	cli := mocks_logstore_client.NewLogStoreClient(t)
	cli.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(metas("a:1"), nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "a:1").Return(cli, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "c:1").Return(nil, errors.New("connection refused"))

	d, err := NewQuorumDiscovery(ctx, exclusionTestConfig("a:1", "c:1"), pool)
	require.NoError(t, err)
	filter := &proto.NodeFilter{Limit: 3, ExcludeEndpoints: []string{"c:1"}}
	_, err = d.(*quorumDiscovery).requestNodesFromPool(ctx, config.QuorumBufferPool{Name: "region-a", Seeds: []string{"a:1", "c:1"}}, filter, 3)
	require.Error(t, err)
	assert.True(t, errors.Is(err, werr.ErrServiceInsufficientQuorum), "%v", err)
}

// The cross-region strategy builds a filter per region and one to top the
// selection up; the exclusion must reach the seed through each of them.
func TestSelectQuorum_CrossRegionCarriesExclusion(t *testing.T) {
	ctx := WithExcludedEndpoints(context.Background(), []string{"x:1"})
	cfg := &config.QuorumConfig{
		BufferPools: config.NewDynamic([]config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"sa:1"}},
			{Name: "region-b", Seeds: []string{"sb:1"}},
		}),
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     config.NewDynamic("cross-region"),
			AffinityMode: config.NewDynamic("soft"),
			Replicas:     config.NewDynamic(3),
		},
	}
	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	cliA := mocks_logstore_client.NewLogStoreClient(t)
	cliB := mocks_logstore_client.NewLogStoreClient(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "sa:1").Return(cliA, nil)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "sb:1").Return(cliB, nil)
	var calls atomic.Int32
	answer := func(nodes ...string) func(context.Context, proto.StrategyType, proto.AffinityMode, []*proto.NodeFilter) ([]*proto.NodeMeta, error) {
		return func(_ context.Context, _ proto.StrategyType, _ proto.AffinityMode, filters []*proto.NodeFilter) ([]*proto.NodeMeta, error) {
			calls.Add(1)
			assert.Equal(t, []string{"x:1"}, filters[0].ExcludeEndpoints)
			return metas(nodes...), nil
		}
	}
	// Region a is asked for 2 and gives 2; region b is asked for 1 and has
	// none, so the selection is topped up, from region a first.
	cliA.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(answer("a1:1", "a2:1")).Once()
	cliB.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(answer()).Once()
	cliA.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(answer("a3:1")).Once()

	d, err := NewQuorumDiscovery(ctx, cfg, pool)
	require.NoError(t, err)
	result, err := d.SelectQuorum(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a1:1", "a2:1", "a3:1"}, result.Nodes)
	assert.Equal(t, int32(3), calls.Load(), "both region requests and the top-up request")
}

// The custom strategy builds a filter per placement rule; the exclusion must
// reach the seed through each of them.
func TestSelectQuorum_CustomPlacementCarriesExclusion(t *testing.T) {
	ctx := WithExcludedEndpoints(context.Background(), []string{"x:1"})
	cfg := &config.QuorumConfig{
		BufferPools: config.NewDynamic([]config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"sa:1"}},
			{Name: "region-b", Seeds: []string{"sb:1"}},
			{Name: "region-c", Seeds: []string{"sc:1"}},
		}),
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     config.NewDynamic("custom"),
			AffinityMode: config.NewDynamic("hard"),
			Replicas:     config.NewDynamic(3),
			CustomPlacement: config.NewDynamic([]config.CustomPlacement{
				{Region: "region-a", Az: "az-1", ResourceGroup: "rg-1"},
				{Region: "region-b", Az: "az-2", ResourceGroup: "rg-2"},
				{Region: "region-c", Az: "az-3", ResourceGroup: "rg-3"},
			}),
		},
	}
	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	for _, r := range []string{"a", "b", "c"} {
		cli := mocks_logstore_client.NewLogStoreClient(t)
		node := r + "1:1"
		cli.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, _ proto.StrategyType, _ proto.AffinityMode, filters []*proto.NodeFilter) ([]*proto.NodeMeta, error) {
				assert.Equal(t, []string{"x:1"}, filters[0].ExcludeEndpoints)
				return metas(node), nil
			}).Once()
		pool.EXPECT().GetLogStoreClient(mock.Anything, "s"+r+":1").Return(cli, nil)
	}

	d, err := NewQuorumDiscovery(ctx, cfg, pool)
	require.NoError(t, err)
	result, err := d.SelectQuorum(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a1:1", "b1:1", "c1:1"}, result.Nodes)
}

// Without an exclusion in the context nothing changes.
func TestSelectQuorum_NoExclusionByDefault(t *testing.T) {
	ctx := context.Background()
	pool := mocks_logstore_client.NewLogStoreClientPool(t)
	cli := mocks_logstore_client.NewLogStoreClient(t)
	pool.EXPECT().GetLogStoreClient(mock.Anything, "s1:1").Return(cli, nil)
	cli.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ proto.StrategyType, _ proto.AffinityMode, filters []*proto.NodeFilter) ([]*proto.NodeMeta, error) {
			assert.Empty(t, filters[0].ExcludeEndpoints)
			return metas("a:1", "b:1", "c:1"), nil
		})
	d, err := NewQuorumDiscovery(ctx, exclusionTestConfig("s1:1"), pool)
	require.NoError(t, err)
	_, err = d.SelectQuorum(ctx)
	require.NoError(t, err)
}

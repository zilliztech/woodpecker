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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
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

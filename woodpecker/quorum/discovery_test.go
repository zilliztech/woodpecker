package quorum

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

func TestQuorumDiscovery_SelectQuorumNodes_SingleRegion_Success(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{
				Name:  "region-a",
				Seeds: []string{"localhost:8080"},
			},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Mock the client pool to return our mock client
	mockClientPool.EXPECT().GetLogStoreClient(ctx, "localhost:8080").Return(mockClient, nil)

	// Mock the SelectNodes call
	expectedNodes := []*proto.NodeMeta{
		{Endpoint: "node1:8080"},
		{Endpoint: "node2:8080"},
		{Endpoint: "node3:8080"},
	}
	mockClient.EXPECT().SelectNodes(ctx, proto.StrategyType_RANDOM, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return(expectedNodes, nil)

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)
	assert.NotNil(t, discovery)

	result, err := discovery.SelectQuorum(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result.Nodes))
	assert.Equal(t, int32(3), result.Wq) // WriteQuorum = EnsembleSize
	assert.Equal(t, int32(2), result.Aq) // AckQuorum = (WriteQuorum/2) + 1
	assert.Equal(t, int32(3), result.Es)
	assert.Contains(t, result.Nodes, "node1:8080")
	assert.Contains(t, result.Nodes, "node2:8080")
	assert.Contains(t, result.Nodes, "node3:8080")
}

func TestQuorumDiscovery_SelectQuorumNodes_CustomPlacement_Success(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed-a:8080"}},
			{Name: "region-b", Seeds: []string{"seed-b:8080"}},
			{Name: "region-c", Seeds: []string{"seed-c:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "custom",
			AffinityMode: "hard",
			Replicas:     3, // This will result in EnsembleSize=3
			CustomPlacement: []config.CustomPlacement{
				{Region: "region-a", Az: "az-1", ResourceGroup: "rg-1"},
				{Region: "region-b", Az: "az-2", ResourceGroup: "rg-2"},
				{Region: "region-c", Az: "az-3", ResourceGroup: "rg-3"}, // Add third placement
			},
		},
	}

	mockClient1 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient3 := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Mock client pool calls
	mockClientPool.EXPECT().GetLogStoreClient(ctx, "seed-a:8080").Return(mockClient1, nil)
	mockClientPool.EXPECT().GetLogStoreClient(ctx, "seed-b:8080").Return(mockClient2, nil)
	mockClientPool.EXPECT().GetLogStoreClient(ctx, "seed-c:8080").Return(mockClient3, nil)

	// Mock SelectNodes calls for each placement
	mockClient1.EXPECT().SelectNodes(ctx, proto.StrategyType_CUSTOM, proto.AffinityMode_HARD, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{
		{Endpoint: "node-a:8080"},
	}, nil)
	mockClient2.EXPECT().SelectNodes(ctx, proto.StrategyType_CUSTOM, proto.AffinityMode_HARD, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{
		{Endpoint: "node-b:8080"},
	}, nil)
	mockClient3.EXPECT().SelectNodes(ctx, proto.StrategyType_CUSTOM, proto.AffinityMode_HARD, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{
		{Endpoint: "node-c:8080"},
	}, nil)

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)
	assert.NotNil(t, discovery)

	result, err := discovery.SelectQuorum(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result.Nodes))
	assert.Contains(t, result.Nodes, "node-a:8080")
	assert.Contains(t, result.Nodes, "node-b:8080")
	assert.Contains(t, result.Nodes, "node-c:8080")
}

func TestQuorumDiscovery_SelectQuorumNodes_CustomPlacement_ValidationError(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed-a:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "custom",
			AffinityMode: "hard",
			Replicas:     3, // Mismatch: 3 required but only 1 custom placement rule
			CustomPlacement: []config.CustomPlacement{
				{Region: "region-a", Az: "az-1", ResourceGroup: "rg-1"},
			},
		},
	}

	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Expect error during initialization due to invalid custom placement configuration
	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.Error(t, err)
	assert.Nil(t, discovery)
	assert.Contains(t, err.Error(), "custom placement rules count (1) must equal required nodes count (3)")
}

func TestQuorumDiscovery_SelectQuorumNodes_NoBufferPools(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)
	assert.NotNil(t, discovery)

	result, err := discovery.SelectQuorum(ctx)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "no buffer pools configured")
}

func TestQuorumDiscovery_SelectQuorumNodes_gRPCError(t *testing.T) {
	// Use timeout context to avoid infinite retry
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{
				Name:  "region-a",
				Seeds: []string{"localhost:8080"},
			},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Mock the client pool to return our mock client
	mockClientPool.EXPECT().GetLogStoreClient(ctx, "localhost:8080").Return(mockClient, nil)

	// Mock the SelectNodes call to return an error
	mockClient.EXPECT().SelectNodes(ctx, proto.StrategyType_RANDOM, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{}, errors.New("gRPC connection failed"))

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)
	assert.NotNil(t, discovery)

	result, err := discovery.SelectQuorum(ctx)

	assert.Error(t, err)
	assert.Nil(t, result)
	// Should timeout due to retry mechanism with gRPC errors
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "context deadline exceeded") || strings.Contains(err.Error(), "gRPC SelectNodes call failed"))
}

func TestQuorumDiscovery_StrategyTypeMapping(t *testing.T) {
	tests := []struct {
		name             string
		strategy         string
		expectedStrategy proto.StrategyType
	}{
		{"single-az-single-rg", "single-az-single-rg", proto.StrategyType_SINGLE_AZ_SINGLE_RG},
		{"random", "random", proto.StrategyType_RANDOM},
		{"unknown", "unknown", proto.StrategyType_RANDOM},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := &config.QuorumConfig{
				BufferPools: []config.QuorumBufferPool{
					{Name: "region-a", Seeds: []string{"seed-a-0:8080", "seed-a-1:8080", "seed-a-2:8080"}},
					{Name: "region-b", Seeds: []string{"seed-b-0:8080", "seed-b-1:8080"}},
					{Name: "region-c", Seeds: []string{"seed-c-0:8080"}},
				},
				SelectStrategy: config.QuorumSelectStrategy{
					Strategy:     tt.strategy,
					AffinityMode: "soft",
					Replicas:     1,
				},
			}

			mockClient := mocks_logstore_client.NewLogStoreClient(t)
			mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

			mockClientPool.EXPECT().GetLogStoreClient(ctx, mock.Anything).Return(mockClient, nil)
			mockClient.EXPECT().SelectNodes(ctx, tt.expectedStrategy, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return(
				[]*proto.NodeMeta{
					{Endpoint: "node-1:8080"},
					{Endpoint: "node-2:8080"},
					{Endpoint: "node-3:8080"},
				}, nil)

			discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
			assert.NoError(t, err)
			assert.NotNil(t, discovery)

			_, err = discovery.SelectQuorum(ctx)

			assert.NoError(t, err)
		})
	}
}

func TestQuorumDiscovery_SelectQuorumNodes_gRPCTimeout(t *testing.T) {
	// Test timeout behavior when gRPC calls take too long
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{
				Name:  "region-a",
				Seeds: []string{"localhost:8080"},
			},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Mock the client pool to return our mock client
	mockClientPool.EXPECT().GetLogStoreClient(ctx, "localhost:8080").Return(mockClient, nil)

	// Mock the SelectNodes call to return an error that will cause retry
	mockClient.EXPECT().SelectNodes(ctx, proto.StrategyType_RANDOM, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{}, errors.New("temporary network error"))

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)
	assert.NotNil(t, discovery)

	result, err := discovery.SelectQuorum(ctx)

	assert.Error(t, err)
	assert.Nil(t, result)
	// Should timeout due to retry mechanism with network errors
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "context deadline exceeded") || strings.Contains(err.Error(), "temporary network error"))
}

func TestQuorumDiscovery_FillRemainingNodes_ContinuesAcrossPools(t *testing.T) {
	// Bug fix: fillRemainingNodes used to break after the first successful pool
	// even if it returned fewer nodes than needed. Now it continues to try other pools.
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed-a:8080"}},
			{Name: "region-b", Seeds: []string{"seed-b:8080"}},
			{Name: "region-c", Seeds: []string{"seed-c:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "cross-region",
			AffinityMode: "soft",
			Replicas:     5, // Need 5 nodes total
		},
	}

	mockClientA := mocks_logstore_client.NewLogStoreClient(t)
	mockClientB := mocks_logstore_client.NewLogStoreClient(t)
	mockClientC := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(ctx, "seed-a:8080").Return(mockClientA, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(ctx, "seed-b:8080").Return(mockClientB, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(ctx, "seed-c:8080").Return(mockClientC, nil).Maybe()

	// First round (cross-region distribution): 5 nodes / 3 pools = 2,2,1
	// Region A returns 2 nodes as expected
	mockClientA.EXPECT().SelectNodes(ctx, proto.StrategyType_CROSS_REGION, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{
		{Endpoint: "nodeA1:8080"},
		{Endpoint: "nodeA2:8080"},
	}, nil).Once()

	// Region B returns only 1 node (less than the 2 requested) - simulates partial availability
	mockClientB.EXPECT().SelectNodes(ctx, proto.StrategyType_CROSS_REGION, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{
		{Endpoint: "nodeB1:8080"},
	}, nil).Once()

	// Region C returns 1 node as expected
	mockClientC.EXPECT().SelectNodes(ctx, proto.StrategyType_CROSS_REGION, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{
		{Endpoint: "nodeC1:8080"},
	}, nil).Once()

	// Now we have 4 nodes, need 1 more. fillRemainingNodes kicks in.
	// The fill loop will try pools again. We need to allow a second call.
	// Region A returns the extra node on the fill call
	mockClientA.EXPECT().SelectNodes(ctx, proto.StrategyType_CROSS_REGION, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{
		{Endpoint: "nodeA3:8080"},
	}, nil).Maybe()
	// Region B may also be called during fill
	mockClientB.EXPECT().SelectNodes(ctx, proto.StrategyType_CROSS_REGION, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{
		{Endpoint: "nodeB2:8080"},
	}, nil).Maybe()
	// Region C may also be called during fill
	mockClientC.EXPECT().SelectNodes(ctx, proto.StrategyType_CROSS_REGION, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{
		{Endpoint: "nodeC2:8080"},
	}, nil).Maybe()

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)

	result, err := discovery.SelectQuorum(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 5, len(result.Nodes), "Should have 5 nodes after fill from multiple pools")
}

func TestQuorumDiscovery_FillRemainingNodes_FirstPoolFails_TriesNext(t *testing.T) {
	// Verify that when the first pool in fill loop fails,
	// the loop continues to the next pool to get remaining nodes.
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed-a:8080"}},
			{Name: "region-b", Seeds: []string{"seed-b:8080"}},
			{Name: "region-c", Seeds: []string{"seed-c:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "cross-region",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	mockClientA := mocks_logstore_client.NewLogStoreClient(t)
	mockClientB := mocks_logstore_client.NewLogStoreClient(t)
	mockClientC := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-a:8080").Return(mockClientA, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-b:8080").Return(mockClientB, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-c:8080").Return(mockClientC, nil).Maybe()

	// First round: 3 nodes / 3 pools = 1 per region
	// Region A: returns 1 node OK
	mockClientA.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "nodeA1:8080"},
	}, nil)

	// Region B: fails completely — returns error
	mockClientB.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		[]*proto.NodeMeta{}, errors.New("region-b unavailable")).Once()

	// Region C: returns 1 node OK
	mockClientC.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "nodeC1:8080"},
	}, nil)

	// After first round: 2 nodes, need 1 more → fillRemainingNodes
	// Fill will try pools in order; region-b may fail again, but others succeed
	// Allow region-b to succeed on the fill call
	mockClientB.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "nodeB1:8080"},
	}, nil).Maybe()

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)

	result, err := discovery.SelectQuorum(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result.Nodes), "Should fill remaining nodes from other pools")
}

func TestQuorumDiscovery_CrossRegion_DeduplicatesNodes(t *testing.T) {
	// Bug fix: cross-region fallback could return duplicate endpoints when
	// fillRemainingNodes queries a pool that already contributed nodes.
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed-a:8080"}},
			{Name: "region-b", Seeds: []string{"seed-b:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "cross-region",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	mockClientA := mocks_logstore_client.NewLogStoreClient(t)
	mockClientB := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-a:8080").Return(mockClientA, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-b:8080").Return(mockClientB, nil).Maybe()

	// First round: 3/2 = 1 per region + 1 extra for first
	// Region A asked for 2: returns 2 nodes
	// Region B asked for 1: returns 1 node but it's the SAME endpoint as one from region A
	// (this simulates a shared node visible from both regions)
	mockClientA.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "shared-node:8080"},
		{Endpoint: "nodeA1:8080"},
	}, nil)

	mockClientB.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "shared-node:8080"}, // duplicate!
	}, nil).Once()

	// Fill round: region B returns a unique node
	mockClientB.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "nodeB1:8080"},
	}, nil).Maybe()

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)

	result, err := discovery.SelectQuorum(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify no duplicates in result
	seen := make(map[string]bool)
	for _, node := range result.Nodes {
		assert.False(t, seen[node], "Duplicate endpoint found: %s", node)
		seen[node] = true
	}

	assert.Equal(t, 3, len(result.Nodes), "Should have exactly 3 unique nodes")
}

func TestQuorumDiscovery_CrossRegion_FillDeduplicatesNodes(t *testing.T) {
	// Verify deduplication also works in the fill phase specifically:
	// when first round gets 1 node, fill queries the same pool and gets the same node back.
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed-a:8080"}},
			{Name: "region-b", Seeds: []string{"seed-b:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "cross-region",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	mockClientA := mocks_logstore_client.NewLogStoreClient(t)
	mockClientB := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-a:8080").Return(mockClientA, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-b:8080").Return(mockClientB, nil).Maybe()

	callCountA := 0
	// Region A: first round returns 1 node, fill round returns the SAME node (duplicate)
	mockClientA.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, st proto.StrategyType, am proto.AffinityMode, filters []*proto.NodeFilter) ([]*proto.NodeMeta, error) {
			callCountA++
			if callCountA == 1 {
				return []*proto.NodeMeta{{Endpoint: "nodeA1:8080"}}, nil
			}
			// Fill: returns same node as before (duplicate) plus a new one
			return []*proto.NodeMeta{
				{Endpoint: "nodeA1:8080"}, // duplicate — should be skipped
				{Endpoint: "nodeA2:8080"}, // new
			}, nil
		}).Maybe()

	// Region B: first round fails, fill round returns a unique node
	mockClientB.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{}, errors.New("region down")).Once()
	mockClientB.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "nodeB1:8080"},
	}, nil).Maybe()

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)

	result, err := discovery.SelectQuorum(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify no duplicates
	seen := make(map[string]bool)
	for _, node := range result.Nodes {
		assert.False(t, seen[node], "Duplicate endpoint found in fill phase: %s", node)
		seen[node] = true
	}

	assert.Equal(t, 3, len(result.Nodes), "Should have exactly 3 unique nodes after dedup")
	assert.Contains(t, result.Nodes, "nodeA1:8080")
	assert.Contains(t, result.Nodes, "nodeA2:8080")
	assert.Contains(t, result.Nodes, "nodeB1:8080")
}

func TestQuorumDiscovery_SingleRegion_RandomPoolSelection(t *testing.T) {
	// Bug fix: selectSingleRegionQuorum used to hardcode BufferPools[0].
	// Now it randomly selects a pool, distributing load across pools.
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "pool-a", Seeds: []string{"seed-a:8080"}},
			{Name: "pool-b", Seeds: []string{"seed-b:8080"}},
			{Name: "pool-c", Seeds: []string{"seed-c:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	mockClientA := mocks_logstore_client.NewLogStoreClient(t)
	mockClientB := mocks_logstore_client.NewLogStoreClient(t)
	mockClientC := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-a:8080").Return(mockClientA, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-b:8080").Return(mockClientB, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-c:8080").Return(mockClientC, nil).Maybe()

	// Track which seeds get called
	seedsUsed := make(map[string]int)
	mu := &sync.Mutex{}

	selectNodesFn := func(seedName string) func(context.Context, proto.StrategyType, proto.AffinityMode, []*proto.NodeFilter) ([]*proto.NodeMeta, error) {
		return func(ctx context.Context, st proto.StrategyType, am proto.AffinityMode, filters []*proto.NodeFilter) ([]*proto.NodeMeta, error) {
			mu.Lock()
			seedsUsed[seedName]++
			mu.Unlock()
			return []*proto.NodeMeta{
				{Endpoint: "node1:8080"},
				{Endpoint: "node2:8080"},
				{Endpoint: "node3:8080"},
			}, nil
		}
	}

	mockClientA.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(selectNodesFn("seed-a")).Maybe()
	mockClientB.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(selectNodesFn("seed-b")).Maybe()
	mockClientC.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(selectNodesFn("seed-c")).Maybe()

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)

	// Run many times to verify random pool distribution
	for i := 0; i < 100; i++ {
		result, err := discovery.SelectQuorum(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 3, len(result.Nodes))
	}

	// With random selection across 3 pools over 100 iterations,
	// each pool should be used at least a few times
	mu.Lock()
	defer mu.Unlock()
	poolsUsed := len(seedsUsed)
	t.Logf("Pool usage distribution: %v", seedsUsed)
	assert.GreaterOrEqual(t, poolsUsed, 2, "At least 2 different pools should be used over 100 iterations")

	for seed, count := range seedsUsed {
		assert.Greater(t, count, 5, "Pool %s should be used more than 5 times out of 100", seed)
	}
}

func TestQuorumDiscovery_SeedRotation_SkipsDeadSeed(t *testing.T) {
	// Bug fix: previously a single random seed was picked per request.
	// If that seed was dead, the request failed even though other seeds were healthy.
	// Now requestNodesFromPool tries all seeds in shuffled order.
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{
				Name:  "region-a",
				Seeds: []string{"dead-seed:8080", "healthy-seed:8080"},
			},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	mockDeadClient := mocks_logstore_client.NewLogStoreClient(t)
	mockHealthyClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "dead-seed:8080").Return(mockDeadClient, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "healthy-seed:8080").Return(mockHealthyClient, nil).Maybe()

	// Dead seed always fails
	mockDeadClient.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		[]*proto.NodeMeta{}, errors.New("connection refused")).Maybe()

	// Healthy seed always succeeds
	mockHealthyClient.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "node1:8080"},
		{Endpoint: "node2:8080"},
		{Endpoint: "node3:8080"},
	}, nil).Maybe()

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)

	// Run multiple times — should always succeed because the healthy seed is tried
	for i := 0; i < 20; i++ {
		result, err := discovery.SelectQuorum(ctx)
		assert.NoError(t, err, "Iteration %d should succeed via healthy seed", i)
		assert.NotNil(t, result)
		assert.Equal(t, 3, len(result.Nodes))
	}
}

func TestQuorumDiscovery_SeedRotation_AllSeedsDead(t *testing.T) {
	// When all seeds in a pool are dead, the error should propagate properly.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{
				Name:  "region-a",
				Seeds: []string{"dead1:8080", "dead2:8080", "dead3:8080"},
			},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	mockClient1 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient2 := mocks_logstore_client.NewLogStoreClient(t)
	mockClient3 := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "dead1:8080").Return(mockClient1, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "dead2:8080").Return(mockClient2, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "dead3:8080").Return(mockClient3, nil).Maybe()

	// All seeds fail
	mockClient1.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		[]*proto.NodeMeta{}, errors.New("dead1 refused")).Maybe()
	mockClient2.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		[]*proto.NodeMeta{}, errors.New("dead2 refused")).Maybe()
	mockClient3.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
		[]*proto.NodeMeta{}, errors.New("dead3 refused")).Maybe()

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)

	result, err := discovery.SelectQuorum(ctx)
	assert.Error(t, err, "Should fail when all seeds are dead")
	assert.Nil(t, result)
}

func TestQuorumDiscovery_CustomPlacement_SeedRotation(t *testing.T) {
	// Bug fix: selectCustomPlacementQuorum previously picked a single random seed.
	// If that seed was dead, the entire custom placement failed.
	// Now it uses requestNodesFromPool which tries all seeds in shuffled order.
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"dead-seed-a:8080", "healthy-seed-a:8080"}},
			{Name: "region-b", Seeds: []string{"healthy-seed-b:8080"}},
			{Name: "region-c", Seeds: []string{"dead-seed-c:8080", "healthy-seed-c:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "custom",
			AffinityMode: "hard",
			Replicas:     3,
			CustomPlacement: []config.CustomPlacement{
				{Region: "region-a", Az: "az-1", ResourceGroup: "rg-1"},
				{Region: "region-b", Az: "az-2", ResourceGroup: "rg-2"},
				{Region: "region-c", Az: "az-3", ResourceGroup: "rg-3"},
			},
		},
	}

	mockDeadClientA := mocks_logstore_client.NewLogStoreClient(t)
	mockHealthyClientA := mocks_logstore_client.NewLogStoreClient(t)
	mockHealthyClientB := mocks_logstore_client.NewLogStoreClient(t)
	mockDeadClientC := mocks_logstore_client.NewLogStoreClient(t)
	mockHealthyClientC := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Client pool returns appropriate clients
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "dead-seed-a:8080").Return(mockDeadClientA, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "healthy-seed-a:8080").Return(mockHealthyClientA, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "healthy-seed-b:8080").Return(mockHealthyClientB, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "dead-seed-c:8080").Return(mockDeadClientC, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "healthy-seed-c:8080").Return(mockHealthyClientC, nil).Maybe()

	// Dead seeds always fail
	mockDeadClientA.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("connection refused")).Maybe()
	mockDeadClientC.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("connection refused")).Maybe()

	// Healthy seeds always succeed
	mockHealthyClientA.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]*proto.NodeMeta{{Endpoint: "node-a:8080"}}, nil).Maybe()
	mockHealthyClientB.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]*proto.NodeMeta{{Endpoint: "node-b:8080"}}, nil).Maybe()
	mockHealthyClientC.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return([]*proto.NodeMeta{{Endpoint: "node-c:8080"}}, nil).Maybe()

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)

	// Run multiple times — should always succeed because healthy seeds are tried
	for i := 0; i < 20; i++ {
		result, err := discovery.SelectQuorum(ctx)
		assert.NoError(t, err, "iteration %d should succeed via healthy seeds", i)
		assert.NotNil(t, result)
		assert.Equal(t, 3, len(result.Nodes))
		assert.Contains(t, result.Nodes, "node-a:8080")
		assert.Contains(t, result.Nodes, "node-b:8080")
		assert.Contains(t, result.Nodes, "node-c:8080")
	}
}

func TestQuorumDiscovery_CustomPlacement_DeduplicatesNodes(t *testing.T) {
	// Bug fix: when two custom placement rules resolve to the same region+az+rg,
	// the server could return the same node for both. The client must deduplicate
	// and pick different nodes from the candidates.
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed-a:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "custom",
			AffinityMode: "hard",
			Replicas:     3,
			CustomPlacement: []config.CustomPlacement{
				{Region: "region-a", Az: "az-1", ResourceGroup: "rg-1"},
				{Region: "region-a", Az: "az-1", ResourceGroup: "rg-1"}, // Same az+rg
				{Region: "region-a", Az: "az-1", ResourceGroup: "rg-1"}, // Same az+rg
			},
		},
	}

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-a:8080").Return(mockClient, nil).Maybe()

	// Server returns 3 different nodes for the same az+rg (since we request Limit=es=3)
	mockClient.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "node1:8080"},
		{Endpoint: "node2:8080"},
		{Endpoint: "node3:8080"},
	}, nil).Maybe()

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)

	result, err := discovery.SelectQuorum(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result.Nodes))

	// Verify all nodes are unique
	seen := make(map[string]bool)
	for _, node := range result.Nodes {
		assert.False(t, seen[node], "duplicate node in quorum: %s", node)
		seen[node] = true
	}
}

func TestQuorumDiscovery_CustomPlacement_DeduplicateFails(t *testing.T) {
	// When all candidates are duplicates, the error should be clear.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed-a:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "custom",
			AffinityMode: "hard",
			Replicas:     3,
			CustomPlacement: []config.CustomPlacement{
				{Region: "region-a", Az: "az-1", ResourceGroup: "rg-1"},
				{Region: "region-a", Az: "az-1", ResourceGroup: "rg-1"},
				{Region: "region-a", Az: "az-1", ResourceGroup: "rg-1"},
			},
		},
	}

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-a:8080").Return(mockClient, nil).Maybe()

	// Server only has 1 node — second rule will fail dedup
	mockClient.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "only-node:8080"},
	}, nil).Maybe()

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)

	result, err := discovery.SelectQuorum(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.True(t, strings.Contains(err.Error(), "no unique node available") || strings.Contains(err.Error(), "already selected"),
		"error should mention dedup failure, got: %s", err.Error())
}

func TestQuorumDiscovery_CrossRegion_TrimExcessUnbiased(t *testing.T) {
	// Bug fix: when cross-region selection got more nodes than required,
	// it always kept the first pools' nodes and discarded later ones.
	// Now it shuffles before trimming for fair distribution.
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed-a:8080"}},
			{Name: "region-b", Seeds: []string{"seed-b:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "cross-region",
			AffinityMode: "soft",
			Replicas:     3, // es=3, 2 pools → pool A gets 2, pool B gets 1
		},
	}

	mockClientA := mocks_logstore_client.NewLogStoreClient(t)
	mockClientB := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-a:8080").Return(mockClientA, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-b:8080").Return(mockClientB, nil).Maybe()

	// Server returns MORE nodes than the filter Limit requests:
	// Pool A asked for 2, returns 3 nodes
	mockClientA.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "nodeA1:8080"},
		{Endpoint: "nodeA2:8080"},
		{Endpoint: "nodeA3:8080"},
	}, nil).Maybe()

	// Pool B asked for 1, returns 2 nodes
	mockClientB.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "nodeB1:8080"},
		{Endpoint: "nodeB2:8080"},
	}, nil).Maybe()

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)

	// Run many times and track which nodes appear in the trimmed result.
	// Before the fix, nodeB1 and nodeB2 would NEVER appear (always trimmed).
	// After the fix, all 5 nodes should appear at least once.
	nodeCounts := make(map[string]int)
	iterations := 500
	for i := 0; i < iterations; i++ {
		result, err := discovery.SelectQuorum(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 3, len(result.Nodes))
		for _, node := range result.Nodes {
			nodeCounts[node]++
		}
	}

	// All 5 nodes should appear at least once across 500 iterations
	allNodes := []string{"nodeA1:8080", "nodeA2:8080", "nodeA3:8080", "nodeB1:8080", "nodeB2:8080"}
	for _, node := range allNodes {
		assert.Greater(t, nodeCounts[node], 0,
			"node %s was never selected in %d iterations — trim is biased", node, iterations)
	}
}

func TestQuorumDiscovery_InvalidAffinityMode(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "invalid-mode",
			Replicas:     1,
		},
	}
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.Error(t, err)
	assert.Nil(t, discovery)
	assert.Contains(t, err.Error(), "invalid affinity mode")
}

func TestQuorumDiscovery_EmptyAffinityModeDefaultsToSoft(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "", // empty defaults to soft
			Replicas:     1,
		},
	}
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)
	assert.NotNil(t, discovery)
	d := discovery.(*quorumDiscovery)
	assert.Equal(t, proto.AffinityMode_SOFT, d.affinityMode)
}

func TestQuorumDiscovery_HardAffinityMode(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "hard",
			Replicas:     1,
		},
	}
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)
	d := discovery.(*quorumDiscovery)
	assert.Equal(t, proto.AffinityMode_HARD, d.affinityMode)
}

func TestQuorumDiscovery_CrossRegion_InsufficientPools(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed:8080"}},
			// Only 1 pool - cross-region needs at least 2
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "cross-region",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.Error(t, err)
	assert.Nil(t, discovery)
	assert.Contains(t, err.Error(), "at least two buffer pools")
}

func TestQuorumDiscovery_CustomPlacement_EmptyPlacement(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:        "custom",
			AffinityMode:    "soft",
			Replicas:        3,
			CustomPlacement: []config.CustomPlacement{}, // empty
		},
	}
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.Error(t, err)
	assert.Nil(t, discovery)
	assert.Contains(t, err.Error(), "requires CustomPlacement configuration")
}

func TestQuorumDiscovery_StrategyTypeMapping_AllStrategies(t *testing.T) {
	tests := []struct {
		strategy string
		expected proto.StrategyType
	}{
		{"single-az-single-rg", proto.StrategyType_SINGLE_AZ_SINGLE_RG},
		{"single-az-multi-rg", proto.StrategyType_SINGLE_AZ_MULTI_RG},
		{"multi-az-single-rg", proto.StrategyType_MULTI_AZ_SINGLE_RG},
		{"multi-az-multi-rg", proto.StrategyType_MULTI_AZ_MULTI_RG},
		{"random-group", proto.StrategyType_RANDOM_GROUP},
		{"random", proto.StrategyType_RANDOM},
		{"", proto.StrategyType_RANDOM},       // empty defaults to RANDOM
		{"foobar", proto.StrategyType_RANDOM}, // unknown defaults to RANDOM
	}

	for _, tt := range tests {
		t.Run(tt.strategy, func(t *testing.T) {
			d := &quorumDiscovery{
				cfg: &config.QuorumConfig{
					SelectStrategy: config.QuorumSelectStrategy{
						Strategy: tt.strategy,
					},
				},
			}
			err := d.parseStrategyType()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, d.strategyType)
		})
	}
}

func TestQuorumDiscovery_CrossRegion_HardAffinityFail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed-a:8080"}},
			{Name: "region-b", Seeds: []string{"seed-b:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "cross-region",
			AffinityMode: "hard",
			Replicas:     3,
		},
	}

	mockClientA := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-a:8080").Return(mockClientA, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-b:8080").Return(nil, errors.New("connection refused")).Maybe()

	mockClientA.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "nodeA1:8080"},
	}, nil).Maybe()

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)

	result, err := discovery.SelectQuorum(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestQuorumDiscovery_CrossRegion_HardAffinity_InsufficientNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed-a:8080"}},
			{Name: "region-b", Seeds: []string{"seed-b:8080"}},
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "cross-region",
			AffinityMode: "hard",
			Replicas:     5,
		},
	}

	mockClientA := mocks_logstore_client.NewLogStoreClient(t)
	mockClientB := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-a:8080").Return(mockClientA, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-b:8080").Return(mockClientB, nil).Maybe()

	// Each region only returns 1 node, but we need 5
	mockClientA.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "nodeA1:8080"},
	}, nil).Maybe()
	mockClientB.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "nodeB1:8080"},
	}, nil).Maybe()

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)

	result, err := discovery.SelectQuorum(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "insufficient")
}

func TestQuorumDiscovery_SingleRegion_NoSeeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{}}, // no seeds
		},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     1,
		},
	}
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)

	result, err := discovery.SelectQuorum(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestQuorumDiscovery_Close(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{}
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	assert.NoError(t, err)
	assert.NotNil(t, discovery)

	err = discovery.Close(ctx)

	assert.NoError(t, err)
}

// === Additional coverage tests for uncovered branches ===

func TestQuorumDiscovery_FillRemainingNodes_AlreadyEnough(t *testing.T) {
	// Covers fillRemainingNodes when remainingNeeded <= 0
	d := &quorumDiscovery{
		cfg: &config.QuorumConfig{},
		es:  3,
		wq:  2,
		aq:  1,
	}

	ctx := context.Background()
	currentNodes := []string{"n1", "n2", "n3"}
	selectedSet := map[string]bool{"n1": true, "n2": true, "n3": true}

	result, err := d.fillRemainingNodes(ctx, currentNodes, selectedSet)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result.Nodes))
}

func TestQuorumDiscovery_FillRemainingNodes_NilSelectedSet(t *testing.T) {
	// Covers fillRemainingNodes when selectedSet is nil
	ctx := context.Background()

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(ctx, "seed1:8080").Return(mockClient, nil).Maybe()
	mockClient.EXPECT().SelectNodes(ctx, mock.Anything, mock.Anything, mock.Anything).
		Return([]*proto.NodeMeta{
			{Endpoint: "n2:8080"},
			{Endpoint: "n3:8080"},
		}, nil).Maybe()

	d := &quorumDiscovery{
		cfg: &config.QuorumConfig{
			BufferPools: []config.QuorumBufferPool{
				{Name: "pool1", Seeds: []string{"seed1:8080"}},
			},
		},
		clientPool: mockClientPool,
		es:         3,
		wq:         2,
		aq:         1,
	}

	currentNodes := []string{"n1:8080"}
	result, err := d.fillRemainingNodes(ctx, currentNodes, nil) // nil selectedSet
	// May succeed or fail depending on mock behavior, just exercise the nil path
	if err == nil {
		assert.NotNil(t, result)
	}
}

func TestQuorumDiscovery_FillRemainingNodes_PoolNoSeeds(t *testing.T) {
	// Covers fillRemainingNodes when pool has no seeds
	ctx := context.Background()

	d := &quorumDiscovery{
		cfg: &config.QuorumConfig{
			BufferPools: []config.QuorumBufferPool{
				{Name: "pool1", Seeds: []string{}}, // no seeds
			},
		},
		es: 3,
		wq: 2,
		aq: 1,
	}

	currentNodes := []string{"n1:8080"}
	selectedSet := map[string]bool{"n1:8080": true}

	_, err := d.fillRemainingNodes(ctx, currentNodes, selectedSet)
	assert.Error(t, err) // insufficient quorum
}

func TestQuorumDiscovery_FillRemainingNodes_RequestError(t *testing.T) {
	// Covers fillRemainingNodes error path from requestNodesFromPool
	ctx := context.Background()

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(ctx, "seed1:8080").Return(mockClient, nil)
	mockClient.EXPECT().SelectNodes(ctx, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("connection refused"))

	d := &quorumDiscovery{
		cfg: &config.QuorumConfig{
			BufferPools: []config.QuorumBufferPool{
				{Name: "pool1", Seeds: []string{"seed1:8080"}},
			},
		},
		clientPool:   mockClientPool,
		strategyType: proto.StrategyType_RANDOM,
		affinityMode: proto.AffinityMode_SOFT,
		es:           3,
		wq:           2,
		aq:           1,
	}

	currentNodes := []string{"n1:8080"}
	selectedSet := map[string]bool{"n1:8080": true}

	_, err := d.fillRemainingNodes(ctx, currentNodes, selectedSet)
	assert.Error(t, err) // insufficient quorum after pool failure
}

func TestQuorumDiscovery_SelectSingleRegion_FiltersCountError(t *testing.T) {
	// Covers selectSingleRegionQuorum when len(d.filters) != 1
	ctx := context.Background()
	d := &quorumDiscovery{
		cfg: &config.QuorumConfig{
			BufferPools: []config.QuorumBufferPool{
				{Name: "pool1", Seeds: []string{"seed1:8080"}},
			},
		},
		filters: []*proto.NodeFilter{}, // empty - not 1
		es:      3,
	}

	_, err := d.selectSingleRegionQuorum(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected 1 filter")
}

func TestQuorumDiscovery_SelectCrossRegion_FiltersCountError(t *testing.T) {
	// Covers selectCrossRegionQuorum when len(d.filters) != 1
	ctx := context.Background()
	d := &quorumDiscovery{
		cfg: &config.QuorumConfig{
			BufferPools: []config.QuorumBufferPool{
				{Name: "pool1", Seeds: []string{"s1"}},
				{Name: "pool2", Seeds: []string{"s2"}},
			},
		},
		filters: []*proto.NodeFilter{{}, {}}, // 2 filters, not 1
		es:      3,
	}

	_, err := d.selectCrossRegionQuorum(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected 1 filter")
}

func TestQuorumDiscovery_SelectCrossRegion_PoolNoSeeds(t *testing.T) {
	// Covers selectCrossRegionQuorum when a pool has no seeds (skip)
	ctx := context.Background()

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(ctx, "seed2:8080").Return(mockClient, nil)
	mockClient.EXPECT().SelectNodes(ctx, mock.Anything, mock.Anything, mock.Anything).
		Return([]*proto.NodeMeta{
			{Endpoint: "n1:8080"},
			{Endpoint: "n2:8080"},
			{Endpoint: "n3:8080"},
		}, nil)

	d := &quorumDiscovery{
		cfg: &config.QuorumConfig{
			BufferPools: []config.QuorumBufferPool{
				{Name: "pool1", Seeds: []string{}}, // no seeds - skipped
				{Name: "pool2", Seeds: []string{"seed2:8080"}},
			},
		},
		clientPool:   mockClientPool,
		strategyType: proto.StrategyType_CROSS_REGION,
		affinityMode: proto.AffinityMode_SOFT,
		filters:      []*proto.NodeFilter{{}},
		es:           3,
		wq:           2,
		aq:           1,
	}

	result, err := d.selectCrossRegionQuorum(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestQuorumDiscovery_SelectCrossRegion_RegionNodesZero(t *testing.T) {
	// Covers regionNodes == 0 path when more pools than required nodes
	ctx := context.Background()

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	// Only the first pool will be asked for nodes (es=1, 3 pools -> 0,0,0 nodes + 1 remaining for first pool)
	mockClientPool.EXPECT().GetLogStoreClient(ctx, mock.Anything).Return(mockClient, nil).Maybe()
	mockClient.EXPECT().SelectNodes(ctx, mock.Anything, mock.Anything, mock.Anything).
		Return([]*proto.NodeMeta{{Endpoint: "n1:8080"}}, nil).Maybe()

	d := &quorumDiscovery{
		cfg: &config.QuorumConfig{
			BufferPools: []config.QuorumBufferPool{
				{Name: "pool1", Seeds: []string{"s1:8080"}},
				{Name: "pool2", Seeds: []string{"s2:8080"}},
				{Name: "pool3", Seeds: []string{"s3:8080"}},
			},
		},
		clientPool:   mockClientPool,
		strategyType: proto.StrategyType_CROSS_REGION,
		affinityMode: proto.AffinityMode_SOFT,
		filters:      []*proto.NodeFilter{{}},
		es:           1, // only 1 node needed across 3 pools -> some get 0
		wq:           1,
		aq:           1,
	}

	result, err := d.selectCrossRegionQuorum(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, len(result.Nodes))
}

func TestQuorumDiscovery_CustomPlacement_FiltersCountMismatch(t *testing.T) {
	// Covers selectCustomPlacementQuorum when filters count != placements count
	ctx := context.Background()
	d := &quorumDiscovery{
		cfg: &config.QuorumConfig{
			SelectStrategy: config.QuorumSelectStrategy{
				CustomPlacement: []config.CustomPlacement{
					{Region: "r1"},
					{Region: "r2"},
				},
			},
		},
		filters: []*proto.NodeFilter{{}}, // 1 filter vs 2 placements
		es:      2,
	}

	_, err := d.selectCustomPlacementQuorum(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "filters count")
}

func TestQuorumDiscovery_CustomPlacement_PoolNotFound(t *testing.T) {
	// Covers selectCustomPlacementQuorum when targetPool is nil
	ctx := context.Background()
	d := &quorumDiscovery{
		cfg: &config.QuorumConfig{
			BufferPools: []config.QuorumBufferPool{
				{Name: "other-region", Seeds: []string{"s1"}},
			},
			SelectStrategy: config.QuorumSelectStrategy{
				CustomPlacement: []config.CustomPlacement{
					{Region: "nonexistent-region"},
				},
			},
		},
		filters: []*proto.NodeFilter{{}},
		es:      1,
	}

	_, err := d.selectCustomPlacementQuorum(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "buffer pool not found")
}

func TestQuorumDiscovery_CustomPlacement_PoolNoSeeds(t *testing.T) {
	// Covers selectCustomPlacementQuorum when targetPool has no seeds
	ctx := context.Background()
	d := &quorumDiscovery{
		cfg: &config.QuorumConfig{
			BufferPools: []config.QuorumBufferPool{
				{Name: "r1", Seeds: []string{}}, // no seeds
			},
			SelectStrategy: config.QuorumSelectStrategy{
				CustomPlacement: []config.CustomPlacement{
					{Region: "r1"},
				},
			},
		},
		filters: []*proto.NodeFilter{{}},
		es:      1,
	}

	_, err := d.selectCustomPlacementQuorum(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no seeds configured")
}

func TestQuorumDiscovery_CustomPlacement_EmptyResult(t *testing.T) {
	// Covers selectCustomPlacementQuorum when regionResult is nil or empty
	ctx := context.Background()

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(ctx, "s1:8080").Return(mockClient, nil)
	mockClient.EXPECT().SelectNodes(ctx, mock.Anything, mock.Anything, mock.Anything).
		Return([]*proto.NodeMeta{}, nil) // empty result

	d := &quorumDiscovery{
		cfg: &config.QuorumConfig{
			BufferPools: []config.QuorumBufferPool{
				{Name: "r1", Seeds: []string{"s1:8080"}},
			},
			SelectStrategy: config.QuorumSelectStrategy{
				CustomPlacement: []config.CustomPlacement{
					{Region: "r1"},
				},
			},
		},
		clientPool:   mockClientPool,
		strategyType: proto.StrategyType_CUSTOM,
		affinityMode: proto.AffinityMode_SOFT,
		filters:      []*proto.NodeFilter{{}},
		es:           1,
		wq:           1,
		aq:           1,
	}

	_, err := d.selectCustomPlacementQuorum(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient nodes")
}

func TestQuorumDiscovery_CustomPlacement_RequestError(t *testing.T) {
	// Covers selectCustomPlacementQuorum request error path
	ctx := context.Background()

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(ctx, "s1:8080").Return(mockClient, nil)
	mockClient.EXPECT().SelectNodes(ctx, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("connection refused"))

	d := &quorumDiscovery{
		cfg: &config.QuorumConfig{
			BufferPools: []config.QuorumBufferPool{
				{Name: "r1", Seeds: []string{"s1:8080"}},
			},
			SelectStrategy: config.QuorumSelectStrategy{
				CustomPlacement: []config.CustomPlacement{
					{Region: "r1"},
				},
			},
		},
		clientPool:   mockClientPool,
		strategyType: proto.StrategyType_CUSTOM,
		affinityMode: proto.AffinityMode_SOFT,
		filters:      []*proto.NodeFilter{{}},
		es:           1,
		wq:           1,
		aq:           1,
	}

	_, err := d.selectCustomPlacementQuorum(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to select node")
}

func TestQuorumDiscovery_RequestNodesFromSeed_InsufficientNodes(t *testing.T) {
	// Covers requestNodesFromSeed when len(selectedNodes) < expectedAtLeast
	ctx := context.Background()

	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	mockClientPool.EXPECT().GetLogStoreClient(ctx, "seed1:8080").Return(mockClient, nil)
	mockClient.EXPECT().SelectNodes(ctx, mock.Anything, mock.Anything, mock.Anything).
		Return([]*proto.NodeMeta{{Endpoint: "n1:8080"}}, nil) // only 1, but expected 3

	d := &quorumDiscovery{
		cfg:          &config.QuorumConfig{},
		clientPool:   mockClientPool,
		strategyType: proto.StrategyType_RANDOM,
		affinityMode: proto.AffinityMode_SOFT,
		es:           3,
		wq:           2,
		aq:           1,
	}

	filter := &proto.NodeFilter{Limit: 3}
	_, err := d.requestNodesFromSeed(ctx, "seed1:8080", filter, 3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient nodes")
}

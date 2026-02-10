package quorum

import (
	"context"
	"errors"
	"strings"
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

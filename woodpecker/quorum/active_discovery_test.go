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
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

// MockLogStoreClientPool is a mock implementation of LogStoreClientPool
type MockLogStoreClientPool struct {
	mock.Mock
}

func (m *MockLogStoreClientPool) GetLogStoreClient(ctx context.Context, target string) (client.LogStoreClient, error) {
	args := m.Called(ctx, target)
	return args.Get(0).(client.LogStoreClient), args.Error(1)
}

func (m *MockLogStoreClientPool) Clear(ctx context.Context, target string) {
	m.Called(ctx, target)
}

func (m *MockLogStoreClientPool) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func TestNewActiveQuorumDiscovery(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		NodeDiscoveryMode: "active",
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}
	mockClientPool := &MockLogStoreClientPool{}

	discovery := NewActiveQuorumDiscovery(ctx, cfg, mockClientPool)

	assert.NotNil(t, discovery)
	assert.IsType(t, &activeQuorumDiscovery{}, discovery)
}

func TestActiveQuorumDiscovery_SelectQuorumNodes_SingleRegion_Success(t *testing.T) {
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
	mockClientPool := &MockLogStoreClientPool{}

	// Mock the client pool to return our mock client
	mockClientPool.On("GetLogStoreClient", ctx, "localhost:8080").Return(mockClient, nil)

	// Mock the SelectNodes call
	expectedNodes := []*proto.NodeMeta{
		{Endpoint: "node1:8080"},
		{Endpoint: "node2:8080"},
		{Endpoint: "node3:8080"},
	}
	mockClient.On("SelectNodes", ctx, proto.StrategyType_RANDOM, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return(expectedNodes, nil)

	discovery := NewActiveQuorumDiscovery(ctx, cfg, mockClientPool)
	result, err := discovery.SelectQuorumNodes(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result.Nodes))
	assert.Equal(t, int32(3), result.Wq) // WriteQuorum = EnsembleSize
	assert.Equal(t, int32(2), result.Aq) // AckQuorum = (WriteQuorum/2) + 1
	assert.Equal(t, int32(3), result.Es)
	assert.Contains(t, result.Nodes, "node1:8080")
	assert.Contains(t, result.Nodes, "node2:8080")
	assert.Contains(t, result.Nodes, "node3:8080")

	mockClientPool.AssertExpectations(t)
}

func TestActiveQuorumDiscovery_SelectQuorumNodes_CustomPlacement_Success(t *testing.T) {
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
	mockClientPool := &MockLogStoreClientPool{}

	// Mock client pool calls
	mockClientPool.On("GetLogStoreClient", ctx, "seed-a:8080").Return(mockClient1, nil)
	mockClientPool.On("GetLogStoreClient", ctx, "seed-b:8080").Return(mockClient2, nil)
	mockClientPool.On("GetLogStoreClient", ctx, "seed-c:8080").Return(mockClient3, nil)

	// Mock SelectNodes calls for each placement
	mockClient1.On("SelectNodes", ctx, proto.StrategyType_CUSTOM, proto.AffinityMode_HARD, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{
		{Endpoint: "node-a:8080"},
	}, nil)
	mockClient2.On("SelectNodes", ctx, proto.StrategyType_CUSTOM, proto.AffinityMode_HARD, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{
		{Endpoint: "node-b:8080"},
	}, nil)
	mockClient3.On("SelectNodes", ctx, proto.StrategyType_CUSTOM, proto.AffinityMode_HARD, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{
		{Endpoint: "node-c:8080"},
	}, nil)

	discovery := NewActiveQuorumDiscovery(ctx, cfg, mockClientPool)
	result, err := discovery.SelectQuorumNodes(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result.Nodes))
	assert.Contains(t, result.Nodes, "node-a:8080")
	assert.Contains(t, result.Nodes, "node-b:8080")
	assert.Contains(t, result.Nodes, "node-c:8080")

	mockClientPool.AssertExpectations(t)
}

func TestActiveQuorumDiscovery_SelectQuorumNodes_CustomPlacement_ValidationError(t *testing.T) {
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

	mockClientPool := &MockLogStoreClientPool{}
	discovery := NewActiveQuorumDiscovery(ctx, cfg, mockClientPool)

	result, err := discovery.SelectQuorumNodes(ctx)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "custom placement rules count (1) must equal required nodes count (3)")
}

func TestActiveQuorumDiscovery_SelectQuorumNodes_NoBufferPools(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: []config.QuorumBufferPool{},
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     "random",
			AffinityMode: "soft",
			Replicas:     3,
		},
	}

	mockClientPool := &MockLogStoreClientPool{}
	discovery := NewActiveQuorumDiscovery(ctx, cfg, mockClientPool)

	result, err := discovery.SelectQuorumNodes(ctx)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "no buffer pools configured")
}

func TestActiveQuorumDiscovery_SelectQuorumNodes_gRPCError(t *testing.T) {
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
	mockClientPool := &MockLogStoreClientPool{}

	// Mock the client pool to return our mock client
	mockClientPool.On("GetLogStoreClient", ctx, "localhost:8080").Return(mockClient, nil)

	// Mock the SelectNodes call to return an error
	mockClient.On("SelectNodes", ctx, proto.StrategyType_RANDOM, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{}, errors.New("gRPC connection failed"))

	discovery := NewActiveQuorumDiscovery(ctx, cfg, mockClientPool)
	result, err := discovery.SelectQuorumNodes(ctx)

	assert.Error(t, err)
	assert.Nil(t, result)
	// Should timeout due to retry mechanism with gRPC errors
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "context deadline exceeded") || strings.Contains(err.Error(), "gRPC SelectNodes call failed"))

	mockClientPool.AssertExpectations(t)
}

func TestActiveQuorumDiscovery_StrategyTypeMapping(t *testing.T) {
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
					{Name: "region-a", Seeds: []string{"seed-a:8080"}},
				},
				SelectStrategy: config.QuorumSelectStrategy{
					Strategy:     tt.strategy,
					AffinityMode: "soft",
					Replicas:     1,
				},
			}

			mockClient := mocks_logstore_client.NewLogStoreClient(t)
			mockClientPool := &MockLogStoreClientPool{}

			mockClientPool.On("GetLogStoreClient", ctx, "seed-a:8080").Return(mockClient, nil)
			mockClient.On("SelectNodes", ctx, tt.expectedStrategy, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{
				{Endpoint: "node:8080"},
			}, nil)

			discovery := NewActiveQuorumDiscovery(ctx, cfg, mockClientPool)
			_, err := discovery.SelectQuorumNodes(ctx)

			assert.NoError(t, err)
		})
	}
}

func TestActiveQuorumDiscovery_SelectQuorumNodes_gRPCTimeout(t *testing.T) {
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
	mockClientPool := &MockLogStoreClientPool{}

	// Mock the client pool to return our mock client
	mockClientPool.On("GetLogStoreClient", ctx, "localhost:8080").Return(mockClient, nil)

	// Mock the SelectNodes call to return an error that will cause retry
	mockClient.On("SelectNodes", ctx, proto.StrategyType_RANDOM, proto.AffinityMode_SOFT, mock.AnythingOfType("[]*proto.NodeFilter")).Return([]*proto.NodeMeta{}, errors.New("temporary network error"))

	discovery := NewActiveQuorumDiscovery(ctx, cfg, mockClientPool)
	result, err := discovery.SelectQuorumNodes(ctx)

	assert.Error(t, err)
	assert.Nil(t, result)
	// Should timeout due to retry mechanism with network errors
	assert.True(t, errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "context deadline exceeded") || strings.Contains(err.Error(), "temporary network error"))

	mockClientPool.AssertExpectations(t)
}

func TestActiveQuorumDiscovery_Close(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{}
	mockClientPool := &MockLogStoreClientPool{}

	discovery := NewActiveQuorumDiscovery(ctx, cfg, mockClientPool)
	err := discovery.Close(ctx)

	assert.NoError(t, err)
}

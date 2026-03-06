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

package woodpecker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	mocks_logstore_client "github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	mocks_quorum "github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_quorum"
	"github.com/zilliztech/woodpecker/proto"
)

// newTestWoodpeckerClient creates a woodpeckerClient with mocks for testing.
func newTestWoodpeckerClient(t *testing.T) (*woodpeckerClient, *mocks_meta.MetadataProvider, *mocks_logstore_client.LogStoreClientPool, *mocks_quorum.QuorumDiscovery) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockQuorum := mocks_quorum.NewQuorumDiscovery(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentRollingPolicy: config.SegmentRollingPolicyConfig{
					MaxSize:     config.ByteSize(100000000),
					MaxInterval: config.NewDurationSecondsFromInt(800),
					MaxBlocks:   1000,
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-root",
		},
	}
	c := &woodpeckerClient{
		cfg:             cfg,
		Metadata:        mockMeta,
		clientPool:      mockPool,
		quorumDiscovery: mockQuorum,
	}
	c.closeState.Store(false)
	return c, mockMeta, mockPool, mockQuorum
}

// ============================================================
// Tests for createLogUnsafe
// ============================================================

func TestCreateLogUnsafe_Success(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	ctx := context.Background()

	mockMeta.EXPECT().CreateLog(mock.Anything, "test-log").Return(nil).Once()

	err := createLogUnsafe(ctx, mockMeta, "test-log")
	assert.NoError(t, err)
}

func TestCreateLogUnsafe_NonTxnError(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	ctx := context.Background()

	someErr := fmt.Errorf("some non-txn error")
	mockMeta.EXPECT().CreateLog(mock.Anything, "test-log").Return(someErr).Once()

	err := createLogUnsafe(ctx, mockMeta, "test-log")
	assert.Error(t, err)
	assert.Equal(t, someErr, err)
}

func TestCreateLogUnsafe_RetryOnTxnConflict(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	ctx := context.Background()

	// First two calls fail with txn error, third succeeds
	txnErr := werr.ErrMetadataCreateLogTxn.WithCauseErrMsg("txn conflict")
	mockMeta.EXPECT().CreateLog(mock.Anything, "test-log").Return(txnErr).Times(2)
	mockMeta.EXPECT().CreateLog(mock.Anything, "test-log").Return(nil).Once()

	err := createLogUnsafe(ctx, mockMeta, "test-log")
	assert.NoError(t, err)
}

func TestCreateLogUnsafe_ExhaustRetries(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	ctx := context.Background()

	// All 5 retries fail with txn error
	txnErr := werr.ErrMetadataCreateLogTxn.WithCauseErrMsg("txn conflict")
	mockMeta.EXPECT().CreateLog(mock.Anything, "test-log").Return(txnErr).Times(5)

	err := createLogUnsafe(ctx, mockMeta, "test-log")
	assert.Error(t, err)
	assert.True(t, werr.ErrMetadataCreateLogTxn.Is(err))
}

// ============================================================
// Tests for openLogUnsafe
// ============================================================

func TestOpenLogUnsafe_Success(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	ctx := context.Background()
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentRollingPolicy: config.SegmentRollingPolicyConfig{
					MaxSize:     config.ByteSize(100000000),
					MaxInterval: config.NewDurationSecondsFromInt(800),
					MaxBlocks:   1000,
				},
			},
		},
		Minio: config.MinioConfig{
			BucketName: "test-bucket",
			RootPath:   "test-root",
		},
	}

	logMeta := &meta.LogMeta{
		Metadata: &proto.LogMeta{LogId: 1},
		Revision: 1,
	}
	segmentsMeta := map[int64]*meta.SegmentMeta{}
	mockMeta.EXPECT().OpenLog(mock.Anything, "test-log").Return(logMeta, segmentsMeta, nil).Once()

	selectFunc := func(ctx context.Context) (*proto.QuorumInfo, error) {
		return &proto.QuorumInfo{Id: 1, Wq: 1, Aq: 1, Es: 1, Nodes: []string{"localhost:8888"}}, nil
	}

	handle, err := openLogUnsafe(ctx, mockMeta, "test-log", mockPool, cfg, selectFunc)
	require.NoError(t, err)
	assert.NotNil(t, handle)
}

func TestOpenLogUnsafe_Error(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	ctx := context.Background()
	cfg := &config.Configuration{}

	openErr := fmt.Errorf("log not found")
	mockMeta.EXPECT().OpenLog(mock.Anything, "test-log").Return(nil, nil, openErr).Once()

	selectFunc := func(ctx context.Context) (*proto.QuorumInfo, error) {
		return nil, nil
	}

	handle, err := openLogUnsafe(ctx, mockMeta, "test-log", mockPool, cfg, selectFunc)
	assert.Error(t, err)
	assert.Nil(t, handle)
	assert.Equal(t, openErr, err)
}

// ============================================================
// Tests for logExistsUnsafe
// ============================================================

func TestLogExistsUnsafe_Exists(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	ctx := context.Background()

	mockMeta.EXPECT().CheckExists(mock.Anything, "test-log").Return(true, nil).Once()

	exists, err := logExistsUnsafe(ctx, mockMeta, "test-log")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestLogExistsUnsafe_NotExists(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	ctx := context.Background()

	mockMeta.EXPECT().CheckExists(mock.Anything, "test-log").Return(false, nil).Once()

	exists, err := logExistsUnsafe(ctx, mockMeta, "test-log")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestLogExistsUnsafe_Error(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	ctx := context.Background()

	someErr := fmt.Errorf("etcd error")
	mockMeta.EXPECT().CheckExists(mock.Anything, "test-log").Return(false, someErr).Once()

	exists, err := logExistsUnsafe(ctx, mockMeta, "test-log")
	assert.Error(t, err)
	assert.False(t, exists)
}

// ============================================================
// Tests for getAllLogsUnsafe
// ============================================================

func TestGetAllLogsUnsafe_Success(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	ctx := context.Background()

	expected := []string{"log1", "log2", "log3"}
	mockMeta.EXPECT().ListLogs(mock.Anything).Return(expected, nil).Once()

	logs, err := getAllLogsUnsafe(ctx, mockMeta)
	assert.NoError(t, err)
	assert.Equal(t, expected, logs)
}

func TestGetAllLogsUnsafe_Empty(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	ctx := context.Background()

	mockMeta.EXPECT().ListLogs(mock.Anything).Return([]string{}, nil).Once()

	logs, err := getAllLogsUnsafe(ctx, mockMeta)
	assert.NoError(t, err)
	assert.Empty(t, logs)
}

func TestGetAllLogsUnsafe_Error(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	ctx := context.Background()

	someErr := fmt.Errorf("list failed")
	mockMeta.EXPECT().ListLogs(mock.Anything).Return(nil, someErr).Once()

	logs, err := getAllLogsUnsafe(ctx, mockMeta)
	assert.Error(t, err)
	assert.Nil(t, logs)
}

// ============================================================
// Tests for getLogsWithPrefix
// ============================================================

func TestGetLogsWithPrefix_Success(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	ctx := context.Background()

	expected := []string{"prefix-log1", "prefix-log2"}
	mockMeta.EXPECT().ListLogsWithPrefix(mock.Anything, "prefix-").Return(expected, nil).Once()

	logs, err := getLogsWithPrefix(ctx, mockMeta, "prefix-")
	assert.NoError(t, err)
	assert.Equal(t, expected, logs)
}

func TestGetLogsWithPrefix_Error(t *testing.T) {
	mockMeta := mocks_meta.NewMetadataProvider(t)
	ctx := context.Background()

	someErr := fmt.Errorf("prefix query failed")
	mockMeta.EXPECT().ListLogsWithPrefix(mock.Anything, "prefix-").Return(nil, someErr).Once()

	logs, err := getLogsWithPrefix(ctx, mockMeta, "prefix-")
	assert.Error(t, err)
	assert.Nil(t, logs)
}

// ============================================================
// Tests for woodpeckerClient.GetMetadataProvider
// ============================================================

func TestWoodpeckerClient_GetMetadataProvider(t *testing.T) {
	c, mockMeta, _, _ := newTestWoodpeckerClient(t)
	assert.Equal(t, mockMeta, c.GetMetadataProvider())
}

// ============================================================
// Tests for woodpeckerClient.CreateLog
// ============================================================

func TestWoodpeckerClient_CreateLog_Success(t *testing.T) {
	c, mockMeta, _, _ := newTestWoodpeckerClient(t)
	ctx := context.Background()

	mockMeta.EXPECT().CreateLog(mock.Anything, "test-log").Return(nil).Once()

	err := c.CreateLog(ctx, "test-log")
	assert.NoError(t, err)
}

func TestWoodpeckerClient_CreateLog_Closed(t *testing.T) {
	c, _, _, _ := newTestWoodpeckerClient(t)
	c.closeState.Store(true)
	ctx := context.Background()

	err := c.CreateLog(ctx, "test-log")
	assert.Error(t, err)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

// ============================================================
// Tests for woodpeckerClient.OpenLog
// ============================================================

func TestWoodpeckerClient_OpenLog_Success(t *testing.T) {
	c, mockMeta, _, _ := newTestWoodpeckerClient(t)
	ctx := context.Background()

	logMeta := &meta.LogMeta{
		Metadata: &proto.LogMeta{LogId: 1},
		Revision: 1,
	}
	segmentsMeta := map[int64]*meta.SegmentMeta{}
	mockMeta.EXPECT().OpenLog(mock.Anything, "test-log").Return(logMeta, segmentsMeta, nil).Once()

	handle, err := c.OpenLog(ctx, "test-log")
	require.NoError(t, err)
	assert.NotNil(t, handle)
}

func TestWoodpeckerClient_OpenLog_Error(t *testing.T) {
	c, mockMeta, _, _ := newTestWoodpeckerClient(t)
	ctx := context.Background()

	openErr := fmt.Errorf("open failed")
	mockMeta.EXPECT().OpenLog(mock.Anything, "test-log").Return(nil, nil, openErr).Once()

	handle, err := c.OpenLog(ctx, "test-log")
	assert.Error(t, err)
	assert.Nil(t, handle)
}

func TestWoodpeckerClient_OpenLog_Closed(t *testing.T) {
	c, _, _, _ := newTestWoodpeckerClient(t)
	c.closeState.Store(true)
	ctx := context.Background()

	handle, err := c.OpenLog(ctx, "test-log")
	assert.Error(t, err)
	assert.Nil(t, handle)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

// ============================================================
// Tests for woodpeckerClient.SelectQuorumNodes
// ============================================================

func TestWoodpeckerClient_SelectQuorumNodes_Success(t *testing.T) {
	c, _, _, mockQuorum := newTestWoodpeckerClient(t)
	ctx := context.Background()

	expected := &proto.QuorumInfo{Id: 1, Wq: 3, Aq: 2, Es: 3, Nodes: []string{"node1", "node2", "node3"}}
	mockQuorum.EXPECT().SelectQuorum(mock.Anything).Return(expected, nil).Once()

	result, err := c.SelectQuorumNodes(ctx)
	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestWoodpeckerClient_SelectQuorumNodes_Error(t *testing.T) {
	c, _, _, mockQuorum := newTestWoodpeckerClient(t)
	ctx := context.Background()

	quorumErr := fmt.Errorf("no nodes available")
	mockQuorum.EXPECT().SelectQuorum(mock.Anything).Return(nil, quorumErr).Once()

	result, err := c.SelectQuorumNodes(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestWoodpeckerClient_SelectQuorumNodes_Closed(t *testing.T) {
	c, _, _, _ := newTestWoodpeckerClient(t)
	c.closeState.Store(true)
	ctx := context.Background()

	result, err := c.SelectQuorumNodes(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

// ============================================================
// Tests for woodpeckerClient.DeleteLog
// ============================================================

func TestWoodpeckerClient_DeleteLog(t *testing.T) {
	c, _, _, _ := newTestWoodpeckerClient(t)
	ctx := context.Background()

	err := c.DeleteLog(ctx, "test-log")
	assert.Error(t, err)
	assert.True(t, werr.ErrOperationNotSupported.Is(err))
}

// ============================================================
// Tests for woodpeckerClient.LogExists
// ============================================================

func TestWoodpeckerClient_LogExists_True(t *testing.T) {
	c, mockMeta, _, _ := newTestWoodpeckerClient(t)
	ctx := context.Background()

	mockMeta.EXPECT().CheckExists(mock.Anything, "test-log").Return(true, nil).Once()

	exists, err := c.LogExists(ctx, "test-log")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestWoodpeckerClient_LogExists_False(t *testing.T) {
	c, mockMeta, _, _ := newTestWoodpeckerClient(t)
	ctx := context.Background()

	mockMeta.EXPECT().CheckExists(mock.Anything, "test-log").Return(false, nil).Once()

	exists, err := c.LogExists(ctx, "test-log")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestWoodpeckerClient_LogExists_Closed(t *testing.T) {
	c, _, _, _ := newTestWoodpeckerClient(t)
	c.closeState.Store(true)
	ctx := context.Background()

	exists, err := c.LogExists(ctx, "test-log")
	assert.Error(t, err)
	assert.False(t, exists)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

// ============================================================
// Tests for woodpeckerClient.GetAllLogs
// ============================================================

func TestWoodpeckerClient_GetAllLogs_Success(t *testing.T) {
	c, mockMeta, _, _ := newTestWoodpeckerClient(t)
	ctx := context.Background()

	expected := []string{"log1", "log2"}
	mockMeta.EXPECT().ListLogs(mock.Anything).Return(expected, nil).Once()

	logs, err := c.GetAllLogs(ctx)
	assert.NoError(t, err)
	assert.Equal(t, expected, logs)
}

func TestWoodpeckerClient_GetAllLogs_Closed(t *testing.T) {
	c, _, _, _ := newTestWoodpeckerClient(t)
	c.closeState.Store(true)
	ctx := context.Background()

	logs, err := c.GetAllLogs(ctx)
	assert.Error(t, err)
	assert.Nil(t, logs)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

// ============================================================
// Tests for woodpeckerClient.GetLogsWithPrefix
// ============================================================

func TestWoodpeckerClient_GetLogsWithPrefix_Success(t *testing.T) {
	c, mockMeta, _, _ := newTestWoodpeckerClient(t)
	ctx := context.Background()

	expected := []string{"prefix-a", "prefix-b"}
	mockMeta.EXPECT().ListLogsWithPrefix(mock.Anything, "prefix-").Return(expected, nil).Once()

	logs, err := c.GetLogsWithPrefix(ctx, "prefix-")
	assert.NoError(t, err)
	assert.Equal(t, expected, logs)
}

func TestWoodpeckerClient_GetLogsWithPrefix_Closed(t *testing.T) {
	c, _, _, _ := newTestWoodpeckerClient(t)
	c.closeState.Store(true)
	ctx := context.Background()

	logs, err := c.GetLogsWithPrefix(ctx, "prefix-")
	assert.Error(t, err)
	assert.Nil(t, logs)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

// ============================================================
// Tests for woodpeckerClient.Close
// ============================================================

func TestWoodpeckerClient_Close_Success(t *testing.T) {
	c, mockMeta, mockPool, mockQuorum := newTestWoodpeckerClient(t)
	ctx := context.Background()

	mockMeta.EXPECT().Close().Return(nil).Once()
	mockPool.EXPECT().Close(mock.Anything).Return(nil).Once()
	mockQuorum.EXPECT().Close(mock.Anything).Return(nil).Once()

	err := c.Close(ctx)
	assert.NoError(t, err)
	assert.True(t, c.closeState.Load())
}

func TestWoodpeckerClient_Close_AlreadyClosed(t *testing.T) {
	c, _, _, _ := newTestWoodpeckerClient(t)
	c.closeState.Store(true)
	ctx := context.Background()

	err := c.Close(ctx)
	assert.Error(t, err)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

func TestWoodpeckerClient_Close_WithErrors(t *testing.T) {
	c, mockMeta, mockPool, mockQuorum := newTestWoodpeckerClient(t)
	ctx := context.Background()

	closeMetaErr := fmt.Errorf("meta close error")
	closePoolErr := fmt.Errorf("pool close error")
	mockMeta.EXPECT().Close().Return(closeMetaErr).Once()
	mockPool.EXPECT().Close(mock.Anything).Return(closePoolErr).Once()
	mockQuorum.EXPECT().Close(mock.Anything).Return(nil).Once()

	err := c.Close(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "meta close error")
	assert.Contains(t, err.Error(), "pool close error")
}

func TestWoodpeckerClient_Close_NilQuorumDiscovery(t *testing.T) {
	c, mockMeta, mockPool, _ := newTestWoodpeckerClient(t)
	c.quorumDiscovery = nil
	ctx := context.Background()

	mockMeta.EXPECT().Close().Return(nil).Once()
	mockPool.EXPECT().Close(mock.Anything).Return(nil).Once()

	err := c.Close(ctx)
	assert.NoError(t, err)
}

func TestWoodpeckerClient_Close_QuorumDiscoveryError(t *testing.T) {
	c, mockMeta, mockPool, mockQuorum := newTestWoodpeckerClient(t)
	ctx := context.Background()

	discoveryErr := fmt.Errorf("discovery close error")
	mockMeta.EXPECT().Close().Return(nil).Once()
	mockPool.EXPECT().Close(mock.Anything).Return(nil).Once()
	mockQuorum.EXPECT().Close(mock.Anything).Return(discoveryErr).Once()

	err := c.Close(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "discovery close error")
}

// ============================================================
// Tests for woodpeckerClient.initClient
// ============================================================

func TestWoodpeckerClient_InitClient_Success(t *testing.T) {
	c, mockMeta, _, _ := newTestWoodpeckerClient(t)
	ctx := context.Background()

	mockMeta.EXPECT().InitIfNecessary(mock.Anything).Return(nil).Once()

	err := c.initClient(ctx)
	assert.NoError(t, err)
}

func TestWoodpeckerClient_InitClient_Error(t *testing.T) {
	c, mockMeta, _, _ := newTestWoodpeckerClient(t)
	ctx := context.Background()

	initErr := fmt.Errorf("init failed")
	mockMeta.EXPECT().InitIfNecessary(mock.Anything).Return(initErr).Once()

	err := c.initClient(ctx)
	assert.Error(t, err)
	assert.True(t, werr.ErrWoodpeckerClientInitFailed.Is(err))
}

// ============================================================
// Tests for woodpeckerClient.initQuorumDiscovery
// ============================================================

func TestWoodpeckerClient_InitQuorumDiscovery_Success(t *testing.T) {
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				Quorum: config.QuorumConfig{
					BufferPools: []config.QuorumBufferPool{
						{Name: "default-pool", Seeds: []string{"node1:8888"}},
					},
					SelectStrategy: config.QuorumSelectStrategy{
						AffinityMode: "soft",
						Replicas:     3,
						Strategy:     "random",
					},
				},
			},
		},
	}
	c := &woodpeckerClient{
		cfg:        cfg,
		clientPool: mockPool,
	}

	err := c.initQuorumDiscovery(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, c.quorumDiscovery)
}

func TestWoodpeckerClient_InitQuorumDiscovery_InvalidAffinityMode(t *testing.T) {
	mockPool := mocks_logstore_client.NewLogStoreClientPool(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				Quorum: config.QuorumConfig{
					BufferPools: []config.QuorumBufferPool{
						{Name: "default-pool", Seeds: []string{"node1:8888"}},
					},
					SelectStrategy: config.QuorumSelectStrategy{
						AffinityMode: "invalid_mode",
						Replicas:     3,
						Strategy:     "random",
					},
				},
			},
		},
	}
	c := &woodpeckerClient{
		cfg:        cfg,
		clientPool: mockPool,
	}

	err := c.initQuorumDiscovery(context.Background())
	assert.Error(t, err)
	assert.Nil(t, c.quorumDiscovery)
}

// ============================================================
// Tests for woodpeckerClient.Close with managed etcd
// ============================================================

func TestWoodpeckerClient_Close_ManagedEtcdSuccess(t *testing.T) {
	c, mockMeta, mockPool, mockQuorum := newTestWoodpeckerClient(t)
	ctx := context.Background()

	// Create a real etcd client (lazy connection, no actual etcd needed)
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:12345"},
		DialTimeout: 1 * time.Millisecond,
	})
	require.NoError(t, err)

	c.managedCli = true
	c.etcdCli = etcdCli

	mockMeta.EXPECT().Close().Return(nil).Once()
	mockPool.EXPECT().Close(mock.Anything).Return(nil).Once()
	mockQuorum.EXPECT().Close(mock.Anything).Return(nil).Once()

	closeErr := c.Close(ctx)
	assert.NoError(t, closeErr)
}

// ============================================================
// Tests for NewClient
// ============================================================

func TestNewClient_InitQuorumDiscoveryError(t *testing.T) {
	ctx := context.Background()
	// Create a real etcd client (lazy connection, won't actually connect)
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:12345"},
		DialTimeout: 1 * time.Millisecond,
	})
	require.NoError(t, err)
	defer etcdCli.Close()

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				Quorum: config.QuorumConfig{
					BufferPools: []config.QuorumBufferPool{
						{Name: "default-pool", Seeds: []string{"node1:8888"}},
					},
					SelectStrategy: config.QuorumSelectStrategy{
						AffinityMode: "invalid_mode", // causes initQuorumDiscovery to fail
						Replicas:     3,
						Strategy:     "random",
					},
				},
			},
		},
		Trace: config.TraceConfig{Exporter: "noop"},
	}

	c, err := NewClient(ctx, cfg, etcdCli, false)
	assert.Error(t, err)
	assert.Nil(t, c)
	assert.True(t, werr.ErrWoodpeckerClientInitFailed.Is(err))
}

func TestNewClient_InitClientError(t *testing.T) {
	// Use a short-lived context so InitIfNecessary fails quickly
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:12345"},
		DialTimeout: 1 * time.Millisecond,
	})
	require.NoError(t, err)
	defer etcdCli.Close()

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				Quorum: config.QuorumConfig{
					BufferPools: []config.QuorumBufferPool{
						{Name: "default-pool", Seeds: []string{"node1:8888"}},
					},
					SelectStrategy: config.QuorumSelectStrategy{
						AffinityMode: "soft",
						Replicas:     3,
						Strategy:     "random",
					},
				},
			},
		},
		Trace: config.TraceConfig{Exporter: "noop"},
	}

	c, err := NewClient(ctx, cfg, etcdCli, false)
	assert.Error(t, err)
	assert.Nil(t, c)
	assert.True(t, werr.ErrWoodpeckerClientInitFailed.Is(err))
}

func TestWoodpeckerClient_Close_ManagedEtcdWithAllErrors(t *testing.T) {
	c, mockMeta, mockPool, mockQuorum := newTestWoodpeckerClient(t)
	ctx := context.Background()

	// Create a real etcd client and close it first to make Close() return an error
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:12345"},
		DialTimeout: 1 * time.Millisecond,
	})
	require.NoError(t, err)
	// Pre-close the client so second close returns error
	etcdCli.Close()

	c.managedCli = true
	c.etcdCli = etcdCli

	metaErr := fmt.Errorf("meta err")
	poolErr := fmt.Errorf("pool err")
	discoveryErr := fmt.Errorf("discovery err")
	mockMeta.EXPECT().Close().Return(metaErr).Once()
	mockPool.EXPECT().Close(mock.Anything).Return(poolErr).Once()
	mockQuorum.EXPECT().Close(mock.Anything).Return(discoveryErr).Once()

	closeErr := c.Close(ctx)
	assert.Error(t, closeErr)
}

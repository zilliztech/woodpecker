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
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/tracer"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server"
	"github.com/zilliztech/woodpecker/woodpecker/client"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// embedLogStore is the singleton of embedded logStore server
var (
	embedLogStoreMu   sync.Mutex
	embedLogStore     server.LogStore
	isLogStoreRunning bool
)

func startEmbedLogStore(cfg *config.Configuration, etcdCli *clientv3.Client, storageClient storageclient.ObjectStorage) (bool, error) {
	embedLogStoreMu.Lock()
	defer embedLogStoreMu.Unlock()

	if isLogStoreRunning && embedLogStore != nil {
		return false, nil
	}

	var takeControlOfClients bool = false
	embedLogStore = server.NewLogStore(context.Background(), cfg, etcdCli, storageClient)
	embedLogStore.SetAddress("127.0.0.1:59456") // TODO only placeholder now

	initError := embedLogStore.Start()
	if initError != nil {
		return false, initError
	}

	isLogStoreRunning = true
	takeControlOfClients = true
	return takeControlOfClients, nil
}

func StopEmbedLogStore() error {
	embedLogStoreMu.Lock()
	defer embedLogStoreMu.Unlock()

	if !isLogStoreRunning || embedLogStore == nil {
		return nil
	}

	stopErr := embedLogStore.Stop()
	if stopErr == nil {
		isLogStoreRunning = false
		embedLogStore = nil
	}

	return stopErr
}

var _ Client = (*woodpeckerEmbedClient)(nil)

// Implementation of the client interface for embed mode.
type woodpeckerEmbedClient struct {
	mu  sync.RWMutex
	cfg *config.Configuration

	Metadata   meta.MetadataProvider
	clientPool client.LogStoreClientPool

	managedCli    bool
	etcdCli       *clientv3.Client
	storageClient storageclient.ObjectStorage

	// close state
	closeState atomic.Bool
}

func NewEmbedClientFromConfig(ctx context.Context, config *config.Configuration) (Client, error) {
	logger.InitLogger(config)
	initTraceErr := tracer.InitTracer(config, "woodpecker", 1001)
	if initTraceErr != nil {
		logger.Ctx(ctx).Info("init tracer failed", zap.Error(initTraceErr))
	}
	if config.Woodpecker.Storage.IsStorageService() {
		return nil, werr.ErrOperationNotSupported.WithCauseErrMsg("embed client not support service storage mode")
	}
	etcdCli, err := etcd.GetRemoteEtcdClient(config.Etcd.GetEndpoints())
	if err != nil {
		return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErr(err)
	}
	var storageCli storageclient.ObjectStorage
	if config.Woodpecker.Storage.IsStorageMinio() {
		storageCli, err = storageclient.NewObjectStorage(ctx, config)
		if err != nil {
			return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErr(err)
		}
	}
	return NewEmbedClient(ctx, config, etcdCli, storageCli, true)
}

func NewEmbedClient(ctx context.Context, cfg *config.Configuration, etcdCli *clientv3.Client, storageClient storageclient.ObjectStorage, managed bool) (Client, error) {
	// init logger
	logger.InitLogger(cfg)
	if storageClient != nil {
		// Check if conditional write is supported.
		// For MinIO, the version must be v20240510 or later.
		// For cloud storage, the if-none-match feature is required.
		storageclient.CheckIfConditionWriteSupport(ctx, storageClient, cfg.Minio.BucketName, cfg.Minio.RootPath)
	}
	// start embedded logStore
	managedByLogStore, err := startEmbedLogStore(cfg, etcdCli, storageClient)
	if err != nil {
		return nil, err
	}
	if managedByLogStore && managed {
		managed = false
	}
	clientPool := client.NewLogStoreClientPoolLocal(embedLogStore)
	c := woodpeckerEmbedClient{
		cfg:           cfg,
		Metadata:      meta.NewMetadataProvider(ctx, etcdCli),
		clientPool:    clientPool,
		managedCli:    managed,
		etcdCli:       etcdCli,
		storageClient: storageClient,
	}
	c.closeState.Store(false)
	initErr := c.initClient(ctx)
	if initErr != nil {
		return nil, werr.ErrWoodpeckerClientInitFailed.WithCauseErr(initErr)
	}
	// Increment active connections metric
	metrics.WpClientActiveConnections.WithLabelValues("default").Inc()
	return &c, nil
}

func (c *woodpeckerEmbedClient) initClient(ctx context.Context) error {
	start := time.Now()
	initErr := c.Metadata.InitIfNecessary(ctx)
	if initErr != nil {
		metrics.WpClientOperationsTotal.WithLabelValues("init_client", "error").Inc()
		metrics.WpClientOperationLatency.WithLabelValues("init_client", "error").Observe(float64(time.Since(start).Milliseconds()))
		return werr.ErrWoodpeckerClientInitFailed.WithCauseErr(initErr)
	}
	metrics.WpClientOperationsTotal.WithLabelValues("init_client", "success").Inc()
	metrics.WpClientOperationLatency.WithLabelValues("init_client", "success").Observe(float64(time.Since(start).Milliseconds()))
	return nil
}

func (c *woodpeckerEmbedClient) GetMetadataProvider() meta.MetadataProvider {
	return c.Metadata
}

// CreateLog creates a new log with the specified name.
// It stores segment metadata for the new log.
func (c *woodpeckerEmbedClient) CreateLog(ctx context.Context, logName string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closeState.Load() {
		return werr.ErrWoodpeckerClientClosed
	}
	return createLogUnsafe(ctx, c.Metadata, logName)
}

// OpenLog opens an existing log with the specified name and returns a log handle.
// It retrieves the log metadata and segments metadata, then creates a new log handle.
func (c *woodpeckerEmbedClient) OpenLog(ctx context.Context, logName string) (log.LogHandle, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeState.Load() {
		return nil, werr.ErrWoodpeckerClientClosed
	}
	return openLogUnsafe(ctx, c.Metadata, logName, c.clientPool, c.cfg, c.SelectQuorumNodes)
}

func (c *woodpeckerEmbedClient) SelectQuorumNodes(ctx context.Context) (*proto.QuorumInfo, error) {
	return &proto.QuorumInfo{
		Id:    -1,
		Wq:    1,
		Aq:    1,
		Es:    1,
		Nodes: []string{"127.0.0.1:59456"},
	}, nil
}

// DeleteLog deletes the log with the specified name.
// It should remove the log and its associated metadata.
func (c *woodpeckerEmbedClient) DeleteLog(ctx context.Context, logName string) error {
	// This is just a stub - you'll need to implement this
	return werr.ErrOperationNotSupported.WithCauseErrMsg("delete log is not supported currently")
}

// LogExists checks if a log with the specified name exists.
// It returns true if the log exists, otherwise false.
func (c *woodpeckerEmbedClient) LogExists(ctx context.Context, logName string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closeState.Load() {
		return false, werr.ErrWoodpeckerClientClosed
	}
	return logExistsUnsafe(ctx, c.Metadata, logName)
}

// GetAllLogs retrieves all log names.
// It returns a slice containing all log names.
func (c *woodpeckerEmbedClient) GetAllLogs(ctx context.Context) ([]string, error) {
	if c.closeState.Load() {
		return nil, werr.ErrWoodpeckerClientClosed
	}
	return getAllLogsUnsafe(ctx, c.Metadata)
}

// GetLogsWithPrefix retrieves log names that start with the specified prefix.
// It returns a slice containing log names that match the prefix.
func (c *woodpeckerEmbedClient) GetLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error) {
	if c.closeState.Load() {
		return nil, werr.ErrWoodpeckerClientClosed
	}
	return getLogsWithPrefix(ctx, c.Metadata, logNamePrefix)
}

func (c *woodpeckerEmbedClient) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closeState.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Info("client already closed, skip")
		return werr.ErrWoodpeckerClientClosed
	}

	// Decrement active connections metric
	metrics.WpClientActiveConnections.WithLabelValues("default").Dec()
	closeErr := c.Metadata.Close()
	if closeErr != nil {
		logger.Ctx(ctx).Info("close metadata failed", zap.Error(closeErr))
	}
	closePoolErr := c.clientPool.Close(ctx)
	if closePoolErr != nil {
		logger.Ctx(ctx).Info("close client pool failed", zap.Error(closePoolErr))
	}
	if c.managedCli {
		closeEtcdCliErr := c.etcdCli.Close()
		if closeEtcdCliErr != nil {
			logger.Ctx(ctx).Info("close client etcd client failed", zap.Error(closeEtcdCliErr))
			return werr.Combine(closeErr, closePoolErr, closeEtcdCliErr)
		}
	}
	return werr.Combine(closeErr, closePoolErr)
}

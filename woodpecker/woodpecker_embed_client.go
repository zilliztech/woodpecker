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
	"github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/tracer"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
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

func startEmbedLogStore(cfg *config.Configuration, etcdCli *clientv3.Client, minioCli minio.MinioHandler) (bool, error) {
	embedLogStoreMu.Lock()
	defer embedLogStoreMu.Unlock()

	if isLogStoreRunning && embedLogStore != nil {
		return false, nil
	}

	var takeControlOfClients bool = false
	embedLogStore = server.NewLogStore(context.Background(), cfg, etcdCli, minioCli)
	embedLogStore.SetAddress("127.0.0.1:59456") // TODO only placeholder now

	initError := embedLogStore.Start()
	if initError != nil {
		return takeControlOfClients, initError
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

// Implementation of the client interface for Z'eembed mode.
type woodpeckerEmbedClient struct {
	mu  sync.RWMutex
	cfg *config.Configuration

	Metadata   meta.MetadataProvider
	clientPool client.LogStoreClientPool
	logHandles map[string]log.LogHandle

	managedCli bool
	etcdCli    *clientv3.Client
	minioCli   minio.MinioHandler

	// close state
	closeState atomic.Bool
}

func NewEmbedClientFromConfig(ctx context.Context, config *config.Configuration) (Client, error) {
	logger.InitLogger(config)
	initTraceErr := tracer.InitTracer(config, "woodpecker", 1001)
	if initTraceErr != nil {
		logger.Ctx(ctx).Info("init tracer failed", zap.Error(initTraceErr))
	}
	etcdCli, err := etcd.GetRemoteEtcdClient(config.Etcd.GetEndpoints())
	if err != nil {
		return nil, werr.ErrCreateConnection.WithCauseErr(err)
	}
	var minioCli minio.MinioHandler
	if config.Woodpecker.Storage.IsStorageMinio() {
		minioCli, err = minio.NewMinioHandler(ctx, config)
		if err != nil {
			return nil, werr.ErrCreateConnection.WithCauseErr(err)
		}
	}
	return NewEmbedClient(ctx, config, etcdCli, minioCli, true)
}

func NewEmbedClient(ctx context.Context, cfg *config.Configuration, etcdCli *clientv3.Client, minioCli minio.MinioHandler, managed bool) (Client, error) {
	// init logger
	logger.InitLogger(cfg)
	if minioCli != nil {
		// check if condition write support
		minio.CheckIfConditionWriteSupport(ctx, minioCli, cfg.Minio.BucketName, cfg.Minio.RootPath)
	}
	// start embedded logStore
	managedByLogStore, err := startEmbedLogStore(cfg, etcdCli, minioCli)
	if err != nil {
		return nil, err
	}
	if managedByLogStore && managed {
		managed = false
	}
	clientPool := client.NewLogStoreClientPoolLocal(embedLogStore)
	c := woodpeckerEmbedClient{
		cfg:        cfg,
		Metadata:   meta.NewMetadataProvider(ctx, etcdCli),
		clientPool: clientPool,
		logHandles: make(map[string]log.LogHandle),
		managedCli: managed,
		etcdCli:    etcdCli,
		minioCli:   minioCli,
	}
	c.closeState.Store(false)
	initErr := c.initClient(ctx)
	if initErr != nil {
		return nil, werr.ErrInitClient.WithCauseErr(initErr)
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
		return werr.ErrInitClient.WithCauseErr(initErr)
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
	start := time.Now()
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closeState.Load() {
		return werr.ErrClientClosed
	}
	// early return if cache exists
	if _, exists := c.logHandles[logName]; exists {
		metrics.WpClientOperationsTotal.WithLabelValues("create_log", "error").Inc()
		metrics.WpClientOperationLatency.WithLabelValues("create_log", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("create log failed, log already exists", zap.String("logName", logName))
		return werr.ErrLogAlreadyExists
	}

	// otherwise try create new log
	// Add retry logic for CreateLog when encountering ErrCreateLogMetadataTxn
	const maxRetries = 5
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		// Try to create the log
		err := c.Metadata.CreateLog(ctx, logName)
		if err == nil {
			// Success, return immediately
			metrics.WpClientOperationsTotal.WithLabelValues("create_log", "success").Inc()
			metrics.WpClientOperationLatency.WithLabelValues("create_log", "success").Observe(float64(time.Since(start).Milliseconds()))
			return nil
		}

		// Check if the error is txn (ErrCreateLogMetadataTxn)
		if werr.ErrCreateLogMetadataTxn.Is(err) {
			// auto retry
			logger.Ctx(ctx).Info("Retrying create log due to transaction conflict",
				zap.String("logName", logName),
				zap.Int("attempt", i+1),
				zap.Int("maxRetries", maxRetries),
				zap.Error(err))

			// Keep the last error for potential return
			lastErr = err
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// For non-txn errors or final attempt, return the error directly
		metrics.WpClientOperationsTotal.WithLabelValues("create_log", "error").Inc()
		metrics.WpClientOperationLatency.WithLabelValues("create_log", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("create log failed", zap.String("logName", logName), zap.Error(err))
		return err
	}

	// If we exhausted all retries, return the last error
	metrics.WpClientOperationsTotal.WithLabelValues("create_log", "error").Inc()
	metrics.WpClientOperationLatency.WithLabelValues("create_log", "error").Observe(float64(time.Since(start).Milliseconds()))
	return lastErr
}

// OpenLog opens an existing log with the specified name and returns a log handle.
// It retrieves the log metadata and segments metadata, then creates a new log handle.
func (c *woodpeckerEmbedClient) OpenLog(ctx context.Context, logName string) (log.LogHandle, error) {
	start := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeState.Load() {
		return nil, werr.ErrClientClosed
	}
	// early return if cache exists
	if logHandle, exists := c.logHandles[logName]; exists {
		metrics.WpClientOperationsTotal.WithLabelValues("open_log", "cache_hit").Inc()
		metrics.WpClientOperationLatency.WithLabelValues("open_log", "cache_hit").Observe(float64(time.Since(start).Milliseconds()))
		return logHandle, nil
	}
	// Open log and retrieve metadata with detailed comments
	logMeta, segmentsMeta, err := c.Metadata.OpenLog(ctx, logName)
	if err != nil {
		metrics.WpClientOperationsTotal.WithLabelValues("open_log", "error").Inc()
		metrics.WpClientOperationLatency.WithLabelValues("open_log", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("open log failed", zap.String("logName", logName), zap.Error(err))
		return nil, err
	}
	newLogHandle := log.NewLogHandle(logName, logMeta.GetLogId(), segmentsMeta, c.GetMetadataProvider(), c.clientPool, c.cfg)
	c.logHandles[logName] = newLogHandle
	metrics.WpClientOperationsTotal.WithLabelValues("open_log", "success").Inc()
	metrics.WpClientOperationLatency.WithLabelValues("open_log", "success").Observe(float64(time.Since(start).Milliseconds()))
	metrics.WpLogNameIdMapping.WithLabelValues(logName).Set(float64(logMeta.GetLogId()))
	return newLogHandle, nil
}

// DeleteLog deletes the log with the specified name.
// It should remove the log and its associated metadata.
func (c *woodpeckerEmbedClient) DeleteLog(ctx context.Context, logName string) error {
	// This is just a stub - you'll need to implement this
	panic("implement me")
}

// LogExists checks if a log with the specified name exists.
// It returns true if the log exists, otherwise false.
func (c *woodpeckerEmbedClient) LogExists(ctx context.Context, logName string) (bool, error) {
	start := time.Now()
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closeState.Load() {
		return false, werr.ErrClientClosed
	}
	// early return if cache exists
	if _, exists := c.logHandles[logName]; exists {
		metrics.WpClientOperationsTotal.WithLabelValues("log_exists", "cache_hit").Inc()
		metrics.WpClientOperationLatency.WithLabelValues("log_exists", "cache_hit").Observe(float64(time.Since(start).Milliseconds()))
		return true, nil
	}
	// Check if log exists in meta
	exists, err := c.Metadata.CheckExists(ctx, logName)
	if err != nil {
		metrics.WpClientOperationsTotal.WithLabelValues("log_exists", "error").Inc()
		metrics.WpClientOperationLatency.WithLabelValues("log_exists", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("check log exists failed", zap.String("logName", logName), zap.Error(err))
	} else {
		status := "found"
		if !exists {
			status = "not_found"
		}
		metrics.WpClientOperationsTotal.WithLabelValues("log_exists", status).Inc()
		metrics.WpClientOperationLatency.WithLabelValues("log_exists", status).Observe(float64(time.Since(start).Milliseconds()))
	}
	return exists, err
}

// GetAllLogs retrieves all log names.
// It returns a slice containing all log names.
func (c *woodpeckerEmbedClient) GetAllLogs(ctx context.Context) ([]string, error) {
	if c.closeState.Load() {
		return nil, werr.ErrClientClosed
	}
	start := time.Now()
	// Retrieve all logs with detailed comments
	logs, err := c.Metadata.ListLogs(ctx)
	if err != nil {
		metrics.WpClientOperationsTotal.WithLabelValues("get_all_logs", "error").Inc()
		metrics.WpClientOperationLatency.WithLabelValues("get_all_logs", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("get all logs failed", zap.Error(err))
	} else {
		metrics.WpClientOperationsTotal.WithLabelValues("get_all_logs", "success").Inc()
		metrics.WpClientOperationLatency.WithLabelValues("get_all_logs", "success").Observe(float64(time.Since(start).Milliseconds()))
	}
	return logs, err
}

// GetLogsWithPrefix retrieves log names that start with the specified prefix.
// It returns a slice containing log names that match the prefix.
func (c *woodpeckerEmbedClient) GetLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error) {
	if c.closeState.Load() {
		return nil, werr.ErrClientClosed
	}
	start := time.Now()
	// Retrieve logs with the given prefix with detailed comments
	logs, err := c.Metadata.ListLogsWithPrefix(ctx, logNamePrefix)
	if err != nil {
		metrics.WpClientOperationsTotal.WithLabelValues("get_logs_with_prefix", "error").Inc()
		metrics.WpClientOperationLatency.WithLabelValues("get_logs_with_prefix", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("get all logs with prefix failed", zap.Error(err))
	} else {
		metrics.WpClientOperationsTotal.WithLabelValues("get_logs_with_prefix", "success").Inc()
		metrics.WpClientOperationLatency.WithLabelValues("get_logs_with_prefix", "success").Observe(float64(time.Since(start).Milliseconds()))
	}
	return logs, err
}

func (c *woodpeckerEmbedClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closeState.CompareAndSwap(false, true) {
		logger.Ctx(context.TODO()).Info("client already closed, skip")
		return werr.ErrClientClosed
	}

	// close all logHandle
	for _, logHandle := range c.logHandles {
		logHandle.Close(context.Background())
	}

	// Decrement active connections metric
	metrics.WpClientActiveConnections.WithLabelValues("default").Dec()
	closeErr := c.Metadata.Close()
	if closeErr != nil {
		logger.Ctx(context.TODO()).Info("close metadata failed", zap.Error(closeErr))
	}
	closePoolErr := c.clientPool.Close()
	if closePoolErr != nil {
		logger.Ctx(context.TODO()).Info("close client pool failed", zap.Error(closePoolErr))
	}
	if c.managedCli {
		closeEtcdCliErr := c.etcdCli.Close()
		if closeEtcdCliErr != nil {
			logger.Ctx(context.TODO()).Info("close client etcd client failed", zap.Error(closeEtcdCliErr))
		}
		return werr.Combine(closeErr, closePoolErr, closeEtcdCliErr)
	}
	return werr.Combine(closeErr, closePoolErr)
}

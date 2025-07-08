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

	"go.uber.org/zap"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/woodpecker/client"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

type Client interface {
	// CreateLog creates a new log with the specified name.
	CreateLog(context.Context, string) error
	// OpenLog opens an existing log with the specified name and returns a log handle.
	OpenLog(context.Context, string) (log.LogHandle, error)
	// DeleteLog deletes the log with the specified name.
	DeleteLog(context.Context, string) error
	// LogExists checks if a log with the specified name exists.
	LogExists(context.Context, string) (bool, error)
	// GetAllLogs retrieves all log names.
	GetAllLogs(context.Context) ([]string, error)
	// GetLogsWithPrefix retrieves log names that start with the specified prefix.
	GetLogsWithPrefix(context.Context, string) ([]string, error)
	// GetMetadataProvider returns the metadata provider associated with the client.
	GetMetadataProvider() meta.MetadataProvider
	// Close closes the client.
	Close(context.Context) error
}

var _ Client = (*woodpeckerClient)(nil)

// Implementation of the client interface for Distributed mode.
type woodpeckerClient struct {
	mu         sync.RWMutex
	cfg        *config.Configuration
	Metadata   meta.MetadataProvider
	clientPool client.LogStoreClientPool
	logHandles map[string]log.LogHandle

	// close state
	closeState atomic.Bool
}

func NewClient(ctx context.Context, etcdClient *clientv3.Client, cfg *config.Configuration) (Client, error) {
	clientPool := client.NewLogStoreClientPool()
	c := &woodpeckerClient{
		cfg:        cfg,
		Metadata:   meta.NewMetadataProvider(ctx, etcdClient),
		clientPool: clientPool,
		logHandles: make(map[string]log.LogHandle),
	}
	err := c.initClient(ctx)
	if err != nil {
		return nil, werr.ErrInitClient.WithCauseErr(err)
	}
	// Increment active connections metric
	metrics.WpClientActiveConnections.WithLabelValues("default").Inc() // TODO use local addr as label
	return c, nil
}

func (c *woodpeckerClient) initClient(ctx context.Context) error {
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

func (c *woodpeckerClient) GetMetadataProvider() meta.MetadataProvider {
	return c.Metadata
}

// CreateLog creates a new log with the specified name.
func (c *woodpeckerClient) CreateLog(ctx context.Context, logName string) error {
	if c.closeState.Load() {
		return werr.ErrClientClosed
	}
	start := time.Now()
	c.mu.RLock()
	defer c.mu.RUnlock()
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
func (c *woodpeckerClient) OpenLog(ctx context.Context, logName string) (log.LogHandle, error) {
	if c.closeState.Load() {
		return nil, werr.ErrClientClosed
	}
	start := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
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
	return newLogHandle, nil
}

// DeleteLog deletes the log with the specified name.
func (c *woodpeckerClient) DeleteLog(ctx context.Context, logName string) error {
	// Implement the DeleteLog method
	// This is just a stub - you'll need to implement this
	panic("implement me")
}

// LogExists checks if a log with the specified name exists.
func (c *woodpeckerClient) LogExists(ctx context.Context, logName string) (bool, error) {
	if c.closeState.Load() {
		return false, werr.ErrClientClosed
	}
	start := time.Now()
	c.mu.RLock()
	defer c.mu.RUnlock()
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
func (c *woodpeckerClient) GetAllLogs(ctx context.Context) ([]string, error) {
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
func (c *woodpeckerClient) GetLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error) {
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

func (c *woodpeckerClient) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closeState.CompareAndSwap(false, true) {
		logger.Ctx(ctx).Info("client already closed, skip")
		return werr.ErrClientClosed
	}

	// close all logHandle
	for _, logHandle := range c.logHandles {
		logHandle.Close(ctx)
	}
	// Decrement active connections metric
	metrics.WpClientActiveConnections.WithLabelValues("default").Dec()
	closeErr := c.Metadata.Close()
	if closeErr != nil {
		logger.Ctx(ctx).Info("close metadata failed", zap.Error(closeErr))
	}
	closePoolErr := c.clientPool.Close()
	if closePoolErr != nil {
		logger.Ctx(ctx).Info("close client pool failed", zap.Error(closePoolErr))
	}
	return werr.Combine(closeErr, closePoolErr)
}

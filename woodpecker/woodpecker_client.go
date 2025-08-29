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
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/membership"
	"github.com/zilliztech/woodpecker/common/metrics"
	netutil "github.com/zilliztech/woodpecker/common/net"
	"github.com/zilliztech/woodpecker/common/retry"
	"github.com/zilliztech/woodpecker/common/tracer"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/client"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

type Client interface {
	// CreateLog creates a new log with the specified name.
	CreateLog(ctx context.Context, logName string) error
	// OpenLog opens an existing log with the specified name and returns a log handle.
	OpenLog(ctx context.Context, logName string) (log.LogHandle, error)
	// DeleteLog deletes the log with the specified name.
	DeleteLog(ctx context.Context, logName string) error
	// LogExists checks if a log with the specified name exists.
	LogExists(ctx context.Context, logName string) (bool, error)
	// GetAllLogs retrieves all log names.
	GetAllLogs(ctx context.Context) ([]string, error)
	// GetLogsWithPrefix retrieves log names that start with the specified prefix.
	GetLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error)
	// GetMetadataProvider returns the metadata provider associated with the client.
	GetMetadataProvider() meta.MetadataProvider
	// Close closes the client.
	Close(ctx context.Context) error
}

var _ Client = (*woodpeckerClient)(nil)

// Implementation of the client interface for distributed service mode.
type woodpeckerClient struct {
	mu         sync.RWMutex
	cfg        *config.Configuration
	Metadata   meta.MetadataProvider
	clientPool client.LogStoreClientPool

	// managed cli
	managedCli bool
	etcdCli    *clientv3.Client

	// node for service discovery
	clientNode *membership.ClientNode

	// close state
	closeState atomic.Bool
}

func NewClient(ctx context.Context, cfg *config.Configuration, etcdClient *clientv3.Client, managed bool) (Client, error) {
	logger.InitLogger(cfg)
	initTraceErr := tracer.InitTracer(cfg, "woodpecker", 1001)
	if initTraceErr != nil {
		logger.Ctx(ctx).Info("init tracer failed", zap.Error(initTraceErr))
	}
	clientPool := client.NewLogStoreClientPool()
	c := &woodpeckerClient{
		cfg:        cfg,
		Metadata:   meta.NewMetadataProvider(ctx, etcdClient),
		clientPool: clientPool,
		managedCli: managed,
		etcdCli:    etcdClient,
	}
	c.closeState.Store(false)
	// join to discover nodes
	clientConfig := &membership.ClientConfig{
		NodeID:   fmt.Sprintf("C%d", time.Now().Nanosecond()),
		BindAddr: netutil.GetLocalIP(),
		BindPort: 0,
	}
	clientNode, newNodeErr := membership.NewClientNode(clientConfig)
	if newNodeErr != nil {
		return nil, werr.ErrWoodpeckerClientInitFailed.WithCauseErr(newNodeErr)
	}
	c.clientNode = clientNode
	seeds := cfg.Woodpecker.Client.ServiceSeedNodes
	seedList := strings.Split(seeds, ",")
	joinErr := clientNode.Join(seedList)
	time.Sleep(2 * time.Second)
	if joinErr != nil {
		return nil, werr.ErrWoodpeckerClientInitFailed.WithCauseErr(joinErr)
	}
	// init if necessary
	err := c.initClient(ctx)
	if err != nil {
		return nil, werr.ErrWoodpeckerClientInitFailed.WithCauseErr(err)
	}
	// Increment active connections metric
	metrics.WpClientActiveConnections.WithLabelValues(fmt.Sprintf("%s", netutil.GetLocalIP())).Inc()
	return c, nil
}

func (c *woodpeckerClient) initClient(ctx context.Context) error {
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

func (c *woodpeckerClient) GetMetadataProvider() meta.MetadataProvider {
	return c.Metadata
}

// CreateLog creates a new log with the specified name.
func (c *woodpeckerClient) CreateLog(ctx context.Context, logName string) error {
	if c.closeState.Load() {
		return werr.ErrWoodpeckerClientClosed
	}
	start := time.Now()
	c.mu.RLock()
	defer c.mu.RUnlock()
	// try create new log
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

		// Check if the error is txn (ErrMetadataCreateLogTxn)
		if werr.ErrMetadataCreateLogTxn.Is(err) {
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
		return nil, werr.ErrWoodpeckerClientClosed
	}
	start := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	// Open log and retrieve metadata with detailed comments
	logMeta, segmentsMeta, err := c.Metadata.OpenLog(ctx, logName)
	if err != nil {
		metrics.WpClientOperationsTotal.WithLabelValues("open_log", "error").Inc()
		metrics.WpClientOperationLatency.WithLabelValues("open_log", "error").Observe(float64(time.Since(start).Milliseconds()))
		logger.Ctx(ctx).Warn("open log failed", zap.String("logName", logName), zap.Error(err))
		return nil, err
	}
	newLogHandle := log.NewLogHandle(logName, logMeta.Metadata.GetLogId(), segmentsMeta, c.GetMetadataProvider(), c.clientPool, c.cfg, c.SelectQuorumNodes)
	metrics.WpClientOperationsTotal.WithLabelValues("open_log", "success").Inc()
	metrics.WpClientOperationLatency.WithLabelValues("open_log", "success").Observe(float64(time.Since(start).Milliseconds()))
	metrics.WpLogNameIdMapping.WithLabelValues(logName).Set(float64(logMeta.Metadata.GetLogId()))
	return newLogHandle, nil
}

func (c *woodpeckerClient) SelectQuorumNodes(ctx context.Context) (*proto.QuorumInfo, error) {
	// TODO currently we only support 3 nodes quorum, we will support more nodes quorum in the future, which will be configurable
	const requiredNodes = 3
	const ackNodes = 2

	logger.Ctx(ctx).Info("Starting quorum node selection", zap.Int("requiredNodes", requiredNodes))

	var result *proto.QuorumInfo

	err := retry.Do(ctx, func() error {
		// Get all available servers from service discovery
		discovery := c.clientNode.GetDiscovery()
		allServersMap := discovery.GetAllServers()

		// Convert map to slice
		allServers := make([]*membership.ServerMeta, 0, len(allServersMap))
		for _, server := range allServersMap {
			allServers = append(allServers, server)
		}

		if len(allServers) >= requiredNodes {
			// Randomly select 3 nodes
			selectedNodes := c.randomSelectNodes(allServers, requiredNodes)

			// Convert to endpoint addresses
			nodeEndpoints := make([]string, len(selectedNodes))
			for i, server := range selectedNodes {
				nodeEndpoints[i] = server.Endpoint
			}

			// Atomically create quorum info with the selected nodes
			result = &proto.QuorumInfo{
				Id:    1,
				Nodes: nodeEndpoints,
				Aq:    ackNodes,
				Wq:    int32(requiredNodes),
				Es:    int32(requiredNodes),
			}
			return nil
		}

		// Not enough nodes, return error to trigger retry
		logger.Ctx(ctx).Info("Insufficient nodes for quorum, waiting for more nodes",
			zap.Int("available", len(allServers)),
			zap.Int("required", requiredNodes))

		return werr.ErrWoodpeckerClientConnectionFailed.WithCauseErrMsg("insufficient nodes for quorum selection")
	}, retry.AttemptAlways(), retry.Sleep(200*time.Millisecond), retry.MaxSleepTime(2*time.Second))

	if err != nil {
		return nil, werr.ErrWoodpeckerClientClosed.WithCauseErr(err)
	}

	return result, nil
}

// DeleteLog deletes the log with the specified name.
func (c *woodpeckerClient) DeleteLog(ctx context.Context, logName string) error {
	// Implement the DeleteLog method
	// This is just a stub - you'll need to implement this
	return werr.ErrOperationNotSupported.WithCauseErrMsg("delete log is not supported currently")
}

// LogExists checks if a log with the specified name exists.
func (c *woodpeckerClient) LogExists(ctx context.Context, logName string) (bool, error) {
	start := time.Now()
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closeState.Load() {
		return false, werr.ErrWoodpeckerClientClosed
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
		return nil, werr.ErrWoodpeckerClientClosed
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
		return nil, werr.ErrWoodpeckerClientClosed
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

// randomSelectNodes randomly selects n nodes from the available servers
func (c *woodpeckerClient) randomSelectNodes(servers []*membership.ServerMeta, n int) []*membership.ServerMeta {
	if len(servers) <= n {
		return servers
	}

	// Create a copy to avoid modifying the original slice
	serversCopy := make([]*membership.ServerMeta, len(servers))
	copy(serversCopy, servers)

	// Fisher-Yates shuffle algorithm to randomly select n nodes
	for i := len(serversCopy) - 1; i > len(serversCopy)-n-1; i-- {
		j := rand.Intn(i + 1)
		serversCopy[i], serversCopy[j] = serversCopy[j], serversCopy[i]
	}

	// Return the last n elements (which are randomly selected)
	return serversCopy[len(serversCopy)-n:]
}

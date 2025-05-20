package woodpecker

import (
	"context"
	"io"
	"sync"
	"time"

	"go.uber.org/zap"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/server/client"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

type Client interface {
	io.Closer
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
}

var _ Client = (*woodpeckerClient)(nil)

// Implementation of the client interface for Distributed mode.
type woodpeckerClient struct {
	mu         sync.RWMutex
	cfg        *config.Configuration
	Metadata   meta.MetadataProvider
	clientPool client.LogStoreClientPool
	logHandles map[string]log.LogHandle
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
	metrics.WpClientActiveConnections.WithLabelValues("all").Inc()
	return c, nil
}

func (c *woodpeckerClient) initClient(ctx context.Context) error {
	return c.Metadata.InitIfNecessary(ctx)
}

func (c *woodpeckerClient) GetMetadataProvider() meta.MetadataProvider {
	return c.Metadata
}

// CreateLog creates a new log with the specified name.
func (c *woodpeckerClient) CreateLog(ctx context.Context, logName string) error {
	start := time.Now()
	c.mu.RLock()
	defer c.mu.RUnlock()
	// early return if cache exists
	if _, exists := c.logHandles[logName]; exists {
		metrics.WpClientOperationsTotal.WithLabelValues(logName, "create_log", "already_exists").Inc()
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
			metrics.WpClientOperationsTotal.WithLabelValues(logName, "create_log", "success").Inc()
			metrics.WpClientOperationLatency.WithLabelValues(logName, "create_log").Observe(float64(time.Since(start).Milliseconds()))
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
		metrics.WpClientOperationsTotal.WithLabelValues(logName, "create_log", "error").Inc()
		metrics.WpClientOperationLatency.WithLabelValues(logName, "create_log").Observe(float64(time.Since(start).Milliseconds()))
		return err
	}

	// If we exhausted all retries, return the last error
	metrics.WpClientOperationsTotal.WithLabelValues(logName, "create_log", "retry_exhausted").Inc()
	metrics.WpClientOperationLatency.WithLabelValues(logName, "create_log").Observe(float64(time.Since(start).Milliseconds()))
	return lastErr
}

// OpenLog opens an existing log with the specified name and returns a log handle.
func (c *woodpeckerClient) OpenLog(ctx context.Context, logName string) (log.LogHandle, error) {
	start := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	// early return if cache exists
	if logHandle, exists := c.logHandles[logName]; exists {
		metrics.WpClientOperationsTotal.WithLabelValues(logName, "open_log", "cache_hit").Inc()
		metrics.WpClientOperationLatency.WithLabelValues(logName, "open_log").Observe(float64(time.Since(start).Milliseconds()))
		return logHandle, nil
	}
	// Open log and retrieve metadata with detailed comments
	logMeta, segmentsMeta, err := c.Metadata.OpenLog(ctx, logName)
	if err != nil {
		metrics.WpClientOperationsTotal.WithLabelValues(logName, "open_log", "error").Inc()
		metrics.WpClientOperationLatency.WithLabelValues(logName, "open_log").Observe(float64(time.Since(start).Milliseconds()))
		return nil, err
	}
	newLogHandle := log.NewLogHandle(logName, logMeta.GetLogId(), segmentsMeta, c.GetMetadataProvider(), c.clientPool, c.cfg)
	c.logHandles[logName] = newLogHandle
	metrics.WpClientOperationsTotal.WithLabelValues(logName, "open_log", "success").Inc()
	metrics.WpClientOperationLatency.WithLabelValues(logName, "open_log").Observe(float64(time.Since(start).Milliseconds()))
	return newLogHandle, nil
}

// DeleteLog deletes the log with the specified name.
func (c *woodpeckerClient) DeleteLog(ctx context.Context, logName string) error {
	// Implement the DeleteLog method
	// This is just a stub - you'll need to implement this
	metrics.WpClientOperationsTotal.WithLabelValues(logName, "delete_log", "not_implemented").Inc()
	panic("implement me")
}

// LogExists checks if a log with the specified name exists.
func (c *woodpeckerClient) LogExists(ctx context.Context, logName string) (bool, error) {
	start := time.Now()
	c.mu.RLock()
	defer c.mu.RUnlock()
	// early return if cache exists
	if _, exists := c.logHandles[logName]; exists {
		metrics.WpClientOperationsTotal.WithLabelValues(logName, "log_exists", "cache_hit").Inc()
		metrics.WpClientOperationLatency.WithLabelValues(logName, "log_exists").Observe(float64(time.Since(start).Milliseconds()))
		return true, nil
	}
	// Check if log exists in meta
	exists, err := c.Metadata.CheckExists(ctx, logName)
	if err != nil {
		metrics.WpClientOperationsTotal.WithLabelValues(logName, "log_exists", "error").Inc()
	} else {
		status := "found"
		if !exists {
			status = "not_found"
		}
		metrics.WpClientOperationsTotal.WithLabelValues(logName, "log_exists", status).Inc()
	}
	metrics.WpClientOperationLatency.WithLabelValues(logName, "log_exists").Observe(float64(time.Since(start).Milliseconds()))
	return exists, err
}

// GetAllLogs retrieves all log names.
func (c *woodpeckerClient) GetAllLogs(ctx context.Context) ([]string, error) {
	start := time.Now()
	// Retrieve all logs with detailed comments
	logs, err := c.Metadata.ListLogs(ctx)
	if err != nil {
		metrics.WpClientOperationsTotal.WithLabelValues("all", "get_all_logs", "error").Inc()
	} else {
		metrics.WpClientOperationsTotal.WithLabelValues("all", "get_all_logs", "success").Inc()
	}
	metrics.WpClientOperationLatency.WithLabelValues("all", "get_all_logs").Observe(float64(time.Since(start).Milliseconds()))
	return logs, err
}

// GetLogsWithPrefix retrieves log names that start with the specified prefix.
func (c *woodpeckerClient) GetLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error) {
	start := time.Now()
	// Retrieve logs with the given prefix with detailed comments
	logs, err := c.Metadata.ListLogsWithPrefix(ctx, logNamePrefix)
	if err != nil {
		metrics.WpClientOperationsTotal.WithLabelValues(logNamePrefix, "get_logs_with_prefix", "error").Inc()
	} else {
		metrics.WpClientOperationsTotal.WithLabelValues(logNamePrefix, "get_logs_with_prefix", "success").Inc()
	}
	metrics.WpClientOperationLatency.WithLabelValues(logNamePrefix, "get_logs_with_prefix").Observe(float64(time.Since(start).Milliseconds()))
	return logs, err
}

func (c *woodpeckerClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Decrement active connections metric
	metrics.WpClientActiveConnections.WithLabelValues("all").Dec()
	closeErr := c.Metadata.Close()
	closePoolErr := c.clientPool.Close()
	return werr.Combine(closeErr, closePoolErr)
}

package woodpecker

import (
	"context"
	"io"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/config"
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
	c.mu.RLock()
	defer c.mu.RUnlock()
	// early return if cache exists
	if _, exists := c.logHandles[logName]; exists {
		return werr.ErrLogAlreadyExists
	}
	// otherwise try create new log
	return c.Metadata.CreateLog(ctx, logName)
}

// OpenLog opens an existing log with the specified name and returns a log handle.
func (c *woodpeckerClient) OpenLog(ctx context.Context, logName string) (log.LogHandle, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// early return if cache exists
	if logHandle, exists := c.logHandles[logName]; exists {
		return logHandle, nil
	}
	// Open log and retrieve metadata with detailed comments
	logMeta, segmentsMeta, err := c.Metadata.OpenLog(ctx, logName)
	if err != nil {
		return nil, err
	}
	newLogHandle := log.NewLogHandle(logName, logMeta.GetLogId(), segmentsMeta, c.GetMetadataProvider(), c.clientPool, c.cfg)
	c.logHandles[logName] = newLogHandle
	return newLogHandle, nil
}

// DeleteLog deletes the log with the specified name.
func (c *woodpeckerClient) DeleteLog(ctx context.Context, logName string) error {
	// Implement the DeleteLog method
	panic("implement me")
}

// LogExists checks if a log with the specified name exists.
func (c *woodpeckerClient) LogExists(ctx context.Context, logName string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// early return if cache exists
	if _, exists := c.logHandles[logName]; exists {
		return true, nil
	}
	// Check if log exists in meta
	return c.Metadata.CheckExists(ctx, logName)
}

// GetAllLogs retrieves all log names.
func (c *woodpeckerClient) GetAllLogs(ctx context.Context) ([]string, error) {
	// Retrieve all logs with detailed comments
	return c.Metadata.ListLogs(ctx)
}

// GetLogsWithPrefix retrieves log names that start with the specified prefix.
func (c *woodpeckerClient) GetLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error) {
	// Retrieve logs with the given prefix with detailed comments
	return c.Metadata.ListLogsWithPrefix(ctx, logNamePrefix)
}

func (c *woodpeckerClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	closeErr := c.Metadata.Close()
	closePoolErr := c.clientPool.Close()
	return werr.Combine(closeErr, closePoolErr)
}

package woodpecker

import (
	"context"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/server"
	"github.com/zilliztech/woodpecker/server/client"
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
}

func NewEmbedClientFromConfig(ctx context.Context, config *config.Configuration) (Client, error) {
	logger.InitLogger(config)
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
	logger.InitLogger(cfg)
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
	initErr := c.initClient(ctx)
	if initErr != nil {
		return nil, werr.ErrInitClient.WithCauseErr(initErr)
	}
	return &c, nil
}

func (c *woodpeckerEmbedClient) initClient(ctx context.Context) error {
	initMeta := c.Metadata.InitIfNecessary(ctx)
	if initMeta != nil {
		return initMeta
	}
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
	// early return if cache exists
	if _, exists := c.logHandles[logName]; exists {
		return werr.ErrLogAlreadyExists
	}
	// otherwise try create new log
	return c.Metadata.CreateLog(ctx, logName)
}

// OpenLog opens an existing log with the specified name and returns a log handle.
// It retrieves the log metadata and segments metadata, then creates a new log handle.
func (c *woodpeckerEmbedClient) OpenLog(ctx context.Context, logName string) (log.LogHandle, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// early return if cache exists
	if logHandle, exists := c.logHandles[logName]; exists {
		return logHandle, nil
	}
	// otherwise open one
	logMeta, segmentsMeta, err := c.Metadata.OpenLog(ctx, logName)
	if err != nil {
		return nil, err
	}
	newLogHandle := log.NewLogHandle(logName, logMeta.GetLogId(), segmentsMeta, c.GetMetadataProvider(), c.clientPool, c.cfg)
	c.logHandles[logName] = newLogHandle
	return newLogHandle, nil
}

// DeleteLog deletes the log with the specified name.
// It should remove the log and its associated metadata.
func (c *woodpeckerEmbedClient) DeleteLog(ctx context.Context, logName string) error {
	//TODO implement me
	panic("implement me")
}

// LogExists checks if a log with the specified name exists.
// It returns true if the log exists, otherwise false.
func (c *woodpeckerEmbedClient) LogExists(ctx context.Context, logName string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// early return if cache exists
	if _, exists := c.logHandles[logName]; exists {
		return true, nil
	}
	// otherwise check meta
	return c.Metadata.CheckExists(ctx, logName)
}

// GetAllLogs retrieves all log names.
// It returns a slice containing all log names.
func (c *woodpeckerEmbedClient) GetAllLogs(ctx context.Context) ([]string, error) {
	return c.Metadata.ListLogs(ctx)
}

// GetLogsWithPrefix retrieves log names that start with the specified prefix.
// It returns a slice containing log names that match the prefix.
func (c *woodpeckerEmbedClient) GetLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error) {
	return c.Metadata.ListLogsWithPrefix(ctx, logNamePrefix)
}

func (c *woodpeckerEmbedClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	closeErr := c.Metadata.Close()
	closePoolErr := c.clientPool.Close()
	if c.managedCli {
		closeEtcdCliErr := c.etcdCli.Close()
		return werr.Combine(closeErr, closePoolErr, closeEtcdCliErr)
	}
	return werr.Combine(closeErr, closePoolErr)
}

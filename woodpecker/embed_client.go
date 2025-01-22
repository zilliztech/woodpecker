package woodpecker

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/server"
	"github.com/zilliztech/woodpecker/server/client"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

var _ Client = (*woodpeckerEmbedClient)(nil)

// Implementation of the client interface for Z'eembed mode.
type woodpeckerEmbedClient struct {
	cfg           *config.Configuration
	Metadata      meta.MetadataProvider
	embedLogStore server.LogStore

	managedCli bool
	etcdCli    *clientv3.Client
	minioCli   minio.MinioHandler
}

func NewEmbedClientFromConfig(ctx context.Context, config *config.Configuration) (client Client, err error) {
	etcdCli, err := etcd.GetRemoteEtcdClient(config.Etcd.GetEndpoints())
	if err != nil {
		return nil, werr.ErrCreateConnection.WithCauseErr(err)
	}
	minioCli, err := minio.NewMinioHandler(ctx, config)
	if err != nil {
		return nil, werr.ErrCreateConnection.WithCauseErr(err)
	}
	return NewEmbedClient(ctx, config, etcdCli, minioCli, true)
}

func NewEmbedClient(ctx context.Context, cfg *config.Configuration, etcdCli *clientv3.Client, minioCli minio.MinioHandler, managed bool) (client Client, err error) {
	instance := server.NewLogStore(context.Background(), cfg, etcdCli, minioCli)
	c := woodpeckerEmbedClient{
		cfg:           cfg,
		Metadata:      meta.NewMetadataProvider(ctx, etcdCli),
		embedLogStore: instance,
		managedCli:    managed,
		etcdCli:       etcdCli,
		minioCli:      minioCli,
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
	c.embedLogStore.SetAddress("127.0.0.1:59456") // TODO
	embedLogStoreStartErr := c.embedLogStore.Start()
	return embedLogStoreStartErr
}

func (c *woodpeckerEmbedClient) GetMetadataProvider() meta.MetadataProvider {
	return c.Metadata
}

// CreateLog creates a new log with the specified name.
// It stores segment metadata for the new log.
func (c *woodpeckerEmbedClient) CreateLog(ctx context.Context, logName string) error {
	return c.Metadata.CreateLog(ctx, logName)
}

// OpenLog opens an existing log with the specified name and returns a log handle.
// It retrieves the log metadata and segments metadata, then creates a new log handle.
func (c *woodpeckerEmbedClient) OpenLog(ctx context.Context, logName string) (log.LogHandle, error) {
	logMeta, segmentsMeta, err := c.Metadata.OpenLog(ctx, logName)
	if err != nil {
		return nil, err
	}
	return log.NewLogHandle(logName, logMeta, segmentsMeta, c.GetMetadataProvider(), client.NewLogStoreClientPoolLocal(c.embedLogStore), c.cfg), nil
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
	c.embedLogStore.Stop()
	c.Metadata.Close()
	if c.managedCli {
		c.etcdCli.Close()
	}
	return nil
}

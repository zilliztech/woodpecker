package stream

import (
	"context"
	"github.com/milvus-io/woodpecker/common/minio"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/woodpecker/meta"
	"github.com/milvus-io/woodpecker/proto"
	"github.com/milvus-io/woodpecker/server"
	"github.com/milvus-io/woodpecker/server/client"
	"github.com/milvus-io/woodpecker/stream/log"
)

func NewWoodpeckerEmbedClient(ctx context.Context, etcdCli *clientv3.Client) (client WoodpeckerClient, err error) {
	minioCli, err := minio.NewMinioClient(ctx, meta.ServicePrefix)
	if err != nil {
		return nil, err
	}
	instance := server.NewLogStore(context.Background(), etcdCli, minioCli)
	c := woodpeckerEmbedClient{
		Metadata:      meta.NewMetadataProvider(ctx, etcdCli),
		embedLogStore: instance,
	}
	initErr := c.initClient(ctx)
	if initErr != nil {
		return nil, initErr
	}
	return &c, nil
}

var _ WoodpeckerClient = (*woodpeckerEmbedClient)(nil)

/**
 * Implementation of the client interface for embed mode.
 */
type woodpeckerEmbedClient struct {
	Metadata      meta.MetadataProvider
	embedLogStore *server.LogStore
}

func (c *woodpeckerEmbedClient) initClient(ctx context.Context) error {
	initMeta := c.Metadata.InitIfNecessary(ctx)
	if initMeta != nil {
		return initMeta
	}
	c.embedLogStore.SetAddress("127.0.0.1:59456")
	embedLogStoreStartErr := c.embedLogStore.Start()
	return embedLogStoreStartErr
}

func (c *woodpeckerEmbedClient) GetMetadataProvider() meta.MetadataProvider {
	return c.Metadata
}

func (c *woodpeckerEmbedClient) CreateLog(ctx context.Context, logName string) error {
	a := int64(0)
	c.Metadata.StoreSegmentMetadata(ctx, logName, &proto.SegmentMetadata{
		SegNo:    0,
		State:    proto.SegmentState_Active,
		QuorumId: &a,
	})
	return nil
}

func (c *woodpeckerEmbedClient) OpenLog(ctx context.Context, logName string) (log.LogHandle, error) {
	logMeta, segmentsMeta, err := c.Metadata.OpenLog(ctx, logName)
	if err != nil {
		return nil, err
	}
	return log.NewLogHandle(logName, logMeta, segmentsMeta, c.GetMetadataProvider(), client.NewLogStoreClientPoolLocal(c.embedLogStore)), nil
}

func (c *woodpeckerEmbedClient) DeleteLog(ctx context.Context, logName string) error {
	//TODO implement me
	panic("implement me")
}

func (c *woodpeckerEmbedClient) LogExists(ctx context.Context, logName string) (bool, error) {
	return c.Metadata.CheckExists(ctx, logName)
}

func (c *woodpeckerEmbedClient) GetAllLogs(ctx context.Context) ([]string, error) {
	return c.Metadata.ListLogs(ctx)
}

func (c *woodpeckerEmbedClient) GetLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error) {
	return c.Metadata.ListLogsWithPrefix(ctx, logNamePrefix)
}

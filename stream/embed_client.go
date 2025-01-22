package stream

import (
	"context"
	"github.com/milvus-io/woodpecker/meta"
	"github.com/milvus-io/woodpecker/proto"
	"github.com/milvus-io/woodpecker/server"
	"github.com/milvus-io/woodpecker/server/client"
	"github.com/milvus-io/woodpecker/stream/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func NewWoodpeckerEmbedClient(ctx context.Context) WoodpeckerClient {
	instance := server.NewLogStore(context.Background(), nil)
	return &woodpeckerEmbedClient{
		Metadata:      meta.NewMetadataProviderMemory(),
		embedLogStore: instance,
	}
}

func NewWoodpeckerEmbedClientWithEtcd(ctx context.Context, etcdCli *clientv3.Client) (client WoodpeckerClient, err error) {
	instance := server.NewLogStore(context.Background(), etcdCli)
	instance.SetAddress("127.0.0.1")
	instance.Register(ctx)

	c := woodpeckerEmbedClient{
		Metadata:      meta.NewMetadataProviderEtcd(ctx, etcdCli),
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
	return c.Metadata.InitIfNecessary(ctx)
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

func (c *woodpeckerEmbedClient) DeleteLog(ctx context.Context, logName string) error {
	//TODO implement me
	panic("implement me")
}

func (c *woodpeckerEmbedClient) OpenLog(ctx context.Context, logName string) (log.LogHandle, error) {
	logMeta, segmentsMeta, err := c.Metadata.OpenLog(ctx, logName)
	if err != nil {
		return nil, err
	}
	return log.NewLogHandle(logName, logMeta, segmentsMeta, c.GetMetadataProvider(), client.NewLogStoreClientPoolLocal(c.embedLogStore)), nil
}

func (c *woodpeckerEmbedClient) LogExists(ctx context.Context, logName string) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (c *woodpeckerEmbedClient) GetAllLogs(ctx context.Context) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (c *woodpeckerEmbedClient) GetLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

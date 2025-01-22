package stream

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/woodpecker/meta"
	"github.com/milvus-io/woodpecker/proto"
	"github.com/milvus-io/woodpecker/server/client"
	"github.com/milvus-io/woodpecker/stream/log"
)

type WoodpeckerClient interface {
	CreateLog(context.Context, string) error
	OpenLog(context.Context, string) (log.LogHandle, error)
	DeleteLog(context.Context, string) error
	LogExists(context.Context, string) (bool, error)
	GetAllLogs(context.Context) ([]string, error)
	GetLogsWithPrefix(context.Context, string) ([]string, error)
	GetMetadataProvider() meta.MetadataProvider
}

func NewWoodpeckerClient(ctx context.Context, etcdClient *clientv3.Client) (WoodpeckerClient, error) {
	c := &woodpeckerClient{
		Metadata: meta.NewMetadataProvider(ctx, etcdClient),
	}
	err := c.initClient(ctx)
	if err != nil {
		return nil, err
	}
	return c, nil
}

var _ WoodpeckerClient = (*woodpeckerClient)(nil)

/**
 * Implementation of the client interface for Distributed mode.
 */
type woodpeckerClient struct {
	Metadata meta.MetadataProvider
}

func (c *woodpeckerClient) initClient(ctx context.Context) error {
	return c.Metadata.InitIfNecessary(ctx)
}

func (c *woodpeckerClient) GetMetadataProvider() meta.MetadataProvider {
	return c.Metadata
}

func (c *woodpeckerClient) CreateLog(ctx context.Context, logName string) error {
	a := int64(0)
	c.Metadata.StoreSegmentMetadata(ctx, logName, &proto.SegmentMetadata{
		SegNo:    0,
		State:    proto.SegmentState_Active,
		QuorumId: &a,
	})
	return nil
}

func (c *woodpeckerClient) OpenLog(ctx context.Context, logName string) (log.LogHandle, error) {
	logMeta, segmentsMeta, err := c.Metadata.OpenLog(ctx, logName)
	if err != nil {
		return nil, err
	}
	return log.NewLogHandle(logName, logMeta, segmentsMeta, c.GetMetadataProvider(), client.NewLogStoreClientPool()), nil
}

func (c *woodpeckerClient) DeleteLog(ctx context.Context, logName string) error {
	//TODO implement me
	panic("implement me")
}

func (c *woodpeckerClient) LogExists(ctx context.Context, logName string) (bool, error) {
	return c.Metadata.CheckExists(ctx, logName)
}

func (c *woodpeckerClient) GetAllLogs(ctx context.Context) ([]string, error) {
	return c.Metadata.ListLogs(ctx)
}

func (c *woodpeckerClient) GetLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error) {
	return c.Metadata.ListLogsWithPrefix(ctx, logNamePrefix)
}

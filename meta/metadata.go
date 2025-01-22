package meta

import (
	"context"
	"github.com/milvus-io/woodpecker/proto"
	"io"
)

type MetadataProvider interface {
	io.Closer
	StoreSegmentMetadata(context.Context, string, *proto.SegmentMetadata) error
	GetSegmentMetadata(context.Context, string, int64) (*proto.SegmentMetadata, error)
	GetAllSegmentMetadata(context.Context, string) ([]*proto.SegmentMetadata, error)
	StoreQuorumInfo(context.Context, *proto.QuorumInfo) error
	GetQuorumInfo(context.Context, int64) (*proto.QuorumInfo, error)
	StoreVersionInfo(context.Context, *proto.Version) error
	GetVersionInfo(context.Context) (*proto.Version, error)

	InitIfNecessary(ctx context.Context) error
	CreateLog(ctx context.Context, logName string) error
	OpenLog(ctx context.Context, logName string) (*proto.LogMeta, []*proto.SegmentMetadata, error)
}

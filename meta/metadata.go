package meta

import (
	"context"
	"io"

	"github.com/milvus-io/woodpecker/proto"
)

type MetadataProvider interface {
	io.Closer
	// InitIfNecessary initializes the metadata provider if necessary.
	InitIfNecessary(ctx context.Context) error

	GetVersionInfo(context.Context) (*proto.Version, error)

	CreateLog(ctx context.Context, logName string) error
	OpenLog(ctx context.Context, logName string) (*proto.LogMeta, []*proto.SegmentMetadata, error)
	CheckExists(ctx context.Context, logName string) (bool, error)
	ListLogs(ctx context.Context) ([]string, error)
	ListLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error)
	GetLogMeta(ctx context.Context, logName string) (*proto.LogMeta, error)

	StoreSegmentMetadata(context.Context, string, *proto.SegmentMetadata) error
	GetSegmentMetadata(context.Context, string, int64) (*proto.SegmentMetadata, error)
	GetAllSegmentMetadata(context.Context, string) ([]*proto.SegmentMetadata, error)

	StoreQuorumInfo(ctx context.Context, info *proto.QuorumInfo) error
	GetQuorumInfo(context.Context, int64) (*proto.QuorumInfo, error)
}

package meta

import (
	"context"
	"io"

	"github.com/zilliztech/woodpecker/proto"
)

//go:generate  mockery --dir=./meta --name=MetadataProvider --structname=MetadataProvider --output=mocks/mocks_meta --filename=mock_metadata.go --with-expecter=true  --outpkg=mocks_meta
type MetadataProvider interface {
	io.Closer
	// InitIfNecessary initializes the metadata provider if necessary.
	InitIfNecessary(ctx context.Context) error

	GetVersionInfo(context.Context) (*proto.Version, error)

	CreateLog(ctx context.Context, logName string) error
	OpenLog(ctx context.Context, logName string) (*proto.LogMeta, map[int64]*proto.SegmentMetadata, error)
	CheckExists(ctx context.Context, logName string) (bool, error)
	ListLogs(ctx context.Context) ([]string, error)
	ListLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error)
	GetLogMeta(ctx context.Context, logName string) (*proto.LogMeta, error)
	UpdateLogMeta(ctx context.Context, logName string, logMeta *proto.LogMeta) error
	AcquireLogWriterLock(ctx context.Context, logName string) error
	ReleaseLogWriterLock(ctx context.Context, logName string) error

	StoreSegmentMetadata(context.Context, string, *proto.SegmentMetadata) error
	UpdateSegmentMetadata(context.Context, string, *proto.SegmentMetadata) error
	GetSegmentMetadata(context.Context, string, int64) (*proto.SegmentMetadata, error)
	GetAllSegmentMetadata(context.Context, string) (map[int64]*proto.SegmentMetadata, error)
	CheckSegmentExists(ctx context.Context, name string, i int64) (bool, error)

	StoreQuorumInfo(ctx context.Context, info *proto.QuorumInfo) error
	GetQuorumInfo(context.Context, int64) (*proto.QuorumInfo, error)
}

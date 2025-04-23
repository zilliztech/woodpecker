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
	DeleteSegmentMetadata(context.Context, string, int64) error
	GetSegmentMetadata(context.Context, string, int64) (*proto.SegmentMetadata, error)
	GetAllSegmentMetadata(context.Context, string) (map[int64]*proto.SegmentMetadata, error)
	CheckSegmentExists(ctx context.Context, name string, i int64) (bool, error)

	StoreQuorumInfo(ctx context.Context, info *proto.QuorumInfo) error
	GetQuorumInfo(context.Context, int64) (*proto.QuorumInfo, error)

	CreateReaderTempInfo(context.Context, string, int64, int64, int64) error
	// GetReaderTempInfo returns the temporary information for a specific reader
	GetReaderTempInfo(ctx context.Context, logId int64, readerName string) (*proto.ReaderTempInfo, error)
	// GetAllReaderTempInfoForLog returns all reader temporary information for a given log
	GetAllReaderTempInfoForLog(ctx context.Context, logId int64) ([]*proto.ReaderTempInfo, error)
	// UpdateReaderTempInfo updates the reader's recent read position
	UpdateReaderTempInfo(ctx context.Context, logId int64, readerName string, recentReadSegmentId int64, recentReadEntryId int64) error
	// DeleteReaderTempInfo deletes the temporary information for a reader when it closes
	DeleteReaderTempInfo(ctx context.Context, logId int64, readerName string) error

	// CreateSegmentCleanupStatus creates a new segment cleanup status record
	CreateSegmentCleanupStatus(ctx context.Context, status *proto.SegmentCleanupStatus) error
	// UpdateSegmentCleanupStatus updates an existing segment cleanup status
	UpdateSegmentCleanupStatus(ctx context.Context, status *proto.SegmentCleanupStatus) error
	// GetSegmentCleanupStatus retrieves the cleanup status for a segment
	GetSegmentCleanupStatus(ctx context.Context, logId, segmentId int64) (*proto.SegmentCleanupStatus, error)
	// DeleteSegmentCleanupStatus deletes the cleanup status for a segment
	DeleteSegmentCleanupStatus(ctx context.Context, logId, segmentId int64) error
	// ListSegmentCleanupStatus lists all cleanup statuses for a log
	ListSegmentCleanupStatus(ctx context.Context, logId int64) ([]*proto.SegmentCleanupStatus, error)
}

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"context"
	"io"

	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/zilliztech/woodpecker/proto"
)

//go:generate  mockery --dir=./meta --name=MetadataProvider --structname=MetadataProvider --output=mocks/mocks_meta --filename=mock_metadata.go --with-expecter=true  --outpkg=mocks_meta
type MetadataProvider interface {
	io.Closer
	// InitIfNecessary initializes the metadata provider if necessary.
	InitIfNecessary(ctx context.Context) error
	// GetVersionInfo returns the version information of the metadata provider.
	GetVersionInfo(ctx context.Context) (*proto.Version, error)

	// CreateLog creates a new log in the metadata provider.
	CreateLog(ctx context.Context, logName string) error
	// OpenLog opens an existing log in the metadata provider.
	OpenLog(ctx context.Context, logName string) (*proto.LogMeta, map[int64]*proto.SegmentMetadata, error)
	// CheckExists checks if a log exists in the metadata provider.
	CheckExists(ctx context.Context, logName string) (bool, error)
	// ListLogs returns a list of all logs in the metadata provider.
	ListLogs(ctx context.Context) ([]string, error)
	// ListLogsWithPrefix returns a list of all logs in the metadata provider with a given prefix.
	ListLogsWithPrefix(ctx context.Context, logNamePrefix string) ([]string, error)
	// GetLogMeta returns the metadata for a specific log.
	GetLogMeta(ctx context.Context, logName string) (*proto.LogMeta, error)
	// UpdateLogMeta updates the metadata for a specific log.
	UpdateLogMeta(ctx context.Context, logName string, logMeta *proto.LogMeta) error
	// AcquireLogWriterLock acquires a lock for writing to a specific log.
	AcquireLogWriterLock(ctx context.Context, logName string) (*concurrency.Session, error)
	// ReleaseLogWriterLock releases a lock for writing to a specific log.
	ReleaseLogWriterLock(ctx context.Context, logName string) error

	// StoreSegmentMetadata stores the metadata for a specific segment.
	StoreSegmentMetadata(ctx context.Context, logName string, segmentMeta *proto.SegmentMetadata) error
	// UpdateSegmentMetadata updates the metadata for a specific segment.
	UpdateSegmentMetadata(ctx context.Context, logName string, segmentMeta *proto.SegmentMetadata) error
	// DeleteSegmentMetadata deletes the metadata for a specific segment.
	DeleteSegmentMetadata(ctx context.Context, logName string, segmentId int64) error
	// GetSegmentMetadata returns the metadata for a specific segment.
	GetSegmentMetadata(ctx context.Context, logName string, segmentId int64) (*proto.SegmentMetadata, error)
	// GetAllSegmentMetadata returns all segment metadata for a specific log.
	GetAllSegmentMetadata(ctx context.Context, logName string) (map[int64]*proto.SegmentMetadata, error)
	// CheckSegmentExists checks if a segment exists in the metadata provider.
	CheckSegmentExists(ctx context.Context, name string, segmentId int64) (bool, error)

	// StoreQuorumInfo stores the quorum information for a specific quorum.
	StoreQuorumInfo(ctx context.Context, info *proto.QuorumInfo) error
	// GetQuorumInfo returns the quorum information for a specific quorum.
	GetQuorumInfo(ctx context.Context, quorumId int64) (*proto.QuorumInfo, error)

	// CreateReaderTempInfo creates a new reader temporary information record.
	CreateReaderTempInfo(ctx context.Context, readerName string, logId int64, segmentId int64, entryId int64) error
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

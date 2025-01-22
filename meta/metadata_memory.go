package meta

import (
	"context"
	"errors"
	"fmt"
	"github.com/milvus-io/woodpecker/proto"
	"sync"
)

var _ MetadataProvider = (*metadataProviderMemory)(nil)

type metadataProviderMemory struct {
	sync.Mutex
	segmentsMetadata map[string][]*proto.SegmentMetadata
	quorumInfo       map[int64]*proto.QuorumInfo
	version          *proto.Version
}

func NewMetadataProviderMemory() MetadataProvider {
	return &metadataProviderMemory{
		segmentsMetadata: make(map[string][]*proto.SegmentMetadata),
		quorumInfo:       make(map[int64]*proto.QuorumInfo),
		version:          &proto.Version{},
	}
}

func (m *metadataProviderMemory) Close() error {
	return nil
}

func (m *metadataProviderMemory) StoreSegmentMetadata(ctx context.Context, logName string, metadata *proto.SegmentMetadata) error {
	m.Lock()
	defer m.Unlock()
	m.segmentsMetadata[logName] = append(m.segmentsMetadata[logName], metadata)
	return nil
}

func (m *metadataProviderMemory) StoreQuorumInfo(ctx context.Context, quorumInfo *proto.QuorumInfo) error {
	m.Lock()
	defer m.Unlock()
	m.quorumInfo[quorumInfo.Id] = quorumInfo
	return nil
}

func (m *metadataProviderMemory) StoreVersionInfo(ctx context.Context, version *proto.Version) error {
	m.Lock()
	defer m.Unlock()
	m.version = version
	return nil
}

func (m *metadataProviderMemory) GetSegmentMetadata(ctx context.Context, logName string, segmentId int64) (*proto.SegmentMetadata, error) {
	segmentsMeta, logExists := m.segmentsMetadata[logName]
	if !logExists {
		return nil, errors.New(fmt.Sprintf("ErrLogNotFound for log:%s", logName))
	}
	if int64(len(segmentsMeta)) < segmentId {
		return nil, errors.New(fmt.Sprintf("ErrSegmentNotFound for log:%s segId:%d", logName, segmentId))
	}
	return segmentsMeta[segmentId], nil
}

func (m *metadataProviderMemory) GetAllSegmentMetadata(ctx context.Context, logName string) ([]*proto.SegmentMetadata, error) {
	segmentsMeta, logExists := m.segmentsMetadata[logName]
	if !logExists {
		return nil, errors.New(fmt.Sprintf("ErrLogNotFound for log:%s", logName))
	}
	return segmentsMeta, nil
}

func (m *metadataProviderMemory) GetQuorumInfo(ctx context.Context, quorumId int64) (*proto.QuorumInfo, error) {
	quorumInfo, quorumExists := m.quorumInfo[quorumId]
	if !quorumExists {
		return nil, errors.New(fmt.Sprintf("ErrQuorumNotFound for quorum:%d", quorumId))
	}
	return quorumInfo, nil
}

func (m *metadataProviderMemory) GetVersionInfo(ctx context.Context) (*proto.Version, error) {
	return m.version, nil
}

func (m *metadataProviderMemory) InitIfNecessary(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (m *metadataProviderMemory) CreateLog(ctx context.Context, logName string) error {
	//TODO implement me
	panic("implement me")
}

func (m *metadataProviderMemory) OpenLog(ctx context.Context, logName string) (*proto.LogMeta, []*proto.SegmentMetadata, error) {
	//TODO implement me
	panic("implement me")
}

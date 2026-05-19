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

package segment

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_objectstorage"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// stubObjectStorage is a minimal stub implementing storageclient.ObjectStorage
// for testing purposes. We only need it to satisfy the interface; actual
// operations are not called in canDirectRead tests.
type stubObjectStorage struct {
	storageclient.ObjectStorage
}

type directReadTestFileReader struct {
	data []byte
	*bytes.Reader
}

func newDirectReadTestFileReader(data []byte) *directReadTestFileReader {
	return &directReadTestFileReader{
		data:   data,
		Reader: bytes.NewReader(data),
	}
}

func (r *directReadTestFileReader) Close() error {
	return nil
}

func (r *directReadTestFileReader) Size() (int64, error) {
	return int64(len(r.data)), nil
}

func newTestCfgWithDirectRead(enabled bool) *config.Configuration {
	return &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{
					QueueSize:  10,
					MaxRetries: 2,
				},
				DirectRead: config.DirectReadConfig{
					Enabled:         enabled,
					MaxBatchSize:    config.ByteSize(16 * 1024 * 1024),
					MaxFetchThreads: 4,
				},
			},
		},
	}
}

func newTestSegmentHandleImpl(state proto.SegmentState, cfg *config.Configuration, objStorage storageclient.ObjectStorage) *segmentHandleImpl {
	mockMetadata := mocks_meta.NewMetadataProvider(&testing.T{})
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(&testing.T{})

	segMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1,
			State: state,
		},
		Revision: 1,
	}

	handle := NewSegmentHandle(context.Background(), 1, "testLog", segMeta, mockMetadata, mockClientPool, cfg, false, objStorage)
	return handle.(*segmentHandleImpl)
}

func TestCanDirectRead_SealedWithClientAndEnabled(t *testing.T) {
	cfg := newTestCfgWithDirectRead(true)
	handle := newTestSegmentHandleImpl(proto.SegmentState_Sealed, cfg, &stubObjectStorage{})
	assert.True(t, handle.canDirectRead(), "should allow direct read for sealed segment with enabled config and non-nil client")
}

func TestCanDirectRead_SealedWithClientButDisabled(t *testing.T) {
	cfg := newTestCfgWithDirectRead(false)
	handle := newTestSegmentHandleImpl(proto.SegmentState_Sealed, cfg, &stubObjectStorage{})
	assert.False(t, handle.canDirectRead(), "should not allow direct read when config is disabled")
}

func TestCanDirectRead_SealedWithoutClient(t *testing.T) {
	cfg := newTestCfgWithDirectRead(true)
	handle := newTestSegmentHandleImpl(proto.SegmentState_Sealed, cfg, nil)
	assert.False(t, handle.canDirectRead(), "should not allow direct read without object storage client")
}

func TestCanDirectRead_ActiveSegment(t *testing.T) {
	cfg := newTestCfgWithDirectRead(true)
	handle := newTestSegmentHandleImpl(proto.SegmentState_Active, cfg, &stubObjectStorage{})
	assert.False(t, handle.canDirectRead(), "should not allow direct read for active segment")
}

func TestCanDirectRead_CompletedSegment(t *testing.T) {
	cfg := newTestCfgWithDirectRead(true)
	handle := newTestSegmentHandleImpl(proto.SegmentState_Completed, cfg, &stubObjectStorage{})
	assert.False(t, handle.canDirectRead(), "should not allow direct read for completed (not yet sealed) segment")
}

func TestCanDirectRead_TruncatedSegment(t *testing.T) {
	cfg := newTestCfgWithDirectRead(true)
	handle := newTestSegmentHandleImpl(proto.SegmentState_Truncated, cfg, &stubObjectStorage{})
	assert.False(t, handle.canDirectRead(), "should not allow direct read for truncated segment")
}

func TestReadBatchAdv_UsesQuorumWhenDirectReadDisabled(t *testing.T) {
	// When direct read is disabled, ReadBatchAdv should use the quorum read path.
	// canDirectRead returns false so the quorum path is selected.
	cfg := newTestCfgWithDirectRead(false)
	handle := newTestSegmentHandleImpl(proto.SegmentState_Sealed, cfg, &stubObjectStorage{})

	// canDirectRead should be false, meaning quorum path will be used
	assert.False(t, handle.canDirectRead())
}

func TestNewSegmentHandleWithObjectStorageClient(t *testing.T) {
	cfg := newTestCfgWithDirectRead(true)
	stubClient := &stubObjectStorage{}

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	segMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1,
			State: proto.SegmentState_Sealed,
		},
		Revision: 1,
	}

	handle := NewSegmentHandle(context.Background(), 1, "testLog", segMeta, mockMetadata, mockClientPool, cfg, false, stubClient)
	impl := handle.(*segmentHandleImpl)

	assert.NotNil(t, impl.objectStorageClient, "objectStorageClient should be set")
	assert.True(t, impl.canDirectRead(), "should be able to direct read")
}

func TestNewSegmentHandleWithNilObjectStorageClient(t *testing.T) {
	cfg := newTestCfgWithDirectRead(true)

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)

	segMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1,
			State: proto.SegmentState_Sealed,
		},
		Revision: 1,
	}

	handle := NewSegmentHandle(context.Background(), 1, "testLog", segMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := handle.(*segmentHandleImpl)

	assert.Nil(t, impl.objectStorageClient, "objectStorageClient should be nil")
	assert.False(t, impl.canDirectRead(), "should not allow direct read without client")
}

func TestDirectReadBatch_ClosesDirectReaderOnEOFAndReopens(t *testing.T) {
	ctx := context.Background()
	cfg := newTestCfgWithDirectRead(true)
	cfg.Minio.BucketName = "bucket"
	cfg.Minio.RootPath = "root"

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockObjectStorage := mocks_objectstorage.NewObjectStorage(t)

	indexData := codec.EncodeRecord(&codec.IndexRecord{
		BlockNumber:  0,
		StartOffset:  0,
		BlockSize:    128,
		FirstEntryID: 0,
		LastEntryID:  1,
	})
	footer := &codec.FooterRecord{
		TotalBlocks:  1,
		TotalRecords: 2,
		TotalSize:    128,
		IndexOffset:  0,
		IndexLength:  uint32(len(indexData)),
		Version:      codec.FormatVersion,
		LAC:          1,
	}
	footer.SetCompacted(true)
	footerData := append(indexData, codec.EncodeRecord(footer)...)

	mockObjectStorage.EXPECT().
		StatObject(mock.Anything, "bucket", mock.Anything, mock.Anything, mock.Anything).
		Return(int64(len(footerData)), false, nil).
		Times(2)
	mockObjectStorage.EXPECT().
		GetObject(mock.Anything, "bucket", mock.Anything, int64(0), int64(len(footerData)), mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, string, string, int64, int64, string, string) (minioHandler.FileReader, error) {
			return newDirectReadTestFileReader(footerData), nil
		}).
		Times(2)

	segMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1,
			State: proto.SegmentState_Sealed,
		},
		Revision: 1,
	}
	handle := NewSegmentHandle(ctx, 1, "testLog", segMeta, mockMetadata, mockClientPool, cfg, false, mockObjectStorage)
	impl := handle.(*segmentHandleImpl)

	result, err := impl.directReadBatch(ctx, 2, 1, nil)
	assert.Nil(t, result)
	assert.True(t, werr.ErrFileReaderEndOfFile.Is(err))
	assert.Nil(t, impl.directReader)

	result, err = impl.directReadBatch(ctx, 2, 1, nil)
	assert.Nil(t, result)
	assert.True(t, werr.ErrFileReaderEndOfFile.Is(err))
	assert.Nil(t, impl.directReader)
}

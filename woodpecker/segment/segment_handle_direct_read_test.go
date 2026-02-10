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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	storageclient "github.com/zilliztech/woodpecker/common/objectstorage"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

// stubObjectStorage is a minimal stub implementing storageclient.ObjectStorage
// for testing purposes. We only need it to satisfy the interface; actual
// operations are not called in canDirectRead tests.
type stubObjectStorage struct {
	storageclient.ObjectStorage
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

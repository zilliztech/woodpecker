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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

func TestSegmentHandle_ActiveSegmentNodeMetric_SetAndClear(t *testing.T) {
	metrics.WpActiveSegmentNode.Reset()
	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.WpActiveSegmentNode)

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockMetadata.EXPECT().UpdateSegmentMetadata(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, mock.Anything).Return(mockClient, nil).Maybe()

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       7,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
			Quorum: &proto.QuorumInfo{
				Id: 1, Aq: 1, Es: 3, Wq: 3,
				Nodes: []string{"10.0.0.1:8000", "10.0.0.2:8000", "10.0.0.3:8000"},
			},
		},
		Revision: 1,
	}

	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, true, nil).(*segmentHandleImpl)

	count, err := testutil.GatherAndCount(reg, "woodpecker_client_active_segment_node")
	assert.NoError(t, err)
	assert.Equal(t, 3, count, "writable segment should publish one series per quorum node")

	// Transition out of writable; clear must fire exactly once (CAS true->false).
	err = sh.doCloseWritingAndUpdateMetaIfNecessaryUnsafe(context.Background(), -1)
	assert.NoError(t, err)

	count, err = testutil.GatherAndCount(reg, "woodpecker_client_active_segment_node")
	assert.NoError(t, err)
	assert.Equal(t, 0, count, "series removed after segment leaves writable state")
}

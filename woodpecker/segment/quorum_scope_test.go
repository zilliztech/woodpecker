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

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/topology"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

func scopeTestQuorum() *proto.QuorumInfo {
	return &proto.QuorumInfo{
		Id:    1,
		Aq:    2,
		Es:    3,
		Wq:    3,
		Nodes: []string{"node-remote-a", "node-local", "node-unmapped"},
		Replicas: []*proto.QuorumNode{
			{Endpoint: "node-remote-a", Region: "region-remote", Az: "az-remote-a"},
			{Endpoint: "node-local", Region: "region-local", Az: "az-local"},
			// node-unmapped has no replica entry: quorum metadata written
			// before placement was recorded.
		},
	}
}

func TestQuorumNodeScopes(t *testing.T) {
	t.Run("with local placement", func(t *testing.T) {
		t.Setenv("REGION", "region-local")
		t.Setenv("AVAILABILITY_ZONE", "az-local")

		assert.Equal(t,
			[]string{topology.ScopeCrossRegion, topology.ScopeLocal, topology.ScopeUnknown},
			quorumNodeScopes(scopeTestQuorum()))
	})

	t.Run("same region other az", func(t *testing.T) {
		t.Setenv("REGION", "region-remote")
		t.Setenv("AVAILABILITY_ZONE", "az-remote-b")

		assert.Equal(t,
			[]string{topology.ScopeCrossAZ, topology.ScopeCrossRegion, topology.ScopeUnknown},
			quorumNodeScopes(scopeTestQuorum()))
	})

	t.Run("client without placement knows nothing", func(t *testing.T) {
		t.Setenv("REGION", "")
		t.Setenv("AVAILABILITY_ZONE", "")

		assert.Equal(t,
			[]string{topology.ScopeUnknown, topology.ScopeUnknown, topology.ScopeUnknown},
			quorumNodeScopes(scopeTestQuorum()))
	})
}

func TestActiveSegmentNodes(t *testing.T) {
	assert.Equal(t, []metrics.ActiveSegmentNode{
		{Node: "node-remote-a", AZ: "az-remote-a"},
		{Node: "node-local", AZ: "az-local"},
		{Node: "node-unmapped", AZ: ""},
	}, activeSegmentNodes(scopeTestQuorum()))
}

func TestOrderedQuorumReadCandidates_CarriesScope(t *testing.T) {
	t.Setenv("REGION", "region-local")
	t.Setenv("AVAILABILITY_ZONE", "az-local")

	candidates := orderedQuorumReadCandidates(scopeTestQuorum(), nil)

	// Local first, and every candidate knows its own distance.
	assert.Equal(t, "node-local", candidates[0].node)
	assert.Equal(t, topology.ScopeLocal, candidates[0].azScope)
	assert.Equal(t, topology.ScopeCrossRegion, candidates[1].azScope)
	assert.Equal(t, topology.ScopeUnknown, candidates[2].azScope)
}

// A read that falls off the local replica onto a remote one is the case that
// costs money, so it must show up as both cross-scope read traffic and a
// failover.
func TestReadBatchAdv_RecordsCrossScopeReadAndFailover(t *testing.T) {
	t.Setenv("REGION", "region-local")
	t.Setenv("AVAILABILITY_ZONE", "az-local")

	metrics.WpClientQuorumReadTotal.Reset()
	metrics.WpClientQuorumReadBytesTotal.Reset()
	metrics.WpClientReadFailoverTotal.Reset()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	localClient := mocks_logstore_client.NewLogStoreClient(t)
	remoteClient := mocks_logstore_client.NewLogStoreClient(t)

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node-local").Return(localClient, nil).Once()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node-remote-a").Return(remoteClient, nil).Once()

	localClient.EXPECT().
		ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(0), int64(10), (*proto.LastReadState)(nil)).
		Return(nil, errors.New("replica unavailable")).Once()
	remoteClient.EXPECT().
		ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(0), int64(10), (*proto.LastReadState)(nil)).
		Return(&proto.BatchReadResult{
			Entries: []*proto.LogEntry{
				{SegId: 1, EntryId: 0, Values: []byte("0123456789")},
			},
			LastReadState: &proto.LastReadState{},
		}, nil).Once()

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	cfg.Minio.BucketName = "a-bucket"
	cfg.Minio.RootPath = "files"

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
			Quorum:      scopeTestQuorum(),
		},
		Revision: 1,
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	result, err := segmentHandle.ReadBatchAdv(context.Background(), 0, 10, nil)
	assert.NoError(t, err)
	assert.Equal(t, "node-remote-a", result.LastReadState.Node)

	logNs := metrics.BuildLogNs("a-bucket", "files")
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.WpClientQuorumReadTotal.WithLabelValues(logNs, "1", topology.ScopeLocal, "error"),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.WpClientQuorumReadTotal.WithLabelValues(logNs, "1", topology.ScopeCrossRegion, "success"),
	))
	assert.Equal(t, float64(10), testutil.ToFloat64(
		metrics.WpClientQuorumReadBytesTotal.WithLabelValues(logNs, "1", topology.ScopeCrossRegion),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.WpClientReadFailoverTotal.WithLabelValues(logNs, "1", topology.ScopeLocal, topology.ScopeCrossRegion),
	))
}

// Serving from the preferred replica is not a failover.
func TestReadBatchAdv_LocalReadIsNotAFailover(t *testing.T) {
	t.Setenv("REGION", "region-local")
	t.Setenv("AVAILABILITY_ZONE", "az-local")

	metrics.WpClientQuorumReadTotal.Reset()
	metrics.WpClientReadFailoverTotal.Reset()

	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	localClient := mocks_logstore_client.NewLogStoreClient(t)

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node-local").Return(localClient, nil).Once()
	localClient.EXPECT().
		ReadEntriesBatchAdv(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), int64(0), int64(10), (*proto.LastReadState)(nil)).
		Return(&proto.BatchReadResult{
			Entries:       []*proto.LogEntry{{SegId: 1, EntryId: 0, Values: []byte("abc")}},
			LastReadState: &proto.LastReadState{},
		}, nil).Once()

	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}
	cfg.Minio.BucketName = "a-bucket"
	cfg.Minio.RootPath = "files"

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo:       1,
			State:       proto.SegmentState_Active,
			LastEntryId: -1,
			Quorum:      scopeTestQuorum(),
		},
		Revision: 1,
	}

	segmentHandle := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	_, err := segmentHandle.ReadBatchAdv(context.Background(), 0, 10, nil)
	assert.NoError(t, err)

	logNs := metrics.BuildLogNs("a-bucket", "files")
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.WpClientQuorumReadTotal.WithLabelValues(logNs, "1", topology.ScopeLocal, "success"),
	))
	assert.Equal(t, 0, testutil.CollectAndCount(metrics.WpClientReadFailoverTotal))
}

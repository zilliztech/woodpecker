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

package quorum

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

// QuorumNode.endpoint duplicated nodes[i] purely as a join key, and that copy
// is a third of every segment's metadata (issue #280). The address now lives
// only in nodes[]; replicas[] is paired with it by index.
func TestQuorumNodeFromMeta_OmitsDeprecatedEndpoint(t *testing.T) {
	replica := quorumNodeFromMeta(&proto.NodeMeta{
		Endpoint:      "wp-node-0.wp-headless.ns.svc.cluster.local:18080",
		NodeId:        "wp-node-0",
		ClusterName:   "wp-cluster",
		Region:        "region-a",
		Az:            "az-a",
		ResourceGroup: "rg-default",
		Tags:          map[string]string{"role": "wal"},
	})

	require.NotNil(t, replica)
	assert.Empty(t, replica.Endpoint, "endpoint is deprecated; nodes[i] carries the address")
	assert.Equal(t, "wp-node-0", replica.NodeId)
	assert.Equal(t, "wp-cluster", replica.ClusterName)
	assert.Equal(t, "region-a", replica.Region)
	assert.Equal(t, "az-a", replica.Az)
	assert.Equal(t, "rg-default", replica.ResourceGroup)
	assert.Equal(t, map[string]string{"role": "wal"}, replica.Tags)
}

func TestQuorumNodeFromMeta_NilNode(t *testing.T) {
	assert.Nil(t, quorumNodeFromMeta(nil))
}

// Every selection path must hand back replicas[] paired with nodes[] by index,
// because that pairing is now the only way to tell where a node sits.
func TestQuorumDiscovery_CrossRegion_ReplicasAlignedWithNodesByIndex(t *testing.T) {
	ctx := context.Background()
	cfg := &config.QuorumConfig{
		BufferPools: config.NewDynamic([]config.QuorumBufferPool{
			{Name: "region-a", Seeds: []string{"seed-a:8080"}},
			{Name: "region-b", Seeds: []string{"seed-b:8080"}},
		}),
		SelectStrategy: config.QuorumSelectStrategy{
			Strategy:     config.NewDynamic("cross-region"),
			AffinityMode: config.NewDynamic("soft"),
			Replicas:     config.NewDynamic(3),
		},
	}

	mockClientA := mocks_logstore_client.NewLogStoreClient(t)
	mockClientB := mocks_logstore_client.NewLogStoreClient(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-a:8080").Return(mockClientA, nil).Maybe()
	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "seed-b:8080").Return(mockClientB, nil).Maybe()

	// az is what the placement lookup ultimately reads, so make it derivable
	// from the endpoint: nodeX lives in az-X.
	mockClientA.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "nodeA1:8080", NodeId: "nA1", Region: "region-a", Az: "az-A1"},
		{Endpoint: "nodeA2:8080", NodeId: "nA2", Region: "region-a", Az: "az-A2"},
	}, nil).Maybe()
	mockClientB.EXPECT().SelectNodes(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*proto.NodeMeta{
		{Endpoint: "nodeB1:8080", NodeId: "nB1", Region: "region-b", Az: "az-B1"},
	}, nil).Maybe()

	discovery, err := NewQuorumDiscovery(ctx, cfg, mockClientPool)
	require.NoError(t, err)

	result, err := discovery.SelectQuorum(ctx)
	require.NoError(t, err)
	require.Len(t, result.Nodes, 3)
	require.Len(t, result.Replicas, len(result.Nodes),
		"replicas must be index-aligned with nodes, so the lists must be the same length")

	for i, node := range result.Nodes {
		replica := result.Replicas[i]
		require.NotNil(t, replica, "node %s has no placement at index %d", node, i)
		assert.Empty(t, replica.Endpoint, "endpoint is deprecated and must not be written")
		// "nodeA1:8080" -> node id "nA1", az "az-A1"
		want := "n" + node[len("node"):len(node)-len(":8080")]
		assert.Equal(t, want, replica.NodeId,
			"replicas[%d] must describe nodes[%d] (%s)", i, i, node)
	}
}

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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/topology"
	"github.com/zilliztech/woodpecker/proto"
)

// alignedQuorumWithoutEndpoint is the shape a segment written after issue #280
// has: replicas[] is index-aligned with nodes[], and QuorumNode.endpoint — which
// only ever duplicated nodes[i] to serve as a join key — is no longer written.
func alignedQuorumWithoutEndpoint() *proto.QuorumInfo {
	return &proto.QuorumInfo{
		Id:    1,
		Aq:    2,
		Es:    3,
		Wq:    3,
		Nodes: []string{"node-remote-a", "node-local", "node-remote-b"},
		Replicas: []*proto.QuorumNode{
			{NodeId: "n-remote-a", Region: "region-remote", Az: "az-remote-a"},
			{NodeId: "n-local", Region: "region-local", Az: "az-local"},
			{NodeId: "n-remote-b", Region: "region-remote", Az: "az-remote-b"},
		},
	}
}

func TestQuorumNodeScopes_AlignsByIndexWhenEndpointOmitted(t *testing.T) {
	t.Setenv("REGION", "region-local")
	t.Setenv("AVAILABILITY_ZONE", "az-local")

	assert.Equal(t,
		[]string{topology.ScopeCrossRegion, topology.ScopeLocal, topology.ScopeCrossRegion},
		quorumNodeScopes(alignedQuorumWithoutEndpoint()))
}

func TestActiveSegmentNodes_AlignsByIndexWhenEndpointOmitted(t *testing.T) {
	assert.Equal(t, []metrics.ActiveSegmentNode{
		{Node: "node-remote-a", AZ: "az-remote-a"},
		{Node: "node-local", AZ: "az-local"},
		{Node: "node-remote-b", AZ: "az-remote-b"},
	}, activeSegmentNodes(alignedQuorumWithoutEndpoint()))
}

func TestOrderedQuorumReadCandidates_PrefersLocalWhenEndpointOmitted(t *testing.T) {
	t.Setenv("REGION", "region-local")
	t.Setenv("AVAILABILITY_ZONE", "az-local")

	candidates := orderedQuorumReadCandidates(alignedQuorumWithoutEndpoint(), nil)

	assert.Equal(t, "node-local", candidates[0].node)
	assert.Equal(t, topology.ScopeLocal, candidates[0].azScope)
}

// Segments written before the deprecation carry an endpoint on every replica.
// Where that endpoint disagrees with the positional pairing, it is the endpoint
// that is authoritative — the lists were joined by address when they were written.
func TestQuorumNodeScopes_LegacyEndpointWinsWhenOrderDisagrees(t *testing.T) {
	t.Setenv("REGION", "region-local")
	t.Setenv("AVAILABILITY_ZONE", "az-local")

	quorum := &proto.QuorumInfo{
		Id: 1, Aq: 2, Es: 2, Wq: 2,
		Nodes: []string{"node-a", "node-b"},
		Replicas: []*proto.QuorumNode{
			{Endpoint: "node-b", Region: "region-local", Az: "az-local"},
			{Endpoint: "node-a", Region: "region-remote", Az: "az-remote"},
		},
	}

	assert.Equal(t,
		[]string{topology.ScopeCrossRegion, topology.ScopeLocal},
		quorumNodeScopes(quorum))
}

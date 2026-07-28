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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_meta"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

type completeNodeResponse struct {
	lastEntryID int64
	completeErr error
	clientErr   error
}

// TestCompleteSegmentQuorum_ResponseMatrix documents and covers the completion
// quorum decision matrix.
//
// Each replica response belongs to exactly one class:
//   - Q (qualified): Finalize RPC succeeds and local LastEntryId >= target LAC.
//   - P (partial): Finalize RPC succeeds but local LastEntryId < target LAC.
//     The replica is durably frozen for read failover, but does not count toward Aq.
//   - E (error): client acquisition or Finalize RPC fails and does not count toward Aq.
//
// Completion succeeds iff the number of Q responses reaches Aq. Node ordering is
// intentionally not enumerated because responses are collected concurrently and
// the decision depends only on the qualified count.
//
// For the canonical Es=3, Aq=2 matrix (order-independent):
//
//	QQQ, QQP, QQE  -> success
//	QPP, QPE, QEE  -> failure
//	PPP, PPE, PEE, EEE -> failure
//
// The cases below also cover Aq=1/Aq=Es boundaries, client acquisition errors,
// the empty-segment LAC sentinel (-1), and the first valid entry ID (0).
func TestCompleteSegmentQuorum_ResponseMatrix(t *testing.T) {
	completeFailure := errors.New("complete failed")
	clientFailure := errors.New("client unavailable")

	tests := []struct {
		name      string
		targetLAC int64
		aq        int32
		responses []completeNodeResponse
		wantErr   bool
	}{
		// Aq=2 canonical Q/P/E combinations for a three-node ensemble.
		{name: "Q Q Q succeeds", targetLAC: 5, aq: 2, responses: []completeNodeResponse{{5, nil, nil}, {5, nil, nil}, {5, nil, nil}}},
		{name: "Q ahead P succeeds", targetLAC: 5, aq: 2, responses: []completeNodeResponse{{5, nil, nil}, {6, nil, nil}, {4, nil, nil}}},
		{name: "Q Q E succeeds", targetLAC: 5, aq: 2, responses: []completeNodeResponse{{5, nil, nil}, {5, nil, nil}, {-1, completeFailure, nil}}},
		{name: "Q Q client error succeeds", targetLAC: 5, aq: 2, responses: []completeNodeResponse{{5, nil, nil}, {5, nil, nil}, {-1, nil, clientFailure}}},
		{name: "Q P P fails", targetLAC: 5, aq: 2, responses: []completeNodeResponse{{5, nil, nil}, {4, nil, nil}, {3, nil, nil}}, wantErr: true},
		{name: "Q P E fails", targetLAC: 5, aq: 2, responses: []completeNodeResponse{{5, nil, nil}, {4, nil, nil}, {-1, completeFailure, nil}}, wantErr: true},
		{name: "Q E E fails", targetLAC: 5, aq: 2, responses: []completeNodeResponse{{5, nil, nil}, {-1, completeFailure, nil}, {-1, completeFailure, nil}}, wantErr: true},
		{name: "P P P fails", targetLAC: 5, aq: 2, responses: []completeNodeResponse{{4, nil, nil}, {3, nil, nil}, {2, nil, nil}}, wantErr: true},
		{name: "P P E fails", targetLAC: 5, aq: 2, responses: []completeNodeResponse{{4, nil, nil}, {3, nil, nil}, {-1, completeFailure, nil}}, wantErr: true},
		{name: "P E E fails", targetLAC: 5, aq: 2, responses: []completeNodeResponse{{4, nil, nil}, {-1, completeFailure, nil}, {-1, completeFailure, nil}}, wantErr: true},
		{name: "E E E fails", targetLAC: 5, aq: 2, responses: []completeNodeResponse{{-1, completeFailure, nil}, {-1, completeFailure, nil}, {-1, completeFailure, nil}}, wantErr: true},

		// Ack-quorum boundaries.
		{name: "Aq one needs one qualified", targetLAC: 5, aq: 1, responses: []completeNodeResponse{{5, nil, nil}, {4, nil, nil}, {-1, completeFailure, nil}}},
		{name: "Aq all requires every replica qualified", targetLAC: 5, aq: 3, responses: []completeNodeResponse{{5, nil, nil}, {5, nil, nil}, {5, nil, nil}}},
		{name: "Aq all rejects one partial", targetLAC: 5, aq: 3, responses: []completeNodeResponse{{5, nil, nil}, {5, nil, nil}, {4, nil, nil}}, wantErr: true},

		// LAC sentinel and first-entry boundaries.
		{name: "empty target minus one counts empty replicas", targetLAC: -1, aq: 2, responses: []completeNodeResponse{{-1, nil, nil}, {-1, nil, nil}, {-1, nil, nil}}},
		{name: "entry zero target has two qualified replicas", targetLAC: 0, aq: 2, responses: []completeNodeResponse{{0, nil, nil}, {0, nil, nil}, {-1, nil, nil}}},
		{name: "entry zero target with one qualified fails", targetLAC: 0, aq: 2, responses: []completeNodeResponse{{0, nil, nil}, {-1, nil, nil}, {-1, completeFailure, nil}}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockMetadata := mocks_meta.NewMetadataProvider(t)
			mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
			nodes := make([]string, len(tt.responses))

			for i, response := range tt.responses {
				node := fmt.Sprintf("node%d", i+1)
				nodes[i] = node
				if response.clientErr != nil {
					mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, node).Return(nil, response.clientErr)
					continue
				}
				mockClient := mocks_logstore_client.NewLogStoreClient(t)
				mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, node).Return(mockClient, nil)
				mockClient.EXPECT().CompleteSegment(
					mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1), tt.targetLAC,
				).Return(response.lastEntryID, response.completeErr)
			}

			cfg := &config.Configuration{
				Woodpecker: config.WoodpeckerConfig{
					Client: config.ClientConfig{
						SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
					},
				},
			}
			segmentMeta := &meta.SegmentMeta{
				Metadata: &proto.SegmentMetadata{SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1},
				Revision: 1,
			}
			sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
			impl := sh.(*segmentHandleImpl)
			quorum := &proto.QuorumInfo{Id: 1, Aq: tt.aq, Es: int32(len(nodes)), Wq: int32(len(nodes)), Nodes: nodes}

			err := impl.completeSegmentQuorum(context.Background(), quorum, tt.targetLAC)
			if tt.wantErr {
				assert.Error(t, err)
				assert.True(t, werr.ErrAppendOpQuorumFailed.Is(err), "expected quorum failure, got %v", err)
				assert.Equal(t, int64(-1), impl.lastAddConfirmed.Load(), "failed completion must not advance LAC")
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.targetLAC, impl.lastAddConfirmed.Load())
		})
	}
}

func TestPrepareComplete_DoesNotLowerKnownConfirmedLAC(t *testing.T) {
	mockMetadata := mocks_meta.NewMetadataProvider(t)
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockClient := mocks_logstore_client.NewLogStoreClient(t)
	cfg := &config.Configuration{
		Woodpecker: config.WoodpeckerConfig{
			Client: config.ClientConfig{
				SegmentAppend: config.SegmentAppendConfig{QueueSize: 10, MaxRetries: 2},
			},
		},
	}

	mockClientPool.EXPECT().GetLogStoreClient(mock.Anything, "node1").Return(mockClient, nil)
	mockClient.EXPECT().FenceSegment(mock.Anything, mock.Anything, mock.Anything, int64(1), int64(1)).Return(int64(4), nil)

	segmentMeta := &meta.SegmentMeta{
		Metadata: &proto.SegmentMetadata{
			SegNo: 1, State: proto.SegmentState_Active, LastEntryId: -1,
			Quorum: &proto.QuorumInfo{Id: 1, Aq: 1, Es: 1, Wq: 1, Nodes: []string{"node1"}},
		},
		Revision: 1,
	}
	sh := NewSegmentHandle(context.Background(), 1, "testLog", segmentMeta, mockMetadata, mockClientPool, cfg, false, nil)
	impl := sh.(*segmentHandleImpl)
	impl.lastAddConfirmed.Store(5)

	_, targetLAC, err := impl.prepareComplete(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(5), targetLAC)
}

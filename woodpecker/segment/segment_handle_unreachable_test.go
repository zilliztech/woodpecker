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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/proto"
)

// A replica that cannot be reached is remembered for the next segment's
// selection, also when its failure arrives after the entry already completed on
// the other replicas, which is how production saw it. A replica that answered
// with a refusal is not.
func TestHandleAppendRequestFailure_RemembersUnreachableReplicas(t *testing.T) {
	h := readTimeoutTestHandle(t, proto.SegmentState_Active, mocks_logstore_client.NewLogStoreClientPool(t)).(*segmentHandleImpl)
	ctx := context.Background()

	// No pending op for these entries: they completed already.
	h.HandleAppendRequestFailure(ctx, 7, status.Error(codes.Unavailable, "connection refused"), 0, "node1")
	h.HandleAppendRequestFailure(ctx, 8, status.Error(codes.Canceled, "grpc: the client connection is closing"), 1, "node2")
	h.HandleAppendRequestFailure(ctx, 9, context.DeadlineExceeded, 1, "node2")
	h.HandleAppendRequestFailure(ctx, 10, werr.ErrSegmentFenced, 2, "node3")

	assert.Equal(t, []string{"node1", "node2"}, h.UnreachableReplicas())
}

func TestIsReplicaUnreachable(t *testing.T) {
	assert.True(t, isReplicaUnreachable(status.Error(codes.Unavailable, "x")))
	assert.True(t, isReplicaUnreachable(status.Error(codes.Canceled, "grpc: the client connection is closing")))
	assert.True(t, isReplicaUnreachable(context.DeadlineExceeded))
	assert.False(t, isReplicaUnreachable(nil))
	assert.False(t, isReplicaUnreachable(werr.ErrSegmentFenced))
	assert.False(t, isReplicaUnreachable(status.Error(codes.Canceled, "context canceled")))
}

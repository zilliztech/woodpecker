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

	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_logstore_client"
	"github.com/zilliztech/woodpecker/mocks/mocks_woodpecker/mocks_segment_handle"
	"github.com/zilliztech/woodpecker/proto"
)

func TestNewAppendRequestRetryOp(t *testing.T) {
	ctx := context.Background()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Aq:    1,
		Es:    1,
		Wq:    1,
		Nodes: []string{"127.0.0.1"},
	}

	innerOp := NewAppendOp(
		"test-bucket",
		"test-root",
		1,
		1,
		0,
		[]byte("test-data"),
		func(segmentId int64, entryId int64, err error) {},
		mockClientPool,
		mockSegHandle,
		quorumInfo,
	)

	retryOp := NewAppendRequestRetryOp(ctx, 0, innerOp)
	assert.NotNil(t, retryOp)
	assert.Equal(t, 0, retryOp.serverIndex)
	assert.Equal(t, innerOp, retryOp.innerOp)
}

func TestAppendRequestRetryOp_Identifier(t *testing.T) {
	ctx := context.Background()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Aq:    1,
		Es:    1,
		Wq:    1,
		Nodes: []string{"127.0.0.1"},
	}

	innerOp := NewAppendOp(
		"test-bucket",
		"test-root",
		10,
		20,
		30,
		[]byte("test-data"),
		func(segmentId int64, entryId int64, err error) {},
		mockClientPool,
		mockSegHandle,
		quorumInfo,
	)

	retryOp := NewAppendRequestRetryOp(ctx, 0, innerOp)

	// Identifier should delegate to innerOp
	expectedIdentifier := "10/20/30"
	assert.Equal(t, expectedIdentifier, retryOp.Identifier())
}

func TestAppendRequestRetryOp_IdentifierDelegatesToInnerOp(t *testing.T) {
	ctx := context.Background()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Aq:    1,
		Es:    1,
		Wq:    1,
		Nodes: []string{"127.0.0.1"},
	}

	innerOp := NewAppendOp(
		"bucket",
		"root",
		99,
		88,
		77,
		[]byte("data"),
		func(segmentId int64, entryId int64, err error) {},
		mockClientPool,
		mockSegHandle,
		quorumInfo,
	)

	retryOp := NewAppendRequestRetryOp(ctx, 0, innerOp)

	// Both should return the same identifier
	assert.Equal(t, innerOp.Identifier(), retryOp.Identifier())
}

func TestAppendRequestRetryOp_ImplementsOperationInterface(t *testing.T) {
	ctx := context.Background()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Aq:    1,
		Es:    1,
		Wq:    1,
		Nodes: []string{"127.0.0.1"},
	}

	innerOp := NewAppendOp(
		"bucket",
		"root",
		1,
		1,
		0,
		[]byte("data"),
		func(segmentId int64, entryId int64, err error) {},
		mockClientPool,
		mockSegHandle,
		quorumInfo,
	)

	var op Operation = NewAppendRequestRetryOp(ctx, 0, innerOp)
	assert.NotNil(t, op)
	assert.Equal(t, "1/1/0", op.Identifier())
}

func TestAppendRequestRetryOp_DifferentServerIndices(t *testing.T) {
	ctx := context.Background()
	mockClientPool := mocks_logstore_client.NewLogStoreClientPool(t)
	mockSegHandle := mocks_segment_handle.NewSegmentHandle(t)
	quorumInfo := &proto.QuorumInfo{
		Id:    1,
		Aq:    2,
		Es:    3,
		Wq:    3,
		Nodes: []string{"node1", "node2", "node3"},
	}

	innerOp := NewAppendOp(
		"bucket",
		"root",
		1,
		1,
		0,
		[]byte("data"),
		func(segmentId int64, entryId int64, err error) {},
		mockClientPool,
		mockSegHandle,
		quorumInfo,
	)

	// Create retry ops for different server indices
	retryOp0 := NewAppendRequestRetryOp(ctx, 0, innerOp)
	retryOp1 := NewAppendRequestRetryOp(ctx, 1, innerOp)
	retryOp2 := NewAppendRequestRetryOp(ctx, 2, innerOp)

	assert.Equal(t, 0, retryOp0.serverIndex)
	assert.Equal(t, 1, retryOp1.serverIndex)
	assert.Equal(t, 2, retryOp2.serverIndex)

	// All should have the same identifier since they refer to the same inner op
	assert.Equal(t, retryOp0.Identifier(), retryOp1.Identifier())
	assert.Equal(t, retryOp1.Identifier(), retryOp2.Identifier())
}

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

package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/zilliztech/woodpecker/common/werr"
	mocks_server "github.com/zilliztech/woodpecker/mocks/mocks_server"
)

func TestLocalPool_NewLogStoreClientPoolLocal(t *testing.T) {
	mockStore := mocks_server.NewLogStore(t)
	pool := NewLogStoreClientPoolLocal(mockStore)
	assert.NotNil(t, pool)
}

func TestLocalPool_GetLogStoreClient_Success(t *testing.T) {
	mockStore := mocks_server.NewLogStore(t)
	pool := NewLogStoreClientPoolLocal(mockStore)
	ctx := context.Background()

	client, err := pool.GetLogStoreClient(ctx, "target1")
	assert.NoError(t, err)
	assert.NotNil(t, client)
	assert.False(t, client.IsRemoteClient())
}

func TestLocalPool_GetLogStoreClient_ReturnsNewClientEachTime(t *testing.T) {
	mockStore := mocks_server.NewLogStore(t)
	pool := NewLogStoreClientPoolLocal(mockStore)
	ctx := context.Background()

	client1, err1 := pool.GetLogStoreClient(ctx, "target1")
	assert.NoError(t, err1)

	client2, err2 := pool.GetLogStoreClient(ctx, "target2")
	assert.NoError(t, err2)

	// Each call returns a new local client wrapping the same store
	assert.NotNil(t, client1)
	assert.NotNil(t, client2)
}

func TestLocalPool_GetLogStoreClient_DifferentTargets(t *testing.T) {
	mockStore := mocks_server.NewLogStore(t)
	pool := NewLogStoreClientPoolLocal(mockStore)
	ctx := context.Background()

	// Local pool ignores the target parameter - always returns a local client
	client1, err1 := pool.GetLogStoreClient(ctx, "localhost:8080")
	assert.NoError(t, err1)
	assert.NotNil(t, client1)

	client2, err2 := pool.GetLogStoreClient(ctx, "remotehost:9090")
	assert.NoError(t, err2)
	assert.NotNil(t, client2)
}

func TestLocalPool_GetLogStoreClient_AfterClose(t *testing.T) {
	mockStore := mocks_server.NewLogStore(t)
	pool := NewLogStoreClientPoolLocal(mockStore)
	ctx := context.Background()

	// Close the pool
	err := pool.Close(ctx)
	assert.NoError(t, err)

	// Attempt to get a client after closing should fail
	client, err := pool.GetLogStoreClient(ctx, "target1")
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

func TestLocalPool_Close_Success(t *testing.T) {
	mockStore := mocks_server.NewLogStore(t)
	pool := NewLogStoreClientPoolLocal(mockStore)
	ctx := context.Background()

	err := pool.Close(ctx)
	assert.NoError(t, err)
}

func TestLocalPool_Close_Idempotent(t *testing.T) {
	mockStore := mocks_server.NewLogStore(t)
	pool := NewLogStoreClientPoolLocal(mockStore)
	ctx := context.Background()

	// Close twice - should not panic or error
	err1 := pool.Close(ctx)
	assert.NoError(t, err1)

	err2 := pool.Close(ctx)
	assert.NoError(t, err2)
}

func TestLocalPool_Clear_NoOp(t *testing.T) {
	mockStore := mocks_server.NewLogStore(t)
	pool := NewLogStoreClientPoolLocal(mockStore)
	ctx := context.Background()

	// Clear is a no-op in local pool - should not panic
	pool.Clear(ctx, "target1")
	pool.Clear(ctx, "target2")
	pool.Clear(ctx, "")

	// Pool should still work after clearing
	client, err := pool.GetLogStoreClient(ctx, "target1")
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestLocalPool_ImplementsInterface(t *testing.T) {
	// Verify that logStoreClientPoolLocal implements LogStoreClientPool
	var _ LogStoreClientPool = (*logStoreClientPoolLocal)(nil)
}

func TestLocalPool_ClientUsesUnderlyingStore(t *testing.T) {
	// Verify that the client returned by the pool actually delegates to the mock store
	mockStore := mocks_server.NewLogStore(t)
	pool := NewLogStoreClientPoolLocal(mockStore)
	ctx := context.Background()

	client, err := pool.GetLogStoreClient(ctx, "target1")
	assert.NoError(t, err)

	// Use the client to call GetLastAddConfirmed and verify it delegates to the store
	mockStore.EXPECT().GetSegmentLastAddConfirmed(mock.Anything, "bucket", "root", int64(1), int64(0)).
		Return(int64(77), nil)

	lac, err := client.GetLastAddConfirmed(ctx, "bucket", "root", 1, 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(77), lac)
}

func TestLocalPool_ClientCloseDoesNotAffectPool(t *testing.T) {
	// Closing an individual client should not affect the pool
	mockStore := mocks_server.NewLogStore(t)
	pool := NewLogStoreClientPoolLocal(mockStore)
	ctx := context.Background()

	client1, err := pool.GetLogStoreClient(ctx, "target1")
	assert.NoError(t, err)

	// Close the individual client
	err = client1.Close(ctx)
	assert.NoError(t, err)

	// Pool should still be able to return new clients
	client2, err := pool.GetLogStoreClient(ctx, "target2")
	assert.NoError(t, err)
	assert.NotNil(t, client2)
}

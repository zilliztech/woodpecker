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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/werr"
)

func TestRemotePool_NewLogStoreClientPool(t *testing.T) {
	pool := NewLogStoreClientPool(4*1024*1024, 4*1024*1024)
	assert.NotNil(t, pool)
}

func TestRemotePool_Close_Idempotent(t *testing.T) {
	pool := NewLogStoreClientPool(4*1024*1024, 4*1024*1024)
	ctx := context.Background()

	err1 := pool.Close(ctx)
	assert.NoError(t, err1)

	err2 := pool.Close(ctx)
	assert.NoError(t, err2)
}

func TestRemotePool_GetLogStoreClient_AfterClose(t *testing.T) {
	pool := NewLogStoreClientPool(4*1024*1024, 4*1024*1024)
	ctx := context.Background()

	err := pool.Close(ctx)
	assert.NoError(t, err)

	client, err := pool.GetLogStoreClient(ctx, "target")
	assert.Error(t, err)
	assert.Nil(t, client)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

func TestRemotePool_Clear_NoTarget(t *testing.T) {
	pool := NewLogStoreClientPool(4*1024*1024, 4*1024*1024)
	ctx := context.Background()

	// Clear a target that doesn't exist - should not panic
	pool.Clear(ctx, "nonexistent-target")
}

func TestRemotePool_ImplementsInterface(t *testing.T) {
	var _ LogStoreClientPool = (*logStoreClientPool)(nil)
}

func TestRemotePool_GetLogStoreClient_Success(t *testing.T) {
	pool := NewLogStoreClientPool(4*1024*1024, 4*1024*1024)
	ctx := context.Background()

	// grpc.NewClient doesn't actually connect until first RPC
	client, err := pool.GetLogStoreClient(ctx, "localhost:12345")
	assert.NoError(t, err)
	assert.NotNil(t, client)
	assert.True(t, client.IsRemoteClient())
}

func TestRemotePool_GetLogStoreClient_CachesClient(t *testing.T) {
	pool := NewLogStoreClientPool(4*1024*1024, 4*1024*1024)
	ctx := context.Background()

	client1, err := pool.GetLogStoreClient(ctx, "localhost:12345")
	assert.NoError(t, err)

	client2, err := pool.GetLogStoreClient(ctx, "localhost:12345")
	assert.NoError(t, err)

	// Same target should return same cached client
	assert.Same(t, client1, client2)
}

func TestRemotePool_GetLogStoreClient_DifferentTargets(t *testing.T) {
	pool := NewLogStoreClientPool(4*1024*1024, 4*1024*1024)
	ctx := context.Background()

	client1, err := pool.GetLogStoreClient(ctx, "localhost:12345")
	assert.NoError(t, err)

	client2, err := pool.GetLogStoreClient(ctx, "localhost:12346")
	assert.NoError(t, err)

	// Different targets should return different clients
	assert.NotSame(t, client1, client2)
}

func TestRemotePool_Clear_ExistingTarget(t *testing.T) {
	pool := NewLogStoreClientPool(4*1024*1024, 4*1024*1024)
	ctx := context.Background()

	// Create a client for a target
	client1, err := pool.GetLogStoreClient(ctx, "localhost:12345")
	assert.NoError(t, err)
	assert.NotNil(t, client1)

	// Clear the target
	pool.Clear(ctx, "localhost:12345")

	// Getting a client for the same target should create a new one
	client2, err := pool.GetLogStoreClient(ctx, "localhost:12345")
	assert.NoError(t, err)
	assert.NotNil(t, client2)
	assert.NotSame(t, client1, client2)
}

func TestRemotePool_Close_WithClients(t *testing.T) {
	pool := NewLogStoreClientPool(4*1024*1024, 4*1024*1024)
	ctx := context.Background()

	// Create a few clients
	_, err := pool.GetLogStoreClient(ctx, "localhost:12345")
	assert.NoError(t, err)
	_, err = pool.GetLogStoreClient(ctx, "localhost:12346")
	assert.NoError(t, err)

	// Close should clean everything up
	err = pool.Close(ctx)
	assert.NoError(t, err)

	// After close, GetLogStoreClient should fail
	_, err = pool.GetLogStoreClient(ctx, "localhost:12345")
	assert.Error(t, err)
	assert.True(t, werr.ErrWoodpeckerClientClosed.Is(err))
}

// TestRemotePool_ConcurrentGetLogStoreClient_SameTarget verifies that concurrent
// requests for the same target correctly hit the double-check cache path (L79-81).
func TestRemotePool_ConcurrentGetLogStoreClient_SameTarget(t *testing.T) {
	pool := NewLogStoreClientPool(4*1024*1024, 4*1024*1024)
	ctx := context.Background()

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	clients := make([]LogStoreClient, goroutines)
	errs := make([]error, goroutines)

	// Synchronize goroutine start to maximize contention
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			<-start
			clients[i], errs[i] = pool.GetLogStoreClient(ctx, "localhost:12345")
		}()
	}

	close(start) // release all at once
	wg.Wait()

	// All goroutines should succeed with the same client instance
	for i := 0; i < goroutines; i++ {
		assert.NoError(t, errs[i])
		assert.NotNil(t, clients[i])
		assert.Same(t, clients[0], clients[i])
	}
}

// TestRemotePool_GetLogStoreClient_RaceWithClose runs GetLogStoreClient and Close
// concurrently to exercise the double-check for clientClosed inside the write lock (L72-74).
// The race window between RUnlock (L62) and Lock (L67) is very small, so this test
// runs multiple iterations to increase the chance of hitting it. Even if the exact
// coverage line isn't hit every time, -race will detect any data-race issues.
func TestRemotePool_GetLogStoreClient_RaceWithClose(t *testing.T) {
	for i := 0; i < 100; i++ {
		pool := NewLogStoreClientPool(4*1024*1024, 4*1024*1024)
		ctx := context.Background()

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			// Request an uncached target — will pass read-lock, then race for write-lock
			_, _ = pool.GetLogStoreClient(ctx, "uncached-target")
		}()

		go func() {
			defer wg.Done()
			// Close races with GetLogStoreClient for the write lock
			_ = pool.Close(ctx)
		}()

		wg.Wait()
		// No deadlock and no panic is the success criterion
	}
}

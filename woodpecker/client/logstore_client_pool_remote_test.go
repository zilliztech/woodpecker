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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/connectivity"

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

// TestRemotePool_RebuildsOnTransientFailure is the end-to-end integration
// test for the "stale-IP-after-pod-restart" fix.
//
// Scenario: the pool caches a gRPC ClientConn pinned to an address. The peer
// goes away (pod killed / network partition). Without the fix, subsequent
// GetLogStoreClient calls keep returning the same broken client forever,
// causing the observed multi-tens-of-minutes stall in Milvus streamingNode
// after all Woodpecker pods are recreated with new IPs. With the fix, a
// transport-level error on any RPC causes logStoreClientRemote to evict
// itself from the pool; the next GetLogStoreClient call builds a fresh
// ClientConn that re-resolves DNS and recovers in seconds.
//
// The test drives a real gRPC transport failure by dialing a port that had
// a listener and then closed it — every dial fails with connection refused.
// It asserts the two observable consequences of the fix:
//  1. After a failed RPC, the pool no longer holds the client/connection
//     entry for that target (client self-evicted).
//  2. The next GetLogStoreClient call returns a DIFFERENT client instance
//     backed by a different *grpc.ClientConn.
func TestRemotePool_RebuildsOnTransientFailure(t *testing.T) {
	// Reserve and immediately release a port so that subsequent dials to it
	// are guaranteed to fail with "connection refused".
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	target := lis.Addr().String()
	require.NoError(t, lis.Close())

	poolIface := NewLogStoreClientPool(4*1024*1024, 4*1024*1024)
	pool := poolIface.(*logStoreClientPool)
	defer func() { _ = pool.Close(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// First acquisition populates the cache.
	client1, err := pool.GetLogStoreClient(ctx, target)
	require.NoError(t, err)
	require.NotNil(t, client1)

	pool.RLock()
	cnx1 := pool.connections[target]
	pool.RUnlock()
	require.NotNil(t, cnx1, "pool should hold a ClientConn for the target after first acquisition")

	// Trigger a real RPC. The dial will fail with "connection refused",
	// surfacing as codes.Unavailable; logStoreClientRemote must then
	// self-evict from the pool.
	callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
	_, rpcErr := client1.FenceSegment(callCtx, "bucket", "root", 1, 1)
	callCancel()
	require.Error(t, rpcErr, "RPC to a dead port should fail")

	// Wait for the self-eviction to complete (Clear happens in the deferred
	// maybeDropCachedConn, effectively synchronously, but we poll to be
	// robust against any future async path).
	require.Eventually(t, func() bool {
		pool.RLock()
		_, stillCached := pool.clients[target]
		pool.RUnlock()
		return !stillCached
	}, 5*time.Second, 20*time.Millisecond,
		"after a transport-level RPC failure, logStoreClientRemote must self-evict "+
			"from the pool so retries re-resolve DNS")

	// The next acquisition must produce a BRAND-NEW client backed by a
	// different *grpc.ClientConn. This is what gives us fast recovery on a
	// peer IP change: the new ClientConn spins up a new DNS resolver on its
	// first RPC and picks up the peer's new address.
	client2, err := pool.GetLogStoreClient(ctx, target)
	require.NoError(t, err)
	require.NotNil(t, client2)
	assert.NotSame(t, client1, client2,
		"pool must hand out a fresh client after the old one self-evicted; "+
			"reusing the stale ClientConn is what caused the ~30min stall bug")

	pool.RLock()
	cnx2 := pool.connections[target]
	pool.RUnlock()
	require.NotNil(t, cnx2)
	assert.NotSame(t, cnx1, cnx2,
		"the replacement must use a fresh *grpc.ClientConn (not the stale one)")
	assert.NotEqual(t, connectivity.Shutdown, cnx2.GetState(),
		"replacement ClientConn must not already be in SHUTDOWN")
}

// TestRemotePool_ClearForcesRebuild is a focused test for the explicit Clear
// path used by fenceSegmentOnNode / sendWriteRequest / requestNodesFromSeed
// when they observe a transport-level RPC error. It verifies that Clear
// removes both the client wrapper and the underlying gRPC connection, so the
// next GetLogStoreClient builds a fresh one instead of handing back the dead
// cached entry.
//
// This complements TestRemotePool_RebuildsOnTransientFailure: the state-check
// path handles connections that already moved into TRANSIENT_FAILURE, while
// Clear gives callers a way to drop a connection immediately on a transport
// error even before the subchannel state flips.
func TestRemotePool_ClearForcesRebuild(t *testing.T) {
	poolIface := NewLogStoreClientPool(4*1024*1024, 4*1024*1024)
	pool := poolIface.(*logStoreClientPool)
	defer func() { _ = pool.Close(context.Background()) }()

	ctx := context.Background()
	const target = "127.0.0.1:12349"

	client1, err := pool.GetLogStoreClient(ctx, target)
	require.NoError(t, err)

	pool.RLock()
	cnx1 := pool.connections[target]
	pool.RUnlock()
	require.NotNil(t, cnx1)

	pool.Clear(ctx, target)

	// After Clear both maps should no longer contain the target.
	pool.RLock()
	_, stillHasClient := pool.clients[target]
	_, stillHasCnx := pool.connections[target]
	pool.RUnlock()
	assert.False(t, stillHasClient, "Clear should remove the cached client wrapper")
	assert.False(t, stillHasCnx, "Clear should remove the cached gRPC connection")

	// Next acquisition must return a fresh wrapper backed by a fresh conn.
	client2, err := pool.GetLogStoreClient(ctx, target)
	require.NoError(t, err)
	assert.NotSame(t, client1, client2,
		"after Clear, the pool must build a fresh client (not return the evicted one)")

	pool.RLock()
	cnx2 := pool.connections[target]
	pool.RUnlock()
	assert.NotSame(t, cnx1, cnx2,
		"after Clear, the pool must build a fresh gRPC connection so the next "+
			"dial re-resolves DNS")
}

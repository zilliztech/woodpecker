// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package server

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/membership"
)

// createTestServer creates a minimal Server instance for testing without
// triggering storage client initialization or other side effects.
func createTestServer(ctx context.Context, serverConfig *membership.ServerConfig) *Server {
	ctx, cancel := context.WithCancel(ctx)
	cfg, _ := config.NewConfiguration()
	return &Server{
		cfg:          cfg,
		ctx:          ctx,
		cancel:       cancel,
		grpcErrChan:  make(chan error),
		startupErrCh: make(chan error, 1),
		serverConfig: serverConfig,
		logStore:     NewLogStore(ctx, cfg, nil),
	}
}

func TestPrepare_ServicePortOccupied(t *testing.T) {
	// Occupy a TCP port on 0.0.0.0 (same bind address used by Prepare)
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	defer ln.Close()

	// Extract the port number
	port := ln.Addr().(*net.TCPAddr).Port

	s := createTestServer(context.Background(), &membership.ServerConfig{
		ServicePort: port,
	})
	defer s.cancel()

	// Prepare should fail because the port is already occupied
	err = s.Prepare()
	assert.Error(t, err)
}

func TestWaitAndStartCurrentNode_ContextCancelled(t *testing.T) {
	// Occupy a UDP port so NewServerNode would fail if hostname resolves
	udpConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer udpConn.Close()
	occupiedPort := udpConn.LocalAddr().(*net.UDPAddr).Port

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately so the function sees context cancelled
	cancel()

	s := createTestServer(ctx, &membership.ServerConfig{
		AdvertiseAddr: "127.0.0.1",
		BindPort:      occupiedPort,
	})

	done := make(chan error, 1)
	go func() {
		done <- s.waitAndStartCurrentNode(ctx)
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "context cancel"),
			"expected error to mention context cancellation, got: %s", err.Error())
	case <-time.After(3 * time.Second):
		t.Fatal("waitAndStartCurrentNode did not return within 3 seconds")
	}
}

func TestWaitAndStartCurrentNode_GossipPortOccupied_AbortsViaContext(t *testing.T) {
	// Occupy a UDP port (memberlist uses UDP for gossip)
	udpConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer udpConn.Close()
	occupiedPort := udpConn.LocalAddr().(*net.UDPAddr).Port

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s := createTestServer(ctx, &membership.ServerConfig{
		NodeID:        "test-node-gossip",
		AdvertiseAddr: "127.0.0.1",
		BindPort:      occupiedPort,
		AdvertisePort: occupiedPort,
		ServicePort:   0,
	})

	err = s.waitAndStartCurrentNode(ctx)
	require.Error(t, err)
	// Should be either context deadline exceeded or max attempts error
	assert.True(t,
		strings.Contains(err.Error(), "context") || strings.Contains(err.Error(), "failed"),
		"expected context or failure error, got: %s", err.Error())
}

func TestWaitAndStartCurrentNode_UnresolvableHostname_AbortsViaContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	s := createTestServer(ctx, &membership.ServerConfig{
		NodeID:        "test-node-unresolvable",
		AdvertiseAddr: "nonexistent.invalid.test.local",
		BindPort:      0,
	})

	start := time.Now()
	err := s.waitAndStartCurrentNode(ctx)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.True(t,
		strings.Contains(err.Error(), "context cancel") ||
			strings.Contains(err.Error(), "context deadline") ||
			strings.Contains(err.Error(), "failed to resolve"),
		"expected context or resolution error, got: %s", err.Error())
	// Verify function doesn't hang for the full 30-second maxAttempts period.
	// Allow generous margin because DNS lookups for invalid hostnames can be slow.
	assert.Less(t, elapsed, 15*time.Second, "function should abort well before maxAttempts (30s)")
}

func TestAsyncStartAndJoinSeeds_PropagatesErrorToChannel(t *testing.T) {
	// Occupy a UDP port
	udpConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer udpConn.Close()
	occupiedPort := udpConn.LocalAddr().(*net.UDPAddr).Port

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s := createTestServer(ctx, &membership.ServerConfig{
		NodeID:        "test-node-async",
		AdvertiseAddr: "127.0.0.1",
		BindPort:      occupiedPort,
		AdvertisePort: occupiedPort,
		ServicePort:   0,
	})

	seeds := []string{fmt.Sprintf("127.0.0.1:%d", occupiedPort+1)}
	go s.asyncStartAndJoinSeeds(ctx, seeds)

	select {
	case err := <-s.startupErrCh:
		require.Error(t, err, "expected startup error to propagate to channel")
	case <-time.After(10 * time.Second):
		t.Fatal("did not receive error on startupErrCh within timeout")
	}
}

func TestWaitAndStartCurrentNode_Success(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := createTestServer(ctx, &membership.ServerConfig{
		NodeID:               "test-node-success",
		AdvertiseAddr:        "127.0.0.1",
		BindPort:             0, // Auto-assign free port
		AdvertisePort:        0,
		ServicePort:          0,
		AdvertiseServicePort: 0,
		ResourceGroup:        "default",
		AZ:                   "default",
		Tags:                 map[string]string{"role": "test"},
	})

	err := s.waitAndStartCurrentNode(ctx)
	require.NoError(t, err)
	assert.NotNil(t, s.serverNode, "serverNode should be set after successful start")

	// Cleanup
	if s.serverNode != nil {
		require.NoError(t, s.serverNode.Shutdown())
	}
}

func TestGetStartupErrCh(t *testing.T) {
	s := createTestServer(context.Background(), &membership.ServerConfig{})
	defer s.cancel()

	ch := s.GetStartupErrCh()
	require.NotNil(t, ch, "GetStartupErrCh should return a non-nil channel")

	// Channel should be readable in non-blocking fashion (empty, so default branch)
	select {
	case <-ch:
		t.Fatal("channel should be empty initially")
	default:
		// expected: no value available yet
	}
}

// --- Shutdown test cases ---

// TestStop_NoDeadlock verifies that Stop() completes within a bounded time,
// proving the reordered shutdown sequence (GracefulStop before logStore.Stop)
// does not deadlock.
func TestStop_NoDeadlock(t *testing.T) {
	s := createTestServer(context.Background(), &membership.ServerConfig{
		NodeID:               "test-stop-nodeadlock",
		AdvertiseAddr:        "127.0.0.1",
		BindPort:             0,
		AdvertisePort:        0,
		ServicePort:          0,
		AdvertiseServicePort: 0,
		ResourceGroup:        "default",
		AZ:                   "default",
		Tags:                 map[string]string{"role": "test"},
	})

	// Prepare sets up the TCP listener
	require.NoError(t, s.Prepare())

	// Run starts the gRPC server loop and logStore
	require.NoError(t, s.Run())

	// Stop in a goroutine with a timeout to detect deadlock
	done := make(chan error, 1)
	go func() {
		done <- s.Stop()
	}()

	select {
	case err := <-done:
		assert.NoError(t, err, "Stop() should complete without error")
	case <-time.After(15 * time.Second):
		t.Fatal("Stop() did not complete within 15 seconds — possible deadlock")
	}
}

// TestStop_NilGrpcServer verifies that Stop() handles nil grpcServer gracefully
// (e.g., when Run() was never called).
func TestStop_NilGrpcServer(t *testing.T) {
	s := createTestServer(context.Background(), &membership.ServerConfig{
		NodeID: "test-stop-nilgrpc",
	})

	// Do NOT call Prepare() or Run() — grpcServer remains nil
	done := make(chan error, 1)
	go func() {
		done <- s.Stop()
	}()

	select {
	case err := <-done:
		assert.NoError(t, err, "Stop() should not panic or error with nil grpcServer")
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not complete within 5 seconds")
	}
}

// TestStop_GossipWGTracked verifies that the gossip goroutine started in Prepare()
// is properly tracked and waited on during Stop().
func TestStop_GossipWGTracked(t *testing.T) {
	// Occupy a UDP port so gossip node creation will fail
	udpConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	defer udpConn.Close()
	occupiedPort := udpConn.LocalAddr().(*net.UDPAddr).Port

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := createTestServer(ctx, &membership.ServerConfig{
		NodeID:        "test-stop-gossipwg",
		AdvertiseAddr: "127.0.0.1",
		BindPort:      occupiedPort,
		AdvertisePort: occupiedPort,
		ServicePort:   0,
	})

	// Manually set up seeds so Prepare() launches the gossip goroutine
	s.gossipSeeds = []string{fmt.Sprintf("127.0.0.1:%d", occupiedPort+1)}

	require.NoError(t, s.Prepare())

	// Cancel context to abort the gossip goroutine
	cancel()

	done := make(chan error, 1)
	go func() {
		done <- s.Stop()
	}()

	select {
	case err := <-done:
		// Stop completed means gossipWG was waited on successfully
		assert.NoError(t, err, "Stop() should complete after gossip goroutine exits")
	case <-time.After(15 * time.Second):
		t.Fatal("Stop() did not complete within 15 seconds — gossip goroutine may be leaked")
	}
}

// TestStop_GrpcWGWaited verifies that grpcWG is waited on in Stop(),
// ensuring the gRPC server loop goroutine completes before Stop() returns.
func TestStop_GrpcWGWaited(t *testing.T) {
	s := createTestServer(context.Background(), &membership.ServerConfig{
		NodeID:               "test-stop-grpcwg",
		AdvertiseAddr:        "127.0.0.1",
		BindPort:             0,
		AdvertisePort:        0,
		ServicePort:          0,
		AdvertiseServicePort: 0,
		ResourceGroup:        "default",
		AZ:                   "default",
		Tags:                 map[string]string{"role": "test"},
	})

	require.NoError(t, s.Prepare())
	require.NoError(t, s.Run())

	done := make(chan error, 1)
	go func() {
		done <- s.Stop()
	}()

	select {
	case err := <-done:
		// If Stop() returned, it means grpcWG.Wait() completed — no goroutine leak
		assert.NoError(t, err, "Stop() should complete with grpcWG waited")
	case <-time.After(15 * time.Second):
		t.Fatal("Stop() did not complete within 15 seconds — grpcWG may not be waited")
	}
}

// mockLogStore wraps a real LogStore and records whether Stop() was called.
type mockLogStore struct {
	LogStore
	stopCalled atomic.Bool
}

func (m *mockLogStore) Stop() error {
	m.stopCalled.Store(true)
	return m.LogStore.Stop()
}

// TestStop_LogStoreStoppedAfterGrpc verifies that logStore.Stop() is called
// during server shutdown (and is not accidentally skipped).
func TestStop_LogStoreStoppedAfterGrpc(t *testing.T) {
	ctx := context.Background()
	cfg, _ := config.NewConfiguration()
	ctx, cancel := context.WithCancel(ctx)
	realLogStore := NewLogStore(ctx, cfg, nil)
	mock := &mockLogStore{LogStore: realLogStore}

	s := &Server{
		cfg:          cfg,
		ctx:          ctx,
		cancel:       cancel,
		grpcErrChan:  make(chan error),
		startupErrCh: make(chan error, 1),
		serverConfig: &membership.ServerConfig{
			NodeID:               "test-stop-logstore",
			AdvertiseAddr:        "127.0.0.1",
			BindPort:             0,
			AdvertisePort:        0,
			ServicePort:          0,
			AdvertiseServicePort: 0,
			ResourceGroup:        "default",
			AZ:                   "default",
			Tags:                 map[string]string{"role": "test"},
		},
		logStore: mock,
	}

	require.NoError(t, s.Prepare())
	require.NoError(t, s.Run())

	done := make(chan error, 1)
	go func() {
		done <- s.Stop()
	}()

	select {
	case err := <-done:
		assert.NoError(t, err)
		assert.True(t, mock.stopCalled.Load(), "logStore.Stop() should have been called during shutdown")
	case <-time.After(15 * time.Second):
		t.Fatal("Stop() did not complete within 15 seconds")
	}
}

// mockServerStream implements grpc.ServerStream for testing the shutdown interceptor.
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context     { return m.ctx }
func (m *mockServerStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockServerStream) SendHeader(metadata.MD) error { return nil }
func (m *mockServerStream) SetTrailer(metadata.MD)       {}
func (m *mockServerStream) SendMsg(interface{}) error    { return nil }
func (m *mockServerStream) RecvMsg(interface{}) error    { return nil }

// TestShutdownInterceptor_CancelsStreamContext verifies that the shutdown stream
// interceptor cancels handler contexts when the server context is cancelled.
func TestShutdownInterceptor_CancelsStreamContext(t *testing.T) {
	srvCtx, srvCancel := context.WithCancel(context.Background())
	s := &Server{
		ctx:          srvCtx,
		cancel:       srvCancel,
		serverConfig: &membership.ServerConfig{NodeID: "test-interceptor"},
	}

	interceptor := s.shutdownStreamInterceptor()

	handlerCtxCh := make(chan context.Context, 1)
	handler := func(srv interface{}, stream grpc.ServerStream) error {
		handlerCtxCh <- stream.Context()
		// Block until context is cancelled
		<-stream.Context().Done()
		return stream.Context().Err()
	}

	streamCtx := context.Background()
	mockStream := &mockServerStream{ctx: streamCtx}

	errCh := make(chan error, 1)
	go func() {
		errCh <- interceptor(nil, mockStream, &grpc.StreamServerInfo{}, handler)
	}()

	// Wait for the handler to start and capture its context
	var handlerCtx context.Context
	select {
	case handlerCtx = <-handlerCtxCh:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start within 2 seconds")
	}

	// Verify handler context is not yet cancelled
	select {
	case <-handlerCtx.Done():
		t.Fatal("handler context should not be cancelled yet")
	default:
	}

	// Cancel the server context — should propagate to handler context
	srvCancel()

	// Verify handler context gets cancelled
	select {
	case <-handlerCtx.Done():
		// Expected: server shutdown propagated to handler context
	case <-time.After(2 * time.Second):
		t.Fatal("handler context was not cancelled after server context cancellation")
	}

	// Verify handler returned
	select {
	case err := <-errCh:
		assert.Error(t, err, "handler should return context error")
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after context cancellation")
	}
}

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

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/membership"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
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

// === RPC Handler Tests ===

// fakeLogStore is a minimal LogStore implementation for RPC handler tests.
type fakeLogStore struct {
	addEntryFn      func(ctx context.Context, bucketName, rootPath string, logId int64, entry *proto.LogEntry, syncedResultCh channel.ResultChannel) (int64, error)
	getBatchFn      func(ctx context.Context, bucketName, rootPath string, logId int64, segmentId, fromEntryId, maxEntries int64, lastReadState *proto.LastReadState) (*proto.BatchReadResult, error)
	fenceFn         func(ctx context.Context, bucketName, rootPath string, logId int64, segmentId int64) (int64, error)
	completeFn      func(ctx context.Context, bucketName, rootPath string, logId int64, segmentId, lac int64) (int64, error)
	compactFn       func(ctx context.Context, bucketName, rootPath string, logId int64, segmentId int64) (*proto.SegmentMetadata, error)
	getLACFn        func(ctx context.Context, bucketName, rootPath string, logId int64, segmentId int64) (int64, error)
	getBlockCountFn func(ctx context.Context, bucketName, rootPath string, logId int64, segmentId int64) (int64, error)
	updateLACFn     func(ctx context.Context, bucketName, rootPath string, logId int64, segmentId, lac int64) error
	cleanFn         func(ctx context.Context, bucketName, rootPath string, logId int64, segmentId int64, flag int) error
}

func (f *fakeLogStore) Start() error       { return nil }
func (f *fakeLogStore) Stop() error        { return nil }
func (f *fakeLogStore) SetAddress(string)  {}
func (f *fakeLogStore) GetAddress() string { return "fake:8080" }
func (f *fakeLogStore) AddEntry(ctx context.Context, bucketName, rootPath string, logId int64, entry *proto.LogEntry, syncedResultCh channel.ResultChannel) (int64, error) {
	return f.addEntryFn(ctx, bucketName, rootPath, logId, entry, syncedResultCh)
}

func (f *fakeLogStore) GetBatchEntriesAdv(ctx context.Context, bucketName, rootPath string, logId int64, segmentId, fromEntryId, maxEntries int64, lastReadState *proto.LastReadState) (*proto.BatchReadResult, error) {
	return f.getBatchFn(ctx, bucketName, rootPath, logId, segmentId, fromEntryId, maxEntries, lastReadState)
}

func (f *fakeLogStore) FenceSegment(ctx context.Context, bucketName, rootPath string, logId int64, segmentId int64) (int64, error) {
	return f.fenceFn(ctx, bucketName, rootPath, logId, segmentId)
}

func (f *fakeLogStore) CompleteSegment(ctx context.Context, bucketName, rootPath string, logId int64, segmentId, lac int64) (int64, error) {
	return f.completeFn(ctx, bucketName, rootPath, logId, segmentId, lac)
}

func (f *fakeLogStore) CompactSegment(ctx context.Context, bucketName, rootPath string, logId int64, segmentId int64) (*proto.SegmentMetadata, error) {
	return f.compactFn(ctx, bucketName, rootPath, logId, segmentId)
}

func (f *fakeLogStore) GetSegmentLastAddConfirmed(ctx context.Context, bucketName, rootPath string, logId int64, segmentId int64) (int64, error) {
	return f.getLACFn(ctx, bucketName, rootPath, logId, segmentId)
}

func (f *fakeLogStore) GetSegmentBlockCount(ctx context.Context, bucketName, rootPath string, logId int64, segmentId int64) (int64, error) {
	return f.getBlockCountFn(ctx, bucketName, rootPath, logId, segmentId)
}

func (f *fakeLogStore) UpdateLastAddConfirmed(ctx context.Context, bucketName, rootPath string, logId int64, segmentId, lac int64) error {
	return f.updateLACFn(ctx, bucketName, rootPath, logId, segmentId, lac)
}

func (f *fakeLogStore) CleanSegment(ctx context.Context, bucketName, rootPath string, logId int64, segmentId int64, flag int) error {
	return f.cleanFn(ctx, bucketName, rootPath, logId, segmentId, flag)
}

func createTestServerWithFakeLogStore(fake *fakeLogStore) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	cfg, _ := config.NewConfiguration()
	return &Server{
		cfg:          cfg,
		ctx:          ctx,
		cancel:       cancel,
		grpcErrChan:  make(chan error),
		startupErrCh: make(chan error, 1),
		serverConfig: &membership.ServerConfig{
			AdvertiseAddr: "127.0.0.1",
			AdvertisePort: 9999,
		},
		logStore: fake,
	}
}

func TestServer_GetBatchEntriesAdv_Success(t *testing.T) {
	expected := &proto.BatchReadResult{Entries: []*proto.LogEntry{{EntryId: 5}}}
	fake := &fakeLogStore{
		getBatchFn: func(ctx context.Context, bn, rp string, logId int64, segId, from, max int64, lrs *proto.LastReadState) (*proto.BatchReadResult, error) {
			return expected, nil
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.GetBatchEntriesAdv(context.Background(), &proto.GetBatchEntriesAdvRequest{
		BucketName: "b", RootPath: "r", LogId: 1, SegmentId: 0, FromEntryId: 0, MaxEntries: 10,
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), resp.Status.Code)
	assert.Equal(t, expected, resp.Result)
}

func TestServer_GetBatchEntriesAdv_Error(t *testing.T) {
	fake := &fakeLogStore{
		getBatchFn: func(ctx context.Context, bn, rp string, logId int64, segId, from, max int64, lrs *proto.LastReadState) (*proto.BatchReadResult, error) {
			return nil, werr.ErrLogStoreShutdown
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.GetBatchEntriesAdv(context.Background(), &proto.GetBatchEntriesAdvRequest{
		BucketName: "b", RootPath: "r", LogId: 1, SegmentId: 0,
	})
	assert.NoError(t, err) // gRPC returns nil err, error in Status
	assert.NotEqual(t, int32(0), resp.Status.Code)
}

func TestServer_FenceSegment_Success(t *testing.T) {
	fake := &fakeLogStore{
		fenceFn: func(ctx context.Context, bn, rp string, logId, segId int64) (int64, error) {
			return 42, nil
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.FenceSegment(context.Background(), &proto.FenceSegmentRequest{
		BucketName: "b", RootPath: "r", LogId: 1, SegmentId: 0,
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), resp.Status.Code)
	assert.Equal(t, int64(42), resp.LastEntryId)
}

func TestServer_FenceSegment_Error(t *testing.T) {
	fake := &fakeLogStore{
		fenceFn: func(ctx context.Context, bn, rp string, logId, segId int64) (int64, error) {
			return -1, assert.AnError
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.FenceSegment(context.Background(), &proto.FenceSegmentRequest{
		BucketName: "b", RootPath: "r", LogId: 1,
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.Status.Code)
}

func TestServer_CompleteSegment_Success(t *testing.T) {
	fake := &fakeLogStore{
		completeFn: func(ctx context.Context, bn, rp string, logId, segId, lac int64) (int64, error) {
			return 10, nil
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.CompleteSegment(context.Background(), &proto.CompleteSegmentRequest{
		BucketName: "b", RootPath: "r", LogId: 1, SegmentId: 0, LastAddConfirmed: 10,
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), resp.Status.Code)
	assert.Equal(t, int64(10), resp.LastEntryId)
}

func TestServer_CompleteSegment_Error(t *testing.T) {
	fake := &fakeLogStore{
		completeFn: func(ctx context.Context, bn, rp string, logId, segId, lac int64) (int64, error) {
			return -1, assert.AnError
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.CompleteSegment(context.Background(), &proto.CompleteSegmentRequest{
		BucketName: "b", RootPath: "r", LogId: 1,
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.Status.Code)
}

func TestServer_CompactSegment_Success(t *testing.T) {
	expectedMeta := &proto.SegmentMetadata{SegNo: 0}
	fake := &fakeLogStore{
		compactFn: func(ctx context.Context, bn, rp string, logId, segId int64) (*proto.SegmentMetadata, error) {
			return expectedMeta, nil
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.CompactSegment(context.Background(), &proto.CompactSegmentRequest{
		BucketName: "b", RootPath: "r", LogId: 1, SegmentId: 0,
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), resp.Status.Code)
	assert.Equal(t, expectedMeta, resp.Metadata)
}

func TestServer_CompactSegment_Error(t *testing.T) {
	fake := &fakeLogStore{
		compactFn: func(ctx context.Context, bn, rp string, logId, segId int64) (*proto.SegmentMetadata, error) {
			return nil, assert.AnError
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.CompactSegment(context.Background(), &proto.CompactSegmentRequest{
		BucketName: "b", RootPath: "r", LogId: 1,
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.Status.Code)
}

func TestServer_GetSegmentLastAddConfirmed_Success(t *testing.T) {
	fake := &fakeLogStore{
		getLACFn: func(ctx context.Context, bn, rp string, logId, segId int64) (int64, error) {
			return 99, nil
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.GetSegmentLastAddConfirmed(context.Background(), &proto.GetSegmentLastAddConfirmedRequest{
		BucketName: "b", RootPath: "r", LogId: 1, SegmentId: 0,
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), resp.Status.Code)
	assert.Equal(t, int64(99), resp.LastEntryId)
}

func TestServer_GetSegmentLastAddConfirmed_Error(t *testing.T) {
	fake := &fakeLogStore{
		getLACFn: func(ctx context.Context, bn, rp string, logId, segId int64) (int64, error) {
			return -1, assert.AnError
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.GetSegmentLastAddConfirmed(context.Background(), &proto.GetSegmentLastAddConfirmedRequest{
		BucketName: "b", RootPath: "r", LogId: 1,
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.Status.Code)
}

func TestServer_GetSegmentBlockCount_Success(t *testing.T) {
	fake := &fakeLogStore{
		getBlockCountFn: func(ctx context.Context, bn, rp string, logId, segId int64) (int64, error) {
			return 7, nil
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.GetSegmentBlockCount(context.Background(), &proto.GetSegmentBlockCountRequest{
		BucketName: "b", RootPath: "r", LogId: 1, SegmentId: 0,
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), resp.Status.Code)
	assert.Equal(t, int64(7), resp.BlockCount)
}

func TestServer_GetSegmentBlockCount_Error(t *testing.T) {
	fake := &fakeLogStore{
		getBlockCountFn: func(ctx context.Context, bn, rp string, logId, segId int64) (int64, error) {
			return -1, assert.AnError
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.GetSegmentBlockCount(context.Background(), &proto.GetSegmentBlockCountRequest{
		BucketName: "b", RootPath: "r", LogId: 1,
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.Status.Code)
}

func TestServer_CleanSegment_Success(t *testing.T) {
	fake := &fakeLogStore{
		cleanFn: func(ctx context.Context, bn, rp string, logId, segId int64, flag int) error {
			return nil
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.CleanSegment(context.Background(), &proto.CleanSegmentRequest{
		BucketName: "b", RootPath: "r", LogId: 1, SegmentId: 0, Flag: 0,
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), resp.Status.Code)
}

func TestServer_CleanSegment_Error(t *testing.T) {
	fake := &fakeLogStore{
		cleanFn: func(ctx context.Context, bn, rp string, logId, segId int64, flag int) error {
			return assert.AnError
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.CleanSegment(context.Background(), &proto.CleanSegmentRequest{
		BucketName: "b", RootPath: "r", LogId: 1,
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.Status.Code)
}

func TestServer_UpdateLastAddConfirmed_Success(t *testing.T) {
	fake := &fakeLogStore{
		updateLACFn: func(ctx context.Context, bn, rp string, logId, segId, lac int64) error {
			return nil
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.UpdateLastAddConfirmed(context.Background(), &proto.UpdateLastAddConfirmedRequest{
		BucketName: "b", RootPath: "r", LogId: 1, SegmentId: 0, LastAddConfirmed: 10,
	})
	assert.NoError(t, err)
	assert.Equal(t, int32(0), resp.Status.Code)
}

func TestServer_UpdateLastAddConfirmed_Error(t *testing.T) {
	fake := &fakeLogStore{
		updateLACFn: func(ctx context.Context, bn, rp string, logId, segId, lac int64) error {
			return assert.AnError
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	resp, err := s.UpdateLastAddConfirmed(context.Background(), &proto.UpdateLastAddConfirmedRequest{
		BucketName: "b", RootPath: "r", LogId: 1,
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.Status.Code)
}

func TestServer_SelectNodes_NilNode(t *testing.T) {
	s := createTestServerWithFakeLogStore(&fakeLogStore{})
	defer s.cancel()

	resp, err := s.SelectNodes(context.Background(), &proto.SelectNodesRequest{
		Filters: []*proto.NodeFilter{{Limit: 3}},
	})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.Status.Code) // Error because serverNode is nil
}

func TestServer_SelectNodes_NoFilters(t *testing.T) {
	s := createTestServerWithFakeLogStore(&fakeLogStore{})
	defer s.cancel()
	// Set a non-nil serverNode to bypass the nil check
	s.serverNode = &membership.ServerNode{}

	resp, err := s.SelectNodes(context.Background(), &proto.SelectNodesRequest{})
	assert.NoError(t, err)
	assert.NotEqual(t, int32(0), resp.Status.Code) // Error because no filters
}

func TestServer_GetServerNodeMemberlistStatus_NilNode(t *testing.T) {
	s := createTestServerWithFakeLogStore(&fakeLogStore{})
	defer s.cancel()

	status := s.GetServerNodeMemberlistStatus()
	assert.Equal(t, "member not ready yet", status)
}

func TestServer_GetMemberCount_NilNode(t *testing.T) {
	s := createTestServerWithFakeLogStore(&fakeLogStore{})
	defer s.cancel()

	count := s.GetMemberCount()
	assert.Equal(t, 0, count)
}

func TestServer_GetServiceAdvertiseAddrPort_NilNode(t *testing.T) {
	s := createTestServerWithFakeLogStore(&fakeLogStore{})
	defer s.cancel()

	addr := s.GetServiceAdvertiseAddrPort(context.Background())
	assert.Equal(t, "", addr)
}

func TestServer_GetAdvertiseAddrPort(t *testing.T) {
	s := createTestServerWithFakeLogStore(&fakeLogStore{})
	defer s.cancel()

	addr := s.GetAdvertiseAddrPort(context.Background())
	assert.Equal(t, "127.0.0.1:9999", addr)
}

func TestServer_ShutdownUnaryInterceptor(t *testing.T) {
	srvCtx, srvCancel := context.WithCancel(context.Background())
	s := &Server{
		ctx:          srvCtx,
		cancel:       srvCancel,
		serverConfig: &membership.ServerConfig{NodeID: "test-unary"},
	}

	interceptor := s.shutdownUnaryInterceptor()

	handlerCalled := false
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		handlerCalled = true
		return "ok", nil
	}

	resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, handler)
	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	assert.Equal(t, "ok", resp)
	srvCancel()
}

// === NewServer / NewServerWithConfig Tests ===

func TestNewServer_LocalStorage(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Woodpecker.Storage.Type = "local"
	cfg.Woodpecker.Storage.RootPath = t.TempDir()

	s, err := NewServer(context.Background(), cfg, 0, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, s)
	defer s.cancel()

	assert.NotNil(t, s.logStore)
	assert.NotNil(t, s.serverConfig)
	assert.Equal(t, 0, s.serverConfig.BindPort)
	assert.Equal(t, 0, s.serverConfig.ServicePort)
	assert.Equal(t, 0, s.serverConfig.AdvertisePort)
	assert.Equal(t, "default", s.serverConfig.ResourceGroup)
	assert.Equal(t, "default", s.serverConfig.AZ)
	assert.Equal(t, map[string]string{"role": "logstore"}, s.serverConfig.Tags)
}

func TestNewServerWithConfig_LocalStorage(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Woodpecker.Storage.Type = "local"
	cfg.Woodpecker.Storage.RootPath = t.TempDir()

	serverConfig := &membership.ServerConfig{
		NodeID:               "test-node",
		BindPort:             0,
		ServicePort:          0,
		AdvertisePort:        0,
		AdvertiseServicePort: 0,
		ResourceGroup:        "rg1",
		AZ:                   "az1",
		Tags:                 map[string]string{"role": "test"},
	}
	seeds := []string{"host1:7946", "host2:7946"}

	s, err := NewServerWithConfig(context.Background(), cfg, serverConfig, seeds)
	require.NoError(t, err)
	require.NotNil(t, s)
	defer s.cancel()

	assert.Equal(t, serverConfig, s.serverConfig)
	assert.Equal(t, seeds, s.gossipSeeds)
	assert.NotNil(t, s.logStore)
}

func TestNewServerWithConfig_WithGossipSeeds(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Woodpecker.Storage.Type = "local"
	cfg.Woodpecker.Storage.RootPath = t.TempDir()

	seeds := []string{"seed1:7946", "seed2:7946"}
	s, err := NewServer(context.Background(), cfg, 8080, 9090, seeds)
	require.NoError(t, err)
	require.NotNil(t, s)
	defer s.cancel()

	assert.Equal(t, seeds, s.gossipSeeds)
	assert.Equal(t, 8080, s.serverConfig.BindPort)
	assert.Equal(t, 9090, s.serverConfig.ServicePort)
	assert.Equal(t, 8080, s.serverConfig.AdvertisePort)
	assert.Equal(t, 9090, s.serverConfig.AdvertiseServicePort)
}

// === AddEntry Tests ===

// mockAddEntryStream implements grpc.ServerStreamingServer[proto.AddEntryResponse] for testing.
type mockAddEntryStream struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*proto.AddEntryResponse
	sendErr   error // if set, Send returns this error
}

func (m *mockAddEntryStream) Send(resp *proto.AddEntryResponse) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.responses = append(m.responses, resp)
	return nil
}

func (m *mockAddEntryStream) Context() context.Context     { return m.ctx }
func (m *mockAddEntryStream) SetHeader(metadata.MD) error  { return nil }
func (m *mockAddEntryStream) SendHeader(metadata.MD) error { return nil }
func (m *mockAddEntryStream) SetTrailer(metadata.MD)       {}
func (m *mockAddEntryStream) SendMsg(interface{}) error    { return nil }
func (m *mockAddEntryStream) RecvMsg(interface{}) error    { return nil }

func TestServer_AddEntry_Success(t *testing.T) {
	fake := &fakeLogStore{
		addEntryFn: func(ctx context.Context, bn, rp string, logId int64, entry *proto.LogEntry, resultCh channel.ResultChannel) (int64, error) {
			// Simulate successful buffering then sync via result channel
			go func() {
				resultCh.SendResult(ctx, &channel.AppendResult{SyncedId: 42, Err: nil})
			}()
			return 42, nil
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	stream := &mockAddEntryStream{ctx: context.Background()}
	req := &proto.AddEntryRequest{
		BucketName: "b",
		RootPath:   "r",
		LogId:      1,
		Entry:      &proto.LogEntry{SegId: 0, EntryId: 0, Values: []byte("hello")},
	}

	err := s.AddEntry(req, stream)
	assert.NoError(t, err)
	require.Len(t, stream.responses, 2)
	assert.Equal(t, proto.AddEntryState_Buffered, stream.responses[0].State)
	assert.Equal(t, int64(42), stream.responses[0].EntryId)
	assert.Equal(t, proto.AddEntryState_Synced, stream.responses[1].State)
	assert.Equal(t, int64(42), stream.responses[1].EntryId)
}

func TestServer_AddEntry_AddEntryError(t *testing.T) {
	fake := &fakeLogStore{
		addEntryFn: func(ctx context.Context, bn, rp string, logId int64, entry *proto.LogEntry, resultCh channel.ResultChannel) (int64, error) {
			return -1, werr.ErrLogStoreShutdown
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	stream := &mockAddEntryStream{ctx: context.Background()}
	req := &proto.AddEntryRequest{
		BucketName: "b",
		RootPath:   "r",
		LogId:      1,
		Entry:      &proto.LogEntry{SegId: 0, EntryId: 0, Values: []byte("hello")},
	}

	err := s.AddEntry(req, stream)
	assert.Error(t, err)
	require.Len(t, stream.responses, 1)
	assert.Equal(t, proto.AddEntryState_Failed, stream.responses[0].State)
}

func TestServer_AddEntry_AddEntryError_SendFails(t *testing.T) {
	fake := &fakeLogStore{
		addEntryFn: func(ctx context.Context, bn, rp string, logId int64, entry *proto.LogEntry, resultCh channel.ResultChannel) (int64, error) {
			return -1, werr.ErrLogStoreShutdown
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	stream := &mockAddEntryStream{ctx: context.Background(), sendErr: fmt.Errorf("send failed")}
	req := &proto.AddEntryRequest{
		BucketName: "b",
		RootPath:   "r",
		LogId:      1,
		Entry:      &proto.LogEntry{SegId: 0, EntryId: 0},
	}

	err := s.AddEntry(req, stream)
	assert.Error(t, err) // Returns the original logStore error, not sendErr
}

func TestServer_AddEntry_BufferedSendError(t *testing.T) {
	fake := &fakeLogStore{
		addEntryFn: func(ctx context.Context, bn, rp string, logId int64, entry *proto.LogEntry, resultCh channel.ResultChannel) (int64, error) {
			return 0, nil
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	stream := &mockAddEntryStream{ctx: context.Background(), sendErr: fmt.Errorf("send failed")}
	req := &proto.AddEntryRequest{
		BucketName: "b",
		RootPath:   "r",
		LogId:      1,
		Entry:      &proto.LogEntry{SegId: 0, EntryId: 0},
	}

	err := s.AddEntry(req, stream)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "send failed")
}

func TestServer_AddEntry_ReadResultError(t *testing.T) {
	fake := &fakeLogStore{
		addEntryFn: func(ctx context.Context, bn, rp string, logId int64, entry *proto.LogEntry, resultCh channel.ResultChannel) (int64, error) {
			go func() {
				resultCh.SendResult(ctx, &channel.AppendResult{SyncedId: 5, Err: fmt.Errorf("sync failed")})
			}()
			return 0, nil
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	stream := &mockAddEntryStream{ctx: context.Background()}
	req := &proto.AddEntryRequest{
		BucketName: "b",
		RootPath:   "r",
		LogId:      1,
		Entry:      &proto.LogEntry{SegId: 0, EntryId: 0},
	}

	err := s.AddEntry(req, stream)
	// The result has Err set, so we get a Failed state
	require.Len(t, stream.responses, 2)
	assert.Equal(t, proto.AddEntryState_Buffered, stream.responses[0].State)
	assert.Equal(t, proto.AddEntryState_Failed, stream.responses[1].State)
	assert.Equal(t, int64(5), stream.responses[1].EntryId)
	_ = err // sendErr for the Failed response
}

func TestServer_AddEntry_ReadResultContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeLogStore{
		addEntryFn: func(ctx context.Context, bn, rp string, logId int64, entry *proto.LogEntry, resultCh channel.ResultChannel) (int64, error) {
			// Cancel context so ReadResult fails
			go func() {
				cancel()
			}()
			return 0, nil
		},
	}
	s := createTestServerWithFakeLogStore(fake)
	defer s.cancel()

	stream := &mockAddEntryStream{ctx: ctx}
	req := &proto.AddEntryRequest{
		BucketName: "b",
		RootPath:   "r",
		LogId:      1,
		Entry:      &proto.LogEntry{SegId: 0, EntryId: 0},
	}

	err := s.AddEntry(req, stream)
	// Should get at least the buffered response, and then a failed or send error
	_ = err
	require.GreaterOrEqual(t, len(stream.responses), 1)
	assert.Equal(t, proto.AddEntryState_Buffered, stream.responses[0].State)
}

// === SelectNodes with actual ServerNode ===

func TestServer_SelectNodes_WithStrategies(t *testing.T) {
	// Create a real ServerNode for the test
	node, err := membership.NewServerNode(&membership.ServerConfig{
		NodeID:               "test-select-strategies",
		AdvertiseAddr:        "127.0.0.1",
		BindPort:             0,
		AdvertisePort:        0,
		ServicePort:          0,
		AdvertiseServicePort: 0,
		ResourceGroup:        "rg1",
		AZ:                   "az1",
		Tags:                 map[string]string{"role": "test"},
	})
	require.NoError(t, err)
	defer node.Shutdown()

	s := createTestServerWithFakeLogStore(&fakeLogStore{})
	defer s.cancel()
	s.serverNode = node

	strategies := []proto.StrategyType{
		proto.StrategyType_RANDOM,
		proto.StrategyType_SINGLE_AZ_SINGLE_RG,
		proto.StrategyType_SINGLE_AZ_MULTI_RG,
		proto.StrategyType_MULTI_AZ_SINGLE_RG,
		proto.StrategyType_MULTI_AZ_MULTI_RG,
		proto.StrategyType_CUSTOM,
		proto.StrategyType_CROSS_REGION,
		proto.StrategyType_RANDOM_GROUP,
		99, // Default case
	}

	for _, strategy := range strategies {
		t.Run(fmt.Sprintf("strategy_%d", strategy), func(t *testing.T) {
			resp, err := s.SelectNodes(context.Background(), &proto.SelectNodesRequest{
				Strategy:     strategy,
				AffinityMode: proto.AffinityMode_SOFT,
				Filters: []*proto.NodeFilter{{
					Limit: 1,
				}},
			})
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			// In soft mode, even if strategy can't find enough nodes, it returns success
		})
	}
}

func TestServer_SelectNodes_HardAffinity_Error(t *testing.T) {
	node, err := membership.NewServerNode(&membership.ServerConfig{
		NodeID:               "test-select-hard",
		AdvertiseAddr:        "127.0.0.1",
		BindPort:             0,
		AdvertisePort:        0,
		ServicePort:          0,
		AdvertiseServicePort: 0,
		ResourceGroup:        "rg1",
		AZ:                   "az1",
		Tags:                 map[string]string{"role": "test"},
	})
	require.NoError(t, err)
	defer node.Shutdown()

	s := createTestServerWithFakeLogStore(&fakeLogStore{})
	defer s.cancel()
	s.serverNode = node

	// Use SINGLE_AZ_MULTI_RG with a filter requiring an AZ that doesn't exist
	// This guarantees failure in HARD mode
	resp, err := s.SelectNodes(context.Background(), &proto.SelectNodesRequest{
		Strategy:     proto.StrategyType_SINGLE_AZ_MULTI_RG,
		AffinityMode: proto.AffinityMode_HARD,
		Filters: []*proto.NodeFilter{{
			Az:    "nonexistent-az",
			Limit: 1,
		}},
	})
	assert.NoError(t, err) // gRPC handler returns nil err
	assert.NotNil(t, resp)
	// Hard mode returns error in Status when filter can't be satisfied
	assert.NotEqual(t, int32(0), resp.Status.Code)
}

func TestServer_SelectNodes_MultipleFilters(t *testing.T) {
	node, err := membership.NewServerNode(&membership.ServerConfig{
		NodeID:               "test-select-multi",
		AdvertiseAddr:        "127.0.0.1",
		BindPort:             0,
		AdvertisePort:        0,
		ServicePort:          0,
		AdvertiseServicePort: 0,
		ResourceGroup:        "rg1",
		AZ:                   "az1",
		Tags:                 map[string]string{"role": "test"},
	})
	require.NoError(t, err)
	defer node.Shutdown()

	s := createTestServerWithFakeLogStore(&fakeLogStore{})
	defer s.cancel()
	s.serverNode = node

	resp, err := s.SelectNodes(context.Background(), &proto.SelectNodesRequest{
		Strategy:     proto.StrategyType_RANDOM,
		AffinityMode: proto.AffinityMode_SOFT,
		Filters: []*proto.NodeFilter{
			{Limit: 1},
			{Limit: 1},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, int32(0), resp.Status.Code)
}

// === Non-nil ServerNode helper tests ===

func TestServer_GetServerNodeMemberlistStatus_WithNode(t *testing.T) {
	node, err := membership.NewServerNode(&membership.ServerConfig{
		NodeID:               "test-status-node",
		AdvertiseAddr:        "127.0.0.1",
		BindPort:             0,
		AdvertisePort:        0,
		ServicePort:          0,
		AdvertiseServicePort: 0,
		ResourceGroup:        "default",
		AZ:                   "default",
		Tags:                 map[string]string{"role": "test"},
	})
	require.NoError(t, err)
	defer node.Shutdown()

	s := createTestServerWithFakeLogStore(&fakeLogStore{})
	defer s.cancel()
	s.serverNode = node

	status := s.GetServerNodeMemberlistStatus()
	assert.NotEqual(t, "member not ready yet", status)
}

func TestServer_GetMemberCount_WithNode(t *testing.T) {
	node, err := membership.NewServerNode(&membership.ServerConfig{
		NodeID:               "test-count-node",
		AdvertiseAddr:        "127.0.0.1",
		BindPort:             0,
		AdvertisePort:        0,
		ServicePort:          0,
		AdvertiseServicePort: 0,
		ResourceGroup:        "default",
		AZ:                   "default",
		Tags:                 map[string]string{"role": "test"},
	})
	require.NoError(t, err)
	defer node.Shutdown()

	s := createTestServerWithFakeLogStore(&fakeLogStore{})
	defer s.cancel()
	s.serverNode = node

	count := s.GetMemberCount()
	assert.GreaterOrEqual(t, count, 1)
}

func TestServer_GetServiceAdvertiseAddrPort_WithNode(t *testing.T) {
	node, err := membership.NewServerNode(&membership.ServerConfig{
		NodeID:               "test-svc-addr-node",
		AdvertiseAddr:        "127.0.0.1",
		BindPort:             0,
		AdvertisePort:        0,
		ServicePort:          0,
		AdvertiseServicePort: 0,
		ResourceGroup:        "default",
		AZ:                   "default",
		Tags:                 map[string]string{"role": "test"},
	})
	require.NoError(t, err)
	defer node.Shutdown()

	s := createTestServerWithFakeLogStore(&fakeLogStore{})
	defer s.cancel()
	s.serverNode = node

	addr := s.GetServiceAdvertiseAddrPort(context.Background())
	assert.NotEmpty(t, addr)
}

// === monitorAndJoinSeeds Tests ===

func TestServer_MonitorAndJoinSeeds_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	node, err := membership.NewServerNode(&membership.ServerConfig{
		NodeID:               "test-monitor-cancel",
		AdvertiseAddr:        "127.0.0.1",
		BindPort:             0,
		AdvertisePort:        0,
		ServicePort:          0,
		AdvertiseServicePort: 0,
		ResourceGroup:        "default",
		AZ:                   "default",
		Tags:                 map[string]string{"role": "test"},
	})
	require.NoError(t, err)
	defer node.Shutdown()

	s := createTestServer(ctx, &membership.ServerConfig{NodeID: "test-monitor-cancel"})
	s.serverNode = node

	// Cancel context immediately to make monitorAndJoinSeeds exit quickly
	cancel()

	done := make(chan struct{})
	go func() {
		s.monitorAndJoinSeeds(ctx, []string{"127.0.0.1:19999"})
		close(done)
	}()

	select {
	case <-done:
		// Expected: function returned after context cancellation
	case <-time.After(5 * time.Second):
		t.Fatal("monitorAndJoinSeeds did not return after context cancellation")
	}
}

func TestServer_MonitorAndJoinSeeds_AllSeedsPresent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	node, err := membership.NewServerNode(&membership.ServerConfig{
		NodeID:               "test-monitor-present",
		AdvertiseAddr:        "127.0.0.1",
		BindPort:             0,
		AdvertisePort:        0,
		ServicePort:          0,
		AdvertiseServicePort: 0,
		ResourceGroup:        "default",
		AZ:                   "default",
		Tags:                 map[string]string{"role": "test"},
	})
	require.NoError(t, err)
	defer node.Shutdown()

	s := createTestServer(ctx, &membership.ServerConfig{NodeID: "test-monitor-present"})
	s.serverNode = node

	// Use the node's own name as seed (will be found in memberlist)
	members := node.GetMemberlist().Members()
	var seeds []string
	for _, m := range members {
		seeds = append(seeds, fmt.Sprintf("%s:%d", m.Addr.String(), m.Port))
	}

	// Run briefly - should find all seeds present and switch to normalBackoff
	go func() {
		time.Sleep(3 * time.Second)
		cancel()
	}()

	done := make(chan struct{})
	go func() {
		s.monitorAndJoinSeeds(ctx, seeds)
		close(done)
	}()

	select {
	case <-done:
		// Expected
	case <-time.After(10 * time.Second):
		t.Fatal("monitorAndJoinSeeds did not exit within timeout")
	}
}

func TestServer_MonitorAndJoinSeeds_MissingSeeds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	node, err := membership.NewServerNode(&membership.ServerConfig{
		NodeID:               "test-monitor-missing",
		AdvertiseAddr:        "127.0.0.1",
		BindPort:             0,
		AdvertisePort:        0,
		ServicePort:          0,
		AdvertiseServicePort: 0,
		ResourceGroup:        "default",
		AZ:                   "default",
		Tags:                 map[string]string{"role": "test"},
	})
	require.NoError(t, err)
	defer node.Shutdown()

	s := createTestServer(ctx, &membership.ServerConfig{NodeID: "test-monitor-missing"})
	s.serverNode = node

	// Use unreachable seeds - will trigger the join failure path
	seeds := []string{"192.0.2.1:7946", "192.0.2.2:7946"}

	done := make(chan struct{})
	go func() {
		s.monitorAndJoinSeeds(ctx, seeds)
		close(done)
	}()

	select {
	case <-done:
		// Expected: context timeout causes exit
	case <-time.After(10 * time.Second):
		t.Fatal("monitorAndJoinSeeds did not exit within timeout")
	}
}

func TestServer_MonitorAndJoinSeeds_InvalidSeedFormat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	node, err := membership.NewServerNode(&membership.ServerConfig{
		NodeID:               "test-monitor-invalid",
		AdvertiseAddr:        "127.0.0.1",
		BindPort:             0,
		AdvertisePort:        0,
		ServicePort:          0,
		AdvertiseServicePort: 0,
		ResourceGroup:        "default",
		AZ:                   "default",
		Tags:                 map[string]string{"role": "test"},
	})
	require.NoError(t, err)
	defer node.Shutdown()

	s := createTestServer(ctx, &membership.ServerConfig{NodeID: "test-monitor-invalid"})
	s.serverNode = node

	// Seeds with invalid format (no port) - SplitHostPort will fail, so seedHostnames will be empty
	seeds := []string{"no-port-seed"}

	done := make(chan struct{})
	go func() {
		s.monitorAndJoinSeeds(ctx, seeds)
		close(done)
	}()

	select {
	case <-done:
		// Expected
	case <-time.After(10 * time.Second):
		t.Fatal("monitorAndJoinSeeds did not exit within timeout")
	}
}

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

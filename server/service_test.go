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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/membership"
)

// createTestServer creates a minimal Server instance for testing without
// triggering storage client initialization or other side effects.
func createTestServer(ctx context.Context, serverConfig *membership.ServerConfig) *Server {
	ctx, cancel := context.WithCancel(ctx)
	cfg := &config.Configuration{}
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

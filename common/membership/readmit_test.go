// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package membership

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	ml "github.com/hashicorp/memberlist"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readmitFreePort returns a port free for both TCP and UDP: memberlist binds
// both on its gossip port.
func readmitFreePort(t *testing.T) int {
	t.Helper()
	for i := 0; i < 20; i++ {
		l, err := net.Listen("tcp", "0.0.0.0:0")
		require.NoError(t, err)
		p := l.Addr().(*net.TCPAddr).Port
		u, err := net.ListenPacket("udp", fmt.Sprintf("0.0.0.0:%d", p))
		_ = l.Close()
		if err != nil {
			continue
		}
		_ = u.Close()
		return p
	}
	t.Fatal("no port free for both TCP and UDP")
	return 0
}

// readmitNode starts a server and returns it with its gossip and service ports.
func readmitNode(t *testing.T, id string) (*ServerNode, int, int) {
	t.Helper()
	gossip, service := readmitFreePort(t), readmitFreePort(t)
	n, err := NewServerNode(&ServerConfig{
		NodeID: id, ResourceGroup: "rg", AZ: "az",
		BindPort: gossip, AdvertiseAddr: "127.0.0.1", AdvertisePort: gossip, ServicePort: service,
	})
	require.NoError(t, err)
	return n, gossip, service
}

// endpointOf returns the service endpoint n's discovery holds for name, which
// is what node selection hands out, or "" when n has no entry for it. (The
// memberlist's own Members() cannot be read while it gossips: it hands out its
// live node records.)
func endpointOf(n *ServerNode, name string) string {
	if m, ok := n.GetDiscovery().GetAllServers()[name]; ok {
		return m.GetEndpoint()
	}
	return ""
}

// startTrio starts servers a, b and c, c joined through a, and waits until
// every one of them sees the other two.
func startTrio(t *testing.T) (a, b, c *ServerNode, seed string) {
	t.Helper()
	a, pa, _ := readmitNode(t, "a")
	t.Cleanup(func() { _ = a.Shutdown() })
	b, _, _ = readmitNode(t, "b")
	t.Cleanup(func() { _ = b.Shutdown() })
	c, _, _ = readmitNode(t, "c")
	seed = fmt.Sprintf("127.0.0.1:%d", pa)
	require.NoError(t, b.Join([]string{seed}))
	require.NoError(t, c.Join([]string{seed}))
	require.Eventually(t, func() bool {
		for _, n := range []*ServerNode{a, b, c} {
			if len(n.GetDiscovery().GetAllServers()) != 3 {
				return false
			}
		}
		return true
	}, 10*time.Second, 50*time.Millisecond)
	return a, b, c, seed
}

// restartAtNewAddress starts a new server under c's name on new ports, joined
// through seed, and returns how long until a and b both hand out its new
// endpoint.
func restartAtNewAddress(t *testing.T, a, b *ServerNode, seed string, within time.Duration) time.Duration {
	t.Helper()
	c2, _, service := readmitNode(t, "c")
	t.Cleanup(func() { _ = c2.Shutdown() })
	start := time.Now()
	require.NoError(t, c2.Join([]string{seed}))
	suffix := fmt.Sprintf(":%d", service)
	accepted := func(n *ServerNode) bool { return strings.HasSuffix(endpointOf(n, "c"), suffix) }
	require.Eventually(t, func() bool { return accepted(a) && accepted(b) }, within, 50*time.Millisecond,
		"peers did not accept c at its new address within %v of its restart", within)
	return time.Since(start)
}

// A server that dies without leaving and restarts at a new address before its
// peers declare it dead is refused while its old entry is alive or suspect
// ("Conflicting address"). Once the old entry is declared dead, each peer joins
// the address it refused, and takes the server back. Without that it waited
// for the old entry to be reaped and a later full-state sync: 30-50s (#395).
// The bound covers declaring the old entry dead (at most 3s x
// SuspicionMaxTimeoutMult 2 after the first failed probe) plus slack.
func TestServerNode_ReadmittedWhenRestartedBeforeDeclaredDead(t *testing.T) {
	a, b, c, seed := startTrio(t)
	require.NoError(t, c.Shutdown()) // no Leave: like SIGKILL
	took := restartAtNewAddress(t, a, b, seed, 12*time.Second)
	t.Logf("readmitted %v after the restart", took)
}

// A server that restarts at a new address after its peers declared it dead is
// taken back at once: its name is reclaimable as soon as it is dead. Without
// that it waited for the dead entry to be reaped (GossipToTheDeadTime) and a
// later full-state sync.
func TestServerNode_ReadmittedWhenRestartedAfterDeclaredDead(t *testing.T) {
	a, b, c, seed := startTrio(t)
	require.NoError(t, c.Shutdown()) // no Leave: like SIGKILL
	require.Eventually(t, func() bool { return endpointOf(a, "c") == "" && endpointOf(b, "c") == "" },
		30*time.Second, 50*time.Millisecond, "peers never declared c dead")
	took := restartAtNewAddress(t, a, b, seed, 3*time.Second)
	t.Logf("readmitted %v after the restart", took)
}

func TestReadmitter_JoinsRefusedAddressWhenOldEntryDies(t *testing.T) {
	var mu sync.Mutex
	var joined []string
	done := make(chan struct{}, 1)
	r := newReadmitter(func(addrs []string) (int, error) {
		mu.Lock()
		joined = append(joined, addrs...)
		mu.Unlock()
		done <- struct{}{}
		return len(addrs), nil
	})

	r.nodeLeft("c") // nothing refused for c: nothing to join
	r.NotifyConflict(&ml.Node{Name: "c", Addr: net.ParseIP("10.0.0.1"), Port: 7946},
		&ml.Node{Name: "c", Addr: net.ParseIP("10.0.0.2"), Port: 7946})
	r.nodeLeft("c")
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the refused address was not joined")
	}
	mu.Lock()
	assert.Equal(t, []string{"10.0.0.2:7946"}, joined)
	mu.Unlock()

	r.nodeLeft("c") // already handled: not joined again
	select {
	case <-done:
		t.Fatal("joined twice")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestReadmitter_ForgetsStaleRefusals(t *testing.T) {
	r := newReadmitter(func(addrs []string) (int, error) {
		t.Fatalf("joined %v for a refusal older than readmitPendingTTL", addrs)
		return 0, nil
	})
	r.pending["c"] = pendingAddr{addr: "10.0.0.2:7946", at: time.Now().Add(-2 * readmitPendingTTL)}
	r.nodeLeft("c")
	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, r.pending)
}

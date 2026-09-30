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

package faultproxy

import (
	"bufio"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// startEcho runs a line-echo server and returns its address.
func startEcho(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if _, err := c.Write([]byte(line)); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String(), func() { _ = ln.Close() }
}

// roundTrip sends one line and waits up to d for the echo.
func roundTrip(c net.Conn, d time.Duration) error {
	if _, err := c.Write([]byte("ping\n")); err != nil {
		return err
	}
	_ = c.SetReadDeadline(time.Now().Add(d))
	_, err := bufio.NewReader(c).ReadString('\n')
	return err
}

func isTimeout(err error) bool {
	ne, ok := err.(net.Error)
	return ok && ne.Timeout() || os.IsTimeout(err)
}

func TestProxy_ForwardRelays(t *testing.T) {
	backend, stop := startEcho(t)
	defer stop()
	p, err := New("127.0.0.1:0", backend)
	require.NoError(t, err)
	defer p.Close()

	c, err := net.Dial("tcp", p.Addr())
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, roundTrip(c, time.Second))
	require.EqualValues(t, 1, p.Stats().Forwarded)
}

func TestProxy_RefuseFailsDialFast(t *testing.T) {
	backend, stop := startEcho(t)
	defer stop()
	p, err := New("127.0.0.1:0", backend)
	require.NoError(t, err)
	defer p.Close()

	p.Refuse()
	start := time.Now()
	_, err = net.DialTimeout("tcp", p.Addr(), 3*time.Second)
	require.Error(t, err)
	require.False(t, isTimeout(err), "a refused dial must fail fast, not time out: %v", err)
	require.Less(t, time.Since(start), time.Second)

	// The address comes back on the same port.
	require.NoError(t, p.SetForward(backend))
	c, err := net.Dial("tcp", p.Addr())
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, roundTrip(c, time.Second))
}

func TestProxy_BlackholeAcceptsButNeverAnswers(t *testing.T) {
	backend, stop := startEcho(t)
	defer stop()
	p, err := New("127.0.0.1:0", backend)
	require.NoError(t, err)
	defer p.Close()

	require.NoError(t, p.Blackhole())
	c, err := net.DialTimeout("tcp", p.Addr(), time.Second)
	require.NoError(t, err, "the dial itself succeeds; only the answer never comes")
	defer c.Close()
	err = roundTrip(c, 300*time.Millisecond)
	require.True(t, isTimeout(err), "expected silence, got %v", err)
	require.EqualValues(t, 1, p.Stats().Blackholed)
}

func TestProxy_BlackholeFreezesEstablished_AndStaysFrozenAfterForward(t *testing.T) {
	backend, stop := startEcho(t)
	defer stop()
	p, err := New("127.0.0.1:0", backend)
	require.NoError(t, err)
	defer p.Close()

	old, err := net.Dial("tcp", p.Addr())
	require.NoError(t, err)
	defer old.Close()
	require.NoError(t, roundTrip(old, time.Second))

	require.NoError(t, p.Blackhole())
	require.True(t, isTimeout(roundTrip(old, 300*time.Millisecond)), "established connection must go silent")
	require.EqualValues(t, 1, p.Stats().Frozen)

	// A dial made during the blackhole is pinned to it. The dial returns once
	// the kernel has completed the handshake, which can be before the proxy
	// accepts the connection and classifies it, so wait for that before the
	// replacement comes up.
	pinned, err := net.Dial("tcp", p.Addr())
	require.NoError(t, err)
	defer pinned.Close()
	require.Eventually(t, func() bool { return p.Stats().Blackholed == 1 }, 2*time.Second, 5*time.Millisecond)

	// The replacement pod comes up. New dials work; the pinned and frozen ones do not.
	require.NoError(t, p.SetForward(backend))
	fresh, err := net.Dial("tcp", p.Addr())
	require.NoError(t, err)
	defer fresh.Close()
	require.NoError(t, roundTrip(fresh, time.Second))
	require.True(t, isTimeout(roundTrip(pinned, 300*time.Millisecond)), "a dial made during the blackhole must stay pinned")
	require.True(t, isTimeout(roundTrip(old, 300*time.Millisecond)), "a frozen connection must stay frozen")
}

func TestProxy_ResetAllSendsRST(t *testing.T) {
	backend, stop := startEcho(t)
	defer stop()
	p, err := New("127.0.0.1:0", backend)
	require.NoError(t, err)
	defer p.Close()

	c, err := net.Dial("tcp", p.Addr())
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, roundTrip(c, time.Second))

	p.ResetAll()
	err = roundTrip(c, time.Second)
	require.Error(t, err)
	require.False(t, isTimeout(err), "a reset connection must fail, not go silent: %v", err)
}

func TestProxy_ForwardToDeadBackendResets(t *testing.T) {
	backend, stop := startEcho(t)
	stop() // nothing listens there any more
	p, err := New("127.0.0.1:0", backend)
	require.NoError(t, err)
	defer p.Close()

	c, err := net.Dial("tcp", p.Addr())
	require.NoError(t, err)
	defer c.Close()
	err = roundTrip(c, 3*time.Second)
	require.Error(t, err)
	require.False(t, isTimeout(err), "an unreachable backend must fail fast: %v", err)
}

func TestProxy_RefuseAfterResetsLate(t *testing.T) {
	backend, stop := startEcho(t)
	defer stop()
	p, err := New("127.0.0.1:0", backend)
	require.NoError(t, err)
	defer p.Close()

	require.NoError(t, p.RefuseAfter(200*time.Millisecond))
	c, err := net.Dial("tcp", p.Addr())
	require.NoError(t, err)
	defer c.Close()
	start := time.Now()
	err = roundTrip(c, 3*time.Second)
	require.Error(t, err)
	require.False(t, isTimeout(err), "a late refusal must end in a reset, not silence: %v", err)
	require.GreaterOrEqual(t, time.Since(start), 150*time.Millisecond)
	require.EqualValues(t, 1, p.Stats().RefusedLate)
}

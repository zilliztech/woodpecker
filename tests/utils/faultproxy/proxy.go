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

// Package faultproxy is a TCP proxy that stands in for a pod's stable network
// identity (its DNS name) so a test can reproduce what a client sees while that
// pod is replaced.
//
// On localhost a stopped server's port answers a dial with RST at once, so an
// in-process "kill" can only ever produce a fast "connection refused". A real pod
// replacement has a second phase that localhost cannot produce: the old pod's IP
// is reclaimed while the name still resolves to it, and a dial to it gets no
// answer at all. That silence, not the refusal, is what pins a gRPC connect
// attempt until its connect deadline. The proxy's modes map onto those phases:
//
//	Forward   the pod is serving (or the replacement pod is, after SetForward to its port)
//	Refuse    the process is gone but its IP still exists: dials fail fast with RST
//	SlowRefuse the same, with the RST arriving a round trip later
//	Blackhole the IP is gone: dials are accepted and then never answered, and
//	          connections already open go silent in both directions
//
// A connection that went silent stays silent after the proxy returns to Forward,
// the way a connect attempt already pinned to the old IP does not move to the
// new pod.
package faultproxy

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Mode is the state of the network path the proxy stands in for.
type Mode int

const (
	// Forward relays new and established connections to the backend.
	Forward Mode = iota
	// Refuse closes the listener, so new dials get RST immediately.
	Refuse
	// Blackhole accepts new dials but never answers them, and freezes every
	// connection that is already open.
	Blackhole
	// SlowRefuse accepts new dials and resets them after a delay: a refusal
	// that reaches the dialer only after a network round trip, rather than the
	// instant one localhost produces.
	SlowRefuse
)

func (m Mode) String() string {
	switch m {
	case Forward:
		return "forward"
	case Refuse:
		return "refuse"
	case Blackhole:
		return "blackhole"
	case SlowRefuse:
		return "slow-refuse"
	default:
		return fmt.Sprintf("mode(%d)", int(m))
	}
}

// Stats counts what the proxy did, so a failing test can tell a stall spent on
// a silent connection from one spent elsewhere.
type Stats struct {
	Forwarded    int64 // dials relayed to a backend
	Blackholed   int64 // dials accepted in Blackhole mode and never answered
	Frozen       int64 // established connections silenced by Blackhole
	Reset        int64 // connections closed with RST by ResetAll
	RefusedLate  int64 // dials reset after a delay in SlowRefuse mode
	BackendFails int64 // dials whose backend could not be reached
}

// Proxy relays one stable listen address to a replaceable backend.
type Proxy struct {
	mu      sync.Mutex
	addr    string
	ln      net.Listener
	mode    Mode
	backend string
	delay   time.Duration // SlowRefuse delay
	conns   map[*link]struct{}
	closed  bool
	acceptW sync.WaitGroup

	forwarded    atomic.Int64
	blackholed   atomic.Int64
	frozen       atomic.Int64
	reset        atomic.Int64
	refusedLate  atomic.Int64
	backendFails atomic.Int64
}

// New starts a proxy on listenAddr (use "127.0.0.1:0" for a free port) that
// forwards to backend.
func New(listenAddr, backend string) (*Proxy, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	p := &Proxy{
		addr:    ln.Addr().String(),
		mode:    Forward,
		backend: backend,
		conns:   make(map[*link]struct{}),
	}
	p.startAcceptLocked(ln)
	return p, nil
}

// Addr is the stable address clients dial, the pod's "DNS name".
func (p *Proxy) Addr() string { return p.addr }

// Mode returns the current mode.
func (p *Proxy) Mode() Mode {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode
}

// Backend returns the address new connections are forwarded to.
func (p *Proxy) Backend() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.backend
}

// Stats returns a snapshot of the counters.
func (p *Proxy) Stats() Stats {
	return Stats{
		Forwarded:    p.forwarded.Load(),
		Blackholed:   p.blackholed.Load(),
		Frozen:       p.frozen.Load(),
		Reset:        p.reset.Load(),
		RefusedLate:  p.refusedLate.Load(),
		BackendFails: p.backendFails.Load(),
	}
}

// SetForward relays new connections to backend. Connections silenced earlier
// stay silent.
func (p *Proxy) SetForward(backend string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("faultproxy: closed")
	}
	p.backend = backend
	p.mode = Forward
	return p.ensureListeningLocked()
}

// Refuse stops listening, so a dial fails at once with "connection refused".
// Established connections are left alone: the backend that owns them decides
// how they end.
func (p *Proxy) Refuse() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mode = Refuse
	p.stopListeningLocked()
}

// RefuseAfter makes new dials fail with RST, but only after delay, the way a
// refusal from a remote host arrives a round trip later. Established
// connections are left alone, as with Refuse.
func (p *Proxy) RefuseAfter(delay time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("faultproxy: closed")
	}
	p.mode = SlowRefuse
	p.delay = delay
	return p.ensureListeningLocked()
}

// Blackhole makes the address silent: new dials are accepted and never
// answered, and every open connection stops relaying in both directions
// without being closed.
func (p *Proxy) Blackhole() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("faultproxy: closed")
	}
	p.mode = Blackhole
	for l := range p.conns {
		if l.freeze() {
			p.frozen.Add(1)
		}
	}
	return p.ensureListeningLocked()
}

// ResetAll closes every open connection with RST, the way a kernel answers
// for a process that died.
func (p *Proxy) ResetAll() {
	p.mu.Lock()
	links := make([]*link, 0, len(p.conns))
	for l := range p.conns {
		links = append(links, l)
	}
	p.mu.Unlock()
	for _, l := range links {
		l.reset()
		p.reset.Add(1)
	}
}

// Close stops the proxy and drops every connection.
func (p *Proxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.stopListeningLocked()
	links := make([]*link, 0, len(p.conns))
	for l := range p.conns {
		links = append(links, l)
	}
	p.mu.Unlock()
	for _, l := range links {
		l.reset()
	}
	p.acceptW.Wait()
}

func (p *Proxy) ensureListeningLocked() error {
	if p.ln != nil {
		return nil
	}
	// Re-bind the same port: the address is the pod's stable name. A freshly
	// closed listening socket can be re-bound at once (SO_REUSEADDR is Go's
	// default), but retry briefly in case the kernel lags.
	var ln net.Listener
	var err error
	for i := 0; i < 50; i++ {
		ln, err = net.Listen("tcp", p.addr)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		return fmt.Errorf("faultproxy: re-listen %s: %w", p.addr, err)
	}
	p.startAcceptLocked(ln)
	return nil
}

func (p *Proxy) stopListeningLocked() {
	if p.ln != nil {
		_ = p.ln.Close()
		p.ln = nil
	}
}

func (p *Proxy) startAcceptLocked(ln net.Listener) {
	p.ln = ln
	p.acceptW.Add(1)
	go func() {
		defer p.acceptW.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			p.handle(c)
		}
	}()
}

func (p *Proxy) handle(client net.Conn) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = client.Close()
		return
	}
	mode, backend, delay := p.mode, p.backend, p.delay
	l := &link{client: client, gate: make(chan struct{})}
	p.conns[l] = struct{}{}
	p.mu.Unlock()

	if mode == SlowRefuse {
		p.refusedLate.Add(1)
		go func() {
			select {
			case <-time.After(delay):
			case <-l.gate:
			}
			l.reset()
			p.forget(l)
		}()
		return
	}
	if mode == Blackhole {
		// Accepted and never answered: the dialer waits for a server preface
		// that never comes, exactly as for an IP that drops the SYN.
		p.blackholed.Add(1)
		l.freeze()
		go p.drainSilently(l)
		return
	}

	go func() {
		server, err := net.DialTimeout("tcp", backend, 2*time.Second)
		if err != nil {
			// The backend is down while the proxy still forwards (a pod that
			// has not started listening yet): answer like that pod's IP would.
			p.backendFails.Add(1)
			l.reset()
			p.forget(l)
			return
		}
		p.forwarded.Add(1)
		l.setServer(server)
		p.mu.Lock()
		frozenMeanwhile := p.mode == Blackhole
		p.mu.Unlock()
		if frozenMeanwhile && l.freeze() {
			p.frozen.Add(1)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); l.pipe(server, client) }()
		go func() { defer wg.Done(); l.pipe(client, server) }()
		wg.Wait()
		l.close()
		p.forget(l)
	}()
}

// drainSilently reads and discards what the client sends so its writes do not
// fail, and never writes back. It ends when the connection is closed.
func (p *Proxy) drainSilently(l *link) {
	_, _ = io.Copy(io.Discard, l.client)
	l.close()
	p.forget(l)
}

func (p *Proxy) forget(l *link) {
	p.mu.Lock()
	delete(p.conns, l)
	p.mu.Unlock()
}

// link is one client connection and, once forwarded, its backend connection.
type link struct {
	mu     sync.Mutex
	client net.Conn
	server net.Conn
	frozen bool
	gate   chan struct{} // closed when the link is torn down; frozen pipes wait on it
	done   bool
}

func (l *link) setServer(c net.Conn) {
	l.mu.Lock()
	l.server = c
	l.mu.Unlock()
}

// freeze silences the link; it reports whether this call froze it.
func (l *link) freeze() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.frozen || l.done {
		return false
	}
	l.frozen = true
	return true
}

func (l *link) isFrozen() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.frozen
}

// pipe relays src to dst until either side ends. Once the link is frozen,
// whatever is read is held back and the pipe waits for the link to be torn
// down, so the peer sees silence, not an error.
func (l *link) pipe(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if l.isFrozen() {
			<-l.gate
			return
		}
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				l.close()
				return
			}
		}
		if err != nil {
			l.close()
			return
		}
	}
}

func (l *link) reset() {
	l.mu.Lock()
	client, server := l.client, l.server
	l.mu.Unlock()
	for _, c := range []net.Conn{client, server} {
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
	}
	l.close()
}

func (l *link) close() {
	l.mu.Lock()
	if l.done {
		l.mu.Unlock()
		return
	}
	l.done = true
	close(l.gate)
	client, server := l.client, l.server
	l.mu.Unlock()
	if client != nil {
		_ = client.Close()
	}
	if server != nil {
		_ = server.Close()
	}
}

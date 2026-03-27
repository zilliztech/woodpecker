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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

// Fast failover after a peer pod restarts with a new IP is driven by
// logStoreClientRemote.maybeDropCachedConn — it evicts this pool entry as
// soon as an RPC returns a transport error, so the next GetLogStoreClient
// builds a fresh grpc.ClientConn with a fresh DNS resolver. The pool also
// evicts cached connections whose state is TRANSIENT_FAILURE/SHUTDOWN when
// handing them out (see GetLogStoreClient below), as a safety net.
//
// We intentionally do NOT call grpc's dns.SetMinResolutionInterval here,
// even though shortening it would help edge cases that skip self-eviction:
// that setter mutates a process-global variable in grpc-go and would quietly
// affect every other gRPC client in the same process (Milvus coord/node
// channels, etcd client, etc.). The self-eviction path already recovers
// within seconds on the primary failure mode, so the global side-effect
// isn't worth it.

var _ LogStoreClientPool = (*logStoreClientPool)(nil)

type logStoreClientPool struct {
	sync.RWMutex
	maxSendMsgSize int
	maxRecvMsgSize int
	connections    map[string]*grpc.ClientConn
	clients        map[string]LogStoreClient
	clientClosed   atomic.Bool
	lastUsed       map[string]time.Time // track last use time per connection
	cleanupDone    chan struct{}        // signal to stop cleanup goroutine
}

func NewLogStoreClientPool(maxSendMsgSize int, maxRecvMsgSize int) LogStoreClientPool {
	p := &logStoreClientPool{
		maxSendMsgSize: maxSendMsgSize,
		maxRecvMsgSize: maxRecvMsgSize,
		connections:    make(map[string]*grpc.ClientConn),
		clients:        make(map[string]LogStoreClient),
		lastUsed:       make(map[string]time.Time),
		cleanupDone:    make(chan struct{}),
	}
	p.clientClosed.Store(false)
	go p.idleCleanupLoop()
	return p
}

func (p *logStoreClientPool) GetLogStoreClient(ctx context.Context, target string) (LogStoreClient, error) {
	p.RLock()
	if p.clientClosed.Load() {
		p.RUnlock()
		return nil, werr.ErrWoodpeckerClientClosed
	}
	client, ok := p.clients[target]
	cnx := p.connections[target]
	p.RUnlock()
	if ok && !isConnectionBroken(cnx) {
		// Update last-used timestamp
		p.Lock()
		p.lastUsed[target] = time.Now()
		p.Unlock()
		return client, nil
	}

	// Either no cached client, or the cached connection is in a broken state
	// (TRANSIENT_FAILURE / SHUTDOWN). Drop the stale entry and rebuild so that
	// the next dial goes through the DNS resolver again and picks up the new
	// peer address after a pod restart.
	if ok {
		p.Clear(ctx, target)
	}

	p.Lock()
	defer p.Unlock()
	if p.clientClosed.Load() {
		return nil, werr.ErrWoodpeckerClientClosed
	}

	if existing, ok := p.clients[target]; ok {
		// Another goroutine may have raced us and already built a fresh client
		// for the same target. Reuse it unless it is also broken.
		if !isConnectionBroken(p.connections[target]) {
			p.lastUsed[target] = time.Now()
			return existing, nil
		}
		p.clearUnsafe(target)
	}

	newCnx, err := p.getConnectionFromPoolUnsafe(target)
	if err != nil {
		return nil, err
	}

	client = &logStoreClientRemote{
		innerClient: proto.NewLogStoreClient(newCnx),
		// Back-refs so that RPC methods on this client can self-evict from
		// the pool when they observe a transport-level failure. This keeps
		// the Clear-on-error logic inside logstore_client_remote.go and out
		// of every caller (fence / append / discovery / etc.).
		pool:   p,
		target: target,
	}
	p.clients[target] = client
	p.lastUsed[target] = time.Now()
	return client, nil
}

// isConnectionBroken reports whether the given gRPC ClientConn is in a state
// from which meaningful recovery requires a brand-new connection (so that DNS
// is re-resolved and a new subchannel is created).
//
// We intentionally do NOT treat Idle/Connecting/Ready as broken — subchannels
// routinely pass through Idle between calls and Connecting during initial
// handshake. Dropping those would cause connection thrash under normal load.
func isConnectionBroken(cnx *grpc.ClientConn) bool {
	if cnx == nil {
		return false
	}
	state := cnx.GetState()
	return state == connectivity.TransientFailure || state == connectivity.Shutdown
}

// getConnectionFromPoolUnsafe is used when the caller already holds the lock
func (p *logStoreClientPool) getConnectionFromPoolUnsafe(target string) (grpc.ClientConnInterface, error) {
	cnx, ok := p.connections[target]
	if ok {
		return cnx, nil
	}

	cnx, err := p.newConnection(target)
	if err != nil {
		return nil, err
	}
	p.connections[target] = cnx
	return cnx, nil
}

func (p *logStoreClientPool) newConnection(target string) (*grpc.ClientConn, error) {
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(p.maxRecvMsgSize),
			grpc.MaxCallSendMsgSize(p.maxSendMsgSize),
			// NOTE: grpc.WaitForReady(true) is not used here because it will block the connection
			grpc.WaitForReady(false),
		),
		// NOTE: grpc.WithBlock() is not used here because it will block the connection
		grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(otelgrpc.StreamClientInterceptor()),
	}
	cnx, err := grpc.NewClient(target, options...)
	if err != nil {
		return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErr(err)
	}
	return cnx, nil
}

func (p *logStoreClientPool) Clear(ctx context.Context, target string) {
	p.Lock()
	defer p.Unlock()
	p.clearUnsafe(target)
}

// clearUnsafe removes the cached client/connection for target. Caller must
// already hold p's write lock.
func (p *logStoreClientPool) clearUnsafe(target string) {
	if cnx, ok := p.connections[target]; ok {
		_ = cnx.Close()
		delete(p.connections, target)
	}

	if client, ok := p.clients[target]; ok {
		_ = client.Close(context.TODO())
		delete(p.clients, target)
	}

	delete(p.lastUsed, target)
}

func (p *logStoreClientPool) Close(ctx context.Context) error {
	p.Lock()
	defer p.Unlock()
	if p.clientClosed.Load() {
		// already closed
		return nil
	}

	// Stop the cleanup goroutine before marking as closed
	close(p.cleanupDone)
	p.clientClosed.Store(true)

	// Close all clients first
	for target, client := range p.clients {
		err := client.Close(ctx)
		if err != nil {
			fmt.Printf("Error closing logstore client[%s]: %s\n", target, err.Error())
		}
	}

	// Then close all connections
	for target, cnx := range p.connections {
		err := cnx.Close()
		if err != nil {
			fmt.Printf("Error closing logstore connection[%s]: %s\n", target, err.Error())
		}
	}

	p.connections = make(map[string]*grpc.ClientConn)
	p.clients = make(map[string]LogStoreClient)
	p.lastUsed = make(map[string]time.Time)
	return nil
}

const (
	idleCleanupInterval = 1 * time.Minute
	maxIdleTime         = 5 * time.Minute
)

func (p *logStoreClientPool) idleCleanupLoop() {
	ticker := time.NewTicker(idleCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.cleanIdleConnections()
		case <-p.cleanupDone:
			return
		}
	}
}

func (p *logStoreClientPool) cleanIdleConnections() {
	p.Lock()
	defer p.Unlock()

	if p.clientClosed.Load() {
		return
	}

	now := time.Now()
	for target, lastUse := range p.lastUsed {
		if now.Sub(lastUse) > maxIdleTime {
			// Close idle connection
			if cnx, ok := p.connections[target]; ok {
				cnx.Close()
				delete(p.connections, target)
			}
			if client, ok := p.clients[target]; ok {
				client.Close(context.TODO())
				delete(p.clients, target)
			}
			delete(p.lastUsed, target)
		}
	}
}

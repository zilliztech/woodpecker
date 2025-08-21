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
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

var _ LogStoreClientPool = (*logStoreClientPool)(nil)

type logStoreClientPool struct {
	sync.RWMutex
	connections map[string]*grpc.ClientConn
	clients     map[string]LogStoreClient
}

func NewLogStoreClientPool() LogStoreClientPool {
	return &logStoreClientPool{
		connections: make(map[string]*grpc.ClientConn),
		clients:     make(map[string]LogStoreClient),
	}
}

func (p *logStoreClientPool) GetLogStoreClient(ctx context.Context, target string) (LogStoreClient, error) {
	p.RLock()
	client, ok := p.clients[target]
	p.RUnlock()
	if ok {
		return client, nil
	}

	p.Lock()
	defer p.Unlock()

	client, ok = p.clients[target]
	if ok {
		return client, nil
	}

	cnx, err := p.getConnectionFromPoolUnsafe(target)
	if err != nil {
		return nil, err
	}

	client = &logStoreClientRemote{
		innerClient: proto.NewLogStoreClient(cnx),
	}
	p.clients[target] = client
	return client, nil
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
	// TODO make more options configurable
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
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

	if cnx, ok := p.connections[target]; ok {
		_ = cnx.Close()
		delete(p.connections, target)
	}

	if client, ok := p.clients[target]; ok {
		_ = client.Close(context.TODO())
		delete(p.clients, target)
	}
}

func (p *logStoreClientPool) Close(ctx context.Context) error {
	p.Lock()
	defer p.Unlock()

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
	return nil
}

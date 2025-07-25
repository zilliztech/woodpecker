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
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server"
)

//go:generate mockery --dir=./woodpecker/client --name=LogStoreClientPool --structname=LogStoreClientPool --output=mocks/mocks_woodpecker/mocks_logstore_client --filename=mock_client_pool.go --with-expecter=true  --outpkg=mocks_logstore_client
type LogStoreClientPool interface {
	io.Closer
	GetLogStoreClient(target string) (LogStoreClient, error)
	Clear(target string)
}

func NewLogStoreClientPoolLocal(logStore server.LogStore) LogStoreClientPool {
	return &logStoreClientPoolLocal{
		innerLogStore: logStore,
	}
}

var _ LogStoreClientPool = (*logStoreClientPoolLocal)(nil)

type logStoreClientPoolLocal struct {
	sync.RWMutex
	innerLogStore server.LogStore
}

func (p *logStoreClientPoolLocal) GetLogStoreClient(target string) (LogStoreClient, error) {
	return &logStoreClientLocal{
		store: p.innerLogStore,
	}, nil
}

func (p *logStoreClientPoolLocal) Clear(target string) {
}

func (p *logStoreClientPoolLocal) Close() error {
	return nil
}

func NewLogStoreClientPool() LogStoreClientPool {
	return &logStoreClientPool{
		connections: make(map[string]*grpc.ClientConn),
		clients:     make(map[string]LogStoreClient),
	}
}

var _ LogStoreClientPool = (*logStoreClientPool)(nil)

type logStoreClientPool struct {
	sync.RWMutex
	connections map[string]*grpc.ClientConn
	clients     map[string]LogStoreClient
}

func (p *logStoreClientPool) GetLogStoreClient(target string) (LogStoreClient, error) {
	cnx, err := p.getConnectionFromPool(target)
	if err != nil {
		return nil, err
	}
	return &logStoreClientRemote{
		innerClient: proto.NewLogStoreClient(cnx),
	}, nil
}

func (p *logStoreClientPool) getConnectionFromPool(target string) (grpc.ClientConnInterface, error) {
	p.RLock()
	cnx, ok := p.connections[target]
	p.RUnlock()
	if ok {
		return cnx, nil
	}

	p.Lock()
	defer p.Unlock()

	cnx, ok = p.connections[target]
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
	options := []grpc.DialOption{}
	cnx, err := grpc.NewClient(target, options...)
	if err != nil {
		return nil, werr.ErrWoodpeckerClientConnectionFailed.WithCauseErr(err)
	}
	return cnx, nil
}

func (p *logStoreClientPool) Clear(target string) {
	p.Lock()
	defer p.Unlock()

}

func (p *logStoreClientPool) Close() error {
	p.Lock()
	defer p.Unlock()
	for target, cnx := range p.connections {
		err := cnx.Close()
		if err != nil {
			fmt.Printf("Error closing logstore client[%s]: %s\n", target, err.Error())
		}
	}
	p.connections = make(map[string]*grpc.ClientConn)
	p.clients = make(map[string]LogStoreClient)
	return nil
}

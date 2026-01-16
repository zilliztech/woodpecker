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
	"sync"
	"sync/atomic"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server"
)

var _ LogStoreClientPool = (*logStoreClientPoolLocal)(nil)

type logStoreClientPoolLocal struct {
	sync.RWMutex
	innerLogStore server.LogStore
	clientClosed  atomic.Bool
}

func NewLogStoreClientPoolLocal(logStore server.LogStore) LogStoreClientPool {
	p := &logStoreClientPoolLocal{
		innerLogStore: logStore,
	}
	p.clientClosed.Store(false)
	return p
}

func (p *logStoreClientPoolLocal) GetLogStoreClient(ctx context.Context, target string) (LogStoreClient, error) {
	if p.clientClosed.Load() {
		return nil, werr.ErrWoodpeckerClientClosed
	}
	return &logStoreClientLocal{
		store: p.innerLogStore,
	}, nil
}

func (p *logStoreClientPoolLocal) Clear(ctx context.Context, target string) {
}

func (p *logStoreClientPoolLocal) Close(ctx context.Context) error {
	if p.clientClosed.Load() {
		return nil
	}
	p.clientClosed.Store(true)
	return nil
}

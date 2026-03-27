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

import "context"

// LogStoreClientPool manages gRPC connections to LogStore server nodes.
//
// Connection Sharing: The pool maintains one gRPC connection per server address.
// All logs within the same Client instance share these connections via HTTP/2
// multiplexing. Creating multiple Client instances per process is discouraged
// as each creates a separate pool with its own connections.
//
// Best Practice: Create ONE Client instance (via NewClient/NewEmbedClient) per
// process and share it across all log operations. This ensures optimal connection
// reuse. Creating a Client per log is an anti-pattern that multiplies server-side
// connection count unnecessarily.
//
// Idle Cleanup: Connections unused for 5 minutes are automatically closed and
// removed from the pool. They will be re-established on next use.
//
//go:generate mockery --dir=./woodpecker/client --name=LogStoreClientPool --structname=LogStoreClientPool --output=mocks/mocks_woodpecker/mocks_logstore_client --filename=mock_client_pool.go --with-expecter=true  --outpkg=mocks_logstore_client
type LogStoreClientPool interface {
	GetLogStoreClient(ctx context.Context, target string) (LogStoreClient, error)
	Clear(ctx context.Context, target string)
	Close(ctx context.Context) error
}

//go:build noembedetcd

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

package etcd

import (
	"errors"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var ErrEmbedEtcdNotCompiled = errors.New("embedded etcd is not supported in this build (noembedetcd)")

func GetEmbedEtcdClient() (*clientv3.Client, error) { return nil, ErrEmbedEtcdNotCompiled }

func InitEtcdServer(useEmbedEtcd bool, configPath, dataDir, logPath, logLevel string) error {
	if useEmbedEtcd {
		return ErrEmbedEtcdNotCompiled
	}
	return nil
}

func HasServer() bool { return false }

func StopEtcdServer() {}

func StartEtcdServerUnsafe(useEmbedEtcd bool, configPath, dataDir, logPath, logLevel string) error {
	if useEmbedEtcd {
		return ErrEmbedEtcdNotCompiled
	}
	return nil
}

func ShutdownEtcdServerUnsafe() {}

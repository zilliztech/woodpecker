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
	"log"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

// EtcdServer is the singleton of embedded etcd server
var (
	initOnce   sync.Once
	closeOnce  sync.Once
	etcdServer *embed.Etcd
)

// GetEmbedEtcdClient returns client of embed etcd server
func GetEmbedEtcdClient() (*clientv3.Client, error) {
	client := v3client.New(etcdServer.Server)
	return client, nil
}

// InitEtcdServer initializes embedded etcd server singleton.
func InitEtcdServer(
	useEmbedEtcd bool,
	configPath string,
	dataDir string,
	logPath string,
	logLevel string,
) error {
	if useEmbedEtcd {
		var initError error
		initOnce.Do(func() {
			initError = StartEtcdServerUnsafe(useEmbedEtcd, configPath, dataDir, logPath, logLevel)
		})
		return initError
	}
	return nil
}

func HasServer() bool {
	return etcdServer != nil
}

// StopEtcdServer stops embedded etcd server singleton.
func StopEtcdServer() {
	if etcdServer != nil {
		closeOnce.Do(func() {
			etcdServer.Close()
		})
	}
}

func StartEtcdServerUnsafe(useEmbedEtcd bool,
	configPath string,
	dataDir string,
	logPath string,
	logLevel string) error {
	var initError error
	path := configPath
	var cfg *embed.Config
	if len(path) > 0 {
		cfgFromFile, err := embed.ConfigFromFile(path)
		if err != nil {
			initError = err
		}
		cfg = cfgFromFile
	} else {
		cfg = embed.NewConfig()
	}
	cfg.Dir = dataDir
	cfg.LogOutputs = []string{logPath}
	cfg.LogLevel = logLevel
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		log.Printf("failed to init embedded Etcd server %v", err)
		initError = err
	}
	etcdServer = e
	log.Printf("finish init Etcd config path:%s, dataDir:%s", path, dataDir)
	return initError
}

func ShutdownEtcdServerUnsafe() {
	etcdServer.Close()
}

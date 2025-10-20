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

package benchmark

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/meta"
)

// TestShowEtcd Test only for debug etcd
func TestShowEtcd(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // Address of etcd server
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. show simple dirs
	directoryPrefix := "woodpecker" // Directory prefix to print
	printDirContents(ctx, cli, directoryPrefix, "")

	// 2. show meta detail
	printMetaContents(t, ctx, cli)
}

func TestCheckLogExists(t *testing.T) {
	logName := "by-dev-rootcoord-dml_1"
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // Address of etcd server
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	metaProvider := meta.NewMetadataProvider(context.Background(), cli, 10000)
	defer metaProvider.Close()
	exists, err := metaProvider.CheckExists(context.Background(), logName)
	assert.NoError(t, err)
	assert.False(t, exists)
}

// Test only
func printDirContents(ctx context.Context, cli *clientv3.Client, prefix string, indent string) {
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		fmt.Printf("fatal %v", err)
	}

	for _, kv := range resp.Kvs {
		fmt.Printf("%s%s: %s\n", indent, string(kv.Key), string(kv.Value))

		// Recursively print contents of subdirectories
		if strings.HasSuffix(string(kv.Key), "/") {
			newPrefix := string(kv.Key)
			printDirContents(ctx, cli, newPrefix, indent+"  ")
		}
	}
}

func printMetaContents(t *testing.T, ctx context.Context, cli *clientv3.Client) {
	metaProvider := meta.NewMetadataProvider(ctx, cli, 10000)
	defer metaProvider.Close()
	logs, err := metaProvider.ListLogs(ctx)
	assert.NoError(t, err)
	for _, log := range logs {
		t.Logf("logName: %s", log)
		meta, err := metaProvider.GetLogMeta(ctx, log)
		if err != nil {
			t.Error(err)
		}
		t.Logf("logName:%s logMeta: %v", log, meta)
		segs, err := metaProvider.GetAllSegmentMetadata(ctx, log)
		if err != nil {
			t.Error(err)
		}
		// Sort the keys in segs
		var keys []int64
		for k := range segs {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
		// Print segMeta in sorted key order
		for _, key := range keys {
			t.Logf("segMeta: %v", segs[key])
		}
	}
}

// Test only
func TestClear(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // Address of etcd server
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = cli.Delete(ctx, meta.ServicePrefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("fatal %v", err)
	}
	t.Logf("clear finished")
}

func TestAcquireLogWriterLockPerformance(t *testing.T) {
	logName := "test_log_lock_perf"
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	assert.NoError(t, err)
	metaProvider := meta.NewMetadataProvider(context.Background(), etcdCli, 10000)
	defer metaProvider.Close()
	_ = metaProvider.InitIfNecessary(context.TODO())
	exists, err := metaProvider.CheckExists(context.Background(), logName)
	assert.NoError(t, err)
	assert.False(t, exists)

	// lock success
	start := time.Now()
	for i := 0; i < 10000; i++ {
		se, getLockErr := metaProvider.AcquireLogWriterLock(context.Background(), logName)
		assert.NoError(t, getLockErr)
		assert.NotNil(t, se)
		//if i%100 == 0 {
		//	c := time.Now().Sub(start)
		//logger.Ctx(context.Background()).Debug(fmt.Sprintf("lock %d spend %d ms", i, c.Milliseconds()))
		//}
	}
	cost := time.Now().Sub(start)
	// ~0.75ms per lock op
	assert.True(t, cost.Seconds() > 5)
}

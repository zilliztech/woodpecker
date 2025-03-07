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
		Endpoints:   []string{"localhost:2379"}, // etcd 服务器的地址
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. show simple dirs
	directoryPrefix := "woodpecker" // 要打印的目录前缀
	printDirContents(ctx, cli, directoryPrefix, "")

	// 2. show meta detail
	printMetaContents(t, ctx, cli)
}

func TestCheckLogExists(t *testing.T) {
	logName := "by-dev-rootcoord-dml_1"
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // etcd 服务器的地址
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	metaProvider := meta.NewMetadataProvider(context.Background(), cli)
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

		// 递归地打印子目录的内容
		if strings.HasSuffix(string(kv.Key), "/") {
			newPrefix := string(kv.Key)
			printDirContents(ctx, cli, newPrefix, indent+"  ")
		}
	}
}

func printMetaContents(t *testing.T, ctx context.Context, cli *clientv3.Client) {
	metaProvider := meta.NewMetadataProvider(ctx, cli)
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
		// 对 segs 的 key 进行排序
		var keys []int64
		for k := range segs {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
		// 按照排序后的 key 顺序打印 segMeta
		for _, key := range keys {
			t.Logf("segMeta: %v", segs[key])
		}
	}
}

// Test only
func TestClear(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // etcd 服务器的地址
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
	fmt.Printf("clear finished")
}

func TestAcquireLogWriterLockPerformance(t *testing.T) {
	logName := "test_log_lock_perf"
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	assert.NoError(t, err)
	metaProvider := meta.NewMetadataProvider(context.Background(), etcdCli)
	defer metaProvider.Close()
	_ = metaProvider.InitIfNecessary(context.TODO())
	exists, err := metaProvider.CheckExists(context.Background(), logName)
	assert.NoError(t, err)
	assert.False(t, exists)

	// lock success
	start := time.Now()
	for i := 0; i < 10000; i++ {
		getLockErr := metaProvider.AcquireLogWriterLock(context.Background(), logName)
		assert.NoError(t, getLockErr)
		//if i%100 == 0 {
		//	c := time.Now().Sub(start)
		//logger.Ctx(context.Background()).Debug(fmt.Sprintf("lock %d spend %d ms", i, c.Milliseconds()))
		//}
	}
	cost := time.Now().Sub(start)
	// ~0.75ms per lock op
	assert.True(t, cost.Seconds() > 5)
}

package etcd

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestEmbedEtcdServer(t *testing.T) {
	configPath := ""
	dataDir := "/tmp/test_etcd_data"
	logPath := "/tmp/test_etcd.log"
	logLevel := "info"
	// init etcd embed server
	if err := InitEtcdServer(true, configPath, dataDir, logPath, logLevel); err != nil {
		t.Fatalf("init embed etcd server failed: %v", err)
	}
	defer StopEtcdServer()

	// wait for etcd server start
	time.Sleep(2 * time.Second)

	// create etcd client
	client, err := GetEmbedEtcdClient()
	if err != nil {
		t.Fatalf("get etcd client failed: %v", err)
	}
	defer client.Close()

	// write data
	key := "test_key" + time.Now().Format("20060102150405")
	value := "test_value" + time.Now().Format("20060102150405")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("write data failed: %v", err)
	}

	// request kv
	getResponse, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("get data failed: %v", err)
	}

	// check kv
	assert.Equalf(t, 1, len(getResponse.Kvs),
		"get data failed, expect 1 key exists, but got %d", len(getResponse.Kvs))
	assert.Equalf(t, value, string(getResponse.Kvs[0].Value),
		"get data failed, expect value=%s, but got %s", value, string(getResponse.Kvs[0].Value))

	// test summary
	t.Logf("test embed etcd server success")
}

package stream

import (
	"context"
	"fmt"
	"github.com/milvus-io/woodpecker/common/etcd"
	"github.com/milvus-io/woodpecker/meta"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"strings"
	"testing"
	"time"
)

// TestShowEtcd Test only for debug etcd
func TestShowEtcd(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // etcd 服务器的地址
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	directoryPrefix := "/" // 要打印的目录前缀
	printDirContents(ctx, cli, directoryPrefix, "")
}

func printDirContents(ctx context.Context, cli *clientv3.Client, prefix string, indent string) {
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("fatal %v", err)
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

func TestClear(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // etcd 服务器的地址
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = cli.Delete(ctx, meta.ServicePrefix, clientv3.WithPrefix())
	if err != nil {
		log.Fatalf("fatal %v", err)
	}
	log.Printf("clear finished")
}

func TestE2EWrite(t *testing.T) {
	etcdCli, err := etcd.GetRemoteEtcdClient([]string{"127.0.0.1:2379"})
	if err != nil {
		fmt.Println(err)
		return
	}

	//client, err := NewWoodpeckerClientWithEtcd(context.Background(), etcdCli)
	client, err := NewWoodpeckerEmbedClientWithEtcd(context.Background(), etcdCli)
	if err != nil {
		fmt.Println(err)
	}

	// ###  CreateLog
	//createLogErr := client.GetMetadataProvider().CreateLog(context.Background(), "test_my_log")
	//if createLogErr != nil {
	//	fmt.Printf("Create log failed, err:%v\n", createLogErr)
	//	panic(createLogErr)
	//}

	// ### OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), "test_my_log")
	if openErr != nil {
		fmt.Printf("Open log failed, err:%v\n", openErr)
		panic(openErr)
	}
	logHandle.GetName()

	//	### OpenWriter
	logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
	if openWriterErr != nil {
		fmt.Printf("Open writer failed, err:%v\n", openWriterErr)
		panic(openWriterErr)
	}
	writeResultChan := logWriter.WriteAsync(context.Background(), []byte("hello world 1"))
	writeResult := <-writeResultChan
	if writeResult.Err != nil {
		fmt.Println(writeResult.Err)
		panic(writeResult.Err)
	}
	fmt.Printf("write success, returned recordId:%v\n", writeResult.LogMessageId)
	//writeResult := logWriter.Write(context.Background(), []byte("hello world 1"))
	//if writeResult.Err != nil {
	//	fmt.Println(writeResult.Err)
	//	panic(writeResult.Err)
	//}
	//fmt.Printf("write success, returned recordId:%v\n", writeResult.LogMessageId)

	writeResult = logWriter.Write(context.Background(), []byte("hello world 2"))
	if writeResult.Err != nil {
		fmt.Println(writeResult.Err)
		panic(writeResult.Err)
	}
	fmt.Printf("write success, returned recordId:%v\n", writeResult.LogMessageId)

}

// TestWrite example to show how to use woodpecker client to write msg to  unbounded log
func TestWrite(t *testing.T) {
	client := NewWoodpeckerClient(context.Background())
	createErr := client.CreateLog(context.Background(), "hello_log")
	if createErr != nil {
		fmt.Printf("Create log failed, err:%v\n", createErr)
		panic(createErr)
	}
	logHandle, openErr := client.OpenLog(context.Background(), "hello_log")
	if openErr != nil {
		fmt.Printf("Open log failed, err:%v\n", openErr)
		panic(openErr)
	}
	writer, openWriterErr := logHandle.OpenLogWriter(context.Background())
	if openWriterErr != nil {
		fmt.Printf("Open writer failed, err:%v\n", openWriterErr)
		panic(openWriterErr)
	}
	for i := 0; i < 10; i++ {
		writeResult := writer.Write(context.Background(), []byte(fmt.Sprintf("hello world %d", i)))
		if writeResult.Err != nil {
			fmt.Printf("write failed, err:%v\n", writeResult.Err)
			panic(writeResult.Err)
		}
		fmt.Printf("write %d success, returned recordId:%v\n", i, writeResult.LogMessageId)
	}

	closeErr := writer.Close(context.Background())
	if closeErr != nil {
		fmt.Printf("close failed, err:%v\n", closeErr)
		panic(closeErr)
	}

	fmt.Printf("Test Write finished\n")
}

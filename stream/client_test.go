package stream

import (
	"context"
	"fmt"
	"github.com/google/gops/agent"
	"net/http"
	"net/http/pprof"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/stream/log"
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

	directoryPrefix := "/" // 要打印的目录前缀
	printDirContents(ctx, cli, directoryPrefix, "")
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

func TestE2EWrite(t *testing.T) {
	etcdCli, err := etcd.GetRemoteEtcdClient([]string{"127.0.0.1:2379"})
	if err != nil {
		fmt.Println(err)
		return
	}

	client, err := NewWoodpeckerEmbedClient(context.Background(), etcdCli)
	if err != nil {
		fmt.Println(err)
	}

	// ###  CreateLog
	//createLogErr := client.GetMetadataProvider().CreateLog(context.Background(), "test_log")
	//if createLogErr != nil {
	//	fmt.Printf("Create log failed, err:%v\n", createLogErr)
	//	panic(createLogErr)
	//}

	// ### OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), "test_log")
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
	writeResult = logWriter.Write(context.Background(), []byte("hello world 2"))
	if writeResult.Err != nil {
		fmt.Println(writeResult.Err)
		panic(writeResult.Err)
	}
	fmt.Printf("write success, returned recordId:%v\n", writeResult.LogMessageId)
}

func startGopsAgent() {
	// start gops agent
	if err := agent.Listen(agent.Options{}); err != nil {
		panic(err)
	}
	http.HandleFunc("/pprof/cmdline", pprof.Cmdline)
	http.HandleFunc("/pprof/profile", pprof.Profile)
	http.HandleFunc("/pprof/symbol", pprof.Symbol)
	http.HandleFunc("/pprof/trace", pprof.Trace)
	go func() {
		fmt.Println("Starting gops agent on :6060")
		http.ListenAndServe(":6060", nil)
	}()
}

// TestWrite example to show how to use woodpecker client to write msg to  unbounded log
func TestWriteThroughput(t *testing.T) {
	startGopsAgent()

	etcdCli, err := etcd.GetRemoteEtcdClient([]string{"127.0.0.1:2379"})
	if err != nil {
		fmt.Println(err)
		return
	}

	client, err := NewWoodpeckerEmbedClient(context.Background(), etcdCli)
	if err != nil {
		fmt.Println(err)
	}

	// ###  CreateLog
	//createLogErr := client.GetMetadataProvider().CreateLog(context.Background(), "test_log")
	//if createLogErr != nil {
	//	fmt.Printf("Create log failed, err:%v\n", createLogErr)
	//	panic(createLogErr)
	//}

	// ### OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), "test_log")
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

	resultChan := make([]<-chan *log.WriteResult, 10000000)
	failedIdxs := make([]int, 0)
	successCount := 0
	for i := 0; i < 10000000; i++ {
		writeResultChan := logWriter.WriteAsync(context.Background(), []byte(fmt.Sprintf("hello world %d", i)))
		resultChan[i] = writeResultChan
	}
	for i := 0; i < 10000000; i++ {
		//fmt.Printf("wait %d\n", i)
		writeResult := <-resultChan[i]
		if writeResult.Err != nil {
			failedIdxs = append(failedIdxs, i)
			//fmt.Printf(writeResult.Err.Error())
		} else {
			successCount += 1
			//fmt.Printf("write %d success, returned recordId:%v\n", i, writeResult.LogMessageId)
		}
	}
	fmt.Printf("round 0 success count: %d \n", successCount)

	for i := 1; i <= 100; i++ {
		tmpFailedIdxs := make([]int, 0)
		successCount = 0
		for _, idx := range failedIdxs {
			writeResultChan := logWriter.WriteAsync(context.Background(),
				[]byte(fmt.Sprintf("hello world %d", idx)))
			resultChan[idx] = writeResultChan
		}
		for _, idx := range failedIdxs {
			writeResult := <-resultChan[idx]
			if writeResult.Err != nil {
				tmpFailedIdxs = append(tmpFailedIdxs, idx)
				//fmt.Printf(writeResult.Err.Error() + "\n")
			} else {
				successCount += 1
				//fmt.Printf("write %d success, returned recordId:%v\n", i, writeResult.LogMessageId)
			}
		}
		fmt.Printf("round %d success count: %d \n", i, successCount)
		failedIdxs = tmpFailedIdxs
		if len(failedIdxs) == 0 {
			break
		}
	}

	fmt.Printf("start close log writer \n")
	closeErr := logWriter.Close(context.Background())
	if closeErr != nil {
		fmt.Printf("close failed, err:%v\n", closeErr)
		panic(closeErr)
	}

	fmt.Printf("Test Write finished\n")
}

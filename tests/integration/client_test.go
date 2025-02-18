package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/pprof"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/gops/agent"
	minio2 "github.com/minio/minio-go/v7"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/stream"
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

func TestE2EWrite(t *testing.T) {
	startGopsAgent()

	cfg, err := config.NewConfiguration("")
	assert.NoError(t, err)
	client, err := stream.NewWoodpeckerEmbedClientFromConfig(context.Background(), cfg)
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
	writeResultChan := logWriter.WriteAsync(context.Background(),
		&log.WriterMessage{
			Payload: []byte("hello world 1"),
		},
	)
	writeResult := <-writeResultChan
	if writeResult.Err != nil {
		fmt.Println(writeResult.Err)
		panic(writeResult.Err)
	}
	fmt.Printf("write success, returned recordId:%v\n", writeResult.LogMessageId)
	//writeResult = logWriter.Write(context.Background(), []byte("hello world 2"))
	//if writeResult.Err != nil {
	//	fmt.Println(writeResult.Err)
	//	panic(writeResult.Err)
	//}
	//fmt.Printf("write success, returned recordId:%v\n", writeResult.LogMessageId)
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

var testMetricsRegistry prometheus.Registerer

var (
	MinioPutBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "minio",
			Subsystem: "test",
			Name:      "put_bytes",
			Help:      "bytes of put data",
		},
		[]string{"thread_id"},
	)
	MinioPutLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "minio",
			Subsystem: "test",
			Name:      "put_latency",
			Help:      "The latency of put operation",
		},
		[]string{"thread_id"},
	)
)

func startMetrics() {
	testMetricsRegistry = prometheus.DefaultRegisterer
	metrics.RegisterWoodpeckerWithRegisterer(testMetricsRegistry)
	testMetricsRegistry.MustRegister(MinioPutBytes)
	testMetricsRegistry.MustRegister(MinioPutLatency)

	// start metrics
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":29092", nil)
	}()
	//go recordMetrics()
}

//func recordMetrics() {
//	go func() {
//		for {
//			metrics.WpAppendRequestsCounter.WithLabelValues("testmylog").Inc()
//			time.Sleep(2 * time.Second)
//		}
//	}()
//}
//func TestAsyncWritePerformance2(t *testing.T) {
//	startMetrics()
//	for {
//		time.Sleep(1 * time.Second)
//	}
//}

func generateRandomBytes(length int) ([]byte, error) {
	randomBytes := make([]byte, length)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return nil, err
	}
	return randomBytes, nil
}

func TestMinioReadPerformance(t *testing.T) {
	startGopsAgent()
	startMetrics()
	cfg, err := config.NewConfiguration("")
	assert.NoError(t, err)
	cfg.Minio.BucketName = "zilliz-aws-us-west-2-wdxlw6gkyo"
	cfg.Minio.IamEndpoint = "s3.us-west-2.amazonaws.com"
	minioCli, err := minio.NewMinioClientFromConfig(context.Background(), cfg)
	assert.NoError(t, err)
	concurrentCh := make(chan int, 1)
	for i := 0; i < 1000; i++ {
		concurrentCh <- 1
		go func(ch chan int) {
			start := time.Now()
			obj, getErr := minioCli.GetObject(
				context.Background(),
				cfg.Minio.BucketName,
				fmt.Sprintf("test_object_%d", i),
				minio2.GetObjectOptions{})
			assert.NoError(t, getErr)
			objInfo, statErr := obj.Stat()
			assert.NoError(t, statErr)
			objData := make([]byte, objInfo.Size)
			readSize, readErr := obj.Read(objData)
			assert.Contains(t, readErr.Error(), "EOF") //
			cost := time.Now().Sub(start)
			fmt.Printf("Get test_object_%d completed,read %d bytes cost: %d ms \n", i, readSize, cost.Milliseconds())
			<-ch
		}(concurrentCh)
	}
	fmt.Printf("Test Minio Finish \n")
}

func TestMinioDelete(t *testing.T) {
	startGopsAgent()
	startMetrics()
	cfg, err := config.NewConfiguration("")
	assert.NoError(t, err)
	cfg.Minio.BucketName = "zilliz-aws-us-west-2-wdxlw6gkyo"
	cfg.Minio.IamEndpoint = "s3.us-west-2.amazonaws.com"

	minioCli, err := minio.NewMinioClientFromConfig(context.Background(), cfg)
	assert.NoError(t, err)
	concurrentCh := make(chan int, 1)
	for i := 0; i < 1000; i++ {
		concurrentCh <- 1
		go func(ch chan int) {
			removeErr := minioCli.RemoveObject(
				context.Background(),
				cfg.Minio.BucketName,
				fmt.Sprintf("test_object_%d", i),
				minio2.RemoveObjectOptions{})
			assert.NoError(t, removeErr)
			if removeErr != nil {
				fmt.Printf("remove test_object_%d failed,err:%v\n", i, removeErr)
				return
			}
			fmt.Printf("remove test_object_%d completed,\n", i)
			<-ch
		}(concurrentCh)
	}
	fmt.Printf("Test Minio Finish \n")
}

var totalBytes atomic.Int64

func startReporting() {
	go func(total *atomic.Int64) {
		ticker := time.NewTicker(time.Duration(1000 * int(time.Millisecond)))
		defer ticker.Stop()
		lastTime := time.Now().UnixMilli()
		lastTotal := total.Load()
		for {
			select {
			case <-ticker.C:
				currentTime := time.Now().UnixMilli()
				currentTotal := total.Load()
				rate := float64(currentTotal-lastTotal) / float64(currentTime-lastTime) * 1000 / 1_000_000
				fmt.Printf("put bytes Rate: %d MB/s \n", rate)
			}
		}

	}(&totalBytes)
}

func TestMinioWritePerformance(t *testing.T) {
	startGopsAgent()
	startMetrics()
	startReporting()
	cfg, err := config.NewConfiguration("")
	assert.NoError(t, err)
	cfg.Minio.BucketName = "zilliz-aws-us-west-2-wdxlw6gkyo"
	cfg.Minio.IamEndpoint = "s3.us-west-2.amazonaws.com"
	//minioCli, err := minio.NewMinioClient(context.Background(), bucketName)
	minioCli, err := minio.NewMinioClientFromConfig(context.Background(), cfg)
	assert.NoError(t, err)
	payloadStaticData, err := generateRandomBytes(4 * 1024)
	concurrentCh := make(chan int, 1)

	for i := 0; i < 1000; i++ {
		concurrentCh <- 1
		go func(ch chan int) {
			start := time.Now()
			_, putErr := minioCli.PutObject(
				context.Background(),
				cfg.Minio.BucketName,
				fmt.Sprintf("test_object_%d", i),
				bytes.NewReader(payloadStaticData),
				int64(len(payloadStaticData)),
				minio2.PutObjectOptions{})
			assert.NoError(t, putErr)
			cost := time.Now().Sub(start)
			fmt.Printf("Put test_object_%d completed,  cost: %d ms \n", i, cost.Milliseconds())
			<-ch
			MinioPutBytes.WithLabelValues("0").Observe(float64(len(payloadStaticData)))
			MinioPutLatency.WithLabelValues("0").Observe(float64(cost.Milliseconds()))
			// Test only
			totalBytes.Add(int64(len(payloadStaticData)))
		}(concurrentCh)
	}
	fmt.Printf("Test Minio Finish \n")
}

func TestAsyncWritePerformance(t *testing.T) {
	startGopsAgent()
	startMetrics()
	cfg, err := config.NewConfiguration("")
	assert.NoError(t, err)

	client, err := stream.NewWoodpeckerEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ###  CreateLog
	//createLogErr := client.CreateLog(context.Background(), "test_log")
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

	payloadStaticData, err := generateRandomBytes(4 * 1024)
	assert.NoError(t, err)

	resultChan := make([]<-chan *log.WriteResult, 100000000)
	failedIdxs := make([]int, 0)
	successCount := 0
	for i := 0; i < 100000000; i++ {
		writeResultChan := logWriter.WriteAsync(context.Background(),
			&log.WriterMessage{
				//Payload: []byte(fmt.Sprintf("hello world %d", i)),
				Payload: payloadStaticData,
				Properties: map[string]string{
					"key": fmt.Sprintf("value%d", i),
				},
			},
		)
		resultChan[i] = writeResultChan
	}
	for i := 0; i < 100000000; i++ {
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
	for i := 1; i <= 100000000; i++ {
		tmpFailedIdxs := make([]int, 0)
		successCount = 0
		for _, idx := range failedIdxs {
			writeResultChan := logWriter.WriteAsync(context.Background(),
				&log.WriterMessage{
					Payload: payloadStaticData,
				},
			)
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

// TestWrite example to show how to use woodpecker client to write msg to  unbounded log
func TestWriteThroughput(t *testing.T) {
	startGopsAgent()
	cfg, err := config.NewConfiguration("")
	assert.NoError(t, err)

	client, err := stream.NewWoodpeckerEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ###  CreateLog
	//createLogErr := client.CreateLog(context.Background(), "test_log")
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

	resultChan := make([]<-chan *log.WriteResult, 1001)
	failedIdxs := make([]int, 0)
	successCount := 0
	for i := 0; i < 1001; i++ {
		writeResultChan := logWriter.WriteAsync(context.Background(),
			&log.WriterMessage{
				Payload: []byte(fmt.Sprintf("hello world %d", i)),
				Properties: map[string]string{
					"key": fmt.Sprintf("value%d", i),
				},
			},
		)
		resultChan[i] = writeResultChan
	}
	for i := 0; i < 1001; i++ {
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
				&log.WriterMessage{
					Payload: []byte(fmt.Sprintf("hello world %d", idx)),
				},
			)
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

func TestReadThroughput(t *testing.T) {
	startGopsAgent()
	cfg, err := config.NewConfiguration("")
	assert.NoError(t, err)
	client, err := stream.NewWoodpeckerEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ### OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), "test_log")
	if openErr != nil {
		fmt.Printf("Open log failed, err:%v\n", openErr)
		panic(openErr)
	}

	//	### OpenReader
	start := &log.LogMessageId{
		SegmentId: 21,
		EntryId:   0,
	}
	logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), start)
	if openReaderErr != nil {
		fmt.Printf("Open reader failed, err:%v\n", openReaderErr)
		panic(openReaderErr)
	}

	// 调用reader遍历所有的数据 logReader.ReadNext(context.Background())
	for {
		msg, err := logReader.ReadNext(context.Background())
		if err != nil {
			fmt.Printf("read failed, err:%v\n", err)
			panic(err)
		} else {
			fmt.Printf("read success, msg:%v\n", msg)
		}
	}

	fmt.Printf("Test Read finished\n")
}

func TestReadFromEarliest(t *testing.T) {
	startGopsAgent()
	cfg, err := config.NewConfiguration("")
	assert.NoError(t, err)
	client, err := stream.NewWoodpeckerEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ### OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), "test_log")
	if openErr != nil {
		fmt.Printf("Open log failed, err:%v\n", openErr)
		panic(openErr)
	}

	//	### OpenReader
	start := log.EarliestLogMessageID
	logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), start)
	if openReaderErr != nil {
		fmt.Printf("Open reader failed, err:%v\n", openReaderErr)
		panic(openReaderErr)
	}

	// 调用reader遍历所有的数据 logReader.ReadNext(context.Background())
	totalEntries := 0
	for {
		msg, err := logReader.ReadNext(context.Background())
		if err != nil {
			fmt.Printf("read failed, err:%v\n", err)
			break
		} else {
			fmt.Printf("read success, msg:%v\n", msg)
		}
		totalEntries += 1
		if totalEntries%10000 == 0 {
			fmt.Printf(" read %d success, the msg:%v \n", totalEntries, msg)
		}
	}
	fmt.Printf("final read %d success \n", totalEntries)

	fmt.Printf("Test Read finished\n")
}

func TestReadFromLatest(t *testing.T) {
	startGopsAgent()
	cfg, err := config.NewConfiguration("")
	assert.NoError(t, err)
	client, err := stream.NewWoodpeckerEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ### OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), "test_log")
	if openErr != nil {
		fmt.Printf("Open log failed, err:%v\n", openErr)
		panic(openErr)
	}

	//	### OpenReader
	start := log.LatestLogMessageID
	logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), start)
	if openReaderErr != nil {
		fmt.Printf("Open reader failed, err:%v\n", openReaderErr)
		panic(openReaderErr)
	}

	// 调用reader遍历所有的数据 logReader.ReadNext(context.Background())
	for {
		msg, err := logReader.ReadNext(context.Background())
		if err != nil {
			fmt.Printf("read failed, err:%v\n", err)
			t.Error(err)
		} else {
			fmt.Printf("read success, msg:%v\n", msg)
		}
	}

	fmt.Printf("Test Read finished\n")
}

func TestReadFromSpecifiedPosition(t *testing.T) {
	startGopsAgent()
	cfg, err := config.NewConfiguration("")
	assert.NoError(t, err)
	client, err := stream.NewWoodpeckerEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ### OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), "test_log")
	if openErr != nil {
		fmt.Printf("Open log failed, err:%v\n", openErr)
		panic(openErr)
	}

	//	### OpenReader
	start := &log.LogMessageId{
		SegmentId: 1,
		EntryId:   0,
	}
	logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), start)
	if openReaderErr != nil {
		fmt.Printf("Open reader failed, err:%v\n", openReaderErr)
		panic(openReaderErr)
	}

	// 调用reader遍历所有的数据 logReader.ReadNext(context.Background())
	for {
		msg, err := logReader.ReadNext(context.Background())
		if err != nil {
			fmt.Printf("read failed, err:%v\n", err)
			t.Error(err)
		} else {
			fmt.Printf("read success, msg:%v\n", msg)
		}
	}

	fmt.Printf("Test Read finished\n")
}

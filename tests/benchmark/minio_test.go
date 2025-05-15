package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
)

const (
	TEST_OBJECT_PREFIX = "test_object_"
	TEST_COUNT         = 100
	TEST_OBJECT_SIZE   = 128_000_000
)

func TestMinioReadPerformance(t *testing.T) {
	startGopsAgent()
	startMetrics()
	startReporting()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)
	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	assert.NoError(t, err)
	concurrentCh := make(chan int, 1)
	for i := 0; i < TEST_COUNT; i++ {
		concurrentCh <- 1
		objectId := i
		go func(ch chan int) {
			start := time.Now()
			getOpts := minio.GetObjectOptions{}
			//optErr := getOpts.SetRange(0, 10) // start
			//optErr := getOpts.SetRange(0, 1_000_000) // start
			//optErr := getOpts.SetRange(0, 4_000_000) // start

			//optErr := getOpts.SetRange(8_000_000, 8_000_010) // mid
			//optErr := getOpts.SetRange(8_000_000, 9_000_010) // mid
			//optErr := getOpts.SetRange(6_000_000, 10_000_010) // mid
			//optErr := getOpts.SetRange(128_000_000, 129_000_000) // mid
			//optErr := getOpts.SetRange(128_000_000, 132_000_000) // mid

			//optErr := getOpts.SetRange(0, -10) // last
			//optErr := getOpts.SetRange(0, -1000000) // last
			optErr := getOpts.SetRange(0, -4000000) // last
			assert.NoError(t, optErr)

			obj, getErr := minioCli.GetObject(
				context.Background(),
				cfg.Minio.BucketName,
				fmt.Sprintf("%s%d", TEST_OBJECT_PREFIX, objectId),
				getOpts)
			assert.NoError(t, getErr)
			defer obj.Close()

			readData, err := ioutil.ReadAll(obj)
			assert.NoError(t, err)
			readSize := len(readData)
			cost := time.Now().Sub(start)
			//fmt.Printf("Get test_object_%d completed,read %d bytes cost: %d ms \n", i, readSize, cost.Milliseconds())
			<-ch
			MinioIOBytes.WithLabelValues("0").Observe(float64(readSize))
			MinioIOLatency.WithLabelValues("0").Observe(float64(cost.Milliseconds()))
		}(concurrentCh)
	}
	fmt.Printf("Test Minio Finish \n")
}

func TestMinioDelete(t *testing.T) {
	startGopsAgent()
	startMetrics()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)
	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	assert.NoError(t, err)
	concurrentCh := make(chan int, 1)
	for i := 0; i < TEST_COUNT; i++ {
		concurrentCh <- 1
		objectId := i
		go func(ch chan int) {
			removeErr := minioCli.RemoveObject(
				context.Background(),
				cfg.Minio.BucketName,
				fmt.Sprintf("%s%d", TEST_OBJECT_PREFIX, objectId),
				minio.RemoveObjectOptions{})
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

func TestMinioWritePerformance(t *testing.T) {
	startGopsAgent()
	startMetrics()
	startReporting()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)
	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	assert.NoError(t, err)
	payloadStaticData, err := generateRandomBytes(TEST_OBJECT_SIZE) //
	concurrentCh := make(chan int, 1)                               // 1 concurrency
	wg := sync.WaitGroup{}
	for i := 0; i < TEST_COUNT; i++ {
		concurrentCh <- 1
		objectId := i
		wg.Add(1)
		go func(ch chan int) {
			start := time.Now()
			_, putErr := minioCli.PutObject(
				context.Background(),
				cfg.Minio.BucketName,
				fmt.Sprintf("%s%d", TEST_OBJECT_PREFIX, objectId),
				bytes.NewReader(payloadStaticData),
				int64(len(payloadStaticData)),
				minio.PutObjectOptions{})
			assert.NoError(t, putErr)
			cost := time.Now().Sub(start)
			//fmt.Printf("Put test_object_%d completed,  cost: %d ms \n", i, cost.Milliseconds())
			<-ch
			wg.Done()
			MinioIOBytes.WithLabelValues("0").Observe(float64(len(payloadStaticData)))
			MinioIOLatency.WithLabelValues("0").Observe(float64(cost.Milliseconds()))
		}(concurrentCh)
	}
	wg.Wait()
	fmt.Printf("Test Minio Finish \n")
}

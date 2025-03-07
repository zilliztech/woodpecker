package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
)

func TestMinioReadPerformance(t *testing.T) {
	startGopsAgent()
	startMetrics()
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Minio.BucketName = "zilliz-aws-us-west-2-wdxlw6gkyo"
	cfg.Minio.IamEndpoint = "s3.us-west-2.amazonaws.com"
	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
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
				minio.GetObjectOptions{})
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
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Minio.BucketName = "zilliz-aws-us-west-2-wdxlw6gkyo"
	cfg.Minio.IamEndpoint = "s3.us-west-2.amazonaws.com"

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	assert.NoError(t, err)
	concurrentCh := make(chan int, 1)
	for i := 0; i < 1000; i++ {
		concurrentCh <- 1
		go func(ch chan int) {
			removeErr := minioCli.RemoveObject(
				context.Background(),
				cfg.Minio.BucketName,
				fmt.Sprintf("test_object_%d", i),
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
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Minio.BucketName = "zilliz-aws-us-west-2-wdxlw6gkyo"
	cfg.Minio.IamEndpoint = "s3.us-west-2.amazonaws.com"
	//minioCli, err := minio.NewMinioClient(context.Background(), bucketName)
	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
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
				minio.PutObjectOptions{})
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

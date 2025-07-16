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
	TEST_OBJECT_PREFIX     = "test_object_"
	TEST_COUNT             = 1000
	TEST_OBJECT_SIZE       = 64_000
	CONCURRENT             = 1
	CONDITION_WRITE_ENABLE = true
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
			//t.Logf("Get test_object_%d completed,read %d bytes cost: %d ms \n", i, readSize, cost.Milliseconds())
			<-ch
			MinioIOBytes.WithLabelValues("0").Observe(float64(readSize))
			MinioIOLatency.WithLabelValues("0").Observe(float64(cost.Milliseconds()))
		}(concurrentCh)
	}
	t.Logf("Test Minio Finish \n")
}

func TestMinioDelete(t *testing.T) {
	startGopsAgent()
	startMetrics()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)
	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	assert.NoError(t, err)
	concurrentCh := make(chan int, CONCURRENT)
	wg := sync.WaitGroup{}
	for i := 0; i < TEST_COUNT; i++ {
		concurrentCh <- 1
		objectId := i
		wg.Add(1)
		go func(ch chan int) {
			removeErr := minioCli.RemoveObject(
				context.Background(),
				cfg.Minio.BucketName,
				fmt.Sprintf("%s%d", TEST_OBJECT_PREFIX, objectId),
				minio.RemoveObjectOptions{})
			assert.NoError(t, removeErr)
			if removeErr != nil {
				t.Logf("remove test_object_%d failed,err:%v\n", i, removeErr)
				return
			}
			t.Logf("remove test_object_%d completed,\n", i)
			<-ch
			wg.Done()
		}(concurrentCh)
	}
	wg.Wait()
	t.Logf("Test Minio Finish \n")
}

func TestMinioWritePerformance(t *testing.T) {
	startGopsAgent()
	startMetrics()
	startReporting()
	startTime := time.Now()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)
	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	assert.NoError(t, err)
	payloadStaticData, err := generateRandomBytes(TEST_OBJECT_SIZE) //
	concurrentCh := make(chan int, CONCURRENT)                      //  concurrency
	wg := sync.WaitGroup{}
	fmt.Printf("Test Minio Start, objectSize:%d concurrent:%d condition:%v ms \n", TEST_OBJECT_SIZE, CONCURRENT, CONDITION_WRITE_ENABLE)
	for i := 0; i < TEST_COUNT; i++ {
		concurrentCh <- 1
		objectId := i
		wg.Add(1)
		go func(ch chan int) {
			start := time.Now()
			if CONDITION_WRITE_ENABLE {
				_, putErr := minioCli.PutObjectIfNoneMatch(
					context.Background(),
					cfg.Minio.BucketName,
					fmt.Sprintf("%s%d", TEST_OBJECT_PREFIX, objectId),
					bytes.NewReader(payloadStaticData),
					int64(len(payloadStaticData)))
				assert.NoError(t, putErr)
			} else {
				_, putErr := minioCli.PutObject(
					context.Background(),
					cfg.Minio.BucketName,
					fmt.Sprintf("%s%d", TEST_OBJECT_PREFIX, objectId),
					bytes.NewReader(payloadStaticData),
					int64(len(payloadStaticData)),
					minio.PutObjectOptions{})
				assert.NoError(t, putErr)
			}
			cost := time.Now().Sub(start)
			//t.Logf("Put test_object_%d completed,  cost: %d ms \n", i, cost.Milliseconds())
			<-ch
			wg.Done()
			MinioIOBytes.WithLabelValues("0").Observe(float64(len(payloadStaticData)))
			MinioIOLatency.WithLabelValues("0").Observe(float64(cost.Milliseconds()))
		}(concurrentCh)
	}
	wg.Wait()
	fmt.Printf("Test Minio Finish, cost: %d ms \n", time.Now().Sub(startTime).Milliseconds())
}

// TestMinioSingleThreadLatency tests single-threaded MinIO put latency with different object sizes
func TestMinioSingleThreadLatency(t *testing.T) {
	startGopsAgent()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	assert.NoError(t, err)

	// Different object sizes to test with corresponding object counts
	type testConfig struct {
		size  int
		count int
	}

	testConfigs := []testConfig{
		{1024, 5000},    // 1KB - 5000 objects
		{4096, 5000},    // 4KB - 5000 objects
		{8192, 3000},    // 8KB - 3000 objects
		{16384, 2000},   // 16KB - 2000 objects
		{32768, 1500},   // 32KB - 1500 objects
		{65536, 1000},   // 64KB - 1000 objects
		{131072, 800},   // 128KB - 800 objects
		{262144, 600},   // 256KB - 600 objects
		{524288, 400},   // 512KB - 400 objects
		{1048576, 300},  // 1MB - 300 objects
		{2097152, 200},  // 2MB - 200 objects
		{4194304, 150},  // 4MB - 150 objects
		{8388608, 120},  // 8MB - 120 objects
		{16777216, 100}, // 16MB - 100 objects
	}
	uniqueId := time.Now().UnixMilli()

	fmt.Printf("Test MinIO Single Thread Latency Start\n")

	for _, config := range testConfigs {
		objectSize := config.size
		objectCount := config.count

		fmt.Printf("\n=== Testing Object Size: %d bytes (%.2f MB) with %d objects ===\n",
			objectSize, float64(objectSize)/(1024*1024), objectCount)

		payloadStaticData, err := generateRandomBytes(objectSize)
		assert.NoError(t, err)

		objectPrefix := fmt.Sprintf("latency_test_%d_%d_", uniqueId, objectSize)
		startTime := time.Now()

		var totalLatency time.Duration
		var minLatency time.Duration = time.Hour // Initialize to a large value
		var maxLatency time.Duration

		// Single-threaded sequential puts
		for i := 0; i < objectCount; i++ {
			objectKey := fmt.Sprintf("%s%d", objectPrefix, i)
			start := time.Now()

			_, putErr := minioCli.PutObject(
				context.Background(),
				cfg.Minio.BucketName,
				objectKey,
				bytes.NewReader(payloadStaticData),
				int64(len(payloadStaticData)),
				minio.PutObjectOptions{})
			assert.NoError(t, putErr)

			latency := time.Since(start)
			totalLatency += latency

			if latency < minLatency {
				minLatency = latency
			}
			if latency > maxLatency {
				maxLatency = latency
			}

			// Progress reporting - adapt interval based on object count
			reportInterval := objectCount / 5 // Report 5 times during the test
			if reportInterval < 10 {
				reportInterval = 10 // Minimum interval
			}
			if (i+1)%reportInterval == 0 {
				avgLatency := totalLatency / time.Duration(i+1)
				fmt.Printf("Progress: %d/%d, Avg latency: %.2f ms\n",
					i+1, objectCount, float64(avgLatency.Microseconds())/1000.0)
			}
		}

		totalTime := time.Since(startTime)
		avgLatency := totalLatency / time.Duration(objectCount)
		throughput := float64(objectCount) / totalTime.Seconds()
		totalBytes := objectCount * objectSize
		mbps := float64(totalBytes) / totalTime.Seconds() / (1024 * 1024)

		fmt.Printf("Results for %d bytes objects:\n", objectSize)
		fmt.Printf("  Total time: %v\n", totalTime)
		fmt.Printf("  Average latency: %.2f ms\n", float64(avgLatency.Microseconds())/1000.0)
		fmt.Printf("  Min latency: %.2f ms\n", float64(minLatency.Microseconds())/1000.0)
		fmt.Printf("  Max latency: %.2f ms\n", float64(maxLatency.Microseconds())/1000.0)
		fmt.Printf("  Throughput: %.2f objects/sec\n", throughput)
		fmt.Printf("  Data throughput: %.2f MB/s\n", mbps)
	}

	fmt.Printf("\nTest MinIO Single Thread Latency Finish\n")
}

// TestMinioSingleReadLatency tests single-threaded MinIO get latency with different object sizes
func TestMinioSingleReadLatency(t *testing.T) {
	startGopsAgent()

	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)

	minioCli, err := minioHandler.NewMinioHandler(context.Background(), cfg)
	assert.NoError(t, err)

	// Different object sizes to test with corresponding object counts
	type testConfig struct {
		size  int
		count int
	}

	testConfigs := []testConfig{
		{1024, 5000},    // 1KB - 5000 objects
		{4096, 5000},    // 4KB - 5000 objects
		{8192, 3000},    // 8KB - 3000 objects
		{16384, 2000},   // 16KB - 2000 objects
		{32768, 1500},   // 32KB - 1500 objects
		{65536, 1000},   // 64KB - 1000 objects
		{131072, 800},   // 128KB - 800 objects
		{262144, 600},   // 256KB - 600 objects
		{524288, 400},   // 512KB - 400 objects
		{1048576, 300},  // 1MB - 300 objects
		{2097152, 200},  // 2MB - 200 objects
		{4194304, 150},  // 4MB - 150 objects
		{8388608, 120},  // 8MB - 120 objects
		{16777216, 100}, // 16MB - 100 objects
	}
	uniqueId := time.Now().UnixMilli()

	fmt.Printf("Test MinIO Single Thread Read Latency Start\n")

	for _, config := range testConfigs {
		objectSize := config.size
		objectCount := config.count

		fmt.Printf("\n=== Testing Read Object Size: %d bytes (%.2f MB) with %d objects ===\n",
			objectSize, float64(objectSize)/(1024*1024), objectCount)

		// First, create test objects for reading
		payloadStaticData, err := generateRandomBytes(objectSize)
		assert.NoError(t, err)

		objectPrefix := fmt.Sprintf("read_latency_test_%d_%d_", uniqueId, objectSize)

		// Create objects first
		fmt.Printf("Creating %d test objects...\n", objectCount)
		for i := 0; i < objectCount; i++ {
			objectKey := fmt.Sprintf("%s%d", objectPrefix, i)
			_, putErr := minioCli.PutObject(
				context.Background(),
				cfg.Minio.BucketName,
				objectKey,
				bytes.NewReader(payloadStaticData),
				int64(len(payloadStaticData)),
				minio.PutObjectOptions{})
			assert.NoError(t, putErr)
		}

		// Now test read latency
		fmt.Printf("Testing read latency...\n")
		startTime := time.Now()

		var totalLatency time.Duration
		var minLatency time.Duration = time.Hour // Initialize to a large value
		var maxLatency time.Duration

		// Single-threaded sequential reads
		for i := 0; i < objectCount; i++ {
			objectKey := fmt.Sprintf("%s%d", objectPrefix, i)
			start := time.Now()

			obj, getErr := minioCli.GetObject(
				context.Background(),
				cfg.Minio.BucketName,
				objectKey,
				minio.GetObjectOptions{})
			assert.NoError(t, getErr)

			readData, err := ioutil.ReadAll(obj)
			assert.NoError(t, err)
			obj.Close()

			// Verify we read the expected amount
			assert.Equal(t, objectSize, len(readData))

			latency := time.Since(start)
			totalLatency += latency

			if latency < minLatency {
				minLatency = latency
			}
			if latency > maxLatency {
				maxLatency = latency
			}

			// Progress reporting - adapt interval based on object count
			reportInterval := objectCount / 5 // Report 5 times during the test
			if reportInterval < 10 {
				reportInterval = 10 // Minimum interval
			}
			if (i+1)%reportInterval == 0 {
				avgLatency := totalLatency / time.Duration(i+1)
				fmt.Printf("Progress: %d/%d, Avg latency: %.2f ms\n",
					i+1, objectCount, float64(avgLatency.Microseconds())/1000.0)
			}
		}

		totalTime := time.Since(startTime)
		avgLatency := totalLatency / time.Duration(objectCount)
		throughput := float64(objectCount) / totalTime.Seconds()
		totalBytes := objectCount * objectSize
		mbps := float64(totalBytes) / totalTime.Seconds() / (1024 * 1024)

		fmt.Printf("Read Results for %d bytes objects:\n", objectSize)
		fmt.Printf("  Total time: %v\n", totalTime)
		fmt.Printf("  Average latency: %.2f ms\n", float64(avgLatency.Microseconds())/1000.0)
		fmt.Printf("  Min latency: %.2f ms\n", float64(minLatency.Microseconds())/1000.0)
		fmt.Printf("  Max latency: %.2f ms\n", float64(maxLatency.Microseconds())/1000.0)
		fmt.Printf("  Throughput: %.2f objects/sec\n", throughput)
		fmt.Printf("  Data throughput: %.2f MB/s\n", mbps)

		// Clean up test objects
		fmt.Printf("Cleaning up test objects...\n")
		for i := 0; i < objectCount; i++ {
			objectKey := fmt.Sprintf("%s%d", objectPrefix, i)
			minioCli.RemoveObject(
				context.Background(),
				cfg.Minio.BucketName,
				objectKey,
				minio.RemoveObjectOptions{})
		}
	}

	fmt.Printf("\nTest MinIO Single Thread Read Latency Finish\n")
}

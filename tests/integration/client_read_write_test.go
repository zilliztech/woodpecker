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

package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// Helper function to get the minimum of two integers
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestReadTheWrittenDataSequentially(t *testing.T) {
	utils.StartGopsAgentWithPort(6060)
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestReadTheWrittenDataSequentially")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
		needCluster bool // Whether to start cluster for service mode
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
			needCluster: false,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
			needCluster: false,
		},
		{
			name:        "ServiceStorage",
			storageType: "service",
			rootPath:    rootPath + "_service",
			needCluster: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			cfg.Log.Level = "debug"

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Setup cluster if needed
			var client woodpecker.Client
			if tc.needCluster {
				// Start cluster for service mode
				const nodeCount = 3
				cluster, cfg, seeds := utils.StartMiniClusterWithCfg(t, nodeCount, tc.rootPath, cfg)
				seedList := strings.Join(seeds, ",")
				cfg.Woodpecker.Client.ServiceSeedNodes = seedList
				defer func() {
					cluster.StopMultiNodeCluster(t)
				}()

				// Setup etcd client for service mode
				etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
				assert.NoError(t, err)
				defer etcdCli.Close()

				// Create service mode client
				client, err = woodpecker.NewClient(ctx, cfg, etcdCli, true)
				assert.NoError(t, err)
				defer func() {
					if client != nil {
						_ = client.Close(ctx)
					}
				}()
			} else {
				// Use embed client for local and object storage
				client, err = woodpecker.NewEmbedClientFromConfig(ctx, cfg)
				assert.NoError(t, err)
				defer func() {
					// stop embed LogStore singleton only for non-service mode
					stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
					assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
				}()
			}
			defer client.Close(context.TODO())

			// CreateLog if not exists
			logName := "TestReadTheWrittenDataSequentially_" + t.Name() + "_" + time.Now().Format("20060102150405")
			client.CreateLog(context.Background(), logName)

			// OpenLog
			logHandle, openErr := client.OpenLog(context.Background(), logName)
			assert.NoError(t, openErr)

			// Test write
			// OpenWriter
			logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, openWriterErr)
			resultChan := make([]<-chan *log.WriteResult, 1000)
			for i := 0; i < 1000; i++ {
				writeResultChan := logWriter.WriteAsync(context.Background(),
					&log.WriteMessage{
						Payload: []byte(fmt.Sprintf("hello world %d", i)),
						Properties: map[string]string{
							"key": fmt.Sprintf("value%d", i),
						},
					},
				)
				resultChan[i] = writeResultChan
			}
			failedIds := make([]*log.WriteResult, 0)
			successIds := make([]*log.LogMessageId, 0)
			for i := 0; i < 1000; i++ {
				writeResult := <-resultChan[i]
				if writeResult.Err != nil {
					failedIds = append(failedIds, writeResult)
				} else {
					successIds = append(successIds, writeResult.LogMessageId)
				}
			}

			assert.Equal(t, 0, len(failedIds))
			assert.Equal(t, 1000, len(successIds))
			for idx, msgId := range successIds {
				if idx == 0 {
					continue
				}
				if msgId.SegmentId == successIds[idx-1].SegmentId {
					assert.True(t, msgId.EntryId == successIds[idx-1].EntryId+1)
				} else {
					assert.True(t, msgId.SegmentId > successIds[idx-1].SegmentId)
				}
			}
			closeErr := logWriter.Close(context.Background())
			assert.NoError(t, closeErr)
			fmt.Println("Test Write finished")

			// Test read all from earliest
			{
				earliest := log.EarliestLogMessageID()
				logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &earliest, "client-earliest-reader")
				assert.NoError(t, openReaderErr)
				readMsgs := make([]*log.LogMessage, 0)
				for i := 0; i < 1000; i++ {
					msg, err := logReader.ReadNext(context.Background())
					assert.NoError(t, err)
					assert.NotNil(t, msg)
					readMsgs = append(readMsgs, msg)
				}
				assert.Equal(t, 1000, len(readMsgs))
				for idx, msg := range readMsgs {
					// The sequence of data read and written is the same
					assert.Equal(t, successIds[idx].SegmentId, msg.Id.SegmentId)
					if idx == 0 {
						continue
					}
					// The order of reading data is incremented one by one
					if msg.Id.SegmentId == readMsgs[idx-1].Id.SegmentId {
						assert.True(t, msg.Id.EntryId == readMsgs[idx-1].Id.EntryId+1)
					} else {
						assert.True(t, msg.Id.SegmentId > readMsgs[idx-1].Id.SegmentId)
					}
				}

				closeReaderErr := logReader.Close(context.Background())
				assert.NoError(t, closeReaderErr)
			}

			// Test read from point
			{
				start := &log.LogMessageId{
					SegmentId: 0,
					EntryId:   50,
				}
				logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), start, "client-specific-position-reader")
				assert.NoError(t, openReaderErr)
				readMsgs := make([]*log.LogMessage, 0)
				for i := 0; i < 950; i++ {
					msg, err := logReader.ReadNext(context.Background())
					assert.NoError(t, err)
					assert.NotNil(t, msg)
					readMsgs = append(readMsgs, msg)
				}
				assert.Equal(t, 950, len(readMsgs))
				for idx, msg := range readMsgs {
					// The sequence of data read and written is the same
					assert.Equal(t, successIds[idx+50].SegmentId, msg.Id.SegmentId)
					if idx == 0 {
						continue
					}
					// The order of reading data is incremented one by one
					if msg.Id.SegmentId == readMsgs[idx-1].Id.SegmentId {
						assert.True(t, msg.Id.EntryId == readMsgs[idx-1].Id.EntryId+1)
					} else {
						assert.True(t, msg.Id.SegmentId > readMsgs[idx-1].Id.SegmentId)
					}
				}
				closeReaderErr := logReader.Close(context.Background())
				assert.NoError(t, closeReaderErr)
			}

			// Test read from latest
			{
				latest := log.LatestLogMessageID()
				logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &latest, "client-latest-reader")
				assert.NoError(t, openReaderErr)
				readOne := false
				go func() {
					_, err := logReader.ReadNext(context.Background())
					if err == nil {
						readOne = true
					}
				}()
				time.Sleep(2 * time.Second)
				assert.False(t, readOne)
				closeReaderErr := logReader.Close(context.Background())
				assert.NoError(t, closeReaderErr)
			}

			fmt.Println("Test Read finished")
		})
	}
}

func TestReadWriteLoop(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestReadWriteLoop")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
		needCluster bool // Whether to start cluster for service mode
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
			needCluster: false,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
			needCluster: false,
		},
		{
			name:        "ServiceStorage",
			storageType: "service",
			rootPath:    rootPath + "_service",
			needCluster: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Setup cluster if needed
			var client woodpecker.Client
			if tc.needCluster {
				// Start cluster for service mode
				const nodeCount = 3
				cluster, cfg, seeds := utils.StartMiniClusterWithCfg(t, nodeCount, tc.rootPath, cfg)
				seedList := strings.Join(seeds, ",")
				cfg.Woodpecker.Client.ServiceSeedNodes = seedList
				defer func() {
					cluster.StopMultiNodeCluster(t)
				}()

				// Setup etcd client for service mode
				etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
				assert.NoError(t, err)
				defer etcdCli.Close()

				// Create service mode client
				client, err = woodpecker.NewClient(ctx, cfg, etcdCli, true)
				assert.NoError(t, err)
				defer func() {
					if client != nil {
						_ = client.Close(ctx)
					}
				}()
			} else {
				// Use embed client for local and object storage
				client, err = woodpecker.NewEmbedClientFromConfig(ctx, cfg)
				assert.NoError(t, err)
				defer func() {
					// stop embed LogStore singleton only for non-service mode
					stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
					assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
				}()
			}
			defer client.Close(context.TODO())

			// CreateLog if not exists
			logName := "TestReadWriteLoop_" + t.Name() + "_" + time.Now().Format("20060102150405")
			client.CreateLog(context.Background(), logName)

			// write/read loop test
			for i := 0; i < 3; i++ {
				// test write
				writtenIds := testWrite(t, client, logName, 1000)
				// test read
				testRead(t, client, logName, writtenIds, 1000)
			}

			fmt.Println("Test Read finished")
		})
	}
}

func testWrite(t *testing.T, client woodpecker.Client, logName string, count int) []*log.LogMessageId {
	// OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), logName)
	assert.NoError(t, openErr)

	// Test write
	// OpenWriter
	logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, openWriterErr)
	resultChan := make([]<-chan *log.WriteResult, count)
	for i := 0; i < count; i++ {
		writeResultChan := logWriter.WriteAsync(context.Background(),
			&log.WriteMessage{
				Payload: []byte(fmt.Sprintf("hello world %d", i)),
				Properties: map[string]string{
					"key": fmt.Sprintf("value%d", i),
				},
			},
		)
		resultChan[i] = writeResultChan
	}
	failedIds := make([]*log.WriteResult, 0)
	successIds := make([]*log.LogMessageId, 0)
	for i := 0; i < count; i++ {
		writeResult := <-resultChan[i]
		if writeResult.Err != nil {
			failedIds = append(failedIds, writeResult)
		} else {
			successIds = append(successIds, writeResult.LogMessageId)
		}
	}

	assert.Equal(t, 0, len(failedIds))
	assert.Equal(t, count, len(successIds))
	for idx, msgId := range successIds {
		if idx == 0 {
			continue
		}
		if msgId.SegmentId == successIds[idx-1].SegmentId {
			assert.True(t, msgId.EntryId == successIds[idx-1].EntryId+1)
		} else {
			assert.True(t, msgId.SegmentId > successIds[idx-1].SegmentId)
		}
	}
	closeErr := logWriter.Close(context.Background())
	assert.NoError(t, closeErr)
	t.Logf("Test Write finished %v,%v \n", successIds[0], successIds[count-1])
	return successIds
}

func testRead(t *testing.T, client woodpecker.Client, logName string, writtenIds []*log.LogMessageId, count int) {
	// OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), logName)
	assert.NoError(t, openErr)
	// Test read all from earliest
	from := writtenIds[0]
	logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), from, "client-from-position-reader")
	assert.NoError(t, openReaderErr)
	readMsgs := make([]*log.LogMessage, 0)
	for i := 0; i < count; i++ {
		msg, err := logReader.ReadNext(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, msg)
		readMsgs = append(readMsgs, msg)
	}
	assert.Equal(t, count, len(readMsgs))
	for idx, msg := range readMsgs {
		// The sequence of data read and written is the same
		assert.Equal(t, writtenIds[idx].SegmentId, msg.Id.SegmentId)
		if idx == 0 {
			continue
		}
		// The order of reading data is incremented one by one
		if msg.Id.SegmentId == readMsgs[idx-1].Id.SegmentId {
			assert.True(t, msg.Id.EntryId == readMsgs[idx-1].Id.EntryId+1)
		} else {
			assert.True(t, msg.Id.SegmentId > readMsgs[idx-1].Id.SegmentId)
		}
	}

	closeReaderErr := logReader.Close(context.Background())
	assert.NoError(t, closeReaderErr)
	t.Logf("Test Read finished %v,%v \n", readMsgs[0].Id, readMsgs[count-1].Id)
}

func TestMultiAppendSyncLoop(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestMultiAppendSyncLoop")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
		needCluster bool // Whether to start cluster for service mode
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
			needCluster: false,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
			needCluster: false,
		},
		{
			name:        "ServiceStorage",
			storageType: "service",
			rootPath:    rootPath + "_service",
			needCluster: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Setup cluster if needed
			var client woodpecker.Client
			if tc.needCluster {
				// Start cluster for service mode
				const nodeCount = 3
				cluster, cfg, seeds := utils.StartMiniClusterWithCfg(t, nodeCount, tc.rootPath, cfg)
				seedList := strings.Join(seeds, ",")
				cfg.Woodpecker.Client.ServiceSeedNodes = seedList
				defer func() {
					cluster.StopMultiNodeCluster(t)
				}()

				// Setup etcd client for service mode
				etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
				assert.NoError(t, err)
				defer etcdCli.Close()

				// Create service mode client
				client, err = woodpecker.NewClient(ctx, cfg, etcdCli, true)
				assert.NoError(t, err)
				defer func() {
					if client != nil {
						_ = client.Close(ctx)
					}
				}()
			} else {
				// Use embed client for local and object storage
				client, err = woodpecker.NewEmbedClientFromConfig(ctx, cfg)
				assert.NoError(t, err)
				defer func() {
					// stop embed LogStore singleton only for non-service mode
					stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
					assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
				}()
			}
			defer client.Close(context.TODO())

			// CreateLog if not exists
			logName := "TestMultiAppendSyncLoop_" + t.Name() + "_" + time.Now().Format("20060102150405")
			client.CreateLog(context.Background(), logName)

			// sync write loop test
			count := 1_00
			for i := 0; i < 3; i++ {
				// test write
				writtenIds := testMultiAppendSync(t, client, logName, count)
				assert.Equal(t, count, len(writtenIds))
			}

			fmt.Println("Test write finished")
		})
	}
}

func testMultiAppendSync(t *testing.T, client woodpecker.Client, logName string, count int) []*log.LogMessageId {
	// OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), logName)
	assert.NoError(t, openErr)

	// Test write
	// OpenWriter
	logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
	assert.NoError(t, openWriterErr)

	concurrency := make(chan int, 5)
	wg := sync.WaitGroup{}
	resultList := make([]*log.WriteResult, count)
	for i := 0; i < count; i++ {
		concurrency <- i
		wg.Add(1)
		go func(no int) {
			writeResult := logWriter.Write(context.Background(),
				&log.WriteMessage{
					Payload:    []byte(fmt.Sprintf("hello world %d", i)),
					Properties: map[string]string{"key": fmt.Sprintf("value%d", i)},
				},
			)
			//resultList = append(resultList, writeResult)
			resultList[no] = writeResult
			if writeResult.Err != nil {
				t.Logf("write failed %v \n", writeResult.Err)
			} else {
				t.Logf("write %d %d success \n", writeResult.LogMessageId.SegmentId, writeResult.LogMessageId.EntryId)
			}
			<-concurrency
			wg.Done()
		}(i)
	}
	wg.Wait()
	failedIds := make([]*log.WriteResult, 0)
	successIds := make([]*log.LogMessageId, 0)
	for _, wr := range resultList {
		if wr.Err != nil {
			failedIds = append(failedIds, wr)
		} else {
			successIds = append(successIds, wr.LogMessageId)
		}
	}

	assert.Equal(t, 0, len(failedIds))
	assert.Equal(t, count, len(successIds))
	if count != len(successIds) {
		t.Logf("unexpected success num")
	}
	t.Logf("success: ")
	for _, msgId := range successIds {
		t.Logf("%d:%d ,", msgId.SegmentId, msgId.EntryId)
	}
	t.Logf("\n")
	sort.Slice(successIds, func(i, j int) bool {
		if successIds[i].SegmentId < successIds[j].SegmentId {
			return true
		} else if successIds[i].SegmentId > successIds[j].SegmentId {
			return false
		} else {
			return successIds[i].EntryId < successIds[j].EntryId
		}
	})
	t.Logf("success order: ")
	for _, msgId := range successIds {
		t.Logf("%d:%d ,", msgId.SegmentId, msgId.EntryId)
	}
	t.Logf("\n")
	t.Logf("successIds:%v \n", successIds)
	for idx, msgId := range successIds {
		if idx == 0 {
			continue
		}
		if msgId.SegmentId == successIds[idx-1].SegmentId {
			if msgId.EntryId != successIds[idx-1].EntryId+1 {
				t.Logf("idx:%d,msgId:%v,successIds:%v \n", idx, msgId, successIds)
			}
			assert.Equal(t, msgId.EntryId, successIds[idx-1].EntryId+1, fmt.Sprintf("expected:%d actual:%d ", msgId.EntryId, successIds[idx-1].EntryId+1))
		} else {
			assert.True(t, msgId.SegmentId > successIds[idx-1].SegmentId, fmt.Sprintf("expected:%d actual:%d ", msgId.SegmentId, successIds[idx-1].SegmentId))
		}
	}
	closeErr := logWriter.Close(context.Background())
	assert.NoError(t, closeErr)
	return successIds
}

func TestTailReadBlockingBehavior(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestTailReadBlockingBehavior")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
		needCluster bool // Whether to start cluster for service mode
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
			needCluster: false,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
			needCluster: false,
		},
		{
			name:        "ServiceStorage",
			storageType: "service",
			rootPath:    rootPath + "_service",
			needCluster: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Setup cluster if needed
			var client woodpecker.Client
			if tc.needCluster {
				// Start cluster for service mode
				const nodeCount = 3
				cluster, cfg, seeds := utils.StartMiniClusterWithCfg(t, nodeCount, tc.rootPath, cfg)
				seedList := strings.Join(seeds, ",")
				cfg.Woodpecker.Client.ServiceSeedNodes = seedList
				defer func() {
					cluster.StopMultiNodeCluster(t)
				}()

				// Setup etcd client for service mode
				etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
				assert.NoError(t, err)
				defer etcdCli.Close()

				// Create service mode client
				client, err = woodpecker.NewClient(ctx, cfg, etcdCli, true)
				assert.NoError(t, err)
				defer func() {
					if client != nil {
						_ = client.Close(ctx)
					}
				}()
			} else {
				// Use embed client for local and object storage
				client, err = woodpecker.NewEmbedClientFromConfig(ctx, cfg)
				assert.NoError(t, err)
				defer func() {
					// stop embed LogStore singleton only for non-service mode
					stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
					assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
				}()
			}
			defer client.Close(context.TODO())

			// CreateLog if not exists
			logName := "TestTailReadBlockingBehavior_" + t.Name() + "_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open the log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			// First open a reader starting from the "latest" position
			latest := log.LatestLogMessageID()
			logReader, err := logHandle.OpenLogReader(context.Background(), &latest, "client-latest-reader-sync")
			assert.NoError(t, err)

			totalMessages := 20
			readMessages := make([]*log.LogMessage, 0, totalMessages)
			readCh := make(chan *log.LogMessage, totalMessages)
			readErrorCh := make(chan error, 1)
			readCompleteCh := make(chan struct{})

			// Start a goroutine to continuously read messages
			go func() {
				for {
					msg, err := logReader.ReadNext(context.Background())
					if err != nil {
						readErrorCh <- err
						return
					}

					readCh <- msg
					readMessages = append(readMessages, msg)
					t.Logf("Read message seg:%d entry:%d payload:%v props:%v\n", msg.Id.SegmentId, msg.Id.EntryId, msg.Payload, msg.Properties)

					// If we've read enough messages, signal completion
					if len(readMessages) == totalMessages {
						close(readCompleteCh)
						break
					}
				}
			}()

			// Now open a writer and write messages at intervals
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			// Write messages in batches with delays
			writeMessages := make([]*log.LogMessageId, 0, totalMessages)
			batchSize := 5
			numBatches := totalMessages / batchSize

			for batch := 0; batch < numBatches; batch++ {
				// Write a batch of messages
				for i := 0; i < batchSize; i++ {
					idx := batch*batchSize + i
					result := logWriter.Write(context.Background(), &log.WriteMessage{
						Payload: []byte(fmt.Sprintf("message %d", idx)),
						Properties: map[string]string{
							"k1": fmt.Sprintf("%d", batch),
							"k2": fmt.Sprintf("%d", i),
						},
					})

					assert.NoError(t, result.Err)
					writeMessages = append(writeMessages, result.LogMessageId)
					t.Logf("Written message %d: %v\n", idx, result.LogMessageId)
				}

				// Sleep between batches to simulate time-delayed writes
				time.Sleep(1 * time.Second)
			}

			// Wait for all reads to complete
			select {
			case <-readCompleteCh:
				fmt.Println("All messages read successfully")
			case err := <-readErrorCh:
				t.Fatalf("Error reading messages: %v", err)
			case <-time.After(20 * time.Second):
				t.Fatalf("Timeout waiting for messages to be read")
			}

			// Verify that the timeout goroutine completes, confirming blocking behavior
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			newMessage, newErr := logReader.ReadNext(ctx)
			cancel()
			assert.Error(t, newErr)
			assert.True(t, errors.IsAny(newErr, context.Canceled, context.DeadlineExceeded))
			assert.Nil(t, newMessage)

			// Verify the messages we read match what we wrote
			assert.Equal(t, totalMessages, len(readMessages))
			for i, msg := range readMessages {
				assert.Equal(t, writeMessages[i].SegmentId, msg.Id.SegmentId)
				assert.Equal(t, writeMessages[i].EntryId, msg.Id.EntryId)
				assert.Equal(t, fmt.Sprintf("message %d", i), string(msg.Payload))
			}

			// Clean up
			err = logReader.Close(context.Background())
			assert.NoError(t, err)

			err = logWriter.Close(context.Background())
			assert.NoError(t, err)

			fmt.Println("Test completed successfully")
		})
	}
}

func TestTailReadBlockingAfterWriting(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestTailReadBlockingBehavior")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
		needCluster bool // Whether to start cluster for service mode
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
			needCluster: false,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
			needCluster: false,
		},
		{
			name:        "ServiceStorage",
			storageType: "service",
			rootPath:    rootPath + "_service",
			needCluster: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Setup cluster if needed
			var client woodpecker.Client
			if tc.needCluster {
				// Start cluster for service mode
				const nodeCount = 3
				cluster, cfg, seeds := utils.StartMiniClusterWithCfg(t, nodeCount, tc.rootPath, cfg)
				seedList := strings.Join(seeds, ",")
				cfg.Woodpecker.Client.ServiceSeedNodes = seedList
				defer func() {
					cluster.StopMultiNodeCluster(t)
				}()

				// Setup etcd client for service mode
				etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
				assert.NoError(t, err)
				defer etcdCli.Close()

				// Create service mode client
				client, err = woodpecker.NewClient(ctx, cfg, etcdCli, true)
				assert.NoError(t, err)
				defer func() {
					if client != nil {
						_ = client.Close(ctx)
					}
				}()
			} else {
				// Use embed client for local and object storage
				client, err = woodpecker.NewEmbedClientFromConfig(ctx, cfg)
				assert.NoError(t, err)
				defer func() {
					// stop embed LogStore singleton only for non-service mode
					stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
					assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
				}()
			}
			defer client.Close(context.TODO())

			// CreateLog if not exists
			logName := "TestTailReadBlockingAfterWriting_" + t.Name() + "_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open the log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			totalMessages := 20
			// Now open a writer and write messages at intervals
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			// Write messages in batches with delays
			writeMessages := make([]*log.LogMessageId, 0, totalMessages)
			batchSize := 5
			numBatches := totalMessages / batchSize
			for batch := 0; batch < numBatches; batch++ {
				// Write a batch of messages
				for i := 0; i < batchSize; i++ {
					idx := batch*batchSize + i
					result := logWriter.Write(context.Background(), &log.WriteMessage{
						Payload: []byte(fmt.Sprintf("message %d", idx)),
						Properties: map[string]string{
							"k1": fmt.Sprintf("%d", batch),
							"k2": fmt.Sprintf("%d", i),
						},
					})

					assert.NoError(t, result.Err)
					writeMessages = append(writeMessages, result.LogMessageId)
					t.Logf("Written message %d: %v\n", idx, result.LogMessageId)
				}
			}

			// Verify that tail read timeout
			latest := log.LatestLogMessageID()
			logReader, err := logHandle.OpenLogReader(context.Background(), &latest, "client-latest-reader-async")
			assert.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			newMessage, newErr := logReader.ReadNext(ctx)
			cancel()
			assert.Error(t, newErr)
			assert.True(t, strings.Contains(newErr.Error(), context.DeadlineExceeded.Error()))
			assert.Nil(t, newMessage)

			// Clean up
			err = logReader.Close(context.Background())
			assert.NoError(t, err)
			err = logWriter.Close(context.Background())
			assert.NoError(t, err)
			fmt.Println("Test completed successfully")

		})
	}
}

func TestConcurrentWriteWithClose(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestConcurrentWriteWithClose")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
		needCluster bool // Whether to start cluster for service mode
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
			needCluster: false,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
			needCluster: false,
		},
		{
			name:        "ServiceStorage",
			storageType: "service",
			rootPath:    rootPath + "_service",
			needCluster: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			cfg.Log.Level = "debug"

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Setting a larger value to turn off auto sync during the test period
			cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval = 60 * 1000 // 60s

			// Setup cluster if needed
			var client woodpecker.Client
			if tc.needCluster {
				// Start cluster for service mode
				const nodeCount = 3
				cluster, cfg, seeds := utils.StartMiniClusterWithCfg(t, nodeCount, tc.rootPath, cfg)
				seedList := strings.Join(seeds, ",")
				cfg.Woodpecker.Client.ServiceSeedNodes = seedList
				defer func() {
					cluster.StopMultiNodeCluster(t)
				}()

				// Setup etcd client for service mode
				etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
				assert.NoError(t, err)
				defer etcdCli.Close()

				// Create service mode client
				client, err = woodpecker.NewClient(ctx, cfg, etcdCli, true)
				assert.NoError(t, err)
				defer func() {
					if client != nil {
						_ = client.Close(ctx)
					}
				}()
			} else {
				// Use embed client for local and object storage
				client, err = woodpecker.NewEmbedClientFromConfig(ctx, cfg)
				assert.NoError(t, err)
				defer func() {
					// stop embed LogStore singleton only for non-service mode
					stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
					assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")
				}()
			}
			defer client.Close(context.TODO())

			// CreateLog if not exists
			logName := "TestConcurrentWriteWithClose_" + t.Name() + "_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open the log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open a writer
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			// Set up for concurrent writes
			numWriters := 10
			var wg sync.WaitGroup
			var writeMutex sync.Mutex
			writeResults := make([]*log.WriteResult, numWriters)

			// Use a WaitGroup to track when all writes have been attempted
			wg.Add(numWriters)

			// Start the 10 concurrent write operations
			for i := 0; i < numWriters; i++ {
				go func(idx int) {
					defer wg.Done()
					// Slightly randomize when writes happen
					time.Sleep(time.Duration(idx*30) * time.Millisecond)

					// Synchronous write
					result := logWriter.Write(context.Background(), &log.WriteMessage{
						Payload: []byte(fmt.Sprintf("concurrent message %d", idx)),
						Properties: map[string]string{
							"index": fmt.Sprintf("%d", idx),
						},
					})

					// Store the result
					writeMutex.Lock()
					writeResults[idx] = result
					writeMutex.Unlock()

					if result.Err != nil {
						t.Logf("Write %d failed: %v\n", idx, result.Err)
					} else {
						t.Logf("Write %d succeeded: seg:%d entry:%d\n",
							idx, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
					}
				}(i)
			}

			// Wait a bit to let some writes start, then close the writer
			time.Sleep(100 * time.Millisecond)
			fmt.Println("Closing writer while writes are in progress...")
			closeErr := logWriter.Close(context.Background())
			assert.NoError(t, closeErr)
			fmt.Println("Writer closed")

			// Wait for all write operations to complete
			wg.Wait()
			fmt.Println("All writes have completed")

			// Count successful and failed writes
			successfulWrites := 0
			failedWrites := 0
			successfulIds := make([]*log.LogMessageId, 0)

			for i, result := range writeResults {
				if result.Err != nil {
					failedWrites++
					t.Logf("Write %d failed with error: %v\n", i, result.Err)
				} else {
					successfulWrites++
					successfulIds = append(successfulIds, result.LogMessageId)
					t.Logf("Write %d succeeded with ID: seg:%d entry:%d\n",
						i, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
				}
			}

			t.Logf("Summary: %d writes succeeded, %d writes failed\n",
				successfulWrites, failedWrites)

			// Now read all messages and verify
			if successfulWrites > 0 {
				// Open a reader from the beginning
				earliest := log.EarliestLogMessageID()
				logReader, err := logHandle.OpenLogReader(context.Background(), &earliest, "concurrent-write-close-reader")
				assert.NoError(t, err)

				// Read all the messages that were actually written
				readMessages := make([]*log.LogMessage, 0, successfulWrites)
				for i := 0; i < successfulWrites; i++ {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					msg, err := logReader.ReadNext(ctx)
					cancel()
					assert.NoError(t, err, "Failed to read message %d", i)
					if err != nil {
						break
					}
					readMessages = append(readMessages, msg)
					t.Logf("Read message: seg:%d entry:%d payload:%s\n",
						msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
				}

				// Try to read one more message - should time out or error
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err = logReader.ReadNext(ctx)
				assert.Error(t, err, "Expected error or timeout when trying to read more than written")
				cancel()

				// Verify that all successfully written messages were read back
				if len(readMessages) != successfulWrites {
					t.Logf("Warning: Number of read messages (%d) does not match reported successful writes (%d)",
						len(readMessages), successfulWrites)
					// Adjust successfulIds to match actually read messages
					if len(readMessages) < len(successfulIds) {
						successfulIds = successfulIds[:len(readMessages)]
					}
				}

				// Compare the message IDs between what was reported as written and what was read
				// First sort both lists by segmentId and entryId
				sort.Slice(successfulIds, func(i, j int) bool {
					if successfulIds[i].SegmentId == successfulIds[j].SegmentId {
						return successfulIds[i].EntryId < successfulIds[j].EntryId
					}
					return successfulIds[i].SegmentId < successfulIds[j].SegmentId
				})

				sort.Slice(readMessages, func(i, j int) bool {
					if readMessages[i].Id.SegmentId == readMessages[j].Id.SegmentId {
						return readMessages[i].Id.EntryId < readMessages[j].Id.EntryId
					}
					return readMessages[i].Id.SegmentId < readMessages[j].Id.SegmentId
				})

				// Now compare message IDs
				compareCount := minInt(len(readMessages), len(successfulIds))
				for i := 0; i < compareCount; i++ {
					readMsg := readMessages[i]
					assert.Equal(t, successfulIds[i].SegmentId, readMsg.Id.SegmentId,
						"Segment ID mismatch at index %d", i)
					assert.Equal(t, successfulIds[i].EntryId, readMsg.Id.EntryId,
						"Entry ID mismatch at index %d", i)
				}

				// Make sure we have the right number of messages
				if compareCount > 0 {
					assert.Equal(t, len(successfulIds), len(readMessages),
						"Number of read messages should match number of successful write IDs")
				}

				// Close the reader
				err = logReader.Close(context.Background())
				assert.NoError(t, err)
			}

			fmt.Println("Test completed successfully")
		})
	}
}

func TestConcurrentWriteWithClientClose(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestConcurrentWriteWithClientClose")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create initial configuration
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			cfg.Log.Level = "debug"

			// Setting a larger value to turn off auto sync during the test period
			cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval = 30 * 1000                // 30s
			cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage = 30 * 1000 // 30s

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Create first client
			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			// Create a unique log name for this test run
			logName := "TestConcurrentWriteWithClientClose_" + t.Name() + "_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open the log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open a writer
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			// Set up for concurrent writes
			numWriters := 10
			var wg sync.WaitGroup
			var writeMutex sync.Mutex
			writeResults := make([]*log.WriteResult, numWriters)

			// Use a WaitGroup to track when all writes have been attempted
			wg.Add(numWriters)

			// Start the 10 concurrent write operations
			for i := 0; i < numWriters; i++ {
				entryId := i
				go func(idx int) {
					defer wg.Done()
					// Slightly randomize when writes happen
					time.Sleep(time.Duration(idx*30) * time.Millisecond)

					// Synchronous write
					t.Logf("Write %d entry start", idx)
					result := logWriter.Write(context.Background(), &log.WriteMessage{
						Payload: []byte(fmt.Sprintf("client close test message %d", idx)),
						Properties: map[string]string{
							"index": fmt.Sprintf("%d", idx),
						},
					})

					// Store the result
					writeMutex.Lock()
					writeResults[idx] = result
					writeMutex.Unlock()

					if result.Err != nil {
						t.Logf("Write %d failed: %v\n", idx, result.Err)
					} else {
						t.Logf("Write %d succeeded: seg:%d entry:%d\n",
							idx, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
					}
				}(entryId)
			}

			// Wait a bit to let some writes start, then close the entire client
			time.Sleep(100 * time.Millisecond)
			fmt.Println("Closing client while writes are in progress...")
			closeErr := client.Close(context.TODO())
			assert.NoError(t, closeErr)
			fmt.Println("Client closed")

			// Wait for all write operations to complete
			wg.Wait()
			fmt.Println("All writes have completed")

			// Count successful and failed writes
			successfulWrites := 0
			failedWrites := 0
			successfulIds := make([]*log.LogMessageId, 0)

			for i, result := range writeResults {
				if result.Err != nil {
					failedWrites++
					t.Logf("Write %d failed with error: %v\n", i, result.Err)
				} else {
					successfulWrites++
					successfulIds = append(successfulIds, result.LogMessageId)
					t.Logf("Write %d succeeded with ID: seg:%d entry:%d\n",
						i, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
				}
			}

			t.Logf("Summary: %d writes succeeded, %d writes failed\n",
				successfulWrites, failedWrites)

			// Now create a new client to read the data
			fmt.Println("Creating a new client to read data...")
			newClient, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)
			defer func() {
				newClient.Close(context.TODO())
			}()

			// Now read all messages and verify
			if successfulWrites > 0 {
				// Open the log with the new client
				newLogHandle, err := newClient.OpenLog(context.Background(), logName)
				assert.NoError(t, err)

				// Open a reader from the beginning
				earliest := log.EarliestLogMessageID()
				logReader, err := newLogHandle.OpenLogReader(context.Background(), &earliest, "client-close-reader")
				assert.NoError(t, err)
				defer logReader.Close(context.Background())

				// Read all the messages that were actually written
				readMessages := make([]*log.LogMessage, 0, successfulWrites)
				for i := 0; i < successfulWrites; i++ {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					msg, err := logReader.ReadNext(ctx)
					cancel()
					assert.NoError(t, err, "Failed to read message %d", i)
					if err != nil {
						break
					}
					readMessages = append(readMessages, msg)
					t.Logf("Read message: seg:%d entry:%d payload:%s\n",
						msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
				}

				// Try to read one more message - should time out or error
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				msgMore, err := logReader.ReadNext(ctx)
				assert.Error(t, err, "Expected error or timeout when trying to read more than written")
				assert.Nil(t, msgMore, fmt.Sprintf("%v", msgMore))
				cancel()

				// Verify that all successfully written messages were read back
				if len(readMessages) != successfulWrites {
					t.Logf("Warning: Number of read messages (%d) does not match reported successful writes (%d)",
						len(readMessages), successfulWrites)
					// Adjust successfulIds to match actually read messages
					if len(readMessages) < len(successfulIds) {
						successfulIds = successfulIds[:len(readMessages)]
					}
				}

				// Compare the message IDs between what was reported as written and what was read
				// First sort both lists by segmentId and entryId
				sort.Slice(successfulIds, func(i, j int) bool {
					if successfulIds[i].SegmentId == successfulIds[j].SegmentId {
						return successfulIds[i].EntryId < successfulIds[j].EntryId
					}
					return successfulIds[i].SegmentId < successfulIds[j].SegmentId
				})

				sort.Slice(readMessages, func(i, j int) bool {
					if readMessages[i].Id.SegmentId == readMessages[j].Id.SegmentId {
						return readMessages[i].Id.EntryId < readMessages[j].Id.EntryId
					}
					return readMessages[i].Id.SegmentId < readMessages[j].Id.SegmentId
				})

				// Now compare message IDs
				compareCount := minInt(len(readMessages), len(successfulIds))
				for i := 0; i < compareCount; i++ {
					readMsg := readMessages[i]
					assert.Equal(t, successfulIds[i].SegmentId, readMsg.Id.SegmentId,
						"Segment ID mismatch at index %d", i)
					assert.Equal(t, successfulIds[i].EntryId, readMsg.Id.EntryId,
						"Entry ID mismatch at index %d", i)
				}

				// Make sure we have the right number of messages
				if compareCount > 0 {
					assert.Equal(t, len(successfulIds), len(readMessages),
						"Number of read messages should match number of successful write IDs")
				}
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			fmt.Println("Test completed successfully")
		})
	}
}

func TestConcurrentWriteWithAllCloseAndEmbeddedLogStoreShutdown(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestConcurrentWriteWithAllCloseAndEmbeddedLogStoreShutdown")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "", // Using default storage type minio-compatible
			rootPath:    "", // No need to specify path for default storage
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create initial configuration
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			cfg.Log.Level = "debug"

			// Setting a larger value to turn off auto sync during the test period
			cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval = 30 * 1000                // 30s
			cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxIntervalForLocalStorage = 30 * 1000 // 30s

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			// Create first client
			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)

			// Create a unique log name for this test run
			logName := "TestConcurrentWriteWithAllCloseAndEmbeddedLogStoreShutdown_" + t.Name() + "_" + time.Now().Format("20060102150405")
			err = client.CreateLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open the log
			logHandle, err := client.OpenLog(context.Background(), logName)
			assert.NoError(t, err)

			// Open a writer
			logWriter, err := logHandle.OpenLogWriter(context.Background())
			assert.NoError(t, err)

			// Set up for concurrent writes
			numWriters := 10
			var wg sync.WaitGroup
			var writeMutex sync.Mutex
			writeResults := make([]*log.WriteResult, numWriters)

			// Use a WaitGroup to track when all writes have been attempted
			wg.Add(numWriters)

			// Start the 10 concurrent write operations
			for i := 0; i < numWriters; i++ {
				go func(idx int) {
					defer wg.Done()
					// Slightly randomize when writes happen
					time.Sleep(time.Duration(idx*30) * time.Millisecond)

					// Synchronous write
					result := logWriter.Write(context.Background(), &log.WriteMessage{
						Payload: []byte(fmt.Sprintf("client close test message %d", idx)),
						Properties: map[string]string{
							"index": fmt.Sprintf("%d", idx),
						},
					})

					// Store the result
					writeMutex.Lock()
					writeResults[idx] = result
					writeMutex.Unlock()

					if result.Err != nil {
						t.Logf("Write %d failed: %v\n", idx, result.Err)
					} else {
						t.Logf("Write %d succeeded: seg:%d entry:%d\n",
							idx, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
					}
				}(i)
			}

			// Wait a bit to let some writes start, then close the entire client
			time.Sleep(100 * time.Millisecond)
			fmt.Println("Closing client while writes are in progress...")
			closeErr := client.Close(context.TODO())
			assert.NoError(t, closeErr)
			fmt.Println("Client closed")

			fmt.Println("Shutdown embedded LogStore...")
			stopErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopErr)
			fmt.Println("Embedded LogStore stop")

			// Wait for all write operations to complete
			wg.Wait()
			fmt.Println("All writes have completed")

			// Count successful and failed writes
			successfulWrites := 0
			failedWrites := 0
			successfulIds := make([]*log.LogMessageId, 0)

			for i, result := range writeResults {
				if result.Err != nil {
					failedWrites++
					t.Logf("Write %d failed with error: %v\n", i, result.Err)
				} else {
					successfulWrites++
					successfulIds = append(successfulIds, result.LogMessageId)
					t.Logf("Write %d succeeded with ID: seg:%d entry:%d\n",
						i, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
				}
			}

			t.Logf("Summary: %d writes succeeded, %d writes failed\n",
				successfulWrites, failedWrites)

			// Now create a new client to read the data
			fmt.Println("Creating a new client to read data...")
			newClient, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			assert.NoError(t, err)
			defer func() {
				newClient.Close(context.TODO())
			}()

			// Now read all messages and verify
			if successfulWrites > 0 {
				// Open the log with the new client
				newLogHandle, err := newClient.OpenLog(context.Background(), logName)
				assert.NoError(t, err)

				// Open a reader from the beginning
				earliest := log.EarliestLogMessageID()
				logReader, err := newLogHandle.OpenLogReader(context.Background(), &earliest, "client-close-reader")
				assert.NoError(t, err)
				defer logReader.Close(context.Background())

				// Read all the messages that were actually written
				readMessages := make([]*log.LogMessage, 0, successfulWrites)
				for i := 0; i < successfulWrites; i++ {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					msg, err := logReader.ReadNext(ctx)
					cancel()
					assert.NoError(t, err, "Failed to read message %d", i)
					if err != nil {
						break
					}
					readMessages = append(readMessages, msg)
					t.Logf("Read message: seg:%d entry:%d payload:%s\n",
						msg.Id.SegmentId, msg.Id.EntryId, string(msg.Payload))
				}

				// Try to read one more message - should time out or error
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_, err = logReader.ReadNext(ctx)
				assert.Error(t, err, "Expected error or timeout when trying to read more than written")
				cancel()

				// Verify that all successfully written messages were read back
				if len(readMessages) != successfulWrites {
					t.Logf("Warning: Number of read messages (%d) does not match reported successful writes (%d)",
						len(readMessages), successfulWrites)
					// Adjust successfulIds to match actually read messages
					if len(readMessages) < len(successfulIds) {
						successfulIds = successfulIds[:len(readMessages)]
					}
				}

				// Compare the message IDs between what was reported as written and what was read
				// First sort both lists by segmentId and entryId
				sort.Slice(successfulIds, func(i, j int) bool {
					if successfulIds[i].SegmentId == successfulIds[j].SegmentId {
						return successfulIds[i].EntryId < successfulIds[j].EntryId
					}
					return successfulIds[i].SegmentId < successfulIds[j].SegmentId
				})

				sort.Slice(readMessages, func(i, j int) bool {
					if readMessages[i].Id.SegmentId == readMessages[j].Id.SegmentId {
						return readMessages[i].Id.EntryId < readMessages[j].Id.EntryId
					}
					return readMessages[i].Id.SegmentId < readMessages[j].Id.SegmentId
				})

				// Now compare message IDs
				compareCount := minInt(len(readMessages), len(successfulIds))
				for i := 0; i < compareCount; i++ {
					readMsg := readMessages[i]
					assert.Equal(t, successfulIds[i].SegmentId, readMsg.Id.SegmentId,
						"Segment ID mismatch at index %d", i)
					assert.Equal(t, successfulIds[i].EntryId, readMsg.Id.EntryId,
						"Entry ID mismatch at index %d", i)
				}

				// Make sure we have the right number of messages
				if compareCount > 0 {
					assert.Equal(t, len(successfulIds), len(readMessages),
						"Number of read messages should match number of successful write IDs")
				}
			}

			// stop embed LogStore singleton
			stopEmbedLogStoreErr := woodpecker.StopEmbedLogStore()
			assert.NoError(t, stopEmbedLogStoreErr, "close embed LogStore instance error")

			fmt.Println("Test completed successfully")
		})
	}
}

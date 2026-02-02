package chaos

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// Test basic functionality of docker compose deployed cluster, requires pre-deployed docker compose cluster

func TestBasicReadWriteInServiceMode(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestBasicReadWriteInServiceMode")
	testCases := []struct {
		name         string
		storageType  string
		rootPath     string
		serviceSeeds []string
	}{
		{
			name:         "ServiceStorage",
			storageType:  "service",
			rootPath:     rootPath + "_service",
			serviceSeeds: []string{"localhost:18080", "localhost:18081", "localhost:18082"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../../config/woodpecker.yaml")
			require.NoError(t, err)
			cfg.Log.Level = "debug"

			cfg.Woodpecker.Storage.Type = tc.storageType
			cfg.Woodpecker.Storage.RootPath = tc.rootPath

			// Start cluster for service mode
			cfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = tc.serviceSeeds

			// Setup etcd client for service mode
			etcdCli, err := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
			require.NoError(t, err)
			defer etcdCli.Close()

			// Create service mode client
			client, err := woodpecker.NewClient(ctx, cfg, etcdCli, true)
			require.NoError(t, err)
			defer func() {
				if client != nil {
					_ = client.Close(ctx)
				}
			}()

			// CreateLog if not exists
			logName := "TestBasicReadWriteInServiceMode" + t.Name() + "_" + time.Now().Format("20060102150405")
			client.CreateLog(context.Background(), logName)

			// OpenLog
			logHandle, openErr := client.OpenLog(context.Background(), logName)
			require.NoError(t, openErr)

			// Test write
			// OpenWriter
			logWriter, openWriterErr := logHandle.OpenLogWriter(context.Background())
			require.NoError(t, openWriterErr)
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
				require.NoError(t, openReaderErr)
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
				require.NoError(t, openReaderErr)
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
				require.NoError(t, openReaderErr)
				var readOne atomic.Bool
				go func() {
					_, err := logReader.ReadNext(context.Background())
					if err == nil {
						readOne.Store(true)
					}
				}()
				time.Sleep(2 * time.Second)
				assert.False(t, readOne.Load())
				closeReaderErr := logReader.Close(context.Background())
				assert.NoError(t, closeReaderErr)
			}

			fmt.Println("Test Read finished")
		})
	}
}

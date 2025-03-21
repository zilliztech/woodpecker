package integration

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func TestReadTheWrittenDataSequentially(t *testing.T) {
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestReadTheWrittenDataSequentially",
		},
		{
			name:        "ObjectStorage",
			storageType: "", // 使用默认存储类型minio-compatible
			rootPath:    "", // 使用默认存储无需指定路径
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration()
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			if err != nil {
				fmt.Println(err)
			}

			// CreateLog if not exists
			logName := "test_log" + time.Now().Format("20060102150405")
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
					&log.WriterMessage{
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
				logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &earliest)
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
				logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), start)
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
				logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &latest)
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
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestReadWriteLoop",
		},
		{
			name:        "ObjectStorage",
			storageType: "", // 使用默认存储类型 minio-compatible
			rootPath:    "", // 使用默认存储无需指定路径
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration()
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			if err != nil {
				fmt.Println(err)
			}
			// CreateLog if not exists
			logName := "test_log" + time.Now().Format("20060102150405")
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
			&log.WriterMessage{
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
	fmt.Printf("Test Write finished %v,%v \n", successIds[0], successIds[count-1])
	return successIds
}

func testRead(t *testing.T, client woodpecker.Client, logName string, writtenIds []*log.LogMessageId, count int) {
	// OpenLog
	logHandle, openErr := client.OpenLog(context.Background(), logName)
	assert.NoError(t, openErr)
	// Test read all from earliest
	from := writtenIds[0]
	logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), from)
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
	fmt.Printf("Test Read finished %v,%v \n", readMsgs[0].Id, readMsgs[count-1].Id)
}

func TestMultiAppendSyncLoop(t *testing.T) {
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    "/tmp/TestMultiAppendSyncLoop",
		},
		{
			name:        "ObjectStorage",
			storageType: "", // 使用默认存储类型 minio-compatible
			rootPath:    "", // 使用默认存储无需指定路径
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := config.NewConfiguration()
			assert.NoError(t, err)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
			if err != nil {
				fmt.Println(err)
			}
			// CreateLog if not exists
			logName := "test_log" + time.Now().Format("20060102150405")
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
				&log.WriterMessage{
					Payload:    []byte(fmt.Sprintf("hello world %d", i)),
					Properties: map[string]string{"key": fmt.Sprintf("value%d", i)},
				},
			)
			//resultList = append(resultList, writeResult)
			resultList[no] = writeResult
			if writeResult.Err != nil {
				fmt.Printf("write failed %v \n", writeResult.Err)
			} else {
				fmt.Printf("write %d %d success \n", writeResult.LogMessageId.SegmentId, writeResult.LogMessageId.EntryId)
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
		fmt.Printf("unexpected success num")
	}
	fmt.Printf("success: ")
	for _, msgId := range successIds {
		fmt.Printf("%d:%d ,", msgId.SegmentId, msgId.EntryId)
	}
	fmt.Printf("\n")
	sort.Slice(successIds, func(i, j int) bool {
		if successIds[i].SegmentId < successIds[j].SegmentId {
			return true
		} else if successIds[i].SegmentId > successIds[j].SegmentId {
			return false
		} else {
			return successIds[i].EntryId < successIds[j].EntryId
		}
	})
	fmt.Printf("success order: ")
	for _, msgId := range successIds {
		fmt.Printf("%d:%d ,", msgId.SegmentId, msgId.EntryId)
	}
	fmt.Printf("\n")
	fmt.Printf("successIds:%v \n", successIds)
	for idx, msgId := range successIds {
		if idx == 0 {
			continue
		}
		if msgId.SegmentId == successIds[idx-1].SegmentId {
			if msgId.EntryId != successIds[idx-1].EntryId+1 {
				fmt.Printf("idx:%d,msgId:%v,successIds:%v \n", idx, msgId, successIds)
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

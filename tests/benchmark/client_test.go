package benchmark

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func TestE2EWrite(t *testing.T) {
	startGopsAgent()

	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ###  CreateLog if not exists
	client.GetMetadataProvider().CreateLog(context.Background(), "test_log")

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

func TestAsyncWritePerformance(t *testing.T) {
	startGopsAgent()
	startMetrics()
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)

	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ###  CreateLog if not exists
	client.CreateLog(context.Background(), "test_log")

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

	resultChan := make([]<-chan *log.WriteResult, 1000000)
	failedIdxs := make([]int, 0)
	successCount := 0
	for i := 0; i < 1000000; i++ {
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
	for i := 0; i < 1000000; i++ {
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
	for i := 1; i <= 1000000; i++ {
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
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)

	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
	}

	// ###  CreateLog if not exists
	client.CreateLog(context.Background(), "test_log")

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
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
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
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
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
	earliest := log.EarliestLogMessageID()
	logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &earliest)
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
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
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
	latest := log.LatestLogMessageID()
	logReader, openReaderErr := logHandle.OpenLogReader(context.Background(), &latest)
	if openReaderErr != nil {
		fmt.Printf("Open reader failed, err:%v\n", openReaderErr)
		panic(openReaderErr)
	}

	more := false
	go func() {
		msg, err := logReader.ReadNext(context.Background())
		if err != nil {
			fmt.Printf("read failed, err:%v\n", err)
			t.Error(err)
		} else {
			more = true
			fmt.Printf("read success, msg:%v\n", msg)
		}
	}()
	time.Sleep(time.Second * 2)
	assert.False(t, more, "should read nothing and timeout")
}

func TestReadFromSpecifiedPosition(t *testing.T) {
	startGopsAgent()
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	client, err := woodpecker.NewEmbedClientFromConfig(context.Background(), cfg)
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
		SegmentId: 5,
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

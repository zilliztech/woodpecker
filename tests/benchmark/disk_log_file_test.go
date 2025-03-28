package benchmark

import (
	"context"
	"fmt"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/disk"
)

// min返回a和b中较小的一个值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestDiskLogFileWritePerformance 测试 DiskLogFile 写入性能
// 关注点：buffer大小、flush频率、文件大小对写入性能的影响
func TestDiskLogFileWritePerformance(t *testing.T) {
	startGopsAgent()
	startMetrics()
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "info"
	logger.InitLogger(cfg)

	// 测试参数
	testCases := []struct {
		name         string
		fragmentSize int           // fragment文件大小
		bufferSize   int           // 最大buffer大小
		dataSize     int           // 每条数据大小
		writeCount   int           // 总写入次数
		flushRate    time.Duration // flush频率，0表示不自动flush
	}{
		// 小数据测试 - 减少数据量方便调试
		{"小文件_小buffer_不自动flush", 10 * 1024 * 1024, 1 * 1024 * 1024, 4 * 1024, 100, 0},
		{"小文件_小buffer_高频flush", 10 * 1024 * 1024, 1 * 1024 * 1024, 4 * 1024, 100, 100 * time.Millisecond},
		{"小文件_大buffer_不自动flush", 10 * 1024 * 1024, 8 * 1024 * 1024, 4 * 1024, 1000, 0},
		{"小文件_大buffer_低频flush", 10 * 1024 * 1024, 8 * 1024 * 1024, 4 * 1024, 1000, 500 * time.Millisecond},

		// 中数据测试
		{"中文件_小buffer_不自动flush", 50 * 1024 * 1024, 1 * 1024 * 1024, 32 * 1024, 500, 0},
		{"中文件_小buffer_高频flush", 50 * 1024 * 1024, 1 * 1024 * 1024, 32 * 1024, 500, 100 * time.Millisecond},
		{"中文件_大buffer_不自动flush", 50 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024, 500, 0},
		{"中文件_大buffer_低频flush", 50 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024, 500, 500 * time.Millisecond},

		// 大数据测试
		{"大文件_小buffer_不自动flush", 100 * 1024 * 1024, 4 * 1024 * 1024, 256 * 1024, 200, 0},
		{"大文件_小buffer_高频flush", 100 * 1024 * 1024, 4 * 1024 * 1024, 256 * 1024, 200, 100 * time.Millisecond},
		{"大文件_大buffer_不自动flush", 100 * 1024 * 1024, 32 * 1024 * 1024, 256 * 1024, 200, 0},
		{"大文件_大buffer_低频flush", 100 * 1024 * 1024, 32 * 1024 * 1024, 256 * 1024, 200, 500 * time.Millisecond},

		// Little big dataset Test, 2MB*500 = 1GB
		{"大文件_大buffer_低频flush", 128 * 1024 * 1024, 32 * 1024 * 1024, 2 * 1024 * 1024, 500, 10 * time.Millisecond},
		{"大文件_大buffer_超高频flush", 2 * 1024 * 1024 * 1024, 64 * 1024 * 1024, 2 * 1024 * 1024, 500, 0},
		{"大文件_大buffer_超高频flush", 1 * 1024 * 1024 * 1024, 4 * 1024 * 1024, 4 * 1024, 5000, 0},
	}

	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("disklogfile_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var options []disk.Option
			options = append(options, disk.WithFragmentSize(tc.fragmentSize))
			options = append(options, disk.WithMaxBufferSize(tc.bufferSize))

			// 如果不自动flush，禁用自动同步
			if tc.flushRate == 0 {
				options = append(options, disk.WithDisableAutoSync())
			}

			// 创建DiskLogFile实例
			logFile, err := disk.NewDiskLogFile(1, tempDir, options...)
			if err != nil {
				t.Fatalf("创建DiskLogFile失败: %v", err)
			}
			defer logFile.Close()

			// 准备测试数据和上下文
			ctx := context.Background()
			totalBytesWritten := 0
			manualFlushCount := 0
			dataSet := make([][]byte, tc.writeCount)

			// 生成随机测试数据
			for i := 0; i < tc.writeCount; i++ {
				data := make([]byte, tc.dataSize)
				rand.Read(data)
				dataSet[i] = data
			}

			// 记录写入和同步时间
			writeTime := time.Duration(0)
			flushTime := time.Duration(0)
			totalManualFlushTime := time.Duration(0)
			lastFlushTime := time.Now()

			start := time.Now()

			// 执行写入测试
			for i := 0; i < tc.writeCount; i += 100 {
				// 确定此批次的大小
				batchSize := min(100, tc.writeCount-i)

				// 保存每个写入操作的channel
				resultChannels := make([]<-chan int64, batchSize)
				entryIds := make([]int64, batchSize)

				// 批量提交写入请求
				for j := 0; j < batchSize; j++ {
					writeStart := time.Now()
					entryId := int64(i + j)
					entryIds[j] = entryId

					_, ch, err := logFile.AppendAsync(ctx, entryId, dataSet[i+j])
					if err != nil {
						t.Fatalf("写入失败: %v", err)
					}
					resultChannels[j] = ch

					writeTime += time.Since(writeStart)
					totalBytesWritten += len(dataSet[i+j])
				}

				// 检查是否需要手动flush (对于不自动flush的情况)
				if tc.flushRate == 0 {
					flushStart := time.Now()
					err := logFile.Sync(ctx)
					flushDuration := time.Since(flushStart)
					totalManualFlushTime += flushDuration

					if err != nil {
						t.Fatalf("手动同步失败: %v", err)
					}
					manualFlushCount++
				}
				// 对于有自动flush的情况，记录距离上次flush经过的时间
				if tc.flushRate > 0 && time.Since(lastFlushTime) >= tc.flushRate {
					flushStart := time.Now()
					err := logFile.Sync(ctx)
					flushDuration := time.Since(flushStart)
					flushTime += flushDuration

					if err != nil {
						t.Fatalf("定时同步失败: %v", err)
					}
					lastFlushTime = time.Now()
				}

				// 等待所有写入完成
				for j, ch := range resultChannels {
					select {
					case result := <-ch:
						if result < 0 {
							t.Fatalf("写入未能成功完成: id=%d", entryIds[j])
						}
					case <-time.After(5 * time.Second):
						t.Fatalf("写入超时: id=%d", entryIds[j])
					}
				}

				// 报告进度
				t.Logf("进度: %d/%d 操作完成", min(i+batchSize, tc.writeCount), tc.writeCount)
			}

			// 最后一次同步确保所有数据写入磁盘
			finalFlushStart := time.Now()
			if err := logFile.Sync(ctx); err != nil {
				t.Fatalf("最后同步失败: %v", err)
			}
			finalFlushTime := time.Since(finalFlushStart)

			if tc.flushRate == 0 {
				totalManualFlushTime += finalFlushTime
				manualFlushCount++
			} else {
				flushTime += finalFlushTime
			}

			duration := time.Since(start)

			// 计算性能指标
			throughput := float64(totalBytesWritten) / (1024 * 1024) / duration.Seconds()
			opsPerSecond := float64(tc.writeCount) / duration.Seconds()
			writeAvgTime := writeTime.Seconds() * 1000 / float64(tc.writeCount) // 毫秒

			var flushAvgTime float64
			if tc.flushRate == 0 && manualFlushCount > 0 {
				flushAvgTime = totalManualFlushTime.Seconds() * 1000 / float64(manualFlushCount) // 毫秒
			}

			// 输出结果
			t.Logf("DiskLogFile写入测试结果 - %s:", tc.name)
			t.Logf("  Fragment大小: %d MB", tc.fragmentSize/(1024*1024))
			t.Logf("  Buffer大小: %d MB", tc.bufferSize/(1024*1024))
			t.Logf("  总写入数据: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  写入次数: %d", tc.writeCount)
			t.Logf("  数据块大小: %d KB", tc.dataSize/1024)

			if tc.flushRate == 0 {
				t.Logf("  手动刷新: 每100次写入")
				t.Logf("  手动刷新次数: %d", manualFlushCount)
				t.Logf("  手动刷新总耗时: %v (%.1f%%)", totalManualFlushTime, float64(totalManualFlushTime)/float64(duration)*100)
				t.Logf("  单次手动刷新平均耗时: %.3f ms", flushAvgTime)
			} else {
				t.Logf("  自动刷新频率: %v", tc.flushRate)
				t.Logf("  自动刷新总耗时: %v (%.1f%%)", flushTime, float64(flushTime)/float64(duration)*100)
			}

			t.Logf("  总耗时: %v", duration)
			t.Logf("  写入总耗时: %v (%.1f%%)", writeTime, float64(writeTime)/float64(duration)*100)
			t.Logf("  吞吐量: %.2f MB/s", throughput)
			t.Logf("  操作速率: %.2f ops/s", opsPerSecond)
			t.Logf("  单次写入平均耗时: %.3f ms", writeAvgTime)
		})
	}
}

// TestDiskLogFileReadPerformance 测试 DiskLogFile 读取性能
// 关注点：读取模式和数据块大小对读取性能的影响
func TestDiskLogFileReadPerformance(t *testing.T) {
	// 测试参数
	testCases := []struct {
		name       string
		dataSize   int  // 每条数据大小
		entryCount int  // 要写入的条目数量
		sequential bool // 是否顺序读取
	}{
		{"小数据_顺序读取", 4 * 1024, 1000, true},
		{"小数据_随机读取", 4 * 1024, 1000, false},
		{"中数据_顺序读取", 32 * 1024, 500, true},
		{"中数据_随机读取", 32 * 1024, 500, false},
		{"大数据_顺序读取", 256 * 1024, 100, true},
		{"大数据_随机读取", 256 * 1024, 100, false},
	}

	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("disklogfile_read_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// 步骤1：先创建并填充 DiskLogFile
			t.Logf("准备测试文件，写入 %d 条目...", tc.entryCount)
			options := []disk.Option{
				disk.WithFragmentSize(128 * 1024 * 1024), // 128MB
			}
			logFile, err := disk.NewDiskLogFile(1, tempDir, options...)
			if err != nil {
				t.Fatalf("创建DiskLogFile失败: %v", err)
			}

			// 生成随机测试数据并存储引用，以便后续验证读取
			testData := make([][]byte, tc.entryCount)
			entryIds := make([]int64, tc.entryCount)
			resultChannels := make([]<-chan int64, tc.entryCount)

			// 批量生成测试数据
			for i := 0; i < tc.entryCount; i++ {
				data := make([]byte, tc.dataSize)
				rand.Read(data)
				testData[i] = data
				entryIds[i] = int64(i + 1) // 从1开始的连续ID
			}

			// 分批提交异步写入请求
			batchSize := 100
			for i := 0; i < tc.entryCount; i += batchSize {
				end := min(i+batchSize, tc.entryCount)
				// 提交一批数据
				for j := i; j < end; j++ {
					_, ch, err := logFile.AppendAsync(ctx, entryIds[j], testData[j])
					if err != nil {
						logFile.Close()
						t.Fatalf("写入数据失败: %v", err)
					}
					resultChannels[j] = ch
				}

				// 等待这一批写入完成
				for j := i; j < end; j++ {
					select {
					case result := <-resultChannels[j]:
						if result < 0 {
							logFile.Close()
							t.Fatalf("写入未能成功完成: %d", j)
						}
					case <-time.After(5 * time.Second):
						logFile.Close()
						t.Fatalf("写入超时: %d", j)
					}
				}

				t.Logf("批量写入进度: %d/%d 条数据已写入", end, tc.entryCount)
			}

			// 确保所有数据都写入磁盘
			if err := logFile.Sync(ctx); err != nil {
				logFile.Close()
				t.Fatalf("同步数据失败: %v", err)
			}

			t.Logf("准备完成，开始读取测试...")

			// 步骤2：创建Reader并测试读取性能
			reader, err := logFile.NewReader(ctx, storage.ReaderOpt{
				StartSequenceNum: 1, // 从第一个条目开始
				EndSequenceNum:   0, // 0表示读取到末尾
			})
			if err != nil {
				logFile.Close()
				t.Fatalf("创建Reader失败: %v", err)
			}

			// 准备测量读取性能的变量
			totalBytesRead := 0
			readCount := 0
			readErrors := 0

			// 如果是随机读取，则打乱读取顺序
			readOrder := make([]int, tc.entryCount)
			for i := 0; i < tc.entryCount; i++ {
				readOrder[i] = i
			}

			if !tc.sequential {
				rand.Shuffle(tc.entryCount, func(i, j int) {
					readOrder[i], readOrder[j] = readOrder[j], readOrder[i]
				})
			}

			// 开始读取测试
			start := time.Now()

			if tc.sequential {
				// 顺序读取
				for reader.HasNext() {
					entry, err := reader.ReadNext()
					if err != nil {
						readErrors++
						continue
					}

					totalBytesRead += len(entry.Values)
					readCount++
				}
			} else {
				// 随机读取 - 先关闭当前Reader并创建多个新Reader进行随机位置读取
				reader.Close()

				for _, idx := range readOrder {
					// 为每个ID创建一个单独的Reader
					entryId := entryIds[idx]

					singleReader, err := logFile.NewReader(ctx, storage.ReaderOpt{
						StartSequenceNum: entryId,     // 从特定ID开始
						EndSequenceNum:   entryId + 1, // 只读取一个条目
					})

					if err != nil {
						readErrors++
						continue
					}

					if singleReader.HasNext() {
						entry, err := singleReader.ReadNext()
						if err != nil {
							readErrors++
							singleReader.Close()
							continue
						}

						totalBytesRead += len(entry.Values)
						readCount++
					}

					singleReader.Close()
				}
			}

			duration := time.Since(start)

			// 关闭文件
			reader.Close()
			logFile.Close()

			// 计算性能指标
			throughput := float64(totalBytesRead) / (1024 * 1024) / duration.Seconds()
			opsPerSecond := float64(readCount) / duration.Seconds()
			avgReadSize := float64(totalBytesRead) / float64(readCount) / 1024 // KB

			// 输出结果
			t.Logf("DiskLogFile读取测试结果 - %s:", tc.name)
			t.Logf("  总读取数据: %.2f MB", float64(totalBytesRead)/(1024*1024))
			t.Logf("  总条目数: %d", tc.entryCount)
			t.Logf("  成功读取条目: %d", readCount)
			t.Logf("  读取错误数: %d", readErrors)
			t.Logf("  数据块大小: %d KB", tc.dataSize/1024)
			t.Logf("  平均实际读取: %.2f KB/次", avgReadSize)
			readModeStr := "顺序读取"
			if !tc.sequential {
				readModeStr = "随机读取"
			}
			t.Logf("  读取模式: %s", readModeStr)
			t.Logf("  总耗时: %v", duration)
			t.Logf("  吞吐量: %.2f MB/s", throughput)
			t.Logf("  操作速率: %.2f ops/s", opsPerSecond)
			t.Logf("  单次读取平均耗时: %.3f µs", duration.Seconds()*1000000/float64(readCount))
		})
	}
}

// TestDiskLogFileMixedPerformance 测试混合读写场景下的 DiskLogFile 性能
func TestDiskLogFileMixedPerformance(t *testing.T) {
	// 测试参数
	testCases := []struct {
		name           string
		fragmentSize   int // fragment文件大小
		bufferSize     int // buffer大小
		dataSize       int // 每条数据大小
		totalOps       int // 总操作数
		readPercentage int // 读取操作百分比
	}{
		{"小文件_读写均衡", 10 * 1024 * 1024, 1 * 1024 * 1024, 4 * 1024, 1000, 50},
		{"小文件_读多写少", 10 * 1024 * 1024, 1 * 1024 * 1024, 4 * 1024, 1000, 80},
		{"小文件_写多读少", 10 * 1024 * 1024, 1 * 1024 * 1024, 4 * 1024, 1000, 20},
		{"大文件_读写均衡", 100 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024, 500, 50},
		{"大文件_读多写少", 100 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024, 500, 80},
		{"大文件_写多读少", 100 * 1024 * 1024, 16 * 1024 * 1024, 32 * 1024, 500, 20},
	}

	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("disklogfile_mixed_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// 创建DiskLogFile实例
			options := []disk.Option{
				disk.WithFragmentSize(tc.fragmentSize),
				disk.WithMaxBufferSize(tc.bufferSize),
			}

			logFile, err := disk.NewDiskLogFile(1, tempDir, options...)
			if err != nil {
				t.Fatalf("创建DiskLogFile失败: %v", err)
			}
			defer logFile.Close()

			// 准备参数
			readOps := tc.totalOps * tc.readPercentage / 100
			writeOps := tc.totalOps - readOps

			// 创建操作数组 (0=写入, 1=读取)
			operations := make([]int, tc.totalOps)
			for i := 0; i < readOps; i++ {
				operations[i] = 1 // 读取
			}
			for i := readOps; i < tc.totalOps; i++ {
				operations[i] = 0 // 写入
			}

			// 打乱操作顺序
			rand.Shuffle(tc.totalOps, func(i, j int) {
				operations[i], operations[j] = operations[j], operations[i]
			})

			// 统计变量
			syncCount := 0
			totalBytesWritten := 0
			totalBytesRead := 0
			writeTime := time.Duration(0)
			readTime := time.Duration(0)
			syncTime := time.Duration(0)
			maxEntryId := int64(0)

			// 预生成写入数据
			writeData := make([][]byte, writeOps)
			for i := 0; i < writeOps; i++ {
				data := make([]byte, tc.dataSize)
				rand.Read(data)
				writeData[i] = data
			}

			start := time.Now()

			// 执行混合操作
			writeOpsCount := 0
			readOpsCount := 0

			// 处理写操作
			{
				var pendingWrites []int
				resultChannels := make([]<-chan int64, 0, writeOps)
				entryIds := make([]int64, 0, writeOps)

				// 找出所有写操作的索引
				for i, op := range operations {
					if op == 0 {
						pendingWrites = append(pendingWrites, i)
					}
				}

				// 分批处理写操作，每批最多10个
				for i := 0; i < len(pendingWrites); i += 10 {
					batchSize := min(10, len(pendingWrites)-i)
					batchIndices := pendingWrites[i : i+batchSize]

					// 提交这一批的写操作
					for range batchIndices {
						if writeOpsCount >= writeOps {
							continue
						}

						entryId := int64(maxEntryId + 1)
						maxEntryId++

						writeStart := time.Now()
						_, ch, err := logFile.AppendAsync(ctx, entryId, writeData[writeOpsCount])
						if err != nil {
							t.Fatalf("写入失败: %v", err)
						}

						resultChannels = append(resultChannels, ch)
						entryIds = append(entryIds, entryId)
						writeOpsCount++
						writeTime += time.Since(writeStart)
						totalBytesWritten += len(writeData[writeOpsCount-1])
					}

					// 等待这一批写操作完成
					for j, ch := range resultChannels[len(resultChannels)-batchSize:] {
						select {
						case result := <-ch:
							if result < 0 {
								t.Fatalf("写入未能成功完成: %d", entryIds[len(entryIds)-batchSize+j])
							}
						case <-time.After(5 * time.Second):
							t.Fatalf("写入超时: %d", entryIds[len(entryIds)-batchSize+j])
						}
					}

					// 每10次写入执行一次Sync
					syncStart := time.Now()
					err := logFile.Sync(ctx)
					syncTime += time.Since(syncStart)

					if err != nil {
						t.Fatalf("同步失败: %v", err)
					}
					syncCount++

					// 报告进度
					completedOps := writeOpsCount + readOpsCount
					if completedOps%100 == 0 || completedOps == tc.totalOps {
						t.Logf("进度: %d/%d 操作完成 (写入: %d, 读取: %d)",
							completedOps, tc.totalOps, writeOpsCount, readOpsCount)
					}
				}
			}

			// 处理读操作
			{
				for _, op := range operations {
					if op == 1 {
						// 读取操作 - 只能读取已写入的条目
						if maxEntryId == 0 || readOpsCount >= readOps {
							continue // 没有可读取的数据或已达到读取上限
						}

						// 随机选择一个已写入的条目ID
						entryId := int64(rand.Intn(int(maxEntryId))) + 1

						// 创建Reader读取特定条目
						readStart := time.Now()
						reader, err := logFile.NewReader(ctx, storage.ReaderOpt{
							StartSequenceNum: entryId,
							EndSequenceNum:   entryId + 1,
						})

						if err != nil {
							t.Fatalf("创建Reader失败: %v", err)
						}

						if reader.HasNext() {
							entry, err := reader.ReadNext()
							if err == nil {
								totalBytesRead += len(entry.Values)
								readOpsCount++
							}
						}

						reader.Close()
						readTime += time.Since(readStart)

						// 报告进度
						completedOps := writeOpsCount + readOpsCount
						if completedOps%100 == 0 || completedOps == tc.totalOps {
							t.Logf("进度: %d/%d 操作完成 (写入: %d, 读取: %d)",
								completedOps, tc.totalOps, writeOpsCount, readOpsCount)
						}
					}
				}
			}

			// 最后一次同步
			finalSyncStart := time.Now()
			if err := logFile.Sync(ctx); err != nil {
				t.Fatalf("最后同步失败: %v", err)
			}
			syncTime += time.Since(finalSyncStart)
			syncCount++

			duration := time.Since(start)

			// 计算性能指标
			writeThroughput := float64(totalBytesWritten) / (1024 * 1024) / duration.Seconds()
			readThroughput := float64(totalBytesRead) / (1024 * 1024) / duration.Seconds()
			totalThroughput := float64(totalBytesWritten+totalBytesRead) / (1024 * 1024) / duration.Seconds()

			writeOpsPerSecond := float64(writeOpsCount) / duration.Seconds()
			readOpsPerSecond := float64(readOpsCount) / duration.Seconds()
			totalOpsPerSecond := float64(writeOpsCount+readOpsCount) / duration.Seconds()

			// 如果有操作，计算平均时间
			writeAvgTime := 0.0
			if writeOpsCount > 0 {
				writeAvgTime = writeTime.Seconds() * 1000 / float64(writeOpsCount) // 毫秒
			}
			readAvgTime := 0.0
			if readOpsCount > 0 {
				readAvgTime = readTime.Seconds() * 1000 / float64(readOpsCount) // 毫秒
			}
			syncAvgTime := 0.0
			if syncCount > 0 {
				syncAvgTime = syncTime.Seconds() * 1000 / float64(syncCount) // 毫秒
			}

			// 输出结果
			t.Logf("DiskLogFile混合测试结果 - %s:", tc.name)
			t.Logf("  Fragment大小: %d MB", tc.fragmentSize/(1024*1024))
			t.Logf("  Buffer大小: %d MB", tc.bufferSize/(1024*1024))
			t.Logf("  数据块大小: %d KB", tc.dataSize/1024)
			t.Logf("  读写比例: %d%%读/%d%%写", tc.readPercentage, 100-tc.readPercentage)
			t.Logf("  总操作数: %d (写入: %d, 读取: %d)", writeOpsCount+readOpsCount, writeOpsCount, readOpsCount)
			t.Logf("  总写入数据: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  总读取数据: %.2f MB", float64(totalBytesRead)/(1024*1024))
			t.Logf("  同步次数: %d", syncCount)
			t.Logf("  同步间隔: 每10次写入")
			t.Logf("  总耗时: %v", duration)
			t.Logf("  写入总耗时: %v (%.1f%%)", writeTime, float64(writeTime)/float64(duration)*100)
			t.Logf("  读取总耗时: %v (%.1f%%)", readTime, float64(readTime)/float64(duration)*100)
			t.Logf("  同步总耗时: %v (%.1f%%)", syncTime, float64(syncTime)/float64(duration)*100)
			t.Logf("  总吞吐量: %.2f MB/s", totalThroughput)
			t.Logf("  写入吞吐量: %.2f MB/s", writeThroughput)
			t.Logf("  读取吞吐量: %.2f MB/s", readThroughput)
			t.Logf("  总操作速率: %.2f ops/s", totalOpsPerSecond)
			t.Logf("  写入操作速率: %.2f ops/s", writeOpsPerSecond)
			t.Logf("  读取操作速率: %.2f ops/s", readOpsPerSecond)
			t.Logf("  单次写入平均耗时: %.3f ms", writeAvgTime)
			t.Logf("  单次读取平均耗时: %.3f ms", readAvgTime)
			t.Logf("  单次同步平均耗时: %.3f ms", syncAvgTime)
		})
	}
}

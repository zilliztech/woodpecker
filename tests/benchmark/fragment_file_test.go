package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zilliztech/woodpecker/server/storage/disk"
)

// TestFragmentWritePerformance 测试 FragmentFile 写入性能
// 关注点：数据块大小、flush频率、文件大小对写入性能的影响
func TestFragmentWritePerformance(t *testing.T) {
	// 测试参数
	testCases := []struct {
		name          string
		fileSize      int64 // 文件大小
		dataBlockSize int   // 每次写入的数据块大小
		flushInterval int   // 每写入多少次执行一次flush
		writeCount    int   // 总写入次数
	}{
		// 小数据块测试
		{"小块_小文件_不flush", 10 * 1024 * 1024, 4 * 1024, 0, 1000},
		{"小块_小文件_适中flush", 10 * 1024 * 1024, 4 * 1024, 100, 1000},
		{"小块_小文件_频繁flush", 10 * 1024 * 1024, 4 * 1024, 10, 1000},
		{"小块_大文件_不flush", 100 * 1024 * 1024, 4 * 1024, 0, 10000},
		{"小块_大文件_适中flush", 100 * 1024 * 1024, 4 * 1024, 100, 10000},
		{"小块_大文件_频繁flush", 100 * 1024 * 1024, 4 * 1024, 10, 10000},

		// 中等数据块测试
		{"中块_小文件_不flush", 10 * 1024 * 1024, 32 * 1024, 0, 250},
		{"中块_小文件_适中flush", 10 * 1024 * 1024, 32 * 1024, 50, 250},
		{"中块_小文件_频繁flush", 10 * 1024 * 1024, 32 * 1024, 10, 250},
		{"中块_大文件_不flush", 100 * 1024 * 1024, 32 * 1024, 0, 2500},
		{"中块_大文件_适中flush", 100 * 1024 * 1024, 32 * 1024, 50, 2500},
		{"中块_大文件_频繁flush", 100 * 1024 * 1024, 32 * 1024, 10, 2500},

		// 大数据块测试
		{"大块_小文件_不flush", 20 * 1024 * 1024, 256 * 1024, 0, 50},
		{"大块_小文件_适中flush", 20 * 1024 * 1024, 256 * 1024, 10, 50},
		{"大块_小文件_频繁flush", 20 * 1024 * 1024, 256 * 1024, 5, 50},
		{"大块_大文件_不flush", 200 * 1024 * 1024, 256 * 1024, 0, 500},
		{"大块_大文件_适中flush", 200 * 1024 * 1024, 256 * 1024, 10, 500},
		{"大块_大文件_频繁flush", 200 * 1024 * 1024, 256 * 1024, 5, 500},
	}

	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("fragment_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("fragment_%s.data", tc.name))

			// 创建FragmentFile实例
			fragment, err := disk.NewFragmentFile(filePath, tc.fileSize, 1, 1)
			if err != nil {
				t.Fatalf("创建FragmentFile失败: %v", err)
			}

			defer fragment.Release()

			// 准备测试数据和上下文
			ctx := context.Background()
			flushCount := 0
			totalBytesWritten := 0
			bytesSinceLastFlush := 0
			flushSizes := make([]int, 0) // 记录每次flush的数据量

			// 生成随机测试数据
			dataSet := make([][]byte, tc.writeCount)
			for i := 0; i < tc.writeCount; i++ {
				data := make([]byte, tc.dataBlockSize)
				rand.Read(data)
				dataSet[i] = data
			}

			// 记录写入和刷新时间
			writeTime := time.Duration(0)
			flushTime := time.Duration(0)

			start := time.Now()

			for i := 0; i < tc.writeCount; i++ {
				// 写入数据
				writeStart := time.Now()
				err := fragment.Write(ctx, dataSet[i])
				writeTime += time.Since(writeStart)

				if err != nil {
					t.Fatalf("写入失败: %v", err)
				}

				totalBytesWritten += len(dataSet[i])
				bytesSinceLastFlush += len(dataSet[i])

				// 根据刷新策略执行刷新
				shouldFlush := tc.flushInterval > 0 && (i+1)%tc.flushInterval == 0

				if shouldFlush {
					flushStart := time.Now()
					err := fragment.Flush(ctx)
					flushTime += time.Since(flushStart)

					if err != nil {
						t.Fatalf("刷新文件失败: %v", err)
					}

					flushSizes = append(flushSizes, bytesSinceLastFlush) // 记录本次flush的数据量
					flushCount++
					bytesSinceLastFlush = 0
				}
			}

			// 最后一次flush确保所有数据写入磁盘
			if bytesSinceLastFlush > 0 {
				flushStart := time.Now()
				if err := fragment.Flush(ctx); err != nil {
					t.Fatalf("最后刷新文件失败: %v", err)
				}
				flushTime += time.Since(flushStart)
				flushSizes = append(flushSizes, bytesSinceLastFlush)
				flushCount++
			}

			duration := time.Since(start)

			// 统计flush数据量
			var minFlushSize, maxFlushSize int
			totalFlushSize := 0
			if len(flushSizes) > 0 {
				minFlushSize = flushSizes[0]
				maxFlushSize = flushSizes[0]
				for _, size := range flushSizes {
					totalFlushSize += size
					if size < minFlushSize {
						minFlushSize = size
					}
					if size > maxFlushSize {
						maxFlushSize = size
					}
				}
			}
			avgFlushSize := 0
			if flushCount > 0 {
				avgFlushSize = totalFlushSize / flushCount
			}

			// 计算性能指标
			throughput := float64(totalBytesWritten) / (1024 * 1024) / duration.Seconds()
			opsPerSecond := float64(tc.writeCount) / duration.Seconds()
			writeAvgTime := writeTime.Seconds() * 1000 / float64(tc.writeCount) // 毫秒
			flushAvgTime := 0.0
			if flushCount > 0 {
				flushAvgTime = flushTime.Seconds() * 1000 / float64(flushCount) // 毫秒
			}

			// 输出结果
			t.Logf("FragmentFile写入测试结果 - %s:", tc.name)
			t.Logf("  文件大小: %d MB", tc.fileSize/(1024*1024))
			t.Logf("  总写入数据: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  写入次数: %d", tc.writeCount)
			t.Logf("  数据块大小: %d KB", tc.dataBlockSize/1024)
			t.Logf("  刷新次数: %d", flushCount)
			t.Logf("  刷新间隔: 每%d次写入", tc.flushInterval)
			if flushCount > 0 {
				t.Logf("  平均每次刷新数据量: %.2f KB", float64(avgFlushSize)/1024)
				t.Logf("  最小刷新数据量: %.2f KB", float64(minFlushSize)/1024)
				t.Logf("  最大刷新数据量: %.2f KB", float64(maxFlushSize)/1024)
			}
			t.Logf("  总耗时: %v", duration)
			t.Logf("  写入总耗时: %v (%.1f%%)", writeTime, float64(writeTime)/float64(duration)*100)
			if flushCount > 0 {
				t.Logf("  刷新总耗时: %v (%.1f%%)", flushTime, float64(flushTime)/float64(duration)*100)
			}
			t.Logf("  吞吐量: %.2f MB/s", throughput)
			t.Logf("  操作速率: %.2f ops/s", opsPerSecond)
			t.Logf("  单次写入平均耗时: %.3f ms", writeAvgTime)
			if flushCount > 0 {
				t.Logf("  单次刷新平均耗时: %.3f ms", flushAvgTime)
			}

			// 获取最终文件大小
			fileInfo, err := os.Stat(filePath)
			if err == nil {
				t.Logf("  最终文件大小: %.2f MB", float64(fileInfo.Size())/(1024*1024))
			}
		})
	}
}

// TestFragmentReadPerformance 测试 FragmentFile 读取性能
// 关注点：读取模式和数据块大小对读取性能的影响
func TestFragmentReadPerformance(t *testing.T) {
	// 测试参数
	testCases := []struct {
		name          string
		fileSize      int64 // 文件大小
		dataBlockSize int   // 写入的数据块大小
		entryCount    int   // 要写入的条目数量
	}{
		{"小文件_小数据块", 10 * 1024 * 1024, 4 * 1024, 1000},
		{"小文件_中数据块", 10 * 1024 * 1024, 32 * 1024, 250},
		{"中文件_小数据块", 50 * 1024 * 1024, 4 * 1024, 5000},
		{"中文件_中数据块", 50 * 1024 * 1024, 32 * 1024, 1250},
		{"大文件_小数据块", 100 * 1024 * 1024, 4 * 1024, 10000},
		{"大文件_中数据块", 100 * 1024 * 1024, 32 * 1024, 2500},
		{"大文件_大数据块", 100 * 1024 * 1024, 256 * 1024, 350},
	}

	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("fragment_read_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("fragment_read_%s.data", tc.name))
			ctx := context.Background()

			// 步骤1：先创建并填充 FragmentFile
			t.Logf("准备测试文件，写入 %d 条目...", tc.entryCount)
			fragment, err := disk.NewFragmentFile(filePath, tc.fileSize, 1, 1)
			if err != nil {
				t.Fatalf("创建FragmentFile失败: %v", err)
			}

			// 生成随机测试数据并存储引用，以便后续验证读取
			testData := make([][]byte, tc.entryCount)
			for i := 0; i < tc.entryCount; i++ {
				data := make([]byte, tc.dataBlockSize)
				rand.Read(data)
				testData[i] = data

				// 写入数据
				if err := fragment.Write(ctx, data); err != nil {
					fragment.Release()
					t.Fatalf("写入数据失败: %v", err)
				}
			}

			// 确保所有数据都写入磁盘
			if err := fragment.Flush(ctx); err != nil {
				fragment.Release()
				t.Fatalf("刷新数据失败: %v", err)
			}

			// 关闭并重新打开fragment以确保从磁盘读取
			fragment.Release()

			// 步骤2：重新打开 FragmentFile 并测试读取性能
			fragment, err = disk.OpenFragment(filePath, tc.fileSize, 1)
			if err != nil {
				t.Fatalf("打开FragmentFile失败: %v", err)
			}
			defer fragment.Release()

			// 获取条目范围
			firstId, err := fragment.GetFirstEntryId()
			if err != nil {
				t.Fatalf("获取第一个条目ID失败: %v", err)
			}
			lastId, err := fragment.GetLastEntryId()
			if err != nil {
				t.Fatalf("获取最后一个条目ID失败: %v", err)
			}

			t.Logf("读取测试 - 条目范围: %d 到 %d", firstId, lastId)

			// 准备测量读取性能的变量
			totalBytesRead := 0
			readCount := 0
			readErrors := 0

			start := time.Now()

			// 顺序读取所有条目
			for id := firstId; id <= lastId; id++ {
				data, err := fragment.GetEntry(id)
				if err != nil {
					readErrors++
					continue
				}

				totalBytesRead += len(data)
				readCount++

				// 验证读取的数据是否正确
				expectedData := testData[id-firstId]
				if !bytes.Equal(data, expectedData) {
					t.Fatalf("数据不匹配: 条目ID %d", id)
				}
			}

			duration := time.Since(start)

			// 计算性能指标
			throughput := float64(totalBytesRead) / (1024 * 1024) / duration.Seconds()
			opsPerSecond := float64(readCount) / duration.Seconds()
			avgReadSize := float64(totalBytesRead) / float64(readCount) / 1024 // KB

			// 输出结果
			t.Logf("FragmentFile读取测试结果 - %s:", tc.name)
			t.Logf("  文件大小: %.2f MB", float64(tc.fileSize)/(1024*1024))
			t.Logf("  总读取数据: %.2f MB", float64(totalBytesRead)/(1024*1024))
			t.Logf("  总条目数: %d", tc.entryCount)
			t.Logf("  成功读取条目: %d", readCount)
			t.Logf("  读取错误数: %d", readErrors)
			t.Logf("  数据块大小: %d KB", tc.dataBlockSize/1024)
			t.Logf("  平均实际读取: %.2f KB/次", avgReadSize)
			t.Logf("  总耗时: %v", duration)
			t.Logf("  吞吐量: %.2f MB/s", throughput)
			t.Logf("  操作速率: %.2f ops/s", opsPerSecond)
			t.Logf("  单次读取平均耗时: %.3f µs", duration.Seconds()*1000000/float64(readCount))
		})
	}
}

// TestFragmentMixedPerformance 测试混合读写场景下的 FragmentFile 性能
func TestFragmentMixedPerformance(t *testing.T) {
	// 测试参数
	testCases := []struct {
		name           string
		fileSize       int
		dataBlockSize  int
		flushInterval  int
		writeCount     int
		readPercentage int
	}{
		{"小文件_读写均衡", 10 * 1024 * 1024, 4 * 1024, 10, 1000, 50},
		{"中文件_读写均衡", 50 * 1024 * 1024, 32 * 1024, 10, 1000, 50},
		{"大文件_读写均衡", 200 * 1024 * 1024, 256 * 1024, 10, 500, 50},
		{"小文件_读多写少", 10 * 1024 * 1024, 4 * 1024, 10, 1000, 80},
		{"中文件_读多写少", 50 * 1024 * 1024, 32 * 1024, 10, 1000, 80},
		{"大文件_读多写少", 200 * 1024 * 1024, 256 * 1024, 10, 500, 80},
		{"小文件_写多读少", 10 * 1024 * 1024, 4 * 1024, 10, 1000, 20},
		{"中文件_写多读少", 50 * 1024 * 1024, 32 * 1024, 10, 1000, 20},
		{"大文件_写多读少", 200 * 1024 * 1024, 256 * 1024, 10, 500, 20},
	}

	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("fragment_mixed_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("fragment_mixed_%s.data", tc.name))
			ctx := context.Background()

			// 创建FragmentFile实例
			fragment, err := disk.NewFragmentFile(filePath, int64(tc.fileSize), 1, 1)
			if err != nil {
				t.Fatalf("创建FragmentFile失败: %v", err)
			}
			defer fragment.Release()

			// 准备测试数据
			totalOps := tc.writeCount
			readOps := totalOps * tc.readPercentage / 100
			writeOps := totalOps - readOps
			dataSet := make([][]byte, writeOps)

			for i := 0; i < writeOps; i++ {
				data := make([]byte, tc.dataBlockSize)
				rand.Read(data)
				dataSet[i] = data
			}

			// 操作统计
			writeCount := 0
			readCount := 0
			flushCount := 0
			totalBytesWritten := 0
			totalBytesRead := 0
			bytesSinceLastFlush := 0
			writeTime := time.Duration(0)
			readTime := time.Duration(0)
			flushTime := time.Duration(0)

			// 生成操作序列 (0=写入, 1=读取)
			operations := make([]int, totalOps)
			for i := 0; i < totalOps; i++ {
				if i < readOps {
					operations[i] = 1 // 读取
				} else {
					operations[i] = 0 // 写入
				}
			}
			// 打乱操作顺序
			rand.Shuffle(totalOps, func(i, j int) {
				operations[i], operations[j] = operations[j], operations[i]
			})

			start := time.Now()

			// 执行混合操作
			for i, op := range operations {
				if op == 0 && writeCount < writeOps {
					// 写入操作
					writeStart := time.Now()
					err := fragment.Write(ctx, dataSet[writeCount])
					writeTime += time.Since(writeStart)

					if err != nil {
						t.Fatalf("写入失败: %v", err)
					}

					totalBytesWritten += len(dataSet[writeCount])
					bytesSinceLastFlush += len(dataSet[writeCount])
					writeCount++

					// 检查是否需要刷新
					shouldFlush := tc.flushInterval > 0 && writeCount%tc.flushInterval == 0
					if shouldFlush {
						flushStart := time.Now()
						err := fragment.Flush(ctx)
						flushTime += time.Since(flushStart)

						if err != nil {
							t.Fatalf("刷新文件失败: %v", err)
						}

						flushCount++
						bytesSinceLastFlush = 0
					}
				} else if op == 1 && readCount < writeCount { // 只能读取已写入的条目
					// 读取操作
					// 随机选择一个已写入的条目ID
					entryId := int64(1 + rand.Intn(writeCount))

					readStart := time.Now()
					data, err := fragment.GetEntry(entryId)
					readTime += time.Since(readStart)

					if err != nil {
						// 在混合测试中，可能会尝试读取尚未写入的条目，这里我们忽略这种错误
						continue
					}

					totalBytesRead += len(data)
					readCount++
				}

				// 每100个操作报告一次进度
				if (i+1)%100 == 0 || i == totalOps-1 {
					t.Logf("进度: %d/%d 操作完成", i+1, totalOps)
				}
			}

			// 最后一次flush
			if bytesSinceLastFlush > 0 {
				flushStart := time.Now()
				if err := fragment.Flush(ctx); err != nil {
					t.Fatalf("最后刷新文件失败: %v", err)
				}
				flushTime += time.Since(flushStart)
				flushCount++
			}

			duration := time.Since(start)

			// 计算性能指标
			writeThroughput := float64(totalBytesWritten) / (1024 * 1024) / duration.Seconds()
			readThroughput := float64(totalBytesRead) / (1024 * 1024) / duration.Seconds()
			totalThroughput := float64(totalBytesWritten+totalBytesRead) / (1024 * 1024) / duration.Seconds()
			writeOpsPerSecond := float64(writeCount) / duration.Seconds()
			readOpsPerSecond := float64(readCount) / duration.Seconds()
			totalOpsPerSecond := float64(writeCount+readCount) / duration.Seconds()

			// 如果有操作，计算平均时间
			writeAvgTime := 0.0
			if writeCount > 0 {
				writeAvgTime = writeTime.Seconds() * 1000 / float64(writeCount) // 毫秒
			}
			readAvgTime := 0.0
			if readCount > 0 {
				readAvgTime = readTime.Seconds() * 1000 / float64(readCount) // 毫秒
			}
			flushAvgTime := 0.0
			if flushCount > 0 {
				flushAvgTime = flushTime.Seconds() * 1000 / float64(flushCount) // 毫秒
			}

			// 输出结果
			t.Logf("FragmentFile混合测试结果 - %s:", tc.name)
			t.Logf("  文件大小: %d MB", tc.fileSize/(1024*1024))
			t.Logf("  数据块大小: %d KB", tc.dataBlockSize/1024)
			t.Logf("  读写比例: %d%%读/%d%%写", tc.readPercentage, 100-tc.readPercentage)
			t.Logf("  总操作数: %d (写入: %d, 读取: %d)", writeCount+readCount, writeCount, readCount)
			t.Logf("  总写入数据: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  总读取数据: %.2f MB", float64(totalBytesRead)/(1024*1024))
			t.Logf("  刷新次数: %d", flushCount)
			t.Logf("  刷新间隔: 每%d次写入", tc.flushInterval)
			t.Logf("  总耗时: %v", duration)
			t.Logf("  写入总耗时: %v (%.1f%%)", writeTime, float64(writeTime)/float64(duration)*100)
			t.Logf("  读取总耗时: %v (%.1f%%)", readTime, float64(readTime)/float64(duration)*100)
			if flushCount > 0 {
				t.Logf("  刷新总耗时: %v (%.1f%%)", flushTime, float64(flushTime)/float64(duration)*100)
			}
			t.Logf("  总吞吐量: %.2f MB/s", totalThroughput)
			t.Logf("  写入吞吐量: %.2f MB/s", writeThroughput)
			t.Logf("  读取吞吐量: %.2f MB/s", readThroughput)
			t.Logf("  总操作速率: %.2f ops/s", totalOpsPerSecond)
			t.Logf("  写入操作速率: %.2f ops/s", writeOpsPerSecond)
			t.Logf("  读取操作速率: %.2f ops/s", readOpsPerSecond)
			t.Logf("  单次写入平均耗时: %.3f ms", writeAvgTime)
			t.Logf("  单次读取平均耗时: %.3f ms", readAvgTime)
			if flushCount > 0 {
				t.Logf("  单次刷新平均耗时: %.3f ms", flushAvgTime)
			}

			// 获取最终文件大小
			fileInfo, err := os.Stat(filePath)
			if err == nil {
				t.Logf("  最终文件大小: %.2f MB", float64(fileInfo.Size())/(1024*1024))
			}
		})
	}
}

package benchmark

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestFileAppendPerformance 测试标准文件追加写入性能
// 关注点：追加数据块大小、flush频率、预分配大小对性能的影响
func TestFileAppendPerformance(t *testing.T) {
	// 测试参数
	testCases := []struct {
		name           string
		preallocSize   int64  // 预分配文件大小，0表示不预分配
		totalWriteSize int64  // 总计写入数据大小
		blockSize      int    // 每次写入的数据块大小
		flushInterval  int    // 每写入多少次执行一次flush
		syncMode       string // none, flush, sync, datasync
	}{
		// 小数据块测试
		{"小块_不预分配_不flush", 0, 50 * 1024 * 1024, 4 * 1024, 0, "none"},
		{"小块_不预分配_少量flush", 0, 50 * 1024 * 1024, 4 * 1024, 100, "flush"},
		{"小块_不预分配_频繁flush", 0, 50 * 1024 * 1024, 4 * 1024, 10, "flush"},
		{"小块_预分配_不flush", 100 * 1024 * 1024, 50 * 1024 * 1024, 4 * 1024, 0, "none"},
		{"小块_预分配_少量flush", 100 * 1024 * 1024, 50 * 1024 * 1024, 4 * 1024, 100, "flush"},
		{"小块_预分配_频繁flush", 100 * 1024 * 1024, 50 * 1024 * 1024, 4 * 1024, 10, "flush"},

		// 中等数据块测试
		{"中块_不预分配_不flush", 0, 100 * 1024 * 1024, 32 * 1024, 0, "none"},
		{"中块_不预分配_少量flush", 0, 100 * 1024 * 1024, 32 * 1024, 50, "flush"},
		{"中块_不预分配_频繁flush", 0, 100 * 1024 * 1024, 32 * 1024, 10, "flush"},
		{"中块_预分配_不flush", 200 * 1024 * 1024, 100 * 1024 * 1024, 32 * 1024, 0, "none"},
		{"中块_预分配_少量flush", 200 * 1024 * 1024, 100 * 1024 * 1024, 32 * 1024, 50, "flush"},
		{"中块_预分配_频繁flush", 200 * 1024 * 1024, 100 * 1024 * 1024, 32 * 1024, 10, "flush"},

		// 大数据块测试
		{"大块_不预分配_不flush", 0, 200 * 1024 * 1024, 1 * 1024 * 1024, 0, "none"},
		{"大块_不预分配_少量flush", 0, 200 * 1024 * 1024, 1 * 1024 * 1024, 20, "flush"},
		{"大块_不预分配_频繁flush", 0, 200 * 1024 * 1024, 1 * 1024 * 1024, 5, "flush"},
		{"大块_预分配_不flush", 500 * 1024 * 1024, 200 * 1024 * 1024, 1 * 1024 * 1024, 0, "none"},
		{"大块_预分配_少量flush", 500 * 1024 * 1024, 200 * 1024 * 1024, 1 * 1024 * 1024, 20, "flush"},
		{"大块_预分配_频繁flush", 500 * 1024 * 1024, 200 * 1024 * 1024, 1 * 1024 * 1024, 5, "flush"},

		// Sync测试（保证数据持久化）
		{"小块_Sync", 0, 10 * 1024 * 1024, 4 * 1024, 50, "sync"},
		{"中块_Sync", 0, 20 * 1024 * 1024, 32 * 1024, 10, "sync"},
		{"大块_Sync", 0, 50 * 1024 * 1024, 1 * 1024 * 1024, 5, "sync"},
	}

	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("file_append_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("append_%s.data", tc.name))

			// 创建文件
			var file *os.File
			var err error

			flags := os.O_CREATE | os.O_WRONLY
			if tc.preallocSize > 0 {
				// 如果需要预分配空间，先创建文件并设置大小
				file, err = os.OpenFile(filePath, flags, 0644)
				if err != nil {
					t.Fatalf("无法创建文件: %v", err)
				}

				// 使用truncate预分配空间
				if err := file.Truncate(tc.preallocSize); err != nil {
					file.Close()
					t.Fatalf("无法预分配文件空间: %v", err)
				}

				// 关闭文件后以追加方式重新打开
				file.Close()
				flags |= os.O_APPEND
				file, err = os.OpenFile(filePath, flags, 0644)
			} else {
				// 不预分配，直接以追加模式打开
				flags |= os.O_APPEND
				file, err = os.OpenFile(filePath, flags, 0644)
			}

			if err != nil {
				t.Fatalf("无法打开文件: %v", err)
			}

			defer func() {
				// 清理资源
				if err := file.Close(); err != nil {
					t.Errorf("无法关闭文件: %v", err)
				}
			}()

			// 准备测试参数
			writeCount := int(tc.totalWriteSize) / tc.blockSize
			flushCount := 0
			totalBytesWritten := 0
			bytesSinceLastFlush := 0
			flushSizes := make([]int, 0) // 记录每次flush的数据量

			// 生成随机测试数据
			randomData := make([]byte, tc.blockSize)
			rand.Read(randomData)

			// 记录写入和刷新时间
			writeTime := time.Duration(0)
			flushTime := time.Duration(0)

			start := time.Now()

			for i := 0; i < writeCount; i++ {
				// 写入数据
				writeStart := time.Now()
				n, err := file.Write(randomData)
				writeTime += time.Since(writeStart)

				if err != nil {
					t.Fatalf("写入失败: %v", err)
				}

				totalBytesWritten += n
				bytesSinceLastFlush += n

				// 根据刷新策略执行刷新
				shouldFlush := tc.flushInterval > 0 && (i+1)%tc.flushInterval == 0

				if shouldFlush {
					flushStart := time.Now()

					switch tc.syncMode {
					case "flush":
						// 仅刷新文件缓冲区
						err = file.Sync()
					case "sync":
						// 完全同步到磁盘，包括元数据
						err = file.Sync()
					case "datasync":
						// 仅同步数据到磁盘，不包括元数据
						// 在Linux下可以使用系统调用fdatasync，这里简化为Sync
						err = file.Sync()
					}

					if err != nil {
						t.Fatalf("刷新文件失败: %v", err)
					}

					flushTime += time.Since(flushStart)
					flushSizes = append(flushSizes, bytesSinceLastFlush) // 记录本次flush的数据量
					flushCount++
					bytesSinceLastFlush = 0
				}
			}

			// 最后一次flush确保所有数据写入磁盘
			if tc.syncMode != "none" && bytesSinceLastFlush > 0 {
				flushStart := time.Now()
				if err := file.Sync(); err != nil {
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
			opsPerSecond := float64(writeCount) / duration.Seconds()
			writeAvgTime := writeTime.Seconds() * 1000 / float64(writeCount) // 毫秒
			flushAvgTime := 0.0
			if flushCount > 0 {
				flushAvgTime = flushTime.Seconds() * 1000 / float64(flushCount) // 毫秒
			}

			// 输出结果
			t.Logf("标准文件追加测试结果 - %s:", tc.name)
			t.Logf("  预分配大小: %d MB", tc.preallocSize/(1024*1024))
			t.Logf("  总写入数据: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  写入次数: %d", writeCount)
			t.Logf("  数据块大小: %d KB", tc.blockSize/1024)
			t.Logf("  刷新模式: %s", tc.syncMode)
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

// TestFileReadPerformance 测试标准文件顺序读取性能
// 关注点：读取块大小、文件大小对读取性能的影响
func TestFileReadPerformance(t *testing.T) {
	// 测试参数
	testCases := []struct {
		name          string
		fileSize      int64 // 用于测试的文件大小
		readBlockSize int   // 每次读取的数据块大小
	}{
		{"小文件_小块读取", 10 * 1024 * 1024, 4 * 1024},
		{"小文件_大块读取", 10 * 1024 * 1024, 64 * 1024},
		{"中文件_小块读取", 50 * 1024 * 1024, 4 * 1024},
		{"中文件_大块读取", 50 * 1024 * 1024, 64 * 1024},
		{"大文件_小块读取", 200 * 1024 * 1024, 4 * 1024},
		{"大文件_中块读取", 200 * 1024 * 1024, 64 * 1024},
		{"大文件_大块读取", 200 * 1024 * 1024, 1 * 1024 * 1024},
	}

	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("file_read_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("read_%s.data", tc.name))

			// 先创建并填充测试文件
			t.Logf("准备测试文件，大小: %d MB...", tc.fileSize/(1024*1024))

			// 创建文件并预分配空间
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				t.Fatalf("无法创建文件: %v", err)
			}

			// 使用truncate预分配空间
			if err := file.Truncate(tc.fileSize); err != nil {
				file.Close()
				t.Fatalf("无法预分配文件空间: %v", err)
			}

			// 生成随机数据并填充文件
			writeBlockSize := 1 * 1024 * 1024 // 使用1MB块来快速填充文件
			writeBuffer := make([]byte, writeBlockSize)
			rand.Read(writeBuffer) // 生成随机内容

			bytesWritten := int64(0)
			for bytesWritten < tc.fileSize {
				writeSize := writeBlockSize
				if bytesWritten+int64(writeSize) > tc.fileSize {
					writeSize = int(tc.fileSize - bytesWritten)
				}

				n, err := file.Write(writeBuffer[:writeSize])
				if err != nil {
					file.Close()
					t.Fatalf("写入文件失败: %v", err)
				}

				bytesWritten += int64(n)
			}

			file.Close()

			// 开始读取测试
			readFile, err := os.Open(filePath)
			if err != nil {
				t.Fatalf("无法打开文件进行读取: %v", err)
			}

			defer readFile.Close()

			readBuffer := make([]byte, tc.readBlockSize)
			totalBytesRead := 0
			readCount := 0

			start := time.Now()

			// 顺序读取整个文件
			for {
				n, err := readFile.Read(readBuffer)
				if err != nil {
					if err == io.EOF {
						break
					}
					t.Fatalf("读取文件失败: %v", err)
				}

				totalBytesRead += n
				readCount++
			}

			duration := time.Since(start)

			// 计算性能指标
			throughput := float64(totalBytesRead) / (1024 * 1024) / duration.Seconds()
			opsPerSecond := float64(readCount) / duration.Seconds()
			avgReadSize := float64(totalBytesRead) / float64(readCount) / 1024 // KB

			// 输出结果
			t.Logf("标准文件读取测试结果 - %s:", tc.name)
			t.Logf("  文件大小: %.2f MB", float64(tc.fileSize)/(1024*1024))
			t.Logf("  总读取数据: %.2f MB", float64(totalBytesRead)/(1024*1024))
			t.Logf("  读取次数: %d", readCount)
			t.Logf("  读取块大小: %d KB", tc.readBlockSize/1024)
			t.Logf("  平均实际读取: %.2f KB/次", avgReadSize)
			t.Logf("  总耗时: %v", duration)
			t.Logf("  吞吐量: %.2f MB/s", throughput)
			t.Logf("  操作速率: %.2f ops/s", opsPerSecond)
			t.Logf("  单次读取平均耗时: %.3f µs", duration.Seconds()*1000000/float64(readCount))
		})
	}
}

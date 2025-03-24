package benchmark

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/edsrzf/mmap-go"
)

const (
	// 文件头和尾部大小
	headerSize = 4 * 1024
	footerSize = 4 * 1024
)

// TestMmapWritePerformance 测试mmap写入性能
// 关注点：写入次数、数据块大小、文件大小对性能的影响
func TestMmapWritePerformance(t *testing.T) {
	// 设置测试参数
	testCases := []struct {
		name          string
		fileSize      int64
		writeCount    int
		dataBlockSize int
		flushInterval int // 每写入多少次执行一次flush
	}{
		{"小文件_小数据块_频繁flush", 10 * 1024 * 1024, 10000, 512, 100},
		{"小文件_小数据块_少量flush", 10 * 1024 * 1024, 10000, 512, 1000},
		{"小文件_大数据块_频繁flush", 50 * 1024 * 1024, 5000, 4096, 50},
		{"小文件_大数据块_少量flush", 50 * 1024 * 1024, 5000, 4096, 500},
		{"大文件_小数据块_频繁flush", 100 * 1024 * 1024, 50000, 512, 500},
		{"大文件_小数据块_少量flush", 100 * 1024 * 1024, 50000, 512, 5000},
		{"大文件_大数据块_频繁flush", 200 * 1024 * 1024, 25000, 4096, 250},
		{"大文件_大数据块_少量flush", 200 * 1024 * 1024, 25000, 4096, 2500},
	}

	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("mmap_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("test_%s.data", tc.name))

			// 创建文件
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				t.Fatalf("无法创建文件: %v", err)
			}

			// 设置文件大小
			if err := file.Truncate(tc.fileSize); err != nil {
				file.Close()
				t.Fatalf("无法设置文件大小: %v", err)
			}

			// 映射文件到内存
			mappedFile, err := mmap.MapRegion(file, int(tc.fileSize), mmap.RDWR, 0, 0)
			if err != nil {
				file.Close()
				t.Fatalf("无法映射文件: %v", err)
			}

			defer func() {
				// 清理资源
				if err := mappedFile.Unmap(); err != nil {
					t.Errorf("无法解除文件映射: %v", err)
				}
				file.Close()
			}()

			// 写入基础文件头
			copy(mappedFile[0:8], []byte("TESTMMAP"))
			binary.LittleEndian.PutUint32(mappedFile[8:12], 1) // 版本号

			// 性能测试：写入数据
			dataOffset := uint32(headerSize)
			flushCount := 0
			totalBytesWritten := 0
			bytesSinceLastFlush := 0
			flushSizes := make([]int, 0) // 记录每次flush的数据量

			// 生成随机数据
			randomData := make([]byte, tc.dataBlockSize)
			rand.Read(randomData)

			start := time.Now()

			for i := 0; i < tc.writeCount; i++ {
				// 计算所需内存
				requiredSpace := dataOffset + 4 + uint32(tc.dataBlockSize)

				// 检查是否会超出文件大小
				if requiredSpace >= uint32(tc.fileSize) {
					t.Logf("警告：数据将超出文件大小，在%d/%d次写入后停止", i, tc.writeCount)
					break
				}

				// 写入数据长度 (4字节)
				binary.LittleEndian.PutUint32(mappedFile[dataOffset:], uint32(tc.dataBlockSize))

				// 写入数据
				copy(mappedFile[dataOffset+4:], randomData)

				// 更新offset
				dataOffset += 4 + uint32(tc.dataBlockSize)
				totalBytesWritten += tc.dataBlockSize + 4
				bytesSinceLastFlush += tc.dataBlockSize + 4

				// 根据刷新间隔执行flush
				if (i+1)%tc.flushInterval == 0 {
					if err := mappedFile.Flush(); err != nil {
						t.Fatalf("刷新文件失败: %v", err)
					}
					flushSizes = append(flushSizes, bytesSinceLastFlush) // 记录本次flush的数据量
					flushCount++
					bytesSinceLastFlush = 0
				}
			}

			// 最后一次flush确保所有数据写入磁盘
			if err := mappedFile.Flush(); err != nil {
				t.Fatalf("最后刷新文件失败: %v", err)
			}
			if bytesSinceLastFlush > 0 {
				flushSizes = append(flushSizes, bytesSinceLastFlush) // 记录最后一次flush的数据量
			}
			flushCount++

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

			t.Logf("测试结果 - %s:", tc.name)
			t.Logf("  文件大小: %d MB", tc.fileSize/(1024*1024))
			t.Logf("  写入次数: %d", tc.writeCount)
			t.Logf("  数据块大小: %d 字节", tc.dataBlockSize)
			t.Logf("  总写入数据: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  刷新次数: %d", flushCount)
			t.Logf("  平均每次刷新数据量: %.2f KB", float64(avgFlushSize)/1024)
			t.Logf("  最小刷新数据量: %.2f KB", float64(minFlushSize)/1024)
			t.Logf("  最大刷新数据量: %.2f KB", float64(maxFlushSize)/1024)
			t.Logf("  耗时: %v", duration)
			t.Logf("  吞吐量: %.2f MB/s", throughput)
			t.Logf("  操作速率: %.2f ops/s", opsPerSecond)
		})
	}
}

// TestMmapFlushStrategies 测试不同的flush策略对性能的影响
func TestMmapFlushStrategies(t *testing.T) {
	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("mmap_flush_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 固定参数
	fileSize := int64(200 * 1024 * 1024) // 200MB
	dataBlockSize := 1024                // 1KB 数据块
	writeCount := 50000                  // 写50000次

	// 不同的flush策略
	strategies := []struct {
		name       string
		flushType  string
		flushParam int // 根据策略类型不同，可能表示间隔次数或字节数
	}{
		{"无flush", "none", 0},                       // 只在最后flush一次
		{"每100次写入flush", "count", 100},              // 每100次写入flush一次
		{"每500次写入flush", "count", 500},              // 每500次写入flush一次
		{"每2500次写入flush", "count", 2500},            // 每2500次写入flush一次
		{"每1MB数据flush", "bytes", 1 * 1024 * 1024},   // 每写入1MB数据flush一次
		{"每5MB数据flush", "bytes", 5 * 1024 * 1024},   // 每写入5MB数据flush一次
		{"每10MB数据flush", "bytes", 10 * 1024 * 1024}, // 每写入10MB数据flush一次
		{"每30MB数据flush", "bytes", 30 * 1024 * 1024}, // 每写入30MB数据flush一次
	}

	for _, strategy := range strategies {
		t.Run(strategy.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("flush_%s.data", strategy.name))

			// 创建文件
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				t.Fatalf("无法创建文件: %v", err)
			}

			// 设置文件大小
			if err := file.Truncate(fileSize); err != nil {
				file.Close()
				t.Fatalf("无法设置文件大小: %v", err)
			}

			// 映射文件到内存
			mappedFile, err := mmap.MapRegion(file, int(fileSize), mmap.RDWR, 0, 0)
			if err != nil {
				file.Close()
				t.Fatalf("无法映射文件: %v", err)
			}

			defer func() {
				// 清理资源
				if err := mappedFile.Unmap(); err != nil {
					t.Errorf("无法解除文件映射: %v", err)
				}
				file.Close()
			}()

			// 准备测试数据
			testData := make([]byte, dataBlockSize)
			rand.Read(testData)

			// 性能测试
			dataOffset := uint32(headerSize)
			flushCount := 0
			totalBytesWritten := 0
			bytesSinceLastFlush := 0
			flushSizes := make([]int, 0) // 记录每次flush的数据量

			start := time.Now()

			for i := 0; i < writeCount; i++ {
				// 计算所需内存
				requiredSpace := dataOffset + 4 + uint32(dataBlockSize)

				// 检查是否会超出文件大小
				if requiredSpace >= uint32(fileSize) {
					t.Logf("警告：数据将超出文件大小，在%d/%d次写入后停止", i, writeCount)
					break
				}

				// 写入长度
				binary.LittleEndian.PutUint32(mappedFile[dataOffset:], uint32(dataBlockSize))

				// 写入数据
				copy(mappedFile[dataOffset+4:], testData)

				// 更新offset和计数器
				dataOffset += 4 + uint32(dataBlockSize)
				totalBytesWritten += dataBlockSize + 4
				bytesSinceLastFlush += dataBlockSize + 4

				// 根据策略进行flush
				shouldFlush := false

				switch strategy.flushType {
				case "none":
					// 不进行中间flush
					shouldFlush = false
				case "count":
					shouldFlush = (i+1)%strategy.flushParam == 0
				case "bytes":
					shouldFlush = bytesSinceLastFlush >= strategy.flushParam
				}

				if shouldFlush {
					if err := mappedFile.Flush(); err != nil {
						t.Fatalf("刷新文件失败: %v", err)
					}
					flushSizes = append(flushSizes, bytesSinceLastFlush) // 记录本次flush的数据量
					flushCount++
					bytesSinceLastFlush = 0
				}
			}

			// 最后一次flush确保所有数据写入磁盘
			if err := mappedFile.Flush(); err != nil {
				t.Fatalf("最后刷新文件失败: %v", err)
			}
			if bytesSinceLastFlush > 0 {
				flushSizes = append(flushSizes, bytesSinceLastFlush) // 记录最后一次flush的数据量
			}
			flushCount++

			duration := time.Since(start)

			// 写入footer (模拟真实场景)
			footerOffset := fileSize - footerSize
			binary.LittleEndian.PutUint32(mappedFile[footerOffset:], uint32(writeCount))

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

			t.Logf("Flush策略测试结果 - %s:", strategy.name)
			t.Logf("  总写入数据: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  刷新次数: %d", flushCount)
			t.Logf("  平均每次刷新数据量: %.2f KB", float64(avgFlushSize)/1024)
			t.Logf("  最小刷新数据量: %.2f KB", float64(minFlushSize)/1024)
			t.Logf("  最大刷新数据量: %.2f KB", float64(maxFlushSize)/1024)
			t.Logf("  耗时: %v", duration)
			t.Logf("  吞吐量: %.2f MB/s", throughput)
			t.Logf("  每次操作平均耗时: %.3f µs", duration.Seconds()*1000000/float64(writeCount))
		})
	}
}

// TestMmapLargeBlockPerformance 测试大块数据(1MB/2MB/4MB)的写入性能
func TestMmapLargeBlockPerformance(t *testing.T) {
	// 设置测试参数
	testCases := []struct {
		name          string
		fileSize      int64
		writeCount    int
		dataBlockSize int
		flushInterval int // 每写入多少次执行一次flush
	}{
		{"大块数据_1MB块_少量flush", 500 * 1024 * 1024, 50, 1 * 1024 * 1024, 5},
		{"大块数据_1MB块_单次flush", 500 * 1024 * 1024, 50, 1 * 1024 * 1024, 50},
		{"大块数据_2MB块_少量flush", 1024 * 1024 * 1024, 30, 2 * 1024 * 1024, 5},
		{"大块数据_2MB块_单次flush", 1024 * 1024 * 1024, 30, 2 * 1024 * 1024, 30},
		{"大块数据_4MB块_少量flush", 1024 * 1024 * 1024, 20, 4 * 1024 * 1024, 5},
		{"大块数据_4MB块_单次flush", 1024 * 1024 * 1024, 20, 4 * 1024 * 1024, 20},
	}

	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("mmap_large_block_test_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("test_%s.data", tc.name))

			// 创建文件
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				t.Fatalf("无法创建文件: %v", err)
			}

			// 设置文件大小
			if err := file.Truncate(tc.fileSize); err != nil {
				file.Close()
				t.Fatalf("无法设置文件大小: %v", err)
			}

			// 映射文件到内存
			mappedFile, err := mmap.MapRegion(file, int(tc.fileSize), mmap.RDWR, 0, 0)
			if err != nil {
				file.Close()
				t.Fatalf("无法映射文件: %v", err)
			}

			defer func() {
				// 清理资源
				if err := mappedFile.Unmap(); err != nil {
					t.Errorf("无法解除文件映射: %v", err)
				}
				file.Close()
			}()

			// 写入基础文件头
			copy(mappedFile[0:8], []byte("TESTMMAP"))
			binary.LittleEndian.PutUint32(mappedFile[8:12], 1) // 版本号

			// 性能测试：写入数据
			dataOffset := uint32(headerSize)
			flushCount := 0
			totalBytesWritten := 0
			bytesSinceLastFlush := 0
			flushSizes := make([]int, 0) // 记录每次flush的数据量

			// 生成随机数据
			t.Logf("生成 %d MB 随机数据...", tc.dataBlockSize/(1024*1024))
			randomData := make([]byte, tc.dataBlockSize)
			rand.Read(randomData)

			start := time.Now()

			for i := 0; i < tc.writeCount; i++ {
				// 计算所需内存
				requiredSpace := dataOffset + 4 + uint32(tc.dataBlockSize)

				// 检查是否会超出文件大小
				if requiredSpace >= uint32(tc.fileSize) {
					t.Logf("警告：数据将超出文件大小，在%d/%d次写入后停止", i, tc.writeCount)
					break
				}

				// 写入数据块开始时间
				blockStart := time.Now()

				// 写入数据长度 (4字节)
				binary.LittleEndian.PutUint32(mappedFile[dataOffset:], uint32(tc.dataBlockSize))

				// 写入数据
				copy(mappedFile[dataOffset+4:], randomData)

				blockDuration := time.Since(blockStart)

				// 更新offset
				dataOffset += 4 + uint32(tc.dataBlockSize)
				totalBytesWritten += tc.dataBlockSize + 4
				bytesSinceLastFlush += tc.dataBlockSize + 4

				t.Logf("  第%d块写入耗时: %v", i+1, blockDuration)

				// 根据刷新间隔执行flush
				if (i+1)%tc.flushInterval == 0 {
					flushStart := time.Now()
					if err := mappedFile.Flush(); err != nil {
						t.Fatalf("刷新文件失败: %v", err)
					}
					flushDuration := time.Since(flushStart)

					flushSizes = append(flushSizes, bytesSinceLastFlush) // 记录本次flush的数据量
					t.Logf("  Flush %d 耗时: %v (数据量: %.2f MB)",
						flushCount+1, flushDuration, float64(bytesSinceLastFlush)/(1024*1024))

					flushCount++
					bytesSinceLastFlush = 0
				}
			}

			// 最后一次flush确保所有数据写入磁盘
			if bytesSinceLastFlush > 0 {
				flushStart := time.Now()
				if err := mappedFile.Flush(); err != nil {
					t.Fatalf("最后刷新文件失败: %v", err)
				}
				flushDuration := time.Since(flushStart)

				flushSizes = append(flushSizes, bytesSinceLastFlush) // 记录最后一次flush的数据量
				t.Logf("  最后一次Flush耗时: %v (数据量: %.2f MB)",
					flushDuration, float64(bytesSinceLastFlush)/(1024*1024))

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
			avgOpTime := duration.Seconds() * 1000 / float64(tc.writeCount) // 毫秒

			t.Logf("测试结果 - %s:", tc.name)
			t.Logf("  文件大小: %d MB", tc.fileSize/(1024*1024))
			t.Logf("  写入次数: %d", tc.writeCount)
			t.Logf("  数据块大小: %d MB", tc.dataBlockSize/(1024*1024))
			t.Logf("  总写入数据: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  刷新次数: %d", flushCount)
			t.Logf("  平均每次刷新数据量: %.2f MB", float64(avgFlushSize)/(1024*1024))
			t.Logf("  最小刷新数据量: %.2f MB", float64(minFlushSize)/(1024*1024))
			t.Logf("  最大刷新数据量: %.2f MB", float64(maxFlushSize)/(1024*1024))
			t.Logf("  总耗时: %v", duration)
			t.Logf("  吞吐量: %.2f MB/s", throughput)
			t.Logf("  每操作平均耗时: %.2f ms", avgOpTime)
		})
	}
}

// TestMmapMixedWritePattern 测试混合写入模式：
// 1. 从文件头部顺序写入数据块
// 2. 同时在文件尾部写入8字节索引/元数据
// 这种模式常见于日志文件或数据库，前部写数据，尾部维护索引/元数据
func TestMmapMixedWritePattern(t *testing.T) {
	// 测试参数组合
	testCases := []struct {
		name          string
		fileSize      int64
		writeCount    int
		dataBlockSize int
		flushInterval int // 每写入多少次执行一次flush
	}{
		{"小文件_小块_频繁flush", 50 * 1024 * 1024, 1000, 4 * 1024, 10},
		{"小文件_小块_少量flush", 50 * 1024 * 1024, 1000, 4 * 1024, 100},
		{"小文件_大块_频繁flush", 200 * 1024 * 1024, 500, 32 * 1024, 10},
		{"小文件_大块_少量flush", 200 * 1024 * 1024, 500, 32 * 1024, 100},
		{"大文件_小块_频繁flush", 500 * 1024 * 1024, 5000, 4 * 1024, 50},
		{"大文件_小块_少量flush", 500 * 1024 * 1024, 5000, 4 * 1024, 500},
		{"大文件_大块_频繁flush", 1024 * 1024 * 1024, 1000, 64 * 1024, 20},
		{"大文件_大块_少量flush", 1024 * 1024 * 1024, 1000, 64 * 1024, 200},
		{"大文件_很大块_频量flush", 1024 * 1024 * 1024, 500, 1024 * 1024, 10},
		{"大文件_很大块_少量flush", 1024 * 1024 * 1024, 500, 1024 * 1024, 100},
	}

	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("mmap_mixed_write_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("mixed_%s.data", tc.name))

			// 创建文件
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				t.Fatalf("无法创建文件: %v", err)
			}

			// 设置文件大小
			if err := file.Truncate(tc.fileSize); err != nil {
				file.Close()
				t.Fatalf("无法设置文件大小: %v", err)
			}

			// 映射文件到内存
			mappedFile, err := mmap.MapRegion(file, int(tc.fileSize), mmap.RDWR, 0, 0)
			if err != nil {
				file.Close()
				t.Fatalf("无法映射文件: %v", err)
			}

			defer func() {
				// 清理资源
				if err := mappedFile.Unmap(); err != nil {
					t.Errorf("无法解除文件映射: %v", err)
				}
				file.Close()
			}()

			// 写入基础文件头
			copy(mappedFile[0:8], []byte("MIXWRITE"))
			binary.LittleEndian.PutUint32(mappedFile[8:12], 1) // 版本号

			// 准备尾部元数据区域 (文件最后4KB用于元数据索引)
			footerOffset := tc.fileSize - footerSize

			// 性能测试参数
			dataOffset := uint32(headerSize)
			totalHeadWrites := 0
			totalTailWrites := 0
			totalBytesWritten := 0
			flushCount := 0
			headWriteTime := time.Duration(0)
			tailWriteTime := time.Duration(0)
			flushTime := time.Duration(0)

			// 生成随机数据块
			randomData := make([]byte, tc.dataBlockSize)
			rand.Read(randomData)

			start := time.Now()

			for i := 0; i < tc.writeCount; i++ {
				// 计算头部写入所需空间
				requiredHeadSpace := dataOffset + 4 + uint32(tc.dataBlockSize)

				// 检查是否会超出安全区域 (保留尾部区域)
				if requiredHeadSpace >= uint32(footerOffset) {
					t.Logf("警告：头部数据将超出安全区域，在%d/%d次写入后停止", i, tc.writeCount)
					break
				}

				// 1. 头部写入数据块
				headStart := time.Now()

				// 写入数据长度 (4字节)
				binary.LittleEndian.PutUint32(mappedFile[dataOffset:], uint32(tc.dataBlockSize))

				// 写入数据块
				copy(mappedFile[dataOffset+4:], randomData)

				// 更新偏移量
				dataOffset += 4 + uint32(tc.dataBlockSize)
				totalBytesWritten += tc.dataBlockSize + 4
				totalHeadWrites++

				headWriteTime += time.Since(headStart)

				// 2. 尾部写入元数据 (8字节，记录最新数据块位置)
				tailStart := time.Now()

				// 在尾部区域写入最新数据块的偏移量和序列号
				tailOffset := footerOffset + int64(i*8)%(footerSize-8)
				binary.LittleEndian.PutUint32(mappedFile[tailOffset:], dataOffset-uint32(tc.dataBlockSize)-4) // 数据块起始位置
				binary.LittleEndian.PutUint32(mappedFile[tailOffset+4:], uint32(i))                           // 数据块序列号

				totalTailWrites++
				totalBytesWritten += 8

				tailWriteTime += time.Since(tailStart)

				// 根据刷新间隔执行flush
				if (i+1)%tc.flushInterval == 0 {
					flushStart := time.Now()
					if err := mappedFile.Flush(); err != nil {
						t.Fatalf("刷新文件失败: %v", err)
					}
					flushTime += time.Since(flushStart)
					flushCount++
				}
			}

			// 最后一次flush确保所有数据写入磁盘
			flushStart := time.Now()
			if err := mappedFile.Flush(); err != nil {
				t.Fatalf("最后刷新文件失败: %v", err)
			}
			flushTime += time.Since(flushStart)
			flushCount++

			totalDuration := time.Since(start)

			// 计算性能指标
			dataThroughput := float64(totalBytesWritten) / (1024 * 1024) / totalDuration.Seconds()
			headAvgTime := headWriteTime.Seconds() * 1000 / float64(totalHeadWrites) // 毫秒
			tailAvgTime := tailWriteTime.Seconds() * 1000 / float64(totalTailWrites) // 毫秒
			flushAvgTime := flushTime.Seconds() * 1000 / float64(flushCount)         // 毫秒

			// 输出结果
			t.Logf("混合写入模式测试结果 - %s:", tc.name)
			t.Logf("  文件大小: %d MB", tc.fileSize/(1024*1024))
			t.Logf("  数据块大小: %d KB", tc.dataBlockSize/1024)
			t.Logf("  写入次数: %d (头部), %d (尾部)", totalHeadWrites, totalTailWrites)
			t.Logf("  总写入数据: %.2f MB", float64(totalBytesWritten)/(1024*1024))
			t.Logf("  刷新次数: %d", flushCount)
			t.Logf("  刷新间隔: 每%d次写入", tc.flushInterval)
			t.Logf("  总耗时: %v", totalDuration)
			t.Logf("  头部写入总耗时: %v (%.1f%%)", headWriteTime, float64(headWriteTime)/float64(totalDuration)*100)
			t.Logf("  尾部写入总耗时: %v (%.1f%%)", tailWriteTime, float64(tailWriteTime)/float64(totalDuration)*100)
			t.Logf("  刷新总耗时: %v (%.1f%%)", flushTime, float64(flushTime)/float64(totalDuration)*100)
			t.Logf("  吞吐量: %.2f MB/s", dataThroughput)
			t.Logf("  头部单次写入平均耗时: %.3f ms", headAvgTime)
			t.Logf("  尾部单次写入平均耗时: %.3f ms", tailAvgTime)
			t.Logf("  单次刷新平均耗时: %.3f ms", flushAvgTime)
		})
	}
}

// TestMmapWritePerformanceDetailed 测试mmap写入性能的详细指标
// 特别关注copy和flush操作的耗时分布
func TestMmapWritePerformanceDetailed(t *testing.T) {
	// 测试参数组合
	testCases := []struct {
		name          string
		fileSize      int64
		writeCount    int
		dataBlockSize int
		flushInterval int // 每写入多少次执行一次flush
	}{
		{"小数据块_4KB", 1024 * 1024 * 1024, 100000, 4 * 1024, 1000},
		{"中数据块_64KB", 1024 * 1024 * 1024, 10000, 64 * 1024, 100},
		{"大数据块_1MB", 1024 * 1024 * 1024, 1000, 1 * 1024 * 1024, 10},
		{"超大数据块_4MB", 4 * 1024 * 1024 * 1024, 250, 4 * 1024 * 1024, 5},
		{"超大数据块_16MB", 4 * 1024 * 1024 * 1024, 62, 16 * 1024 * 1024, 2},
		{"极限数据块_64MB", 8 * 1024 * 1024 * 1024, 16, 64 * 1024 * 1024, 1},
	}

	// 创建临时目录
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("mmap_detailed_perf_%d", time.Now().UnixNano()))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filePath := filepath.Join(tempDir, fmt.Sprintf("detailed_%s.data", tc.name))

			// 测量文件创建时间
			fileCreateStart := time.Now()
			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
			if err != nil {
				t.Fatalf("无法创建文件: %v", err)
			}

			// 设置文件大小
			truncateStart := time.Now()
			fileCreateTime := truncateStart.Sub(fileCreateStart)

			if err := file.Truncate(tc.fileSize); err != nil {
				file.Close()
				t.Fatalf("无法设置文件大小: %v", err)
			}
			truncateTime := time.Since(truncateStart)

			// 测量映射时间
			mmapStart := time.Now()
			mappedFile, err := mmap.MapRegion(file, int(tc.fileSize), mmap.RDWR, 0, 0)
			if err != nil {
				file.Close()
				t.Fatalf("无法映射文件: %v", err)
			}
			mmapTime := time.Since(mmapStart)

			defer func() {
				// 测量解除映射时间
				unmapStart := time.Now()
				if err := mappedFile.Unmap(); err != nil {
					t.Errorf("无法解除文件映射: %v", err)
				}
				unmapTime := time.Since(unmapStart)
				t.Logf("  解除映射耗时: %v", unmapTime)
				file.Close()
			}()

			// 生成随机数据
			dataGenStart := time.Now()
			randomData := make([]byte, tc.dataBlockSize)
			rand.Read(randomData)
			dataGenTime := time.Since(dataGenStart)

			// 数据写入统计
			copyTimes := make([]time.Duration, 0, tc.writeCount)
			flushTimes := make([]time.Duration, 0, tc.writeCount/tc.flushInterval+1)
			totalBytesWritten := 0
			flushCount := 0

			// 数据结构存储写入子操作耗时
			type WriteOpTimes struct {
				lengthWriteTime time.Duration
				dataCopyTime    time.Duration
				totalWriteTime  time.Duration
			}
			writeOpTimes := make([]WriteOpTimes, 0, tc.writeCount)

			// 写入数据的起始偏移
			dataOffset := uint32(headerSize)

			// 测量总体写入时间
			totalWriteStart := time.Now()

			for i := 0; i < tc.writeCount; i++ {
				opStart := time.Now()

				// 计算所需内存
				requiredSpace := dataOffset + 4 + uint32(tc.dataBlockSize)

				// 检查是否会超出文件大小
				if requiredSpace >= uint32(tc.fileSize) {
					t.Logf("警告：数据将超出文件大小，在%d/%d次写入后停止", i, tc.writeCount)
					break
				}

				// 测量写入长度字段的时间
				lengthWriteStart := time.Now()
				binary.LittleEndian.PutUint32(mappedFile[dataOffset:], uint32(tc.dataBlockSize))
				lengthWriteTime := time.Since(lengthWriteStart)

				// 测量数据复制时间
				copyStart := time.Now()
				copy(mappedFile[dataOffset+4:], randomData)
				copyTime := time.Since(copyStart)
				copyTimes = append(copyTimes, copyTime)

				// 更新统计
				writeOpTimes = append(writeOpTimes, WriteOpTimes{
					lengthWriteTime: lengthWriteTime,
					dataCopyTime:    copyTime,
					totalWriteTime:  time.Since(opStart),
				})

				// 更新offset和计数器
				dataOffset += 4 + uint32(tc.dataBlockSize)
				totalBytesWritten += tc.dataBlockSize + 4

				// 根据刷新间隔执行flush
				if (i+1)%tc.flushInterval == 0 || i == tc.writeCount-1 {
					flushStart := time.Now()
					if err := mappedFile.Flush(); err != nil {
						t.Fatalf("刷新文件失败: %v", err)
					}
					flushTime := time.Since(flushStart)
					flushTimes = append(flushTimes, flushTime)
					flushCount++
				}
			}

			totalWriteTime := time.Since(totalWriteStart)

			// 计算性能指标
			bytesPerSecond := float64(0)
			mbPerSecond := float64(0)
			if totalWriteTime > 0 {
				bytesPerSecond = float64(totalBytesWritten) / totalWriteTime.Seconds()
				mbPerSecond = bytesPerSecond / (1024 * 1024)
			}

			// 计算copy和flush时间统计
			var totalCopyTime time.Duration
			var totalFlushTime time.Duration
			var minCopyTime, maxCopyTime time.Duration
			var minFlushTime, maxFlushTime time.Duration

			if len(copyTimes) > 0 {
				minCopyTime = copyTimes[0]
				maxCopyTime = copyTimes[0]
				for _, t := range copyTimes {
					totalCopyTime += t
					if t < minCopyTime {
						minCopyTime = t
					}
					if t > maxCopyTime {
						maxCopyTime = t
					}
				}
			}

			if len(flushTimes) > 0 {
				minFlushTime = flushTimes[0]
				maxFlushTime = flushTimes[0]
				for _, t := range flushTimes {
					totalFlushTime += t
					if t < minFlushTime {
						minFlushTime = t
					}
					if t > maxFlushTime {
						maxFlushTime = t
					}
				}
			}

			avgCopyTimeNs := int64(0)
			if len(copyTimes) > 0 {
				avgCopyTimeNs = totalCopyTime.Nanoseconds() / int64(len(copyTimes))
			}

			avgFlushTimeMs := float64(0)
			if len(flushTimes) > 0 {
				avgFlushTimeMs = float64(totalFlushTime.Milliseconds()) / float64(len(flushTimes))
			}

			copyThroughputMBs := float64(0)
			if totalCopyTime > 0 {
				copyThroughputMBs = float64(totalBytesWritten) / (1024 * 1024) / totalCopyTime.Seconds()
			}

			// 计算每秒写入操作数 (IOPS)
			iops := float64(0)
			if totalWriteTime > 0 {
				iops = float64(len(copyTimes)) / totalWriteTime.Seconds()
			}

			// 计算copy操作分布
			copyNanosP50 := calculatePercentile(copyTimes, 50)
			copyNanosP95 := calculatePercentile(copyTimes, 95)
			copyNanosP99 := calculatePercentile(copyTimes, 99)

			// 计算flush操作分布
			flushMsP50 := float64(calculatePercentile(flushTimes, 50)) / float64(time.Millisecond)
			flushMsP95 := float64(calculatePercentile(flushTimes, 95)) / float64(time.Millisecond)
			flushMsP99 := float64(calculatePercentile(flushTimes, 99)) / float64(time.Millisecond)

			// 输出详细性能报告
			t.Logf("======== mmap写入性能详细测试 - %s ========", tc.name)
			t.Logf("初始化阶段:")
			t.Logf("  文件创建耗时: %v", fileCreateTime)
			t.Logf("  文件truncate耗时: %v", truncateTime)
			t.Logf("  mmap映射耗时: %v", mmapTime)
			t.Logf("  测试数据生成耗时: %v", dataGenTime)
			t.Logf("\n文件和数据信息:")
			t.Logf("  文件大小: %d MB", tc.fileSize/(1024*1024))
			t.Logf("  数据块大小: %s", formatSize(int64(tc.dataBlockSize)))
			t.Logf("  写入次数: %d", len(copyTimes))
			t.Logf("  总写入数据: %s", formatSize(int64(totalBytesWritten)))
			t.Logf("  刷新次数: %d", flushCount)
			t.Logf("\n性能总结:")
			t.Logf("  总写入耗时: %v", totalWriteTime)
			t.Logf("  平均写入吞吐量: %.2f MB/s", mbPerSecond)
			t.Logf("  每秒IO操作数(IOPS): %.2f", iops)
			t.Logf("\ncopy操作性能分析:")
			t.Logf("  copy操作总耗时: %v (占总时间的 %.1f%%)", totalCopyTime, float64(totalCopyTime)/float64(totalWriteTime)*100)
			t.Logf("  平均单次copy耗时: %d ns", avgCopyTimeNs)
			t.Logf("  copy操作延迟分布: P50=%d ns, P95=%d ns, P99=%d ns",
				copyNanosP50, copyNanosP95, copyNanosP99)
			t.Logf("  copy操作吞吐量: %.2f MB/s", copyThroughputMBs)
			t.Logf("  最短copy耗时: %v", minCopyTime)
			t.Logf("  最长copy耗时: %v", maxCopyTime)
			t.Logf("\nflush操作性能分析:")
			t.Logf("  flush操作总耗时: %v (占总时间的 %.1f%%)", totalFlushTime, float64(totalFlushTime)/float64(totalWriteTime)*100)
			t.Logf("  平均单次flush耗时: %.2f ms", avgFlushTimeMs)
			t.Logf("  flush操作延迟分布: P50=%.2f ms, P95=%.2f ms, P99=%.2f ms",
				flushMsP50, flushMsP95, flushMsP99)
			t.Logf("  最短flush耗时: %v", minFlushTime)
			t.Logf("  最长flush耗时: %v", maxFlushTime)

			// 修复除以零错误
			avgFlushDataSize := "0 bytes"
			if flushCount > 0 {
				avgFlushDataSize = formatSize(int64(totalBytesWritten / flushCount))
			}
			t.Logf("  平均每次flush的数据量: %s", avgFlushDataSize)
			t.Logf("\n结论:")

			if copyThroughputMBs > 3000 {
				t.Logf("  内存复制速度非常快(%.2f MB/s)，接近理论内存带宽限制", copyThroughputMBs)
			} else if copyThroughputMBs > 1000 {
				t.Logf("  内存复制速度良好(%.2f MB/s)", copyThroughputMBs)
			} else {
				t.Logf("  内存复制速度较慢(%.2f MB/s)，可能存在瓶颈", copyThroughputMBs)
			}

			// 分析总体性能瓶颈
			copyPercentage := float64(0)
			flushPercentage := float64(0)
			otherPercentage := float64(0)

			if totalWriteTime > 0 {
				copyPercentage = float64(totalCopyTime) / float64(totalWriteTime) * 100
				flushPercentage = float64(totalFlushTime) / float64(totalWriteTime) * 100
				otherPercentage = 100 - copyPercentage - flushPercentage
			}

			t.Logf("  总体性能分布: copy操作占比%.1f%%, flush操作占比%.1f%%, 其他操作占比%.1f%%",
				copyPercentage, flushPercentage, otherPercentage)

			if flushPercentage > 50 {
				t.Logf("  性能瓶颈: flush操作是主要瓶颈，减少flush频率可能会提高整体性能")
			} else if copyPercentage > 50 {
				t.Logf("  性能瓶颈: 内存复制是主要瓶颈，考虑使用更大的块大小或优化内存访问模式")
			} else {
				t.Logf("  性能分布均衡，没有明显瓶颈")
			}
		})
	}
}

// calculatePercentile 计算持续时间切片的百分位数 (返回纳秒)
func calculatePercentile(durations []time.Duration, percentile int) int64 {
	if len(durations) == 0 {
		return 0
	}

	// 复制切片以避免修改原始数据
	durationsCopy := make([]time.Duration, len(durations))
	copy(durationsCopy, durations)

	// 转换为纳秒并排序
	nanoseconds := make([]int64, len(durationsCopy))
	for i, d := range durationsCopy {
		nanoseconds[i] = d.Nanoseconds()
	}
	sort.Slice(nanoseconds, func(i, j int) bool { return nanoseconds[i] < nanoseconds[j] })

	// 计算百分位数
	index := int(float64(len(nanoseconds)-1) * float64(percentile) / 100.0)
	return nanoseconds[index]
}

// formatSize 将字节数格式化为人类可读的大小
func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}

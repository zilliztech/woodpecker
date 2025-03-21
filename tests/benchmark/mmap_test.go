package benchmark

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
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

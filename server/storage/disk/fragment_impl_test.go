package disk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
)

func TestNewFragmentFile(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建新的FragmentFile
	ff, err := NewFragmentFile(filePath, 1024*1024, 1, 100) // 1MB文件，fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)
	assert.Equal(t, int64(1), ff.GetFragmentId())

	// 验证文件是否存在
	_, err = os.Stat(filePath)
	assert.NoError(t, err)

	// 验证文件大小
	info, err := os.Stat(filePath)
	assert.NoError(t, err)
	assert.Equal(t, int64(1024*1024), info.Size())
}

func TestFragmentFile_WriteAndRead(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建新的FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFile(filePath, 1024*1024, 1, startEntryID) // 1MB文件，fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 写入测试数据
	testData := []byte("test data")
	err = ff.Write(context.Background(), testData, startEntryID)
	assert.NoError(t, err)

	// 读取数据
	data, err := ff.GetEntry(startEntryID)
	assert.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestFragmentFile_MultipleEntries(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建新的FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFile(filePath, 1024*1024, 1, startEntryID) // 1MB文件，fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 写入多个测试数据
	testData1 := []byte("test data 1")
	testData2 := []byte("test data 2")
	testData3 := []byte("test data 3")

	// 写入数据
	err = ff.Write(context.Background(), testData1, startEntryID)
	assert.NoError(t, err)
	err = ff.Write(context.Background(), testData2, startEntryID+1)
	assert.NoError(t, err)
	err = ff.Write(context.Background(), testData3, startEntryID+2)
	assert.NoError(t, err)

	// 刷新数据
	err = ff.Flush(context.Background())
	assert.NoError(t, err)

	// 检查条目ID范围
	firstID, err := ff.GetFirstEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID, firstID)

	lastID, err := ff.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID+2, lastID)

	t.Logf("条目ID范围: %d 到 %d", firstID, lastID)

	// 获取并输出实际存储顺序 (调试信息)
	for i := firstID; i <= lastID; i++ {
		data, err := ff.GetEntry(i)
		if err != nil {
			t.Logf("读取条目 %d 失败: %v", i, err)
		} else {
			t.Logf("条目 %d: %s", i, string(data))
		}
		assert.NoError(t, err)
	}

	// 根据实际存储顺序验证数据
	data1, err := ff.GetEntry(startEntryID)
	assert.NoError(t, err)
	assert.Equal(t, testData1, data1, "第一个条目应该是testData1")

	data2, err := ff.GetEntry(startEntryID + 1)
	assert.NoError(t, err)
	assert.Equal(t, testData2, data2, "第二个条目应该是testData2")

	data3, err := ff.GetEntry(startEntryID + 2)
	assert.NoError(t, err)
	assert.Equal(t, testData3, data3, "第三个条目应该是testData3")
}

func TestFragmentFile_LoadAndReload(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建新的FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFile(filePath, 1024*1024, 1, startEntryID) // 1MB文件，fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 写入测试数据
	testData := []byte("test data")
	err = ff.Write(context.Background(), testData, startEntryID)
	assert.NoError(t, err)

	// 刷新数据到磁盘（这一步很重要）
	err = ff.Flush(context.Background())
	assert.NoError(t, err)

	// 关闭文件
	err = ff.Release()
	assert.NoError(t, err)

	// 重新打开文件
	ff2, err := NewROFragmentFile(filePath, 1024*1024, 1) // firstEntryID会被忽略，从文件加载
	assert.NoError(t, err)
	assert.NotNil(t, ff2)

	// 加载数据
	err = ff2.Load(context.Background())
	assert.NoError(t, err)

	// 检查加载的 firstEntryID
	firstID, err := ff2.GetFirstEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID, firstID)

	lastID, err := ff2.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID, lastID)

	// 读取数据
	data, err := ff2.GetEntry(startEntryID)
	assert.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestFragmentFileLargeWriteAndRead(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建新的FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFile(filePath, 128*1024*1024, 1, startEntryID) // 1MB文件，fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 写入测试数据
	baseData := make([]byte, 1*1024*1024)
	for i := 0; i < 120; i++ {
		testData := []byte(fmt.Sprintf("test data_%d", i))
		testData = append(testData, baseData...)
		err = ff.Write(context.Background(), testData, startEntryID+int64(i))
		assert.NoError(t, err)
	}

	// 刷新数据到磁盘（这一步很重要）
	err = ff.Flush(context.Background())
	assert.NoError(t, err)

	// 关闭文件
	err = ff.Release()
	assert.NoError(t, err)

	// 重新打开文件
	ff2, err := NewROFragmentFile(filePath, 128*1024*1024, 1) // firstEntryID会被忽略，从文件加载
	assert.NoError(t, err)
	assert.NotNil(t, ff2)

	// 加载数据
	err = ff2.Load(context.Background())
	assert.NoError(t, err)

	// 检查加载的 firstEntryID
	firstID, err := ff2.GetFirstEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID, firstID)

	lastID, err := ff2.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID+119, lastID)

	// 读取数据
	for i := 0; i < 120; i++ {
		data, err := ff2.GetEntry(startEntryID + int64(i))
		assert.NoError(t, err)
		expected := append([]byte(fmt.Sprintf("test data_%d", i)), baseData...)
		assert.Equal(t, expected, data)
	}
}

func TestFragmentFile_CRCValidation(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建新的FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFile(filePath, 1024*1024, 1, startEntryID) // 1MB文件，fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 写入测试数据
	testData := []byte("test data")
	err = ff.Write(context.Background(), testData, startEntryID)
	assert.NoError(t, err)

	// 修改文件中的数据（破坏CRC）
	ff.mappedFile[ff.dataOffset-2] = 'X' // 修改数据的一部分

	// 尝试读取数据
	_, err = ff.GetEntry(startEntryID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CRC mismatch")
}

func TestFragmentFile_OutOfSpace(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建一个小文件，使得写入大数据时空间不足
	// 文件结构:
	// - headerSize(4KB): 文件头
	// - dataArea: 实际数据存储区域
	// - indexArea: 索引区域，每个条目占4字节(indexItemSize)
	// - footerSize(4KB): 文件尾部
	fileSize := int64(12*1024 + 1024) // 13KB

	// 创建新的FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFile(filePath, fileSize, 1, startEntryID)
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 写入一些小数据，确保能够成功
	smallData := []byte("small test data")
	err = ff.Write(context.Background(), smallData, int64(startEntryID))
	assert.NoError(t, err)

	// 尝试写入大数据，应该导致空间不足错误
	largeData := make([]byte, 5*1024) // 5KB
	err = ff.Write(context.Background(), largeData, int64(startEntryID+1))
	assert.Error(t, err)
	assert.True(t, werr.ErrDiskFragmentNoSpace.Is(err))
}

func TestFragmentFile_InvalidEntryId(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建新的FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFile(filePath, 1024*1024, 1, startEntryID) // 1MB文件，fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 尝试读取不存在的条目
	_, err = ff.GetEntry(startEntryID - 1) // 小于起始ID
	assert.Error(t, err)
	assert.True(t, werr.ErrInvalidEntryId.Is(err))
	assert.Contains(t, err.Error(), "not in the range ")

	_, err = ff.GetEntry(startEntryID + 1) // 大于已有条目ID
	assert.Error(t, err)
	assert.True(t, werr.ErrInvalidEntryId.Is(err))
	assert.Contains(t, err.Error(), "not in the range ")
}

func TestFragmentFile_Release(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建新的FragmentFile
	startEntryID := int64(100)
	ff, err := NewFragmentFile(filePath, 1024*1024, 1, startEntryID) // 1MB文件，fragmentId=1, firstEntryID=100
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 写入测试数据
	testData := []byte("test data")
	err = ff.Write(context.Background(), testData, int64(0))
	assert.NoError(t, err)

	// 释放资源
	ff.Close()

	// 尝试在释放后写入数据
	err = ff.Write(context.Background(), testData, int64(1))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fragment file is closed")
}

func TestFragmentFile_ConcurrentReadWrite(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "concurrent_test.frag")

	// 创建新的FragmentFile
	startEntryID := int64(100)
	fileSize := int64(10 * 1024 * 1024) // 10MB
	ff, err := NewFragmentFile(filePath, fileSize, 1, startEntryID)
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 写入的条目数量
	entryCount := 100
	// 同步通道，用于协调读写操作
	entryWritten := make(chan int64, entryCount)
	done := make(chan struct{})
	ctx := context.Background()

	// 启动写入goroutine
	go func() {
		defer close(entryWritten)

		for i := 0; i < entryCount; i++ {
			entryID := startEntryID + int64(i)
			testData := []byte(fmt.Sprintf("concurrent test data %d", i))

			// 写入数据
			err := ff.Write(ctx, testData, entryID)
			if err != nil {
				t.Logf("写入条目 %d 失败: %v", entryID, err)
				return
			}

			// 每写入10个条目，刷新一次数据到磁盘
			if i > 0 && i%10 == 0 {
				err = ff.Flush(ctx)
				if err != nil {
					t.Logf("刷新数据失败: %v", err)
					return
				}
				t.Logf("已刷新数据到条目 %d", entryID)
			}

			// 通知读取goroutine有新的条目写入
			entryWritten <- entryID
			// 短暂休眠，让读取goroutine有机会读取
			time.Sleep(10 * time.Millisecond)
		}

		// 最后一次刷新，确保所有数据都写入磁盘
		err = ff.Flush(ctx)
		if err != nil {
			t.Logf("最终刷新数据失败: %v", err)
		}

		t.Logf("写入完成，共写入 %d 个条目", entryCount)
	}()

	// 启动读取goroutine
	go func() {
		defer close(done)

		readCount := 0
		lastReadID := startEntryID - 1

		for entryID := range entryWritten {
			// 尝试读取新写入的条目及之前可能未读取的条目
			for readID := lastReadID + 1; readID <= entryID; readID++ {
				expectedData := []byte(fmt.Sprintf("concurrent test data %d", int(readID-startEntryID)))

				// 重试几次读取，因为写入可能需要一点时间才能被读取到
				var readData []byte
				var readErr error
				success := false

				for attempt := 0; attempt < 5; attempt++ {
					readData, readErr = ff.GetEntry(readID)
					if readErr == nil {
						// 验证读取的数据是否正确
						if !assert.Equal(t, expectedData, readData, "条目 %d 的数据不匹配", readID) {
							t.Logf("条目 %d 读取成功但数据不匹配: 期望=%s, 实际=%s",
								readID, string(expectedData), string(readData))
						} else {
							t.Logf("条目 %d 读取成功: %s", readID, string(readData))
							readCount++
							success = true
						}
						break
					}

					if strings.Contains(readErr.Error(), "out of range") {
						// 条目可能尚未对读取可见，等待一会再试
						time.Sleep(20 * time.Millisecond)
						continue
					} else {
						// 其他错误，直接报告
						t.Logf("读取条目 %d 失败: %v", readID, readErr)
						break
					}
				}

				if !success {
					t.Logf("尝试5次后仍无法读取条目 %d: %v", readID, readErr)
				}

				lastReadID = readID
			}
		}

		t.Logf("读取完成，成功读取 %d 个条目", readCount)
		// 验证最终读取数量是否符合预期
		assert.Equal(t, entryCount, readCount, "读取的条目数量不等于写入的条目数量")
	}()

	// 等待读取完成
	<-done

	// 释放资源
	err = ff.Release()
	assert.NoError(t, err)

	// 验证最终写入的数据量
	firstID, err := ff.GetFirstEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID, firstID)

	err = ff.Load(ctx)
	assert.NoError(t, err)

	lastID, err := ff.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID+int64(entryCount-1), lastID)
}

func TestFragmentFile_ConcurrentReadWriteDifferentInstances(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "concurrent_diff_inst.frag")

	// 创建用于写入的FragmentFile
	startEntryID := int64(100)
	fileSize := int64(10 * 1024 * 1024) // 10MB
	writerFF, err := NewFragmentFile(filePath, fileSize, 1, startEntryID)
	assert.NoError(t, err)
	assert.NotNil(t, writerFF)

	// 创建用于读取的FragmentFile（同一文件的不同实例）
	readerFF, err := NewROFragmentFile(filePath, fileSize, 1)
	assert.NoError(t, err)
	assert.NotNil(t, readerFF)

	// 先加载读取实例
	err = readerFF.Load(context.Background()) // TODO test an other case: assert load error the first time, because invalid header
	assert.NoError(t, err)

	// 写入的条目数量
	entryCount := 100
	// 同步通道，用于协调读写操作
	entryWritten := make(chan int64, entryCount)
	done := make(chan struct{})
	ctx := context.Background()

	// 启动写入goroutine
	go func() {
		defer close(entryWritten)

		for i := 0; i < entryCount; i++ {
			entryID := startEntryID + int64(i)
			testData := []byte(fmt.Sprintf("concurrent different instances test data %d", i))

			// 写入数据
			err := writerFF.Write(ctx, testData, entryID)
			if err != nil {
				t.Logf("写入条目 %d 失败: %v", entryID, err)
				return
			}

			// 每写入5个条目，刷新一次数据到磁盘
			if i > 0 && i%5 == 0 {
				err = writerFF.Flush(ctx)
				if err != nil {
					t.Logf("刷新数据失败: %v", err)
					return
				}
				t.Logf("已刷新数据到条目 %d", entryID)
			}

			// 通知读取goroutine有新的条目写入
			entryWritten <- entryID
			// 短暂休眠，让读取goroutine有机会读取
			time.Sleep(10 * time.Millisecond)
		}

		// 最后一次刷新，确保所有数据都写入磁盘
		err = writerFF.Flush(ctx)
		if err != nil {
			t.Logf("最终刷新数据失败: %v", err)
		}

		t.Logf("写入完成，共写入 %d 个条目", entryCount)
	}()

	// 启动读取goroutine
	go func() {
		defer close(done)

		readCount := 0
		lastReadID := startEntryID - 1

		for entryID := range entryWritten {
			// 尝试读取新写入的条目
			for readID := lastReadID + 1; readID <= entryID; readID++ {
				expectedData := []byte(fmt.Sprintf("concurrent different instances test data %d", int(readID-startEntryID)))

				// 重试几次读取，因为写入需要时间才能对另一个mmap实例可见
				var readData []byte
				var readErr error
				success := false

				for attempt := 0; attempt < 10; attempt++ {
					readData, readErr = readerFF.GetEntry(readID)
					if readErr == nil {
						// 验证读取的数据是否正确
						if !assert.Equal(t, expectedData, readData, "条目 %d 的数据不匹配", readID) {
							t.Logf("条目 %d 读取成功但数据不匹配: 期望=%s, 实际=%s",
								readID, string(expectedData), string(readData))
						} else {
							t.Logf("条目 %d 读取成功: %s", readID, string(readData))
							readCount++
							success = true
						}
						break
					}
					// 错误处理和重试逻辑
					t.Logf("尝试 %d: 读取条目 %d 失败: %v", attempt+1, readID, readErr)
					time.Sleep(50 * time.Millisecond)
				}

				if !success {
					t.Logf("尝试10次后仍无法读取条目 %d", readID)
				}

				lastReadID = readID
			}
		}

		t.Logf("读取完成，成功读取 %d 个条目", readCount)
		// 验证最终读取数量是否符合预期
		assert.Equal(t, entryCount, readCount, "读取的条目数量不等于写入的条目数量")
	}()

	// 等待读取完成
	<-done

	// 释放资源
	err = writerFF.Release()
	assert.NoError(t, err)

	err = readerFF.Release()
	assert.NoError(t, err)
}

func TestFragmentFile_WriteAndReadMultipleEntries(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_multiple.frag")

	// Create new FragmentFile
	startEntryID := int64(1000)
	fileSize := int64(1 * 1024 * 1024) // 1MB
	ff, err := NewFragmentFile(filePath, fileSize, 1, startEntryID)
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// Write 10 entries
	entryCount := 10
	writtenData := make([][]byte, entryCount)
	ctx := context.Background()

	t.Log("Writing 10 entries...")
	for i := 0; i < entryCount; i++ {
		entryID := startEntryID + int64(i)
		// Each entry has different content and size
		data := []byte(fmt.Sprintf("Test data #%d - Random content: %d", i, time.Now().UnixNano()))
		writtenData[i] = data

		err := ff.Write(ctx, data, entryID)
		assert.NoError(t, err, "Failed to write entry %d", entryID)
		t.Logf("Wrote entry %d: %s", entryID, string(data))
	}

	// Flush data to disk
	err = ff.Flush(ctx)
	assert.NoError(t, err, "Failed to flush data")
	t.Log("Data flushed to disk")

	// Verify first and last entry IDs
	firstID, err := ff.GetFirstEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID, firstID, "First entry ID mismatch")

	lastID, err := ff.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startEntryID+int64(entryCount-1), lastID, "Last entry ID mismatch")

	// Read and verify each entry
	t.Log("Reading and verifying 10 entries...")
	for i := 0; i < entryCount; i++ {
		entryID := startEntryID + int64(i)
		data, err := ff.GetEntry(entryID)
		assert.NoError(t, err, "Failed to read entry %d", entryID)

		// Verify data content matches
		assert.Equal(t, writtenData[i], data, "Data mismatch for entry %d", entryID)
		t.Logf("Read entry %d: %s", entryID, string(data))
	}
	t.Log("All entries verified")

	// Try to read non-existent entries
	_, err = ff.GetEntry(startEntryID - 1)
	assert.Error(t, err, "Should not be able to read entry outside range")

	_, err = ff.GetEntry(startEntryID + int64(entryCount))
	assert.Error(t, err, "Should not be able to read entry outside range")

	// Release and reload test
	t.Log("Releasing and reloading file...")
	err = ff.Release()
	assert.NoError(t, err, "Failed to release resources")

	// Reopen file
	ff2, err := NewROFragmentFile(filePath, fileSize, 1)
	assert.NoError(t, err)
	assert.NotNil(t, ff2)

	// Load data
	err = ff2.Load(ctx)
	assert.NoError(t, err, "Failed to load data")

	// Verify data again
	t.Log("Verifying data after reload...")
	for i := 0; i < entryCount; i++ {
		entryID := startEntryID + int64(i)
		data, err := ff2.GetEntry(entryID)
		assert.NoError(t, err, "Failed to read entry %d after reload", entryID)

		// Verify data content matches
		assert.Equal(t, writtenData[i], data, "Data mismatch for entry %d after reload", entryID)
	}
	t.Log("All entries verified after reload")

	// Final cleanup
	err = ff2.Release()
	assert.NoError(t, err, "Failed to release resources")
}

func TestFragmentFile_AlternatingWriteFlushRead(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	// Create temporary directory
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "alternating_test.frag")

	// Create new FragmentFile
	startEntryID := int64(500)
	fileSize := int64(1 * 1024 * 1024) // 1MB
	ff, err := NewFragmentFile(filePath, fileSize, 1, startEntryID)
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// open read file
	ff2, err := NewROFragmentFile(filePath, fileSize, 1)
	assert.NoError(t, err)
	assert.NotNil(t, ff2)

	// Number of test entries
	entryCount := 15
	ctx := context.Background()

	t.Log("Starting alternating write/read test...")

	// Store all written data for final verification
	allWrittenData := make([][]byte, entryCount)

	// Alternating write and read
	for i := 0; i < entryCount; i++ {
		entryID := startEntryID + int64(i)

		// 1. Write a piece of data
		testData := []byte(fmt.Sprintf("data#%d", entryID))
		allWrittenData[i] = testData

		t.Logf("Step %d.1: Writing entry %d", i+1, entryID)
		err = ff.Write(ctx, testData, entryID)
		assert.NoError(t, err, "Failed to write entry %d", entryID)

		// 2. Flush data immediately
		t.Logf("Step %d.2: Flushing entry %d", i+1, entryID)
		err = ff.Flush(ctx)
		assert.NoError(t, err, "Failed to flush entry %d", entryID)

		// 3. Read and verify the data just written
		t.Logf("Step %d.3: Reading entry %d", i+1, entryID)
		readData, err := ff2.GetEntry(entryID)
		assert.NoError(t, err, "Failed to read entry %d", entryID)
		assert.Equal(t, testData, readData, "Data mismatch for entry %d", entryID)
		t.Logf("Entry %d verified successfully: %s", entryID, string(readData))

		// 4. Ensure lastEntryID is updated correctly
		t.Logf("Step %d.4: Ensure lastEntryID is updated correctly", i+1)
		lastID, err := ff2.GetLastEntryId()
		assert.NoError(t, err, "Failed to get last entry ID")
		assert.Equal(t, entryID, lastID, "Last entry ID mismatch")

		// Add a small delay for test clarity
		time.Sleep(10 * time.Millisecond)
	}

	t.Log("Alternating write/read test completed")

	// Final verification of all entries
	t.Log("Performing final verification of all entries...")
	for i := 0; i < entryCount; i++ {
		entryID := startEntryID + int64(i)
		data, err := ff2.GetEntry(entryID)
		assert.NoError(t, err, "Final verification: Failed to read entry %d", entryID)
		assert.Equal(t, allWrittenData[i], data, "Final verification: Data mismatch for entry %d", entryID)
	}

	// Reload test
	t.Log("Releasing and reloading file for verification...")
	err = ff2.Release()
	assert.NoError(t, err, "Failed to release resources")

	// Open file in read-only mode
	roFF, err := NewROFragmentFile(filePath, fileSize, 1)
	assert.NoError(t, err, "Failed to create read-only file")
	assert.NotNil(t, roFF)

	// Load data
	err = roFF.Load(ctx)
	assert.NoError(t, err, "Failed to load data")

	// Verify first and last IDs
	firstID, err := roFF.GetFirstEntryId()
	assert.NoError(t, err, "Failed to get first entry ID")
	assert.Equal(t, startEntryID, firstID, "First entry ID mismatch")

	lastID, err := roFF.GetLastEntryId()
	assert.NoError(t, err, "Failed to get last entry ID")
	assert.Equal(t, startEntryID+int64(entryCount-1), lastID, "Last entry ID mismatch")

	// Re-verify all entries
	t.Log("Verifying all entries after reload...")
	for i := 0; i < entryCount; i++ {
		entryID := startEntryID + int64(i)
		data, err := roFF.GetEntry(entryID)
		assert.NoError(t, err, "After reload: Failed to read entry %d", entryID)
		assert.Equal(t, allWrittenData[i], data, "After reload: Data mismatch for entry %d", entryID)
		t.Logf("After reload: Entry %d verified successfully", entryID)
	}

	// Clean up resources
	err = roFF.Release()
	assert.NoError(t, err, "Failed to release read-only file resources")
	t.Log("All alternating write/flush/read tests passed!")
}

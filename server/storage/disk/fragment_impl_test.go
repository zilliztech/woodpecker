package disk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/zilliztech/woodpecker/common/werr"

	"github.com/stretchr/testify/assert"
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
		err = ff.Write(context.Background(), testData, startEntryID)
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
	assert.Contains(t, err.Error(), "out of range")

	_, err = ff.GetEntry(startEntryID + 1) // 大于已有条目ID
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")
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
	err = ff.Release()
	assert.NoError(t, err)

	// 尝试在释放后写入数据
	err = ff.Write(context.Background(), testData, int64(1))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fragment file is closed")
}

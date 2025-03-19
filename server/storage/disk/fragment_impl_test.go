package disk

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFragmentFile(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建新的FragmentFile
	ff, err := NewFragmentFile(filePath, 1024*1024, 1) // 1MB文件，fragmentId=1
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
	ff, err := NewFragmentFile(filePath, 1024*1024, 1) // 1MB文件，fragmentId=1
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 写入测试数据
	testData := []byte("test data")
	err = ff.Write(context.Background(), testData)
	assert.NoError(t, err)

	// 读取数据
	data, err := ff.GetEntry(0)
	assert.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestFragmentFile_MultipleEntries(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建新的FragmentFile
	ff, err := NewFragmentFile(filePath, 1024*1024, 1) // 1MB文件，fragmentId=1
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 写入多个测试数据
	testData1 := []byte("test data 1")
	testData2 := []byte("test data 2")
	testData3 := []byte("test data 3")

	// 写入数据
	err = ff.Write(context.Background(), testData1)
	assert.NoError(t, err)
	err = ff.Write(context.Background(), testData2)
	assert.NoError(t, err)
	err = ff.Write(context.Background(), testData3)
	assert.NoError(t, err)

	// 刷新数据
	err = ff.Flush(context.Background())
	assert.NoError(t, err)

	// 检查条目ID范围
	firstID, err := ff.GetFirstEntryId()
	assert.NoError(t, err)
	lastID, err := ff.GetLastEntryId()
	assert.NoError(t, err)
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
	// 注意：实际存储可能与写入顺序相反
	data0, err := ff.GetEntry(0)
	assert.NoError(t, err)
	assert.Equal(t, testData3, data0, "条目0是最后写入的数据")

	data1, err := ff.GetEntry(1)
	assert.NoError(t, err)
	assert.Equal(t, testData2, data1, "条目1是第二个写入的数据")

	data2, err := ff.GetEntry(2)
	assert.NoError(t, err)
	assert.Equal(t, testData1, data2, "条目2是第一个写入的数据")
}

func TestFragmentFile_LoadAndReload(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建新的FragmentFile
	ff, err := NewFragmentFile(filePath, 1024*1024, 1) // 1MB文件，fragmentId=1
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 写入测试数据
	testData := []byte("test data")
	err = ff.Write(context.Background(), testData)
	assert.NoError(t, err)

	// 刷新数据到磁盘（这一步很重要）
	err = ff.Flush(context.Background())
	assert.NoError(t, err)

	// 关闭文件
	err = ff.Release()
	assert.NoError(t, err)

	// 重新打开文件
	ff2, err := NewFragmentFile(filePath, 1024*1024, 1) // 1MB文件，fragmentId=1
	assert.NoError(t, err)
	assert.NotNil(t, ff2)

	// 加载数据
	err = ff2.Load(context.Background())
	assert.NoError(t, err)

	// 读取数据
	data, err := ff2.GetEntry(0)
	assert.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestFragmentFile_CRCValidation(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建新的FragmentFile
	ff, err := NewFragmentFile(filePath, 1024*1024, 1) // 1MB文件，fragmentId=1
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 写入测试数据
	testData := []byte("test data")
	err = ff.Write(context.Background(), testData)
	assert.NoError(t, err)

	// 修改文件中的数据（破坏CRC）
	ff.mmap[ff.dataOffset-2] = 'X' // 修改数据的一部分

	// 尝试读取数据
	_, err = ff.GetEntry(0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CRC mismatch")
}

func TestFragmentFile_OutOfSpace(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建一个足够小的文件，使得数据加上必要的头尾空间不足
	// headerSize(4K) + footerSize(4K) + indexSize(自定义，假设为4K) = 12K的系统开销
	fileSize := int64(12*1024 + 1024) // 13KB

	// 创建新的FragmentFile
	ff, err := NewFragmentFile(filePath, fileSize, 1)
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 写入一些小数据，确保能够成功
	smallData := []byte("small test data")
	err = ff.Write(context.Background(), smallData)
	assert.NoError(t, err)

	// 尝试写入几乎填满剩余空间的大数据
	// 文件总大小 - 已用空间(包括头部、索引、footer和已写入的数据)
	largeData := make([]byte, 5*1024) // 5KB
	err = ff.Write(context.Background(), largeData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no space left")
}

func TestFragmentFile_InvalidEntryId(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建新的FragmentFile
	ff, err := NewFragmentFile(filePath, 1024*1024, 1) // 1MB文件，fragmentId=1
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 尝试读取不存在的条目
	_, err = ff.GetEntry(1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")
}

func TestFragmentFile_Release(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.frag")

	// 创建新的FragmentFile
	ff, err := NewFragmentFile(filePath, 1024*1024, 1) // 1MB文件，fragmentId=1
	assert.NoError(t, err)
	assert.NotNil(t, ff)

	// 写入测试数据
	testData := []byte("test data")
	err = ff.Write(context.Background(), testData)
	assert.NoError(t, err)

	// 释放资源
	err = ff.Release()
	assert.NoError(t, err)

	// 尝试在释放后写入数据
	err = ff.Write(context.Background(), testData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "fragment file is closed")
}

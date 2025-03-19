package disk

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/edsrzf/mmap-go"

	"github.com/zilliztech/woodpecker/server/storage"
)

var _ storage.Fragment = (*FragmentFile)(nil)

const (
	// 文件头大小
	headerSize = 4 * 1024
	// Footer大小
	footerSize = 4 * 1024
	// 每个索引项大小
	indexItemSize = 4
)

// FragmentFile manages file-based fragments.
type FragmentFile struct {
	mu           sync.RWMutex
	filePath     string
	mmap         mmap.MMap
	fileSize     int64
	dataOffset   uint32 // 当前数据写入位置
	indexOffset  uint32 // 当前索引写入位置（从后向前）
	lastEntryID  int64  // 最后一个条目的ID
	firstEntryID int64  // 第一个条目的ID
	entryCount   int32  // 当前条目数量
	closed       bool
	fragmentId   int64 // fragment的唯一标识符
}

// NewFragmentFile creates a new FragmentFile.
func NewFragmentFile(filePath string, fileSize int64, fragmentId int64, firstEntryID int64) (*FragmentFile, error) {
	ff := &FragmentFile{
		filePath:     filePath,
		fileSize:     fileSize,
		dataOffset:   headerSize,
		fragmentId:   fragmentId,
		firstEntryID: firstEntryID,
		lastEntryID:  firstEntryID - 1, // 初始化为比firstEntryID小1，表示没有entries
	}

	// 创建或打开文件
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open fragment file %s", filePath)
	}

	// 设置文件大小
	if err := file.Truncate(fileSize); err != nil {
		file.Close()
		return nil, errors.Wrapf(err, "failed to truncate file %s", filePath)
	}

	// 映射文件到内存
	ff.mmap, err = mmap.MapRegion(file, int(fileSize), mmap.RDWR, 0, 0)
	if err != nil {
		file.Close()
		return nil, errors.Wrapf(err, "failed to map fragment file %s", filePath)
	}

	// 写入文件头
	if err := ff.writeHeader(); err != nil {
		ff.Release()
		return nil, err
	}

	return ff, nil
}

// writeHeader writes the file header.
func (ff *FragmentFile) writeHeader() error {
	// 写入magic string
	copy(ff.mmap[0:8], []byte("FRAGMENT"))

	// 写入版本号
	binary.LittleEndian.PutUint32(ff.mmap[8:12], 1)

	return nil
}

// GetFragmentId returns the fragment ID.
func (ff *FragmentFile) GetFragmentId() int64 {
	return ff.fragmentId
}

// GetFragmentKey returns the fragment key (file path).
func (ff *FragmentFile) GetFragmentKey() string {
	return ff.filePath
}

// Flush ensures all data is written to disk.
func (ff *FragmentFile) Flush(ctx context.Context) error {
	ff.mu.Lock()
	defer ff.mu.Unlock()

	if ff.closed {
		return errors.New("fragment file is closed")
	}

	// 写入footer
	if err := ff.writeFooter(); err != nil {
		return err
	}

	// 同步到磁盘
	if err := ff.mmap.Flush(); err != nil {
		return errors.Wrap(err, "failed to flush fragment file")
	}

	return nil
}

// writeFooter writes the file footer.
func (ff *FragmentFile) writeFooter() error {
	footerOffset := uint32(ff.fileSize - footerSize)
	// 写入条目数量
	binary.LittleEndian.PutUint32(ff.mmap[footerOffset:], uint32(ff.entryCount))

	// 写入第一个和最后一个条目ID
	binary.LittleEndian.PutUint64(ff.mmap[footerOffset+4:], uint64(ff.firstEntryID))
	binary.LittleEndian.PutUint64(ff.mmap[footerOffset+12:], uint64(ff.lastEntryID))

	return nil
}

// Load loads the fragment file.
func (ff *FragmentFile) Load(ctx context.Context) error {
	ff.mu.Lock()
	defer ff.mu.Unlock()

	if ff.closed {
		return errors.New("fragment file is closed")
	}

	// 读取文件头
	if !ff.validateHeader() {
		return errors.New("invalid fragment file header")
	}

	// 读取footer
	if err := ff.readFooter(); err != nil {
		return err
	}

	// 如果有条目，则需要更新dataOffset
	if ff.entryCount > 0 {
		// 读取最后一个条目的索引位置
		lastIdxPos := uint32(ff.fileSize - footerSize - int64(indexItemSize))

		// 读取最后一个条目的数据偏移量
		lastDataOffset := binary.LittleEndian.Uint32(ff.mmap[lastIdxPos:])

		// 读取最后一个条目的数据长度
		dataLength := binary.LittleEndian.Uint32(ff.mmap[lastDataOffset:])

		// 计算下一个数据写入的偏移量 (lastDataOffset + 长度(4) + CRC(4) + 数据)
		ff.dataOffset = lastDataOffset + 8 + dataLength
	}

	return nil
}

// validateHeader validates the file header.
func (ff *FragmentFile) validateHeader() bool {
	// 检查magic string
	if string(ff.mmap[0:8]) != "FRAGMENT" {
		return false
	}

	// 检查版本号
	version := binary.LittleEndian.Uint32(ff.mmap[8:12])
	return version == 1
}

// readFooter reads the file footer.
func (ff *FragmentFile) readFooter() error {
	footerOffset := uint32(ff.fileSize - footerSize)
	// 读取条目数量
	ff.entryCount = int32(binary.LittleEndian.Uint32(ff.mmap[footerOffset:]))

	// 读取第一个和最后一个条目ID
	ff.firstEntryID = int64(binary.LittleEndian.Uint64(ff.mmap[footerOffset+4:]))
	ff.lastEntryID = int64(binary.LittleEndian.Uint64(ff.mmap[footerOffset+12:]))

	return nil
}

// GetLastEntryId returns the last entry ID.
func (ff *FragmentFile) GetLastEntryId() (int64, error) {
	ff.mu.RLock()
	defer ff.mu.RUnlock()

	if ff.closed {
		return 0, errors.New("fragment file is closed")
	}

	return ff.lastEntryID, nil
}

// GetFirstEntryId returns the first entry ID.
func (ff *FragmentFile) GetFirstEntryId() (int64, error) {
	ff.mu.RLock()
	defer ff.mu.RUnlock()

	if ff.closed {
		return 0, errors.New("fragment file is closed")
	}

	return ff.firstEntryID, nil
}

// GetLastModified returns the last modification time.
func (ff *FragmentFile) GetLastModified() int64 {
	info, err := os.Stat(ff.filePath)
	if err != nil {
		return 0
	}
	return info.ModTime().UnixNano() / 1e6
}

// GetEntry returns the entry at the specified ID.
func (ff *FragmentFile) GetEntry(entryId int64) ([]byte, error) {
	ff.mu.RLock()
	defer ff.mu.RUnlock()

	if ff.closed {
		return nil, errors.New("fragment file is closed")
	}

	// 检查entryId是否在范围内
	if entryId < ff.firstEntryID || entryId > ff.lastEntryID {
		return nil, fmt.Errorf("entry ID %d out of range", entryId)
	}

	// 计算索引位置 - 根据条目ID在索引区中的相对位置
	idxPos := uint32(ff.fileSize - footerSize - int64(indexItemSize)*(int64(entryId-ff.firstEntryID+1)))

	if idxPos < headerSize || idxPos >= uint32(ff.fileSize-footerSize) {
		return nil, fmt.Errorf("invalid index position: %d", idxPos)
	}

	// 读取数据偏移量
	offset := binary.LittleEndian.Uint32(ff.mmap[idxPos:])
	if offset < headerSize || offset >= uint32(ff.fileSize) {
		return nil, fmt.Errorf("invalid data offset: %d", offset)
	}

	// 读取数据长度
	length := binary.LittleEndian.Uint32(ff.mmap[offset:])
	if length == 0 || length > uint32(ff.fileSize)-offset-8 {
		return nil, fmt.Errorf("invalid data length: %d", length)
	}

	// 读取CRC (4字节)
	storedCRC := binary.LittleEndian.Uint32(ff.mmap[offset+4:])

	// 确定数据区域
	dataStart := offset + 8 // 跳过长度(4字节)和CRC(4字节)
	dataEnd := dataStart + length
	if dataEnd > uint32(ff.fileSize) {
		return nil, fmt.Errorf("data region out of bounds: %d-%d", dataStart, dataEnd)
	}

	// 读取数据
	data := make([]byte, length)
	copy(data, ff.mmap[dataStart:dataEnd])

	// 验证CRC
	if crc32.ChecksumIEEE(data) != storedCRC {
		return nil, fmt.Errorf("CRC mismatch for entry ID %d", entryId)
	}

	return data, nil
}

// GetSize returns the current size of the fragment.
func (ff *FragmentFile) GetSize() int64 {
	ff.mu.RLock()
	defer ff.mu.RUnlock()

	return int64(ff.dataOffset)
}

// Release releases the fragment file.
func (ff *FragmentFile) Release() error {
	ff.mu.Lock()
	defer ff.mu.Unlock()

	if ff.closed {
		return nil
	}

	// 解除内存映射
	if ff.mmap != nil {
		if err := ff.mmap.Unmap(); err != nil {
			return errors.Wrap(err, "failed to unmap fragment file")
		}
		ff.mmap = nil
	}

	ff.closed = true
	return nil
}

// Write writes data to the fragment file.
func (ff *FragmentFile) Write(ctx context.Context, data []byte) error {
	ff.mu.Lock()
	defer ff.mu.Unlock()

	fmt.Printf("Fragment写入条目: fragment ID %d, first entry ID %d, 数据长度: %d\n",
		ff.fragmentId, ff.firstEntryID, len(data))

	if ff.closed {
		fmt.Printf("Fragment已关闭，无法写入\n")
		return errors.New("fragment file is closed")
	}

	// 检查空间是否足够
	requiredSpace := uint32(len(data) + 8) // 数据长度(4) + CRC(4) + 数据

	fmt.Printf("需要空间: %d, 当前偏移量: %d, 文件大小: %d\n",
		requiredSpace, ff.dataOffset, ff.fileSize)

	// 确保有足够的空间写入数据和索引
	if ff.dataOffset+requiredSpace >= uint32(ff.fileSize-footerSize-int64(indexItemSize)*(int64(ff.entryCount)+1)) {
		fmt.Printf("Fragment空间不足，无法写入\n")
		return errors.New("no space left in fragment file")
	}

	// 写入数据长度 (4字节)
	binary.LittleEndian.PutUint32(ff.mmap[ff.dataOffset:], uint32(len(data)))

	// 计算CRC
	crc := crc32.ChecksumIEEE(data)

	// 写入CRC (4字节)
	binary.LittleEndian.PutUint32(ff.mmap[ff.dataOffset+4:], crc)

	// 写入数据
	copy(ff.mmap[ff.dataOffset+8:], data)

	// 当前数据的偏移量
	currentDataOffset := ff.dataOffset

	// 更新条目ID
	if ff.entryCount == 0 {
		// firstEntryID已在创建时设置，这里只需更新lastEntryID
		ff.lastEntryID = ff.firstEntryID
	} else {
		ff.lastEntryID++
	}

	// 计算索引位置 - 将最早的条目放在索引区的末尾，最新的条目放在索引区的开始
	// 这样读取时就会按照写入顺序返回数据
	idxPos := uint32(ff.fileSize - footerSize - int64(indexItemSize)*(int64(ff.lastEntryID-ff.firstEntryID+1)))

	// 检查索引位置是否有效
	if idxPos > uint32(ff.fileSize) || idxPos < ff.dataOffset {
		return fmt.Errorf("计算的索引位置无效: %d (文件大小: %d, 数据偏移: %d)", idxPos, ff.fileSize, ff.dataOffset)
	}

	// 安全检查: 确保索引区域不会与数据区域重叠
	if idxPos < ff.dataOffset {
		return fmt.Errorf("索引位置 (%d) 与数据区域 (%d) 重叠", idxPos, ff.dataOffset)
	}

	// 写入索引（存储数据的偏移量）
	binary.LittleEndian.PutUint32(ff.mmap[idxPos:], currentDataOffset)

	// 增加条目计数
	ff.entryCount++

	// 更新下一个数据的偏移量
	ff.dataOffset += 8 + uint32(len(data))

	return nil
}

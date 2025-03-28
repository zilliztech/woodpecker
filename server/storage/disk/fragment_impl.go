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
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
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
	mu         sync.RWMutex
	fragmentId int64 // fragment的唯一标识符

	filePath   string
	mappedFile mmap.MMap
	fileSize   int64

	dataOffset  uint32 // 当前数据写入位置
	indexOffset uint32 // 当前索引写入位置（从后向前）

	lastEntryID  int64 // 最后一个条目的ID
	firstEntryID int64 // 第一个条目的ID
	entryCount   int32 // 当前条目数量
	infoFetched  bool

	closed bool
}

// NewFragmentFile creates a new FragmentFile, which can read and write
func NewFragmentFile(filePath string, fileSize int64, fragmentId int64, firstEntryID int64) (*FragmentFile, error) {
	ff := &FragmentFile{
		filePath:     filePath,
		fileSize:     fileSize,
		dataOffset:   headerSize,
		fragmentId:   fragmentId,
		firstEntryID: firstEntryID,
		lastEntryID:  -1, // 初始化为比firstEntryID小1，表示没有entries
		infoFetched:  true,
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
	ff.mappedFile, err = mmap.MapRegion(file, -1, mmap.RDWR, 0, 0)
	if err != nil {
		file.Close()
		return nil, errors.Wrapf(err, "failed to map fragment file %s", filePath)
	}
	metrics.WpFragmentLoadedGauge.WithLabelValues("0").Inc()

	// 写入文件头
	if err := ff.writeHeader(); err != nil {
		ff.Release()
		return nil, err
	}

	return ff, nil
}

func NewROFragmentFile(filePath string, fileSize int64, fragmentId int64) (*FragmentFile, error) {
	ff := &FragmentFile{
		filePath:     filePath,
		fileSize:     fileSize,
		dataOffset:   headerSize,
		fragmentId:   fragmentId,
		firstEntryID: -1,
		lastEntryID:  -1, // 初始化为比firstEntryID小1，表示没有entries
		infoFetched:  false,
	}

	// 创建或打开文件
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open fragment file %s", filePath)
	}

	// 映射文件到内存
	ff.mappedFile, err = mmap.MapRegion(file, -1, mmap.RDWR, 0, 0)
	if err != nil {
		file.Close()
		return nil, errors.Wrapf(err, "failed to map fragment file %s", filePath)
	}
	metrics.WpFragmentLoadedGauge.WithLabelValues("0").Inc()

	// 加载meta
	err = ff.Load(context.Background())
	if err != nil {
		ff.Release()
		return nil, err
	}

	return ff, nil
}

// writeHeader writes the file header.
func (ff *FragmentFile) writeHeader() error {
	// 写入magic string
	copy(ff.mappedFile[0:8], []byte("FRAGMENT"))

	// 写入版本号
	binary.LittleEndian.PutUint32(ff.mappedFile[8:12], 1)

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
	if err := ff.mappedFile.Flush(); err != nil {
		return errors.Wrap(err, "failed to flush fragment file")
	}

	return nil
}

// writeFooter writes the file footer.
func (ff *FragmentFile) writeFooter() error {
	footerOffset := uint32(ff.fileSize - footerSize)
	// 写入条目数量
	binary.LittleEndian.PutUint32(ff.mappedFile[footerOffset:], uint32(ff.entryCount))

	// 写入第一个和最后一个条目ID
	binary.LittleEndian.PutUint64(ff.mappedFile[footerOffset+4:], uint64(ff.firstEntryID))
	binary.LittleEndian.PutUint64(ff.mappedFile[footerOffset+12:], uint64(ff.lastEntryID))

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
		lastDataOffset := binary.LittleEndian.Uint32(ff.mappedFile[lastIdxPos:])

		// 读取最后一个条目的数据长度
		dataLength := binary.LittleEndian.Uint32(ff.mappedFile[lastDataOffset:])

		// 计算下一个数据写入的偏移量 (lastDataOffset + 长度(4) + CRC(4) + 数据)
		ff.dataOffset = lastDataOffset + 8 + dataLength
	}

	// set infoFetched
	ff.infoFetched = true

	//
	return nil
}

// validateHeader validates the file header.
func (ff *FragmentFile) validateHeader() bool {
	// 检查magic string
	if string(ff.mappedFile[0:8]) != "FRAGMENT" {
		return false
	}

	// 检查版本号
	version := binary.LittleEndian.Uint32(ff.mappedFile[8:12])
	return version == 1
}

// readFooter reads the file footer.
func (ff *FragmentFile) readFooter() error {
	footerOffset := uint32(ff.fileSize - footerSize)
	// 读取条目数量
	ff.entryCount = int32(binary.LittleEndian.Uint32(ff.mappedFile[footerOffset:]))

	// 读取第一个和最后一个条目ID
	ff.firstEntryID = int64(binary.LittleEndian.Uint64(ff.mappedFile[footerOffset+4:]))
	ff.lastEntryID = int64(binary.LittleEndian.Uint64(ff.mappedFile[footerOffset+12:]))

	return nil
}

// GetLastEntryId returns the last entry ID.
func (ff *FragmentFile) GetLastEntryId() (int64, error) {
	ff.mu.RLock()
	defer ff.mu.RUnlock()

	if ff.closed {
		return 0, errors.New("fragment file is closed")
	}

	if !ff.infoFetched {
		err := ff.Load(context.Background())
		if err != nil {
			return -1, err
		}
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

	if !ff.infoFetched {
		err := ff.Load(context.Background())
		if err != nil {
			return 0, err
		}
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
		// Use context.Background() for logging since we don't have a context parameter
		logger.Ctx(context.Background()).Debug("Fragment file is closed")
		return nil, errors.New("fragment file is closed")
	}

	// fetch info
	if !ff.infoFetched {
		err := ff.Load(context.Background())
		if err != nil {
			return nil, err
		}
	}

	// 检查entryId是否在范围内
	if entryId < ff.firstEntryID || entryId > ff.lastEntryID {
		logger.Ctx(context.Background()).Debug("Entry ID out of range",
			zap.Int64("requestedID", entryId),
			zap.Int64("firstEntryID", ff.firstEntryID),
			zap.Int64("lastEntryID", ff.lastEntryID))
		return nil, fmt.Errorf("entry ID %d out of range", entryId)
	}

	// 计算索引位置 - 根据条目ID在索引区中的相对位置
	idxPos := uint32(ff.fileSize - footerSize - int64(indexItemSize)*(int64(entryId-ff.firstEntryID+1)))

	if idxPos < headerSize || idxPos >= uint32(ff.fileSize-footerSize) {
		logger.Ctx(context.Background()).Debug("Invalid index position",
			zap.Uint32("idxPos", idxPos),
			zap.Uint32("headerSize", headerSize),
			zap.Int64("fileSize", ff.fileSize),
			zap.Uint32("footerSize", footerSize))
		return nil, fmt.Errorf("invalid index position: %d", idxPos)
	}

	// 读取数据偏移量
	offset := binary.LittleEndian.Uint32(ff.mappedFile[idxPos:])
	if offset < headerSize || offset >= uint32(ff.fileSize) {
		logger.Ctx(context.Background()).Debug("Invalid data offset",
			zap.Uint32("offset", offset),
			zap.Uint32("headerSize", headerSize),
			zap.Int64("fileSize", ff.fileSize))
		return nil, fmt.Errorf("invalid data offset: %d", offset)
	}

	// 读取数据长度
	length := binary.LittleEndian.Uint32(ff.mappedFile[offset:])
	if length == 0 || length > uint32(ff.fileSize)-offset-8 {
		logger.Ctx(context.Background()).Debug("Invalid data length",
			zap.Uint32("length", length),
			zap.Uint32("offset", offset),
			zap.Int64("fileSize", ff.fileSize))
		return nil, fmt.Errorf("invalid data length: %d", length)
	}

	// 读取CRC (4字节)
	storedCRC := binary.LittleEndian.Uint32(ff.mappedFile[offset+4:])

	// 确定数据区域
	dataStart := offset + 8 // 跳过长度(4字节)和CRC(4字节)
	dataEnd := dataStart + length
	if dataEnd > uint32(ff.fileSize) {
		logger.Ctx(context.Background()).Debug("Data region out of bounds",
			zap.Uint32("dataStart", dataStart),
			zap.Uint32("dataEnd", dataEnd),
			zap.Int64("fileSize", ff.fileSize))
		return nil, fmt.Errorf("data region out of bounds: %d-%d", dataStart, dataEnd)
	}

	// 读取数据
	data := make([]byte, length)
	copy(data, ff.mappedFile[dataStart:dataEnd])

	logger.Ctx(context.Background()).Debug("fragment读取数据",
		zap.String("fragmentFile", ff.filePath),
		zap.Int64("entryId", entryId),
		zap.Uint32("start", dataStart),
		zap.Uint32("end", dataEnd),
		zap.Int("dataSize", len(data)))

	// 验证CRC
	if crc32.ChecksumIEEE(data) != storedCRC {
		logger.Ctx(context.Background()).Debug("CRC mismatch",
			zap.Int64("entryId", entryId),
			zap.Uint32("computedCRC", crc32.ChecksumIEEE(data)),
			zap.Uint32("storedCRC", storedCRC))
		return nil, fmt.Errorf("CRC mismatch for entry ID %d", entryId)
	}

	return data, nil
}

// IteratorPrint for Debug Test only
func (ff *FragmentFile) IteratorPrint() error {
	if ff.closed {
		// Use context.Background() for logging since we don't have a context parameter
		logger.Ctx(context.Background()).Debug("Fragment file is closed")
		return errors.New("fragment file is closed")
	}

	// fetch info
	if !ff.infoFetched {
		err := ff.Load(context.Background())
		if err != nil {
			return err
		}
	}

	// 计算索引位置 - 根据条目ID在索引区中的相对位置
	for i := 0; i < int(ff.entryCount); i++ {

		idxPos := uint32(ff.fileSize - footerSize - int64(indexItemSize)*(int64(i+1)))
		if idxPos < headerSize || idxPos >= uint32(ff.fileSize-footerSize) {
			logger.Ctx(context.Background()).Debug("Invalid index position",
				zap.Uint32("idxPos", idxPos),
				zap.Uint32("headerSize", headerSize),
				zap.Int64("fileSize", ff.fileSize),
				zap.Uint32("footerSize", footerSize))
			return fmt.Errorf("invalid index position: %d", idxPos)
		}

		// 读取数据偏移量
		offset := binary.LittleEndian.Uint32(ff.mappedFile[idxPos:])
		if offset < headerSize || offset >= uint32(ff.fileSize) {
			logger.Ctx(context.Background()).Debug("Invalid data offset",
				zap.Uint32("offset", offset),
				zap.Uint32("headerSize", headerSize),
				zap.Int64("fileSize", ff.fileSize))
			return fmt.Errorf("invalid data offset: %d", offset)
		}

		// 读取数据长度
		length := binary.LittleEndian.Uint32(ff.mappedFile[offset:])
		if length == 0 || length > uint32(ff.fileSize)-offset-8 {
			logger.Ctx(context.Background()).Debug("Invalid data length",
				zap.Uint32("length", length),
				zap.Uint32("offset", offset),
				zap.Int64("fileSize", ff.fileSize))
			return fmt.Errorf("invalid data length: %d", length)
		}

		// 读取CRC (4字节)
		storedCRC := binary.LittleEndian.Uint32(ff.mappedFile[offset+4:])

		// 确定数据区域
		dataStart := offset + 8 // 跳过长度(4字节)和CRC(4字节)
		dataEnd := dataStart + length
		if dataEnd > uint32(ff.fileSize) {
			logger.Ctx(context.Background()).Debug("Data region out of bounds",
				zap.Uint32("dataStart", dataStart),
				zap.Uint32("dataEnd", dataEnd),
				zap.Int64("fileSize", ff.fileSize))
			return fmt.Errorf("data region out of bounds: %d-%d", dataStart, dataEnd)
		}

		// 读取数据
		data := make([]byte, length)
		copy(data, ff.mappedFile[dataStart:dataEnd])

		// 提取里面的 id和data
		actualID := int64(binary.LittleEndian.Uint64(data[:8]))
		actualData := data[8:]

		logger.Ctx(context.Background()).Debug("fragment读取数据",
			zap.String("fragmentFile", ff.filePath),
			zap.Int64("entryId", actualID),
			zap.Int64("segmentEntryId", ff.firstEntryID+int64(i)),
			zap.Uint32("start", dataStart),
			zap.Uint32("end", dataEnd),
			zap.Int("actualDataSize", len(actualData)),
			zap.Uint32("pos", idxPos),
			zap.Int("i", i),
			zap.Int64("firstId", ff.firstEntryID))

		// 验证CRC
		if crc32.ChecksumIEEE(data) != storedCRC {
			logger.Ctx(context.Background()).Debug("CRC mismatch",
				zap.Int64("entryId", actualID),
				zap.Uint32("computedCRC", crc32.ChecksumIEEE(data)),
				zap.Uint32("storedCRC", storedCRC))
			return fmt.Errorf("CRC mismatch for entry ID %d", actualID)
		}
	}
	return nil
}

// GetSize returns the current size of the fragment.
func (ff *FragmentFile) GetSize() int64 {
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
	if ff.mappedFile != nil {
		if err := ff.mappedFile.Unmap(); err != nil {
			return errors.Wrap(err, "failed to unmap fragment file")
		}
		ff.mappedFile = nil
		metrics.WpFragmentBufferBytes.WithLabelValues("0").Sub(float64(ff.GetSize()))
		metrics.WpFragmentLoadedGauge.WithLabelValues("0").Dec()
	}

	ff.closed = true
	return nil
}

// Write writes data to the fragment file.
func (ff *FragmentFile) Write(ctx context.Context, data []byte, writeEntryId int64) error {
	ff.mu.Lock()
	defer ff.mu.Unlock()

	logger.Ctx(ctx).Debug("Fragment写入条目",
		zap.Int64("fragmentId", ff.fragmentId),
		zap.Int64("firstEntryID", ff.firstEntryID),
		zap.Int("dataLength", len(data)))

	if ff.closed {
		logger.Ctx(ctx).Debug("Fragment已关闭，无法写入")
		return errors.New("fragment file is closed")
	}

	// 检查空间是否足够
	requiredSpace := uint32(len(data) + 8) // 数据长度(4) + CRC(4) + 数据

	logger.Ctx(ctx).Debug("需要空间",
		zap.Uint32("requiredSpace", requiredSpace),
		zap.Uint32("currentOffset", ff.dataOffset),
		zap.Int64("fileSize", ff.fileSize))

	// 索引位置所需空间
	indexSpace := int64(indexItemSize)
	if ff.entryCount > 0 {
		indexSpace = int64(indexItemSize) * (int64(ff.entryCount) + 1)
	}

	// 确保有足够的空间写入数据和索引
	if int64(ff.dataOffset)+int64(requiredSpace) >= ff.fileSize-footerSize-indexSpace {
		logger.Ctx(ctx).Debug("Fragment空间不足，无法写入",
			zap.Uint32("requiredSpace", requiredSpace),
			zap.Uint32("dataOffset", ff.dataOffset),
			zap.String("filePath", ff.filePath),
			zap.Int64("fileSize", ff.fileSize),
			zap.Int64("footerSize", footerSize),
			zap.Int64("indexSpace", indexSpace),
			zap.Int64("writeEntryId", writeEntryId))
		return werr.ErrDiskFragmentNoSpace
	}

	// 安全检查: 确保我们不会写入超出映射范围的数据
	if int(ff.dataOffset)+8+len(data) > len(ff.mappedFile) {
		logger.Ctx(ctx).Error("写入可能超出映射范围",
			zap.Uint32("offset", ff.dataOffset),
			zap.Int("dataLength", len(data)),
			zap.Int("mappedFileSize", len(ff.mappedFile)))
		return werr.ErrDiskFragmentNoSpace
	}

	// 写入数据长度 (4字节)
	binary.LittleEndian.PutUint32(ff.mappedFile[ff.dataOffset:ff.dataOffset+4], uint32(len(data)))

	// 计算CRC
	crc := crc32.ChecksumIEEE(data)

	// 写入CRC (4字节)
	binary.LittleEndian.PutUint32(ff.mappedFile[ff.dataOffset+4:ff.dataOffset+8], crc)

	// 写入数据
	copy(ff.mappedFile[ff.dataOffset+8:ff.dataOffset+8+uint32(len(data))], data)

	logger.Ctx(context.Background()).Debug("fragment写入数据",
		zap.String("fragmentFile", ff.filePath),
		zap.Int64("writeEntryId", writeEntryId),
		zap.Uint32("start", ff.dataOffset+8),
		zap.Uint32("end", ff.dataOffset+8+uint32(len(data))),
		zap.Int("dataSize", len(data)))

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
		logger.Ctx(ctx).Debug("计算的索引位置无效",
			zap.Uint32("idxPos", idxPos),
			zap.Int64("fileSize", ff.fileSize),
			zap.Uint32("dataOffset", ff.dataOffset))
		return fmt.Errorf("计算的索引位置无效: %d (文件大小: %d, 数据偏移: %d)", idxPos, ff.fileSize, ff.dataOffset)
	}

	// 安全检查: 确保索引区域不会与数据区域重叠
	if idxPos < ff.dataOffset {
		logger.Ctx(ctx).Debug("索引位置与数据区域重叠",
			zap.Uint32("idxPos", idxPos),
			zap.Uint32("dataOffset", ff.dataOffset))
		return werr.ErrDiskFragmentNoSpace
	}

	// 安全检查: 确保索引写入不会超出映射范围
	if int(idxPos)+4 > len(ff.mappedFile) {
		logger.Ctx(ctx).Error("索引写入可能超出映射范围",
			zap.Uint32("idxPos", idxPos),
			zap.Int("mappedFileSize", len(ff.mappedFile)))
		return werr.ErrDiskFragmentNoSpace
	}

	// 写入索引 - 存储数据的偏移量
	binary.LittleEndian.PutUint32(ff.mappedFile[idxPos:idxPos+4], currentDataOffset)

	// 更新内部状态
	ff.dataOffset += requiredSpace
	ff.entryCount++

	return nil
}

// OpenFragment opens an existing fragment file
func OpenFragment(filePath string, fileSize int64, fragmentId int64) (*FragmentFile, error) {
	// 打开文件
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open fragment file %s", filePath)
	}

	// 获取文件信息
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, errors.Wrapf(err, "failed to get file info %s", filePath)
	}

	// 映射文件到内存
	mappedFile, err := mmap.MapRegion(file, int(fileInfo.Size()), mmap.RDWR, 0, 0)
	if err != nil {
		file.Close()
		return nil, errors.Wrapf(err, "failed to map fragment file %s", filePath)
	}

	// 读取footer以获取firstEntryID和lastEntryID
	footerOffset := fileInfo.Size() - footerSize
	firstEntryID := int64(binary.LittleEndian.Uint64(mappedFile[footerOffset+4 : footerOffset+12]))
	lastEntryID := int64(binary.LittleEndian.Uint64(mappedFile[footerOffset+12 : footerOffset+20]))
	entryCount := int32(binary.LittleEndian.Uint32(mappedFile[footerOffset : footerOffset+4]))

	// 创建FragmentFile实例
	ff := &FragmentFile{
		filePath:     filePath,
		mappedFile:   mappedFile,
		fileSize:     fileInfo.Size(),
		fragmentId:   fragmentId,
		firstEntryID: firstEntryID,
		lastEntryID:  lastEntryID,
		entryCount:   entryCount,
	}

	return ff, nil
}

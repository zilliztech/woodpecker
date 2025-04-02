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

var _ storage.Fragment = (*FragmentFileWriter)(nil)

const (
	// 文件头大小
	headerSize = 4 * 1024
	// Footer大小
	footerSize = 4 * 1024
	// 每个索引项大小
	indexItemSize = 4
)

// FragmentFileWriter manages fragment data write
type FragmentFileWriter struct {
	mu         sync.RWMutex
	fragmentId int64 // fragment的唯一标识符
	filePath   string
	fileSize   int64
	mappedFile mmap.MMap

	dataOffset  uint32 // 当前数据写入位置
	indexOffset uint32 // 当前索引写入位置

	firstEntryID int64 // 第一个条目的ID
	lastEntryID  int64 // 最后一个条目的ID
	entryCount   int32 // 当前条目数量
	isGrowing    bool  // 此fragment还会写入数据

	infoFetched bool
	closed      bool // 是否已经关闭
}

// NewFragmentFileWriter creates a new FragmentFile, which can write only
func NewFragmentFileWriter(filePath string, fileSize int64, fragmentId int64, firstEntryID int64) (*FragmentFileWriter, error) {
	fw := &FragmentFileWriter{
		fragmentId: fragmentId,
		filePath:   filePath,
		fileSize:   fileSize,

		dataOffset:   headerSize,
		indexOffset:  uint32(fileSize - footerSize - indexItemSize),
		firstEntryID: firstEntryID,
		lastEntryID:  -1,
		entryCount:   0,
		isGrowing:    true,

		infoFetched: true,
		closed:      false,
	}

	// 创建或打开文件
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open new fragment file %s", filePath)
	}

	// 设置文件大小
	if err := file.Truncate(fileSize); err != nil {
		file.Close()
		return nil, errors.Wrapf(err, "failed to truncate the new fragment file:%s to size:%d", filePath, fileSize)
	}

	// 映射文件到内存
	fw.mappedFile, err = mmap.MapRegion(file, -1, mmap.RDWR, 0, 0)
	if err != nil {
		file.Close()
		return nil, errors.Wrapf(err, "failed to map fragment file %s", filePath)
	}
	metrics.WpFragmentBufferBytes.WithLabelValues("0").Add(float64(fileSize))
	metrics.WpFragmentLoadedGauge.WithLabelValues("0").Inc()

	// 写入footer
	if err := fw.writeFooter(); err != nil {
		fw.Release()
		return nil, err
	}
	// 写入header
	if err := fw.writeHeader(); err != nil {
		fw.Release()
		return nil, err
	}

	logger.Ctx(context.Background()).Debug("FragmentFile created", zap.String("filePath", filePath), zap.Int64("fragmentId", fragmentId), zap.String("fragmentInst", fmt.Sprintf("%p", fw)))
	return fw, nil
}

// writeHeader writes the file header.
func (fw *FragmentFileWriter) writeHeader() error {
	// 写入magic string
	copy(fw.mappedFile[0:8], []byte("FRAGMENT"))

	// 写入版本号
	binary.LittleEndian.PutUint32(fw.mappedFile[8:12], 1)

	return nil
}

// GetFragmentId returns the fragment ID.
func (fw *FragmentFileWriter) GetFragmentId() int64 {
	return fw.fragmentId
}

// GetFragmentKey returns the fragment key (file path).
func (fw *FragmentFileWriter) GetFragmentKey() string {
	return fw.filePath
}

// Flush ensures all data is written to disk.
func (fw *FragmentFileWriter) Flush(ctx context.Context) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.closed {
		return errors.New("fragment file is closed")
	}

	// 写入footer
	if err := fw.writeFooter(); err != nil {
		return err
	}

	// 同步到磁盘
	if err := fw.mappedFile.Flush(); err != nil {
		return errors.Wrap(err, "failed to flush fragment file")
	}

	return nil
}

// writeFooter writes the file footer.
func (fw *FragmentFileWriter) writeFooter() error {
	footerOffset := uint32(fw.fileSize - footerSize)
	// 写入条目数量
	binary.LittleEndian.PutUint32(fw.mappedFile[footerOffset:], uint32(fw.entryCount))

	// 写入第一个和最后一个条目ID
	binary.LittleEndian.PutUint64(fw.mappedFile[footerOffset+4:], uint64(fw.firstEntryID))
	binary.LittleEndian.PutUint64(fw.mappedFile[footerOffset+12:], uint64(fw.lastEntryID))

	// 写入growing状态
	isGrowingFlag := uint32(0)
	if fw.isGrowing {
		isGrowingFlag = uint32(1)
	}
	binary.LittleEndian.PutUint32(fw.mappedFile[footerOffset+20:], isGrowingFlag)
	logger.Ctx(context.Background()).Debug("write footer",
		zap.Int32("entryCount", fw.entryCount),
		zap.Int64("firstEntryID", fw.firstEntryID),
		zap.Int64("lastEntryID", fw.lastEntryID),
		zap.Bool("isGrowing", fw.isGrowing),
		zap.String("fragmentInst", fmt.Sprintf("%p", fw)))
	return nil
}

// Load loads the fragment file.
func (fw *FragmentFileWriter) Load(ctx context.Context) error {
	return werr.ErrNotSupport.WithCauseErrMsg("Fragment Writer support write only, cannot load data not")
}

// GetLastEntryId returns the last entry ID.
func (fw *FragmentFileWriter) GetLastEntryId() (int64, error) {
	if fw.closed {
		return 0, errors.New("fragment file is closed")
	}
	return fw.lastEntryID, nil
}

// GetFirstEntryId returns the first entry ID.
func (fw *FragmentFileWriter) GetFirstEntryId() (int64, error) {
	if fw.closed {
		return 0, errors.New("fragment file is closed")
	}
	return fw.firstEntryID, nil
}

// GetLastModified returns the last modification time.
func (fw *FragmentFileWriter) GetLastModified() int64 {
	info, err := os.Stat(fw.filePath)
	if err != nil {
		return 0
	}
	return info.ModTime().UnixNano() / 1e6
}

// GetEntry returns the entry at the specified ID.
// NOTE: The result entry maybe not flush to disk yet.
func (fw *FragmentFileWriter) GetEntry(entryId int64) ([]byte, error) {
	if fw.closed {
		logger.Ctx(context.Background()).Warn("failed to get entry from a closed fragment file",
			zap.String("filePath", fw.filePath),
			zap.Int64("fragmentId", fw.fragmentId),
			zap.Int64("readingEntryId", entryId))
		return nil, errors.New("fragment file is closed")
	}

	logger.Ctx(context.Background()).Debug("Try get entry from this fragment",
		zap.String("filePath", fw.filePath),
		zap.Int64("firstEntryId", fw.firstEntryID),
		zap.Int64("lastEntryId", fw.lastEntryID),
		zap.Int64("readingEntryId", entryId),
		zap.String("fragInst", fmt.Sprintf("%p", fw)))

	// 检查entryId是否在范围内
	if entryId < fw.firstEntryID || entryId > fw.lastEntryID {
		logger.Ctx(context.Background()).Debug("entry ID out of range",
			zap.Int64("requestedID", entryId),
			zap.Int64("firstEntryID", fw.firstEntryID),
			zap.Int64("lastEntryID", fw.lastEntryID))
		return nil, werr.ErrInvalidEntryId.WithCauseErrMsg(fmt.Sprintf("entry ID %d not in the range of this fragment", entryId))
	}

	// 计算索引位置 - 根据条目ID在索引区中的相对位置
	idxPos := uint32(fw.fileSize - footerSize - int64(indexItemSize)*(int64(entryId-fw.firstEntryID+1)))

	if idxPos < headerSize || idxPos >= uint32(fw.fileSize-footerSize) {
		logger.Ctx(context.Background()).Debug("Invalid index position",
			zap.Uint32("idxPos", idxPos),
			zap.Uint32("headerSize", headerSize),
			zap.Int64("fileSize", fw.fileSize),
			zap.Uint32("footerSize", footerSize))
		return nil, fmt.Errorf("invalid index position: %d", idxPos)
	}

	// 读取数据偏移量
	offset := binary.LittleEndian.Uint32(fw.mappedFile[idxPos:])
	if offset < headerSize || offset >= uint32(fw.fileSize-footerSize) {
		logger.Ctx(context.Background()).Debug("Invalid data offset",
			zap.Uint32("offset", offset),
			zap.Uint32("headerSize", headerSize),
			zap.Int64("fileSize", fw.fileSize))
		return nil, fmt.Errorf("invalid data offset: %d", offset)
	}

	// 读取数据长度
	length := binary.LittleEndian.Uint32(fw.mappedFile[offset:])
	if length == 0 || length > uint32(fw.fileSize-footerSize)-offset-8 {
		logger.Ctx(context.Background()).Debug("Invalid data length",
			zap.Uint32("length", length),
			zap.Uint32("offset", offset),
			zap.Int64("fileSize", fw.fileSize))
		return nil, fmt.Errorf("invalid data length: %d", length)
	}

	// 读取CRC (4字节)
	storedCRC := binary.LittleEndian.Uint32(fw.mappedFile[offset+4:])

	// 确定数据区域
	dataStart := offset + 8 // 跳过长度(4字节)和CRC(4字节)
	dataEnd := dataStart + length
	if dataEnd > uint32(fw.fileSize-footerSize) {
		logger.Ctx(context.Background()).Debug("Data region out of bounds",
			zap.Uint32("dataStart", dataStart),
			zap.Uint32("dataEnd", dataEnd),
			zap.Int64("fileSize", fw.fileSize))
		return nil, fmt.Errorf("data region out of bounds: %d-%d", dataStart, dataEnd)
	}

	// 读取数据
	data := make([]byte, length)
	copy(data, fw.mappedFile[dataStart:dataEnd])

	logger.Ctx(context.Background()).Debug("fragment读取数据完成",
		zap.String("fragmentFile", fw.filePath),
		zap.Int64("readingEntryId", entryId),
		zap.Uint32("start", dataStart),
		zap.Uint32("end", dataEnd),
		zap.Uint32("idxPos", idxPos),
		zap.Int("dataSize", len(data)),
		//zap.Any("data", data),
		zap.String("fragInst", fmt.Sprintf("%p", fw)))

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

// GetSize returns the current size of the fragment.
func (fw *FragmentFileWriter) GetSize() int64 {
	return fw.fileSize
}

// Release releases the fragment file.
func (fw *FragmentFileWriter) Release() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.closed {
		return nil
	}

	// 解除内存映射
	if fw.mappedFile != nil {
		if err := fw.mappedFile.Unmap(); err != nil {
			return errors.Wrap(err, "failed to unmap fragment file")
		}
		fw.mappedFile = nil
		metrics.WpFragmentBufferBytes.WithLabelValues("0").Sub(float64(fw.GetSize()))
		metrics.WpFragmentLoadedGauge.WithLabelValues("0").Dec()
	}

	// mark data is not fetched in buff
	fw.infoFetched = false

	return nil
}

func (fw *FragmentFileWriter) Close() {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	fw.closed = true
}

// Write writes data to the fragment file.
func (fw *FragmentFileWriter) Write(ctx context.Context, data []byte, writeEntryId int64) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.closed {
		logger.Ctx(ctx).Warn("failed to write entry, fragment is closed", zap.Int64("writingEntryId", writeEntryId), zap.String("fragInst", fmt.Sprintf("%p", fw)))
		return errors.New("fragment file is closed")
	}

	// 检查空间是否足够
	requiredSpace := uint32(len(data) + 8) // 数据长度(4) + CRC(4) + 数据
	logger.Ctx(ctx).Debug("fragment writing entry",
		zap.Int64("fragmentId", fw.fragmentId),
		zap.Int64("firstEntryID", fw.firstEntryID),
		zap.Int64("lastEntryID", fw.lastEntryID),
		zap.Int32("entryCount", fw.entryCount),
		zap.Int64("writingEntryId", writeEntryId),
		//zap.Any("data", data),
		zap.Uint32("requiredSpace", requiredSpace),
		zap.Uint32("dataOffset", fw.dataOffset),
		zap.Uint32("indexOffset", fw.indexOffset),
		zap.String("fragInst", fmt.Sprintf("%p", fw)))

	// 索引位置所需空间
	indexSpace := int64(indexItemSize)
	if fw.entryCount > 0 {
		indexSpace = int64(indexItemSize) * (int64(fw.entryCount) + 1)
	}

	// 确保有足够的空间写入数据和索引
	if int64(fw.dataOffset)+int64(requiredSpace) >= fw.fileSize-footerSize-indexSpace {
		logger.Ctx(ctx).Warn("Fragment空间不足，无法写入",
			zap.Uint32("requiredSpace", requiredSpace),
			zap.Uint32("dataOffset", fw.dataOffset),
			zap.String("filePath", fw.filePath),
			zap.Int64("fileSize", fw.fileSize),
			zap.Int64("footerSize", footerSize),
			zap.Int64("indexSpace", indexSpace),
			zap.Int64("writeEntryId", writeEntryId))
		return werr.ErrDiskFragmentNoSpace
	}

	// 写入数据长度 (4字节)
	binary.LittleEndian.PutUint32(fw.mappedFile[fw.dataOffset:fw.dataOffset+4], uint32(len(data)))
	// 计算CRC
	crc := crc32.ChecksumIEEE(data)
	// 写入CRC (4字节)
	binary.LittleEndian.PutUint32(fw.mappedFile[fw.dataOffset+4:fw.dataOffset+8], crc)
	// 写入数据
	copy(fw.mappedFile[fw.dataOffset+8:fw.dataOffset+8+uint32(len(data))], data)

	// 当前数据的偏移量
	currentDataOffset := fw.dataOffset

	// 第一次写入
	if fw.lastEntryID == -1 {
		fw.lastEntryID = writeEntryId
	} else if fw.lastEntryID+1 == writeEntryId {
		fw.lastEntryID = writeEntryId
	} else {
		logger.Ctx(context.Background()).Warn("fragment写入数据后, lastEntryID自增",
			zap.String("fragmentFile", fw.filePath),
			zap.Int64("writeEntryId", writeEntryId),
			zap.Int64("lastEntryID", fw.lastEntryID),
		)
		return werr.ErrInvalidEntryId.WithCauseErrMsg(fmt.Sprintf("writting the entry:%d is not the expected one: %d", writeEntryId, fw.lastEntryID+1))
	}

	// 计算索引位置 - 将最早的条目放在索引区的末尾，最新的条目放在索引区的开始
	// 这样读取时就会按照写入顺序返回数据
	idxPos := fw.indexOffset

	// 安全检查: 确保索引区域不会与数据区域重叠
	if idxPos <= fw.dataOffset+requiredSpace {
		logger.Ctx(ctx).Warn("索引位置与数据区域重叠",
			zap.Uint32("idxPos", idxPos),
			zap.Uint32("dataOffset", fw.dataOffset))
		return werr.ErrDiskFragmentNoSpace
	}

	// 写入索引 - 存储数据的偏移量
	binary.LittleEndian.PutUint32(fw.mappedFile[idxPos:idxPos+indexItemSize], currentDataOffset)

	logger.Ctx(context.Background()).Debug("fragment已写入数据",
		zap.String("fragmentFile", fw.filePath),
		zap.Int64("writingEntryId", writeEntryId),
		zap.Uint32("start", fw.dataOffset+8),
		zap.Uint32("end", fw.dataOffset+8+uint32(len(data))),
		zap.Uint32("idxPos", idxPos),
		zap.Int("dataSize", len(data)),
		//zap.Any("data", data),
		zap.String("fragInst", fmt.Sprintf("%p", fw)))

	// 更新内部状态
	logger.Ctx(context.Background()).Debug("fragment已写入数据,更新索引前",
		zap.Int64("writingEntryId", writeEntryId),
		zap.Uint32("dataOffset", fw.dataOffset),
		zap.Uint32("indexOffset", fw.indexOffset),
		zap.Uint32("requiredSpace", requiredSpace),
		zap.Int32("entryCount", fw.entryCount))
	fw.dataOffset += requiredSpace
	fw.indexOffset -= indexItemSize
	fw.entryCount++
	logger.Ctx(context.Background()).Debug("fragment已写入数据,更新索引后",
		zap.Int64("writingEntryId", writeEntryId),
		zap.Uint32("dataOffset", fw.dataOffset),
		zap.Uint32("indexOffset", fw.indexOffset),
		zap.Int32("entryCount", fw.entryCount))

	//
	return nil
}

var _ storage.Fragment = (*FragmentFileReader)(nil)

// FragmentFileReader manages fragment data read
type FragmentFileReader struct {
	mu         sync.RWMutex
	fragmentId int64 // fragment的唯一标识符
	filePath   string
	fileSize   int64
	mappedFile mmap.MMap

	firstEntryID int64 // 第一个条目的ID
	lastEntryID  int64 // 最后一个条目的ID
	entryCount   int32 // 当前条目数量
	isGrowing    bool  // 此fragment还会写入数据

	infoFetched bool
	closed      bool // 是否已经关闭
}

// NewFragmentFileReader creates a new FragmentFile, which can read only
func NewFragmentFileReader(filePath string, fileSize int64, fragmentId int64) (*FragmentFileReader, error) {
	ff := &FragmentFileReader{
		filePath:   filePath,
		fileSize:   fileSize,
		fragmentId: fragmentId,

		// variables bellow would be lazy loaded if need
		entryCount:   0,
		firstEntryID: -1,
		lastEntryID:  -1,
		isGrowing:    true,

		infoFetched: false,
	}
	return ff, nil
}

// GetFragmentId returns the fragment ID.
func (fr *FragmentFileReader) GetFragmentId() int64 {
	return fr.fragmentId
}

// GetFragmentKey returns the fragment key (file path).
func (fr *FragmentFileReader) GetFragmentKey() string {
	return fr.filePath
}

// Flush ensures all data is written to disk.
func (fr *FragmentFileReader) Flush(ctx context.Context) error {
	return werr.ErrNotSupport.WithCauseErrMsg("Fragment reader cannot support flush")
}

// Load loads the fragment file.
func (fr *FragmentFileReader) Load(ctx context.Context) error {
	fr.mu.Lock()
	defer fr.mu.Unlock()

	if fr.closed {
		return errors.New("fragment file is closed")
	}

	// 创建或打开文件
	file, err := os.OpenFile(fr.filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return errors.Wrapf(err, "failed to open fragment file %s", fr.filePath)
	}

	// 映射文件到内存
	fr.mappedFile, err = mmap.MapRegion(file, -1, mmap.RDWR, 0, 0)
	if err != nil {
		file.Close()
		return errors.Wrapf(err, "failed to map fragment file %s", fr.filePath)
	}
	metrics.WpFragmentLoadedGauge.WithLabelValues("0").Inc()

	// 读取文件头
	if !fr.validateHeader() {
		return errors.New("invalid fragment file header")
	}

	// 读取footer
	if err := fr.readFooter(); err != nil {
		return err
	}

	// set infoFetched
	fr.infoFetched = true

	//
	return nil
}

// validateHeader validates the file header.
func (fr *FragmentFileReader) validateHeader() bool {
	// 检查magic string
	if string(fr.mappedFile[0:8]) != "FRAGMENT" {
		return false
	}

	// 检查版本号
	version := binary.LittleEndian.Uint32(fr.mappedFile[8:12])
	return version == 1
}

// readFooter reads the file footer.
func (fr *FragmentFileReader) readFooter() error {
	footerOffset := uint32(fr.fileSize - footerSize)
	// 读取条目数量
	fr.entryCount = int32(binary.LittleEndian.Uint32(fr.mappedFile[footerOffset:]))

	// 读取第一个和最后一个条目ID
	fr.firstEntryID = int64(binary.LittleEndian.Uint64(fr.mappedFile[footerOffset+4:]))
	fr.lastEntryID = int64(binary.LittleEndian.Uint64(fr.mappedFile[footerOffset+12:]))

	// 读取write state
	fr.isGrowing = binary.LittleEndian.Uint32(fr.mappedFile[footerOffset+20:]) == 1
	logger.Ctx(context.Background()).Debug("read footer",
		zap.Int32("entryCount", fr.entryCount),
		zap.Int64("firstEntryID", fr.firstEntryID),
		zap.Int64("lastEntryID", fr.lastEntryID),
		zap.Bool("isGrowing", fr.isGrowing),
		zap.String("fragmentInst", fmt.Sprintf("%p", fr)))

	return nil
}

func (fr *FragmentFileReader) refreshFooter() error {
	// 读取文件头
	if !fr.validateHeader() {
		// unchanged
		return nil
	}
	return fr.readFooter()
}

// GetLastEntryId returns the last entry ID.
func (fr *FragmentFileReader) GetLastEntryId() (int64, error) {
	if fr.closed {
		return 0, errors.New("fragment file is closed")
	}

	if !fr.infoFetched {
		err := fr.Load(context.Background())
		if err != nil {
			return -1, err
		}
	} else if fr.isGrowing {
		// refresh
		err := fr.Load(context.Background())
		if err != nil {
			return -1, err
		}
	}

	return fr.lastEntryID, nil
}

// GetFirstEntryId returns the first entry ID.
func (fr *FragmentFileReader) GetFirstEntryId() (int64, error) {
	if fr.closed {
		return 0, errors.New("fragment file is closed")
	}

	if !fr.infoFetched {
		err := fr.Load(context.Background())
		if err != nil {
			return 0, err
		}
	}

	return fr.firstEntryID, nil
}

// GetLastModified returns the last modification time.
func (fr *FragmentFileReader) GetLastModified() int64 {
	info, err := os.Stat(fr.filePath)
	if err != nil {
		return 0
	}
	return info.ModTime().UnixNano() / 1e6
}

// GetEntry returns the entry at the specified ID.
func (fr *FragmentFileReader) GetEntry(entryId int64) ([]byte, error) {
	if fr.closed {
		logger.Ctx(context.Background()).Warn("failed to get entry from a closed fragment file",
			zap.String("filePath", fr.filePath),
			zap.Int64("fragmentId", fr.fragmentId),
			zap.Int64("readingEntryId", entryId))
		return nil, errors.New("fragment file is closed")
	}

	logger.Ctx(context.Background()).Debug("Try get entry from this fragment",
		zap.String("filePath", fr.filePath),
		zap.Int64("firstEntryId", fr.firstEntryID),
		zap.Int64("lastEntryId", fr.lastEntryID),
		zap.Int64("readingEntryId", entryId),
		zap.String("fragInst", fmt.Sprintf("%p", fr)))

	// load data if not loaded
	if !fr.infoFetched {
		err := fr.Load(context.Background())
		if err != nil {
			return nil, err
		}
	}

	// refresh footer info only
	if entryId > fr.lastEntryID && fr.isGrowing {
		err := fr.refreshFooter()
		if err != nil {
			return nil, err
		}
	}

	// 检查entryId是否在范围内
	if entryId < fr.firstEntryID || entryId > fr.lastEntryID {
		logger.Ctx(context.Background()).Debug("entry ID out of range",
			zap.Int64("requestedID", entryId),
			zap.Int64("firstEntryID", fr.firstEntryID),
			zap.Int64("lastEntryID", fr.lastEntryID))
		return nil, werr.ErrInvalidEntryId.WithCauseErrMsg(fmt.Sprintf("entry ID %d not in the range of this fragment", entryId))
	}

	// 计算索引位置 - 根据条目ID在索引区中的相对位置
	idxPos := uint32(fr.fileSize - footerSize - int64(indexItemSize)*(int64(entryId-fr.firstEntryID+1)))

	if idxPos < headerSize || idxPos >= uint32(fr.fileSize-footerSize) {
		logger.Ctx(context.Background()).Debug("Invalid index position",
			zap.Uint32("idxPos", idxPos),
			zap.Uint32("headerSize", headerSize),
			zap.Int64("fileSize", fr.fileSize),
			zap.Uint32("footerSize", footerSize))
		return nil, fmt.Errorf("invalid index position: %d", idxPos)
	}

	// 读取数据偏移量
	offset := binary.LittleEndian.Uint32(fr.mappedFile[idxPos:])
	if offset < headerSize || offset >= uint32(fr.fileSize-footerSize) {
		logger.Ctx(context.Background()).Debug("Invalid data offset",
			zap.Uint32("offset", offset),
			zap.Uint32("headerSize", headerSize),
			zap.Int64("fileSize", fr.fileSize))
		return nil, fmt.Errorf("invalid data offset: %d", offset)
	}

	// 读取数据长度
	length := binary.LittleEndian.Uint32(fr.mappedFile[offset:])
	if length == 0 || length > uint32(fr.fileSize-footerSize)-offset-8 {
		logger.Ctx(context.Background()).Debug("Invalid data length",
			zap.Uint32("length", length),
			zap.Uint32("offset", offset),
			zap.Int64("fileSize", fr.fileSize))
		return nil, fmt.Errorf("invalid data length: %d", length)
	}

	// 读取CRC (4字节)
	storedCRC := binary.LittleEndian.Uint32(fr.mappedFile[offset+4:])

	// 确定数据区域
	dataStart := offset + 8 // 跳过长度(4字节)和CRC(4字节)
	dataEnd := dataStart + length
	if dataEnd > uint32(fr.fileSize-footerSize) {
		logger.Ctx(context.Background()).Debug("Data region out of bounds",
			zap.Uint32("dataStart", dataStart),
			zap.Uint32("dataEnd", dataEnd),
			zap.Int64("fileSize", fr.fileSize))
		return nil, fmt.Errorf("data region out of bounds: %d-%d", dataStart, dataEnd)
	}

	// 读取数据
	data := make([]byte, length)
	copy(data, fr.mappedFile[dataStart:dataEnd])

	logger.Ctx(context.Background()).Debug("fragment读取数据完成",
		zap.String("fragmentFile", fr.filePath),
		zap.Int64("readingEntryId", entryId),
		zap.Uint32("start", dataStart),
		zap.Uint32("end", dataEnd),
		zap.Uint32("idxPos", idxPos),
		zap.Int("dataSize", len(data)),
		//zap.Any("data", data),
		zap.String("fragInst", fmt.Sprintf("%p", fr)))

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
func (fr *FragmentFileReader) IteratorPrint() error {
	if fr.closed {
		// Use context.Background() for logging since we don't have a context parameter
		logger.Ctx(context.Background()).Debug("Fragment file is closed")
		return errors.New("fragment file is closed")
	}

	// fetch info
	if !fr.infoFetched {
		err := fr.Load(context.Background())
		if err != nil {
			return err
		}
	}

	// 计算索引位置 - 根据条目ID在索引区中的相对位置
	for i := 0; i < int(fr.entryCount); i++ {

		idxPos := uint32(fr.fileSize - footerSize - int64(indexItemSize)*(int64(i+1)))
		if idxPos < headerSize || idxPos >= uint32(fr.fileSize-footerSize) {
			logger.Ctx(context.Background()).Debug("Invalid index position",
				zap.Uint32("idxPos", idxPos),
				zap.Uint32("headerSize", headerSize),
				zap.Int64("fileSize", fr.fileSize),
				zap.Uint32("footerSize", footerSize))
			return fmt.Errorf("invalid index position: %d", idxPos)
		}

		// 读取数据偏移量
		offset := binary.LittleEndian.Uint32(fr.mappedFile[idxPos:])
		if offset < headerSize || offset >= uint32(fr.fileSize) {
			logger.Ctx(context.Background()).Debug("Invalid data offset",
				zap.Uint32("offset", offset),
				zap.Uint32("headerSize", headerSize),
				zap.Int64("fileSize", fr.fileSize))
			return fmt.Errorf("invalid data offset: %d", offset)
		}

		// 读取数据长度
		length := binary.LittleEndian.Uint32(fr.mappedFile[offset:])
		if length == 0 || length > uint32(fr.fileSize)-offset-8 {
			logger.Ctx(context.Background()).Debug("Invalid data length",
				zap.Uint32("length", length),
				zap.Uint32("offset", offset),
				zap.Int64("fileSize", fr.fileSize))
			return fmt.Errorf("invalid data length: %d", length)
		}

		// 读取CRC (4字节)
		storedCRC := binary.LittleEndian.Uint32(fr.mappedFile[offset+4:])

		// 确定数据区域
		dataStart := offset + 8 // 跳过长度(4字节)和CRC(4字节)
		dataEnd := dataStart + length
		if dataEnd > uint32(fr.fileSize) {
			logger.Ctx(context.Background()).Debug("Data region out of bounds",
				zap.Uint32("dataStart", dataStart),
				zap.Uint32("dataEnd", dataEnd),
				zap.Int64("fileSize", fr.fileSize))
			return fmt.Errorf("data region out of bounds: %d-%d", dataStart, dataEnd)
		}

		// 读取数据
		data := make([]byte, length)
		copy(data, fr.mappedFile[dataStart:dataEnd])

		// 提取里面的 id和data
		actualID := int64(binary.LittleEndian.Uint64(data[:8]))
		actualData := data[8:]

		logger.Ctx(context.Background()).Debug("fragment读取数据",
			zap.String("fragmentFile", fr.filePath),
			zap.Int64("entryId", actualID),
			zap.Int64("segmentEntryId", fr.firstEntryID+int64(i)),
			zap.Uint32("start", dataStart),
			zap.Uint32("end", dataEnd),
			zap.Int("actualDataSize", len(actualData)),
			zap.Uint32("pos", idxPos),
			zap.Int("i", i),
			zap.Int64("firstId", fr.firstEntryID))

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
func (fr *FragmentFileReader) GetSize() int64 {
	return fr.fileSize
}

// Release releases the fragment file.
func (fr *FragmentFileReader) Release() error {
	fr.mu.Lock()
	defer fr.mu.Unlock()

	if fr.closed {
		return nil
	}

	// 解除内存映射
	if fr.mappedFile != nil {
		if err := fr.mappedFile.Unmap(); err != nil {
			return errors.Wrap(err, "failed to unmap fragment file")
		}
		fr.mappedFile = nil
		metrics.WpFragmentBufferBytes.WithLabelValues("0").Sub(float64(fr.GetSize()))
		metrics.WpFragmentLoadedGauge.WithLabelValues("0").Dec()
	}

	// mark data is not fetched in buff
	fr.infoFetched = false

	return nil
}

func (fr *FragmentFileReader) Close() {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	fr.closed = true
}

// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package disk

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"time"

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
	// Header size
	headerSize = 4 * 1024
	// Footer size
	footerSize = 4 * 1024
	// Size of each index item
	indexItemSize = 4
	// trace scope name
	FragmentScopeName = "DiskFragment"
)

// FragmentFileWriter manages fragment data write
type FragmentFileWriter struct {
	mu         sync.RWMutex
	logId      int64
	segmentId  int64
	fragmentId int64 // Unique identifier for the fragment
	filePath   string
	fileSize   int64
	mappedFile mmap.MMap
	fd         *os.File

	dataOffset  uint32 // Current data write position
	indexOffset uint32 // Current index write position

	firstEntryID int64 // ID of the first entry
	lastEntryID  int64 // ID of the last entry
	entryCount   int32 // Current number of entries
	isGrowing    bool  // Whether this fragment will continue to receive writes

	infoFetched bool
	closed      bool // Whether the file is closed
}

// NewFragmentFileWriter creates a new FragmentFile, which can write only
func NewFragmentFileWriter(ctx context.Context, filePath string, fileSize int64, logId int64, segmentId int64, fragmentId int64, firstEntryID int64) (*FragmentFileWriter, error) {
	fw := &FragmentFileWriter{
		logId:      logId,
		segmentId:  segmentId,
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

	start := time.Now()
	logIdStr := fmt.Sprintf("%d", logId)
	segmentIdStr := fmt.Sprintf("%d", segmentId)

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
	fw.fd = file

	// 映射文件到内存
	fw.mappedFile, err = mmap.MapRegion(file, -1, mmap.RDWR, 0, 0)
	if err != nil {
		file.Close()
		return nil, errors.Wrapf(err, "failed to map fragment file %s", filePath)
	}

	// 写入footer
	if err := fw.writeFooter(ctx); err != nil {
		fw.Release(ctx)
		return nil, err
	}
	// 写入header
	if err := fw.writeHeader(ctx); err != nil {
		fw.Release(ctx)
		return nil, err
	}

	logger.Ctx(ctx).Debug("FragmentFile created", zap.String("filePath", filePath), zap.Int64("fragmentId", fragmentId), zap.String("fragmentInst", fmt.Sprintf("%p", fw)))

	// update metrics
	metrics.WpFragmentLoadBytes.WithLabelValues(logIdStr, segmentIdStr).Add(float64(fileSize))
	metrics.WpFragmentLoadTotal.WithLabelValues(logIdStr, segmentIdStr).Inc()
	metrics.WpFragmentLoadLatency.WithLabelValues(logIdStr, segmentIdStr).Observe(float64(time.Since(start).Milliseconds()))
	return fw, nil
}

// writeHeader writes the file header.
func (fw *FragmentFileWriter) writeHeader(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "writeHeader")
	defer sp.End()
	// Write magic string
	copy(fw.mappedFile[0:8], []byte("FRAGMENT"))

	// Write version number
	binary.LittleEndian.PutUint32(fw.mappedFile[8:12], 1)

	return nil
}

func (fw *FragmentFileWriter) GetLogId() int64 {
	return fw.logId
}

func (fw *FragmentFileWriter) GetSegmentId() int64 {
	return fw.segmentId
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
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "Flush")
	defer sp.End()
	fw.mu.Lock()
	defer fw.mu.Unlock()
	start := time.Now()
	logId := fmt.Sprintf("%d", fw.logId)
	segmentId := fmt.Sprintf("%d", fw.segmentId)

	if fw.closed {
		return errors.New("fragment file is closed")
	}

	// write footer
	if err := fw.writeFooter(ctx); err != nil {
		return err
	}

	// flush content to OS
	if err := fw.mappedFile.Flush(); err != nil {
		return errors.Wrap(err, "failed to flush fragment file")
	}

	// sync OS file to disk
	if err := fw.fd.Sync(); err != nil {
		return errors.Wrap(err, "failed to sync fragment file")
	}

	metrics.WpFragmentFlushTotal.WithLabelValues(logId, segmentId).Inc()
	metrics.WpFragmentFlushLatency.WithLabelValues(logId, segmentId).Observe(float64(time.Since(start).Milliseconds()))
	return nil
}

// writeFooter writes the file footer.
func (fw *FragmentFileWriter) writeFooter(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "writeFooter")
	defer sp.End()
	footerOffset := uint32(fw.fileSize - footerSize)
	// Write entry count
	binary.LittleEndian.PutUint32(fw.mappedFile[footerOffset:], uint32(fw.entryCount))

	// Write first and last entry IDs
	binary.LittleEndian.PutUint64(fw.mappedFile[footerOffset+4:], uint64(fw.firstEntryID))
	binary.LittleEndian.PutUint64(fw.mappedFile[footerOffset+12:], uint64(fw.lastEntryID))

	// Write growing state
	isGrowingFlag := uint32(0)
	if fw.isGrowing {
		isGrowingFlag = uint32(1)
	}
	binary.LittleEndian.PutUint32(fw.mappedFile[footerOffset+20:], isGrowingFlag)
	logger.Ctx(ctx).Debug("write footer",
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
func (fw *FragmentFileWriter) GetLastEntryId(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "GetLastEntryId")
	defer sp.End()
	if fw.closed {
		return 0, errors.New("fragment file is closed")
	}
	return fw.lastEntryID, nil
}

// GetFirstEntryId returns the first entry ID.
func (fw *FragmentFileWriter) GetFirstEntryId(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "GetFirstEntryId")
	defer sp.End()
	if fw.closed {
		return 0, errors.New("fragment file is closed")
	}
	return fw.firstEntryID, nil
}

// GetLastModified returns the last modification time.
func (fw *FragmentFileWriter) GetLastModified(ctx context.Context) int64 {
	info, err := os.Stat(fw.filePath)
	if err != nil {
		return 0
	}
	return info.ModTime().UnixNano() / 1e6
}

// GetEntry returns the entry at the specified ID.
// NOTE: The result entry maybe not flush to disk yet.
func (fw *FragmentFileWriter) GetEntry(ctx context.Context, entryId int64) ([]byte, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "GetEntry")
	defer sp.End()
	if fw.closed {
		logger.Ctx(ctx).Warn("failed to get entry from a closed fragment file",
			zap.String("filePath", fw.filePath),
			zap.Int64("fragmentId", fw.fragmentId),
			zap.Int64("readingEntryId", entryId))
		return nil, errors.New("fragment file is closed")
	}

	logger.Ctx(ctx).Debug("Try get entry from this fragment",
		zap.String("filePath", fw.filePath),
		zap.Int64("firstEntryId", fw.firstEntryID),
		zap.Int64("lastEntryId", fw.lastEntryID),
		zap.Int64("readingEntryId", entryId),
		zap.String("fragInst", fmt.Sprintf("%p", fw)))

	// Check if entryId is within range
	if entryId < fw.firstEntryID || entryId > fw.lastEntryID {
		logger.Ctx(ctx).Debug("entry ID out of range",
			zap.Int64("requestedID", entryId),
			zap.Int64("firstEntryID", fw.firstEntryID),
			zap.Int64("lastEntryID", fw.lastEntryID))
		return nil, werr.ErrInvalidEntryId.WithCauseErrMsg(fmt.Sprintf("entry ID %d not in the range of this fragment", entryId))
	}

	// Calculate index position - relative position of entry ID in index area
	idxPos := uint32(fw.fileSize - footerSize - int64(indexItemSize)*(int64(entryId-fw.firstEntryID+1)))

	if idxPos < headerSize || idxPos >= uint32(fw.fileSize-footerSize) {
		logger.Ctx(ctx).Debug("Invalid index position",
			zap.Uint32("idxPos", idxPos),
			zap.Uint32("headerSize", headerSize),
			zap.Int64("fileSize", fw.fileSize),
			zap.Uint32("footerSize", footerSize))
		return nil, fmt.Errorf("invalid index position: %d", idxPos)
	}

	// Read data offset
	offset := binary.LittleEndian.Uint32(fw.mappedFile[idxPos:])
	if offset < headerSize || offset >= uint32(fw.fileSize-footerSize) {
		logger.Ctx(ctx).Debug("Invalid data offset",
			zap.Uint32("offset", offset),
			zap.Uint32("headerSize", headerSize),
			zap.Int64("fileSize", fw.fileSize))
		return nil, fmt.Errorf("invalid data offset: %d", offset)
	}

	// Read data length
	length := binary.LittleEndian.Uint32(fw.mappedFile[offset:])
	if length == 0 || length > uint32(fw.fileSize-footerSize)-offset-8 {
		logger.Ctx(ctx).Debug("Invalid data length",
			zap.Uint32("length", length),
			zap.Uint32("offset", offset),
			zap.Int64("fileSize", fw.fileSize))
		return nil, fmt.Errorf("invalid data length: %d", length)
	}

	// Read CRC (4 bytes)
	storedCRC := binary.LittleEndian.Uint32(fw.mappedFile[offset+4:])

	// Determine data region
	dataStart := offset + 8 // Skip length(4 bytes) and CRC(4 bytes)
	dataEnd := dataStart + length
	if dataEnd > uint32(fw.fileSize-footerSize) {
		logger.Ctx(ctx).Debug("Data region out of bounds",
			zap.Uint32("dataStart", dataStart),
			zap.Uint32("dataEnd", dataEnd),
			zap.Int64("fileSize", fw.fileSize))
		return nil, fmt.Errorf("data region out of bounds: %d-%d", dataStart, dataEnd)
	}

	// Read data
	data := make([]byte, length)
	copy(data, fw.mappedFile[dataStart:dataEnd])

	logger.Ctx(ctx).Debug("Fragment data read completed",
		zap.String("fragmentFile", fw.filePath),
		zap.Int64("readingEntryId", entryId),
		zap.Uint32("start", dataStart),
		zap.Uint32("end", dataEnd),
		zap.Uint32("idxPos", idxPos),
		zap.Int("dataSize", len(data)),
		zap.String("fragInst", fmt.Sprintf("%p", fw)))

	// Verify CRC
	if crc32.ChecksumIEEE(data) != storedCRC {
		logger.Ctx(ctx).Debug("CRC mismatch",
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

func (fw *FragmentFileWriter) GetRawBufSize() int64 {
	return fw.fileSize
}

// Release releases the fragment file.
func (fw *FragmentFileWriter) Release(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "Release")
	defer sp.End()
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.closed {
		return nil
	}

	// unmap the file
	if fw.mappedFile != nil {
		if err := fw.mappedFile.Unmap(); err != nil {
			return errors.Wrap(err, "failed to unmap fragment file")
		}
		fw.mappedFile = nil
	}

	// close the file
	if fw.fd != nil {
		if err := fw.fd.Close(); err != nil {
			logger.Ctx(ctx).Warn("failed to close fragment file", zap.String("filePath", fw.filePath))
		}
	}

	// mark data is not fetched in buff
	fw.infoFetched = false

	// update metrics
	logIdStr := fmt.Sprintf("%d", fw.logId)
	segmentIdStr := fmt.Sprintf("%d", fw.segmentId)
	metrics.WpFragmentLoadBytes.WithLabelValues(logIdStr, segmentIdStr).Sub(float64(fw.fileSize))
	metrics.WpFragmentLoadTotal.WithLabelValues(logIdStr, segmentIdStr).Dec()

	return nil
}

func (fw *FragmentFileWriter) Close() {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	fw.closed = true
}

// Write writes data to the fragment file.
func (fw *FragmentFileWriter) Write(ctx context.Context, data []byte, writeEntryId int64) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "Write")
	defer sp.End()
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.closed {
		logger.Ctx(ctx).Warn("failed to write entry, fragment is closed", zap.Int64("writingEntryId", writeEntryId), zap.String("fragInst", fmt.Sprintf("%p", fw)))
		return errors.New("fragment file is closed")
	}

	// Check if there's enough space
	requiredSpace := uint32(len(data) + 8) // Data length(4) + CRC(4) + data
	logger.Ctx(ctx).Debug("fragment writing entry",
		zap.Int64("fragmentId", fw.fragmentId),
		zap.Int64("firstEntryID", fw.firstEntryID),
		zap.Int64("lastEntryID", fw.lastEntryID),
		zap.Int32("entryCount", fw.entryCount),
		zap.Int64("writingEntryId", writeEntryId),
		zap.Uint32("requiredSpace", requiredSpace),
		zap.Uint32("dataOffset", fw.dataOffset),
		zap.Uint32("indexOffset", fw.indexOffset),
		zap.String("fragInst", fmt.Sprintf("%p", fw)))

	// Space required for index position
	indexSpace := int64(indexItemSize)
	if fw.entryCount > 0 {
		indexSpace = int64(indexItemSize) * (int64(fw.entryCount) + 1)
	}

	// Ensure there's enough space for data and index
	if int64(fw.dataOffset)+int64(requiredSpace) >= fw.fileSize-footerSize-indexSpace {
		logger.Ctx(ctx).Warn("Insufficient fragment space, cannot write",
			zap.Uint32("requiredSpace", requiredSpace),
			zap.Uint32("dataOffset", fw.dataOffset),
			zap.String("filePath", fw.filePath),
			zap.Int64("fileSize", fw.fileSize),
			zap.Int64("footerSize", footerSize),
			zap.Int64("indexSpace", indexSpace),
			zap.Int64("writeEntryId", writeEntryId))
		return werr.ErrDiskFragmentNoSpace
	}

	// Write data length (4 bytes)
	binary.LittleEndian.PutUint32(fw.mappedFile[fw.dataOffset:fw.dataOffset+4], uint32(len(data)))
	// Calculate CRC
	crc := crc32.ChecksumIEEE(data)
	// Write CRC (4 bytes)
	binary.LittleEndian.PutUint32(fw.mappedFile[fw.dataOffset+4:fw.dataOffset+8], crc)
	// Write data
	copy(fw.mappedFile[fw.dataOffset+8:fw.dataOffset+8+uint32(len(data))], data)

	// Current data offset
	currentDataOffset := fw.dataOffset

	// First write
	if fw.lastEntryID == -1 {
		fw.lastEntryID = writeEntryId
	} else if fw.lastEntryID+1 == writeEntryId {
		fw.lastEntryID = writeEntryId
	} else {
		logger.Ctx(ctx).Warn("fragment write data, lastEntryID auto-increment",
			zap.String("fragmentFile", fw.filePath),
			zap.Int64("writeEntryId", writeEntryId),
			zap.Int64("lastEntryID", fw.lastEntryID),
		)
		return werr.ErrInvalidEntryId.WithCauseErrMsg(fmt.Sprintf("writing the entry:%d is not the expected one: %d", writeEntryId, fw.lastEntryID+1))
	}

	// Calculate index position - place earliest entries at the end of index area, latest entries at the beginning
	// This ensures data is returned in write order when reading
	idxPos := fw.indexOffset

	// Safety check: ensure index area doesn't overlap with data area
	if idxPos <= fw.dataOffset+requiredSpace {
		logger.Ctx(ctx).Warn("Index position overlaps with data area",
			zap.Uint32("idxPos", idxPos),
			zap.Uint32("dataOffset", fw.dataOffset))
		return werr.ErrDiskFragmentNoSpace
	}

	// Write index - store data offset
	binary.LittleEndian.PutUint32(fw.mappedFile[idxPos:idxPos+indexItemSize], currentDataOffset)

	logger.Ctx(ctx).Debug("fragment data written",
		zap.String("fragmentFile", fw.filePath),
		zap.Int64("writingEntryId", writeEntryId),
		zap.Uint32("start", fw.dataOffset+8),
		zap.Uint32("end", fw.dataOffset+8+uint32(len(data))),
		zap.Uint32("idxPos", idxPos),
		zap.Int("dataSize", len(data)),
		zap.String("fragInst", fmt.Sprintf("%p", fw)))

	// Update internal state
	logger.Ctx(ctx).Debug("fragment data written, before index update",
		zap.Int64("writingEntryId", writeEntryId),
		zap.Uint32("dataOffset", fw.dataOffset),
		zap.Uint32("indexOffset", fw.indexOffset),
		zap.Uint32("requiredSpace", requiredSpace),
		zap.Int32("entryCount", fw.entryCount))
	fw.dataOffset += requiredSpace
	fw.indexOffset -= indexItemSize
	fw.entryCount++
	logger.Ctx(ctx).Debug("fragment data written, after index update",
		zap.Int64("writingEntryId", writeEntryId),
		zap.Uint32("dataOffset", fw.dataOffset),
		zap.Uint32("indexOffset", fw.indexOffset),
		zap.Int32("entryCount", fw.entryCount))

	return nil
}

var _ storage.Fragment = (*FragmentFileReader)(nil)

// FragmentFileReader manages fragment data read
type FragmentFileReader struct {
	mu         sync.RWMutex
	logId      int64
	segmentId  int64
	fragmentId int64 // Unique identifier for the fragment
	filePath   string
	fileSize   int64

	// data
	mappedFile mmap.MMap
	fd         *os.File

	// info
	firstEntryID int64 // ID of the first entry
	lastEntryID  int64 // ID of the last entry
	entryCount   int32 // Current number of entries
	isGrowing    bool  // Whether this fragment will continue to receive writes

	// status
	infoFetched bool
	dataLoaded  bool
	closed      bool // Whether the file is closed

	// data refCnt
	dataRefCnt int // The number of references to the fragment data used
}

// NewFragmentFileReader creates a new FragmentFile, which can read only
func NewFragmentFileReader(ctx context.Context, filePath string, fileSize int64, logId int64, segmentId int64, fragmentId int64) (*FragmentFileReader, error) {
	ff := &FragmentFileReader{
		filePath:   filePath,
		fileSize:   fileSize,
		logId:      logId,
		segmentId:  segmentId,
		fragmentId: fragmentId,

		// Variables below would be lazy loaded if needed
		entryCount:   0,
		firstEntryID: -1,
		lastEntryID:  -1,
		isGrowing:    true,

		infoFetched: false,
		dataLoaded:  false,
		closed:      false,

		dataRefCnt: 0, // mmap not open, initial reference count is zero
	}
	logger.Ctx(ctx).Debug("FragmentFile reader created", zap.String("filePath", filePath), zap.Int64("fragmentId", fragmentId), zap.String("fragmentInst", fmt.Sprintf("%p", ff)))
	return ff, nil
}

func (fr *FragmentFileReader) GetLogId() int64 {
	return fr.logId
}

func (fr *FragmentFileReader) GetSegmentId() int64 {
	return fr.segmentId
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
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "Load")
	defer sp.End()
	fr.mu.Lock()
	defer fr.mu.Unlock()
	// already loaded, no need to load again
	if fr.dataLoaded {
		// refresh info
		if fr.isGrowing {
			readFootErr := fr.readFooter(ctx)
			if readFootErr != nil {
				return readFootErr
			}
		}
		// Increase the reference count
		fr.dataRefCnt += 1
		logger.Ctx(ctx).Debug("fragment file loaded, inc ref", zap.Int64("fragmentId", fr.fragmentId), zap.String("filePath", fr.filePath), zap.Int("ref", fr.dataRefCnt), zap.String("fragInst", fmt.Sprintf("%p", fr)))
		return nil
	}

	start := time.Now()
	logId := fmt.Sprintf("%d", fr.logId)
	segmentId := fmt.Sprintf("%d", fr.segmentId)
	if fr.closed {
		return errors.New("fragment file is closed")
	}

	// Open file
	file, err := os.OpenFile(fr.filePath, os.O_RDWR, 0644)
	if err != nil {
		return errors.Wrapf(err, "failed to open fragment file %s", fr.filePath)
	}

	// Map file to memory
	fr.mappedFile, err = mmap.MapRegion(file, -1, mmap.RDWR, 0, 0)
	if err != nil {
		file.Close()
		return errors.Wrapf(err, "failed to map fragment file %s", fr.filePath)
	}

	// Read file header
	if validateHeaderErr := fr.validateHeader(ctx); validateHeaderErr != nil {
		fr.mappedFile.Unmap()
		fr.mappedFile = nil
		fr.fd.Close()
		fr.fd = nil
		return validateHeaderErr
	}

	// Read footer
	if readFootErr := fr.readFooter(ctx); readFootErr != nil {
		fr.mappedFile.Unmap()
		fr.mappedFile = nil
		fr.fd.Close()
		fr.fd = nil
		return readFootErr
	}

	// Set infoFetched flag
	fr.infoFetched = true
	fr.dataLoaded = true

	// Increase the reference count
	fr.dataRefCnt += 1

	logger.Ctx(ctx).Debug("fragment file load finish", zap.Int64("fragmentId", fr.fragmentId), zap.String("filePath", fr.filePath), zap.Int("ref", fr.dataRefCnt), zap.String("fragInst", fmt.Sprintf("%p", fr)))
	// update metrics
	metrics.WpFragmentLoadBytes.WithLabelValues(logId, segmentId).Add(float64(fr.fileSize))
	metrics.WpFragmentLoadTotal.WithLabelValues(logId, segmentId).Inc()
	metrics.WpFragmentLoadLatency.WithLabelValues(logId, segmentId).Observe(float64(time.Since(start).Milliseconds()))
	return nil
}

// validateHeader validates the file header.
func (fr *FragmentFileReader) validateHeader(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "validateHeader")
	defer sp.End()
	if int64(len(fr.mappedFile)) < fr.fileSize {
		logger.Ctx(ctx).Warn("invalid file size retries, file maybe creating",
			zap.String("filePath", fr.filePath),
			zap.Int64("expectedSize", fr.fileSize),
			zap.Int64("actualSize", int64(len(fr.mappedFile))))
		return errors.New("invalid file size, file maybe creating")
	}

	// Check magic string
	if string(fr.mappedFile[0:8]) != "FRAGMENT" {
		logger.Ctx(ctx).Warn("invalid magic bytes, file maybe creating", zap.String("filePath", fr.filePath), zap.Int64("fileSize", fr.fileSize))
		return errors.New("invalid magic bytes, file maybe creating")
	}

	// Check version number
	version := binary.LittleEndian.Uint32(fr.mappedFile[8:12])
	if version != 1 {
		return errors.New("invalid version number, only support version 1")
	}
	return nil
}

// readFooter reads the file footer.
func (fr *FragmentFileReader) readFooter(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "readFooter")
	defer sp.End()
	if int64(len(fr.mappedFile)) < fr.fileSize {
		logger.Ctx(ctx).Warn("invalid file size after retries, file maybe creating",
			zap.String("filePath", fr.filePath),
			zap.Int64("expectedSize", fr.fileSize),
			zap.Int64("actualSize", int64(len(fr.mappedFile))))
		return errors.New("invalid file size, file maybe creating")
	}

	footerOffset := fr.fileSize - footerSize
	if footerOffset > fr.fileSize || footerOffset <= 0 {
		logger.Ctx(ctx).Warn("invalid footer offset, file maybe creating",
			zap.String("filePath", fr.filePath),
			zap.Int64("fileSize", fr.fileSize),
			zap.Int64("footerOffset", footerOffset))
		return errors.New("invalid footer offset, file maybe creating")
	}

	// Read entry count
	fr.entryCount = int32(binary.LittleEndian.Uint32(fr.mappedFile[footerOffset:]))

	// Read first and last entry IDs
	fr.firstEntryID = int64(binary.LittleEndian.Uint64(fr.mappedFile[footerOffset+4:]))
	fr.lastEntryID = int64(binary.LittleEndian.Uint64(fr.mappedFile[footerOffset+12:]))

	// Read write state
	fr.isGrowing = binary.LittleEndian.Uint32(fr.mappedFile[footerOffset+20:]) == 1
	logger.Ctx(ctx).Debug("read footer",
		zap.Int32("entryCount", fr.entryCount),
		zap.Int64("firstEntryID", fr.firstEntryID),
		zap.Int64("lastEntryID", fr.lastEntryID),
		zap.Bool("isGrowing", fr.isGrowing),
		zap.String("fragmentInst", fmt.Sprintf("%p", fr)))

	return nil
}

// IsMMapReadable check and release resource immediately, avoid memory surges when catching up read
func (fr *FragmentFileReader) IsMMapReadable(ctx context.Context) bool {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "IsMMapReadable")
	defer sp.End()
	fr.mu.Lock()
	defer fr.mu.Unlock()

	// Add retry logic for file size check with increasing intervals
	// Define increasing retry intervals
	retryIntervals := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		20 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		100 * time.Millisecond,
		100 * time.Millisecond,
		100 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		200 * time.Millisecond,
		500 * time.Millisecond}
	isReady := false
	for i := 0; i < len(retryIntervals); i++ {
		// open file
		file, err := os.OpenFile(fr.filePath, os.O_RDWR, 0644)
		if err != nil {
			logger.Ctx(ctx).Warn("failed to open fragment file %",
				zap.String("fragmentPath", fr.filePath),
				zap.Error(err),
				zap.Int("attempt", i+1),
				zap.Duration("retryAfter", retryIntervals[i]))

			// Use the appropriate retry interval for current attempt
			retryInterval := retryIntervals[i]
			time.Sleep(retryInterval)
			continue
		}
		// map file to memory
		fr.mappedFile, err = mmap.MapRegion(file, -1, mmap.RDWR, 0, 0)
		if err != nil {
			file.Close()
			logger.Ctx(ctx).Warn("failed to map fragment file",
				zap.String("fragmentPath", fr.filePath),
				zap.Error(err),
				zap.Int("attempt", i+1),
				zap.Duration("retryAfter", retryIntervals[i]))

			// Use the appropriate retry interval for current attempt
			retryInterval := retryIntervals[i]
			time.Sleep(retryInterval)
			continue
		}
		// check header
		if validateHeaderErr := fr.validateHeader(ctx); validateHeaderErr != nil {
			fr.mappedFile.Unmap()
			fr.mappedFile = nil
			file.Close()
			logger.Ctx(ctx).Warn("failed to validate header",
				zap.String("fragmentPath", fr.filePath),
				zap.Error(validateHeaderErr),
				zap.Int("attempt", i+1),
				zap.Duration("retryAfter", retryIntervals[i]))

			// Use the appropriate retry interval for current attempt
			retryInterval := retryIntervals[i]
			time.Sleep(retryInterval)
			continue
		}

		// check footer
		if readFootErr := fr.readFooter(ctx); readFootErr != nil {
			fr.mappedFile.Unmap()
			fr.mappedFile = nil
			file.Close()

			logger.Ctx(ctx).Warn("failed to read footer",
				zap.String("fragmentPath", fr.filePath),
				zap.Error(readFootErr),
				zap.Int("attempt", i+1),
				zap.Duration("retryAfter", retryIntervals[i]))

			// Use the appropriate retry interval for current attempt
			retryInterval := retryIntervals[i]
			time.Sleep(retryInterval)
			continue
		}

		// close resource after check
		fr.mappedFile.Unmap()
		fr.mappedFile = nil
		file.Close()

		// mark it is ready to read
		isReady = true
		// Set infoFetched flag
		fr.infoFetched = true
		fr.dataLoaded = false
		break
	}

	logger.Ctx(ctx).Debug("check fragment file ready state", zap.Int64("fragmentId", fr.fragmentId), zap.String("filePath", fr.filePath), zap.Bool("ready", isReady))
	return isReady
}

func (fr *FragmentFileReader) refreshFooter(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "refreshFooter")
	defer sp.End()
	// Read file header
	if validateHeaderErr := fr.validateHeader(ctx); validateHeaderErr != nil {
		logger.Ctx(ctx).Debug("refreshFooter: validate header failed", zap.String("filePath", fr.filePath), zap.String("fragmentInst", fmt.Sprintf("%p", fr)), zap.Error(validateHeaderErr))
		return validateHeaderErr
	}
	if readFooterErr := fr.readFooter(ctx); readFooterErr != nil {
		logger.Ctx(ctx).Debug("refreshFooter: read footer failed", zap.String("filePath", fr.filePath), zap.String("fragmentInst", fmt.Sprintf("%p", fr)), zap.Error(readFooterErr))
		return readFooterErr
	}
	logger.Ctx(ctx).Debug("refreshFooter: refresh footer finish", zap.String("filePath", fr.filePath), zap.String("fragmentInst", fmt.Sprintf("%p", fr)))
	return nil
}

// GetLastEntryId returns the last entry ID.
func (fr *FragmentFileReader) GetLastEntryId(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "GetLastEntryId")
	defer sp.End()
	fr.mu.RLock()
	defer fr.mu.RUnlock()
	if fr.closed {
		return 0, errors.New("fragment file is closed")
	}
	// if infoFetched and is now growing ,return
	if fr.infoFetched && !fr.isGrowing {
		lastEntryID := fr.lastEntryID
		return lastEntryID, nil
	}
	return -1, werr.ErrFragmentInfoNotFetched.WithCauseErrMsg(fmt.Sprintf("%s info not fetched", fr.filePath))
}

// GetFetchedLastEntryId returns the last refresh fetch lastEntry ID directly
func (fr *FragmentFileReader) GetFetchedLastEntryId(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "GetFetchedLastEntryId")
	defer sp.End()
	fr.mu.RLock()
	defer fr.mu.RUnlock()
	if fr.closed {
		return 0, errors.New("fragment file is closed")
	}
	// if infoFetched and is now growing ,return
	if fr.infoFetched {
		lastEntryID := fr.lastEntryID
		return lastEntryID, nil
	}
	return -1, werr.ErrFragmentInfoNotFetched.WithCauseErrMsg(fmt.Sprintf("%s info not fetched", fr.filePath))
}

// GetFirstEntryId returns the first entry ID.
func (fr *FragmentFileReader) GetFirstEntryId(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "GetFirstEntryId")
	defer sp.End()
	fr.mu.RLock()
	defer fr.mu.RUnlock()
	if fr.closed {
		return 0, errors.New("fragment file is closed")
	}
	// if infoFetched, return
	if fr.infoFetched {
		firstEntryID := fr.firstEntryID
		return firstEntryID, nil
	}
	return -1, werr.ErrFragmentInfoNotFetched.WithCauseErrMsg(fmt.Sprintf("%s info not fetched", fr.filePath))
}

// GetLastModified returns the last modification time.
func (fr *FragmentFileReader) GetLastModified(ctx context.Context) int64 {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "GetLastModified")
	defer sp.End()
	info, err := os.Stat(fr.filePath)
	if err != nil {
		return 0
	}
	return info.ModTime().UnixNano() / 1e6
}

// GetEntry returns the entry at the specified ID.
func (fr *FragmentFileReader) GetEntry(ctx context.Context, entryId int64) ([]byte, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "GetEntry")
	defer sp.End()
	fr.mu.RLock()
	defer fr.mu.RUnlock()
	if fr.closed {
		logger.Ctx(ctx).Warn("failed to get entry from a closed fragment file",
			zap.String("filePath", fr.filePath),
			zap.Int64("fragmentId", fr.fragmentId),
			zap.Int64("readingEntryId", entryId))
		return nil, errors.New("fragment file is closed")
	}
	if !fr.dataLoaded {
		return nil, werr.ErrFragmentNotLoaded.WithCauseErrMsg(fmt.Sprintf("%s not loaded", fr.filePath))
	}

	logger.Ctx(ctx).Debug("Try get entry from this fragment",
		zap.String("filePath", fr.filePath),
		zap.Int64("firstEntryId", fr.firstEntryID),
		zap.Int64("lastEntryId", fr.lastEntryID),
		zap.Int64("readingEntryId", entryId),
		zap.String("fragInst", fmt.Sprintf("%p", fr)))

	return fr.getEntryLocked(ctx, entryId)
}

// getEntryLocked is a helper method to retrieve an entry with lock already held
func (fr *FragmentFileReader) getEntryLocked(ctx context.Context, entryId int64) ([]byte, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "getEntryLocked")
	defer sp.End()
	// Calculate index position - relative position of entry ID in index area
	idxPos := uint32(fr.fileSize - footerSize - int64(indexItemSize)*(int64(entryId-fr.firstEntryID+1)))

	if idxPos < headerSize || idxPos >= uint32(fr.fileSize-footerSize) {
		logger.Ctx(ctx).Debug("Invalid index position",
			zap.Uint32("idxPos", idxPos),
			zap.Uint32("headerSize", headerSize),
			zap.Int64("fileSize", fr.fileSize),
			zap.Uint32("footerSize", footerSize))
		return nil, fmt.Errorf("invalid index position: %d", idxPos)
	}

	// Read data offset
	offset := binary.LittleEndian.Uint32(fr.mappedFile[idxPos:])
	if offset < headerSize || offset >= uint32(fr.fileSize-footerSize) {
		logger.Ctx(ctx).Debug("Invalid data offset",
			zap.Uint32("offset", offset),
			zap.Uint32("headerSize", headerSize),
			zap.Int64("fileSize", fr.fileSize))
		return nil, fmt.Errorf("invalid data offset: %d", offset)
	}

	// Read data length
	length := binary.LittleEndian.Uint32(fr.mappedFile[offset:])
	if length == 0 || length > uint32(fr.fileSize-footerSize)-offset-8 {
		logger.Ctx(ctx).Debug("Invalid data length",
			zap.Uint32("length", length),
			zap.Uint32("offset", offset),
			zap.Int64("fileSize", fr.fileSize))
		return nil, fmt.Errorf("invalid data length: %d", length)
	}

	// Read CRC (4 bytes)
	storedCRC := binary.LittleEndian.Uint32(fr.mappedFile[offset+4:])

	// Determine data region
	dataStart := offset + 8 // Skip length(4 bytes) and CRC(4 bytes)
	dataEnd := dataStart + length
	if dataEnd > uint32(fr.fileSize-footerSize) {
		logger.Ctx(ctx).Debug("Data region out of bounds",
			zap.Uint32("dataStart", dataStart),
			zap.Uint32("dataEnd", dataEnd),
			zap.Int64("fileSize", fr.fileSize))
		return nil, fmt.Errorf("data region out of bounds: %d-%d", dataStart, dataEnd)
	}

	// Read data
	data := make([]byte, length)
	copy(data, fr.mappedFile[dataStart:dataEnd])

	logger.Ctx(ctx).Debug("Fragment data read completed",
		zap.String("fragmentFile", fr.filePath),
		zap.Int64("readingEntryId", entryId),
		zap.Uint32("start", dataStart),
		zap.Uint32("end", dataEnd),
		zap.Uint32("idxPos", idxPos),
		zap.Int("dataSize", len(data)),
		zap.String("fragInst", fmt.Sprintf("%p", fr)))

	// Verify CRC
	if crc32.ChecksumIEEE(data) != storedCRC {
		logger.Ctx(ctx).Debug("CRC mismatch",
			zap.Int64("entryId", entryId),
			zap.Uint32("computedCRC", crc32.ChecksumIEEE(data)),
			zap.Uint32("storedCRC", storedCRC))
		return nil, fmt.Errorf("CRC mismatch for entry ID %d", entryId)
	}

	return data, nil
}

// IteratorPrint for Debug Test only
func (fr *FragmentFileReader) IteratorPrint() error {
	ctx := context.Background()
	if fr.closed {
		// Use context.Background() for logging since we don't have a context parameter
		logger.Ctx(ctx).Debug("Fragment file is closed")
		return errors.New("fragment file is closed")
	}

	// Fetch info
	if !fr.infoFetched {
		err := fr.Load(context.Background())
		if err != nil {
			return err
		}
	}

	// Calculate index position - relative position of entry ID in index area
	for i := 0; i < int(fr.entryCount); i++ {
		idxPos := uint32(fr.fileSize - footerSize - int64(indexItemSize)*(int64(i+1)))
		if idxPos < headerSize || idxPos >= uint32(fr.fileSize-footerSize) {
			logger.Ctx(ctx).Debug("Invalid index position",
				zap.Uint32("idxPos", idxPos),
				zap.Uint32("headerSize", headerSize),
				zap.Int64("fileSize", fr.fileSize),
				zap.Uint32("footerSize", footerSize))
			return fmt.Errorf("invalid index position: %d", idxPos)
		}

		// Read data offset
		offset := binary.LittleEndian.Uint32(fr.mappedFile[idxPos:])
		if offset < headerSize || offset >= uint32(fr.fileSize) {
			logger.Ctx(ctx).Debug("Invalid data offset",
				zap.Uint32("offset", offset),
				zap.Uint32("headerSize", headerSize),
				zap.Int64("fileSize", fr.fileSize))
			return fmt.Errorf("invalid data offset: %d", offset)
		}

		// Read data length
		length := binary.LittleEndian.Uint32(fr.mappedFile[offset:])
		if length == 0 || length > uint32(fr.fileSize)-offset-8 {
			logger.Ctx(ctx).Debug("Invalid data length",
				zap.Uint32("length", length),
				zap.Uint32("offset", offset),
				zap.Int64("fileSize", fr.fileSize))
			return fmt.Errorf("invalid data length: %d", length)
		}

		// Read CRC (4 bytes)
		storedCRC := binary.LittleEndian.Uint32(fr.mappedFile[offset+4:])

		// Determine data region
		dataStart := offset + 8 // Skip length(4 bytes) and CRC(4 bytes)
		dataEnd := dataStart + length
		if dataEnd > uint32(fr.fileSize) {
			logger.Ctx(ctx).Debug("Data region out of bounds",
				zap.Uint32("dataStart", dataStart),
				zap.Uint32("dataEnd", dataEnd),
				zap.Int64("fileSize", fr.fileSize))
			return fmt.Errorf("data region out of bounds: %d-%d", dataStart, dataEnd)
		}

		// Read data
		data := make([]byte, length)
		copy(data, fr.mappedFile[dataStart:dataEnd])

		// Extract id and data
		actualID := int64(binary.LittleEndian.Uint64(data[:8]))
		actualData := data[8:]

		logger.Ctx(ctx).Debug("Fragment data read",
			zap.String("fragmentFile", fr.filePath),
			zap.Int64("entryId", actualID),
			zap.Int64("segmentEntryId", fr.firstEntryID+int64(i)),
			zap.Uint32("start", dataStart),
			zap.Uint32("end", dataEnd),
			zap.Int("actualDataSize", len(actualData)),
			zap.Uint32("pos", idxPos),
			zap.Int("i", i),
			zap.Int64("firstId", fr.firstEntryID))

		// Verify CRC
		if crc32.ChecksumIEEE(data) != storedCRC {
			logger.Ctx(ctx).Debug("CRC mismatch",
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

func (fr *FragmentFileReader) GetRawBufSize() int64 {
	return fr.fileSize
}

// Release releases the fragment file.
func (fr *FragmentFileReader) Release(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "Release")
	defer sp.End()
	fr.mu.Lock()
	defer fr.mu.Unlock()

	if fr.closed {
		return nil
	}

	if !fr.dataLoaded {
		// no data to release
		return nil
	}

	// decrement reference count
	fr.dataRefCnt -= 1
	logger.Ctx(ctx).Debug("call fragment file release, dec ref", zap.Int64("fragmentId", fr.fragmentId), zap.String("filePath", fr.filePath), zap.Int("ref", fr.dataRefCnt), zap.String("fragInst", fmt.Sprintf("%p", fr)))

	if fr.dataRefCnt > 0 {
		// still in use, no need to release
		return nil
	}

	// Unmap memory mapping
	if fr.mappedFile != nil {
		if err := fr.mappedFile.Unmap(); err != nil {
			logger.Ctx(ctx).Warn("failed to unmap fragment file", zap.String("filePath", fr.filePath))
		}
		fr.mappedFile = nil
	}

	// close the file
	if fr.fd != nil {
		if err := fr.fd.Close(); err != nil {
			logger.Ctx(ctx).Warn("failed to close fragment file", zap.String("filePath", fr.filePath))
		}
		fr.fd = nil
	}

	// Mark data as not fetched in buffer
	fr.dataLoaded = false
	logger.Ctx(ctx).Debug("fragment file release finish", zap.Int64("fragmentId", fr.fragmentId), zap.String("filePath", fr.filePath), zap.Int("ref", fr.dataRefCnt), zap.String("fragInst", fmt.Sprintf("%p", fr)))

	// update metrics
	logIdStr := fmt.Sprintf("%d", fr.logId)
	segmentIdStr := fmt.Sprintf("%d", fr.segmentId)
	metrics.WpFragmentLoadBytes.WithLabelValues(logIdStr, segmentIdStr).Sub(float64(fr.fileSize))
	metrics.WpFragmentLoadTotal.WithLabelValues(logIdStr, segmentIdStr).Dec()

	return nil
}

func (fr *FragmentFileReader) Close() {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	fr.closed = true
}

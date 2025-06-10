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

package objectstorage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/codec"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
)

const (
	FragmentVersion   = 1
	FragmentScopeName = "Fragment"
)

var _ storage.Fragment = (*FragmentObject)(nil)

// FragmentObject uses MinIO for object storage.
type FragmentObject struct {
	mu     sync.RWMutex
	client minioHandler.MinioHandler

	// info
	bucket       string
	logId        int64
	segmentId    int64
	fragmentId   int64
	fragmentKey  string
	firstEntryId int64 // First entryId in the fragment
	lastEntryId  int64 // Last entryId in the fragment, inclusive
	lastModified int64 // last modified time

	// data
	entriesData []byte // Bytes of entries
	indexes     []byte // Every 8 bytes represent one index, where each index consists of an offset and a length. The high 32 bits represent the offset, and the low 32 bits represent the length.
	size        int64  // Size of the fragment, including entries and indexes. it is lock free, used for metrics only
	rawBufSize  int64  // Size of the raw pooling buffer allocated for this fragment, cap(entriesData) + cap(indexes)

	// status
	dataLoaded   bool // If this fragment has been loaded to memory
	dataUploaded bool // If this fragment has been uploaded to MinIO
	infoFetched  bool // if info of this fragment has been fetched from MinIO

	// data refCnt
	dataRefCnt int // The number of references to the fragment data used
}

// NewFragmentObject initializes a new FragmentObject.
func NewFragmentObject(ctx context.Context, client minioHandler.MinioHandler, bucket string, logId int64, segmentId int64, fragmentId int64, fragmentKey string, entries []*cache.BufferEntry, firstEntryId int64, dataLoaded, dataUploaded, infoFetched bool) *FragmentObject {
	index, data := genFragmentDataFromRaw(ctx, entries)
	lastEntryId := firstEntryId + int64(len(entries)) - 1
	size := int64(len(data) + len(index))
	rawBufSize := int64(cap(data) + cap(index))
	//metrics.WpFragmentBufferBytes.WithLabelValues(bucket).Add(float64(len(data) + len(index)))
	//metrics.WpFragmentLoadedGauge.WithLabelValues(bucket).Inc()
	f := &FragmentObject{
		client:       client,
		bucket:       bucket,
		logId:        logId,
		segmentId:    segmentId,
		fragmentId:   fragmentId,
		fragmentKey:  fragmentKey,
		entriesData:  data,
		indexes:      index,
		size:         size,
		rawBufSize:   rawBufSize,
		firstEntryId: firstEntryId,
		lastEntryId:  lastEntryId,
		lastModified: time.Now().UnixMilli(),
		dataLoaded:   dataLoaded,
		dataUploaded: dataUploaded,
		infoFetched:  infoFetched,
	}
	if len(data) > 0 {
		// someone create a fragment with data, init the reference count
		f.dataRefCnt = 1
	}
	return f
}

func (f *FragmentObject) GetLogId() int64 {
	return f.logId
}

func (f *FragmentObject) GetSegmentId() int64 {
	return f.segmentId
}

func (f *FragmentObject) GetFragmentId() int64 {
	// This field is immutable after initialization, so no lock is needed
	return int64(f.fragmentId)
}

func (f *FragmentObject) GetFragmentKey() string {
	// This field is immutable after initialization, so no lock is needed
	return f.fragmentKey
}

func (f *FragmentObject) GetSize() int64 {
	return f.size
}

func (f *FragmentObject) GetRawBufSize() int64 {
	return f.rawBufSize
}

// Flush uploads the data to MinIO.
func (f *FragmentObject) Flush(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "Flush")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()
	start := time.Now()
	logId := fmt.Sprintf("%d", f.logId)
	segId := fmt.Sprintf("%d", f.segmentId)
	if !f.dataLoaded {
		return werr.ErrFragmentEmpty
	}

	if len(f.entriesData) == 0 {
		return werr.ErrFragmentEmpty
	}

	fullDataReader, fullDataSize := serializeFragmentToReader(ctx, f)

	// Upload
	_, err := f.client.PutObject(ctx, f.bucket, f.fragmentKey, fullDataReader, int64(fullDataSize), minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}
	cost := time.Now().Sub(start)
	metrics.WpFragmentFlushTotal.WithLabelValues(logId, segId).Inc()
	metrics.WpFragmentFlushLatency.WithLabelValues(logId, segId).Observe(float64(cost.Milliseconds()))
	metrics.WpFragmentFlushBytes.WithLabelValues(logId, segId).Add(float64(fullDataSize))
	f.dataUploaded = true
	return nil
}

// Load reads the data from MinIO.
func (f *FragmentObject) Load(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "Load")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()
	// already loaded, no need to load again
	if f.dataLoaded {
		// Increase the reference count
		f.dataRefCnt += 1
		return nil
	}
	if !f.dataUploaded {
		return werr.ErrFragmentNotUploaded
	}

	start := time.Now()
	logId := fmt.Sprintf("%d", f.logId)
	segId := fmt.Sprintf("%d", f.segmentId)
	fragObjectReader, objDataSize, objLastModified, err := f.client.GetObjectDataAndInfo(ctx, f.bucket, f.fragmentKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object: %w", err)
	}
	defer fragObjectReader.Close()
	f.lastModified = objLastModified

	// Read the entire object into memory
	data, err := minioHandler.ReadObjectFull(ctx, fragObjectReader, objDataSize)
	if err != nil || len(data) != int(objDataSize) {
		return fmt.Errorf("failed to read object: %v", err)
	}

	tmpFrag, deserializeErr := deserializeFragment(ctx, data, objDataSize)
	if deserializeErr != nil {
		return deserializeErr
	}

	// Reset the buffer with the loaded data
	f.entriesData = tmpFrag.entriesData
	f.indexes = tmpFrag.indexes
	f.size = int64(len(tmpFrag.entriesData) + len(tmpFrag.indexes))
	f.rawBufSize = int64(cap(tmpFrag.entriesData) + cap(tmpFrag.indexes))
	f.firstEntryId = tmpFrag.firstEntryId
	f.lastEntryId = tmpFrag.lastEntryId
	f.dataLoaded = true
	f.dataUploaded = true
	f.infoFetched = true
	f.dataRefCnt += 1 // Increase the reference count

	// update metrics
	metrics.WpFragmentLoadTotal.WithLabelValues(logId, segId).Inc()
	metrics.WpFragmentLoadBytes.WithLabelValues(logId, segId).Add(float64(f.GetSize()))
	metrics.WpFragmentLoadLatency.WithLabelValues(logId, segId).Observe(float64(time.Since(start).Milliseconds()))

	return nil
}

func (f *FragmentObject) LoadSizeStateOnly(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "LoadSizeStateOnly")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.size > 0 {
		return f.size, nil
	}
	objInfo, err := f.client.StatObject(ctx, f.bucket, f.fragmentKey, minio.StatObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to get object: %w", err)
	}
	return objInfo.Size, nil
}

func (f *FragmentObject) GetLastEntryId(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "GetLastEntryId")
	defer sp.End()
	// First check with read lock
	f.mu.RLock()
	defer f.mu.RUnlock()
	if !f.infoFetched {
		return -1, werr.ErrFragmentInfoNotFetched.WithCauseErrMsg(fmt.Sprintf("%s info not fetched", f.fragmentKey))
	}
	return f.lastEntryId, nil
}

func (f *FragmentObject) GetFirstEntryId(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "GetFirstEntryId")
	defer sp.End()
	// First check with read lock
	f.mu.RLock()
	defer f.mu.RUnlock()
	if !f.infoFetched {
		return -1, werr.ErrFragmentInfoNotFetched.WithCauseErrMsg(fmt.Sprintf("%s info not fetched", f.fragmentKey))
	}
	return f.firstEntryId, nil
}

func (f *FragmentObject) GetLastModified(ctx context.Context) int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastModified
}

func (f *FragmentObject) GetEntry(ctx context.Context, entryId int64) ([]byte, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "GetEntry")
	defer sp.End()
	// First check if we need to load data
	f.mu.RLock()
	defer f.mu.RUnlock()
	if !f.dataLoaded {
		return nil, werr.ErrFragmentNotLoaded.WithCauseErrMsg(fmt.Sprintf("%s not loaded", f.fragmentKey))
	}
	relatedIdx := (entryId - f.firstEntryId) * 8
	if relatedIdx+8 > int64(len(f.indexes)) {
		return nil, werr.ErrEntryNotFound
	}
	// get entry offset and length
	entryOffset := binary.BigEndian.Uint32(f.indexes[relatedIdx : relatedIdx+4])
	entryLength := binary.BigEndian.Uint32(f.indexes[relatedIdx+4 : relatedIdx+8])
	// copy entry data
	if entryOffset+entryLength > uint32(len(f.entriesData)) {
		logger.Ctx(ctx).Debug("Entry offset out of bounds, maybe file corrupted",
			zap.Uint32("entryOffset", entryOffset),
			zap.Uint32("entryLength", entryLength),
			zap.Int64("fragmentSize", int64(len(f.entriesData))))
		return nil, werr.ErrSegmentReadException
	}
	result := make([]byte, entryLength)
	copy(result, f.entriesData[entryOffset:entryOffset+entryLength])
	// return entry data
	return result, nil
}

// Release releases the memory used by the fragment.
func (f *FragmentObject) Release(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "Release")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.dataLoaded {
		// empty, no need to release again
		return nil
	}
	// decrement reference count
	f.dataRefCnt -= 1

	if f.dataRefCnt > 0 {
		// still in use, no need to release
		return nil
	}

	// release memory
	f.indexes = nil
	f.entriesData = nil
	f.dataLoaded = false

	// update metrics
	logId := fmt.Sprintf("%d", f.logId)
	segId := fmt.Sprintf("%d", f.segmentId)
	metrics.WpFragmentLoadTotal.WithLabelValues(logId, segId).Dec()
	metrics.WpFragmentLoadBytes.WithLabelValues(logId, segId).Sub(float64(f.GetSize()))

	return nil
}

// AppendToMergeTarget append the fragment data to a mergeTarget fragment
func (f *FragmentObject) AppendToMergeTarget(ctx context.Context, mergeTarget *FragmentObject, baseOffset int64) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "AppendToMergeTarget")
	defer sp.End()
	f.mu.RLock()
	defer f.mu.RUnlock()
	if !f.dataLoaded {
		logger.Ctx(ctx).Warn("fragment not loaded", zap.String("fragmentKey", f.fragmentKey))
		return werr.ErrFragmentNotLoaded.WithCauseErrMsg(fmt.Sprintf("%s not loaded", f.fragmentKey))
	}

	// write index to pendingMergedFragment
	for index := 0; index < len(f.indexes); index = index + 8 {
		newEntryOffset := binary.BigEndian.Uint32(f.indexes[index:index+4]) + uint32(baseOffset)
		entryLength := binary.BigEndian.Uint32(f.indexes[index+4 : index+8])

		newIndex := make([]byte, 8)
		binary.BigEndian.PutUint32(newIndex[:4], newEntryOffset)
		binary.BigEndian.PutUint32(newIndex[4:], entryLength)

		mergeTarget.indexes = append(mergeTarget.indexes, newIndex...)
	}
	// write data to pendingMergedFragment
	mergeTarget.entriesData = append(mergeTarget.entriesData, f.entriesData...)
	return nil
}

func serializeFragmentToReader(ctx context.Context, f *FragmentObject) (io.Reader, int) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "serializeFragment")
	defer sp.End()
	// Calculate required space
	headerSize := 24 // 3 int64 fields (version+firstEntryId+lastEntryId)
	totalSize := headerSize + len(f.indexes) + len(f.entriesData)
	header := make([]byte, headerSize)

	// Write header information
	copy(header[0:], codec.Int64ToBytes(FragmentVersion))
	copy(header[8:], codec.Int64ToBytes(f.firstEntryId))
	copy(header[16:], codec.Int64ToBytes(f.lastEntryId))

	return io.MultiReader(
		bytes.NewReader(header),
		bytes.NewReader(f.indexes),
		bytes.NewReader(f.entriesData),
	), totalSize
}

// deserializeFragment from object data bytes
func deserializeFragment(ctx context.Context, data []byte, maxFragmentSize int64) (*FragmentObject, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "deserializeFragment")
	defer sp.End()
	// Create a buffer to read from the data
	buf := bytes.NewBuffer(data)

	// Read version (4 bytes)
	var version uint64
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read version: %v", err)
	}
	if version != FragmentVersion {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	// Read firstEntryID (8 bytes)
	var firstEntryID uint64
	if err := binary.Read(buf, binary.BigEndian, &firstEntryID); err != nil {
		return nil, fmt.Errorf("failed to read firstEntryID: %v", err)
	}

	// Read lastEntryId (8 bytes)
	var lastEntryId uint64
	if err := binary.Read(buf, binary.BigEndian, &lastEntryId); err != nil {
		return nil, fmt.Errorf("failed to read lastEntryId: %v", err)
	}

	// Calculate the number of indexes
	numIndexes := lastEntryId - firstEntryID + 1
	indexesTmp := make([]byte, numIndexes*8)
	// Read indexes (each index is 8 bytes)
	if err := binary.Read(buf, binary.BigEndian, &indexesTmp); err != nil {
		return nil, fmt.Errorf("failed to read index %v", err)
	}
	// Read entriesData (remaining data)
	entriesDataTmp := buf.Bytes()

	return &FragmentObject{
		indexes:      indexesTmp,
		entriesData:  entriesDataTmp,
		firstEntryId: int64(firstEntryID),
		lastEntryId:  int64(lastEntryId),
	}, nil
}

func genFragmentDataFromRaw(ctx context.Context, rawEntries []*cache.BufferEntry) ([]byte, []byte) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "genFragmentDataFromRaw")
	defer sp.End()
	// Calculate total data size
	entriesCount := len(rawEntries)
	if entriesCount == 0 {
		return make([]byte, 0), make([]byte, 0)
	}

	// Calculate total data and index size
	totalDataSize := 0
	for _, item := range rawEntries {
		totalDataSize += len(item.Data)
	}
	indexSize := entriesCount * 8 // Each index entry occupies 8 bytes (offset+length)

	// Get buffers from byte pool
	data := make([]byte, 0, totalDataSize)
	index := make([]byte, 0, indexSize)

	// Temporary index buffer, reuse to reduce memory allocation
	entryIndex := make([]byte, 8)

	// Fill data and indexes
	offset := 0
	for i := 0; i < entriesCount; i++ {
		item := rawEntries[i]
		entryLength := uint32(len(item.Data))
		entryOffset := uint32(offset)

		// Fill index data
		binary.BigEndian.PutUint32(entryIndex[:4], entryOffset)
		binary.BigEndian.PutUint32(entryIndex[4:], entryLength)

		// Add to index buffer
		index = append(index, entryIndex...)

		// Add to data buffer
		data = append(data, item.Data...)

		offset += len(item.Data)
	}

	// No need to return buffers before returning, as they will be held by FragmentObject
	// They will be returned to the pool when FragmentObject is released
	return index, data
}

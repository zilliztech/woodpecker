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

package legacy

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/errors"
	"io"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	minioHandler "github.com/zilliztech/woodpecker/common/minio"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

const (
	FragmentVersion   = 1
	FragmentScopeName = "Fragment"
)

var _ storage.Fragment = (*LegacyFragmentObject)(nil)

// LegacyFragmentObject uses MinIO for object storage.
type LegacyFragmentObject struct {
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

// NewLegacyFragmentObject initializes a new LegacyFragmentObject.
func NewLegacyFragmentObject(ctx context.Context, client minioHandler.MinioHandler, bucket string, logId int64, segmentId int64, fragmentId int64, fragmentKey string, entries []*cache.BufferEntry, firstEntryId int64, dataLoaded, dataUploaded, infoFetched bool) *LegacyFragmentObject {
	index, data := genFragmentDataFromRaw(ctx, entries)
	lastEntryId := firstEntryId + int64(len(entries)) - 1
	size := int64(len(data) + len(index))
	rawBufSize := int64(cap(data) + cap(index))
	//metrics.WpFragmentBufferBytes.WithLabelValues(bucket).Add(float64(len(data) + len(index)))
	//metrics.WpFragmentLoadedGauge.WithLabelValues(bucket).Inc()
	f := &LegacyFragmentObject{
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

func (f *LegacyFragmentObject) GetLogId() int64 {
	return f.logId
}

func (f *LegacyFragmentObject) GetSegmentId() int64 {
	return f.segmentId
}

func (f *LegacyFragmentObject) GetFragmentId() int64 {
	// This field is immutable after initialization, so no lock is needed
	return int64(f.fragmentId)
}

func (f *LegacyFragmentObject) GetFragmentKey() string {
	// This field is immutable after initialization, so no lock is needed
	return f.fragmentKey
}

func (f *LegacyFragmentObject) GetSize() int64 {
	return f.size
}

func (f *LegacyFragmentObject) GetRawBufSize() int64 {
	return f.rawBufSize
}

// Flush uploads the data to MinIO.
func (f *LegacyFragmentObject) Flush(ctx context.Context) error {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "Flush")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()
	start := time.Now()
	logId := fmt.Sprintf("%d", f.logId)
	if !f.dataLoaded {
		logger.Ctx(ctx).Warn("should not flush a fragment without data", zap.String("fragmentKey", f.fragmentKey))
		return werr.ErrFragmentNotLoaded.WithCauseErrMsg("should not flush a fragment without data")
	}

	if len(f.entriesData) == 0 {
		logger.Ctx(ctx).Warn("should not flush an empty fragment", zap.String("fragmentKey", f.fragmentKey))
		return werr.ErrFragmentEmpty.WithCauseErrMsg("should not flush an empty fragment")
	}

	fullDataReader, fullDataSize := serializeFragmentToReader(ctx, f)

	// Upload
	_, err := f.client.PutObjectIfNoneMatch(ctx, f.bucket, f.fragmentKey, fullDataReader, int64(fullDataSize))
	if err != nil {
		if werr.ErrSegmentFenced.Is(err) {
			logger.Ctx(ctx).Info("fragment already fenced", zap.String("fragmentKey", f.fragmentKey))
			return werr.ErrSegmentFenced.WithCauseErrMsg("fragment already fenced")
		}
		if werr.ErrFragmentAlreadyExists.Is(err) {
			logger.Ctx(ctx).Info("fragment already uploaded", zap.String("fragmentKey", f.fragmentKey))
			f.dataUploaded = true
			return nil
		}
		logger.Ctx(ctx).Warn("failed to upload fragment", zap.String("fragmentKey", f.fragmentKey), zap.Error(err))
		return err
	}

	cost := time.Now().Sub(start)
	metrics.WpFragmentFlushTotal.WithLabelValues(logId).Inc()
	metrics.WpFragmentFlushLatency.WithLabelValues(logId).Observe(float64(cost.Milliseconds()))
	metrics.WpFragmentFlushBytes.WithLabelValues(logId).Add(float64(fullDataSize))
	f.dataUploaded = true

	// flush success
	return nil
}

// Load reads the data from MinIO.
func (f *LegacyFragmentObject) Load(ctx context.Context) error {
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
		logger.Ctx(ctx).Warn("should not load a fragment that has not been uploaded", zap.String("fragmentKey", f.fragmentKey))
		return werr.ErrFragmentNotUploaded.WithCauseErrMsg("should not load a fragment that has not been uploaded")
	}

	start := time.Now()
	logId := fmt.Sprintf("%d", f.logId)
	fragObjectReader, objDataSize, objLastModified, err := f.client.GetObjectDataAndInfo(ctx, f.bucket, f.fragmentKey, minio.GetObjectOptions{})
	if err != nil {
		logger.Ctx(ctx).Warn("failed to get object", zap.String("fragmentKey", f.fragmentKey), zap.Error(err))
		return err
	}
	defer fragObjectReader.Close()
	f.lastModified = objLastModified

	// Read the entire object into memory
	data, err := minioHandler.ReadObjectFull(ctx, fragObjectReader, objDataSize)
	if err != nil || len(data) != int(objDataSize) {
		logger.Ctx(ctx).Warn("failed to read object", zap.String("fragmentKey", f.fragmentKey), zap.Error(err))
		return err
	}

	tmpFrag, deserializeErr := deserializeFragment(ctx, data)
	if deserializeErr != nil {
		logger.Ctx(ctx).Warn("failed to deserialize fragment", zap.String("fragmentKey", f.fragmentKey), zap.Error(deserializeErr))
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
	metrics.WpFragmentLoadTotal.WithLabelValues(logId).Inc()
	metrics.WpFragmentLoadBytes.WithLabelValues(logId).Add(float64(f.GetSize()))
	metrics.WpFragmentLoadLatency.WithLabelValues(logId).Observe(float64(time.Since(start).Milliseconds()))

	return nil
}

func (f *LegacyFragmentObject) LoadSizeStateOnly(ctx context.Context) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "LoadSizeStateOnly")
	defer sp.End()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.size > 0 {
		return f.size, nil
	}
	objInfo, err := f.client.StatObject(ctx, f.bucket, f.fragmentKey, minio.StatObjectOptions{})
	if err != nil {
		logger.Ctx(ctx).Warn("failed to get object stat", zap.String("fragmentKey", f.fragmentKey), zap.Error(err))
		return 0, err
	}
	return objInfo.Size, nil
}

func (f *LegacyFragmentObject) GetLastEntryId(ctx context.Context) (int64, error) {
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

func (f *LegacyFragmentObject) GetFirstEntryId(ctx context.Context) (int64, error) {
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

func (f *LegacyFragmentObject) GetLastModified(ctx context.Context) int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastModified
}

func (f *LegacyFragmentObject) GetEntry(ctx context.Context, entryId int64) ([]byte, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "GetEntry")
	defer sp.End()
	// First check if we need to load data
	f.mu.RLock()
	defer f.mu.RUnlock()
	if !f.dataLoaded {
		logger.Ctx(ctx).Warn("fail to get entry from a fragment that is not loaded", zap.String("fragmentKey", f.fragmentKey), zap.Int64("entryId", entryId))
		return nil, werr.ErrFragmentNotLoaded.WithCauseErrMsg(fmt.Sprintf("%s not loaded", f.fragmentKey))
	}
	relatedIdx := (entryId - f.firstEntryId) * 8
	if relatedIdx+8 > int64(len(f.indexes)) {
		logger.Ctx(ctx).Debug("entry not found", zap.String("fragmentKey", f.fragmentKey), zap.Int64("entryId", entryId))
		return nil, werr.ErrEntryNotFound
	}
	// get entry offset and length
	entryOffset := binary.BigEndian.Uint32(f.indexes[relatedIdx : relatedIdx+4])
	entryLength := binary.BigEndian.Uint32(f.indexes[relatedIdx+4 : relatedIdx+8])
	// copy entry data
	if entryOffset+entryLength > uint32(len(f.entriesData)) {
		logger.Ctx(ctx).Warn("Entry offset out of bounds, maybe file corrupted",
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
func (f *LegacyFragmentObject) Release(ctx context.Context) error {
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
	metrics.WpFragmentLoadTotal.WithLabelValues(logId).Dec()
	metrics.WpFragmentLoadBytes.WithLabelValues(logId).Sub(float64(f.GetSize()))

	return nil
}

// AppendToMergeTarget append the fragment data to a mergeTarget fragment
func (f *LegacyFragmentObject) AppendToMergeTarget(ctx context.Context, mergeTarget storage.Fragment, baseOffset int64) error {
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

		// TODO merge to New Format FragmentObject? or legacy fragment does not need to merge all the way, it will be truncated in the future
		mergeTarget.(*LegacyFragmentObject).indexes = append(mergeTarget.(*LegacyFragmentObject).indexes, newIndex...)
	}
	// write data to pendingMergedFragment
	mergeTarget.(*LegacyFragmentObject).entriesData = append(mergeTarget.(*LegacyFragmentObject).entriesData, f.entriesData...)
	return nil
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

	// No need to return buffers before returning, as they will be held by LegacyFragmentObject
	// They will be returned to the pool when LegacyFragmentObject is released
	return index, data
}

// serializeFragmentToReader from fragment object to reader
func serializeFragmentToReader(ctx context.Context, f *LegacyFragmentObject) (io.Reader, int) {
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
func deserializeFragment(ctx context.Context, data []byte) (*LegacyFragmentObject, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, FragmentScopeName, "deserializeFragment")
	defer sp.End()
	// Create a buffer to read from the data
	buf := bytes.NewBuffer(data)

	// Read version (4 bytes)
	var version uint64
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		logger.Ctx(ctx).Warn("failed to read version", zap.Error(err))
		return nil, fmt.Errorf("failed to read version: %v", err)
	}
	if version != FragmentVersion {
		logger.Ctx(ctx).Warn("unsupported version", zap.Uint64("version", version))
		return nil, werr.ErrFragmentVersion.WithCauseErrMsg(fmt.Sprintf("unsupported version: %d", version))
	}

	// Read firstEntryID (8 bytes)
	var firstEntryID uint64
	if err := binary.Read(buf, binary.BigEndian, &firstEntryID); err != nil {
		logger.Ctx(ctx).Warn("failed to read firstEntryID", zap.Error(err))
		return nil, fmt.Errorf("failed to read firstEntryID: %v", err)
	}

	// Read lastEntryId (8 bytes)
	var lastEntryId uint64
	if err := binary.Read(buf, binary.BigEndian, &lastEntryId); err != nil {
		logger.Ctx(ctx).Warn("failed to read lastEntryID", zap.Error(err))
		return nil, fmt.Errorf("failed to read lastEntryId: %v", err)
	}

	// Calculate the number of indexes
	numIndexes := lastEntryId - firstEntryID + 1
	indexesTmp := make([]byte, numIndexes*8)
	// Read indexes (each index is 8 bytes)
	if err := binary.Read(buf, binary.BigEndian, &indexesTmp); err != nil {
		logger.Ctx(ctx).Warn("failed to read index", zap.Error(err))
		return nil, fmt.Errorf("failed to read index %v", err)
	}
	// Read entriesData (remaining data)
	entriesDataTmp := buf.Bytes()

	return &LegacyFragmentObject{
		indexes:      indexesTmp,
		entriesData:  entriesDataTmp,
		firstEntryId: int64(firstEntryID),
		lastEntryId:  int64(lastEntryId),
	}, nil
}

func GetLastEntryIdWithoutDataLoadedIfPossible(ctx context.Context, fragment storage.Fragment) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, "Segment", "getLastEntryIdWithoutDataLoadedIfPossible")
	defer sp.End()
	lastEntryId, err := fragment.GetLastEntryId(ctx)
	if werr.ErrFragmentInfoNotFetched.Is(err) {
		loadErr := fragment.Load(ctx)
		if loadErr != nil {
			return -1, loadErr
		}
		defer fragment.Release(ctx)
		lastEntryId, err = fragment.GetLastEntryId(ctx)
		if err != nil {
			return -1, err
		}
	}
	return lastEntryId, nil
}

func GetFirstEntryIdWithoutDataLoadedIfPossible(ctx context.Context, fragment storage.Fragment) (int64, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, "Segment", "getFirstEntryIdWithoutDataLoadedIfPossible")
	defer sp.End()
	firstEntryId, err := fragment.GetFirstEntryId(ctx)
	if werr.ErrFragmentInfoNotFetched.Is(err) {
		loadErr := fragment.Load(ctx)
		if loadErr != nil {
			return -1, loadErr
		}
		defer fragment.Release(ctx)
		firstEntryId, err = fragment.GetFirstEntryId(ctx)
		if err != nil {
			return -1, err
		}
	}
	return firstEntryId, nil
}

func MergeFragmentsAndReleaseAfterCompletedPro(ctx context.Context, cli minioHandler.MinioHandler, bucket string, mergedFragKey string, mergeFragId int64, fragments []storage.Fragment, pendingMergeSize int64, releaseImmediately bool) (storage.Fragment, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, "Segment", "mergeFragmentsAndReleaseAfterCompleted")
	defer sp.End()
	// Check args
	if len(fragments) == 0 {
		return nil, errors.New("no fragments to merge")
	}

	// Fast merge by rename
	if len(fragments) == 1 {
		// no need to merge here, just rename
		return fastMergeSingleFragment(ctx, cli, bucket, mergedFragKey, mergeFragId, fragments[0])
	}

	// Merge them one by one to reduce memory usage
	startTime := time.Now()
	dataBuff := make([]byte, 0, pendingMergeSize)
	indexBuff := make([]byte, 0, 1024)

	mergeTarget := &LegacyFragmentObject{
		client:       cli,
		bucket:       bucket,
		fragmentId:   mergeFragId,
		fragmentKey:  mergedFragKey,
		entriesData:  dataBuff,
		indexes:      indexBuff,
		firstEntryId: -1,
		lastEntryId:  -1,
		dataUploaded: false,
		dataLoaded:   false,
		infoFetched:  true,
	}
	expectedEntryId := int64(-1)
	fragIds := make([]int64, 0)
	for _, candidateFrag := range fragments {
		fragFirstEntryId, err := GetFirstEntryIdWithoutDataLoadedIfPossible(ctx, candidateFrag)
		if err != nil {
			return nil, err
		}
		fragLastEntryId, err := GetLastEntryIdWithoutDataLoadedIfPossible(ctx, candidateFrag)
		if err != nil {
			return nil, err
		}
		// check the order of entries
		if expectedEntryId == -1 {
			// the first segment
			mergeTarget.firstEntryId = fragFirstEntryId
			expectedEntryId = fragLastEntryId + 1
		} else {
			if expectedEntryId != fragFirstEntryId {
				logger.Ctx(ctx).Warn("fragments are not in order", zap.String("fragmentKey", candidateFrag.GetFragmentKey()), zap.Int64("expectedEntryId", expectedEntryId), zap.Int64("fragFirstEntryId", fragFirstEntryId))
				return nil, errors.New("fragments are not in order")
			}
			expectedEntryId = fragLastEntryId + 1
			mergeTarget.lastEntryId = fragLastEntryId
		}
		// load candidate fragment data
		loadCandidateFragmentDataErr := candidateFrag.Load(ctx)
		if loadCandidateFragmentDataErr != nil {
			logger.Ctx(ctx).Warn("failed to load fragment", zap.String("fragmentKey", candidateFrag.GetFragmentKey()), zap.Error(loadCandidateFragmentDataErr))
			return nil, loadCandidateFragmentDataErr
		}
		// append fragment data to merge target
		baseOffset := len(mergeTarget.entriesData)
		mergeOneFragmentErr := candidateFrag.AppendToMergeTarget(ctx, mergeTarget, int64(baseOffset))
		candidateFrag.Release(ctx) // release candidate fragment data immediately
		if mergeOneFragmentErr != nil {
			logger.Ctx(ctx).Warn("failed to merge fragment", zap.String("fragmentKey", candidateFrag.GetFragmentKey()), zap.Error(mergeOneFragmentErr))
			return nil, mergeOneFragmentErr
		}
		fragIds = append(fragIds, candidateFrag.GetFragmentId())
	}

	// set data cache ready
	mergeTarget.dataLoaded = true
	mergeTarget.size = int64(len(mergeTarget.entriesData) + len(mergeTarget.indexes))
	mergeTarget.rawBufSize = int64(cap(mergeTarget.entriesData) + cap(mergeTarget.indexes))

	if mergeTarget.firstEntryId == -1 {
		logger.Ctx(ctx).Warn("fragment not loaded", zap.String("fragKey", mergeTarget.fragmentKey))
	}
	// upload the mergedFragment
	flushErr := mergeTarget.Flush(ctx)
	if flushErr != nil {
		return nil, flushErr
	}
	// set flag
	mergeTarget.dataUploaded = true
	mergeTarget.infoFetched = true

	if releaseImmediately {
		// release immediately
		mergeTarget.entriesData = nil
		mergeTarget.indexes = nil
		mergeTarget.dataLoaded = false
	}
	logger.Ctx(ctx).Info("merge fragments and release after completed", zap.String("mergedFragKey", mergeTarget.fragmentKey), zap.Int64("mergeFragId", mergeFragId), zap.Int64s("fragmentIds", fragIds), zap.Int64("size", mergeTarget.size), zap.Int64("costMs", time.Since(startTime).Milliseconds()))
	return mergeTarget, nil
}

func fastMergeSingleFragment(ctx context.Context, cli minioHandler.MinioHandler, bucket string, mergedFragKey string, mergeFragId int64, fragment storage.Fragment) (storage.Fragment, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, "Segment", "fastMergeSingleFragment")
	defer sp.End()
	startTime := time.Now()
	// merge
	mergedFrag := &LegacyFragmentObject{
		client:       cli,
		bucket:       bucket,
		fragmentId:   mergeFragId,
		fragmentKey:  mergedFragKey,
		entriesData:  make([]byte, 0),
		indexes:      make([]byte, 0),
		firstEntryId: -1,
		lastEntryId:  -1,
		dataUploaded: false,
		dataLoaded:   false,
		infoFetched:  false,
	}

	// fast rename
	uploadInfo, uploadErr := cli.CopyObject(ctx,
		minio.CopyDestOptions{
			Bucket: mergedFrag.bucket,
			Object: mergedFrag.fragmentKey,
		}, minio.CopySrcOptions{
			Bucket: bucket,
			Object: fragment.GetFragmentKey(),
		})
	if uploadErr != nil {
		return nil, uploadErr
	}

	// set data cache ready
	mergedFrag.size = uploadInfo.Size
	mergedFrag.rawBufSize = uploadInfo.Size
	mergedFrag.dataLoaded = false
	mergedFrag.dataUploaded = true
	mergedFrag.infoFetched = false

	logger.Ctx(ctx).Info("fast merge single fragment completed", zap.String("mergedFragKey", mergedFrag.fragmentKey), zap.Int64("mergeFragId", mergeFragId), zap.Int64("fragmentId", fragment.GetFragmentId()), zap.Int64("size", mergedFrag.size), zap.Int64("costMs", time.Since(startTime).Milliseconds()))
	return mergedFrag, nil
}

func SearchFragment(entryId int64, list []storage.Fragment) (storage.Fragment, error) {
	low, high := 0, len(list)-1
	var candidate storage.Fragment

	for low <= high {
		mid := (low + high) / 2
		frag := list[mid]

		first, err := GetFirstEntryIdWithoutDataLoadedIfPossible(context.TODO(), frag)
		if err != nil {
			return nil, err
		}

		if first > entryId {
			high = mid - 1
		} else {
			last, err := GetLastEntryIdWithoutDataLoadedIfPossible(context.TODO(), frag)
			if err != nil {
				return nil, err
			}
			if last >= entryId {
				candidate = frag
				return candidate, nil
			} else {
				low = mid + 1
			}
		}
	}
	return candidate, nil
}

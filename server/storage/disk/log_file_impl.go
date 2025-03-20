package disk

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/cache"
)

var _ storage.LogFile = (*DiskLogFile)(nil)

// DiskLogFile implements the LogFile interface for disk-based storage
type DiskLogFile struct {
	mu           sync.RWMutex
	id           int64
	basePath     string
	currFragment *FragmentFile

	// Configuration parameters
	fragmentSize    int // Maximum size of each fragment
	maxEntryPerFile int // Maximum number of entries per fragment
	autoSync        bool

	// State
	lastFragmentID atomic.Int64
	lastEntryID    atomic.Int64

	// 使用SequentialBuffer存储窗口内的entries
	buffer        *cache.SequentialBuffer
	maxBufferSize int                  // 最大buffer大小（字节）
	lastSync      atomic.Int64         // 上次同步时间
	syncedChan    map[int64]chan int64 // 同步完成的channel

	// For async writes and control
	closed  bool
	closeCh chan struct{}
}

// NewDiskLogFile creates a new DiskLogFile instance
func NewDiskLogFile(id int64, basePath string, options ...Option) (*DiskLogFile, error) {
	// Set default configuration
	dlf := &DiskLogFile{
		id:              id,
		basePath:        filepath.Join(basePath, fmt.Sprintf("log_%d", id)),
		fragmentSize:    4 * 1024 * 1024, // Default 4MB
		maxEntryPerFile: 100000,          // Default 100k entries per fragment
		maxBufferSize:   1 * 1024 * 1024, // Default 1MB buffer
		syncedChan:      make(map[int64]chan int64),
		closeCh:         make(chan struct{}),
		autoSync:        true, // Default is auto-sync enabled
	}
	dlf.lastSync.Store(time.Now().UnixMilli())

	// Apply options
	for _, opt := range options {
		opt(dlf)
	}

	// Create log directory
	if err := os.MkdirAll(dlf.basePath, 0755); err != nil {
		return nil, err
	}

	// Load existing fragments
	fragments, err := dlf.getFragments()
	if err != nil {
		return nil, err
	}

	// Initialize state from existing fragments
	if len(fragments) > 0 {
		// Find max fragment ID
		maxFragID := int64(0)
		for _, f := range fragments {
			if f.GetFragmentId() > maxFragID {
				maxFragID = f.GetFragmentId()
			}
		}
		dlf.lastFragmentID.Store(maxFragID)

		// Find max entry ID
		lastFragment := fragments[len(fragments)-1]
		lastEntryID, err := lastFragment.GetLastEntryId()
		if err == nil {
			dlf.lastEntryID.Store(lastEntryID)
		}
	} else {
		dlf.lastEntryID.Store(-1)
		dlf.lastFragmentID.Store(-1)
	}
	dlf.buffer = cache.NewSequentialBuffer(dlf.lastEntryID.Load()+1, 10000) // 默认缓存10000个entry

	// 启动定期同步goroutine
	if dlf.autoSync {
		go dlf.run()
	}

	return dlf, nil
}

// run 定期执行同步操作，类似于 objectstorage中的同步机制
func (dlf *DiskLogFile) run() {
	// 定时器
	ticker := time.NewTicker(500 * time.Millisecond) // 500ms同步一次
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 检查是否已关闭
			if dlf.closed {
				return
			}

			// 检查上次同步时间，避免过于频繁同步
			if time.Now().UnixMilli()-dlf.lastSync.Load() < 200 {
				continue
			}

			// 获取当前buffer状态
			bufferEmpty := dlf.buffer.ExpectedNextEntryId.Load() == dlf.buffer.FirstEntryId

			// 如果buffer为空，不需要同步
			if bufferEmpty {
				continue
			}

			err := dlf.Sync(context.Background())
			if err != nil {
				logger.Ctx(context.TODO()).Warn("disk log file sync error",
					zap.String("basePath", dlf.basePath),
					zap.Int64("logFileId", dlf.id),
					zap.Error(err))
			}
		case <-dlf.closeCh:
			fmt.Println("run: 收到关闭信号，退出goroutine")
			// 尝试同步剩余数据
			if err := dlf.Sync(context.Background()); err != nil {
				fmt.Printf("关闭时同步失败: %v\n", err)
			}
			// 关闭当前fragment
			if dlf.currFragment != nil {
				if err := dlf.currFragment.Release(); err != nil {
					fmt.Printf("关闭fragment失败: %v\n", err)
					return
				}
				dlf.currFragment = nil
			}
			fmt.Printf("DiskLogFile已成功关闭\n")
			return
		}
	}
}

// GetId returns the log file ID
func (dlf *DiskLogFile) GetId() int64 {
	return dlf.id
}

// Append synchronously appends a log entry
// Deprecated
func (dlf *DiskLogFile) Append(ctx context.Context, data []byte) error {
	// 获取当前最大ID并加1
	entryId := dlf.lastEntryID.Add(1) // TODO delete this

	fmt.Printf("Append: 同步写入ID=%d\n", entryId)

	// 使用AppendAsync进行异步写入
	_, resultCh, err := dlf.AppendAsync(ctx, entryId, data)
	if err != nil {
		fmt.Printf("Append: 异步写入失败 - %v\n", err)
		return err
	}

	// 等待写入完成
	select {
	case result := <-resultCh:
		if result < 0 {
			fmt.Printf("Append: 写入失败，返回错误码 %d\n", result)
			return fmt.Errorf("failed to append entry, got result %d", result)
		}
		fmt.Printf("Append: 写入成功，写入ID=%d\n", result)
		return nil
	case <-ctx.Done():
		fmt.Printf("Append: 写入超时或取消\n")
		return ctx.Err()
	}
}

// AppendAsync appends data to the log file asynchronously.
func (dlf *DiskLogFile) AppendAsync(ctx context.Context, entryId int64, value []byte) (int64, <-chan int64, error) {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	fmt.Printf("AppendAsync: 尝试写入ID=%d, 数据长度=%d\n", entryId, len(value))

	// 处理已关闭的文件
	if dlf.closed {
		fmt.Printf("AppendAsync: 失败 - 文件已关闭\n")
		return -1, nil, errors.New("diskLogFile closed")
	}

	// 创建结果channel
	ch := make(chan int64, 1)

	// 先检查是否ID已经存在于已同步的数据中
	lastId := dlf.lastEntryID.Load()
	if entryId <= lastId {
		fmt.Printf("AppendAsync: ID=%d 已存在，返回成功\n", entryId)
		// 对于已经写入磁盘的数据，不再尝试重写，直接返回成功
		ch <- entryId
		close(ch)
		return entryId, ch, nil
	}

	// 写入缓冲区
	id, err := dlf.buffer.WriteEntry(entryId, value)
	if err != nil {
		fmt.Printf("AppendAsync: 写入buffer失败 - %v\n", err)
		ch <- -1
		close(ch)
		return -1, ch, err
	}

	fmt.Printf("AppendAsync: 成功写入buffer, ID=%d, ExpectedNextEntryId=%d\n",
		id, dlf.buffer.ExpectedNextEntryId.Load())

	// 保存到待同步通道
	dlf.syncedChan[id] = ch

	// 检查是否需要触发同步
	dataSize := dlf.buffer.DataSize.Load()
	if dataSize >= int64(dlf.maxBufferSize) {
		fmt.Printf("AppendAsync: buffer已满，触发同步\n")
		logger.Ctx(context.TODO()).Debug("reach max buffer size, trigger flush",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id),
			zap.Int64("bufferSize", dlf.buffer.DataSize.Load()),
			zap.Int64("maxSize", int64(dlf.maxBufferSize)))
		syncErr := dlf.Sync(ctx)
		if syncErr != nil {
			fmt.Printf("AppendAsync: 同步失败 - %v\n", syncErr)
			logger.Ctx(context.TODO()).Warn("reach max buffer size, but trigger flush failed",
				zap.String("basePath", dlf.basePath),
				zap.Int64("logFileId", dlf.id),
				zap.Int64("bufferSize", dlf.buffer.DataSize.Load()),
				zap.Int64("maxSize", int64(dlf.maxBufferSize)),
				zap.Error(syncErr))
		}
	}
	return id, ch, nil
}

// Sync 将buffer中的数据刷新到Fragment
func (dlf *DiskLogFile) Sync(ctx context.Context) error {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()
	defer func() {
		dlf.lastSync.Store(time.Now().UnixMilli())
	}()

	fmt.Printf("Sync执行: buffer大小=%d, firstEntryId=%d, expectedNextEntryId=%d\n",
		len(dlf.buffer.Values), dlf.buffer.FirstEntryId, dlf.buffer.ExpectedNextEntryId.Load())

	entryCount := len(dlf.buffer.Values)
	if entryCount == 0 {
		fmt.Printf("Sync跳过: buffer为空\n")
		logger.Ctx(ctx).Debug("Call Sync, but empty, skip ... ",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id))
		return nil
	}

	// 检查是否有数据需要刷新
	if dlf.buffer.ExpectedNextEntryId.Load()-dlf.buffer.FirstEntryId == 0 {
		fmt.Printf("Sync跳过: 没有新数据\n")
		logger.Ctx(ctx).Debug("Call Sync, expected id not received yet, skip ... ",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id),
			zap.Int64("bufferSize", dlf.buffer.DataSize.Load()),
			zap.Int64("expectedNextEntryId", dlf.buffer.ExpectedNextEntryId.Load()))
		return nil
	}

	// 读取需要刷新的数据
	fmt.Printf("Sync读取buffer起始顺序数据: firstEntryId=%d, expectedNextEntryId=%d\n",
		dlf.buffer.FirstEntryId, dlf.buffer.ExpectedNextEntryId.Load())
	toFlushData, err := dlf.buffer.ReadEntriesRange(dlf.buffer.FirstEntryId, dlf.buffer.ExpectedNextEntryId.Load())
	if err != nil {
		fmt.Printf("Sync读取buffer起始顺序数据失败: %v\n", err)
		logger.Ctx(ctx).Error("Call Sync, but ReadEntriesRange failed",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id),
			zap.Error(err))
		return err
	}
	toFlushDataFirstEntryId := dlf.buffer.FirstEntryId
	fmt.Printf("Sync需要写入的数据: %d条, 从ID=%d开始\n", len(toFlushData), toFlushDataFirstEntryId)

	// 确保fragment已创建
	if dlf.currFragment == nil {
		fmt.Printf("Sync需要创建新的fragment\n")
		// TODO 应该要确保rotate的fragment的 firstEntryId和toFlushDataFirstEntryId一样。并且确保 上一个fragment的last entryId和 新rotate 创建的fragment的firstEntryId是连续的
		if err := dlf.rotateFragment(dlf.lastEntryID.Load() + 1); err != nil {
			fmt.Printf("Sync创建新的fragment失败: %v\n", err)
			return err
		}
	}

	// 将数据写入fragment
	var writeError error
	var lastWrittenEntryID int64 = dlf.lastEntryID.Load()
	var lastWrittenFlushedEntryID int64 = lastWrittenEntryID

	fmt.Printf("Sync开始写入数据到fragment\n")
	for i, data := range toFlushData {
		if data == nil {
			// 数据为空，意味着没有数据或者存在空洞，结束本次flush
			logger.Ctx(ctx).Warn("write entry to fragment failed, empty entry data found", zap.String("basePath", dlf.basePath), zap.Int64("logFileId", dlf.id), zap.Int("index", i), zap.Int64("toFlushDataFirstEntryId", toFlushDataFirstEntryId))
			break
		}

		// 检查当前fragment是否已满，如果满了则创建新的fragment
		if dlf.needNewFragment() {
			fmt.Printf("Sync检测到fragment已满，需要创建新的fragment\n")
			// 先将当前fragment刷到磁盘
			if err := dlf.currFragment.Flush(ctx); err != nil {
				fmt.Printf("Sync刷新当前fragment失败: %v\n", err)
				writeError = err
				break
			}
			lastWrittenFlushedEntryID = lastWrittenEntryID

			// 创建新的fragment
			if err := dlf.rotateFragment(lastWrittenFlushedEntryID + 1); err != nil {
				fmt.Printf("Sync创建新的fragment失败: %v\n", err)
				writeError = err
				break
			}
		}

		// 创建包含EntryID的数据 - 将ID作为数据的前8个字节
		entryID := toFlushDataFirstEntryId + int64(i)
		entryIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(entryIDBytes, uint64(entryID))
		dataWithID := append(entryIDBytes, data...)

		// 写入数据
		fmt.Printf("Sync写入数据: entryID=%d, dataLen=%d\n", entryID, len(data))
		writeErr := dlf.currFragment.Write(ctx, dataWithID)
		// 如果是fragment满了的失败，那么就rotate fragment，并重新写入
		if writeErr != nil && strings.Contains(writeErr.Error(), "no space left on device") {
			if rotateErr := dlf.rotateFragment(lastWrittenFlushedEntryID + 1); rotateErr != nil {
				fmt.Printf("Sync创建新的fragment失败: %v\n", rotateErr)
				writeError = rotateErr
				break
			}
			if flushErr := dlf.currFragment.Flush(ctx); flushErr != nil {
				fmt.Printf("Sync刷新当前fragment失败: %v\n", flushErr)
				writeError = flushErr
				break
			}
			lastWrittenFlushedEntryID = lastWrittenEntryID
			// 重试一次
			writeErr = dlf.currFragment.Write(ctx, dataWithID)
		}

		// 如果还写入失败，则中断本次sync
		if writeErr != nil {
			fmt.Printf("Sync写入数据失败: entryID=%d, error=%v\n", entryID, err)
			logger.Ctx(ctx).Warn("write entry to fragment failed",
				zap.String("basePath", dlf.basePath),
				zap.Int64("logFileId", dlf.id),
				zap.Int64("entryId", entryID),
				zap.Error(err))
			break
		}
		lastWrittenEntryID = entryID
	}

	// 说明还有写入数据还没flush到磁盘, 进行一次flush刷盘
	if lastWrittenEntryID != lastWrittenFlushedEntryID {
		flushErr := dlf.currFragment.Flush(ctx)
		if flushErr != nil {
			fmt.Printf("Sync刷新当前fragment失败: %v\n", flushErr)
			writeError = flushErr
		} else {
			lastWrittenFlushedEntryID = lastWrittenEntryID
		}
	}

	// 处理结果通知
	if lastWrittenFlushedEntryID == dlf.lastEntryID.Load() {
		// 没有一条写入成功，通知所有chan写入失败，让客户端重试。
		// no flush success, callback all append sync error
		for syncingId, ch := range dlf.syncedChan {
			// append error
			ch <- -1
			delete(dlf.syncedChan, syncingId)
			close(ch)
			logger.Ctx(ctx).Debug("Call Sync, but flush failed", zap.String("basePath", dlf.basePath), zap.Int64("logFileId", dlf.id), zap.Int64("syncingId", syncingId), zap.Error(writeError))
		}
		// reset buffer as empty
		dlf.buffer.Reset()
	} else if lastWrittenFlushedEntryID > dlf.lastEntryID.Load() {
		if writeError == nil { // 表示全部成功，
			restData, err := dlf.buffer.ReadEntriesToLast(dlf.buffer.ExpectedNextEntryId.Load())
			if err != nil {
				logger.Ctx(ctx).Error("Call Sync, but ReadEntriesToLast failed", zap.Int64("logFileId", dlf.id), zap.Error(err))
				return err
			}
			restDataFirstEntryId := dlf.buffer.ExpectedNextEntryId.Load()
			dlf.buffer = cache.NewSequentialBufferWithData(restDataFirstEntryId, 10000, restData)

			// notify all waiting channels
			for syncingId, ch := range dlf.syncedChan {
				if syncingId < restDataFirstEntryId {
					ch <- syncingId
					delete(dlf.syncedChan, syncingId)
					close(ch)
				}
			}
		} else { // 表示部分成功
			restDataFirstEntryId := lastWrittenFlushedEntryID + 1
			for syncingId, ch := range dlf.syncedChan {
				if syncingId <= lastWrittenFlushedEntryID { // flush落盘的通知写入成功
					// append success
					ch <- syncingId
					delete(dlf.syncedChan, syncingId)
					close(ch)
				} else { // 没有flush落盘的都通知失败
					// append error
					ch <- -1
					delete(dlf.syncedChan, syncingId)
					close(ch)
					logger.Ctx(ctx).Debug("Call Sync, but flush failed", zap.String("basePath", dlf.basePath), zap.Int64("logFileId", dlf.id), zap.Int64("syncingId", syncingId), zap.Error(writeError))
				}
			}
			// 需要重新建立buffer，让客户端能够重试
			// new a empty buffer
			dlf.buffer = cache.NewSequentialBuffer(restDataFirstEntryId, int64(10000)) // TODO config
		}

	}

	// 更新lastEntryID
	if lastWrittenFlushedEntryID > dlf.lastEntryID.Load() {
		fmt.Printf("Sync更新lastEntryID: %d -> %d\n", dlf.lastEntryID.Load(), lastWrittenFlushedEntryID)
		dlf.lastEntryID.Store(lastWrittenFlushedEntryID)
	}

	fmt.Printf("Sync完成\n")
	return nil
}

// needNewFragment checks if a new fragment needs to be created
func (dlf *DiskLogFile) needNewFragment() bool {
	if dlf.currFragment == nil {
		return true
	}

	// 检查fragment是否已关闭
	if dlf.currFragment.closed {
		return true
	}

	// 检查是否已经达到文件大小限制
	currentSize := dlf.currFragment.GetSize()
	if currentSize >= int64(dlf.fragmentSize-1024) { // 留出一些余量，防止溢出
		logger.Ctx(context.TODO()).Debug("Need new fragment due to size limit",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id),
			zap.Int64("currentSize", currentSize),
			zap.Int("fragmentSize", dlf.fragmentSize))
		return true
	}

	// 检查是否达到条目数量限制
	lastEntry, err := dlf.currFragment.GetLastEntryId()
	if err != nil {
		// 如果获取失败，可能是fragment有问题，创建新的
		logger.Ctx(context.TODO()).Warn("Cannot get last entry ID, rotating fragment",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id),
			zap.Error(err))
		return true
	}

	firstEntry, err := dlf.currFragment.GetFirstEntryId()
	if err != nil {
		// 如果获取失败，可能是fragment有问题，创建新的
		logger.Ctx(context.TODO()).Warn("Cannot get first entry ID, rotating fragment",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id),
			zap.Error(err))
		return true
	}

	entriesInFragment := lastEntry - firstEntry + 1
	if entriesInFragment >= int64(dlf.maxEntryPerFile) {
		logger.Ctx(context.TODO()).Debug("Need new fragment due to entry count limit",
			zap.String("basePath", dlf.basePath),
			zap.Int64("logFileId", dlf.id),
			zap.Int64("entriesInFragment", entriesInFragment),
			zap.Int("maxEntryPerFile", dlf.maxEntryPerFile))
		return true
	}

	return false
}

// rotateFragment closes the current fragment and creates a new one
func (dlf *DiskLogFile) rotateFragment(fragmentFirstEntryId int64) error {
	logger.Ctx(context.TODO()).Debug("rotating fragment",
		zap.String("basePath", dlf.basePath),
		zap.Int64("logFileId", dlf.id))

	// 如果当前片段存在，先关闭它
	if dlf.currFragment != nil {
		if err := dlf.currFragment.Flush(context.Background()); err != nil {
			return errors.Wrap(err, "flush current fragment")
		}
		if err := dlf.currFragment.Release(); err != nil {
			return errors.Wrap(err, "close current fragment")
		}
	}

	// 创建新的fragment ID
	fragmentID := dlf.lastFragmentID.Add(1)

	logger.Ctx(context.TODO()).Debug("creating new fragment",
		zap.String("basePath", dlf.basePath),
		zap.Int64("logFileId", dlf.id),
		zap.Int64("fragmentID", fragmentID),
		zap.Int64("firstEntryID", fragmentFirstEntryId))

	// 创建新的fragment
	fragmentPath := filepath.Join(dlf.basePath, fmt.Sprintf("fragment_%d", fragmentID))
	fragment, err := NewFragmentFile(fragmentPath, int64(dlf.fragmentSize), fragmentID, fragmentFirstEntryId)
	if err != nil {
		return errors.Wrapf(err, "create new fragment: %s", fragmentPath)
	}

	// 更新当前fragment
	dlf.currFragment = fragment
	return nil
}

// NewReader creates a new Reader instance
func (dlf *DiskLogFile) NewReader(ctx context.Context, opt storage.ReaderOpt) (storage.Reader, error) {
	// 检查文件是否已关闭
	if dlf.closed {
		return nil, errors.New("logfile is closed")
	}

	// 确保所有数据已同步到磁盘
	if err := dlf.Sync(ctx); err != nil {
		return nil, err
	}

	// 获取所有 fragments
	fragments, err := dlf.getFragments()
	if err != nil {
		return nil, err
	}

	if len(fragments) == 0 {
		return nil, fmt.Errorf("no fragments found")
	}

	// 创建新的DiskReader
	reader := &DiskReader{
		ctx:             ctx,
		fragments:       fragments,
		currFragmentIdx: 0,
		currEntryID:     opt.StartSequenceNum,
		endEntryID:      opt.EndSequenceNum,
		closed:          false,
	}

	return reader, nil
}

// Load loads data from disk
func (dlf *DiskLogFile) Load(ctx context.Context) (int64, storage.Fragment, error) {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	// 加载所有fragments
	fragments, err := dlf.getFragments()
	if err != nil {
		return 0, nil, err
	}

	if len(fragments) == 0 {
		return 0, nil, nil
	}

	// 计算总大小并返回最后一个fragment
	totalSize := int64(0)
	for _, frag := range fragments {
		size := frag.GetSize()
		totalSize += size
	}

	return totalSize, fragments[len(fragments)-1], nil
}

// Merge merges log file fragments
func (dlf *DiskLogFile) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	// 首先同步所有数据到磁盘
	if err := dlf.Sync(ctx); err != nil {
		return nil, nil, nil, err
	}

	// 获取所有fragments
	fragments, err := dlf.getFragments()
	if err != nil {
		return nil, nil, nil, err
	}

	if len(fragments) == 0 {
		return nil, nil, nil, nil
	}

	start := time.Now()
	// TODO should be config
	// file max size, default 128MB
	fileMaxSize := 128_000_000
	mergedFrags := make([]storage.Fragment, 0)
	mergedFragId := uint64(0)
	entryOffset := make([]int32, 0)
	fragmentIdOffset := make([]int32, 0)

	totalMergeSize := 0
	pendingMergeSize := 0
	pendingMergeFrags := make([]*FragmentFile, 0)
	// load all fragment in memory
	for _, frag := range fragments {
		loadFragErr := frag.Load(ctx)
		if loadFragErr != nil {
			return nil, nil, nil, loadFragErr
		}

		pendingMergeFrags = append(pendingMergeFrags, frag)
		pendingMergeSize += int(frag.fileSize) // TODO 应该获得fragment实际的准确数据大小，包括header/footer/index/data 几个部分的实际数据大小。
		if pendingMergeSize >= fileMaxSize {
			// merge immediately
			mergedFrag, mergeErr := mergeFragmentsAndReleaseAfterCompleted(ctx, dlf.getMergedFragmentKey(mergedFragId), mergedFragId, pendingMergeSize, pendingMergeFrags)
			if mergeErr != nil {
				return nil, nil, nil, mergeErr
			}
			mergedFrags = append(mergedFrags, mergedFrag)
			mergedFragId++
			mergedFragFirstEntryId, _ := mergedFrag.GetFirstEntryId()
			entryOffset = append(entryOffset, int32(mergedFragFirstEntryId))
			fragmentIdOffset = append(fragmentIdOffset, int32(pendingMergeFrags[0].fragmentId))
			pendingMergeFrags = make([]*FragmentFile, 0)
			totalMergeSize += pendingMergeSize
			pendingMergeSize = 0
		}
	}
	if pendingMergeSize > 0 && len(pendingMergeFrags) > 0 {
		// merge immediately
		mergedFrag, mergeErr := mergeFragmentsAndReleaseAfterCompleted(ctx, dlf.getMergedFragmentKey(mergedFragId), mergedFragId, pendingMergeSize, pendingMergeFrags)
		if mergeErr != nil {
			return nil, nil, nil, mergeErr
		}
		mergedFrags = append(mergedFrags, mergedFrag)
		mergedFragId++
		mergedFragFirstEntryId, _ := mergedFrag.GetFirstEntryId()
		entryOffset = append(entryOffset, int32(mergedFragFirstEntryId))
		fragmentIdOffset = append(fragmentIdOffset, int32(pendingMergeFrags[0].fragmentId))
		pendingMergeFrags = make([]*FragmentFile, 0)
		totalMergeSize += pendingMergeSize
		pendingMergeSize = 0
	}
	cost := time.Now().Sub(start)
	metrics.WpCompactReqLatency.WithLabelValues(fmt.Sprintf("%d", dlf.id)).Observe(float64(cost.Milliseconds()))
	metrics.WpCompactBytes.WithLabelValues(fmt.Sprintf("%d", dlf.id)).Observe(float64(totalMergeSize))
	return mergedFrags, entryOffset, fragmentIdOffset, nil
}

func (dlf *DiskLogFile) getMergedFragmentKey(mergedFragmentId uint64) string {
	return fmt.Sprintf("%s/%d/m_%d.frag", dlf.basePath, dlf.id, mergedFragmentId)
}

func mergeFragmentsAndReleaseAfterCompleted(ctx context.Context, mergedFragPath string, mergeFragId uint64, mergeFragSize int, fragments []*FragmentFile) (storage.Fragment, error) {
	// check args
	if len(fragments) == 0 {
		return nil, errors.New("no fragments to merge")
	}

	// merge
	fragmentFirstEntryId := fragments[0].lastEntryID
	mergedFragment, err := NewFragmentFile(mergedFragPath, int64(mergeFragSize), int64(mergeFragId), fragmentFirstEntryId)
	if err != nil {
		return nil, errors.Wrapf(err, "create new fragment: %s", mergedFragPath)
	}

	expectedEntryId := int64(-1)
	for _, fragment := range fragments {
		err := fragment.Load(ctx)
		if err != nil {
			return nil, err
		}
		// check the order of entries
		if expectedEntryId == -1 {
			// the first segment
			expectedEntryId = fragment.lastEntryID + 1
		} else {
			if expectedEntryId != fragment.firstEntryID {
				return nil, errors.New("fragments are not in order")
			}
			expectedEntryId = fragment.lastEntryID + 1
		}

		// merge index
		// TODO
		// 把fragment的data拷贝写入到mergedFragment的data区
		// 把fragment的index拷贝写入到mergedFragment的index区,且所有index长度要调整下
	}

	return mergedFragment, nil
}

// Close closes the log file and releases resources
func (dlf *DiskLogFile) Close() error {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	if dlf.closed {
		return nil
	}

	fmt.Printf("关闭DiskLogFile: id=%d, basePath=%s\n", dlf.id, dlf.basePath)

	// 标记为已关闭，阻止新的操作
	dlf.closed = true

	// 发送关闭信号
	close(dlf.closeCh)

	return nil
}

// getFragments returns all fragments in the log file
func (dlf *DiskLogFile) getFragments() ([]*FragmentFile, error) {
	// 读取目录内容
	entries, err := os.ReadDir(dlf.basePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	// 筛选出fragment文件
	fragments := make([]*FragmentFile, 0)
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "fragment_") {
			// 提取fragmentID
			idStr := strings.TrimPrefix(entry.Name(), "fragment_")
			id, err := strconv.ParseInt(idStr, 10, 64)
			if err != nil {
				continue
			}

			// 打开fragment
			fragmentPath := filepath.Join(dlf.basePath, entry.Name())
			fileInfo, err := os.Stat(fragmentPath)
			if err != nil {
				continue
			}

			// 创建FragmentFile实例并加载
			fragment, err := NewFragmentFile(fragmentPath, fileInfo.Size(), id, 0) // firstEntryID会被忽略，从实际文件中加载
			if err != nil {
				continue
			}

			// 加载fragment
			if err := fragment.Load(context.Background()); err != nil {
				fragment.Release()
				continue
			}

			fragments = append(fragments, fragment)
		}
	}

	// 按fragmentID排序
	sort.Slice(fragments, func(i, j int) bool {
		return fragments[i].GetFragmentId() < fragments[j].GetFragmentId()
	})

	return fragments, nil
}

// LastFragmentId returns the last fragment ID
func (dlf *DiskLogFile) LastFragmentId() uint64 {
	return uint64(dlf.lastFragmentID.Load())
}

// GetLastEntryId returns the last entry ID
func (dlf *DiskLogFile) GetLastEntryId() (int64, error) {
	dlf.mu.RLock()
	defer dlf.mu.RUnlock()

	// 如果文件已关闭，返回错误
	if dlf.closed {
		return -1, errors.New("log file is closed")
	}

	// 使用原子变量中存储的lastEntryID
	lastID := dlf.lastEntryID.Load()
	if lastID >= 0 {
		return lastID, nil
	}

	// 如果原子变量中没有值，尝试从当前fragment获取
	if dlf.currFragment != nil {
		fragmentLastID, err := dlf.currFragment.GetLastEntryId()
		if err == nil && fragmentLastID >= 0 {
			// 更新原子变量
			dlf.lastEntryID.Store(fragmentLastID)
			return fragmentLastID, nil
		}
	}

	// 如果当前没有fragment或获取失败，尝试从所有fragment查找
	fragments, err := dlf.getFragments()
	if err != nil {
		return -1, err
	}

	if len(fragments) > 0 {
		// 获取最后一个fragment的最后一个条目ID
		lastFragment := fragments[len(fragments)-1]
		fragmentLastID, err := lastFragment.GetLastEntryId()
		if err == nil && fragmentLastID >= 0 {
			// 更新原子变量
			dlf.lastEntryID.Store(fragmentLastID)
			return fragmentLastID, nil
		}
	}

	// 如果没有任何fragment或无法获取ID，返回-1
	return -1, nil
}

// DiskReader implements the Reader interface
type DiskReader struct {
	ctx             context.Context
	fragments       []*FragmentFile
	currFragmentIdx int   // 当前fragment索引
	currEntryID     int64 // 当前读取的entry ID
	endEntryID      int64 // 终止ID（不包含）
	closed          bool  // 是否已关闭
}

// HasNext returns true if there are more entries to read
func (dr *DiskReader) HasNext() bool {
	if dr.closed {
		return false
	}

	// 如果已达到终止ID，返回false
	if dr.endEntryID > 0 && dr.currEntryID >= dr.endEntryID {
		return false
	}

	// 在当前和后续fragment中查找currEntryID
	for i := dr.currFragmentIdx; i < len(dr.fragments); i++ {
		fragment := dr.fragments[i]
		firstID, err := fragment.GetFirstEntryId()
		if err != nil {
			continue
		}
		lastID, err := fragment.GetLastEntryId()
		if err != nil {
			continue
		}

		// 如果当前ID小于fragment的第一个ID，更新当前ID
		if dr.currEntryID < firstID {
			// 如果结束ID小于fragment的第一个ID，说明没有更多数据
			if dr.endEntryID > 0 && dr.endEntryID <= firstID {
				return false
			}
			dr.currEntryID = firstID
		}

		// 检查当前ID是否在fragment的范围内
		if dr.currEntryID <= lastID {
			dr.currFragmentIdx = i
			return true
		}
	}

	return false
}

// ReadNext reads the next entry
func (dr *DiskReader) ReadNext() (*proto.LogEntry, error) {
	if dr.closed {
		return nil, errors.New("reader is closed")
	}

	if !dr.HasNext() {
		return nil, errors.New("no more entries to read")
	}

	// 获取当前fragment
	fragment := dr.fragments[dr.currFragmentIdx]

	// 从当前fragment读取数据
	data, err := fragment.GetEntry(dr.currEntryID)
	if err != nil {
		return nil, fmt.Errorf("failed to read entry %d: %v", dr.currEntryID, err)
	}

	// 确保数据长度合理
	if len(data) < 8 {
		return nil, fmt.Errorf("invalid data format for entry %d: data too short", dr.currEntryID)
	}

	// 提取entryID和实际数据
	actualID := int64(binary.LittleEndian.Uint64(data[:8]))
	actualData := data[8:]

	// 创建 LogEntry
	entry := &proto.LogEntry{
		EntryId: actualID,
		Values:  actualData,
	}

	// 移动到下一个ID
	dr.currEntryID++

	return entry, nil
}

// Close closes the reader
func (dr *DiskReader) Close() error {
	if dr.closed {
		return nil
	}

	dr.closed = true
	return nil
}

// Option is a function type for configuration options
type Option func(*DiskLogFile)

// WithFragmentSize sets the fragment size
func WithFragmentSize(size int) Option {
	return func(dlf *DiskLogFile) {
		dlf.fragmentSize = size
	}
}

// WithMaxEntryPerFile sets the maximum number of entries per fragment
func WithMaxEntryPerFile(count int) Option {
	return func(dlf *DiskLogFile) {
		dlf.maxEntryPerFile = count
	}
}

// WithDisableAutoSync 禁用自动同步
func WithDisableAutoSync() Option {
	return func(dlf *DiskLogFile) {
		dlf.autoSync = false
	}
}

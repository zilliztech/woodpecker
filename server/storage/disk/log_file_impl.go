package disk

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
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

	// State
	lastFragmentID atomic.Int64
	lastEntryID    atomic.Int64

	// 处理乱序append请求
	pendingRequestsMu sync.Mutex
	pendingRequests   map[int64]appendRequest
	nextExpectedID    int64

	// For async writes
	appendCh       chan appendRequest
	appendResultCh chan appendResult
	closed         bool
	closeCh        chan struct{}
}

// NewDiskLogFile creates a new DiskLogFile instance
func NewDiskLogFile(id int64, basePath string, options ...Option) (*DiskLogFile, error) {
	// Set default configuration
	dlf := &DiskLogFile{
		id:              id,
		basePath:        filepath.Join(basePath, fmt.Sprintf("log_%d", id)),
		fragmentSize:    4 * 1024 * 1024, // Default 4MB
		maxEntryPerFile: 100000,          // Default 100k entries per fragment
		pendingRequests: make(map[int64]appendRequest),
		nextExpectedID:  1, // 从1开始，因为0通常表示自动分配ID TODO-FIX 一个LogFile的entryID是从0开始的
		appendCh:        make(chan appendRequest, 1000),
		appendResultCh:  make(chan appendResult, 1000),
		closeCh:         make(chan struct{}),
	}

	// Apply options
	for _, opt := range options {
		opt(dlf)
	}

	// Create base directory
	if err := os.MkdirAll(dlf.basePath, 0755); err != nil {
		return nil, errors.Wrapf(err, "failed to create directory: %s", dlf.basePath)
	}

	// Start async processing goroutine
	go dlf.processAppendRequests()

	return dlf, nil
}

// GetId returns the log file ID
func (dlf *DiskLogFile) GetId() int64 {
	return dlf.id
}

// Append synchronously appends a log entry
// Deprecated
func (dlf *DiskLogFile) Append(ctx context.Context, data []byte) error {
	resultCh := make(chan int64, 1)
	entryID := dlf.lastEntryID.Add(1)

	select {
	case dlf.appendCh <- appendRequest{entryID: entryID, data: data, resultCh: resultCh}:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-resultCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AppendAsync asynchronously appends a log entry
func (dlf *DiskLogFile) AppendAsync(ctx context.Context, entryId int64, data []byte) (int64, <-chan int64, error) {
	if entryId <= 0 {
		// TODO-FIX 不应该用lastEntryID叠加。不存在entryID <0的情况。不需要检查entryID
		entryId = dlf.lastEntryID.Add(1)
	} else {
		// Update lastEntryID
		for {
			current := dlf.lastEntryID.Load()
			if entryId <= current {
				break
			}
			// TODO-Fix 当entryID大于lastEntryId的时候，不应该直接更新lastEntryID，因为 append async的 entry数据可能是乱序的。比如先写 3再写2,1 为id的entries，这种情况是由可能的
			if dlf.lastEntryID.CompareAndSwap(current, entryId) {
				break
			}
		}
	}

	resultCh := make(chan int64, 1)

	select {
	case dlf.appendCh <- appendRequest{entryID: entryId, data: data, resultCh: resultCh}:
		return entryId, resultCh, nil
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}
}

// processAppendRequests processes async append requests
func (dlf *DiskLogFile) processAppendRequests() {
	for {
		select {
		case req := <-dlf.appendCh:
			// 处理新的请求
			dlf.handleAppendRequest(req)
		case <-dlf.closeCh:
			return
		}
	}
}

// handleAppendRequest 处理单个append请求，维护乱序请求的缓冲区
func (dlf *DiskLogFile) handleAppendRequest(req appendRequest) {
	dlf.pendingRequestsMu.Lock()
	defer dlf.pendingRequestsMu.Unlock()

	fmt.Printf("处理append请求ID: %d, nextExpectedID: %d\n", req.entryID, dlf.nextExpectedID)

	// TODO-FIX 应该等于 nextExpected的时候才写入。而且总是顺序的递增写入。如果不是next expected的话，就应该分为大于或者小于的情况下，如果小于的话，说明重复提交了，直接忽略就行。如果大于的话，那么先放到缓存里，等下次顺序提交到这个位置的时候再从缓存中获取并写入提交
	// 特殊处理：如果请求的ID小于nextExpectedID，直接处理
	if req.entryID < dlf.nextExpectedID {
		fmt.Printf("ID %d 小于 nextExpectedID %d，直接处理\n", req.entryID, dlf.nextExpectedID)
		dlf.pendingRequestsMu.Unlock() // 临时解锁，避免与writeEntryToFragment中的锁冲突
		success := dlf.writeEntryToFragment(req)
		dlf.pendingRequestsMu.Lock() // 重新加锁

		// 如果写入失败，不继续处理
		if !success {
			fmt.Printf("ID %d 写入失败\n", req.entryID)
			return
		}
		fmt.Printf("ID %d 写入成功\n", req.entryID)
	} else {
		// 请求ID大于等于nextExpectedID的正常流程
		// 将请求放入缓冲区
		fmt.Printf("ID %d 放入缓冲区\n", req.entryID)
		// TODO-FIX这里应该 entryID-firstEntryID的方式，不然这个数组会很大。这个write的pending requests 缓存只是一个窗口，并不会包含所有的请求数据。flush的部分就回收掉了。比如flush了01234，那么缓冲区就应该从5开始，即 0 位置的entry的id应该是5
		dlf.pendingRequests[req.entryID] = req

		// 尝试按顺序处理请求
		dlf.processOrderedRequests()
	}
}

// processOrderedRequests 尝试按顺序处理所有待处理的请求
func (dlf *DiskLogFile) processOrderedRequests() {
	fmt.Printf("开始处理有序请求，nextExpectedID: %d, 缓冲区大小: %d\n", dlf.nextExpectedID, len(dlf.pendingRequests))

	// 如果没有下一个预期的ID，但是有其他ID，找到最小的ID作为下一个处理的ID
	if _, exists := dlf.pendingRequests[dlf.nextExpectedID]; !exists && len(dlf.pendingRequests) > 0 {
		// 找到最小的ID
		minID := int64(math.MaxInt64)
		for id := range dlf.pendingRequests {
			if id < minID {
				minID = id
			}
		}

		fmt.Printf("没有找到ID %d的请求，但有其他请求，调整nextExpectedID从 %d 到 %d\n",
			dlf.nextExpectedID, dlf.nextExpectedID, minID)
		dlf.nextExpectedID = minID
	}

	// 顺序处理请求，直到遇到gap
	for {
		req, exists := dlf.pendingRequests[dlf.nextExpectedID]
		if !exists {
			// 没有下一个预期的ID，等待后续请求
			fmt.Printf("没有找到ID %d的请求，等待后续请求\n", dlf.nextExpectedID)
			break
		}

		fmt.Printf("找到ID %d的请求，处理中\n", dlf.nextExpectedID)
		// 从缓冲区中移除该请求
		delete(dlf.pendingRequests, dlf.nextExpectedID)

		// 处理请求并写入数据
		success := dlf.writeEntryToFragment(req)
		if !success {
			// 写入失败，不再继续处理后续请求
			fmt.Printf("ID %d写入失败，停止处理\n", dlf.nextExpectedID)
			break
		}
		fmt.Printf("ID %d写入成功\n", dlf.nextExpectedID)

		// 更新下一个预期的ID
		dlf.nextExpectedID++
		fmt.Printf("下一个预期ID更新为: %d\n", dlf.nextExpectedID)
	}

	fmt.Printf("结束处理有序请求，nextExpectedID: %d, 缓冲区大小: %d\n", dlf.nextExpectedID, len(dlf.pendingRequests))
}

// writeEntryToFragment 将条目写入当前Fragment
func (dlf *DiskLogFile) writeEntryToFragment(req appendRequest) bool {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	fmt.Printf("准备写入ID %d的数据\n", req.entryID)

	// 初始化或轮换Fragment（如果需要）
	if dlf.currFragment == nil || dlf.needNewFragment() {
		fmt.Printf("需要创建或轮换Fragment for ID %d\n", req.entryID)
		if err := dlf.rotateFragment(); err != nil {
			fmt.Printf("轮换Fragment失败: %v\n", err)
			req.resultCh <- -1
			return false
		}
		fmt.Printf("轮换Fragment成功, 新Fragment ID: %d\n", dlf.lastFragmentID.Load())
	}

	// 创建包含EntryID的数据 - 将ID作为数据的前8个字节
	entryIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(entryIDBytes, uint64(req.entryID))
	dataWithID := append(entryIDBytes, req.data...)

	fmt.Printf("将写入数据: ID %d, 实际数据: %s\n", req.entryID, string(req.data))

	// 写入数据
	err := dlf.currFragment.Write(context.Background(), dataWithID)
	if err != nil {
		fmt.Printf("写入数据失败: %v\n", err)
		req.resultCh <- -1
		return false
	}

	fmt.Printf("成功写入ID %d的数据\n", req.entryID)
	// 发送成功结果
	req.resultCh <- req.entryID
	return true
}

// needNewFragment checks if a new fragment needs to be created
func (dlf *DiskLogFile) needNewFragment() bool {
	if dlf.currFragment == nil {
		return true
	}

	// Check if fragment is full
	currentSize := dlf.currFragment.GetSize()
	if currentSize >= int64(dlf.fragmentSize) {
		return true
	}

	// Check if entry count limit is reached
	lastEntry, _ := dlf.currFragment.GetLastEntryId()
	firstEntry, _ := dlf.currFragment.GetFirstEntryId()
	entriesInFragment := lastEntry - firstEntry + 1
	return entriesInFragment >= int64(dlf.maxEntryPerFile)
}

// rotateFragment closes the current fragment and creates a new one
func (dlf *DiskLogFile) rotateFragment() error {
	fmt.Printf("开始轮换Fragment\n")

	// 如果当前片段存在，先关闭它
	if dlf.currFragment != nil {
		fmt.Printf("关闭当前Fragment: fragmentID %d\n", dlf.currFragment.GetFragmentId())
		if err := dlf.currFragment.Flush(context.Background()); err != nil {
			return errors.Wrap(err, "flush current fragment")
		}
		if err := dlf.currFragment.Release(); err != nil {
			return errors.Wrap(err, "close current fragment")
		}
	}

	// 创建新的fragment ID
	fragmentID := dlf.lastFragmentID.Add(1)
	fmt.Printf("创建新Fragment: fragmentID %d\n", fragmentID)

	// 确定新fragment的第一个条目ID
	var firstEntryID int64 = 1
	if dlf.currFragment != nil {
		// 获取当前fragment最后一个条目的ID，并加1作为新fragment的起始ID
		lastID, err := dlf.currFragment.GetLastEntryId()
		if err == nil && lastID > 0 {
			firstEntryID = lastID + 1
			fmt.Printf("从上一个Fragment获取的firstEntryID: %d\n", firstEntryID)
		}
	}

	// 检查pendingRequests是否有小于当前firstEntryID的请求，确定最小的ID作为第一个条目
	// 这里逻辑可能导致死锁，先拿掉

	// 创建新的fragment
	fragmentPath := filepath.Join(dlf.basePath, fmt.Sprintf("fragment_%d", fragmentID))
	fmt.Printf("创建新Fragment文件路径: %s\n", fragmentPath)
	fragment, err := NewFragmentFile(fragmentPath, int64(dlf.fragmentSize), fragmentID, firstEntryID)
	if err != nil {
		return errors.Wrapf(err, "create new fragment: %s", fragmentPath)
	}

	// 更新当前fragment
	dlf.currFragment = fragment
	fmt.Printf("设置新的当前Fragment: fragmentID %d, firstEntryID %d\n", fragmentID, firstEntryID)

	return nil
}

// NewReader creates a new Reader instance
func (dlf *DiskLogFile) NewReader(ctx context.Context, opt storage.ReaderOpt) (storage.Reader, error) {
	dlf.mu.RLock()
	defer dlf.mu.RUnlock()

	// 检查文件是否已关闭
	if dlf.closed {
		return nil, errors.New("logfile is closed")
	}

	// 确保所有数据已同步到磁盘
	if dlf.currFragment != nil {
		if err := dlf.currFragment.Flush(ctx); err != nil {
			return nil, err
		}
	}

	// 获取所有 fragments
	fragments, err := dlf.getFragments()
	if err != nil {
		return nil, err
	}

	if len(fragments) == 0 {
		return nil, fmt.Errorf("no fragments found")
	}

	// 预先加载所有条目
	entries := make(map[int64][]byte)
	for _, fragment := range fragments {
		firstID, err := fragment.GetFirstEntryId()
		if err != nil {
			continue
		}
		lastID, err := fragment.GetLastEntryId()
		if err != nil {
			continue
		}

		fmt.Printf("扫描Fragment: ID %d, firstEntryID %d, lastEntryID %d\n",
			fragment.GetFragmentId(), firstID, lastID)

		// 获取所有条目
		for id := firstID; id <= lastID; id++ {
			data, err := fragment.GetEntry(id)
			if err != nil {
				fmt.Printf("获取条目失败: ID %d, 错误: %v\n", id, err)
				continue
			}

			// 提取实际ID和数据 (我们的数据格式是 [8字节ID][实际数据])
			if len(data) < 8 {
				fmt.Printf("条目数据太短: ID %d, 长度: %d\n", id, len(data))
				continue
			}

			actualID := int64(binary.LittleEndian.Uint64(data[:8]))
			actualData := data[8:]

			fmt.Printf("条目: ID %d, 实际ID %d, 数据: %s\n", id, actualID, string(actualData))

			// 使用实际ID而不是fragment中的序列ID
			entries[actualID] = actualData
		}
	}

	// 获取所有ID并排序
	ids := make([]int64, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	// 空检查
	if len(ids) == 0 {
		return nil, fmt.Errorf("no entries found")
	}

	fmt.Printf("读取到的所有ID: %v\n", ids)

	// 找到起始ID的索引
	startIndex := 0
	// 如果请求的起始ID大于0，找到第一个大于等于它的ID
	for i, id := range ids {
		if id >= opt.StartSequenceNum {
			startIndex = i
			break
		}
		// 如果遍历到最后一个ID仍然小于StartSequenceNum，则使用最后一个索引
		if i == len(ids)-1 {
			startIndex = i
		}
	}

	// 过滤结束ID
	filteredIDs := []int64{}
	filteredEntries := make(map[int64][]byte)

	if opt.EndSequenceNum > 0 {
		for _, id := range ids {
			if id >= opt.StartSequenceNum && id < opt.EndSequenceNum {
				filteredIDs = append(filteredIDs, id)
				filteredEntries[id] = entries[id]
			}
		}
	} else {
		filteredIDs = ids[startIndex:]
		for _, id := range filteredIDs {
			filteredEntries[id] = entries[id]
		}
	}

	fmt.Printf("过滤后的ID: %v\n", filteredIDs)

	reader := &DiskReader{
		ctx:         ctx,
		fragments:   fragments,
		currEntryID: opt.StartSequenceNum,
		endEntryID:  opt.EndSequenceNum,
		entries:     filteredEntries,
		orderedIDs:  filteredIDs,
		currIDIndex: 0,
	}

	return reader, nil
}

// getFragments returns all fragments in the log file
func (dlf *DiskLogFile) getFragments() ([]*FragmentFile, error) {
	// 获取所有fragment文件
	files, err := os.ReadDir(dlf.basePath)
	if err != nil {
		return nil, err
	}

	var fragments []*FragmentFile
	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), "fragment_") {
			// 从文件名中提取fragment ID
			fragmentID, err := strconv.ParseInt(strings.TrimPrefix(file.Name(), "fragment_"), 10, 64)
			if err != nil {
				continue
			}

			// 打开fragment文件
			fragment, err := NewFragmentFile(
				filepath.Join(dlf.basePath, file.Name()),
				int64(dlf.fragmentSize),
				fragmentID,
				0, // firstEntryID will be loaded from file
			)
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

	// 按fragment ID排序
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

	if dlf.currFragment == nil {
		return -1, nil
	}

	return dlf.currFragment.GetLastEntryId()
}

// Sync ensures all data is written to persistent storage
func (dlf *DiskLogFile) Sync(ctx context.Context) error {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	if dlf.currFragment == nil {
		return nil
	}

	return dlf.currFragment.Flush(ctx)
}

// Merge merges log file fragments
func (dlf *DiskLogFile) Merge(ctx context.Context) ([]storage.Fragment, []int32, []int32, error) {
	// In local filesystem implementation, we can simply return empty values
	// Or implement actual fragment merging logic
	return nil, nil, nil, nil
}

// Load loads the log file information
func (dlf *DiskLogFile) Load(ctx context.Context) (int64, storage.Fragment, error) {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	// Find all fragment files in the directory
	fragmentFiles, err := filepath.Glob(filepath.Join(dlf.basePath, "fragment_*"))
	if err != nil {
		return 0, nil, err
	}

	if len(fragmentFiles) == 0 {
		// No fragments found, create new one
		if err := dlf.rotateFragment(); err != nil {
			return 0, nil, err
		}
		return 0, dlf.currFragment, nil
	}

	// Find the latest fragment file
	latestFragment := fragmentFiles[0]
	latestModTime := int64(0)
	for _, file := range fragmentFiles {
		info, err := os.Stat(file)
		if err != nil {
			continue
		}

		modTime := info.ModTime().UnixNano()
		if modTime > latestModTime {
			latestModTime = modTime
			latestFragment = file
		}
	}

	// Extract fragment ID
	var fragmentID int64
	_, err = fmt.Sscanf(filepath.Base(latestFragment), "fragment_%d", &fragmentID)
	if err != nil {
		fragmentID = time.Now().UnixNano()
	}

	// Update lastFragmentID
	dlf.lastFragmentID.Store(fragmentID)

	// Load fragment - 使用0作为firstEntryID，因为实际值会在Load方法中从文件读取
	dlf.currFragment, err = NewFragmentFile(latestFragment, int64(dlf.fragmentSize), fragmentID, 0)
	if err != nil {
		return 0, nil, err
	}

	// Load fragment data
	if err := dlf.currFragment.Load(ctx); err != nil {
		return 0, nil, err
	}

	// Get last entry ID
	lastEntryID, err := dlf.currFragment.GetLastEntryId()
	if err != nil {
		lastEntryID = 0
	}

	// Update lastEntryID
	dlf.lastEntryID.Store(lastEntryID)

	return lastEntryID, dlf.currFragment, nil
}

// Close closes the log file
func (dlf *DiskLogFile) Close() error {
	dlf.mu.Lock()
	defer dlf.mu.Unlock()

	if dlf.closed {
		return nil
	}

	// Close async processing goroutine
	close(dlf.closeCh)
	dlf.closed = true

	// Close current fragment if exists
	if dlf.currFragment != nil {
		if err := dlf.currFragment.Flush(context.Background()); err != nil {
			return err
		}
		if err := dlf.currFragment.Release(); err != nil {
			return err
		}
		dlf.currFragment = nil
	}

	return nil
}

// DiskReader implements the Reader interface
type DiskReader struct {
	ctx               context.Context
	fragments         []*FragmentFile
	currFragmentIndex int
	currEntryID       int64
	endEntryID        int64
	entries           map[int64][]byte // 缓存所有条目的数据
	orderedIDs        []int64          // 所有条目ID的有序列表
	currIDIndex       int              // 当前正在读取的ID索引
}

// HasNext returns true if there are more entries to read
func (dr *DiskReader) HasNext() bool {
	// 如果当前ID已经达到或超过了结束ID，则没有更多的条目
	if dr.endEntryID > 0 && dr.currEntryID >= dr.endEntryID {
		return false
	}

	// 检查是否还有可读的片段
	if dr.currFragmentIndex >= len(dr.fragments) {
		return false
	}

	// 如果我们有orderedIDs，使用它们来进行更精确的判断
	if len(dr.orderedIDs) > 0 {
		if dr.currIDIndex >= len(dr.orderedIDs) {
			return false
		}

		// 获取当前ID
		currentID := dr.orderedIDs[dr.currIDIndex]

		// 如果当前ID已经达到或超过了结束ID，则没有更多的条目
		if dr.endEntryID > 0 && currentID >= dr.endEntryID {
			return false
		}

		return true
	}

	// 尝试在当前片段范围内找到对应的条目
	for i := dr.currFragmentIndex; i < len(dr.fragments); i++ {
		fragment := dr.fragments[i]
		firstID, err := fragment.GetFirstEntryId()
		if err != nil {
			continue
		}
		lastID, err := fragment.GetLastEntryId()
		if err != nil {
			continue
		}

		// 检查目标ID是否在当前片段范围内
		if dr.currEntryID >= firstID && dr.currEntryID <= lastID {
			return true
		}

		// 如果当前片段的最后ID小于当前要读取的ID，检查下一个片段
		if lastID < dr.currEntryID {
			continue
		}
	}

	// 如果遍历所有片段后没有找到匹配的条目，则返回false
	return false
}

// ReadNext reads the next entry
func (dr *DiskReader) ReadNext() (*proto.LogEntry, error) {
	if !dr.HasNext() {
		return nil, fmt.Errorf("no more entries")
	}

	// 如果我们有orderedIDs，优先使用它们
	if len(dr.orderedIDs) > 0 && dr.currIDIndex < len(dr.orderedIDs) {
		entryID := dr.orderedIDs[dr.currIDIndex]
		dr.currIDIndex++

		// 获取条目数据
		data, ok := dr.entries[entryID]
		if !ok {
			fmt.Printf("没有找到ID %d的数据\n", entryID)
			return nil, fmt.Errorf("entry not found for ID %d", entryID)
		}

		fmt.Printf("从缓存读取: ID %d, 数据: %s\n", entryID, string(data))

		return &proto.LogEntry{
			EntryId: entryID,
			Values:  data,
		}, nil
	}

	// 如果没有预加载的数据，尝试直接从fragment读取
	found := false
	var entry *proto.LogEntry

	for i := 0; i < len(dr.fragments); i++ {
		fragment := dr.fragments[i]
		firstID, err := fragment.GetFirstEntryId()
		if err != nil {
			continue
		}
		lastID, err := fragment.GetLastEntryId()
		if err != nil {
			continue
		}

		// 检查目标ID是否在当前片段范围内
		if dr.currEntryID >= firstID && dr.currEntryID <= lastID {
			data, err := fragment.GetEntry(dr.currEntryID)
			if err == nil {
				// 数据的格式是 [8字节ID][实际数据]
				if len(data) < 8 {
					dr.currEntryID++
					return nil, fmt.Errorf("data too short for entry ID %d", dr.currEntryID-1)
				}

				// 提取实际ID和数据
				actualID := int64(binary.LittleEndian.Uint64(data[:8]))
				actualData := data[8:]

				fmt.Printf("从fragment直接读取: 位置ID %d, 实际ID %d, 数据: %s\n",
					dr.currEntryID, actualID, string(actualData))

				entry = &proto.LogEntry{
					EntryId: actualID,
					Values:  actualData,
				}
				found = true
				dr.currFragmentIndex = i // 记录当前片段索引，下次从这里开始
				break
			}
		}

		// 如果当前片段的最后ID小于当前要读取的ID，移动到下一个片段
		if lastID < dr.currEntryID {
			continue
		}
	}

	if !found {
		return nil, fmt.Errorf("entry not found for ID %d", dr.currEntryID)
	}

	// 移动到下一个条目
	dr.currEntryID++

	return entry, nil
}

// Close closes the reader
func (dr *DiskReader) Close() error {
	for _, fragment := range dr.fragments {
		if err := fragment.Release(); err != nil {
			return err
		}
	}
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

type appendRequest struct {
	entryID  int64
	data     []byte
	resultCh chan int64
}

type appendResult struct {
	entryID int64
	err     error
}

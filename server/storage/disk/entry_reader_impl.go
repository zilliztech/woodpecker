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
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/disk/legacy"
)

var _ storage.Reader = (*DiskEntryReader)(nil)

// DiskEntryReader is responsible for reading specified entries from the segment on disk
type DiskEntryReader struct {
	ctx             context.Context
	segment         *RODiskSegmentImpl
	batchSize       int64                      // Max batch size
	currFragmentIdx int                        // Current fragment index
	currFragment    storage.AppendableFragment // Current fragment reader
	currEntryID     int64                      // Current entry ID being read
	endEntryID      int64                      // End ID (not included)
	closed          bool                       // Whether closed
}

func NewDiskEntryReader(ctx context.Context, opt storage.ReaderOpt, batchSize int64, segment *RODiskSegmentImpl) storage.Reader {
	return &DiskEntryReader{
		ctx:         ctx,
		segment:     segment,
		batchSize:   batchSize,
		currEntryID: opt.StartSequenceNum,
		endEntryID:  opt.EndSequenceNum,
		closed:      false,

		currFragment:    nil,
		currFragmentIdx: 0,
	}
}

// HasNext returns true if there are more entries to read
func (dr *DiskEntryReader) HasNext(ctx context.Context) (bool, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "HasNext")
	defer sp.End()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", dr.segment.logId)

	if dr.closed {
		logger.Ctx(ctx).Debug("No more entries to read, current reader is closed", zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID))
		return false, nil
	}

	// If reached endID, return false
	if dr.endEntryID > 0 && dr.currEntryID >= dr.endEntryID {
		logger.Ctx(ctx).Debug("No more entries to read, reach the end entryID", zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID))
		return false, nil
	}

	// current fragment contains this entry, fast return
	if dr.currFragment != nil {
		first, err := legacy.GetFragmentFileFirstEntryIdWithoutDataLoadedIfPossible(ctx, dr.currFragment)
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to get first entry id", zap.String("fragmentFile", dr.currFragment.GetFragmentKey()), zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID), zap.Error(err))
			return false, err
		}
		last, err := legacy.GetFragmentFileLastEntryIdWithoutDataLoadedIfPossible(ctx, dr.currFragment)
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to get last entry id", zap.String("fragmentFile", dr.currFragment.GetFragmentKey()), zap.Int64("currEntryId", dr.currEntryID), zap.Int64("endEntryId", dr.endEntryID), zap.Error(err))
			return false, err
		}
		// fast return if current entry is in this current fragment
		if dr.currEntryID >= first && dr.currEntryID < last {
			return true, nil
		}
	}

	idx, f, pendingReadEntryID, err := dr.segment.findFragmentFrom(ctx, dr.currFragmentIdx, dr.currEntryID, dr.endEntryID)
	if err != nil {
		return false, err
	}
	if f == nil {
		// no more fragment
		return false, nil
	}
	dr.currFragmentIdx = idx
	dr.currFragment = f
	dr.currEntryID = pendingReadEntryID
	metrics.WpFileOperationsTotal.WithLabelValues(logId, "has_next", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, "has_next", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return true, nil
}

func (dr *DiskEntryReader) ReadNextBatch(ctx context.Context) ([]*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "ReadNextBatch")
	defer sp.End()

	startTime := time.Now()
	logId := fmt.Sprintf("%d", dr.segment.logId)
	if dr.closed {
		return nil, errors.New("reader is closed")
	}
	if dr.currFragment == nil {
		return nil, errors.New("no readable Fragment")
	}

	// Get current fragment lastEntryId
	lastID, err := legacy.GetFragmentFileLastEntryIdWithoutDataLoadedIfPossible(context.TODO(), dr.currFragment)
	if err != nil {
		return nil, err
	}

	// read a fragment as a batch
	if dr.currFragment == nil {
		return nil, errors.New("no readable Fragment")
	}
	loadErr := dr.currFragment.Load(context.TODO())
	if loadErr != nil {
		return nil, loadErr
	}
	defer dr.currFragment.Release(ctx)
	entries := make([]*proto.LogEntry, 0, 32)
	maxSize := dr.batchSize
	readSize := int64(0)
	for {
		// Read data from current fragment
		data, err := dr.currFragment.GetEntry(ctx, dr.currEntryID)
		if err != nil {
			// If current entryID not in fragment, may need to move to next fragment
			logger.Ctx(ctx).Warn("Failed to read entry",
				zap.Int64("entryId", dr.currEntryID),
				zap.String("fragmentFile", dr.currFragment.GetFragmentKey()),
				zap.Error(err))
			return nil, err
		}

		// Ensure data length is reasonable
		if len(data) < 8 {
			logger.Ctx(ctx).Warn("Invalid data format: data too short",
				zap.Int64("entryId", dr.currEntryID),
				zap.Int("dataLength", len(data)))
			return nil, fmt.Errorf("invalid data format for entry %d: data too short", dr.currEntryID)
		}

		logger.Ctx(ctx).Debug("Data read complete",
			zap.Int64("entryId", dr.currEntryID),
			zap.String("fragmentPath", dr.currFragment.GetFragmentKey()))

		// Extract entryID and actual data
		actualID := int64(binary.LittleEndian.Uint64(data[:8]))
		actualData := data[8:]

		// Ensure read ID matches expected ID
		if actualID != dr.currEntryID {
			logger.Ctx(ctx).Warn("EntryID mismatch",
				zap.Int64("expectedId", dr.currEntryID),
				zap.Int64("actualId", actualID))
		}

		// Create LogEntry
		entry := &proto.LogEntry{
			EntryId: actualID,
			Values:  actualData,
		}
		entries = append(entries, entry)

		// Move to next ID
		dr.currEntryID++

		// If beyond current fragment range, prepare to move to next fragment
		if dr.currEntryID > lastID {
			break
		}
		// If read size exceeds max size, stop reading
		readSize += int64(len(data))
		if readSize >= maxSize {
			break
		}
	}

	metrics.WpFileOperationsTotal.WithLabelValues(logId, "read_next", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, "read_next", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return entries, nil
}

// ReadNext reads the next entry
func (dr *DiskEntryReader) ReadNext(ctx context.Context) (*proto.LogEntry, error) {
	ctx, sp := logger.NewIntentCtxWithParent(ctx, SegmentScopeName, "ReadNext")
	defer sp.End()
	startTime := time.Now()
	logId := fmt.Sprintf("%d", dr.segment.logId)
	if dr.closed {
		return nil, errors.New("reader is closed")
	}

	// Get current fragment
	lastID, err := legacy.GetFragmentFileLastEntryIdWithoutDataLoadedIfPossible(context.TODO(), dr.currFragment)
	if err != nil {
		return nil, err
	}

	// Read data from current fragment
	if dr.currFragment == nil {
		return nil, errors.New("no readable Fragment")
	}
	loadErr := dr.currFragment.Load(context.TODO())
	if loadErr != nil {
		return nil, loadErr
	}
	defer dr.currFragment.Release(ctx)
	data, err := dr.currFragment.GetEntry(ctx, dr.currEntryID)
	if err != nil {
		// If current entryID not in fragment, may need to move to next fragment
		logger.Ctx(ctx).Warn("Failed to read entry",
			zap.Int64("entryId", dr.currEntryID),
			zap.String("fragmentFile", dr.currFragment.GetFragmentKey()),
			zap.Error(err))
		return nil, err
	}

	// Ensure data length is reasonable
	if len(data) < 8 {
		logger.Ctx(ctx).Warn("Invalid data format: data too short",
			zap.Int64("entryId", dr.currEntryID),
			zap.Int("dataLength", len(data)))
		return nil, fmt.Errorf("invalid data format for entry %d: data too short", dr.currEntryID)
	}

	logger.Ctx(ctx).Debug("Data read complete",
		zap.Int64("entryId", dr.currEntryID),
		zap.String("fragmentPath", dr.currFragment.GetFragmentKey()))

	// Extract entryID and actual data
	actualID := int64(binary.LittleEndian.Uint64(data[:8]))
	actualData := data[8:]

	// Ensure read ID matches expected ID
	if actualID != dr.currEntryID {
		logger.Ctx(ctx).Warn("EntryID mismatch",
			zap.Int64("expectedId", dr.currEntryID),
			zap.Int64("actualId", actualID))
	}

	// Create LogEntry
	entry := &proto.LogEntry{
		EntryId: actualID,
		Values:  actualData,
	}

	// Move to next ID
	dr.currEntryID++

	// If beyond current fragment range, prepare to move to next fragment
	if dr.currEntryID > lastID {
		dr.currFragmentIdx++
	}

	metrics.WpFileOperationsTotal.WithLabelValues(logId, "read_next", "success").Inc()
	metrics.WpFileOperationLatency.WithLabelValues(logId, "read_next", "success").Observe(float64(time.Since(startTime).Milliseconds()))
	return entry, nil
}

// Close closes the reader
func (dr *DiskEntryReader) Close() error {
	if dr.closed {
		return nil
	}

	// No need to explicitly release fragments, they are managed by FragmentManager
	// Fragments are now managed by the FragmentManager, no need to release here
	//dr.fragments = nil
	dr.currFragment = nil
	dr.currFragmentIdx = 0
	dr.segment = nil

	dr.closed = true
	return nil
}

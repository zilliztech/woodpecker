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

package cache

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
)

// BufferEntry represents a single entry in the buffer with its data and notification channel
type BufferEntry struct {
	EntryId     int64                 // The entry ID for this buffer entry
	Data        []byte                // The actual data
	NotifyChan  channel.ResultChannel // Channel to notify when this entry is synced
	EnqueueTime time.Time            // Time when the entry was enqueued for buffer wait latency tracking
}

// SequentialBuffer is a buffer that stores entries in a sequential manner.
type SequentialBuffer struct {
	mu        sync.Mutex
	logId     int64
	segmentId int64
	logIdStr  string // for metrics only

	Entries                 []*BufferEntry // entries with data and notification channels
	MaxEntries              int64          // max amount of entries
	DataSize                atomic.Int64   // total data bytes size of entries
	SequentialReadyDataSize atomic.Int64   // The size of contiguous entries from the beginning, indicating the size of entries that can be flushed

	FirstEntryId        int64
	ExpectedNextEntryId atomic.Int64
}

func NewSequentialBuffer(logId int64, segmentId int64, startEntryId int64, maxEntries int64) *SequentialBuffer {
	b := &SequentialBuffer{
		logId:        logId,
		segmentId:    segmentId,
		logIdStr:     strconv.FormatInt(logId, 10),
		Entries:      make([]*BufferEntry, maxEntries),
		MaxEntries:   maxEntries,
		FirstEntryId: startEntryId,
	}
	b.ExpectedNextEntryId.Store(startEntryId)
	return b
}

func NewSequentialBufferWithData(logId int64, segmentId int64, startEntryId int64, maxEntries int64, restData []*BufferEntry) *SequentialBuffer {
	entries := make([]*BufferEntry, maxEntries)
	// copy restData refs
	if len(restData) > 0 {
		copyLen := len(restData)
		copy(entries, restData[:copyLen])
	}
	b := &SequentialBuffer{
		logId:        logId,
		segmentId:    segmentId,
		logIdStr:     strconv.FormatInt(logId, 10),
		Entries:      entries,
		MaxEntries:   maxEntries,
		FirstEntryId: startEntryId,
	}
	b.ExpectedNextEntryId.Store(startEntryId)
	return b
}

// WriteEntryWithNotify writes a new entry into the buffer with notification channel.
func (b *SequentialBuffer) WriteEntryWithNotify(entryId int64, value []byte, notifyChan channel.ResultChannel) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Validate if entryId is outside the valid range [firstEntryId, firstEntryId + maxEntries)
	if entryId < b.FirstEntryId {
		return -1, werr.ErrFileWriterInvalidEntryId.WithCauseErrMsg(fmt.Sprintf("invalid entryId: %d smaller than %d", entryId, b.FirstEntryId))
	}

	// Validate if entryId exceeds the valid range [firstEntryId, firstEntryId + maxEntries)
	if entryId >= b.FirstEntryId+b.MaxEntries {
		return -1, werr.ErrFileWriterBufferFull.WithCauseErrMsg(fmt.Sprintf("Out of buffer bounds, maybe disorder and write too fast, entryId: %d larger than %d", entryId, b.FirstEntryId+b.MaxEntries))
	}

	relatedIdx := entryId - b.FirstEntryId
	b.Entries[relatedIdx] = &BufferEntry{
		EntryId:     entryId,
		Data:        value,
		NotifyChan:  notifyChan,
		EnqueueTime: time.Now(),
	}
	b.DataSize.Add(int64(len(value)))

	// increase the ExpectedNextEntryId if necessary
	for addedId := entryId; addedId < b.FirstEntryId+b.MaxEntries; addedId++ {
		if b.Entries[addedId-b.FirstEntryId] != nil && addedId == b.ExpectedNextEntryId.Load() {
			b.ExpectedNextEntryId.Add(1)
			bufferEntry := b.Entries[addedId-b.FirstEntryId]
			b.SequentialReadyDataSize.Add(int64(len(bufferEntry.Data)))
		} else {
			break
		}
	}

	return entryId, nil
}

func (b *SequentialBuffer) ReadEntry(entryId int64) (*BufferEntry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Validate if entryId is outside the valid range [firstEntryId, firstEntryId + maxEntries)
	if entryId < b.FirstEntryId {
		return nil, fmt.Errorf("invalid entryId: %d smaller than %d", entryId, b.FirstEntryId)
	}

	// Validate if entryId exceeds the valid range [firstEntryId, firstEntryId + maxEntries)
	if entryId >= b.FirstEntryId+b.MaxEntries {
		return nil, fmt.Errorf("invalid entryId: %d larger than %d", entryId, b.FirstEntryId+b.MaxEntries)
	}

	relatedIdx := entryId - b.FirstEntryId
	entry := b.Entries[relatedIdx]
	if entry == nil {
		return nil, werr.ErrEntryNotFound.WithCauseErrMsg(fmt.Sprintf("entry not found for entryId: %d", entryId))
	}

	return entry, nil
}

func (b *SequentialBuffer) GetFirstEntryId() int64 {
	return b.FirstEntryId
}

func (b *SequentialBuffer) GetExpectedNextEntryId() int64 {
	return b.ExpectedNextEntryId.Load()
}

// NotifyEntriesInRange notifies all entries in the given range with the specified result
// For successful entries (result >= 0), each entry receives its own EntryId
// For failed entries (result < 0), all entries receive the error result
func (b *SequentialBuffer) NotifyEntriesInRange(ctx context.Context, startEntryId int64, endEntryId int64, result int64, resultErr error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	notifiedCount := 0
	var notifiedEntryIds []int64

	for entryId := startEntryId; entryId < endEntryId; entryId++ {
		if entryId < b.FirstEntryId || entryId >= b.FirstEntryId+b.MaxEntries {
			continue
		}

		relatedIdx := entryId - b.FirstEntryId
		entry := b.Entries[relatedIdx]
		if entry != nil && entry.NotifyChan != nil {
			// Track buffer wait latency
			if !entry.EnqueueTime.IsZero() {
				metrics.WpServerBufferWaitLatency.WithLabelValues(b.logIdStr).
					Observe(float64(time.Since(entry.EnqueueTime).Milliseconds()))
			}

			// Verify EntryId consistency for debugging
			if entry.EntryId != entryId {
				// This should not happen, but log it for debugging
				logger.Ctx(ctx).Warn("EntryId mismatch in buffer", zap.Int64("expected", entryId), zap.Int64("actual", entry.EntryId))
			}

			// For successful writes, send the entry's own ID
			// For failed writes, send the error result
			notifyValue := result
			if result >= 0 {
				notifyValue = entry.EntryId
			}

			sendErr := entry.NotifyChan.SendResult(ctx, &channel.AppendResult{
				SyncedId: notifyValue,
				Err:      resultErr,
			})
			if sendErr != nil {
				logger.Ctx(ctx).Warn("Send result to channel failed",
					zap.Int64("entryId", entry.EntryId),
					zap.Int64("notifyValue", notifyValue),
					zap.Error(sendErr))
			} else {
				notifiedCount++
				notifiedEntryIds = append(notifiedEntryIds, entry.EntryId)
			}
			entry.NotifyChan = nil // Clear the channel reference
		}
	}

	// Log notification summary for debugging
	if notifiedCount > 0 {
		if result >= 0 {
			logger.Ctx(ctx).Debug("Notified range entries with their own EntryIds (success)", zap.Int("count", notifiedCount), zap.Int64s("entries", notifiedEntryIds), zap.Int64("startEntryId", startEntryId), zap.Int64("endEntryId", endEntryId))
		} else {
			logger.Ctx(ctx).Debug("Notified range entries with error result", zap.Int("count", notifiedCount), zap.Int64s("entries", notifiedEntryIds), zap.Int64("result", result), zap.Int64("startEntryId", startEntryId), zap.Int64("endEntryId", endEntryId))
		}
	}
}

// NotifyAllPendingEntries notifies all entries with notification channels
// For successful entries (result >= 0), each entry receives its own EntryId
// For failed entries (result < 0), all entries receive the error result
func (b *SequentialBuffer) NotifyAllPendingEntries(ctx context.Context, result int64, resultErr error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	notifiedCount := 0
	var notifiedEntryIds []int64

	for _, entry := range b.Entries {
		if entry != nil && entry.NotifyChan != nil {
			// Track buffer wait latency
			if !entry.EnqueueTime.IsZero() {
				metrics.WpServerBufferWaitLatency.WithLabelValues(b.logIdStr).
					Observe(float64(time.Since(entry.EnqueueTime).Milliseconds()))
			}

			// For successful writes, send the entry's own ID
			// For failed writes, send the error result
			notifyValue := result
			if result >= 0 {
				notifyValue = entry.EntryId
			}

			sendErr := entry.NotifyChan.SendResult(ctx, &channel.AppendResult{
				SyncedId: notifyValue,
				Err:      resultErr,
			})
			if sendErr != nil {
				logger.Ctx(ctx).Warn("Send result to channel failed",
					zap.Int64("entryId", entry.EntryId),
					zap.Int64("notifyValue", notifyValue),
					zap.Error(sendErr),
				)
			} else {
				notifiedCount++
				notifiedEntryIds = append(notifiedEntryIds, entry.EntryId)
			}
			entry.NotifyChan = nil // Clear the channel reference
		}
	}

	// Log notification summary for debugging
	if notifiedCount > 0 {
		if result >= 0 {
			logger.Ctx(ctx).Debug("Notified all pending entries with their own EntryIds (success)", zap.Int("count", notifiedCount), zap.Int64s("entryIds", notifiedEntryIds))
		} else {
			logger.Ctx(ctx).Debug("Notified all pending entries with error result", zap.Int("count", notifiedCount), zap.Int64s("entryIds", notifiedEntryIds), zap.Int64("result", result))
		}
	}
}

func (b *SequentialBuffer) ReadEntriesToLast(fromEntryId int64) ([]*BufferEntry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.Entries) == 0 {
		return nil, werr.ErrLogWriterBufferEmpty
	}

	// the valid range [firstEntryId, firstEntryId + maxEntries)
	if fromEntryId < b.FirstEntryId || fromEntryId > b.FirstEntryId+b.MaxEntries {
		return nil, werr.ErrFileWriterInvalidEntryId.WithCauseErrMsg(
			fmt.Sprintf("fromId:%d not in [%d,%d)", fromEntryId, b.FirstEntryId, b.FirstEntryId+b.MaxEntries))
	}

	if fromEntryId == b.FirstEntryId+b.MaxEntries {
		return make([]*BufferEntry, 0), nil
	}

	return b.readEntriesRangeUnsafe(fromEntryId, b.FirstEntryId+b.MaxEntries)
}

// ReadEntriesRange reads entries from the buffer starting from the startEntryId to the endEntryId (Exclusive).
func (b *SequentialBuffer) ReadEntriesRange(startEntryId int64, endEntryId int64) ([]*BufferEntry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.readEntriesRangeUnsafe(startEntryId, endEntryId)
}

func (b *SequentialBuffer) readEntriesRangeUnsafe(startEntryId int64, endEntryId int64) ([]*BufferEntry, error) {
	if startEntryId >= b.FirstEntryId+b.MaxEntries || startEntryId < b.FirstEntryId {
		return nil, werr.ErrFileWriterInvalidEntryId.WithCauseErrMsg(
			fmt.Sprintf("startEntryId:%d not in [%d,%d)", startEntryId, b.FirstEntryId, b.FirstEntryId+b.MaxEntries))
	}

	if endEntryId > b.FirstEntryId+b.MaxEntries || endEntryId <= startEntryId {
		return nil, werr.ErrFileWriterInvalidEntryId.WithCauseErrMsg(
			fmt.Sprintf("endEntryId:%d not in [%d,%d)", endEntryId, startEntryId, b.FirstEntryId+b.MaxEntries))
	}

	// Extract the entries from the buffer
	ret := make([]*BufferEntry, endEntryId-startEntryId)
	for i := startEntryId; i < endEntryId; i++ {
		relatedIdx := i - b.FirstEntryId
		ret[i-startEntryId] = b.Entries[relatedIdx] // This can be nil
	}
	return ret, nil
}

// Reset clears the buffer and resets the sequence number.
func (b *SequentialBuffer) Reset(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Notify all pending entries with error before reset
	b.notifyAllPendingEntriesUnsafe(ctx, -1, fmt.Errorf("clear"))

	b.Entries = make([]*BufferEntry, b.MaxEntries)
	b.DataSize.Store(0)
	b.SequentialReadyDataSize.Store(0)
	b.ExpectedNextEntryId.Store(b.FirstEntryId)
}

// notifyAllPendingEntriesUnsafe is an internal method that assumes the mutex is already held
// For successful entries (result >= 0), each entry receives its own EntryId
// For failed entries (result < 0), all entries receive the error result
func (b *SequentialBuffer) notifyAllPendingEntriesUnsafe(ctx context.Context, result int64, resultErr error) {
	notifiedCount := 0
	var notifiedEntryIds []int64

	for _, entry := range b.Entries {
		if entry != nil && entry.NotifyChan != nil {
			// For successful writes, send the entry's own ID
			// For failed writes, send the error result
			notifyValue := result
			if result >= 0 {
				notifyValue = entry.EntryId
			}

			sendErr := entry.NotifyChan.SendResult(ctx, &channel.AppendResult{
				SyncedId: notifyValue,
				Err:      resultErr,
			})
			if sendErr != nil {
				logger.Ctx(ctx).Warn("Send result to channel failed",
					zap.Int64("entryId", entry.EntryId),
					zap.Int64("notifyValue", notifyValue),
					zap.Error(sendErr))
			} else {
				notifiedCount++
				notifiedEntryIds = append(notifiedEntryIds, entry.EntryId)
			}

			entry.NotifyChan = nil // Clear the channel reference
		}
	}

	// Log notification summary for debugging
	if notifiedCount > 0 {
		if result >= 0 {
			logger.Ctx(ctx).Debug("Reset - Notified pending entries with their own EntryIds (success)", zap.Int("count", notifiedCount), zap.Int64s("entryIds", notifiedEntryIds))
		} else {
			logger.Ctx(ctx).Debug("Reset - Notified pending entries with error result", zap.Int("count", notifiedCount), zap.Int64s("entryIds", notifiedEntryIds), zap.Int64("result", result))
		}
	}
}

// NotifyPendingEntryDirectly notifies a single entry directly with the specified result
// For successful entries (result >= 0), the entry receives the entryId
// For failed entries (result < 0), the entry receives the error result
func NotifyPendingEntryDirectly(ctx context.Context, logId, segId, entryId int64, notifyChan channel.ResultChannel, result int64, resultErr error) {
	// For successful writes, send the entry's own ID
	// For failed writes, send the error result
	notifyValue := result
	if result >= 0 {
		notifyValue = entryId
	}
	if result >= 0 {
		logger.Ctx(ctx).Debug("Notified pending entry directly with success",
			zap.Int64("logId", logId),
			zap.Int64("segId", segId),
			zap.Int64("entryId", entryId),
			zap.Int64("notifyValue", notifyValue),
			zap.String("ch", fmt.Sprintf("%p", notifyChan)))
	} else {
		logger.Ctx(ctx).Debug("Notified pending entry directly with fail",
			zap.Int64("logId", logId),
			zap.Int64("segId", segId),
			zap.Int64("entryId", entryId),
			zap.Int64("result", result),
			zap.String("ch", fmt.Sprintf("%p", notifyChan)))
	}
	sendErr := notifyChan.SendResult(ctx, &channel.AppendResult{
		SyncedId: notifyValue,
		Err:      resultErr,
	})
	if sendErr != nil {
		logger.Ctx(ctx).Warn("Send result to channel failed",
			zap.Int64("entryId", entryId),
			zap.Int64("notifyValue", notifyValue),
			zap.String("ch", fmt.Sprintf("%p", notifyChan)),
			zap.Error(sendErr))
	} else {
		logger.Ctx(ctx).Debug("Notified pending entry finish",
			zap.Int64("logId", logId),
			zap.Int64("segId", segId),
			zap.Int64("entryId", entryId),
			zap.Int64("notifyValue", notifyValue),
			zap.String("ch", fmt.Sprintf("%p", notifyChan)))
	}
}

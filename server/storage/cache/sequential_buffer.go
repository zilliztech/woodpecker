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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/common/werr"
)

// SequentialBuffer is a buffer that stores entries in a sequential manner.
type SequentialBuffer struct {
	Mu        sync.Mutex
	logId     int64
	segmentId int64

	Values   [][]byte     // values of entries
	MaxSize  int64        // max amount of entries
	DataSize atomic.Int64 // data bytes size of entries

	FirstEntryId        int64
	ExpectedNextEntryId atomic.Int64
}

func NewSequentialBuffer(logId int64, segmentId int64, startEntryId int64, maxSize int64) *SequentialBuffer {
	b := &SequentialBuffer{
		Values:       make([][]byte, maxSize),
		MaxSize:      maxSize,
		FirstEntryId: startEntryId,
	}
	b.ExpectedNextEntryId.Store(startEntryId)
	metrics.WpWriteBufferSlotsTotal.WithLabelValues(fmt.Sprintf("%d", logId), fmt.Sprintf("%d", segmentId)).Set(float64(maxSize))
	return b
}

func NewSequentialBufferWithData(logId int64, segmentId int64, startEntryId int64, maxSize int64, restData [][]byte) *SequentialBuffer {
	v := make([][]byte, maxSize)
	copy(v, restData)
	b := &SequentialBuffer{
		Values:       v,
		MaxSize:      maxSize,
		FirstEntryId: startEntryId,
	}
	b.ExpectedNextEntryId.Store(startEntryId)
	metrics.WpWriteBufferSlotsTotal.WithLabelValues(fmt.Sprintf("%d", logId), fmt.Sprintf("%d", segmentId)).Set(float64(maxSize))
	return b
}

// WriteEntry writes a new entry into the buffer.
func (b *SequentialBuffer) WriteEntry(entryId int64, value []byte) (int64, error) {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if entryId < b.FirstEntryId {
		return -1, werr.ErrInvalidEntryId.WithCauseErrMsg(fmt.Sprintf("invalid entryId: %d smaller then %d", entryId, b.FirstEntryId))
	}

	if entryId >= b.FirstEntryId+b.MaxSize {
		return -1, werr.ErrInvalidEntryId.WithCauseErrMsg(fmt.Sprintf("Out of buffer bounds, maybe disorder and write too fast, entryId: %d larger then %d", entryId, b.FirstEntryId+b.MaxSize))
	}

	relatedIdx := entryId - b.FirstEntryId
	b.Values[relatedIdx] = value
	b.DataSize.Add(int64(len(value)))

	// increase the ExpectedNextEntryId if necessary
	for addedId := entryId; addedId < b.FirstEntryId+b.MaxSize; addedId++ {
		if b.Values[addedId-b.FirstEntryId] != nil && addedId == b.ExpectedNextEntryId.Load() {
			b.ExpectedNextEntryId.Add(1)
			metrics.WpWriteBufferSlotsTotal.WithLabelValues(fmt.Sprintf("%d", b.logId), fmt.Sprintf("%d", b.segmentId)).Dec()
		} else {
			break
		}
	}

	return entryId, nil
}

func (b *SequentialBuffer) ReadEntry(entryId int64) ([]byte, error) {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if entryId < b.FirstEntryId {
		return nil, errors.New(fmt.Sprintf("invalid entryId: %d smaller then %d", entryId, b.FirstEntryId))
	}

	if entryId >= b.FirstEntryId+b.MaxSize {
		return nil, errors.New(fmt.Sprintf("invalid entryId: %d larger then %d", entryId, b.FirstEntryId+b.MaxSize))
	}

	relatedIdx := entryId - b.FirstEntryId
	value := b.Values[relatedIdx]
	if value == nil {
		return nil, errors.New(fmt.Sprintf("entry not found for entryId: %d", entryId))
	}

	return value, nil
}

func (b *SequentialBuffer) GetFirstEntryId() int64 {
	return b.FirstEntryId
}

func (b *SequentialBuffer) GetExpectedNextEntryId() int64 {
	return b.ExpectedNextEntryId.Load()
}

func (b *SequentialBuffer) ReadEntriesToLast(fromEntryId int64) ([][]byte, error) {
	if len(b.Values) == 0 {
		return nil, werr.ErrBufferIsEmpty
	}

	if fromEntryId < b.FirstEntryId || fromEntryId > b.FirstEntryId+b.MaxSize {
		return nil, werr.ErrInvalidEntryId.WithCauseErrMsg(
			fmt.Sprintf("fromId:%d not in [%d,%d)", fromEntryId, b.FirstEntryId, b.FirstEntryId+b.MaxSize))
	}

	if fromEntryId == b.FirstEntryId+b.MaxSize {
		return make([][]byte, 0), nil
	}

	return b.ReadEntriesRange(fromEntryId, b.FirstEntryId+b.MaxSize)
}

// ReadEntriesRange reads bytes from the buffer starting from the startEntryId to the endEntryId (Exclusive).
func (b *SequentialBuffer) ReadEntriesRange(startEntryId int64, endEntryId int64) ([][]byte, error) {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if startEntryId >= b.FirstEntryId+b.MaxSize || startEntryId < b.FirstEntryId {
		return nil, werr.ErrInvalidEntryId.WithCauseErrMsg(
			fmt.Sprintf("startEntryId:%d not in [%d,%d)", startEntryId, b.FirstEntryId, b.FirstEntryId+b.MaxSize))
	}

	if endEntryId > b.FirstEntryId+b.MaxSize || endEntryId < startEntryId {
		return nil, werr.ErrInvalidEntryId.WithCauseErrMsg(
			fmt.Sprintf("endEntryId:%d not in [%d,%d)", endEntryId, startEntryId, b.FirstEntryId+b.MaxSize))
	}

	// Extract the bytes from the buffer
	ret := make([][]byte, endEntryId-startEntryId)
	copy(ret, b.Values[startEntryId-b.FirstEntryId:endEntryId-b.FirstEntryId])
	return ret, nil
}

// Reset clears the buffer and resets the sequence number.
func (b *SequentialBuffer) Reset() {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	b.Values = make([][]byte, b.MaxSize)
	b.DataSize.Store(0)
	b.ExpectedNextEntryId.Store(b.FirstEntryId)
}

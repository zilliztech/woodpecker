// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stability

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func id(seg, entry int64) *log.LogMessageId { return &log.LogMessageId{SegmentId: seg, EntryId: entry} }

// A retried append lands after appends submitted later. That is how a WAL with
// appends in flight behaves, so it must not read as out of order.
func TestAnalyze_RetriedEntryLandsLaterIsInOrder(t *testing.T) {
	t0 := time.Now().Add(-time.Minute)
	writes := []WriteRecord{
		{Seq: 0, SubmitAt: t0, DoneAt: t0.Add(10 * time.Millisecond), Attempts: 1, Id: id(0, 0)},
		{Seq: 1, SubmitAt: t0, DoneAt: t0.Add(300 * time.Millisecond), Attempts: 2, LastErr: errors.New("rolling"), Id: id(1, 1)},
		{Seq: 2, SubmitAt: t0, DoneAt: t0.Add(20 * time.Millisecond), Attempts: 1, Id: id(1, 0)},
	}
	reads := []ReadRecord{
		{Seq: 0, ReadAt: t0.Add(30 * time.Millisecond), Id: id(0, 0)},
		{Seq: 2, ReadAt: t0.Add(40 * time.Millisecond), Id: id(1, 0)},
		{Seq: 1, ReadAt: t0.Add(310 * time.Millisecond), Id: id(1, 1)},
	}
	rep := Analyze("l", writes, reads, nil)
	assert.Empty(t, rep.OutOfOrder)
	assert.Empty(t, rep.Misplaced)
	assert.Empty(t, rep.Unread)
	assert.Equal(t, 1, rep.Retried)
	assert.Equal(t, 1, rep.Retries)
	assert.Equal(t, []int64{0, 1}, rep.Segments)
	assert.Equal(t, 300*time.Millisecond, rep.MaxLatency, "latency runs from submission to the final success")
}

func TestAnalyze_DetectsLogOrderViolations(t *testing.T) {
	t0 := time.Now().Add(-time.Minute)
	writes := []WriteRecord{
		{Seq: 0, SubmitAt: t0, DoneAt: t0, Attempts: 1, Id: id(0, 0)},
		{Seq: 1, SubmitAt: t0, DoneAt: t0, Attempts: 1, Id: id(0, 1)},
		{Seq: 2, SubmitAt: t0, DoneAt: t0, Attempts: 1, Id: id(0, 2)},
	}
	reads := []ReadRecord{
		{Seq: 1, ReadAt: t0, Id: id(0, 1)},
		{Seq: 0, ReadAt: t0, Id: id(0, 0)}, // behind the previous position
		{Seq: 2, ReadAt: t0, Id: id(0, 5)}, // not where it was acked
		{Seq: 2, ReadAt: t0, Id: id(0, 5)}, // twice
	}
	rep := Analyze("l", writes, reads, nil)
	assert.Equal(t, []int64{0}, rep.OutOfOrder)
	assert.Equal(t, []int64{2}, rep.Misplaced)
	assert.Equal(t, []int64{2}, rep.Duplicates)
}

func TestAnalyze_SeparatesFailedHungAndUnread(t *testing.T) {
	t0 := time.Now().Add(-time.Minute)
	writes := []WriteRecord{
		{Seq: 0, SubmitAt: t0, DoneAt: t0, Attempts: 1, Id: id(0, 0)},
		{Seq: 1, SubmitAt: t0, DoneAt: t0, Attempts: 3, Err: errors.New("fenced")},
		{Seq: 2, SubmitAt: t0, DoneAt: t0, Attempts: 1, Hung: true, Err: errors.New("no result")},
	}
	rep := Analyze("l", writes, nil, []ReadError{{At: t0, Err: errors.New("unavailable")}})
	assert.Len(t, rep.Failed, 1)
	assert.Len(t, rep.Hung, 1)
	assert.Equal(t, 1, rep.Acked)
	assert.Equal(t, []int64{0}, rep.Unread, "acked but never delivered")
	assert.Len(t, rep.ReadErrs, 1)
}

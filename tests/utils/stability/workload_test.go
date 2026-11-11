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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// scriptedReader returns the given sequences in order, each after delay, then
// blocks until its context ends.
type scriptedReader struct {
	name  string
	seqs  []int64
	delay time.Duration

	mu   sync.Mutex
	next int
}

func (r *scriptedReader) ReadNext(ctx context.Context) (*log.LogMessage, error) {
	r.mu.Lock()
	if r.next >= len(r.seqs) {
		r.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	seq := r.seqs[r.next]
	r.next++
	r.mu.Unlock()
	select {
	case <-time.After(r.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &log.LogMessage{Id: id(0, seq), Payload: Payload(r.name, seq)}, nil
}

func (r *scriptedReader) Close(context.Context) error { return nil }
func (r *scriptedReader) GetName() string             { return r.name }

// A retried append can land after the last submitted entry, so the highest
// sequence is read before it. Waiting must go on until the retried entry is
// read too, not stop at the highest sequence.
func TestReaderWaitFor_WaitsForEveryAckedEntry(t *testing.T) {
	// Seq 1 was retried and landed last; seq 3 is the highest.
	rd := StartReader("l", &scriptedReader{name: "l", seqs: []int64{0, 2, 3, 1}, delay: 20 * time.Millisecond})
	acked := map[int64]bool{0: true, 1: true, 2: true, 3: true}

	reads, errs := rd.WaitFor(acked, 5*time.Second)
	assert.Empty(t, errs)
	got := make([]int64, 0, len(reads))
	for _, r := range reads {
		got = append(got, r.Seq)
	}
	assert.Equal(t, []int64{0, 2, 3, 1}, got, "stopped before the retried entry was read")
}

// An entry that never arrives costs the wait, and no more.
func TestReaderWaitFor_GivesUpAfterWait(t *testing.T) {
	rd := StartReader("l", &scriptedReader{name: "l", seqs: []int64{0}, delay: time.Millisecond})
	start := time.Now()
	reads, _ := rd.WaitFor(map[int64]bool{0: true, 1: true}, 300*time.Millisecond)
	assert.Len(t, reads, 1)
	assert.Less(t, time.Since(start), 2*time.Second)
}

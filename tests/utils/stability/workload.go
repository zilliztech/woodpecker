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

// Package stability drives a steady write/tail-read workload against a log and
// measures it the way the application sees it, so a test can assert that a
// fault did not stall or break the log.
//
// Latency is taken from the moment the application submits an entry to the
// moment its result arrives. That includes any time the entry spends queued
// inside the client behind an earlier one, which is exactly the time the
// client's own append-latency metric does not see.
//
// The workload behaves the way Milvus drives a woodpecker WAL, so that what it
// counts as a failure is what Milvus would surface: a failed append is retried
// with backoff until it succeeds or the writer is fenced
// (walAdaptorImpl.retryAppendWhenRecoverableError), and a failed read is
// followed by reading on (the scanner backs off and resumes). Retries cost
// latency, and latency is what the budget checks.
package stability

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// Append retry backoff, as Milvus's WAL adaptor retries a failed append.
const (
	retryInitialInterval = 10 * time.Millisecond
	retryMaxInterval     = 5 * time.Second
)

// WriteRecord is one submitted entry.
type WriteRecord struct {
	Seq      int64
	SubmitAt time.Time
	DoneAt   time.Time // when the final result arrived
	Err      error     // final error: the append was given up (fenced), or never completed
	Hung     bool      // no final result by the time the writer stopped waiting
	Attempts int       // appends issued for this entry, 1 when it succeeded first time
	LastErr  error     // the most recent error an attempt returned, retried or not
	Id       *log.LogMessageId
}

// Latency is submit-to-result time.
func (r WriteRecord) Latency() time.Duration { return r.DoneAt.Sub(r.SubmitAt) }

// Writer submits one entry every interval without waiting for earlier results,
// the way a WAL keeps several appends in flight.
type Writer struct {
	name     string
	w        log.LogWriter
	interval time.Duration

	mu      sync.Mutex
	records []*WriteRecord

	stop    chan struct{}
	loopW   sync.WaitGroup
	resultW sync.WaitGroup
}

// Payload is the entry body for seq; ParseSeq inverts it.
func Payload(name string, seq int64) []byte {
	return []byte(fmt.Sprintf("%s|%d|", name, seq))
}

// ParseSeq extracts the sequence number written by Payload.
func ParseSeq(name string, payload []byte) (int64, error) {
	parts := strings.Split(string(payload), "|")
	if len(parts) < 2 || parts[0] != name {
		return -1, fmt.Errorf("foreign payload %q", payload)
	}
	return strconv.ParseInt(parts[1], 10, 64)
}

// StartWriter begins submitting to w.
func StartWriter(name string, w log.LogWriter, interval time.Duration) *Writer {
	wr := &Writer{name: name, w: w, interval: interval, stop: make(chan struct{})}
	wr.loopW.Add(1)
	go wr.loop()
	return wr
}

func (wr *Writer) loop() {
	defer wr.loopW.Done()
	ticker := time.NewTicker(wr.interval)
	defer ticker.Stop()
	for seq := int64(0); ; seq++ {
		select {
		case <-wr.stop:
			return
		case <-ticker.C:
		}
		rec := &WriteRecord{Seq: seq, SubmitAt: time.Now(), Attempts: 1}
		wr.mu.Lock()
		wr.records = append(wr.records, rec)
		wr.mu.Unlock()
		// The first attempt is submitted in order; results, and any retries,
		// are handled concurrently, as several appends are in flight in a WAL.
		msg := &log.WriteMessage{Payload: Payload(wr.name, seq)}
		ch := wr.w.WriteAsync(context.Background(), msg)
		wr.resultW.Add(1)
		go func() {
			defer wr.resultW.Done()
			wr.awaitWithRetry(rec, msg, ch)
		}()
	}
}

// awaitWithRetry waits for an append's result and retries a failure with
// backoff until one succeeds or the writer is fenced.
func (wr *Writer) awaitWithRetry(rec *WriteRecord, msg *log.WriteMessage, ch <-chan *log.WriteResult) {
	interval := retryInitialInterval
	for {
		res := <-ch
		err := errors.New("nil write result")
		if res != nil {
			err = res.Err
		}
		if err == nil {
			wr.mu.Lock()
			rec.DoneAt, rec.Id = time.Now(), res.LogMessageId
			wr.mu.Unlock()
			return
		}
		wr.mu.Lock()
		rec.LastErr = err
		if werr.ErrLogWriterLockLost.Is(err) {
			rec.DoneAt, rec.Err = time.Now(), err
			wr.mu.Unlock()
			return
		}
		rec.Attempts++
		wr.mu.Unlock()

		time.Sleep(interval)
		if interval *= 2; interval > retryMaxInterval {
			interval = retryMaxInterval
		}
		ch = wr.w.WriteAsync(context.Background(), msg)
	}
}

// Stop stops submitting and waits up to wait for outstanding results. Entries
// still outstanding after that are reported with a timeout error.
//
// A submission that is itself blocked (WriteAsync not returning) counts as
// outstanding too: Stop does not wait for the submit loop past the deadline.
func (wr *Writer) Stop(wait time.Duration) []WriteRecord {
	close(wr.stop)
	deadline := time.After(wait)
	done := make(chan struct{})
	go func() { wr.loopW.Wait(); wr.resultW.Wait(); close(done) }()
	select {
	case <-done:
	case <-deadline:
	}
	wr.mu.Lock()
	defer wr.mu.Unlock()
	out := make([]WriteRecord, len(wr.records))
	for i, r := range wr.records {
		out[i] = *r
		if out[i].DoneAt.IsZero() {
			out[i].DoneAt = time.Now()
			out[i].Hung = true
			out[i].Err = fmt.Errorf("no result within %v of stopping", wait)
		}
	}
	return out
}

// ReadRecord is one entry delivered to a tail reader.
type ReadRecord struct {
	Seq    int64
	ReadAt time.Time
	Id     *log.LogMessageId
}

// SlowRead is a ReadNext call that took longer than slowReadThreshold: time the
// reader spent inside the client, as opposed to between calls.
type SlowRead struct {
	Start, End time.Time
	Id         *log.LogMessageId // what the call returned, nil on error
	Err        error
}

const slowReadThreshold = 300 * time.Millisecond

// ReadError is a ReadNext failure.
type ReadError struct {
	At  time.Time
	Err error
}

// Reader tails a log.
type Reader struct {
	name string
	r    log.LogReader

	mu      sync.Mutex
	records []ReadRecord
	errs    []ReadError
	slow    []SlowRead

	cancel context.CancelFunc
	done   chan struct{}
}

// StartReader begins tailing r.
func StartReader(name string, r log.LogReader) *Reader {
	ctx, cancel := context.WithCancel(context.Background())
	rd := &Reader{name: name, r: r, cancel: cancel, done: make(chan struct{})}
	go rd.loop(ctx)
	return rd
}

func (rd *Reader) loop(ctx context.Context) {
	defer close(rd.done)
	for ctx.Err() == nil {
		callStart := time.Now()
		msg, err := rd.r.ReadNext(ctx)
		now := time.Now()
		if now.Sub(callStart) >= slowReadThreshold && ctx.Err() == nil {
			sr := SlowRead{Start: callStart, End: now, Err: err}
			if msg != nil {
				sr.Id = msg.Id
			}
			rd.mu.Lock()
			rd.slow = append(rd.slow, sr)
			rd.mu.Unlock()
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			rd.mu.Lock()
			rd.errs = append(rd.errs, ReadError{At: now, Err: err})
			rd.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		seq, perr := ParseSeq(rd.name, msg.Payload)
		rd.mu.Lock()
		if perr != nil {
			rd.errs = append(rd.errs, ReadError{At: now, Err: perr})
		} else {
			rd.records = append(rd.records, ReadRecord{Seq: seq, ReadAt: now, Id: msg.Id})
		}
		rd.mu.Unlock()
	}
}

// lastSeq returns the highest sequence read so far, or -1.
func (rd *Reader) lastSeq() int64 {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	if len(rd.records) == 0 {
		return -1
	}
	return rd.records[len(rd.records)-1].Seq
}

// SlowReads returns the ReadNext calls that took longer than slowReadThreshold.
func (rd *Reader) SlowReads() []SlowRead {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	return append([]SlowRead(nil), rd.slow...)
}

// WaitFor waits up to wait for the reader to deliver seq, then stops it.
func (rd *Reader) WaitFor(seq int64, wait time.Duration) ([]ReadRecord, []ReadError) {
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) && rd.lastSeq() < seq {
		time.Sleep(50 * time.Millisecond)
	}
	rd.cancel()
	<-rd.done
	rd.mu.Lock()
	defer rd.mu.Unlock()
	return append([]ReadRecord(nil), rd.records...), append([]ReadError(nil), rd.errs...)
}

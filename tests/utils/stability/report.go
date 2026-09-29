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
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// DefaultMaxStall is the longest a single entry may take, submit to result or
// ack to read, while one pod is being replaced. It covers a segment roll and
// fence, which take well under a second when the path is healthy, and it is
// far below the tens of seconds a stuck connect costs.
const DefaultMaxStall = 2 * time.Second

// MaxStall is DefaultMaxStall unless WP_STABILITY_MAX_STALL overrides it
// (a Go duration, e.g. "5s").
func MaxStall(t *testing.T) time.Duration {
	if v := os.Getenv("WP_STABILITY_MAX_STALL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("bad WP_STABILITY_MAX_STALL %q: %v", v, err)
		}
		return d
	}
	return DefaultMaxStall
}

// Report summarises one log's workload.
type Report struct {
	Name string

	Submitted  int
	Acked      int
	Failed     []WriteRecord // given up with an error (writer fenced)
	Hung       []WriteRecord // no result by the time the writer stopped waiting
	Retried    int           // entries that needed more than one append
	Retries    int           // extra appends issued in total
	P50, P99   time.Duration
	MaxLatency time.Duration // submit to final success, retries included
	Slowest    []WriteRecord // longest latencies first
	Segments   []int64       // segments the acked entries landed in, in order

	Read          int
	ReadErrs      []ReadError // ReadNext failures the reader read on from; reported, not failed on
	Unread        []int64     // acked but not delivered to the tail reader within the wait
	Duplicates    []int64
	OutOfOrder    []int64 // read at a log position not after the one before it
	Misplaced     []int64 // read at a different position than the one its append was acked at
	MaxReadLag    time.Duration
	MaxReadLagSeq int64
	LaggiestReads []LaggedRead // largest ack-to-read lags first
}

// LaggedRead is one entry's path from ack to the tail reader.
type LaggedRead struct {
	Seq           int64
	Id            *log.LogMessageId
	AckAt, ReadAt time.Time
	Lag           time.Duration
}

// readLagSettle excludes the last entries from the read-lag measure: an entry
// becomes visible to a tail reader when a later append carries the LAC past it,
// and the last few have no later append.
const readLagSettle = time.Second

func idBefore(a, b *log.LogMessageId) bool {
	if a.SegmentId != b.SegmentId {
		return a.SegmentId < b.SegmentId
	}
	return a.EntryId < b.EntryId
}

// Analyze builds a Report from what a Writer and a Reader recorded.
//
// Order is checked by log position, not by submission: a retried append lands
// after appends submitted later, as it does in a WAL with appends in flight.
// What the log guarantees, and what is checked, is that a reader sees every
// acked entry once, at the position it was acked at, in position order.
func Analyze(name string, writes []WriteRecord, reads []ReadRecord, readErrs []ReadError) Report {
	rep := Report{Name: name, Submitted: len(writes), ReadErrs: readErrs, MaxReadLagSeq: -1}

	acked := make(map[int64]WriteRecord)
	var lats []time.Duration
	var lastSubmit time.Time
	var ackedIds []*log.LogMessageId
	for _, w := range writes {
		if w.SubmitAt.After(lastSubmit) {
			lastSubmit = w.SubmitAt
		}
		if w.Attempts > 1 {
			rep.Retried++
			rep.Retries += w.Attempts - 1
		}
		switch {
		case w.Hung:
			rep.Hung = append(rep.Hung, w)
			continue
		case w.Err != nil:
			rep.Failed = append(rep.Failed, w)
			continue
		}
		rep.Acked++
		acked[w.Seq] = w
		lats = append(lats, w.Latency())
		if w.Id != nil {
			ackedIds = append(ackedIds, w.Id)
		}
	}
	sort.Slice(ackedIds, func(i, j int) bool { return idBefore(ackedIds[i], ackedIds[j]) })
	for _, id := range ackedIds {
		if len(rep.Segments) == 0 || rep.Segments[len(rep.Segments)-1] != id.SegmentId {
			rep.Segments = append(rep.Segments, id.SegmentId)
		}
	}
	if len(lats) > 0 {
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		rep.P50 = lats[len(lats)/2]
		rep.P99 = lats[len(lats)*99/100]
		rep.MaxLatency = lats[len(lats)-1]
	}
	byLatency := append([]WriteRecord(nil), writes...)
	sort.Slice(byLatency, func(i, j int) bool { return byLatency[i].Latency() > byLatency[j].Latency() })
	if len(byLatency) > 5 {
		byLatency = byLatency[:5]
	}
	rep.Slowest = byLatency

	seen := make(map[int64]bool)
	var prevId *log.LogMessageId
	for _, r := range reads {
		rep.Read++
		if seen[r.Seq] {
			rep.Duplicates = append(rep.Duplicates, r.Seq)
			continue
		}
		seen[r.Seq] = true
		if r.Id != nil {
			if prevId != nil && !idBefore(prevId, r.Id) {
				rep.OutOfOrder = append(rep.OutOfOrder, r.Seq)
			}
			prevId = r.Id
		}
		w, ok := acked[r.Seq]
		if !ok {
			continue
		}
		if w.Id != nil && r.Id != nil && (w.Id.SegmentId != r.Id.SegmentId || w.Id.EntryId != r.Id.EntryId) {
			rep.Misplaced = append(rep.Misplaced, r.Seq)
		}
		if w.DoneAt.After(lastSubmit.Add(-readLagSettle)) {
			continue
		}
		// A read can precede the writer's own result callback; that is lag 0.
		lag := r.ReadAt.Sub(w.DoneAt)
		if lag > rep.MaxReadLag {
			rep.MaxReadLag, rep.MaxReadLagSeq = lag, r.Seq
		}
		rep.LaggiestReads = append(rep.LaggiestReads, LaggedRead{Seq: r.Seq, Id: r.Id, AckAt: w.DoneAt, ReadAt: r.ReadAt, Lag: lag})
	}
	for seq := range acked {
		if !seen[seq] {
			rep.Unread = append(rep.Unread, seq)
		}
	}
	sort.Slice(rep.Unread, func(i, j int) bool { return rep.Unread[i] < rep.Unread[j] })
	sort.Slice(rep.LaggiestReads, func(i, j int) bool { return rep.LaggiestReads[i].Lag > rep.LaggiestReads[j].Lag })
	if len(rep.LaggiestReads) > 5 {
		rep.LaggiestReads = rep.LaggiestReads[:5]
	}
	return rep
}

func (r Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "[%s] submitted=%d acked=%d failed=%d hung=%d retried=%d(retries=%d) latency p50=%v p99=%v max=%v segments=%v\n",
		r.Name, r.Submitted, r.Acked, len(r.Failed), len(r.Hung), r.Retried, r.Retries, r.P50, r.P99, r.MaxLatency, r.Segments)
	fmt.Fprintf(&b, "[%s] read=%d readErrs=%d unread=%d dup=%d outOfOrder=%d misplaced=%d maxReadLag=%v (seq %d)\n",
		r.Name, r.Read, len(r.ReadErrs), len(r.Unread), len(r.Duplicates), len(r.OutOfOrder), len(r.Misplaced), r.MaxReadLag, r.MaxReadLagSeq)
	for _, w := range r.Slowest {
		seg, ent := int64(-1), int64(-1)
		if w.Id != nil {
			seg, ent = w.Id.SegmentId, w.Id.EntryId
		}
		fmt.Fprintf(&b, "[%s]   slow seq=%d submit=%s latency=%v attempts=%d id=%d/%d lastErr=%v\n",
			r.Name, w.Seq, w.SubmitAt.Format("15:04:05.000"), w.Latency(), w.Attempts, seg, ent, w.LastErr)
	}
	for _, lr := range r.LaggiestReads {
		fmt.Fprintf(&b, "[%s]   lagged seq=%d id=%d/%d ack=%s read=%s lag=%v\n",
			r.Name, lr.Seq, lr.Id.SegmentId, lr.Id.EntryId, lr.AckAt.Format("15:04:05.000"), lr.ReadAt.Format("15:04:05.000"), lr.Lag)
	}
	writeList := func(what string, ws []WriteRecord) {
		for i, w := range ws {
			if i == 5 {
				fmt.Fprintf(&b, "[%s]   ... %d more %s\n", r.Name, len(ws)-5, what)
				break
			}
			fmt.Fprintf(&b, "[%s]   %s seq=%d at=%s attempts=%d: %v\n", r.Name, what, w.Seq, w.DoneAt.Format("15:04:05.000"), w.Attempts, w.Err)
		}
	}
	writeList("failed", r.Failed)
	writeList("hung", r.Hung)
	for i, e := range r.ReadErrs {
		if i == 5 {
			fmt.Fprintf(&b, "[%s]   ... %d more read errors\n", r.Name, len(r.ReadErrs)-5)
			break
		}
		fmt.Fprintf(&b, "[%s]   read err at=%s: %v\n", r.Name, e.At.Format("15:04:05.000"), e.Err)
	}
	return b.String()
}

// AssertSmooth asserts both AssertComplete and AssertLatency.
func AssertSmooth(t *testing.T, r Report, maxStall time.Duration) {
	t.Helper()
	AssertComplete(t, r)
	AssertLatency(t, r, maxStall)
}

// AssertComplete asserts what must hold however slow the log was: every append
// completed, and every acked entry reached the tail reader once, at the
// position it was acked at, in log order. Read errors the reader read on from
// are reported by the Report and are not failures; what they cost shows up as
// read lag.
func AssertComplete(t *testing.T, r Report) {
	t.Helper()
	if len(r.Hung) > 0 {
		t.Errorf("[%s] %d appends never completed (hung, not failed), first seq %d", r.Name, len(r.Hung), r.Hung[0].Seq)
	}
	if len(r.Failed) > 0 {
		t.Errorf("[%s] %d appends were given up: %v", r.Name, len(r.Failed), r.Failed[0].Err)
	}
	if len(r.Unread) > 0 {
		t.Errorf("[%s] %d acked entries not delivered to the tail reader within the wait, first seq %d (a delivery check, not a durability check)", r.Name, len(r.Unread), r.Unread[0])
	}
	if len(r.Duplicates) > 0 {
		t.Errorf("[%s] %d entries read twice, first seq %d", r.Name, len(r.Duplicates), r.Duplicates[0])
	}
	if len(r.OutOfOrder) > 0 {
		t.Errorf("[%s] %d entries read out of log order, first seq %d", r.Name, len(r.OutOfOrder), r.OutOfOrder[0])
	}
	if len(r.Misplaced) > 0 {
		t.Errorf("[%s] %d entries read at a different position than acked, first seq %d", r.Name, len(r.Misplaced), r.Misplaced[0])
	}
}

// AssertLatency asserts that no append took longer than maxStall end to end,
// and no acked entry reached the tail reader later than maxStall after its ack.
func AssertLatency(t *testing.T, r Report, maxStall time.Duration) {
	t.Helper()
	for _, v := range latencyViolations(r, maxStall) {
		t.Error(v)
	}
}

// ReportLatency logs what AssertLatency would fail on, without failing.
func ReportLatency(t *testing.T, r Report, maxStall time.Duration) {
	t.Helper()
	for _, v := range latencyViolations(r, maxStall) {
		t.Log("[latency budget, report only] " + v)
	}
}

func latencyViolations(r Report, maxStall time.Duration) []string {
	var out []string
	if r.MaxLatency > maxStall {
		out = append(out, fmt.Sprintf("[%s] append stalled %v end to end, retries included (budget %v)", r.Name, r.MaxLatency, maxStall))
	}
	if r.MaxReadLag > maxStall {
		out = append(out, fmt.Sprintf("[%s] tail read lagged %v behind the ack (budget %v)", r.Name, r.MaxReadLag, maxStall))
	}
	return out
}

// LatencyReportOnly reports whether WP_STABILITY_LATENCY=report asks for the
// stall budget to be reported rather than asserted, for environments whose
// timing is not yet known to be steady enough to gate on.
func LatencyReportOnly() bool {
	return os.Getenv("WP_STABILITY_LATENCY") == "report"
}

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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// Defaults every tier of the service stability suite runs its workload with.
const (
	DefaultLogs     = 3
	DefaultInterval = 20 * time.Millisecond
	DefaultWarmup   = 3 * time.Second
	DefaultSettle   = 5 * time.Second
)

// VanishDetection is what a replica that vanishes costs before it is given up.
// A vanished replica answers nothing, not even a reset, so the client learns of
// it only when the bound on an append's first response expires (2s), after
// which the segment rolls. Faster detection would mean either a shorter bound,
// which would also fail replicas that are merely slow, or not waiting for the
// slowest replica at all, which is a change to the append pipeline this suite
// does not assume.
const VanishDetection = 2 * time.Second

// SessionConfig says what workload a Session runs.
type SessionConfig struct {
	Logs     int           // logs, each with a writer and a tail reader; DefaultLogs if 0
	Interval time.Duration // between appends of one log; DefaultInterval if 0
	Warmup   time.Duration // before StartSession returns; DefaultWarmup if 0
	Prefix   string        // log name prefix; "service_stability" if empty
	// ReaderClient, if set, is the client the tail readers use, so they share
	// no connections with the writers, the way a reader in another process
	// would. The writers' client is used otherwise.
	ReaderClient woodpecker.Client
}

// LogSession is one log's writer, tail reader and what they recorded.
type LogSession struct {
	Name   string
	Handle log.LogHandle
	Writer log.LogWriter
	Reader log.LogReader
	W      *Writer
	R      *Reader
}

// Session is a steady workload on several logs: each log gets an append every
// Interval with several in flight, and a tail reader from its first entry.
type Session struct {
	t    *testing.T
	Logs []*LogSession
	// ExtraStall is added to the stall budget for faults that can only be
	// detected by a timeout; see VanishDetection.
	ExtraStall time.Duration
}

// StartSession creates the logs, starts their writers and readers, and returns
// once the workload has warmed up.
func StartSession(t *testing.T, client woodpecker.Client, cfg SessionConfig) *Session {
	t.Helper()
	if cfg.Logs == 0 {
		cfg.Logs = DefaultLogs
	}
	if cfg.Interval == 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.Warmup == 0 {
		cfg.Warmup = DefaultWarmup
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "service_stability"
	}
	readerClient := cfg.ReaderClient
	if readerClient == nil {
		readerClient = client
	}

	ctx := context.Background()
	s := &Session{t: t}
	for i := 0; i < cfg.Logs; i++ {
		name := fmt.Sprintf("%s_%d_%d", cfg.Prefix, time.Now().UnixNano(), i)
		require.NoError(t, client.CreateLog(ctx, name))
		handle, err := client.OpenLog(ctx, name)
		require.NoError(t, err)
		writer, err := handle.OpenLogWriter(ctx)
		require.NoError(t, err)
		readHandle := handle
		if readerClient != client {
			readHandle, err = readerClient.OpenLog(ctx, name)
			require.NoError(t, err)
		}
		reader, err := readHandle.OpenLogReader(ctx, &log.LogMessageId{SegmentId: 0, EntryId: 0}, "service-stability-reader")
		require.NoError(t, err)
		s.Logs = append(s.Logs, &LogSession{
			Name: name, Handle: handle, Writer: writer, Reader: reader,
			W: StartWriter(name, writer, cfg.Interval),
			R: StartReader(name, reader),
		})
	}
	time.Sleep(cfg.Warmup)
	return s
}

// QuorumAddrs returns, for each log, the service addresses of the nodes that
// hold its current writable segment.
func (s *Session) QuorumAddrs() [][]string {
	out := make([][]string, len(s.Logs))
	for i, l := range s.Logs {
		seg := l.Handle.GetCurrentWritableSegmentHandle(context.Background())
		require.NotNil(s.t, seg, "log %s has no writable segment", l.Name)
		qi, err := seg.GetQuorumInfo(context.Background())
		require.NoError(s.t, err)
		out[i] = append(out[i], qi.Nodes...)
		s.t.Logf("log %s writable segment %d quorum nodes %v", l.Name, seg.GetId(context.Background()), qi.Nodes)
	}
	return out
}

// BusiestQuorumAddr is the node in the most logs' writable quorums, so a fault
// on it hits as many logs as possible.
func (s *Session) BusiestQuorumAddr() string {
	return mostCommon(s.QuorumAddrs(), false)
}

// BusiestReadAddr is the node the most tail readers are reading from. A reader
// starts at the first node of the segment's quorum and stays on it while it
// answers.
func (s *Session) BusiestReadAddr() string {
	return mostCommon(s.QuorumAddrs(), true)
}

// IdleAddr is one of all that is in no log's writable quorum, or "".
func (s *Session) IdleAddr(all []string) string {
	used := make(map[string]bool)
	for _, nodes := range s.QuorumAddrs() {
		for _, n := range nodes {
			used[n] = true
		}
	}
	for _, a := range all {
		if !used[a] {
			return a
		}
	}
	return ""
}

func mostCommon(quorums [][]string, firstOnly bool) string {
	counts := make(map[string]int)
	for _, nodes := range quorums {
		if firstOnly && len(nodes) > 0 {
			nodes = nodes[:1]
		}
		for _, n := range nodes {
			counts[n]++
		}
	}
	best, bestCount := "", 0
	for n, c := range counts {
		if c > bestCount || (c == bestCount && n < best) {
			best, bestCount = n, c
		}
	}
	return best
}

// Finish lets the cluster settle, stops the workload, and asserts it was
// smooth: every append completed, and every acked entry reached the tail
// reader once, in log order, at its acked position, both within the stall
// budget (MaxStall plus ExtraStall). Under LatencyReportOnly the budget is
// reported rather than asserted; completeness and order are always asserted.
func (s *Session) Finish(settle time.Duration) []Report {
	t := s.t
	t.Helper()
	time.Sleep(settle)

	maxStall := MaxStall(t) + s.ExtraStall
	reportOnly := LatencyReportOnly()
	var reports []Report
	for _, l := range s.Logs {
		writes := l.W.Stop(60 * time.Second)
		Bounded(t, l.Name+" writer close", 45*time.Second, func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := l.Writer.Close(closeCtx); err != nil {
				t.Logf("[%s] writer close: %v", l.Name, err)
			}
		})

		lastAcked := int64(-1)
		for _, w := range writes {
			if w.Err == nil && !w.Hung && w.Seq > lastAcked {
				lastAcked = w.Seq
			}
		}
		reads, readErrs := l.R.WaitFor(lastAcked, 30*time.Second)
		Bounded(t, l.Name+" reader close", 30*time.Second, func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = l.Reader.Close(closeCtx)
		})

		rep := Analyze(l.Name, writes, reads, readErrs)
		t.Log("\n" + rep.String())
		for _, sr := range l.R.SlowReads() {
			var at string
			if sr.Id != nil {
				at = fmt.Sprintf("%d/%d", sr.Id.SegmentId, sr.Id.EntryId)
			}
			t.Logf("[%s]   slow ReadNext %s -> %s took %v returned %s err=%v", l.Name,
				sr.Start.Format("15:04:05.000"), sr.End.Format("15:04:05.000"), sr.End.Sub(sr.Start), at, sr.Err)
		}
		AssertComplete(t, rep)
		if reportOnly {
			ReportLatency(t, rep, maxStall)
		} else {
			AssertLatency(t, rep, maxStall)
		}
		reports = append(reports, rep)
	}
	return reports
}

// Bounded runs fn and reports an error if it has not returned within d, so a
// client call that ignores its context fails the test instead of hanging the
// whole run. The goroutine is abandoned.
func Bounded(t *testing.T, what string, d time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(d):
		t.Errorf("%s did not return within %v (its context was not honoured)", what, d)
	}
}

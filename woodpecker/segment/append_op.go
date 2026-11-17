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

package segment

import (
	"context"
	"fmt"
	"sync/atomic"

	"go.opentelemetry.io/otel/trace"

	"github.com/zilliztech/woodpecker/common/bitset"
	"github.com/zilliztech/woodpecker/common/metrics"
	"github.com/zilliztech/woodpecker/proto"
)

// AppendOp is one logical append. Immutable entry data can be shared by replica
// appenders. Quorum state is updated only under the owning appender's lock; the
// callback is delivered once by the appender's ordered callback dispatcher.
// Transport channels, retries, deadlines and admission credits live elsewhere.
type AppendOp struct {
	traceParent     trace.SpanContext
	bucketName      string
	rootPath        string
	logId           int64
	segmentId       int64
	entryId         int64
	value           []byte
	callback        func(segmentId, entryId int64, err error)
	logNs           string
	quorumInfo      *proto.QuorumInfo
	nodeScopes      []string
	ackSet          *bitset.BitSet
	finalFailureSet *bitset.BitSet
	terminalErr     error
	completed       atomic.Bool
	fastCalled      atomic.Bool
}

func NewAppendOp(bucketName, rootPath string, logId, segmentId, entryId int64, value []byte, callback func(int64, int64, error), quorumInfo *proto.QuorumInfo, nodeScopes []string) *AppendOp {
	return &AppendOp{
		bucketName: bucketName, rootPath: rootPath, logId: logId, segmentId: segmentId,
		entryId: entryId, value: value, callback: callback, quorumInfo: quorumInfo,
		logNs: metrics.BuildLogNs(bucketName, rootPath), nodeScopes: nodeScopes,
		ackSet: &bitset.BitSet{}, finalFailureSet: &bitset.BitSet{},
	}
}

func (op *AppendOp) Identifier() string {
	return fmt.Sprintf("%d/%d/%d", op.logId, op.segmentId, op.entryId)
}
func (op *AppendOp) Size() int64 { return int64(len(op.value)) }

// applyReplicaResult consumes only final outcomes, never an intermediate failed
// attempt. Caller holds the appender lock. A false return means quorum is still
// possible and this logical append must remain pending.
func (op *AppendOp) applyReplicaResult(replica int, err error) bool {
	if op.completed.Load() {
		return true
	}
	if err == nil {
		if op.ackSet.SetAndCount(replica) >= int(op.quorumInfo.Aq) {
			op.completed.Store(true)
		}
	} else if op.finalFailureSet.SetAndCount(replica) >= int(op.quorumInfo.Wq-op.quorumInfo.Aq+1) {
		op.terminalErr = err
		op.completed.Store(true)
	}
	return op.completed.Load()
}

func (op *AppendOp) FastFail(_ context.Context, err error) {
	if op.fastCalled.CompareAndSwap(false, true) {
		op.callback(op.segmentId, op.entryId, err)
	}
}

func (op *AppendOp) FastSuccess(_ context.Context) {
	if op.fastCalled.CompareAndSwap(false, true) {
		op.callback(op.segmentId, op.entryId, nil)
	}
}

func (op *AppendOp) toLogEntry() *proto.LogEntry {
	return &proto.LogEntry{
		SegId:   op.segmentId,
		EntryId: op.entryId,
		Values:  op.value,
	}
}

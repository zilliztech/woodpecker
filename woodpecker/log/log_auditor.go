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

package log

import (
	"context"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/segment"
)

// This file holds the auditor's per-segment maintenance work, split by work type so each
// auditor cycle reads as a short sequence of clearly named steps (compact / distribute marks /
// collect-and-clean truncated), shared by both writer implementations. A segment is in exactly
// one state per metadata snapshot, so the passes are independent and their order does not
// matter.

// maxCompactedNotifyPerCycle bounds how many sealed segments actually DO compacted-mark
// distribution work in one auditor cycle (settled segments are an in-memory fast path and
// are not counted), so a large backlog — e.g. the first cycles after upgrading a cluster
// with many pre-existing sealed segments — cannot swamp a single cycle with etcd writes and
// RPC fanouts. The remainder is picked up on subsequent cycles.
const maxCompactedNotifyPerCycle = 64

// compactStats summarizes one compactCompletedSegments pass for the auditor cycle log.
type compactStats struct {
	processed int
	compacted int
	failed    int
}

// compactCompletedSegments compacts every Completed segment in the snapshot, sequentially by
// design (to keep each log's background work light, since a cluster may host many logs). A
// per-segment failure is logged and skipped; it never aborts the pass.
func compactCompletedSegments(ctx context.Context, logHandle LogHandle, segs map[int64]*meta.SegmentMeta) compactStats {
	var st compactStats
	for _, seg := range segs {
		if seg.Metadata.State != proto.SegmentState_Completed {
			continue
		}
		st.processed++
		recoverySegmentHandle, err := logHandle.GetRecoverableSegmentHandle(ctx, seg.Metadata.SegNo)
		if err != nil {
			logger.Ctx(ctx).Warn("get log segment failed when log auditor running", zap.String("logName", logHandle.GetName()), zap.Int64("logId", logHandle.GetId()), zap.Int64("segId", seg.Metadata.SegNo), zap.Error(err))
			st.failed++
			continue
		}
		if err := recoverySegmentHandle.Compact(ctx); err != nil {
			logger.Ctx(ctx).Warn("auditor maintain the log segment failed", zap.String("logName", logHandle.GetName()), zap.Int64("logId", logHandle.GetId()), zap.Int64("segId", seg.Metadata.SegNo), zap.Error(err))
			st.failed++
			continue
		}
		st.compacted++
		logger.Ctx(ctx).Info("Successfully compacted segment",
			zap.String("logName", logHandle.GetName()),
			zap.Int64("logId", logHandle.GetId()),
			zap.Int64("segmentId", seg.Metadata.SegNo))
	}
	return st
}

// distributeCompactedMarks drives async compacted-mark distribution (root/marking) for Sealed
// segments, bounded to maxCompactedNotifyPerCycle segments doing real work per cycle. It returns
// how many segments were driven this cycle. A per-segment failure is logged and retried next
// cycle; it never aborts the pass.
func distributeCompactedMarks(ctx context.Context, logHandle LogHandle, notifyManager segment.SegmentCompactedNotifyManager, segs map[int64]*meta.SegmentMeta) int {
	driven := 0
	for _, seg := range segs {
		if seg.Metadata.State != proto.SegmentState_Sealed {
			continue
		}
		if driven >= maxCompactedNotifyPerCycle {
			break
		}
		advanced, err := notifyManager.EnsureSegmentNotified(ctx, logHandle.GetName(), logHandle.GetId(), seg.Metadata.SegNo)
		if err != nil {
			logger.Ctx(ctx).Warn("auditor compacted-mark notify failed; will retry next cycle", zap.String("logName", logHandle.GetName()), zap.Int64("logId", logHandle.GetId()), zap.Int64("segId", seg.Metadata.SegNo), zap.Error(err))
		}
		if advanced {
			driven++
		}
	}
	return driven
}

// collectTruncatedSegments returns the ids of Truncated segments eligible for cleanup.
func collectTruncatedSegments(segs map[int64]*meta.SegmentMeta) []int64 {
	truncated := make([]int64, 0)
	for _, seg := range segs {
		if seg.Metadata.State == proto.SegmentState_Truncated {
			truncated = append(truncated, seg.Metadata.SegNo)
		}
	}
	return truncated
}

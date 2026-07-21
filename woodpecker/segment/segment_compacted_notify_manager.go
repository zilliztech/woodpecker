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
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

// notifyPendingManualAfter is how long a segment's compacted-mark distribution keeps
// auto-retrying unacked nodes (measured from the durable record's StartTime, so it is
// restart-proof) before the record is parked as NOTIFY_PENDING_MANUAL for operator triage.
// The consequences of parking are mild by design: a data-holding node self-heals via the
// server-side pull reconcile, and a node that never held the segment's data only loses the
// serve-from-object-storage read optimization.
const notifyPendingManualAfter = 30 * time.Minute

// SegmentCompactedNotifyManager drives compacted-mark (NotifySegmentCompacted) distribution
// for sealed segments. It is the Sealed-phase sibling of SegmentCleanupManager: per-node
// progress is durable in a SegmentCompactedNotifyStatus record (root/marking/<logId>/<segId>),
// resumed exactly (per node) across writer restarts, while the segment state machine itself
// stays untouched. Driven by the writer's auditor, one idempotent call per sealed segment per
// cycle.
type SegmentCompactedNotifyManager interface {
	// EnsureSegmentNotified idempotently advances mark distribution for one sealed segment:
	// creates the marking record if absent, re-sends only to unacked nodes, completes the
	// record when every node has acked, and parks it as PENDING_MANUAL after the retry budget.
	// advanced reports whether real work was performed (false for the settled fast path), so
	// the caller's per-cycle budget only counts segments that actually did work.
	EnsureSegmentNotified(ctx context.Context, logName string, logId int64, segmentId int64) (advanced bool, err error)

	// MarkSegmentReaped records, in memory, that a segment has entered truncate reclamation on
	// this writer: a Truncated segment never needs mark distribution (its data — local files,
	// object storage, and the marking record — is being deleted), so Ensure must skip it even
	// when the distributor is still working on a stale snapshot that shows it Sealed. Both the
	// distributor and the truncate GC run in this same process, so this in-memory sync closes
	// the late-notify-resurrects-a-cleaned-directory race down to a single in-flight RPC.
	MarkSegmentReaped(segmentId int64)

	// CleanupOrphanedStatuses deletes marking records whose segmentId is below minSegmentId —
	// their segment metadata is already gone (segments are cleaned in ascending order), so the
	// records are orphans. Mirrors SegmentCleanupManager.CleanupOrphanedStatuses.
	CleanupOrphanedStatuses(ctx context.Context, logId int64, minSegmentId int64) error
}

type segmentCompactedNotifyManagerImpl struct {
	bucketName string
	rootPath   string
	metadata   meta.MetadataProvider
	clientPool client.LogStoreClientPool

	mu         sync.Mutex
	inProgress map[int64]bool     // segId -> an EnsureSegmentNotified call is running
	settled    map[int64]struct{} // segId -> record is COMPLETED/PENDING_MANUAL; skip without etcd reads
	seeded     bool               // one-time List seed of `settled` performed
	seedWarned bool               // seed failure already logged (reset on success) to avoid per-segment spam
}

// NewSegmentCompactedNotifyManager creates a new compacted-mark distribution manager.
func NewSegmentCompactedNotifyManager(bucketName string, rootPath string, metadata meta.MetadataProvider, clientPool client.LogStoreClientPool) SegmentCompactedNotifyManager {
	return &segmentCompactedNotifyManagerImpl{
		bucketName: bucketName,
		rootPath:   rootPath,
		metadata:   metadata,
		clientPool: clientPool,
		inProgress: make(map[int64]bool),
		settled:    make(map[int64]struct{}),
	}
}

// nodeNotifyResult holds the result of a notify request to a single node
type nodeNotifyResult struct {
	nodeAddress string
	success     bool
	errorMsg    string
}

// isSettledNotifyState reports whether a marking record is terminal for the manager: no more
// auto-retry, and (once seeded) skipped by EnsureSegmentNotified. COMPLETED = all nodes acked;
// PENDING_MANUAL = retry budget spent, awaiting an operator; OPERATOR_CONFIRMED = an operator
// ran `wp marking confirm`, so it stays settled durably across restarts instead of being
// rebuilt. Physical deletion of any of these is left to truncate-reap / orphan-sweep.
func isSettledNotifyState(state proto.SegmentCompactedNotifyState) bool {
	switch state {
	case proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED,
		proto.SegmentCompactedNotifyState_NOTIFY_PENDING_MANUAL,
		proto.SegmentCompactedNotifyState_NOTIFY_OPERATOR_CONFIRMED:
		return true
	default:
		return false
	}
}

func (s *segmentCompactedNotifyManagerImpl) EnsureSegmentNotified(ctx context.Context, logName string, logId int64, segmentId int64) (bool, error) {
	// One-time seed: warm the settled cache from the durable records so a restarted writer
	// skips already-settled segments without per-segment etcd reads. A seed failure DEGRADES
	// rather than blocks: distribution proceeds with an empty cache (settled segments cost one
	// Get each until a later seed succeeds) — a failing List (e.g. transient etcd trouble) must
	// not disable mark distribution for the whole log, nor bypass the caller's per-cycle budget
	// by erroring out before any real work.
	s.seedIfNecessary(ctx, logId)

	s.mu.Lock()
	if _, ok := s.settled[segmentId]; ok {
		s.mu.Unlock()
		return false, nil
	}
	if s.inProgress[segmentId] {
		s.mu.Unlock()
		return false, nil // another call is already driving this segment
	}
	s.inProgress[segmentId] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.inProgress, segmentId)
		s.mu.Unlock()
	}()

	// Load (or lazily create) the durable per-node progress record.
	status, err := s.metadata.GetSegmentCompactedNotifyStatus(ctx, logId, segmentId)
	if err != nil {
		logger.Ctx(ctx).Warn("get segment compacted notify status failed",
			zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Error(err))
		return true, err
	}

	switch {
	case status == nil:
		quorum, quorumErr := s.getQuorumForSegment(ctx, logName, segmentId)
		if quorumErr != nil {
			return true, quorumErr
		}
		status = &proto.SegmentCompactedNotifyStatus{
			LogId:              logId,
			SegmentId:          segmentId,
			State:              proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS,
			StartTime:          uint64(time.Now().UnixMilli()),
			LastUpdateTime:     uint64(time.Now().UnixMilli()),
			QuorumNotifyStatus: make(map[string]bool, len(quorum.Nodes)),
		}
		for _, nodeAddress := range quorum.Nodes {
			status.QuorumNotifyStatus[nodeAddress] = false
		}
		if createErr := s.metadata.CreateSegmentCompactedNotifyStatus(ctx, status); createErr != nil {
			logger.Ctx(ctx).Warn("create segment compacted notify status failed",
				zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Error(createErr))
			return true, createErr
		}
		logger.Ctx(ctx).Info("created segment compacted notify status; starting mark distribution",
			zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId),
			zap.Strings("nodes", quorum.Nodes))
	case isSettledNotifyState(status.State):
		s.markSettled(segmentId)
		return false, nil
	default:
		// IN_PROGRESS: resume. Reconcile the per-node map against the CURRENT quorum — add any
		// newly-added node (unacked) AND prune any node no longer in the quorum. Without the
		// prune, a node that was unacked at record creation and later removed from the quorum
		// would keep len(unacked) != 0 forever, parking the record as PENDING_MANUAL for a node
		// that no longer belongs to the segment.
		quorum, quorumErr := s.getQuorumForSegment(ctx, logName, segmentId)
		if quorumErr != nil {
			return true, quorumErr
		}
		if status.QuorumNotifyStatus == nil {
			status.QuorumNotifyStatus = make(map[string]bool, len(quorum.Nodes))
		}
		inQuorum := make(map[string]struct{}, len(quorum.Nodes))
		for _, nodeAddress := range quorum.Nodes {
			inQuorum[nodeAddress] = struct{}{}
			if _, exists := status.QuorumNotifyStatus[nodeAddress]; !exists {
				status.QuorumNotifyStatus[nodeAddress] = false
			}
		}
		for nodeAddress := range status.QuorumNotifyStatus {
			if _, ok := inQuorum[nodeAddress]; !ok {
				delete(status.QuorumNotifyStatus, nodeAddress)
			}
		}
	}

	// Fan out to the nodes that have not acked yet.
	var nodesToNotify []string
	for nodeAddress, acked := range status.QuorumNotifyStatus {
		if !acked {
			nodesToNotify = append(nodesToNotify, nodeAddress)
		}
	}
	if len(nodesToNotify) == 0 {
		// Nothing left (e.g. a resumed record already fully acked): just complete it.
		return true, s.finalizeStatus(ctx, logName, status, nil)
	}

	results := s.sendNotifyToNodes(ctx, logName, logId, segmentId, nodesToNotify)
	return true, s.finalizeStatus(ctx, logName, status, results)
}

// seedIfNecessary lists the log's marking records once per manager lifetime and caches the
// settled (COMPLETED/PENDING_MANUAL/OPERATOR_CONFIRMED) ones, so restarts resume exactly
// without re-reading etcd per segment on every pass. Best-effort: a List failure is logged
// ONCE (per failure streak) and retried on later calls; callers proceed either way, so a
// broken seed can only cost extra per-segment Gets, never disable distribution.
func (s *segmentCompactedNotifyManagerImpl) seedIfNecessary(ctx context.Context, logId int64) {
	s.mu.Lock()
	if s.seeded {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	statuses, err := s.metadata.ListSegmentCompactedNotifyStatus(ctx, logId)
	if err != nil {
		s.mu.Lock()
		warned := s.seedWarned
		s.seedWarned = true
		s.mu.Unlock()
		if !warned {
			logger.Ctx(ctx).Warn("seed: list segment compacted notify statuses failed; proceeding unseeded (will retry)",
				zap.Int64("logId", logId), zap.Error(err))
		}
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.seedWarned = false
	if s.seeded {
		return
	}
	settledCount := 0
	for _, st := range statuses {
		if isSettledNotifyState(st.State) {
			s.settled[st.SegmentId] = struct{}{}
			settledCount++
		}
	}
	s.seeded = true
	logger.Ctx(ctx).Info("seeded compacted notify settled cache",
		zap.Int64("logId", logId), zap.Int("records", len(statuses)), zap.Int("settled", settledCount))
}

// MarkSegmentReaped implements the in-process reap sync (see the interface doc).
func (s *segmentCompactedNotifyManagerImpl) MarkSegmentReaped(segmentId int64) {
	s.markSettled(segmentId)
}

func (s *segmentCompactedNotifyManagerImpl) markSettled(segmentId int64) {
	s.mu.Lock()
	s.settled[segmentId] = struct{}{}
	s.mu.Unlock()
}

// getQuorumForSegment reads the segment's quorum nodes from its metadata (read-only; the
// segment state machine is never written here). Falls back to the embed/standalone single
// node exactly like SegmentCleanupManager.
func (s *segmentCompactedNotifyManagerImpl) getQuorumForSegment(ctx context.Context, logName string, segmentId int64) (*proto.QuorumInfo, error) {
	segMeta, err := s.metadata.GetSegmentMetadata(ctx, logName, segmentId)
	if err != nil {
		logger.Ctx(ctx).Warn("get segment metadata for compacted notify failed",
			zap.String("logName", logName), zap.Int64("segmentId", segmentId), zap.Error(err))
		return nil, fmt.Errorf("failed to get segment metadata: %w", err)
	}
	quorum := segMeta.Metadata.GetQuorum()
	if quorum == nil || len(quorum.Nodes) == 0 {
		quorum = &proto.QuorumInfo{
			Id: 0, Es: 1, Wq: 1, Aq: 1,
			Nodes: []string{"127.0.0.1"},
		}
	}
	return quorum, nil
}

// sendNotifyToNodes concurrently sends NotifySegmentCompacted to the given nodes, each on its
// own time-bounded context, and collects per-node results.
func (s *segmentCompactedNotifyManagerImpl) sendNotifyToNodes(ctx context.Context, logName string, logId int64, segmentId int64, nodes []string) []nodeNotifyResult {
	var wg sync.WaitGroup
	var resultsMu sync.Mutex
	results := make([]nodeNotifyResult, 0, len(nodes))

	for _, nodeAddress := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			// Per-node bound: a node that connects but stalls cannot hold this pass beyond
			// the timeout; the next auditor cycle retries it.
			nodeCtx, cancel := context.WithTimeout(ctx, notifySegmentCompactedTimeout)
			defer cancel()
			err := s.sendNotifyRequestToNode(nodeCtx, logId, segmentId, addr)
			result := nodeNotifyResult{nodeAddress: addr, success: err == nil}
			if err != nil {
				result.errorMsg = err.Error()
				logger.Ctx(ctx).Debug("notify segment compacted to node failed; will retry next cycle",
					zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId),
					zap.String("node", addr), zap.Error(err))
			}
			resultsMu.Lock()
			results = append(results, result)
			resultsMu.Unlock()
		}(nodeAddress)
	}

	wg.Wait()
	return results
}

func (s *segmentCompactedNotifyManagerImpl) sendNotifyRequestToNode(ctx context.Context, logId int64, segmentId int64, nodeAddress string) error {
	logStoreCli, err := s.clientPool.GetLogStoreClient(ctx, nodeAddress)
	if err != nil {
		return err
	}
	return logStoreCli.NotifySegmentCompacted(ctx, s.bucketName, s.rootPath, logId, segmentId)
}

// finalizeStatus applies the pass's per-node results to the record and writes it back once:
// COMPLETED when every node acked, PENDING_MANUAL when the retry budget (from StartTime) is
// spent with nodes still unacked, IN_PROGRESS otherwise (retried next auditor cycle).
func (s *segmentCompactedNotifyManagerImpl) finalizeStatus(ctx context.Context, logName string, status *proto.SegmentCompactedNotifyStatus, results []nodeNotifyResult) error {
	for _, r := range results {
		if r.success {
			status.QuorumNotifyStatus[r.nodeAddress] = true
		}
	}

	var unacked []string
	for nodeAddress, acked := range status.QuorumNotifyStatus {
		if !acked {
			unacked = append(unacked, nodeAddress)
		}
	}

	switch {
	case len(unacked) == 0:
		status.State = proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED
		status.ErrorMessage = ""
	case time.Since(time.UnixMilli(int64(status.StartTime))) > notifyPendingManualAfter:
		status.State = proto.SegmentCompactedNotifyState_NOTIFY_PENDING_MANUAL
		status.ErrorMessage = fmt.Sprintf("nodes unacked after %s of retries: %s",
			notifyPendingManualAfter, strings.Join(unacked, ","))
		logger.Ctx(ctx).Warn("compacted-mark distribution parked for manual handling (wp notify-pending)",
			zap.String("logName", logName), zap.Int64("logId", status.LogId), zap.Int64("segmentId", status.SegmentId),
			zap.Strings("unackedNodes", unacked))
	default:
		status.State = proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS
	}
	status.LastUpdateTime = uint64(time.Now().UnixMilli())

	if err := s.metadata.UpdateSegmentCompactedNotifyStatus(ctx, status); err != nil {
		logger.Ctx(ctx).Warn("update segment compacted notify status failed",
			zap.String("logName", logName), zap.Int64("logId", status.LogId), zap.Int64("segmentId", status.SegmentId), zap.Error(err))
		return err
	}

	if status.State == proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED {
		s.markSettled(status.SegmentId)
		logger.Ctx(ctx).Info("compacted-mark distribution completed for segment",
			zap.String("logName", logName), zap.Int64("logId", status.LogId), zap.Int64("segmentId", status.SegmentId))
	} else if status.State == proto.SegmentCompactedNotifyState_NOTIFY_PENDING_MANUAL {
		s.markSettled(status.SegmentId)
	}
	return nil
}

func (s *segmentCompactedNotifyManagerImpl) CleanupOrphanedStatuses(ctx context.Context, logId int64, minSegmentId int64) error {
	allStatuses, err := s.metadata.ListSegmentCompactedNotifyStatus(ctx, logId)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to list compacted notify statuses for orphan check",
			zap.Int64("logId", logId), zap.Error(err))
		return err
	}
	if len(allStatuses) == 0 {
		return nil
	}

	orphanCount := 0
	cleanedCount := 0
	for _, status := range allStatuses {
		if status.SegmentId >= minSegmentId {
			continue
		}
		// Segment metadata already deleted (segments are cleaned in ascending order):
		// the marking record is an orphan, including PENDING_MANUAL ones — the need
		// for a mark dies with the segment.
		orphanCount++
		// Delete by the record's OWN LogId, not the caller's, as defense-in-depth: even if a
		// scan ever returned a cross-log record, we never delete the wrong log's segment.
		if deleteErr := s.metadata.DeleteSegmentCompactedNotifyStatus(ctx, status.LogId, status.SegmentId); deleteErr != nil {
			logger.Ctx(ctx).Warn("Failed to delete orphaned compacted notify status",
				zap.Int64("logId", status.LogId), zap.Int64("segmentId", status.SegmentId), zap.Error(deleteErr))
		} else {
			cleanedCount++
			s.mu.Lock()
			delete(s.settled, status.SegmentId)
			s.mu.Unlock()
		}
	}

	if orphanCount > 0 {
		logger.Ctx(ctx).Info("Orphaned compacted notify status check completed",
			zap.Int64("logId", logId), zap.Int64("minSegmentId", minSegmentId),
			zap.Int("orphansFound", orphanCount), zap.Int("orphansCleaned", cleanedCount))
	}
	return nil
}

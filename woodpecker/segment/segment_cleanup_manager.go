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
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/woodpecker/client"
)

// SegmentCleanupManager manages segment cleanup operations
type SegmentCleanupManager interface {
	// CleanupSegment sends cleanup requests to all quorum nodes concurrently
	CleanupSegment(ctx context.Context, logName string, logId int64, segmentId int64) error

	// CleanupOrphanedStatuses cleans up orphaned cleanup status records
	// whose segmentId is less than minSegmentId. These are leftover records
	// from segments whose metadata was already deleted but cleanup status
	// deletion was interrupted (e.g., by a crash).
	CleanupOrphanedStatuses(ctx context.Context, logId int64, minSegmentId int64) error
}

type segmentCleanupManagerImpl struct {
	bucketName         string
	rootPath           string
	metadata           meta.MetadataProvider
	clientPool         client.LogStoreClientPool
	cleanupMutex       sync.Mutex
	inProgressCleanups map[string]bool
}

// nodeCleanupResult holds the result of a cleanup request to a single node
type nodeCleanupResult struct {
	nodeAddress string
	success     bool
	errorMsg    string
}

// NewSegmentCleanupManager creates a new segment cleanup manager
func NewSegmentCleanupManager(bucketName string, rootPath string, metadata meta.MetadataProvider, clientPool client.LogStoreClientPool) SegmentCleanupManager {
	return &segmentCleanupManagerImpl{
		bucketName:         bucketName,
		rootPath:           rootPath,
		metadata:           metadata,
		clientPool:         clientPool,
		inProgressCleanups: make(map[string]bool),
	}
}

// CleanupSegment sends cleanup requests to all nodes in the quorum
func (s *segmentCleanupManagerImpl) CleanupSegment(ctx context.Context, logName string, logId int64, segmentId int64) error {
	logger.Ctx(ctx).Info("Starting segment cleanup operation",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId))

	// Generate cleanup task ID
	cleanupKey := fmt.Sprintf("%d-%d", logId, segmentId)

	// Lock cleanup state management
	s.cleanupMutex.Lock()

	// Check if cleanup task is already in progress
	if s.inProgressCleanups[cleanupKey] {
		s.cleanupMutex.Unlock()
		logger.Ctx(ctx).Info("Segment cleanup already in progress", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId))
		return nil
	}

	// Mark cleanup task as in progress
	s.inProgressCleanups[cleanupKey] = true
	s.cleanupMutex.Unlock()

	logger.Ctx(ctx).Info("Segment cleanup task registered and started",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.String("cleanupKey", cleanupKey))

	// Ensure cleanup in-progress flag is removed after completion
	defer func() {
		s.cleanupMutex.Lock()
		delete(s.inProgressCleanups, cleanupKey)
		s.cleanupMutex.Unlock()
		logger.Ctx(ctx).Info("Segment cleanup task deregistered",
			zap.String("logName", logName),
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segmentId),
			zap.String("cleanupKey", cleanupKey))
	}()

	// 1. Check if segment already has cleanup status
	existingStatus, err := s.metadata.GetSegmentCleanupStatus(ctx, logId, segmentId)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get segment cleanup status", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Error(err))
		return err
	}

	// TODO  Currently only support embed standalone mode
	// 2. Get segment info and find corresponding quorum info
	// segMeta, err := s.metadata.GetSegmentMetadata(ctx, logName, segmentId)
	// quorum, err := s.metadata.GetQuorumInfo(ctx, segMeta.QuorumId)
	quorum := &proto.QuorumInfo{
		Id: 0,
		Es: 1,
		Wq: 1,
		Aq: 1,
		Nodes: []string{
			"127.0.0.1",
		},
	}

	logger.Ctx(ctx).Info("Quorum configuration loaded for segment cleanup",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.Int64("quorumId", quorum.Id),
		zap.Strings("nodes", quorum.Nodes),
		zap.Bool("hasExistingStatus", existingStatus != nil))

	// Handle different logic based on whether cleanup status exists
	if existingStatus != nil {
		return s.handleExistingCleanupStatus(ctx, logName, logId, segmentId, quorum, existingStatus)
	} else {
		return s.createNewCleanupTask(ctx, logName, logId, segmentId, quorum)
	}
}

// handleExistingCleanupStatus handles existing cleanup status
func (s *segmentCleanupManagerImpl) handleExistingCleanupStatus(
	ctx context.Context,
	logName string,
	logId int64,
	segmentId int64,
	quorum *proto.QuorumInfo,
	status *proto.SegmentCleanupStatus,
) error {
	// Case where cleanup is already completed
	if status.State == proto.SegmentCleanupState_CLEANUP_COMPLETED {
		logger.Ctx(ctx).Info("Segment cleanup already completed", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId))
		// delete segmentMeta, delete segment clean info
		deleteSegMetaErr := s.metadata.DeleteSegmentMetadata(ctx, logName, logId, segmentId, proto.SegmentState_Truncated)
		if deleteSegMetaErr != nil && !werr.ErrSegmentNotFound.Is(deleteSegMetaErr) {
			logger.Ctx(ctx).Warn("failed to clean truncated segment metadata, will retry next auditor cycle", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Error(deleteSegMetaErr))
			return deleteSegMetaErr
		} else {
			logger.Ctx(ctx).Info("Successfully deleted segment metadata after cleanup completion",
				zap.String("logName", logName),
				zap.Int64("logId", logId),
				zap.Int64("segmentId", segmentId))
		}
		deleteCleanSegStatusErr := s.metadata.DeleteSegmentCleanupStatus(ctx, logId, segmentId)
		if deleteCleanSegStatusErr != nil {
			logger.Ctx(ctx).Warn("failed to clean truncated segment clean status", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Error(deleteCleanSegStatusErr))
		} else {
			logger.Ctx(ctx).Info("Successfully deleted segment cleanup status after completion",
				zap.String("logName", logName),
				zap.Int64("logId", logId),
				zap.Int64("segmentId", segmentId))
		}
		return nil
	}

	// Check if status is stale (possibly leftover status)
	lastUpdateTime := time.UnixMilli(int64(status.LastUpdateTime))
	isStaleStatus := time.Since(lastUpdateTime) > 10*time.Minute

	if isStaleStatus {
		logger.Ctx(ctx).Info("Found stale cleanup status, continuing with incomplete nodes", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Time("lastUpdateTime", lastUpdateTime), zap.String("state", status.State.String()))
	} else if status.State == proto.SegmentCleanupState_CLEANUP_FAILED {
		logger.Ctx(ctx).Info("Previous cleanup failed, continuing with incomplete nodes", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.String("previousError", status.ErrorMessage))
	} else {
		logger.Ctx(ctx).Info("Cleanup in progress, continuing with incomplete nodes", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId))
	}

	// Update status to continue cleanup
	status.State = proto.SegmentCleanupState_CLEANUP_IN_PROGRESS
	status.LastUpdateTime = uint64(time.Now().UnixMilli())

	// Count nodes that need cleanup
	nodesToCleanup := 0
	nodesCompleted := 0
	// Ensure status includes all required nodes (in case quorum node configuration has changed)
	for _, nodeAddress := range quorum.Nodes {
		if _, exists := status.QuorumCleanupStatus[nodeAddress]; !exists {
			status.QuorumCleanupStatus[nodeAddress] = false
			nodesToCleanup++
		} else if status.QuorumCleanupStatus[nodeAddress] {
			nodesCompleted++
		} else {
			nodesToCleanup++
		}
	}

	logger.Ctx(ctx).Info("Resuming segment cleanup with existing status",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.Int("totalNodes", len(quorum.Nodes)),
		zap.Int("nodesCompleted", nodesCompleted),
		zap.Int("nodesToCleanup", nodesToCleanup),
		zap.String("previousState", status.State.String()))

	// Update status
	err := s.metadata.UpdateSegmentCleanupStatus(ctx, status)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to update segment cleanup status", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Error(err))
		return err
	}

	logger.Ctx(ctx).Info("Successfully updated segment cleanup status to IN_PROGRESS",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId))

	// Continue processing remaining nodes
	return s.continueCleanupForRemainingNodes(ctx, logName, logId, segmentId, quorum, status)
}

// createNewCleanupTask creates a new cleanup task
func (s *segmentCleanupManagerImpl) createNewCleanupTask(
	ctx context.Context,
	logName string,
	logId int64,
	segmentId int64,
	quorum *proto.QuorumInfo,
) error {
	// For cases where no cleanup status is found, create a new cleanup task
	logger.Ctx(ctx).Info("No existing cleanup status found, start creating new cleanup task", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId))

	// Create cleanup status record
	cleanupStatus := &proto.SegmentCleanupStatus{
		LogId:               logId,
		SegmentId:           segmentId,
		State:               proto.SegmentCleanupState_CLEANUP_IN_PROGRESS,
		StartTime:           uint64(time.Now().UnixMilli()),
		LastUpdateTime:      uint64(time.Now().UnixMilli()),
		QuorumCleanupStatus: make(map[string]bool),
	}

	// Initialize cleanup status for all quorum nodes to false
	for _, nodeAddress := range quorum.Nodes {
		// Use node address as key
		cleanupStatus.QuorumCleanupStatus[nodeAddress] = false
	}

	logger.Ctx(ctx).Info("Created new segment cleanup status record",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.Int64("quorumId", quorum.Id),
		zap.Int("totalNodes", len(quorum.Nodes)),
		zap.Strings("nodes", quorum.Nodes),
		zap.Uint64("startTime", cleanupStatus.StartTime))

	// Store initial cleanup status
	err := s.metadata.CreateSegmentCleanupStatus(ctx, cleanupStatus)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to create segment cleanup status in metadata",
			zap.String("logName", logName),
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segmentId),
			zap.Error(err))
		return werr.ErrLogHandleTruncateFailed.WithCauseErr(err)
	}

	logger.Ctx(ctx).Info("Successfully created segment cleanup status in metadata",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId))

	// Send cleanup requests to all nodes in quorum and wait for completion
	return s.sendCleanupRequestsToQuorumNodes(ctx, logName, logId, segmentId, quorum)
}

// sendCleanupRequestsToQuorumNodes sends cleanup requests to all nodes in quorum and waits for completion
func (s *segmentCleanupManagerImpl) sendCleanupRequestsToQuorumNodes(ctx context.Context, logName string, logId int64, segmentId int64, quorum *proto.QuorumInfo) error {
	logger.Ctx(ctx).Info("Sending cleanup requests to quorum nodes", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Int64("quorumId", quorum.Id), zap.Strings("nodes", quorum.Nodes))

	// Concurrently send cleanup requests to all nodes, collect results
	results := s.sendCleanupToNodes(ctx, logName, logId, segmentId, quorum.Nodes)

	logger.Ctx(ctx).Info("All cleanup requests completed",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.Int("totalNodes", len(results)))

	// Single metadata update with all results
	finalStatus, err := s.updateCleanupStatusWithResults(ctx, logId, segmentId, results)
	if err != nil {
		return err
	}

	// Return result based on final status
	if finalStatus.State == proto.SegmentCleanupState_CLEANUP_FAILED {
		return fmt.Errorf("segment cleanup failed: %s", finalStatus.ErrorMessage)
	}

	logger.Ctx(ctx).Info("Segment cleanup completed successfully", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId))
	return nil
}

// continueCleanupForRemainingNodes continues cleanup operations for nodes that haven't completed yet
func (s *segmentCleanupManagerImpl) continueCleanupForRemainingNodes(ctx context.Context, logName string, logId int64, segmentId int64, quorum *proto.QuorumInfo, status *proto.SegmentCleanupStatus) error {
	logger.Ctx(ctx).Info("Continuing cleanup for remaining nodes", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Int64("quorumId", quorum.Id))

	// Filter out nodes that have already completed cleanup
	var nodesToCleanup []string
	nodesAlreadyCompleted := 0

	for _, nodeAddress := range quorum.Nodes {
		if completed, exists := status.QuorumCleanupStatus[nodeAddress]; exists && completed {
			logger.Ctx(ctx).Info("Skipping node that already completed cleanup", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.String("node", nodeAddress))
			nodesAlreadyCompleted++
			continue
		}
		nodesToCleanup = append(nodesToCleanup, nodeAddress)
	}

	logger.Ctx(ctx).Info("Cleanup continuation summary",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.Int("totalNodes", len(quorum.Nodes)),
		zap.Int("nodesAlreadyCompleted", nodesAlreadyCompleted),
		zap.Int("nodesToProcess", len(nodesToCleanup)))

	// Concurrently send cleanup requests to remaining nodes, collect results
	results := s.sendCleanupToNodes(ctx, logName, logId, segmentId, nodesToCleanup)

	logger.Ctx(ctx).Info("All remaining node cleanup requests completed",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.Int("processedNodes", len(results)))

	// Single metadata update with all results
	finalStatus, err := s.updateCleanupStatusWithResults(ctx, logId, segmentId, results)
	if err != nil {
		return err
	}

	// Return result based on final status
	if finalStatus.State == proto.SegmentCleanupState_CLEANUP_FAILED {
		return fmt.Errorf("segment cleanup failed: %s", finalStatus.ErrorMessage)
	}

	logger.Ctx(ctx).Info("Segment cleanup completed successfully", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId))
	return nil
}

// sendCleanupToNodes concurrently sends cleanup requests to the given nodes and collects results
func (s *segmentCleanupManagerImpl) sendCleanupToNodes(ctx context.Context, logName string, logId int64, segmentId int64, nodes []string) []nodeCleanupResult {
	var wg sync.WaitGroup
	var resultsMu sync.Mutex
	results := make([]nodeCleanupResult, 0, len(nodes))

	for _, nodeAddress := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			err := s.sendCleanupRequestToNode(ctx, logName, logId, segmentId, addr)
			result := nodeCleanupResult{nodeAddress: addr, success: err == nil}
			if err != nil {
				result.errorMsg = err.Error()
			}
			resultsMu.Lock()
			results = append(results, result)
			resultsMu.Unlock()
		}(nodeAddress)
	}

	wg.Wait()
	return results
}

// sendCleanupRequestToNode sends a cleanup request to a single node
// It only performs the RPC call and returns the result; metadata updates are handled by the caller.
func (s *segmentCleanupManagerImpl) sendCleanupRequestToNode(
	ctx context.Context,
	logName string,
	logId int64,
	segmentId int64,
	nodeAddress string,
) error {
	logger.Ctx(ctx).Info("Starting cleanup request to node",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.String("node", nodeAddress))

	// Get node client
	logStoreCli, err := s.clientPool.GetLogStoreClient(ctx, nodeAddress)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get logstore client", zap.String("node", nodeAddress), zap.Error(err))
		return err
	}

	// Use the dedicated SegmentClean interface to clean up the truncated segment
	// The flag parameter specifies the type of cleanup operation, using 0 here to clean truncated segments
	err = logStoreCli.SegmentClean(ctx, s.bucketName, s.rootPath, logId, segmentId, 0)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to cleanup segment on node", zap.String("node", nodeAddress), zap.Error(err))
		return err
	}

	logger.Ctx(ctx).Info("Successfully completed segment cleanup on node",
		zap.String("logName", logName),
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.String("node", nodeAddress))

	return nil
}

// updateCleanupStatusWithResults reads the current cleanup status from metadata,
// applies all node results, determines the final state, and writes back in a single update.
// This avoids concurrent metadata updates from individual goroutines.
func (s *segmentCleanupManagerImpl) updateCleanupStatusWithResults(
	ctx context.Context,
	logId int64,
	segmentId int64,
	results []nodeCleanupResult,
) (*proto.SegmentCleanupStatus, error) {
	// 1. Read current cleanup status
	status, err := s.metadata.GetSegmentCleanupStatus(ctx, logId, segmentId)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to get segment cleanup status for update",
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segmentId),
			zap.Error(err))
		return nil, err
	}

	if status == nil {
		logger.Ctx(ctx).Warn("Segment cleanup status is nil during update", zap.Int64("logId", logId), zap.Int64("segmentId", segmentId))
		return nil, fmt.Errorf("segment cleanup status is nil for logId: %d, segmentId: %d", logId, segmentId)
	}

	if status.QuorumCleanupStatus == nil {
		status.QuorumCleanupStatus = make(map[string]bool)
	}

	// 2. Apply all node results
	for _, r := range results {
		status.QuorumCleanupStatus[r.nodeAddress] = r.success
		if !r.success && (status.ErrorMessage == "" || len(status.ErrorMessage) < 100) {
			nodeErrorMsg := fmt.Sprintf("Node %s: %s", r.nodeAddress, r.errorMsg)
			if status.ErrorMessage == "" {
				status.ErrorMessage = nodeErrorMsg
			} else {
				status.ErrorMessage += "; " + nodeErrorMsg
			}
		}
	}

	// 3. Determine final state based on all node statuses
	allSuccess := true
	totalNodes := len(status.QuorumCleanupStatus)
	successCount := 0

	for _, success := range status.QuorumCleanupStatus {
		if success {
			successCount++
		} else {
			allSuccess = false
		}
	}

	if allSuccess && successCount == totalNodes && totalNodes > 0 {
		status.State = proto.SegmentCleanupState_CLEANUP_COMPLETED
	} else {
		status.State = proto.SegmentCleanupState_CLEANUP_FAILED
		if status.ErrorMessage == "" {
			status.ErrorMessage = fmt.Sprintf("Not all quorum nodes succeeded in cleanup: %d out of %d succeeded", successCount, totalNodes)
		}
	}

	// 4. Update timestamp
	status.LastUpdateTime = uint64(time.Now().UnixMilli())

	logger.Ctx(ctx).Info("Cleanup status determined from node results",
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.String("state", status.State.String()),
		zap.Int("successCount", successCount),
		zap.Int("totalNodes", totalNodes),
		zap.Bool("allSucceeded", allSuccess))

	// 5. Write back once
	err = s.metadata.UpdateSegmentCleanupStatus(ctx, status)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to update segment cleanup status",
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segmentId),
			zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Info("Segment cleanup status updated successfully",
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.String("state", status.State.String()))

	return status, nil
}

// CleanupOrphanedStatuses cleans up orphaned cleanup status records for a log.
// It lists all cleanup statuses for the given log and deletes any whose segmentId
// is less than minSegmentId. These records are orphaned because their segment metadata
// has already been deleted (segments are cleaned in ascending order), but the cleanup
// status deletion was interrupted (e.g., by a crash).
func (s *segmentCleanupManagerImpl) CleanupOrphanedStatuses(ctx context.Context, logId int64, minSegmentId int64) error {
	// List all cleanup statuses for this log
	allStatuses, err := s.metadata.ListSegmentCleanupStatus(ctx, logId)
	if err != nil {
		logger.Ctx(ctx).Warn("Failed to list cleanup statuses for orphan check",
			zap.Int64("logId", logId),
			zap.Error(err))
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

		// This cleanup status is for a segment older than the oldest pending segment,
		// meaning its segment metadata has already been deleted — this is an orphan.
		orphanCount++
		logger.Ctx(ctx).Info("Found orphaned cleanup status, deleting",
			zap.Int64("logId", logId),
			zap.Int64("segmentId", status.SegmentId),
			zap.String("state", status.State.String()),
			zap.Int64("minSegmentId", minSegmentId))

		if deleteErr := s.metadata.DeleteSegmentCleanupStatus(ctx, logId, status.SegmentId); deleteErr != nil {
			logger.Ctx(ctx).Warn("Failed to delete orphaned cleanup status",
				zap.Int64("logId", logId),
				zap.Int64("segmentId", status.SegmentId),
				zap.Error(deleteErr))
		} else {
			cleanedCount++
		}
	}

	if orphanCount > 0 {
		logger.Ctx(ctx).Info("Orphaned cleanup status check completed",
			zap.Int64("logId", logId),
			zap.Int64("minSegmentId", minSegmentId),
			zap.Int("orphansFound", orphanCount),
			zap.Int("orphansCleaned", cleanedCount))
	}

	return nil
}

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
}

type segmentCleanupManagerImpl struct {
	metadata           meta.MetadataProvider
	clientPool         client.LogStoreClientPool
	cleanupMutex       sync.Mutex
	inProgressCleanups map[string]bool
}

// NewSegmentCleanupManager creates a new segment cleanup manager
func NewSegmentCleanupManager(metadata meta.MetadataProvider, clientPool client.LogStoreClientPool) SegmentCleanupManager {
	return &segmentCleanupManagerImpl{
		metadata:           metadata,
		clientPool:         clientPool,
		inProgressCleanups: make(map[string]bool),
	}
}

// CleanupSegment sends cleanup requests to all nodes in the quorum
func (s *segmentCleanupManagerImpl) CleanupSegment(ctx context.Context, logName string, logId int64, segmentId int64) error {
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

	// Ensure cleanup in-progress flag is removed after completion
	defer func() {
		s.cleanupMutex.Lock()
		delete(s.inProgressCleanups, cleanupKey)
		s.cleanupMutex.Unlock()
	}()

	// 1. Check if segment already has cleanup status
	existingStatus, err := s.metadata.GetSegmentCleanupStatus(ctx, logId, segmentId)
	if err != nil {
		logger.Ctx(ctx).Error("Failed to get segment cleanup status", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Error(err))
		return err
	}

	// TODO  Currently only support embed standalone mode
	// 2. Get segment info and find corresponding quorum info
	//segMeta, err := s.metadata.GetSegmentMetadata(ctx, logName, segmentId)
	//quorum, err := s.metadata.GetQuorumInfo(ctx, segMeta.QuorumId)
	quorum := &proto.QuorumInfo{
		Id: 0,
		Es: 1,
		Wq: 1,
		Aq: 1,
		Nodes: []string{
			"127.0.0.1",
		},
	}

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
		deleteSegMetaErr := s.metadata.DeleteSegmentMetadata(ctx, logName, segmentId)
		if deleteSegMetaErr != nil && !werr.ErrSegmentNotFound.Is(deleteSegMetaErr) {
			logger.Ctx(ctx).Warn("failed to clean truncated segment metadata", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Error(deleteSegMetaErr))
		}
		deleteCleanSegStatusErr := s.metadata.DeleteSegmentCleanupStatus(ctx, logId, segmentId)
		if deleteCleanSegStatusErr != nil {
			logger.Ctx(ctx).Warn("failed to clean truncated segment clean status", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Error(deleteCleanSegStatusErr))
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

	// Ensure status includes all required nodes (in case quorum node configuration has changed)
	for _, nodeAddress := range quorum.Nodes {
		if _, exists := status.QuorumCleanupStatus[nodeAddress]; !exists {
			status.QuorumCleanupStatus[nodeAddress] = false
		}
	}

	// Update status
	err := s.metadata.UpdateSegmentCleanupStatus(ctx, status)
	if err != nil {
		logger.Ctx(ctx).Error("Failed to update segment cleanup status", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Error(err))
		return err
	}

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
	logger.Ctx(ctx).Debug("No existing cleanup status found, start creating new cleanup task", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId))

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

	// Store initial cleanup status
	err := s.metadata.CreateSegmentCleanupStatus(ctx, cleanupStatus)
	if err != nil {
		return werr.ErrTruncateLog.WithCauseErr(err)
	}

	// Send cleanup requests to all nodes in quorum and wait for completion
	return s.sendCleanupRequestsToQuorumNodes(ctx, logName, logId, segmentId, quorum)
}

// sendCleanupRequestsToQuorumNodes sends cleanup requests to all nodes in quorum and waits for completion
func (s *segmentCleanupManagerImpl) sendCleanupRequestsToQuorumNodes(ctx context.Context, logName string, logId int64, segmentId int64, quorum *proto.QuorumInfo) error {
	logger.Ctx(ctx).Info("Sending cleanup requests to quorum nodes", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Int64("quorumId", quorum.Id), zap.Strings("nodes", quorum.Nodes))

	// Use sync mutex to protect status updates
	var statusUpdateMutex sync.Mutex

	var wg sync.WaitGroup
	var errorsMutex sync.Mutex
	errors := make([]error, 0)

	// Concurrently send cleanup requests to all nodes in the quorum
	for _, nodeAddress := range quorum.Nodes {
		wg.Add(1)
		go func(nodeAddress string) {
			defer wg.Done()
			// Use the passed mutex to protect status updates
			err := s.sendCleanupRequestToNode(ctx, logName, logId, segmentId, quorum.Id, nodeAddress, &statusUpdateMutex)
			if err != nil {
				errorsMutex.Lock()
				errors = append(errors, err)
				errorsMutex.Unlock()
			}
		}(nodeAddress)
	}

	// Wait for all cleanup requests to complete
	wg.Wait()

	// Check cleanup status and update final state
	finalStatus, err := s.updateFinalCleanupStatus(ctx, logId, segmentId)
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

	// Use sync mutex to protect status updates
	var statusUpdateMutex sync.Mutex

	var wg sync.WaitGroup
	var errorsMutex sync.Mutex
	errors := make([]error, 0)

	// Only send cleanup requests to nodes that haven't completed yet
	for _, nodeAddress := range quorum.Nodes {
		// Check if node has already completed cleanup
		if completed, exists := status.QuorumCleanupStatus[nodeAddress]; exists && completed {
			logger.Ctx(ctx).Debug("Skipping node that already completed cleanup", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.String("node", nodeAddress))
			continue
		}

		wg.Add(1)
		go func(nodeAddress string) {
			defer wg.Done()
			// Pass mutex to protect status updates
			err := s.sendCleanupRequestToNode(ctx, logName, logId, segmentId, quorum.Id, nodeAddress, &statusUpdateMutex)
			if err != nil {
				errorsMutex.Lock()
				errors = append(errors, err)
				errorsMutex.Unlock()
			}
		}(nodeAddress)
	}

	// Wait for all cleanup requests to complete
	wg.Wait()

	// Check cleanup status and update final state
	finalStatus, err := s.updateFinalCleanupStatus(ctx, logId, segmentId)
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

// sendCleanupRequestToNode sends a cleanup request to a single node
func (s *segmentCleanupManagerImpl) sendCleanupRequestToNode(
	ctx context.Context,
	logName string,
	logId int64,
	segmentId int64,
	quorumId int64,
	nodeAddress string,
	statusUpdateMutex *sync.Mutex,
) error {
	logger.Ctx(ctx).Debug("Sending cleanup request to node", zap.String("logName", logName), zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.String("node", nodeAddress))

	// Get node client
	logStoreCli, err := s.clientPool.GetLogStoreClient(nodeAddress)
	if err != nil {
		logger.Ctx(ctx).Error("Failed to get logstore client", zap.String("node", nodeAddress), zap.Error(err))
		// Use mutex to protect status updates
		statusUpdateMutex.Lock()
		defer statusUpdateMutex.Unlock()
		s.processCleanupResult(ctx, logId, segmentId, quorumId, nodeAddress, false, err.Error())
		return err
	}

	// Use the dedicated SegmentClean interface to clean up the truncated segment
	// The flag parameter specifies the type of cleanup operation, using 0 here to clean truncated segments
	// Different flag values can be defined for different cleanup modes based on requirements
	err = logStoreCli.SegmentClean(ctx, logId, segmentId, 0)
	if err != nil {
		logger.Ctx(ctx).Error("Failed to cleanup segment", zap.String("node", nodeAddress), zap.Error(err))
		// Use mutex to protect status updates
		statusUpdateMutex.Lock()
		defer statusUpdateMutex.Unlock()
		s.processCleanupResult(ctx, logId, segmentId, quorumId, nodeAddress, false, err.Error())
		return err
	}

	// Process successful result, using mutex to protect status updates
	statusUpdateMutex.Lock()
	defer statusUpdateMutex.Unlock()
	s.processCleanupResult(ctx, logId, segmentId, quorumId, nodeAddress, true, "")
	return nil
}

// processCleanupResult processes the cleanup result from a single node
// Note: Before calling this method, the caller should have acquired the status update mutex
func (s *segmentCleanupManagerImpl) processCleanupResult(ctx context.Context, logId int64, segmentId int64, quorumId int64, nodeAddress string, success bool, errorMsg string) error {
	// 1. Get current cleanup status
	status, err := s.metadata.GetSegmentCleanupStatus(ctx, logId, segmentId)
	if err != nil {
		logger.Ctx(ctx).Error("Failed to get segment cleanup status", zap.Int64("logId", logId), zap.Int64("segmentId", segmentId), zap.Error(err))
		return err
	}

	// Check if status object is nil
	if status == nil {
		logger.Ctx(ctx).Error("Segment cleanup status is nil", zap.Int64("logId", logId), zap.Int64("segmentId", segmentId))
		return fmt.Errorf("segment cleanup status is nil for logId: %d, segmentId: %d", logId, segmentId)
	}

	// Ensure QuorumCleanupStatus map is initialized
	if status.QuorumCleanupStatus == nil {
		status.QuorumCleanupStatus = make(map[string]bool)
	}

	// 2. Update node cleanup status
	status.QuorumCleanupStatus[nodeAddress] = success
	status.LastUpdateTime = uint64(time.Now().UnixMilli())

	// 3. If cleanup failed, record error message
	if !success && (status.ErrorMessage == "" || len(status.ErrorMessage) < 100) {
		nodeErrorMsg := fmt.Sprintf("Node %s: %s", nodeAddress, errorMsg)
		if status.ErrorMessage == "" {
			status.ErrorMessage = nodeErrorMsg
		} else {
			status.ErrorMessage += "; " + nodeErrorMsg
		}
	}

	// 4. Check if all nodes have completed cleanup
	allNodesResponded := true
	allSuccess := true
	for _, nodeSuccess := range status.QuorumCleanupStatus {
		if !nodeSuccess {
			allSuccess = false
		}
	}

	// 5. Update status
	// Only set state to CLEANUP_COMPLETED when all nodes have successfully completed cleanup
	if allNodesResponded {
		if allSuccess {
			// All nodes succeeded, cleanup successful
			status.State = proto.SegmentCleanupState_CLEANUP_COMPLETED
			logger.Ctx(ctx).Info("All quorum nodes successfully cleaned up segment",
				zap.Int64("logId", logId),
				zap.Int64("segmentId", segmentId),
				zap.Int("nodeCount", len(status.QuorumCleanupStatus)))
		} else {
			// Some nodes failed, cleanup failed
			status.State = proto.SegmentCleanupState_CLEANUP_FAILED
			logger.Ctx(ctx).Warn("Some quorum nodes failed to clean up segment",
				zap.Int64("logId", logId),
				zap.Int64("segmentId", segmentId),
				zap.Any("nodeStatuses", status.QuorumCleanupStatus))
		}
	}

	// 6. Store updated cleanup status
	return s.metadata.UpdateSegmentCleanupStatus(ctx, status)
}

// updateFinalCleanupStatus updates the final cleanup status and returns the final state
func (s *segmentCleanupManagerImpl) updateFinalCleanupStatus(ctx context.Context, logId int64, segmentId int64) (*proto.SegmentCleanupStatus, error) {
	// 1. Get current cleanup status
	status, err := s.metadata.GetSegmentCleanupStatus(ctx, logId, segmentId)
	if err != nil {
		logger.Ctx(ctx).Error("Failed to get segment cleanup status for final update",
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segmentId),
			zap.Error(err))
		return nil, err
	}

	// Check if status object is nil
	if status == nil {
		logger.Ctx(ctx).Error("Segment cleanup status is nil during final update", zap.Int64("logId", logId), zap.Int64("segmentId", segmentId))
		return nil, fmt.Errorf("segment cleanup status is nil for logId: %d, segmentId: %d during final update", logId, segmentId)
	}

	// Ensure QuorumCleanupStatus map is initialized
	if status.QuorumCleanupStatus == nil {
		status.QuorumCleanupStatus = make(map[string]bool)
	}

	// 2. If status is already completed or failed, no need to update
	if status.State == proto.SegmentCleanupState_CLEANUP_COMPLETED ||
		status.State == proto.SegmentCleanupState_CLEANUP_FAILED {
		return status, nil
	}

	// 3. Check status of all nodes
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

	// 4. Determine final status based on node states
	if allSuccess && successCount == totalNodes && totalNodes > 0 {
		// All nodes successfully cleaned up
		status.State = proto.SegmentCleanupState_CLEANUP_COMPLETED
	} else {
		// Some nodes failed or no nodes responded
		status.State = proto.SegmentCleanupState_CLEANUP_FAILED
		if status.ErrorMessage == "" {
			status.ErrorMessage = fmt.Sprintf("Not all quorum nodes succeeded in cleanup: %d out of %d succeeded", successCount, totalNodes)
		}
	}

	// 5. Update timestamp
	status.LastUpdateTime = uint64(time.Now().UnixMilli())

	// 6. Store updated cleanup status
	err = s.metadata.UpdateSegmentCleanupStatus(ctx, status)
	if err != nil {
		logger.Ctx(ctx).Error("Failed to update final segment cleanup status",
			zap.Int64("logId", logId),
			zap.Int64("segmentId", segmentId),
			zap.Error(err))
		return nil, err
	}

	logger.Ctx(ctx).Info("Segment cleanup final status updated",
		zap.Int64("logId", logId),
		zap.Int64("segmentId", segmentId),
		zap.String("state", status.State.String()),
		zap.Int("successCount", successCount),
		zap.Int("totalNodes", totalNodes),
		zap.Bool("allSucceeded", allSuccess))

	return status, nil
}

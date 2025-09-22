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
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
)

type RollingPolicy interface {
	// ShouldRollover returns true if the current segment should be rolled over.
	ShouldRollover(ctx context.Context, currentSegmentSize int64, currentBlocksCount int64, lastRolloverTimeMs int64) bool
}

func NewDefaultRollingPolicy(rolloverIntervalMs int64, rolloverSizeBytes int64, rolloverBlocksCount int64) RollingPolicy {
	// Validate parameters
	if rolloverIntervalMs <= 0 {
		logger.Ctx(context.Background()).Warn("Invalid rolloverIntervalMs, using default 10 minutes",
			zap.Int64("provided", rolloverIntervalMs))
		rolloverIntervalMs = 10 * 60 * 1000 // 10 minutes default
	}
	if rolloverSizeBytes <= 0 {
		logger.Ctx(context.Background()).Warn("Invalid rolloverSizeBytes, using default 64MB",
			zap.Int64("provided", rolloverSizeBytes))
		rolloverSizeBytes = 64 * 1024 * 1024 // 64MB default
	}
	if rolloverBlocksCount <= 0 {
		logger.Ctx(context.Background()).Warn("Invalid rolloverBlocksCount, using default 1000 blocks",
			zap.Int64("provided", rolloverBlocksCount))
		rolloverBlocksCount = 1000 // 1000 blocks default
	}

	return &DefaultRollingPolicy{
		rolloverIntervalMs:  rolloverIntervalMs,
		rolloverSizeBytes:   rolloverSizeBytes,
		rolloverBlocksCount: rolloverBlocksCount,
	}
}

var _ RollingPolicy = &DefaultRollingPolicy{}

type DefaultRollingPolicy struct {
	rolloverIntervalMs  int64
	rolloverSizeBytes   int64
	rolloverBlocksCount int64
}

func (p *DefaultRollingPolicy) ShouldRollover(ctx context.Context, currentSegmentSize int64, currentBlocksCount int64, lastRolloverTimeMs int64) bool {
	// Validate input parameters
	if currentSegmentSize < 0 {
		logger.Ctx(ctx).Info("Invalid currentSegmentSize", zap.Int64("currentSegmentSize", currentSegmentSize))
		return false
	}
	if currentBlocksCount < 0 {
		logger.Ctx(ctx).Debug("Invalid currentBlocksCount", zap.Int64("currentBlocksCount", currentBlocksCount))
		return false
	}

	// Get current time once for consistency
	currentTimeMs := time.Now().UnixMilli()

	// Check size-based rollover
	if currentSegmentSize >= p.rolloverSizeBytes {
		logger.Ctx(ctx).Debug("Rolling by size",
			zap.Int64("rolloverSizeBytes", p.rolloverSizeBytes),
			zap.Int64("actualSize", currentSegmentSize))
		return true
	}

	// Check blocks-based rollover
	if currentBlocksCount >= p.rolloverBlocksCount {
		logger.Ctx(ctx).Debug("Rolling by blocks count",
			zap.Int64("rolloverBlocksCount", p.rolloverBlocksCount),
			zap.Int64("actualBlocksCount", currentBlocksCount))
		return true
	}

	// Check time-based rollover
	// Only consider time-based rollover if segment is not empty and lastRolloverTimeMs is valid
	if currentSegmentSize > 0 && lastRolloverTimeMs > 0 {
		timeSinceLastRollover := currentTimeMs - lastRolloverTimeMs

		// Handle potential clock skew or time going backwards
		if timeSinceLastRollover < 0 {
			logger.Ctx(ctx).Warn("Clock skew detected, time went backwards",
				zap.Int64("currentTimeMs", currentTimeMs),
				zap.Int64("lastRolloverTimeMs", lastRolloverTimeMs),
				zap.Int64("timeDiff", timeSinceLastRollover))
			// Don't rollover on clock skew, wait for time to stabilize
			return false
		}

		if timeSinceLastRollover >= p.rolloverIntervalMs {
			logger.Ctx(ctx).Debug("Rolling by time interval",
				zap.Int64("rolloverIntervalMs", p.rolloverIntervalMs),
				zap.Int64("actualIntervalMs", timeSinceLastRollover),
				zap.Int64("actualSize", currentSegmentSize),
				zap.Int64("actualBlocksCount", currentBlocksCount))
			return true
		}
	}

	// Otherwise, do not roll over
	return false
}

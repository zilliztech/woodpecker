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
	ShouldRollover(currentSegmentSize int64, lastRolloverTimeMs int64) bool
}

func NewDefaultRollingPolicy(rolloverIntervalMs int64, rolloverSizeBytes int64) RollingPolicy {
	return &DefaultRollingPolicy{
		rolloverIntervalMs: rolloverIntervalMs,
		rolloverSizeBytes:  rolloverSizeBytes,
	}
}

var _ RollingPolicy = &DefaultRollingPolicy{}

type DefaultRollingPolicy struct {
	rolloverIntervalMs int64
	rolloverSizeBytes  int64
}

func (p *DefaultRollingPolicy) ShouldRollover(currentSegmentSize int64, lastRolloverTimeMs int64) bool {
	// If the current segment is already larger than the rollover size, or if the last rollover time is more than the rollover interval, roll over.
	if currentSegmentSize >= p.rolloverSizeBytes {
		logger.Ctx(context.TODO()).Debug("Rolling by size",
			zap.Int64("rolloverSizeBytes", p.rolloverSizeBytes),
			zap.Int64("actualSize", currentSegmentSize))
		return true
	}
	// If the current segment is not empty, and the last rollover time is more than the rollover interval, roll over.
	if currentSegmentSize > 0 && (time.Now().UnixMilli()-lastRolloverTimeMs) >= p.rolloverIntervalMs {
		logger.Ctx(context.TODO()).Debug("Rolling by time interval",
			zap.Int64("rolloverIntervalMs", p.rolloverIntervalMs),
			zap.Int64("actualIntervalMs", time.Now().UnixMilli()-lastRolloverTimeMs),
			zap.Int64("actualSize", currentSegmentSize))
		return true
	}
	// Otherwise, do not roll over.
	return false
}

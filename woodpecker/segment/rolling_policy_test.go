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
	"testing"
	"time"
)

func TestDefaultRollingPolicy_ShouldRollover(t *testing.T) {
	policy := NewDefaultRollingPolicy(1000, 100)

	// Test case 1: When the current segment size exceeds the rollover size, ShouldRollover should return true
	lastRolloverTimeMs := time.Now().UnixMilli() - 500
	currentSegmentSize := int64(150)
	if !policy.ShouldRollover(currentSegmentSize, lastRolloverTimeMs) {
		t.Errorf("Expected ShouldRollover to return true, got false")
	}

	// Test case 2: When the current segment size does not exceed the rollover size, but the last rollover time exceeds the rollover interval, ShouldRollover should return true
	lastRolloverTimeMs = time.Now().UnixMilli() - 1500
	currentSegmentSize = int64(50)
	if !policy.ShouldRollover(currentSegmentSize, lastRolloverTimeMs) {
		t.Errorf("Expected ShouldRollover to return true, got false")
	}

	// Test case 3: When the current segment size does not exceed the rollover size, and the last rollover time does not exceed the rollover interval, ShouldRollover should return false
	currentSegmentSize = int64(50)
	lastRolloverTimeMs = time.Now().UnixMilli() - 500
	if policy.ShouldRollover(currentSegmentSize, lastRolloverTimeMs) {
		t.Errorf("Expected ShouldRollover to return false, got true")
	}

	// Test case 4: When the current segment size is 0, and the last rollover time exceeds the rollover interval, ShouldRollover should return false
	currentSegmentSize = int64(0)
	lastRolloverTimeMs = time.Now().UnixMilli() - 1500
	if policy.ShouldRollover(currentSegmentSize, lastRolloverTimeMs) {
		t.Errorf("Expected ShouldRollover to return false, got true")
	}
}

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

package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// === SessionLock Tests ===

func TestSessionLock_IsValid(t *testing.T) {
	sl := &SessionLock{}
	// Default should be false (zero value)
	assert.False(t, sl.IsValid())

	sl.SetValid()
	assert.True(t, sl.IsValid())

	sl.MarkInvalid()
	assert.False(t, sl.IsValid())
}

func TestSessionLock_SetValid(t *testing.T) {
	sl := &SessionLock{}
	sl.SetValid()
	assert.True(t, sl.IsValid())
}

func TestSessionLock_MarkInvalid(t *testing.T) {
	sl := &SessionLock{}
	sl.SetValid()
	assert.True(t, sl.IsValid())

	sl.MarkInvalid()
	assert.False(t, sl.IsValid())
}

func TestSessionLock_GetSession_Nil(t *testing.T) {
	sl := &SessionLock{}
	assert.Nil(t, sl.GetSession())
}

func TestNewSessionLockForTest(t *testing.T) {
	sl := NewSessionLockForTest(nil)
	assert.NotNil(t, sl)
	assert.True(t, sl.IsValid())
	assert.Nil(t, sl.GetSession())
}

// === extractLogName Tests ===

func TestExtractLogName(t *testing.T) {
	t.Run("valid_path", func(t *testing.T) {
		name, err := extractLogName("woodpecker/logs/my-log")
		assert.NoError(t, err)
		assert.Equal(t, "my-log", name)
	})

	t.Run("valid_path_with_segments", func(t *testing.T) {
		name, err := extractLogName("woodpecker/logs/my-log/segments/0")
		assert.NoError(t, err)
		assert.Equal(t, "my-log", name)
	})

	t.Run("too_short_path", func(t *testing.T) {
		_, err := extractLogName("a/b")
		assert.Error(t, err)
	})

	t.Run("single_element", func(t *testing.T) {
		_, err := extractLogName("onlyone")
		assert.Error(t, err)
	})

	t.Run("empty_string", func(t *testing.T) {
		_, err := extractLogName("")
		assert.Error(t, err)
	})
}

// === atoi Tests ===

func TestAtoi(t *testing.T) {
	t.Run("valid_int", func(t *testing.T) {
		v, err := atoi("42")
		assert.NoError(t, err)
		assert.Equal(t, 42, v)
	})

	t.Run("zero", func(t *testing.T) {
		v, err := atoi("0")
		assert.NoError(t, err)
		assert.Equal(t, 0, v)
	})

	t.Run("negative", func(t *testing.T) {
		v, err := atoi("-5")
		assert.NoError(t, err)
		assert.Equal(t, -5, v)
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := atoi("abc")
		assert.Error(t, err)
	})

	t.Run("empty", func(t *testing.T) {
		_, err := atoi("")
		assert.Error(t, err)
	})

	t.Run("float", func(t *testing.T) {
		_, err := atoi("3.14")
		assert.Error(t, err)
	})
}

// === Build Key Tests ===

func TestBuildLogKey(t *testing.T) {
	assert.Equal(t, "woodpecker/logs/mylog", BuildLogKey("mylog"))
	assert.Equal(t, "woodpecker/logs/", BuildLogKey(""))
}

func TestBuildLogLockKey(t *testing.T) {
	assert.Equal(t, "woodpecker/logs/mylog/lock", BuildLogLockKey("mylog"))
}

func TestBuildSegmentInstanceKey(t *testing.T) {
	assert.Equal(t, "woodpecker/logs/mylog/segments/0", BuildSegmentInstanceKey("mylog", "0"))
	assert.Equal(t, "woodpecker/logs/mylog/segments/123", BuildSegmentInstanceKey("mylog", "123"))
}

func TestBuildQuorumInfoKey(t *testing.T) {
	assert.Equal(t, "woodpecker/quorums/q1", BuildQuorumInfoKey("q1"))
}

func TestBuildNodeKey(t *testing.T) {
	assert.Equal(t, "woodpecker/logstores/node1", BuildNodeKey("node1"))
}

func TestBuildLogReaderTempInfoKey(t *testing.T) {
	assert.Equal(t, "woodpecker/readers/1/reader-1", BuildLogReaderTempInfoKey(1, "reader-1"))
}

func TestBuildLogAllReaderTempInfosKey(t *testing.T) {
	assert.Equal(t, "woodpecker/readers/1/", BuildLogAllReaderTempInfosKey(1))
}

func TestBuildAllSegmentsCleanupStatusKey(t *testing.T) {
	assert.Equal(t, "woodpecker/cleaning/1", BuildAllSegmentsCleanupStatusKey(1))
}

func TestBuildSegmentCleanupStatusKey(t *testing.T) {
	assert.Equal(t, "woodpecker/cleaning/1/2", BuildSegmentCleanupStatusKey(1, 2))
}

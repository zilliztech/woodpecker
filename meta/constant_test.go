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
		name, err := extractLogNameWithKeyBuilder(NewKeyBuilder(LegacyServicePrefix), "woodpecker/logs/my-log")
		assert.NoError(t, err)
		assert.Equal(t, "my-log", name)
	})

	t.Run("valid_path_with_segments", func(t *testing.T) {
		name, err := extractLogNameWithKeyBuilder(NewKeyBuilder(LegacyServicePrefix), "woodpecker/logs/my-log/segments/0")
		assert.NoError(t, err)
		assert.Equal(t, "my-log", name)
	})

	t.Run("too_short_path", func(t *testing.T) {
		_, err := extractLogNameWithKeyBuilder(NewKeyBuilder(LegacyServicePrefix), "a/b")
		assert.Error(t, err)
	})

	t.Run("single_element", func(t *testing.T) {
		_, err := extractLogNameWithKeyBuilder(NewKeyBuilder(LegacyServicePrefix), "onlyone")
		assert.Error(t, err)
	})

	t.Run("empty_string", func(t *testing.T) {
		_, err := extractLogNameWithKeyBuilder(NewKeyBuilder(LegacyServicePrefix), "")
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

// === KeyBuilder Tests ===

func TestKeyBuilderWithLegacyPrefix(t *testing.T) {
	builder := NewKeyBuilder(LegacyServicePrefix)

	assert.Equal(t, "woodpecker", builder.Prefix())
	assert.Equal(t, "woodpecker/instance", builder.ServiceInstanceKey())
	assert.Equal(t, "woodpecker/version", builder.VersionKey())
	assert.Equal(t, "woodpecker/logidgen", builder.LogIdGeneratorKey())
	assert.Equal(t, "woodpecker/quorumidgen", builder.QuorumIdGeneratorKey())
	assert.Equal(t, "woodpecker/conditionwrite", builder.ConditionWriteKey())
	assert.Equal(t, "woodpecker/logs/mylog", builder.BuildLogKey("mylog"))
	assert.Equal(t, "woodpecker/logs/", builder.BuildLogKey(""))
	assert.Equal(t, "woodpecker/logs/mylog/lock", builder.BuildLogLockKey("mylog"))
	assert.Equal(t, "woodpecker/logs/mylog/segments/0", builder.BuildSegmentInstanceKey("mylog", "0"))
	assert.Equal(t, "woodpecker/logs/mylog/segments/123", builder.BuildSegmentInstanceKey("mylog", "123"))
	assert.Equal(t, "woodpecker/quorums/q1", builder.BuildQuorumInfoKey("q1"))
	assert.Equal(t, "woodpecker/logstores/node1", builder.BuildNodeKey("node1"))
	assert.Equal(t, "woodpecker/readers/1/reader-1", builder.BuildLogReaderTempInfoKey(1, "reader-1"))
	assert.Equal(t, "woodpecker/readers/1/", builder.BuildLogAllReaderTempInfosKey(1))
	assert.Equal(t, "woodpecker/cleaning/1", builder.BuildAllSegmentsCleanupStatusKey(1))
	assert.Equal(t, "woodpecker/cleaning/1/2", builder.BuildSegmentCleanupStatusKey(1, 2))
}

func TestKeyBuilderWithConfiguredPrefix(t *testing.T) {
	builder := NewKeyBuilder("lakebase/wp")

	assert.Equal(t, "lakebase/wp", builder.Prefix())
	assert.Equal(t, "lakebase/wp/instance", builder.ServiceInstanceKey())
	assert.Equal(t, "lakebase/wp/logs/mylog", builder.BuildLogKey("mylog"))
	assert.Equal(t, "lakebase/wp/cleaning/1/2", builder.BuildSegmentCleanupStatusKey(1, 2))
}

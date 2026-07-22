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

import "fmt"

const (
	// LegacyServicePrefix is the historical hardcoded prefix for metadata keys.
	LegacyServicePrefix = "woodpecker"
	// Major.Minor.Patch is the version of this log service.
	VersionMajor, VersionMinor, VersionPatch = 1, 0, 0
)

// KeyBuilder builds metadata keys scoped by a provider-specific root prefix.
type KeyBuilder struct {
	prefix string
}

// NewKeyBuilder creates a metadata key builder with the given root prefix.
func NewKeyBuilder(prefix string) *KeyBuilder {
	if prefix == "" || prefix == "." {
		prefix = LegacyServicePrefix
	}
	return &KeyBuilder{prefix: prefix}
}

// Prefix returns the root prefix used by this builder.
func (b *KeyBuilder) Prefix() string {
	return b.prefix
}

// ServiceInstanceKey returns the key for service instance.
func (b *KeyBuilder) ServiceInstanceKey() string {
	return fmt.Sprintf("%s/instance", b.prefix)
}

// VersionKey returns the key for version.
func (b *KeyBuilder) VersionKey() string {
	return fmt.Sprintf("%s/version", b.prefix)
}

// LogsPrefix returns the prefix for logs.
func (b *KeyBuilder) LogsPrefix() string {
	return fmt.Sprintf("%s/logs", b.prefix)
}

// LogIdGeneratorKey returns the key for log ID generator.
func (b *KeyBuilder) LogIdGeneratorKey() string {
	return fmt.Sprintf("%s/logidgen", b.prefix)
}

// QuorumsPrefix returns the prefix for quorums.
func (b *KeyBuilder) QuorumsPrefix() string {
	return fmt.Sprintf("%s/quorums", b.prefix)
}

// QuorumIdGeneratorKey returns the key for quorum ID generator.
func (b *KeyBuilder) QuorumIdGeneratorKey() string {
	return fmt.Sprintf("%s/quorumidgen", b.prefix)
}

// NodesPrefix returns the prefix for logstore instances.
func (b *KeyBuilder) NodesPrefix() string {
	return fmt.Sprintf("%s/logstores", b.prefix)
}

// ReaderTempInfoPrefix returns the prefix for reader temporary information.
func (b *KeyBuilder) ReaderTempInfoPrefix() string {
	return fmt.Sprintf("%s/readers", b.prefix)
}

// SegmentCleanupStatusPrefix returns the prefix for segment cleanup status.
func (b *KeyBuilder) SegmentCleanupStatusPrefix() string {
	return fmt.Sprintf("%s/cleaning", b.prefix)
}

// ConditionWriteKey returns the key for conditional write configuration.
func (b *KeyBuilder) ConditionWriteKey() string {
	return fmt.Sprintf("%s/conditionwrite", b.prefix)
}

// BuildLogKey builds the key for a log.
func (b *KeyBuilder) BuildLogKey(logName string) string {
	return fmt.Sprintf("%s/%s", b.LogsPrefix(), logName)
}

// BuildLogLockKey builds the lock key for a log.
func (b *KeyBuilder) BuildLogLockKey(logName string) string {
	return fmt.Sprintf("%s/%s/lock", b.LogsPrefix(), logName)
}

// BuildSegmentInstanceKey builds the key for a segment instance.
func (b *KeyBuilder) BuildSegmentInstanceKey(logName string, segmentId string) string {
	return fmt.Sprintf("%s/%s/segments/%s", b.LogsPrefix(), logName, segmentId)
}

// BuildQuorumInfoKey builds the key for quorum information.
func (b *KeyBuilder) BuildQuorumInfoKey(quorumId string) string {
	return fmt.Sprintf("%s/%s", b.QuorumsPrefix(), quorumId)
}

// BuildNodeKey builds the key for a node.
func (b *KeyBuilder) BuildNodeKey(nodeId string) string {
	return fmt.Sprintf("%s/%s", b.NodesPrefix(), nodeId)
}

// BuildLogReaderTempInfoKey builds the key for reader temporary information.
func (b *KeyBuilder) BuildLogReaderTempInfoKey(logId int64, readerName string) string {
	return fmt.Sprintf("%s/%d/%s", b.ReaderTempInfoPrefix(), logId, readerName)
}

// BuildLogAllReaderTempInfosKey builds the key for all reader temporary information.
func (b *KeyBuilder) BuildLogAllReaderTempInfosKey(logId int64) string {
	return fmt.Sprintf("%s/%d/", b.ReaderTempInfoPrefix(), logId)
}

// BuildAllSegmentsCleanupStatusKey builds a key for all segment cleanup status.
func (b *KeyBuilder) BuildAllSegmentsCleanupStatusKey(logId int64) string {
	return fmt.Sprintf("%s/%d", b.SegmentCleanupStatusPrefix(), logId)
}

// BuildSegmentCleanupStatusKey builds a key for segment cleanup status.
func (b *KeyBuilder) BuildSegmentCleanupStatusKey(logId int64, segmentId int64) string {
	return fmt.Sprintf("%s/%d/%d", b.SegmentCleanupStatusPrefix(), logId, segmentId)
}

// LogDeletedPrefix returns the prefix for parked (soft-deleted) log metadata.
func (b *KeyBuilder) LogDeletedPrefix() string {
	return fmt.Sprintf("%s/logs-deleted", b.prefix)
}

// BuildLogDeletedKey builds the parked LogMeta key for a soft-deleted log.
func (b *KeyBuilder) BuildLogDeletedKey(logName string, deletedTs int64) string {
	return fmt.Sprintf("%s/%s-%d", b.LogDeletedPrefix(), logName, deletedTs)
}

// BuildLogDeletedSegmentKey builds a parked segment key under a soft-deleted log.
func (b *KeyBuilder) BuildLogDeletedSegmentKey(logName string, deletedTs int64, segmentId int64) string {
	return fmt.Sprintf("%s/%s-%d/segments/%d", b.LogDeletedPrefix(), logName, deletedTs, segmentId)
}

// BuildLogCleanupStatusPrefix returns the cleanup-status prefix for one log WITH a
// trailing slash, so a prefix-delete of cleaning/12 cannot also match cleaning/123.
func (b *KeyBuilder) BuildLogCleanupStatusPrefix(logId int64) string {
	return fmt.Sprintf("%s/%d/", b.SegmentCleanupStatusPrefix(), logId)
}

// SegmentCompactedNotifyStatusPrefix returns the prefix for segment compacted-mark
// distribution status ("marking" — the Sealed-phase sibling of "cleaning").
func (b *KeyBuilder) SegmentCompactedNotifyStatusPrefix() string {
	return fmt.Sprintf("%s/marking", b.prefix)
}

// BuildAllSegmentsCompactedNotifyStatusKey builds a key for all compacted-notify status of a log.
func (b *KeyBuilder) BuildAllSegmentsCompactedNotifyStatusKey(logId int64) string {
	return fmt.Sprintf("%s/%d", b.SegmentCompactedNotifyStatusPrefix(), logId)
}

// BuildSegmentCompactedNotifyStatusKey builds a key for one segment's compacted-notify status.
func (b *KeyBuilder) BuildSegmentCompactedNotifyStatusKey(logId int64, segmentId int64) string {
	return fmt.Sprintf("%s/%d/%d", b.SegmentCompactedNotifyStatusPrefix(), logId, segmentId)
}

// BuildLogCompactedNotifyStatusPrefix returns the compacted-notify prefix for one log WITH a
// trailing slash, so a prefix-delete of marking/12 cannot also match marking/123.
func (b *KeyBuilder) BuildLogCompactedNotifyStatusPrefix(logId int64) string {
	return fmt.Sprintf("%s/%d/", b.SegmentCompactedNotifyStatusPrefix(), logId)
}

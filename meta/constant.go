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
	// ServicePrefix is the prefix for this log service.
	ServicePrefix = "woodpecker"
	// ServiceInstanceKey is the key for service instance.
	ServiceInstanceKey = ServicePrefix + "/instance"
	// Major.Minor.Patch is the version of this log service.
	VersionMajor, VersionMinor, VersionPatch = 1, 0, 0
	// VersionKey is the key for version.
	VersionKey = ServicePrefix + "/version"
	// LogsPrefix is the prefix for logs.
	LogsPrefix = ServicePrefix + "/logs"
	// LogIdGeneratorKey is the key for log ID generator.
	LogIdGeneratorKey = ServicePrefix + "/logidgen"
	// QuorumsPrefix is the prefix for quorums.
	QuorumsPrefix = ServicePrefix + "/quorums"
	// QuorumIdGeneratorKey is the key for quorum ID generator.
	QuorumIdGeneratorKey = ServicePrefix + "/quorumidgen"
	// NodesPrefix is the prefix for logstore instances.
	NodesPrefix = ServicePrefix + "/logstores"
	// ReaderTempInfoPrefix is the prefix for reader temporary information.
	ReaderTempInfoPrefix = ServicePrefix + "/readers"
	// SegmentCleanupStatusPrefix is the prefix for segment cleanup status.
	SegmentCleanupStatusPrefix = ServicePrefix + "/cleaning"
)

// BuildLogKey builds the key for a log.
func BuildLogKey(logName string) string {
	return fmt.Sprintf("%s/%s", LogsPrefix, logName)
}

// BuildLogLockKey builds the lock key for a log.
func BuildLogLockKey(logName string) string {
	return fmt.Sprintf("%s/%s/lock", LogsPrefix, logName)
}

// BuildSegmentInstanceKey builds the key for a segment instance.
func BuildSegmentInstanceKey(logName string, segmentId string) string {
	return fmt.Sprintf("%s/%s/segments/%s", LogsPrefix, logName, segmentId)
}

// BuildQuorumInfoKey builds the key for quorum information.
func BuildQuorumInfoKey(quorumId string) string {
	return fmt.Sprintf("%s/%s", QuorumsPrefix, quorumId)
}

// BuildNodeKey builds the key for a node.
func BuildNodeKey(nodeId string) string {
	return fmt.Sprintf("%s/%s", NodesPrefix, nodeId)
}

// BuildLogReaderTempInfoKey builds the key for reader temporary information.
func BuildLogReaderTempInfoKey(logId int64, readerName string) string {
	return fmt.Sprintf("%s/%d/%s", ReaderTempInfoPrefix, logId, readerName)
}

// BuildLogAllReaderTempInfosKey builds the key for all reader temporary information.
func BuildLogAllReaderTempInfosKey(logId int64) string {
	return fmt.Sprintf("%s/%d/", ReaderTempInfoPrefix, logId)
}

// BuildAllSegmentsCleanupStatusKey builds a key for all segment cleanup status
func BuildAllSegmentsCleanupStatusKey(logId int64) string {
	return fmt.Sprintf("%s/%d", SegmentCleanupStatusPrefix, logId)
}

// BuildSegmentCleanupStatusKey builds a key for segment cleanup status
func BuildSegmentCleanupStatusKey(logId int64, segmentId int64) string {
	return fmt.Sprintf("%s/%d/%d", SegmentCleanupStatusPrefix, logId, segmentId)
}

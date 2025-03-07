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
	NodesPrefix = ServicePrefix + "logstores"
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

package meta

import "fmt"

const (
	// ServicePrefix prefix for this log service
	ServicePrefix      = "woodpecker"
	ServiceInstanceKey = ServicePrefix + "/instance"

	VersionMajor, VersionMinor, VersionPatch = 1, 0, 0
	VersionKey                               = ServicePrefix + "/version"

	LogsPrefix        = ServicePrefix + "/logs"
	LogIdGeneratorKey = ServicePrefix + "/logidgen"

	QuorumsPrefix        = ServicePrefix + "/quorums"
	QuorumIdGeneratorKey = ServicePrefix + "/quorumidgen"

	NodesPrefix = ServicePrefix + "logstores"
)

func BuildLogKey(logName string) string {
	return fmt.Sprintf("%s/%s", LogsPrefix, logName)
}

func BuildLogLockKey(logName string) string {
	return fmt.Sprintf("%s/%s/lock", LogsPrefix, logName)
}

func BuildSegmentInstanceKey(logName string, segmentId string) string {
	return fmt.Sprintf("%s/%s/segments/%s", LogsPrefix, logName, segmentId)
}

func BuildQuorumInfoKey(quorumId string) string {
	return fmt.Sprintf("%s/%s", QuorumsPrefix, quorumId)
}

func BuildNodeKey(nodeId string) string {
	return fmt.Sprintf("%s/%s", NodesPrefix, nodeId)
}

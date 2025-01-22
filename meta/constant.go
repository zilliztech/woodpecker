package meta

import "fmt"

const (
	// ServicePrefix prefix for this log service
	ServicePrefix   = "/woodpecker"
	ServiceInstance = ServicePrefix + "/instance"

	LogsPrefix     = ServicePrefix + "/logs"
	logIdGenerator = LogsPrefix + "/idgen"

	QuorumsPrefix     = ServicePrefix + "/quorums"
	QuorumIdGenerator = QuorumsPrefix + "/idgen"

	NodesPrefix = ServicePrefix + "logstores"
)

func BuildLogPath(logName string) string {
	return fmt.Sprintf("%s/%s", LogsPrefix, logName)
}

func BuildSegmentInstancePath(logName string, segmentId string) string {
	return fmt.Sprintf("%s/%s/segments/%s", LogsPrefix, logName, segmentId)
}

func BuildQuorumInfoPath(quorumId string) string {
	return fmt.Sprintf("%s/%s", QuorumsPrefix, quorumId)
}

func BuildNodePath(nodeId string) string {
	return fmt.Sprintf("%s/%s", NodesPrefix, nodeId)
}

package cmd

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/diagnosis"
	"github.com/zilliztech/woodpecker/proto"
)

// Explicit endpoints and prefix prevent auto-discovery of the server's own
// metadata namespace when the operator intended a Milvus WP-client namespace.
func collectDiagnosisQuorum(ctx context.Context, report *diagnosis.Report, o *diagnosis.Options, flags *metaEtcdFlags, ac *client.Client, members *client.Memberlist, logName string, segmentID int64) {
	obs := diagnosis.Observation{ID: report.RunID + ":quorum", SourceID: report.RunID + ":metadata", Kind: "quorum", Status: "unavailable", Code: "quorum_metadata_unavailable", StartedAt: time.Now().UTC()}
	defer func() {
		obs.FinishedAt = time.Now().UTC()
		report.Observations = append(report.Observations, obs)
	}()
	conn, err := resolveMetaEtcd(flags)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, o.RequestTimeout)
	defer cancel()
	cfg := clientv3.Config{Context: ctx, Endpoints: conn.endpoints, DialTimeout: o.RequestTimeout, MaxCallRecvMsgSize: int(o.Limits.ResponseBytes), Username: conn.username, Password: conn.password}
	if conn.useSSL {
		cfg.TLS, err = metaEtcdTLSConfig(conn)
		if err != nil {
			return
		}
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return
	}
	defer cli.Close()
	// One read-only transaction binds both keys to the same etcd revision.
	r, err := cli.Txn(ctx).Then(clientv3.OpGet(conn.kb.BuildLogKey(logName)), clientv3.OpGet(conn.kb.BuildSegmentInstanceKey(logName, fmt.Sprint(segmentID)))).Commit()
	if err != nil || len(r.Responses) != 2 {
		return
	}
	logResult, segmentResult := r.Responses[0].GetResponseRange(), r.Responses[1].GetResponseRange()
	if logResult == nil || segmentResult == nil || len(logResult.Kvs) != 1 || len(segmentResult.Kvs) != 1 {
		return
	}
	logMeta, segment := &proto.LogMeta{}, &proto.SegmentMetadata{}
	if pb.Unmarshal(logResult.Kvs[0].Value, logMeta) != nil || pb.Unmarshal(segmentResult.Kvs[0].Value, segment) != nil || logMeta.LogId < 0 || segment.SegNo != segmentID || segment.Quorum == nil {
		obs.Code = "invalid_quorum_metadata"
		return
	}
	q := segment.Quorum
	if len(q.Nodes) > 32 {
		obs.Code = "quorum_node_limit_exceeded"
		return
	}
	nodeIDs := make([]string, len(q.Nodes))
	for i, addr := range q.Nodes {
		if m, ok := ac.QuorumMember(members, addr); ok && m.ID != "" {
			nodeIDs[i] = m.ID
			// Discovery can expose an address shared by several advertised
			// identities. An external mapping cannot resolve that ambiguity.
			for _, other := range members.Members {
				if (other.ID == addr || other.ServiceAddr == addr || other.GossipAddr == addr) && other.ID != m.ID {
					nodeIDs[i] = ""
				}
			}
		}
	}
	obs.Quorum = &diagnosis.Quorum{LogID: logMeta.LogId, SegmentID: segmentID, State: segment.State.String(), ES: int(q.Es), WQ: int(q.Wq), AQ: int(q.Aq), NodeIDs: nodeIDs, Revision: r.Header.Revision}
	obs.Status, obs.Code = "observed", ""
	o.LogID, o.SegmentID = &logMeta.LogId, &segmentID
}

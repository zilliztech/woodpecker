package cmd

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/diagnosis"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/proto"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"
)

type readDiagnosisFlags struct {
	metadata metaEtcdFlags
	logName  string
	segment  int64
	reader   string
}

func (f *readDiagnosisFlags) register(cmd *cobra.Command, o *diagnosis.Options) {
	f.metadata.register(cmd)
	cmd.Flags().StringVar(&f.logName, "log-name", "", "Explicit WP-client metadata log; requires --segment, --etcd and --meta-prefix")
	cmd.Flags().Int64Var(&f.segment, "segment", 0, "Explicit metadata segment ID for replica/reader checks")
	cmd.Flags().StringVar(&f.reader, "reader", "", "Select one reader by exact name; otherwise inspect up to --max-readers")
	cmd.Flags().StringVar(&o.ReadMode, "read-mode", "unknown", "Expected reader use: unknown|tail|catch-up; not historical metrics retrieval")
	cmd.Flags().IntVar(&o.Limits.MaxReaders, "max-readers", 16, "Maximum reader records and lease checks per metadata sample (1..64)")
}

func (f *readDiagnosisFlags) validate(scenario string) error {
	flags := f.metadata.command.Flags()
	if scenario != "read" && scenario != "all" && (f.reader != "" || flags.Changed("read-mode") || flags.Changed("max-readers")) {
		return wperrors.NewUsageError("reader options require --scenario read (or all)")
	}
	selected := f.logName != "" || f.reader != "" || flags.Changed("segment")
	for _, name := range []string{"etcd", "meta-prefix", "etcd-cert", "etcd-key", "etcd-cacert", "etcd-tls-min-version", "etcd-username", "etcd-password"} {
		selected = selected || flags.Changed(name)
	}
	if !selected {
		return nil
	}
	if (scenario != "write" && scenario != "read" && scenario != "all") || f.logName == "" || !flags.Changed("segment") || f.metadata.etcdEndpoints == "" || f.metadata.metaPrefix == "" {
		return wperrors.NewUsageError("metadata checks require --scenario write, read or all, --log-name, --segment, explicit --etcd and --meta-prefix for the WP-client namespace")
	}
	if strings.ContainsAny(f.reader, "/\x00\r\n") {
		return wperrors.NewUsageError("--reader must be an exact name without slash or control characters")
	}
	return validateMetaTarget("segment", []string{f.logName, fmt.Sprint(f.segment)})
}

func unavailableReadMetadata(code string) diagnosis.Observation {
	now := time.Now().UTC()
	return diagnosis.Observation{Status: "unavailable", Code: code, StartedAt: now, FinishedAt: now}
}

// Resolve only explicitly supplied metadata endpoints/prefix. Never discover a
// WP service's own namespace while inspecting a Milvus WP-client reader.
func (f *readDiagnosisFlags) configure(ctx context.Context, o *diagnosis.Options, ac *client.Client, members *client.Memberlist) func() {
	if f.logName == "" {
		return func() {}
	}
	failed := func() func() {
		o.ReadMetadata = func(context.Context) diagnosis.Observation {
			return unavailableReadMetadata("read_metadata_connection_unavailable")
		}
		return func() {}
	}
	conn, err := resolveMetaEtcd(&f.metadata)
	if err != nil {
		return failed()
	}
	cfg := clientv3.Config{Context: ctx, Endpoints: conn.endpoints, DialTimeout: o.RequestTimeout, MaxCallRecvMsgSize: int(o.Limits.ResponseBytes), Username: conn.username, Password: conn.password}
	if conn.useSSL {
		cfg.TLS, err = metaEtcdTLSConfig(conn)
		if err != nil {
			return failed()
		}
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return failed()
	}
	o.ReadMetadata = func(parent context.Context) (obs diagnosis.Observation) {
		obs = unavailableReadMetadata("read_metadata_unavailable")
		defer func() { obs.FinishedAt = time.Now().UTC() }()
		requestCtx, cancel := context.WithTimeout(parent, o.RequestTimeout)
		defer cancel()
		// Resolve log identity, then read it again with segment/readers at one
		// revision; a log re-created between these requests is not silently joined.
		logResult, err := cli.Get(requestCtx, conn.kb.BuildLogKey(f.logName))
		if err != nil || len(logResult.Kvs) != 1 {
			return obs
		}
		logMeta := &proto.LogMeta{}
		if pb.Unmarshal(logResult.Kvs[0].Value, logMeta) != nil || logMeta.LogId < 0 {
			obs.Code = "invalid_read_log_metadata"
			return obs
		}
		initialLogID := logMeta.LogId
		prefix := conn.kb.BuildLogAllReaderTempInfosKey(logMeta.LogId)
		readerKey, options := prefix, []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithLimit(int64(o.Limits.MaxReaders + 1))}
		if f.reader != "" {
			readerKey, options = conn.kb.BuildLogReaderTempInfoKey(logMeta.LogId, f.reader), nil
		}
		txn, err := cli.Txn(requestCtx).Then(clientv3.OpGet(conn.kb.BuildLogKey(f.logName)), clientv3.OpGet(conn.kb.BuildSegmentInstanceKey(f.logName, fmt.Sprint(f.segment))), clientv3.OpGet(readerKey, options...)).Commit()
		if err != nil || len(txn.Responses) != 3 {
			return obs
		}
		logs, segments, readers := txn.Responses[0].GetResponseRange(), txn.Responses[1].GetResponseRange(), txn.Responses[2].GetResponseRange()
		if logs == nil || segments == nil || readers == nil || len(logs.Kvs) != 1 || len(segments.Kvs) != 1 {
			return obs
		}
		if logs.Kvs[0].CreateRevision != logResult.Kvs[0].CreateRevision || pb.Unmarshal(logs.Kvs[0].Value, logMeta) != nil || logMeta.LogId != initialLogID {
			obs.Code = "read_log_identity_changed"
			return obs
		}
		segment := &proto.SegmentMetadata{}
		if pb.Unmarshal(segments.Kvs[0].Value, segment) != nil || segment.SegNo != f.segment || segment.Quorum == nil || len(segment.Quorum.Nodes) > 32 {
			obs.Code = "invalid_read_segment_metadata"
			return obs
		}
		q := segment.Quorum
		state := &diagnosis.ReadState{
			LogID: logMeta.LogId, SegmentID: f.segment, SegmentState: segment.State.String(), LastEntry: segment.LastEntryId,
			TruncatedSegment: logMeta.TruncatedSegmentId, TruncatedEntry: logMeta.TruncatedEntryId,
			ES: int(q.Es), WQ: int(q.Wq), AQ: int(q.Aq), Revision: txn.Header.Revision, ReportIntervalMS: wplog.UpdateReaderInfoIntervalMs, Readers: []diagnosis.ReaderPosition{},
		}
		for _, addr := range q.Nodes {
			id := ""
			if m, ok := ac.QuorumMember(members, addr); ok && m.ID != "" {
				id = m.ID
				for _, other := range members.Members {
					if memberMatchesAddr(other, addr) && other.ID != m.ID {
						id = ""
					}
				}
			}
			state.NodeIDs = append(state.NodeIDs, id)
		}
		obs.Status, obs.Code, obs.ReadState = "observed", "", state
		if len(readers.Kvs) > o.Limits.MaxReaders || readers.More {
			obs.Status, obs.Code = "truncated", "reader_record_limit"
		}
		leaseTTLs := map[int64]int64{}
		for i, kv := range readers.Kvs {
			if i >= o.Limits.MaxReaders {
				break
			}
			info := &proto.ReaderTempInfo{}
			if pb.Unmarshal(kv.Value, info) != nil || info.ReaderName == "" || info.LogId != state.LogID || string(kv.Key) != conn.kb.BuildLogReaderTempInfoKey(state.LogID, info.ReaderName) ||
				info.OpenTimestamp == 0 || info.OpenTimestamp > math.MaxInt64 || info.RecentReadTimestamp > math.MaxInt64 || info.RecentReadSegmentId < 0 || info.RecentReadEntryId < 0 {
				obs.Status, obs.Code = "invalid", "invalid_reader_metadata_identity"
				continue
			}
			ttl := int64(-1)
			if kv.Lease != 0 {
				var ok bool
				ttl, ok = leaseTTLs[kv.Lease]
				if !ok {
					response, err := cli.TimeToLive(requestCtx, clientv3.LeaseID(kv.Lease))
					if err != nil {
						obs.Status, obs.Code = "unavailable", "reader_lease_check_unavailable"
						continue
					}
					ttl = response.TTL
					leaseTTLs[kv.Lease] = ttl
				}
			}
			state.Readers = append(state.Readers, diagnosis.ReaderPosition{Name: info.ReaderName, OpenedMS: int64(info.OpenTimestamp), ReportedMS: int64(info.RecentReadTimestamp), SegmentID: info.RecentReadSegmentId, NextEntry: info.RecentReadEntryId, CreateRevision: kv.CreateRevision, LeaseTTL: ttl})
		}
		return obs
	}
	return func() { _ = cli.Close() }
}

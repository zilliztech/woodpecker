package cmd

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

type logTruncateOptions struct {
	segment, entry, revision int64
	apply                    bool
	reason, backup           string
}

func (o logTruncateOptions) validate() error {
	if o.segment < 0 || o.entry < 0 || o.revision < 0 {
		return wperrors.NewUsageError("--segment and --entry must be nonnegative; --expected-revision must not be negative")
	}
	if o.apply && (o.revision == 0 || strings.TrimSpace(o.reason) == "" || o.backup == "") {
		return wperrors.NewUsageError("--apply requires --expected-revision, --reason and --backup")
	}
	return nil
}

func newLogTruncateCommand() *cobra.Command {
	var flags metaEtcdFlags
	var opt logTruncateOptions
	c := &cobra.Command{Use: "truncate <logName>", Short: "Preview or advance a log's truncation frontier", Long: "Advance the truncation frontier with the SDK's metadata-update semantics and additional operator guards. Defaults to preview. The target must be a non-empty Completed or Sealed segment. Subsequent auditor rounds mark earlier finalized segments Truncated and reclaim data according to retention and reader protection. The frontier segment is retained. Apply can cause permanent data loss; verify application checkpoints first.", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if err := validateMetaTarget("log", args); err != nil {
			return err
		}
		if err := opt.validate(); err != nil {
			return err
		}
		cli, kb, err := openMeta(&flags)
		if err != nil {
			return err
		}
		defer cli.Close()
		return runLogTruncate(cmd, cli, kb, args[0], opt)
	}}
	flags.register(c)
	c.Flags().Int64Var(&opt.segment, "segment", -1, "Target segment ID (required)")
	c.Flags().Int64Var(&opt.entry, "entry", -1, "Target entry ID (required)")
	_ = c.MarkFlagRequired("segment")
	_ = c.MarkFlagRequired("entry")
	c.Flags().BoolVar(&opt.apply, "apply", false, "Advance the frontier (default: preview only)")
	c.Flags().Int64Var(&opt.revision, "expected-revision", 0, "Log record ModRevision from preview")
	c.Flags().StringVar(&opt.reason, "reason", "", "Operator reason recorded in audit")
	c.Flags().StringVar(&opt.backup, "backup", "", "New pre-change backup file; never overwritten")
	return c
}

func runLogTruncate(cmd *cobra.Command, cli *clientv3.Client, kb *meta.KeyBuilder, name string, opt logTruncateOptions) error {
	if err := validateMetaTarget("log", []string{name}); err != nil {
		return err
	}
	if err := opt.validate(); err != nil {
		return err
	}
	ctx, cancel := metaCtx()
	defer cancel()
	key := kb.BuildLogKey(name)
	segKey := kb.BuildSegmentInstanceKey(name, fmt.Sprint(opt.segment))
	r, err := cli.Txn(ctx).Then(clientv3.OpGet(key), clientv3.OpGet(segKey)).Commit()
	if err != nil {
		return wperrors.NewNetworkError(err.Error())
	}
	logs, segs := r.Responses[0].GetResponseRange().Kvs, r.Responses[1].GetResponseRange().Kvs
	if len(logs) != 1 {
		return wperrors.NewTargetNotFoundError(key)
	}
	if len(segs) != 1 {
		return wperrors.NewTargetNotFoundError(segKey)
	}
	kv, skv := logs[0], segs[0]
	before, msg, err := decodeMeta("log", kv)
	if err != nil {
		return err
	}
	_, sm, err := decodeMeta("segment", skv)
	if err != nil {
		return err
	}
	lm := msg.(*proto.LogMeta)
	seg := sm.(*proto.SegmentMetadata)
	if opt.revision != 0 && opt.revision != kv.ModRevision {
		return wperrors.NewStateConflictError(fmt.Sprintf("revision mismatch: expected %d, current %d", opt.revision, kv.ModRevision))
	}
	if _, err := canonicalMetaSegmentID(segKey, key, seg); err != nil {
		return wperrors.NewStateConflictError(err.Error())
	}
	if seg.State != proto.SegmentState_Completed && seg.State != proto.SegmentState_Sealed {
		return wperrors.NewStateConflictError("target segment must be Completed or Sealed; active entry bounds are not authoritative")
	}
	if seg.LastEntryId < 0 {
		return wperrors.NewStateConflictError("target segment has no finalized entries; choose a non-empty Completed or Sealed segment")
	}
	if opt.entry > seg.LastEntryId {
		return wperrors.NewUsageError(fmt.Sprintf("entry must be within 0..%d", seg.LastEntryId))
	}
	if lm.TruncatedSegmentId < -1 || lm.TruncatedEntryId < -1 || (lm.TruncatedSegmentId == -1 && lm.TruncatedEntryId != -1) {
		return wperrors.NewStateConflictError("existing log truncation frontier is invalid; repair metadata before advancing it")
	}
	if opt.segment < lm.TruncatedSegmentId || (opt.segment == lm.TruncatedSegmentId && opt.entry < lm.TruncatedEntryId) {
		return wperrors.NewStateConflictError("truncation cannot move backward or restore deleted data")
	}
	unchanged := opt.segment == lm.TruncatedSegmentId && opt.entry == lm.TruncatedEntryId
	next := pb.Clone(lm).(*proto.LogMeta)
	next.TruncatedSegmentId = opt.segment
	next.TruncatedEntryId = opt.entry
	if !unchanged {
		next.ModificationTimestamp = uint64(time.Now().Unix())
	}
	encoded, err := pb.Marshal(next)
	if err != nil {
		return err
	}
	afterKV := *kv
	afterKV.Value = encoded
	after, _, err := decodeMeta("log", &afterKV)
	if err != nil {
		return err
	}
	result := map[string]any{"applied": false, "unchanged": unchanged, "before": before, "after": after, "snapshot_revision": r.Header.Revision, "target_segment_revision": skv.ModRevision, "reason": opt.reason, "effect": "Earlier finalized segments become eligible for auditor truncation and retention-based cleanup; the frontier segment is retained. Verify application checkpoints before applying."}
	if !opt.apply || unchanged {
		return renderMeta(cmd, result)
	}
	if err := writeMetaBackup(opt.backup, map[string]any{"prefix": kb.Prefix(), "snapshot_revision": r.Header.Revision, "reason": opt.reason, "created_at": time.Now().UTC(), "records": []metaRawBackup{rawBackup(kv, kb.Prefix()), rawBackup(skv, kb.Prefix())}}); err != nil {
		return wperrors.NewConfigError("persist truncation backup: " + err.Error())
	}
	auditKey := fmt.Sprintf("%s/ops-audit/%d", kb.Prefix(), time.Now().UnixNano())
	audit, err := json.Marshal(map[string]any{"operation": "log.truncate", "target_key": key, "reason": opt.reason, "before": before, "after": after, "target_segment_revision": skv.ModRevision, "backup": opt.backup, "changed_at": time.Now().UTC()})
	if err != nil {
		return err
	}
	putOpts := []clientv3.OpOption{}
	if kv.Lease != 0 {
		putOpts = append(putOpts, clientv3.WithLease(clientv3.LeaseID(kv.Lease)))
	}
	tx, err := cli.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(key), "=", kv.ModRevision), clientv3.Compare(clientv3.ModRevision(segKey), "=", skv.ModRevision), clientv3.Compare(clientv3.Version(auditKey), "=", 0)).Then(clientv3.OpPut(key, string(encoded), putOpts...), clientv3.OpPut(auditKey, string(audit))).Commit()
	if err != nil {
		return wperrors.NewNetworkError(err.Error())
	}
	if !tx.Succeeded {
		return wperrors.NewStateConflictError("log or target segment changed; no truncation applied. Preview again and use a new backup file")
	}
	result["applied"] = true
	result["new_revision"] = tx.Header.Revision
	result["audit_key"] = auditKey
	result["backup"] = opt.backup
	return renderMeta(cmd, result)
}

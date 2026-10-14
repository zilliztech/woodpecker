package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

var metaPatchFields = map[string]map[string]bool{
	"log":     {"max_segment_roll_time_seconds": true, "max_segment_roll_size_bytes": true, "compaction_buffer_size_bytes": true, "max_compaction_file_count": true},
	"segment": {"size": true, "createTime": true, "completionTime": true, "sealedTime": true, "lastEntryId": true, "state": true},
}

func patchMetaMessage(kind string, original pb.Message, patch []byte) (pb.Message, error) {
	msg := pb.Clone(original)
	fields := msg.ProtoReflect().Descriptor().Fields()
	d := json.NewDecoder(bytes.NewReader(patch))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, wperrors.NewUsageError("patch must be a JSON object")
	}
	seen := map[protoreflect.FieldNumber]bool{}
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, wperrors.NewUsageError("invalid patch key")
		}
		fd := fields.ByName(protoreflect.Name(key))
		if fd == nil {
			fd = fields.ByJSONName(key)
		}
		if fd == nil || !metaPatchFields[kind][string(fd.Name())] {
			return nil, wperrors.NewUsageError(fmt.Sprintf("field %q is not patchable; identity, quorum and log truncation are protected", key))
		}
		if seen[fd.Number()] {
			return nil, wperrors.NewUsageError("duplicate patch field: " + key)
		}
		seen[fd.Number()] = true
		var raw json.RawMessage
		if err := d.Decode(&raw); err != nil {
			return nil, wperrors.NewUsageError(err.Error())
		}
		value := string(raw)
		if len(raw) > 0 && raw[0] == '"' {
			if err := json.Unmarshal(raw, &value); err != nil {
				return nil, err
			}
		}
		if fd.Kind() == protoreflect.EnumKind {
			enumValue, named := proto.SegmentState_value[value]
			if !named {
				n, err := strconv.ParseInt(value, 10, 32)
				if err != nil {
					return nil, wperrors.NewUsageError("state must be Active, Completed, Sealed, Truncated or a valid enum number")
				}
				enumValue = int32(n)
			}
			if _, ok := proto.SegmentState_name[enumValue]; !ok {
				return nil, wperrors.NewUsageError("unknown segment state: " + value)
			}
			msg.ProtoReflect().Set(fd, protoreflect.ValueOfEnum(protoreflect.EnumNumber(enumValue)))
			continue
		}
		minimum := int64(0)
		if kind == "segment" && fd.Name() == "lastEntryId" {
			minimum = -1
		}
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil || v < minimum {
			return nil, wperrors.NewUsageError(fmt.Sprintf("field %s requires an int64 >= %d", key, minimum))
		}
		msg.ProtoReflect().Set(fd, protoreflect.ValueOfInt64(v))
	}
	if _, err := d.Token(); err != nil {
		return nil, wperrors.NewUsageError(err.Error())
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, wperrors.NewUsageError("trailing data in patch file")
	}
	if len(seen) == 0 {
		return nil, wperrors.NewUsageError("patch must contain at least one supported field")
	}
	if kind == "log" {
		msg.(*proto.LogMeta).ModificationTimestamp = uint64(time.Now().Unix())
	}
	// Embedded quorum is immutable here. Report its pre-existing problems separately;
	// they must not prevent repairing the segment's scalar fields and lifecycle invariants.
	if issues := validateMetaFields(msg); len(issues) > 0 {
		return nil, wperrors.NewStateConflictError(strings.Join(issues, "; "))
	}
	return msg, nil
}

type metaPatchOptions struct {
	file, reason, backup string
	revision             int64
	apply, offline       bool
}

func (o metaPatchOptions) validateApply() error {
	if o.apply && (!o.offline || o.revision <= 0 || strings.TrimSpace(o.reason) == "" || o.backup == "") {
		return wperrors.NewUsageError("--apply requires --offline, --expected-revision, --reason, and --backup")
	}
	return nil
}

func newMetaPatchCommand(kind string) *cobra.Command {
	var flags metaEtcdFlags
	var opt metaPatchOptions
	use, nargs := "patch <logName>", 1
	if kind == "segment" {
		use = "patch <logName> <segmentId>"
		nargs = 2
	}
	c := &cobra.Command{Use: use, Short: "Preview a restricted metadata correction; use --apply for revision-checked writing", Long: "Patch policy/statistics/timestamps, segment lastEntryId and state. Identity, quorum and log truncation are protected. Segment state correction does not create compacted files or repair replica contents; Truncated can enable cleanup. Applying requires stopped writers/auditors, a revision, reason, and a new backup file.", Args: cobra.ExactArgs(nargs), RunE: func(cmd *cobra.Command, args []string) error {
		if err := validateMetaTarget(kind, args); err != nil {
			return err
		}
		if err := opt.validateApply(); err != nil {
			return err
		}
		patch, err := os.ReadFile(opt.file)
		if err != nil {
			return wperrors.NewConfigError(err.Error())
		}
		if len(patch) > 1<<20 {
			return wperrors.NewUsageError("patch exceeds 1 MiB")
		}
		cli, kb, err := openMeta(&flags)
		if err != nil {
			return err
		}
		defer cli.Close()
		key, err := metaTargetKey(kind, kb, args)
		if err != nil {
			return err
		}
		return runMetaPatch(cmd, cli, kb, kind, args[0], key, patch, opt)
	}}
	flags.register(c)
	c.Flags().StringVar(&opt.file, "patch-file", "", "JSON object containing supported scalar fields (required)")
	_ = c.MarkFlagRequired("patch-file")
	c.Flags().BoolVar(&opt.apply, "apply", false, "Commit the previewed correction (default: no writes)")
	c.Flags().BoolVar(&opt.offline, "offline", false, "Confirm relevant writers/auditors are stopped and will remain stopped during repair")
	c.Flags().Int64Var(&opt.revision, "expected-revision", 0, "Required target ModRevision for apply")
	c.Flags().StringVar(&opt.reason, "reason", "", "Operator's reason, recorded with the change")
	c.Flags().StringVar(&opt.backup, "backup", "", "New pre-change backup file (required for apply; never overwritten)")
	return c
}

func runMetaPatch(cmd *cobra.Command, cli *clientv3.Client, kb *meta.KeyBuilder, kind, logName, key string, patch []byte, opt metaPatchOptions) error {
	if err := opt.validateApply(); err != nil {
		return err
	}
	// Read the log, its segments and locks at a single MVCC revision.
	ctx, cancel := metaCtx()
	defer cancel()
	logKey := kb.BuildLogKey(logName)
	r, err := cli.Get(ctx, logKey, clientv3.WithPrefix())
	if err != nil {
		return wperrors.NewNetworkError(err.Error())
	}
	var kv, logKV *mvccpb.KeyValue
	related := []*mvccpb.KeyValue{}
	for _, entry := range r.Kvs {
		k := string(entry.Key)
		if k == key {
			kv = entry
		}
		if k == logKey {
			logKV = entry
		}
		if k == logKey || strings.HasPrefix(k, logKey+"/segments/") {
			related = append(related, entry)
		}
		if opt.apply && strings.HasPrefix(k, kb.BuildLogLockKey(logName)) {
			return wperrors.NewStateConflictError("log writer lock is present; stop the writer/auditor before applying")
		}
	}
	if kv == nil || logKV == nil {
		return wperrors.NewTargetNotFoundError(key)
	}
	if opt.revision != 0 && opt.revision != kv.ModRevision {
		return wperrors.NewStateConflictError(fmt.Sprintf("revision mismatch: expected %d, current %d", opt.revision, kv.ModRevision))
	}
	before, msg, err := decodeMeta(kind, kv)
	if err != nil {
		return err
	}
	segmentID := int64(-1)
	warnings := []string{}
	if kind == "segment" {
		segment := msg.(*proto.SegmentMetadata)
		segmentID, err = canonicalMetaSegmentID(key, logKey, segment)
		if err != nil {
			return wperrors.NewStateConflictError(err.Error())
		}
		if segment.Quorum != nil {
			for _, issue := range validateMetaMessage(segment.Quorum) {
				warnings = append(warnings, "embedded quorum (unchanged): "+issue)
			}
		}
	}
	afterMsg, err := patchMetaMessage(kind, msg, patch)
	if err != nil {
		return err
	}
	if kind == "segment" {
		_, logMsg, err := decodeMeta("log", logKV)
		if err != nil {
			return err
		}
		logMeta := logMsg.(*proto.LogMeta)
		segment := afterMsg.(*proto.SegmentMetadata)
		if segment.State != proto.SegmentState_Active && logMeta.TruncatedSegmentId == segmentID && logMeta.TruncatedEntryId > segment.LastEntryId {
			return wperrors.NewStateConflictError("lastEntryId is below the log truncation frontier")
		}
	}
	encoded, err := pb.Marshal(afterMsg)
	if err != nil {
		return err
	}
	afterKV := *kv
	afterKV.Value = encoded
	after, _, err := decodeMeta(kind, &afterKV)
	if err != nil {
		return err
	}
	oldFields, newFields := map[string]json.RawMessage{}, map[string]json.RawMessage{}
	if err := json.Unmarshal(before.Metadata, &oldFields); err != nil {
		return err
	}
	if err := json.Unmarshal(after.Metadata, &newFields); err != nil {
		return err
	}
	changes := map[string]any{}
	for field, value := range newFields {
		if !bytes.Equal(value, oldFields[field]) {
			var oldValue, newValue any
			if err := json.Unmarshal(oldFields[field], &oldValue); err != nil {
				return err
			}
			if err := json.Unmarshal(value, &newValue); err != nil {
				return err
			}
			changes[field] = map[string]any{"before": oldValue, "after": newValue}
		}
	}
	preview := map[string]any{"applied": false, "before": before, "after": after, "changes": changes, "reason": opt.reason, "snapshot_revision": r.Header.Revision}
	if len(warnings) > 0 {
		preview["warnings"] = warnings
	}
	if !opt.apply {
		return renderMeta(cmd, preview)
	}
	backups := []metaRawBackup{}
	compares := []clientv3.Cmp{clientv3.Compare(clientv3.Version(kb.BuildLogLockKey(logName)), "=", 0).WithPrefix()}
	for _, entry := range related {
		backups = append(backups, rawBackup(entry, kb.Prefix()))
		if string(entry.Key) == key || string(entry.Key) == logKey {
			compares = append(compares, clientv3.Compare(clientv3.ModRevision(string(entry.Key)), "=", entry.ModRevision))
		}
	}
	if err := writeMetaBackup(opt.backup, map[string]any{"prefix": kb.Prefix(), "snapshot_revision": r.Header.Revision, "reason": opt.reason, "created_at": time.Now().UTC(), "records": backups}); err != nil {
		return err
	}
	auditKey := fmt.Sprintf("%s/ops-audit/%d", kb.Prefix(), time.Now().UnixNano())
	audit, err := json.Marshal(map[string]any{"target_key": key, "reason": opt.reason, "changed_at": time.Now().UTC(), "before": before, "after": after, "backup": opt.backup})
	if err != nil {
		return err
	}
	putOpts := []clientv3.OpOption{}
	if kv.Lease != 0 {
		putOpts = append(putOpts, clientv3.WithLease(clientv3.LeaseID(kv.Lease)))
	}
	compares = append(compares, clientv3.Compare(clientv3.Version(auditKey), "=", 0))
	tx, err := cli.Txn(ctx).If(compares...).Then(clientv3.OpPut(key, string(encoded), putOpts...), clientv3.OpPut(auditKey, string(audit))).Commit()
	if err != nil {
		return wperrors.NewNetworkError("commit metadata correction: " + err.Error())
	}
	if !tx.Succeeded {
		return wperrors.NewStateConflictError("metadata or writer lock changed; no correction applied. Refresh metadata and use a new backup file")
	}
	preview["applied"] = true
	preview["new_revision"] = tx.Header.Revision
	preview["audit_key"] = auditKey
	preview["backup"] = opt.backup
	return renderMeta(cmd, preview)
}

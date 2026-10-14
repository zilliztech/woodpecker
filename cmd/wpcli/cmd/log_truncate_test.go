package cmd

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/proto"
)

func TestLogTruncatePreviewApplyAndNoop(t *testing.T) {
	cli, kb, endpoint := metaValidationFixture(t)
	ctx := context.Background()
	key := kb.BuildLogKey("a")
	segKey := kb.BuildSegmentInstanceKey("a", "1")
	original := &proto.LogMeta{LogId: 7, MaxCompactionFileCount: 600, CreationTimestamp: 100, ModificationTimestamp: 101, TruncatedSegmentId: 0, TruncatedEntryId: 10}
	original.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	raw, err := pb.Marshal(original)
	require.NoError(t, err)
	lease, err := cli.Grant(ctx, 120)
	require.NoError(t, err)
	_, err = cli.Put(ctx, key, string(raw), clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	segment := &proto.SegmentMetadata{SegNo: 1, CreateTime: 100, CompletionTime: 101, SealedTime: 102, State: proto.SegmentState_Sealed, LastEntryId: 99}
	putMetaValidationRecord(t, cli, segKey, segment)
	olderKey := kb.BuildSegmentInstanceKey("a", "0")
	putMetaValidationRecord(t, cli, olderKey, &proto.SegmentMetadata{State: proto.SegmentState_Completed, LastEntryId: 99})
	before, err := metaGet(cli, key)
	require.NoError(t, err)
	beforeSegment, err := metaGet(cli, segKey)
	require.NoError(t, err)
	beforeOlder, err := metaGet(cli, olderKey)
	require.NoError(t, err)
	args := []string{"log", "truncate", "a", "--segment", "1", "--entry", "11"}
	out, err := runMetaCommand(t, endpoint, kb.Prefix(), args...)
	require.NoError(t, err)
	var preview struct {
		Applied               bool `json:"applied"`
		Unchanged             bool `json:"unchanged"`
		Before, After         metaRecord
		TargetSegmentRevision int64 `json:"target_segment_revision"`
		SnapshotRevision      int64 `json:"snapshot_revision"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &preview))
	require.False(t, preview.Applied)
	require.False(t, preview.Unchanged)
	require.Equal(t, before.ModRevision, preview.Before.Revision)
	require.Equal(t, beforeSegment.ModRevision, preview.TargetSegmentRevision)
	require.GreaterOrEqual(t, preview.SnapshotRevision, beforeSegment.ModRevision)
	var proposed map[string]any
	require.NoError(t, json.Unmarshal(preview.After.Metadata, &proposed))
	require.Equal(t, "11", proposed["truncated_entry_id"])
	after, err := metaGet(cli, key)
	require.NoError(t, err)
	require.Equal(t, before, after, "preview must not write")
	out, err = runMetaCommand(t, endpoint, kb.Prefix(), append(append([]string{}, args...), "-o", "yaml")...)
	require.NoError(t, err)
	var yamlResult map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(out), &yamlResult))
	require.Equal(t, false, yamlResult["applied"])

	// Like SDK truncation, this operation may advance retention while a writer exists.
	_, err = cli.Put(ctx, kb.BuildLogLockKey("a")+"/writer", "lock")
	require.NoError(t, err)
	backup := filepath.Join(t.TempDir(), "before.json")
	apply := []string{"--apply", "--expected-revision", fmt.Sprint(before.ModRevision), "--reason", "verified checkpoint", "--backup", backup}
	out, err = runMetaCommand(t, endpoint, kb.Prefix(), append(append([]string{}, args...), apply...)...)
	require.NoError(t, err)
	var applied struct {
		Applied     bool   `json:"applied"`
		AuditKey    string `json:"audit_key"`
		NewRevision int64  `json:"new_revision"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &applied))
	require.True(t, applied.Applied)
	after, err = metaGet(cli, key)
	require.NoError(t, err)
	require.Equal(t, applied.NewRevision, after.ModRevision)
	require.Equal(t, before.Lease, after.Lease)
	updated := &proto.LogMeta{}
	require.NoError(t, pb.Unmarshal(after.Value, updated))
	want := pb.Clone(original).(*proto.LogMeta)
	want.TruncatedSegmentId, want.TruncatedEntryId = 1, 11
	want.ModificationTimestamp = updated.ModificationTimestamp
	require.Greater(t, updated.ModificationTimestamp, original.ModificationTimestamp)
	require.True(t, pb.Equal(want, updated), "unrelated and unknown fields must survive")
	currentSegment, err := metaGet(cli, segKey)
	require.NoError(t, err)
	require.Equal(t, beforeSegment, currentSegment)
	currentOlder, err := metaGet(cli, olderKey)
	require.NoError(t, err)
	require.Equal(t, beforeOlder, currentOlder, "the CLI must not mark earlier segments Truncated")
	data, err := os.ReadFile(backup)
	require.NoError(t, err)
	var snapshot struct {
		Records []metaRawBackup `json:"records"`
	}
	require.NoError(t, json.Unmarshal(data, &snapshot))
	require.Len(t, snapshot.Records, 2)
	for i, kv := range [][]byte{raw, beforeSegment.Value} {
		decoded, err := base64.StdEncoding.DecodeString(snapshot.Records[i].ValueBase64)
		require.NoError(t, err)
		require.Equal(t, kv, decoded)
		require.True(t, json.Valid(snapshot.Records[i].Metadata))
	}
	info, err := os.Stat(backup)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	audit, err := metaGet(cli, applied.AuditKey)
	require.NoError(t, err)
	require.Equal(t, after.ModRevision, audit.ModRevision, "audit and log update must be atomic")
	var event struct {
		Operation string     `json:"operation"`
		Reason    string     `json:"reason"`
		TargetKey string     `json:"target_key"`
		Before    metaRecord `json:"before"`
	}
	require.NoError(t, json.Unmarshal(audit.Value, &event))
	require.Equal(t, "log.truncate", event.Operation)
	require.Equal(t, "verified checkpoint", event.Reason)
	require.Equal(t, key, event.TargetKey)
	require.Equal(t, before.ModRevision, event.Before.Revision)

	_, err = runMetaCommand(t, endpoint, kb.Prefix(), append(append([]string{}, args...), apply...)...)
	require.Equal(t, 4, wperrors.ExitCodeFor(err), "a stale expected revision must be rejected even for a no-op")
	noopBackup := filepath.Join(t.TempDir(), "noop.json")
	apply[2], apply[6] = fmt.Sprint(after.ModRevision), noopBackup
	out, err = runMetaCommand(t, endpoint, kb.Prefix(), append(append([]string{}, args...), apply...)...)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(out), &preview))
	require.False(t, preview.Applied)
	require.True(t, preview.Unchanged)
	require.NoFileExists(t, noopBackup)
	current, err := metaGet(cli, key)
	require.NoError(t, err)
	require.Equal(t, after, current)
	audits, err := cli.Get(ctx, kb.Prefix()+"/ops-audit/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, audits.Kvs, 1)
}

func TestLogTruncateRejectsInvalidMetadataAndTargets(t *testing.T) {
	cli, kb, endpoint := metaValidationFixture(t)
	for _, tc := range []struct {
		name, message string
		code          int
		edit          func(*proto.LogMeta, *proto.SegmentMetadata)
	}{
		{"backward_entry", "backward", 4, func(l *proto.LogMeta, _ *proto.SegmentMetadata) { l.TruncatedEntryId = 12 }},
		{"backward_segment", "backward", 4, func(l *proto.LogMeta, _ *proto.SegmentMetadata) { l.TruncatedSegmentId = 1 }},
		{"active", "Completed or Sealed", 4, func(_ *proto.LogMeta, s *proto.SegmentMetadata) { s.State = proto.SegmentState_Active }},
		{"truncated", "Completed or Sealed", 4, func(_ *proto.LogMeta, s *proto.SegmentMetadata) { s.State = proto.SegmentState_Truncated }},
		{"unknown_state", "Completed or Sealed", 4, func(_ *proto.LogMeta, s *proto.SegmentMetadata) { s.State = 1 }},
		{"empty", "no finalized entries", 4, func(_ *proto.LogMeta, s *proto.SegmentMetadata) { s.LastEntryId = -1 }},
		{"beyond_tail", "0..10", 2, func(_ *proto.LogMeta, s *proto.SegmentMetadata) { s.LastEntryId = 10 }},
		{"identity_mismatch", "canonical segment ID", 4, func(_ *proto.LogMeta, s *proto.SegmentMetadata) { s.SegNo = 1 }},
		{"invalid_frontier", "existing log truncation frontier is invalid", 4, func(l *proto.LogMeta, _ *proto.SegmentMetadata) { l.TruncatedSegmentId = -2 }},
		{"unset_frontier", "existing log truncation frontier is invalid", 4, func(l *proto.LogMeta, _ *proto.SegmentMetadata) { l.TruncatedSegmentId = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logMeta := &proto.LogMeta{LogId: 1, TruncatedSegmentId: 0, TruncatedEntryId: 10}
			segment := &proto.SegmentMetadata{CreateTime: 100, CompletionTime: 101, LastEntryId: 99, State: proto.SegmentState_Completed}
			tc.edit(logMeta, segment)
			key := kb.BuildLogKey(tc.name)
			putMetaValidationRecord(t, cli, key, logMeta)
			segKey := kb.BuildSegmentInstanceKey(tc.name, "0")
			putMetaValidationRecord(t, cli, segKey, segment)
			before, err := metaGet(cli, key)
			require.NoError(t, err)
			beforeSegment, err := metaGet(cli, segKey)
			require.NoError(t, err)
			backup := filepath.Join(t.TempDir(), "before.json")
			args := []string{"log", "truncate", tc.name, "--segment", "0", "--entry", "11", "--apply", "--expected-revision", fmt.Sprint(before.ModRevision), "--reason", "verified", "--backup", backup}
			_, err = runMetaCommand(t, endpoint, kb.Prefix(), args...)
			require.Equal(t, tc.code, wperrors.ExitCodeFor(err))
			require.ErrorContains(t, err, tc.message)
			after, err := metaGet(cli, key)
			require.NoError(t, err)
			require.Equal(t, before, after)
			afterSegment, err := metaGet(cli, segKey)
			require.NoError(t, err)
			require.Equal(t, beforeSegment, afterSegment)
			require.NoFileExists(t, backup)
		})
	}
	audits, err := cli.Get(context.Background(), kb.Prefix()+"/ops-audit/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, audits.Kvs)
}

func TestLogTruncateValidatesBeforeConnecting(t *testing.T) {
	oldGlobals, oldWriter := Globals, sourceWriter
	t.Cleanup(func() { Globals, sourceWriter = oldGlobals, oldWriter })
	base := []string{"log", "truncate", "a", "--segment", "0", "--entry", "11"}
	for _, flags := range [][]string{
		{"--segment", "-1"},
		{"--entry", "-1"},
		{"--expected-revision", "-1"},
		{"--apply"},
		{"--apply", "--expected-revision", "1", "--reason", "verified"},
		{"--apply", "--expected-revision", "1", "--reason", " ", "--backup", "unused.json"},
	} {
		_, err := runMetaCommand(t, "127.0.0.1:1", "test", append(append([]string{}, base...), flags...)...)
		require.Equal(t, 2, wperrors.ExitCodeFor(err))
	}
	base[2] = "a/b"
	_, err := runMetaCommand(t, "127.0.0.1:1", "test", base...)
	require.Equal(t, 2, wperrors.ExitCodeFor(err))
	// Direct transaction callers must enforce the same guards before accessing etcd.
	require.Equal(t, 2, wperrors.ExitCodeFor(runLogTruncate(nil, nil, nil, "a", logTruncateOptions{apply: true})))
}

// Intercept only conditional commits, after their snapshot and backup exist.
// The update uses real etcd, so this exercises the actual comparison/transaction semantics.
type truncateCommitKV struct {
	clientv3.KV
	before func()
}

type truncateCommitTxn struct {
	clientv3.Txn
	before      func()
	conditional bool
}

func (k truncateCommitKV) Txn(ctx context.Context) clientv3.Txn {
	return &truncateCommitTxn{Txn: k.KV.Txn(ctx), before: k.before}
}

func (t *truncateCommitTxn) If(compares ...clientv3.Cmp) clientv3.Txn {
	t.Txn, t.conditional = t.Txn.If(compares...), true
	return t
}

func (t *truncateCommitTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.Txn = t.Txn.Then(ops...)
	return t
}

func (t *truncateCommitTxn) Commit() (*clientv3.TxnResponse, error) {
	if t.conditional {
		t.before()
	}
	return t.Txn.Commit()
}

func TestLogTruncateConcurrentUpdateDoesNotCommitOrAudit(t *testing.T) {
	for _, target := range []string{"log", "segment"} {
		t.Run(target, func(t *testing.T) {
			cli, kb, _ := metaValidationFixture(t)
			ctx := context.Background()
			key := kb.BuildLogKey("a")
			segKey := kb.BuildSegmentInstanceKey("a", "0")
			logMeta := &proto.LogMeta{LogId: 1, TruncatedSegmentId: 0, TruncatedEntryId: 10}
			segment := &proto.SegmentMetadata{State: proto.SegmentState_Completed, LastEntryId: 99}
			putMetaValidationRecord(t, cli, key, logMeta)
			putMetaValidationRecord(t, cli, segKey, segment)
			before, err := metaGet(cli, key)
			require.NoError(t, err)
			backup := filepath.Join(t.TempDir(), "before.json")
			originalKV := cli.KV
			cli.KV = truncateCommitKV{KV: originalKV, before: func() {
				require.FileExists(t, backup, "backup must precede the conditional commit")
				changedKey := key
				var changed pb.Message = logMeta
				if target == "log" {
					logMeta.MaxCompactionFileCount = 123
				} else {
					changedKey, changed = segKey, segment
					segment.LastEntryId = 98
				}
				raw, err := pb.Marshal(changed)
				require.NoError(t, err)
				_, err = originalKV.Put(ctx, changedKey, string(raw))
				require.NoError(t, err)
			}}
			t.Cleanup(func() { cli.KV = originalKV })
			cmd := &cobra.Command{}
			cmd.SetOut(new(bytes.Buffer))
			opt := logTruncateOptions{segment: 0, entry: 11, apply: true, revision: before.ModRevision, reason: "verified", backup: backup}
			require.ErrorContains(t, runLogTruncate(cmd, cli, kb, "a", opt), "log or target segment changed")
			after, err := metaGet(cli, key)
			require.NoError(t, err)
			decoded := &proto.LogMeta{}
			require.NoError(t, pb.Unmarshal(after.Value, decoded))
			require.EqualValues(t, 10, decoded.TruncatedEntryId)
			if target == "log" {
				require.EqualValues(t, 123, decoded.MaxCompactionFileCount)
			} else {
				require.Equal(t, before, after)
			}
			audit, err := cli.Get(ctx, kb.Prefix()+"/ops-audit/", clientv3.WithPrefix())
			require.NoError(t, err)
			require.Empty(t, audit.Kvs)
		})
	}
}

func TestLogTruncateReadAndBackupFailuresDoNotWrite(t *testing.T) {
	cli, kb, endpoint := metaValidationFixture(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		code int
	}{
		{"missing_log", 3},
		{"missing_segment", 3},
		{"corrupt_log", 12},
		{"corrupt_segment", 12},
		{"existing_backup", 12},
		{"missing_backup_directory", 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := kb.BuildLogKey(tc.name)
			segKey := kb.BuildSegmentInstanceKey(tc.name, "0")
			putMetaValidationRecord(t, cli, key, &proto.LogMeta{LogId: 1, TruncatedSegmentId: 0, TruncatedEntryId: 10})
			putMetaValidationRecord(t, cli, segKey, &proto.SegmentMetadata{State: proto.SegmentState_Completed, LastEntryId: 99})
			backup := filepath.Join(t.TempDir(), "before.json")
			switch tc.name {
			case "missing_log":
				_, err := cli.Delete(ctx, key)
				require.NoError(t, err)
			case "missing_segment":
				_, err := cli.Delete(ctx, segKey)
				require.NoError(t, err)
			case "corrupt_log", "corrupt_segment":
				badKey := key
				if tc.name == "corrupt_segment" {
					badKey = segKey
				}
				_, err := cli.Put(ctx, badKey, string([]byte{255}))
				require.NoError(t, err)
			case "existing_backup":
				require.NoError(t, os.WriteFile(backup, []byte("keep this backup"), 0o600))
			case "missing_backup_directory":
				backup = filepath.Join(t.TempDir(), "absent", "before.json")
			}
			before, err := cli.Get(ctx, key)
			require.NoError(t, err)
			beforeSegment, err := cli.Get(ctx, segKey)
			require.NoError(t, err)
			revision := int64(1)
			if len(before.Kvs) > 0 {
				revision = before.Kvs[0].ModRevision
			}
			_, err = runMetaCommand(t, endpoint, kb.Prefix(), "log", "truncate", tc.name, "--segment", "0", "--entry", "11", "--apply", "--expected-revision", fmt.Sprint(revision), "--reason", "verified", "--backup", backup)
			require.Equal(t, tc.code, wperrors.ExitCodeFor(err))
			after, err := cli.Get(ctx, key)
			require.NoError(t, err)
			require.Equal(t, before.Kvs, after.Kvs)
			afterSegment, err := cli.Get(ctx, segKey)
			require.NoError(t, err)
			require.Equal(t, beforeSegment.Kvs, afterSegment.Kvs)
			if tc.name == "existing_backup" {
				data, err := os.ReadFile(backup)
				require.NoError(t, err)
				require.Equal(t, "keep this backup", string(data))
			} else {
				require.NoFileExists(t, backup)
			}
		})
	}
	audit, err := cli.Get(ctx, kb.Prefix()+"/ops-audit/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, audit.Kvs)
}

func TestLogTruncateUsesEtcdContextWithoutAdminAccess(t *testing.T) {
	cli, kb, endpoint := metaValidationFixture(t)
	putMetaValidationRecord(t, cli, kb.BuildLogKey("a"), &proto.LogMeta{LogId: 1, TruncatedSegmentId: -1, TruncatedEntryId: -1})
	putMetaValidationRecord(t, cli, kb.BuildSegmentInstanceKey("a", "0"), &proto.SegmentMetadata{State: proto.SegmentState_Completed, LastEntryId: 99})
	config := filepath.Join(t.TempDir(), "cli.yaml")
	data := fmt.Sprintf("current-context: truncate-test\ncontexts:\n  truncate-test:\n    endpoint: http://127.0.0.1:1\n    etcd:\n      endpoints: [%q]\n      meta_prefix: %s\n", endpoint, kb.Prefix())
	require.NoError(t, os.WriteFile(config, []byte(data), 0o600))
	t.Setenv("WOODPECKER_CLI_CONFIG", config)
	root := NewRootCommand()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(new(bytes.Buffer))
	root.SetArgs([]string{"--context", "truncate-test", "log", "truncate", "a", "--segment", "0", "--entry", "11"})
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), `"applied": false`)
	require.Contains(t, out.String(), `"truncated_entry_id": "11"`)
}

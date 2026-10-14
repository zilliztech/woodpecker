package cmd

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

func TestMetaPatchTransactional(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("test-meta")
	ctx := context.Background()
	key := kb.BuildLogKey("a")
	original := &proto.LogMeta{LogId: 1, TruncatedSegmentId: -1, TruncatedEntryId: -1}
	original.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	raw, err := pb.Marshal(original)
	require.NoError(t, err)
	lease, err := cli.Grant(ctx, 120)
	require.NoError(t, err)
	_, err = cli.Put(ctx, key, string(raw), clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	// Prefix siblings must not be included in this log's backup.
	_, err = cli.Put(ctx, kb.BuildLogKey("ab"), string(raw))
	require.NoError(t, err)
	kv, err := metaGet(cli, key)
	require.NoError(t, err)
	revision := kv.ModRevision
	buffer := new(bytes.Buffer)
	cmd := &cobra.Command{}
	cmd.SetOut(buffer)
	patch := []byte(`{"maxSegmentRollSizeBytes":"4096"}`)
	require.NoError(t, runMetaPatch(cmd, cli, kb, "log", "a", key, patch, metaPatchOptions{}))
	after, err := metaGet(cli, key)
	require.NoError(t, err)
	require.Equal(t, revision, after.ModRevision)
	require.Equal(t, raw, after.Value)
	var preview map[string]any
	require.NoError(t, json.Unmarshal(buffer.Bytes(), &preview))
	require.Equal(t, false, preview["applied"])
	backup := filepath.Join(t.TempDir(), "before.json")
	opt := metaPatchOptions{apply: true, offline: true, revision: revision, reason: "test repair", backup: backup}
	buffer.Reset()
	require.NoError(t, runMetaPatch(cmd, cli, kb, "log", "a", key, patch, opt))
	after, err = metaGet(cli, key)
	require.NoError(t, err)
	decoded := &proto.LogMeta{}
	require.NoError(t, pb.Unmarshal(after.Value, decoded))
	require.Equal(t, int64(4096), decoded.MaxSegmentRollSizeBytes)
	require.Equal(t, original.ProtoReflect().GetUnknown(), decoded.ProtoReflect().GetUnknown())
	require.Equal(t, int64(lease.ID), after.Lease)
	data, err := os.ReadFile(backup)
	require.NoError(t, err)
	var snapshot struct {
		Records []metaRawBackup `json:"records"`
	}
	require.NoError(t, json.Unmarshal(data, &snapshot))
	require.Len(t, snapshot.Records, 1)
	require.Equal(t, key, snapshot.Records[0].Key)
	backed, err := base64.StdEncoding.DecodeString(snapshot.Records[0].ValueBase64)
	require.NoError(t, err)
	require.Equal(t, raw, backed)
	audit, err := cli.Get(ctx, kb.Prefix()+"/ops-audit/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, audit.Kvs, 1)
	var auditRecord map[string]any
	require.NoError(t, json.Unmarshal(audit.Kvs[0].Value, &auditRecord))
	require.Equal(t, "test repair", auditRecord["reason"])
	stale := opt
	stale.backup = filepath.Join(t.TempDir(), "stale.json")
	require.ErrorContains(t, runMetaPatch(cmd, cli, kb, "log", "a", key, patch, stale), "revision mismatch")
	unchanged, err := metaGet(cli, key)
	require.NoError(t, err)
	require.Equal(t, after.ModRevision, unchanged.ModRevision)
	_, err = os.Stat(stale.backup)
	require.True(t, os.IsNotExist(err))
	// A writer's lock under the lock prefix must block applies.
	_, err = cli.Put(ctx, kb.BuildLogLockKey("a")+"/writer", "lock")
	require.NoError(t, err)
	opt.revision = after.ModRevision
	opt.backup = filepath.Join(t.TempDir(), "locked.json")
	require.ErrorContains(t, runMetaPatch(cmd, cli, kb, "log", "a", key, patch, opt), "writer lock")
}

func TestMetaPatchFieldsAndConstraints(t *testing.T) {
	log := &proto.LogMeta{TruncatedSegmentId: -1, TruncatedEntryId: -1}
	for _, patch := range []string{`{"logId":2}`, `{"truncated_entry_id":1}`, `{"max_compaction_file_count":-1}`, `{"max_compaction_file_count":1,"maxCompactionFileCount":2}`, `{} `, `{} {}`} {
		_, err := patchMetaMessage("log", log, []byte(patch))
		require.Error(t, err, patch)
	}
	for _, patch := range []string{`{"max_compaction_file_count":2}`, `{"maxCompactionFileCount":"2"}`} {
		patched, err := patchMetaMessage("log", log, []byte(patch))
		require.NoError(t, err)
		require.Equal(t, int64(2), patched.(*proto.LogMeta).MaxCompactionFileCount)
	}
	zero, err := patchMetaMessage("log", &proto.LogMeta{MaxCompactionFileCount: 2, TruncatedSegmentId: -1, TruncatedEntryId: -1}, []byte(`{"max_compaction_file_count":0}`))
	require.NoError(t, err)
	require.Zero(t, zero.(*proto.LogMeta).MaxCompactionFileCount)
	segment := &proto.SegmentMetadata{SegNo: 0, LastEntryId: -1, CreateTime: 100, State: proto.SegmentState_Active}
	require.Empty(t, validateMetaMessage(segment))
	for _, patch := range []string{`{"state":1}`, `{"lastEntryId":-2}`, `{"quorum":{}}`, `{"createTime":0}`, `{"completionTime":99}`, `{"completionTime":101}`, `{"sealedTime":200}`} {
		_, err := patchMetaMessage("segment", segment, []byte(patch))
		require.Error(t, err, patch)
	}
	patched, err := patchMetaMessage("segment", segment, []byte(`{"size":0}`))
	require.NoError(t, err)
	require.Equal(t, int64(0), patched.(*proto.SegmentMetadata).Size)
	segment.State = proto.SegmentState_Completed
	require.NotEmpty(t, validateMetaMessage(segment))
	segment.CompletionTime = 101
	require.Empty(t, validateMetaMessage(segment))
	segment.State = proto.SegmentState_Sealed
	require.NotEmpty(t, validateMetaMessage(segment))
	segment.SealedTime = 102
	require.Empty(t, validateMetaMessage(segment))
}

func TestMetaTargetKeys(t *testing.T) {
	kb := meta.NewKeyBuilder("test")
	key, err := metaTargetKey("segment", kb, []string{"a", "0"})
	require.NoError(t, err)
	require.Equal(t, kb.BuildLogKey("a")+"/segments/0", key)
	for _, args := range [][]string{{"a", "-1"}, {"a", "bad"}, {"a/b", "0"}} {
		_, err = metaTargetKey("segment", kb, args)
		require.Error(t, err)
	}
}

func TestMetaSegmentStateAndLastEntryRepair(t *testing.T) {
	original := &proto.SegmentMetadata{SegNo: 0, LastEntryId: 999, CreateTime: 100, State: proto.SegmentState_Completed, CompletionTime: 101}
	original.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	for _, patch := range []string{`{"lastEntryId":"99","state":"Sealed","sealedTime":102}`, `{"lastEntryId":99,"state":3,"sealedTime":"102"}`} {
		repaired, err := patchMetaMessage("segment", original, []byte(patch))
		require.NoError(t, err)
		segment := repaired.(*proto.SegmentMetadata)
		require.EqualValues(t, 99, segment.LastEntryId)
		require.Equal(t, proto.SegmentState_Sealed, segment.State)
		require.Equal(t, original.ProtoReflect().GetUnknown(), segment.ProtoReflect().GetUnknown())
	}
	require.EqualValues(t, 999, original.LastEntryId)
	for _, patch := range []string{`{"state":"Sealed"}`, `{"state":1}`, `{"state":4294967296}`, `{"state":"Invalid"}`, `{"state":null}`, `{"lastEntryId":-2}`, `{"state":"Active"}`} {
		_, err := patchMetaMessage("segment", original, []byte(patch))
		require.Error(t, err, patch)
	}
	repaired, err := patchMetaMessage("segment", original, []byte(`{"state":"Active","completionTime":0,"sealedTime":0,"lastEntryId":-1}`))
	require.NoError(t, err)
	require.EqualValues(t, -1, repaired.(*proto.SegmentMetadata).LastEntryId)
}

func TestMetaSegmentRepairTransactional(t *testing.T) {
	cli := startTestEtcd(t)
	kb := meta.NewKeyBuilder("segment-repair")
	ctx := context.Background()
	key := kb.BuildSegmentInstanceKey("a", "0")
	put := func(key string, m pb.Message) {
		data, err := pb.Marshal(m)
		require.NoError(t, err)
		_, err = cli.Put(ctx, key, string(data))
		require.NoError(t, err)
	}
	put(kb.BuildLogKey("a"), &proto.LogMeta{LogId: 1, TruncatedSegmentId: 0, TruncatedEntryId: 10})
	put(key, &proto.SegmentMetadata{SegNo: 0, LastEntryId: 999, CreateTime: 100, CompletionTime: 101, State: proto.SegmentState_Completed})
	kv, err := metaGet(cli, key)
	require.NoError(t, err)
	cmd := &cobra.Command{}
	cmd.SetOut(new(bytes.Buffer))
	patch := []byte(`{"lastEntryId":99,"state":"Sealed","sealedTime":102}`)
	require.NoError(t, runMetaPatch(cmd, cli, kb, "segment", "a", key, patch, metaPatchOptions{}))
	unchanged, err := metaGet(cli, key)
	require.NoError(t, err)
	require.Equal(t, kv.ModRevision, unchanged.ModRevision)
	opt := metaPatchOptions{apply: true, offline: true, revision: kv.ModRevision, reason: "Repair after bug fix", backup: filepath.Join(t.TempDir(), "before.json")}
	require.NoError(t, runMetaPatch(cmd, cli, kb, "segment", "a", key, patch, opt))
	after, err := metaGet(cli, key)
	require.NoError(t, err)
	s := new(proto.SegmentMetadata)
	require.NoError(t, pb.Unmarshal(after.Value, s))
	require.EqualValues(t, 99, s.LastEntryId)
	require.Equal(t, proto.SegmentState_Sealed, s.State)
	require.FileExists(t, opt.backup)
	require.ErrorContains(t, runMetaPatch(cmd, cli, kb, "segment", "a", key, patch, opt), "revision mismatch")
	require.ErrorContains(t, runMetaPatch(cmd, cli, kb, "segment", "a", key, []byte(`{"lastEntryId":9}`), metaPatchOptions{}), "truncation frontier")
	_, err = cli.Put(ctx, kb.BuildLogLockKey("a")+"/writer", "lock")
	require.NoError(t, err)
	opt.revision = after.ModRevision
	opt.backup = filepath.Join(t.TempDir(), "locked.json")
	require.ErrorContains(t, runMetaPatch(cmd, cli, kb, "segment", "a", key, patch, opt), "writer lock")
}

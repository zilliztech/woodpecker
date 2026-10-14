package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

func metaValidationFixture(t *testing.T) (*clientv3.Client, *meta.KeyBuilder, string) {
	t.Helper()
	cli := startTestEtcd(t)
	members, err := cli.MemberList(context.Background())
	require.NoError(t, err)
	require.Len(t, members.Members, 1)
	require.NotEmpty(t, members.Members[0].ClientURLs)
	oldGlobals, oldWriter := Globals, sourceWriter
	t.Cleanup(func() { Globals, sourceWriter = oldGlobals, oldWriter })
	return cli, meta.NewKeyBuilder("meta-validation"), members.Members[0].ClientURLs[0]
}

func putMetaValidationRecord(t *testing.T, cli *clientv3.Client, key string, msg pb.Message) {
	t.Helper()
	raw, err := pb.Marshal(msg)
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), key, string(raw))
	require.NoError(t, err)
}

func TestMetaActiveRollbackRequiresTailReset(t *testing.T) {
	for _, state := range []proto.SegmentState{proto.SegmentState_Completed, proto.SegmentState_Sealed} {
		for _, tail := range []int64{0, 999} {
			t.Run(fmt.Sprintf("%s/tail_%d", state, tail), func(t *testing.T) {
				original := &proto.SegmentMetadata{SegNo: 0, CreateTime: 100, CompletionTime: 101, LastEntryId: tail, State: state}
				if state == proto.SegmentState_Sealed {
					original.SealedTime = 102
				}
				patch := []byte(`{"state":"Active","completionTime":0,"sealedTime":0}`)
				_, err := patchMetaMessage("segment", original, patch)
				require.ErrorContains(t, err, "Active segment requires lastEntryId -1")
				repaired, err := patchMetaMessage("segment", original, []byte(`{"state":"Active","lastEntryId":-1,"completionTime":0,"sealedTime":0}`))
				require.NoError(t, err)
				require.Empty(t, validateMetaMessage(repaired))
				require.Equal(t, tail, original.LastEntryId)
				require.Equal(t, state, original.State)
			})
		}
	}
}

func TestMetaCommandsRejectUnsafeActiveRollback(t *testing.T) {
	cli, kb, endpoint := metaValidationFixture(t)
	putMetaValidationRecord(t, cli, kb.BuildLogKey("a"), &proto.LogMeta{LogId: 1, TruncatedSegmentId: -1, TruncatedEntryId: -1})
	key := kb.BuildSegmentInstanceKey("a", "0")
	original := &proto.SegmentMetadata{SegNo: 0, CreateTime: 100, CompletionTime: 101, LastEntryId: 999, State: proto.SegmentState_Completed}
	putMetaValidationRecord(t, cli, key, original)
	before, err := metaGet(cli, key)
	require.NoError(t, err)
	patchFile := filepath.Join(t.TempDir(), "rollback.json")
	require.NoError(t, os.WriteFile(patchFile, []byte(`{"state":"Active","completionTime":0,"sealedTime":0}`), 0o600))
	backup := filepath.Join(t.TempDir(), "before.json")
	base := []string{"meta", "segment", "patch", "a", "0", "--patch-file", patchFile}
	apply := []string{"--apply", "--offline", "--expected-revision", fmt.Sprint(before.ModRevision), "--reason", "reviewed rollback", "--backup", backup}
	for _, flags := range [][]string{nil, apply} {
		_, err := runMetaCommand(t, endpoint, kb.Prefix(), append(append([]string{}, base...), flags...)...)
		require.Equal(t, 4, wperrors.ExitCodeFor(err))
		require.ErrorContains(t, err, "Active segment requires lastEntryId -1")
	}
	after, err := metaGet(cli, key)
	require.NoError(t, err)
	require.Equal(t, before.ModRevision, after.ModRevision)
	require.Equal(t, before.Value, after.Value)
	require.NoFileExists(t, backup)
	audit, err := cli.Get(context.Background(), kb.Prefix()+"/ops-audit/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, audit.Kvs)

	// Existing bad Active metadata must also be reported, and remain repairable.
	original.State, original.CompletionTime = proto.SegmentState_Active, 0
	putMetaValidationRecord(t, cli, key, original)
	out, err := runMetaCommand(t, endpoint, kb.Prefix(), "meta", "check", "a")
	require.Equal(t, 9, wperrors.ExitCodeFor(err))
	require.Contains(t, out, "Active segment requires lastEntryId -1")
	before, err = metaGet(cli, key)
	require.NoError(t, err)
	apply[3] = fmt.Sprint(before.ModRevision)
	require.NoError(t, os.WriteFile(patchFile, []byte(`{"lastEntryId":-1}`), 0o600))
	_, err = runMetaCommand(t, endpoint, kb.Prefix(), append(append([]string{}, base...), apply...)...)
	require.NoError(t, err)
	out, err = runMetaCommand(t, endpoint, kb.Prefix(), "meta", "check", "a")
	require.NoError(t, err)
	require.Contains(t, out, `"state": "ok"`)
}

func TestMetaCheckRequiresCanonicalSegmentKeys(t *testing.T) {
	cli, kb, endpoint := metaValidationFixture(t)
	for i, suffix := range []string{"00", "+0", "1", "0/child", "invalid"} {
		t.Run(suffix, func(t *testing.T) {
			logName := fmt.Sprintf("log%d", i)
			// A noncanonical record must not hide the real frontier's short tail.
			putMetaValidationRecord(t, cli, kb.BuildLogKey(logName), &proto.LogMeta{LogId: 1, TruncatedSegmentId: 0, TruncatedEntryId: 50})
			good := &proto.SegmentMetadata{SegNo: 0, CreateTime: 100, CompletionTime: 101, LastEntryId: 9, State: proto.SegmentState_Completed}
			putMetaValidationRecord(t, cli, kb.BuildSegmentInstanceKey(logName, "0"), good)
			key := kb.BuildSegmentInstanceKey(logName, suffix)
			bad := pb.Clone(good).(*proto.SegmentMetadata)
			bad.LastEntryId = 999
			putMetaValidationRecord(t, cli, key, bad)
			out, err := runMetaCommand(t, endpoint, kb.Prefix(), "meta", "check", logName)
			require.Equal(t, 9, wperrors.ExitCodeFor(err))
			require.Contains(t, out, "key must use canonical segment ID 0")
			require.Contains(t, out, "truncation frontier exceeds segment LastEntryID")
			_, err = cli.Delete(context.Background(), kb.BuildSegmentInstanceKey(logName, "0"))
			require.NoError(t, err)
			out, err = runMetaCommand(t, endpoint, kb.Prefix(), "meta", "check", logName)
			require.Equal(t, 9, wperrors.ExitCodeFor(err))
			require.Contains(t, out, "truncation frontier segment is missing")
			_, err = cli.Delete(context.Background(), key)
			require.NoError(t, err)
		})
	}
}

func TestMetaSegmentValidationNamesInvalidFields(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field string
		edit  func(*proto.SegmentMetadata)
	}{
		{"segment_id", "segNo must be nonnegative", func(m *proto.SegmentMetadata) { m.SegNo = -1 }},
		{"last_entry", "lastEntryId must be >= -1", func(m *proto.SegmentMetadata) { m.LastEntryId = -2 }},
		{"size", "size must be nonnegative", func(m *proto.SegmentMetadata) { m.Size = -1 }},
		{"creation", "createTime must be positive", func(m *proto.SegmentMetadata) { m.CreateTime = 0 }},
		{"completion", "completionTime must be nonnegative", func(m *proto.SegmentMetadata) { m.CompletionTime = -1 }},
		{"sealed", "sealedTime must be nonnegative", func(m *proto.SegmentMetadata) { m.SealedTime = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &proto.SegmentMetadata{CreateTime: 100, LastEntryId: -1, State: proto.SegmentState_Active}
			tc.edit(m)
			require.Contains(t, validateMetaMessage(m), tc.field)
		})
	}
	legacy := &proto.SegmentMetadata{LastEntryId: -1, State: proto.SegmentState_Active}
	_, err := patchMetaMessage("segment", legacy, []byte(`{"state":"Completed","completionTime":101}`))
	require.ErrorContains(t, err, "createTime must be positive")
	repaired, err := patchMetaMessage("segment", legacy, []byte(`{"state":"Completed","createTime":100,"completionTime":101}`))
	require.NoError(t, err)
	require.Empty(t, validateMetaMessage(repaired))
}

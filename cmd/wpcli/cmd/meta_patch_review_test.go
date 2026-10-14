package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/proto"
)

func TestMetaSegmentPatchRejectsIdentityMismatchBeforeBackup(t *testing.T) {
	cli, kb, endpoint := metaValidationFixture(t)
	logKey := kb.BuildLogKey("a")
	key := kb.BuildSegmentInstanceKey("a", "0")
	putMetaValidationRecord(t, cli, logKey, &proto.LogMeta{LogId: 1, TruncatedSegmentId: 0, TruncatedEntryId: 50})
	segment := &proto.SegmentMetadata{SegNo: 1, CreateTime: 100, CompletionTime: 101, LastEntryId: 99, State: proto.SegmentState_Completed}
	putMetaValidationRecord(t, cli, key, segment)
	before, err := metaGet(cli, key)
	require.NoError(t, err)
	beforeLog, err := metaGet(cli, logKey)
	require.NoError(t, err)
	patch := filepath.Join(t.TempDir(), "patch.json")
	require.NoError(t, os.WriteFile(patch, []byte(`{"lastEntryId":10}`), 0o600))
	backup := filepath.Join(t.TempDir(), "before.json")
	args := []string{"meta", "segment", "patch", "a", "0", "--patch-file", patch}
	apply := []string{"--apply", "--offline", "--expected-revision", fmt.Sprint(before.ModRevision), "--reason", "repair tail", "--backup", backup}
	for _, flags := range [][]string{nil, apply} {
		_, err := runMetaCommand(t, endpoint, kb.Prefix(), append(append([]string{}, args...), flags...)...)
		require.Equal(t, 4, wperrors.ExitCodeFor(err))
		require.ErrorContains(t, err, "canonical segment ID 1 matching segNo")
	}
	after, err := metaGet(cli, key)
	require.NoError(t, err)
	require.Equal(t, before, after)
	afterLog, err := metaGet(cli, logKey)
	require.NoError(t, err)
	require.Equal(t, beforeLog, afterLog)
	require.NoFileExists(t, backup)
	audit, err := cli.Get(context.Background(), kb.Prefix()+"/ops-audit/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, audit.Kvs)

	// A matching identity still must not allow a tail below this key's frontier.
	segment.SegNo = 0
	putMetaValidationRecord(t, cli, key, segment)
	before, err = metaGet(cli, key)
	require.NoError(t, err)
	apply[3] = fmt.Sprint(before.ModRevision)
	_, err = runMetaCommand(t, endpoint, kb.Prefix(), append(append([]string{}, args...), apply...)...)
	require.ErrorContains(t, err, "lastEntryId is below the log truncation frontier")
	after, err = metaGet(cli, key)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoFileExists(t, backup)
}

func TestMetaSegmentPatchPreservesInvalidQuorumWithWarnings(t *testing.T) {
	cli, kb, endpoint := metaValidationFixture(t)
	for _, tc := range []struct {
		name, issue string
		quorum      *proto.QuorumInfo
	}{
		{"short_quorum", "invalid quorum sizes or node count", &proto.QuorumInfo{Id: 1, Es: 3, Wq: 3, Aq: 2, Nodes: []string{"a:18080", "b:18080"}}},
		{"duplicate_node", "empty or duplicate quorum endpoint", &proto.QuorumInfo{Id: 1, Es: 3, Wq: 3, Aq: 2, Nodes: []string{"a:18080", "a:18080", "b:18080"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logKey := kb.BuildLogKey(tc.name)
			putMetaValidationRecord(t, cli, logKey, &proto.LogMeta{LogId: 1, TruncatedSegmentId: -1, TruncatedEntryId: -1})
			key := kb.BuildSegmentInstanceKey(tc.name, "0")
			segment := &proto.SegmentMetadata{CreateTime: 100, CompletionTime: 101, LastEntryId: 99, State: proto.SegmentState_Completed, Quorum: tc.quorum}
			putMetaValidationRecord(t, cli, key, segment)
			before, err := metaGet(cli, key)
			require.NoError(t, err)
			patch := filepath.Join(t.TempDir(), "patch.json")
			require.NoError(t, os.WriteFile(patch, []byte(`{"size":123,"lastEntryId":10,"state":"Completed"}`), 0o600))
			backup := filepath.Join(t.TempDir(), "before.json")
			args := []string{"meta", "segment", "patch", tc.name, "0", "--patch-file", patch}
			apply := []string{"--apply", "--offline", "--expected-revision", fmt.Sprint(before.ModRevision), "--reason", "repair scalar fields", "--backup", backup}
			for _, flags := range [][]string{nil, apply} {
				out, err := runMetaCommand(t, endpoint, kb.Prefix(), append(append([]string{}, args...), flags...)...)
				require.NoError(t, err)
				var result struct {
					Applied  bool     `json:"applied"`
					Warnings []string `json:"warnings"`
				}
				require.NoError(t, json.Unmarshal([]byte(out), &result))
				require.Equal(t, len(flags) > 0, result.Applied)
				require.Contains(t, result.Warnings, "embedded quorum (unchanged): "+tc.issue)
				if flags == nil {
					after, err := metaGet(cli, key)
					require.NoError(t, err)
					require.Equal(t, before, after, "preview must not write")
					require.NoFileExists(t, backup)
				}
			}
			after, err := metaGet(cli, key)
			require.NoError(t, err)
			var repaired proto.SegmentMetadata
			require.NoError(t, pb.Unmarshal(after.Value, &repaired))
			require.EqualValues(t, 10, repaired.LastEntryId)
			require.EqualValues(t, 123, repaired.Size)
			require.True(t, pb.Equal(tc.quorum, repaired.Quorum))
			out, err := runMetaCommand(t, endpoint, kb.Prefix(), "meta", "check", tc.name)
			require.Equal(t, 9, wperrors.ExitCodeFor(err), "check still reports the unresolved quorum")
			require.Contains(t, out, tc.issue)
			// Quorum warnings do not relax lifecycle validation or truncation bounds.
			_, err = patchMetaMessage("segment", &repaired, []byte(`{"state":"Sealed"}`))
			require.ErrorContains(t, err, "Sealed segment requires sealedTime")
			putMetaValidationRecord(t, cli, logKey, &proto.LogMeta{LogId: 1, TruncatedSegmentId: 0, TruncatedEntryId: 9})
			require.NoError(t, os.WriteFile(patch, []byte(`{"lastEntryId":8}`), 0o600))
			_, err = runMetaCommand(t, endpoint, kb.Prefix(), args...)
			require.ErrorContains(t, err, "lastEntryId is below the log truncation frontier")
		})
	}
}

func TestMetaBackupPersistsNewFileWithoutOverwrite(t *testing.T) {
	file := filepath.Join(t.TempDir(), "backup.json")
	want := map[string]string{"reason": "reviewed repair"}
	require.NoError(t, writeMetaBackup(file, want))
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	var got map[string]string
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, want, got)
	info, err := os.Stat(file)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.Equal(t, 12, wperrors.ExitCodeFor(writeMetaBackup(file, map[string]string{"reason": "different"})))
	after, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, data, after)
}

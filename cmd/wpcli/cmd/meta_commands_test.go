package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

func runMetaCommand(t *testing.T, endpoint, prefix string, args ...string) (string, error) {
	t.Helper()
	root := NewRootCommand()
	var out, diagnostic bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&diagnostic)
	root.SetArgs(append(args, "--etcd", endpoint, "--meta-prefix", prefix, "--timeout", "2s"))
	err := root.Execute()
	return out.String(), err
}

func TestMetaCommandsReadCheckAndExport(t *testing.T) {
	cli := startTestEtcd(t)
	members, err := cli.MemberList(context.Background())
	require.NoError(t, err)
	require.Len(t, members.Members, 1)
	require.NotEmpty(t, members.Members[0].ClientURLs)
	endpoint := members.Members[0].ClientURLs[0]
	kb := meta.NewKeyBuilder("meta-commands")
	oldGlobals, oldWriter := Globals, sourceWriter
	t.Cleanup(func() { Globals, sourceWriter = oldGlobals, oldWriter })
	put := func(key string, msg pb.Message) {
		raw, err := pb.Marshal(msg)
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), key, string(raw))
		require.NoError(t, err)
	}
	quorum := &proto.QuorumInfo{Id: 1, Es: 1, Wq: 1, Aq: 1, Nodes: []string{"node:18080"}}
	put(kb.BuildLogKey("a"), &proto.LogMeta{LogId: 7, TruncatedSegmentId: -1, TruncatedEntryId: -1})
	put(kb.BuildSegmentInstanceKey("a", "0"), &proto.SegmentMetadata{SegNo: 0, LastEntryId: 9, CreateTime: 100, CompletionTime: 101, State: proto.SegmentState_Completed, Quorum: quorum})
	put(kb.BuildQuorumInfoKey("1"), quorum)
	for _, args := range [][]string{
		{"meta", "log", "list"},
		{"meta", "log", "show", "a"},
		{"meta", "segment", "list", "a"},
		{"meta", "segment", "show", "a", "0"},
		{"meta", "quorum", "list"},
		{"meta", "quorum", "show", "1"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			out, err := runMetaCommand(t, endpoint, kb.Prefix(), args...)
			require.NoError(t, err)
			require.True(t, json.Valid([]byte(out)), out)
			require.Contains(t, out, `"metadata"`)
			require.Contains(t, out, `"revision"`)
		})
	}
	out, err := runMetaCommand(t, endpoint, kb.Prefix(), "meta", "segment", "show", "a", "0", "-o", "yaml")
	require.NoError(t, err)
	var shown struct {
		Metadata map[string]any `yaml:"metadata"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(out), &shown))
	require.Equal(t, "Completed", shown.Metadata["state"])
	out, err = runMetaCommand(t, endpoint, kb.Prefix(), "meta", "check", "a")
	require.NoError(t, err)
	require.Contains(t, out, `"state": "ok"`)
	require.Contains(t, out, "metadata only")
	file := filepath.Join(t.TempDir(), "export.json")
	_, err = runMetaCommand(t, endpoint, kb.Prefix(), "meta", "export", "--output-file", file)
	require.NoError(t, err)
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Contains(t, string(data), `"metadata": {`)
	require.Contains(t, string(data), `"value_base64"`)
	before := append([]byte(nil), data...)
	_, err = runMetaCommand(t, endpoint, kb.Prefix(), "meta", "export", "--output-file", file)
	require.Equal(t, 12, wperrors.ExitCodeFor(err))
	data, err = os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, before, data, "an existing export must never be overwritten")
	// Corrupt-but-present records are constraint findings, not missing targets.
	_, err = cli.Put(context.Background(), kb.BuildLogKey("a"), string([]byte{255}))
	require.NoError(t, err)
	out, err = runMetaCommand(t, endpoint, kb.Prefix(), "meta", "check", "a")
	require.Equal(t, 9, wperrors.ExitCodeFor(err))
	require.Contains(t, out, `"state": "invalid"`)
	require.Contains(t, out, "invalid log metadata")
	_, err = runMetaCommand(t, endpoint, kb.Prefix(), "meta", "log", "show", "missing")
	require.Equal(t, 3, wperrors.ExitCodeFor(err))
}

func TestMetaApplyRequiresGuardsAndFreshBackup(t *testing.T) {
	cli := startTestEtcd(t)
	members, err := cli.MemberList(context.Background())
	require.NoError(t, err)
	require.Len(t, members.Members, 1)
	require.NotEmpty(t, members.Members[0].ClientURLs)
	endpoint := members.Members[0].ClientURLs[0]
	kb := meta.NewKeyBuilder("meta-apply")
	oldGlobals, oldWriter := Globals, sourceWriter
	t.Cleanup(func() { Globals, sourceWriter = oldGlobals, oldWriter })
	key := kb.BuildLogKey("a")
	raw, err := pb.Marshal(&proto.LogMeta{LogId: 1, TruncatedSegmentId: -1, TruncatedEntryId: -1})
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), key, string(raw))
	require.NoError(t, err)
	before, err := metaGet(cli, key)
	require.NoError(t, err)
	patchFile := filepath.Join(t.TempDir(), "patch.json")
	require.NoError(t, os.WriteFile(patchFile, []byte(`{"max_compaction_file_count":2}`), 0o600))
	backup := filepath.Join(t.TempDir(), "before.json")
	base := []string{"meta", "log", "patch", "a", "--patch-file", patchFile}
	for _, flags := range [][]string{
		{},
		{"--apply"},
		{"--apply", "--offline", "--expected-revision", fmt.Sprint(before.ModRevision), "--reason", "test"},
	} {
		_, err = runMetaCommand(t, endpoint, kb.Prefix(), append(append([]string{}, base...), flags...)...)
		if len(flags) == 0 {
			require.NoError(t, err, "preview is allowed without apply guards")
		} else {
			require.Equal(t, 2, wperrors.ExitCodeFor(err))
		}
		after, getErr := metaGet(cli, key)
		require.NoError(t, getErr)
		require.Equal(t, before.ModRevision, after.ModRevision)
		require.Equal(t, raw, after.Value)
	}
	flags := []string{"--apply", "--offline", "--expected-revision", fmt.Sprint(before.ModRevision), "--reason", "reviewed repair", "--backup", backup}
	_, err = runMetaCommand(t, endpoint, kb.Prefix(), append(append([]string{}, base...), flags...)...)
	require.NoError(t, err)
	require.FileExists(t, backup)
	after, err := metaGet(cli, key)
	require.NoError(t, err)
	data, err := os.ReadFile(backup)
	require.NoError(t, err)
	// Even with a fresh revision, reusing a backup path must fail without writing.
	flags[3] = fmt.Sprint(after.ModRevision)
	_, err = runMetaCommand(t, endpoint, kb.Prefix(), append(append([]string{}, base...), flags...)...)
	require.Equal(t, 12, wperrors.ExitCodeFor(err))
	unchanged, err := metaGet(cli, key)
	require.NoError(t, err)
	require.Equal(t, after.ModRevision, unchanged.ModRevision)
	currentBackup, err := os.ReadFile(backup)
	require.NoError(t, err)
	require.Equal(t, data, currentBackup)
}

func TestMetaRejectsInvalidSelectorsBeforeDial(t *testing.T) {
	oldGlobals, oldWriter := Globals, sourceWriter
	t.Cleanup(func() { Globals, sourceWriter = oldGlobals, oldWriter })
	for _, args := range [][]string{
		{"meta", "log", "show", "a/b"},
		{"meta", "segment", "list", "a/b"},
		{"meta", "segment", "show", "a", "invalid"},
		{"meta", "quorum", "show", "invalid"},
		{"meta", "check", "a/b"},
		{"meta", "export", "--output-file", ""},
	} {
		_, err := runMetaCommand(t, "127.0.0.1:1", "meta-test", args...)
		require.Equal(t, 2, wperrors.ExitCodeFor(err), strings.Join(args, " "))
	}
	require.Equal(t, 2, wperrors.ExitCodeFor(validateMetaTarget("segment", []string{"a", "-1"})))
}

// All entry points must enforce apply guards before any metadata access.
func TestMetaApplyGuardsAtTransactionEntry(t *testing.T) {
	err := runMetaPatch(nil, nil, nil, "log", "a", "key", nil, metaPatchOptions{apply: true})
	require.Equal(t, 2, wperrors.ExitCodeFor(err))
}

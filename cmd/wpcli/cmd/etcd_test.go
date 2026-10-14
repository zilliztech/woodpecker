package cmd

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func TestEtcdRawReadAndPagination(t *testing.T) {
	cli := startTestEtcd(t)
	ctx := context.Background()
	_, err := cli.Put(ctx, "p/a", "hello")
	require.NoError(t, err)
	_, err = cli.Put(ctx, "p/b", string([]byte{0, 255, 1}))
	require.NoError(t, err)
	_, err = cli.Put(ctx, "other/key", "outside")
	require.NoError(t, err)
	cmd := &cobra.Command{}
	out := new(bytes.Buffer)
	cmd.SetOut(out)
	cmd.SetErr(new(bytes.Buffer))
	oldOutput := Globals.Output
	Globals.Output = "json"
	t.Cleanup(func() { Globals.Output = oldOutput })
	require.NoError(t, runEtcdList(cmd, cli, "p/", "", 1, 0))
	var page struct {
		Keys     []etcdKeyRecord `json:"keys"`
		More     bool            `json:"more"`
		Next     string          `json:"next_key"`
		Revision int64           `json:"snapshot_revision"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &page))
	require.Len(t, page.Keys, 1)
	require.True(t, page.More)
	require.Equal(t, "p/a", page.Next)
	_, err = cli.Put(ctx, "p/aa", "new after snapshot")
	require.NoError(t, err)
	out.Reset()
	require.NoError(t, runEtcdList(cmd, cli, "p/", page.Next, 1, page.Revision))
	require.NoError(t, json.Unmarshal(out.Bytes(), &page))
	require.Len(t, page.Keys, 1)
	require.Equal(t, "p/b", page.Keys[0].Key)
	require.False(t, page.More)
	out.Reset()
	require.NoError(t, runEtcdList(cmd, cli, "", "", 100, 0))
	require.Contains(t, out.String(), "other/key")
	out.Reset()
	require.NoError(t, runEtcdValue(cmd, cli, "p/b", 0, "auto", false))
	require.Contains(t, out.String(), "base64")
	require.Contains(t, out.String(), base64.StdEncoding.EncodeToString([]byte{0, 255, 1}))
	out.Reset()
	require.NoError(t, runEtcdValue(cmd, cli, "p/b", 0, "raw", true))
	require.Equal(t, []byte{0, 255, 1}, out.Bytes())
	out.Reset()
	require.NoError(t, runEtcdValue(cmd, cli, "p/a", 0, "text", true))
	require.Equal(t, "hello\n", out.String())
	require.ErrorContains(t, runEtcdValue(cmd, cli, "missing", 0, "auto", false), "TargetNotFound")
	_, _, err = encodeEtcdValue([]byte{255}, "text")
	require.Error(t, err)
}

func TestEtcdNoMetaPrefixOrAdminRequired(t *testing.T) {
	t.Setenv("WOODPECKER_CLI_CONFIG", t.TempDir()+"/absent.yaml")
	// Explicit endpoint and context must not need HTTP admin discovery or a metadata prefix.
	flags := metaEtcdFlags{etcdEndpoints: "127.0.0.1:32379"}
	conn, err := resolveEtcd(&flags, false)
	require.NoError(t, err)
	require.Equal(t, []string{"127.0.0.1:32379"}, conn.endpoints)
}

func TestEtcdCommandsUseContextWithoutAdmin(t *testing.T) {
	cli := startTestEtcd(t)
	members, err := cli.MemberList(context.Background())
	require.NoError(t, err)
	require.Len(t, members.Members, 1)
	require.NotEmpty(t, members.Members[0].ClientURLs)
	endpoint := members.Members[0].ClientURLs[0]
	_, err = cli.Put(context.Background(), "literal/key", "hello")
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "cli.yaml")
	data := fmt.Sprintf("current-context: raw\ncontexts:\n  raw:\n    etcd:\n      endpoints: [%q]\n      meta_prefix: ignored/woodpecker\n  override:\n    etcd:\n      endpoints: [\"127.0.0.1:1\"]\n", endpoint)
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))
	t.Setenv("WOODPECKER_CLI_CONFIG", path)
	oldGlobals, oldWriter := Globals, sourceWriter
	t.Cleanup(func() { Globals, sourceWriter = oldGlobals, oldWriter })
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"list", []string{"etcd", "list", "literal/", "-o", "json"}, "literal/key"},
		{"get", []string{"etcd", "get", "literal/key"}, "hello"},
		{"yaml", []string{"etcd", "get", "literal/key", "-o", "yaml"}, "hello"},
		{"cat", []string{"etcd", "cat", "literal/key", "--encoding", "raw"}, "hello"},
		{"endpoint override", []string{"--context", "override", "etcd", "get", "literal/key", "--etcd", endpoint}, "hello"},
		{"existing metadata command", []string{"marking", "list", "--all-states", "-o", "json"}, "[]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := NewRootCommand()
			var out, diagnostic bytes.Buffer
			root.SetOut(&out)
			root.SetErr(&diagnostic)
			root.SetArgs(append(tc.args, "--timeout", "2s"))
			require.NoError(t, root.Execute(), diagnostic.String())
			require.Contains(t, out.String(), tc.want)
			if tc.name == "cat" {
				require.Equal(t, "hello", out.String(), "raw cat adds no newline or preamble")
			} else if tc.name == "yaml" {
				var report map[string]any
				require.NoError(t, yaml.Unmarshal(out.Bytes(), &report))
			} else {
				require.True(t, json.Valid(out.Bytes()), out.String())
			}
		})
	}
}

func TestEtcdBinaryPaginationCursor(t *testing.T) {
	cli := startTestEtcd(t)
	members, err := cli.MemberList(context.Background())
	require.NoError(t, err)
	require.Len(t, members.Members, 1)
	require.NotEmpty(t, members.Members[0].ClientURLs)
	endpoint := members.Members[0].ClientURLs[0]
	configPath := filepath.Join(t.TempDir(), "cli.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte("current-context: test\ncontexts:\n  test: {}\n"), 0o600))
	t.Setenv("WOODPECKER_CLI_CONFIG", configPath)
	firstKey := string([]byte{'p', '/', 0xff})
	_, err = cli.Put(context.Background(), firstKey, "first")
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), firstKey+"tail", "second")
	require.NoError(t, err)
	old := Globals
	t.Cleanup(func() { Globals = old })
	Globals.Output = "json"
	cmd := &cobra.Command{}
	var out bytes.Buffer
	cmd.SetOut(&out)
	require.NoError(t, runEtcdList(cmd, cli, "p/", "", 1, 0))
	var page struct {
		Next     string          `json:"next_key_base64"`
		Revision int64           `json:"snapshot_revision"`
		Keys     []etcdKeyRecord `json:"keys"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &page))
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte(firstKey)), page.Next)
	root := NewRootCommand()
	out.Reset()
	root.SetOut(&out)
	root.SetErr(new(bytes.Buffer))
	root.SetArgs([]string{"etcd", "list", "p/", "--etcd", endpoint, "--after-key-base64", page.Next, "--revision", fmt.Sprint(page.Revision), "-o", "json"})
	require.NoError(t, root.Execute())
	require.NoError(t, json.Unmarshal(out.Bytes(), &page))
	require.Len(t, page.Keys, 1)
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte(firstKey+"tail")), page.Keys[0].KeyBase64)
}

func TestEtcdRejectsInvalidOptionsBeforeDial(t *testing.T) {
	old := Globals
	t.Cleanup(func() { Globals = old })
	for _, args := range [][]string{
		{"etcd", "list", "--limit", "0"},
		{"etcd", "list", "--revision", "-1"},
		{"etcd", "list", "p/", "--after-key", "q/a"},
		{"etcd", "list", "--after-key-base64", "invalid!"},
		{"etcd", "list", "--after-key", "a", "--after-key-base64", "YQ=="},
		{"etcd", "get", ""},
		{"etcd", "get", "key", "--encoding", "raw"},
		{"etcd", "cat", "key", "--encoding", "invalid"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			root := NewRootCommand()
			root.SetOut(new(bytes.Buffer))
			root.SetErr(new(bytes.Buffer))
			root.SetArgs(args)
			err := root.Execute()
			require.Error(t, err)
			require.Equal(t, 2, wperrors.ExitCodeFor(err))
		})
	}
}

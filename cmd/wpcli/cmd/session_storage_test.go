package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/cmd/wpcli/config"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

func TestSessionStorageReachesAdminRequests(t *testing.T) {
	etcd, endpoint := startShellTestEtcd(t)
	kb := meta.NewKeyBuilder("tenant/wp")
	for key, value := range map[string]pb.Message{
		kb.BuildLogKey("mylog"): &proto.LogMeta{LogId: 7, TruncatedSegmentId: -1},
		kb.BuildSegmentInstanceKey("mylog", "0"): &proto.SegmentMetadata{
			SegNo: 0, State: proto.SegmentState_Completed, LastEntryId: 1,
			Quorum: &proto.QuorumInfo{Id: 1, Es: 1, Wq: 1, Aq: 1, Nodes: []string{"node:18080"}},
		},
	} {
		data, err := pb.Marshal(value)
		require.NoError(t, err)
		_, err = etcd.Put(context.Background(), key, string(data))
		require.NoError(t, err)
	}
	queries := make(chan url.Values, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/admin/memberlist" {
			_, _ = io.WriteString(w, `{"members":[{"id":"node-1","service_addr":"node:18080"}]}`)
			return
		}
		queries <- r.URL.Query()
		_, _ = io.WriteString(w, `{"node_id":"node-1","instances":[],"outcome":"entry_not_found","first_entry":0,"last_entry":1,"survey":{"blocks":[{"block":0,"first_entry_id":0,"last_entry_id":1,"status":"ok"}],"stop_reason":"end_of_segment"}}`)
	}))
	defer server.Close()
	withSessionJSON(t, fmt.Sprintf(`{"schema_version":1,"service_context":{"endpoint":%q,"node_admin_urls":{"node:18080":%q}},"client_meta":{"endpoints":[%q],"meta_prefix":"tenant/wp"},"client_storage":{"bucket":"instance-bucket","root_path":"instance/root"}}`, server.URL, server.URL, endpoint))
	for _, args := range [][]string{
		{"instance", "data", "node-1"},
		{"node", "log-health", "node-1"},
		{"segment", "probe", "mylog", "0"},
		{"segment", "inspect", "mylog", "0"},
		{"log", "scan", "mylog"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			root := NewRootCommand()
			root.SetOut(io.Discard)
			root.SetErr(io.Discard)
			root.SetArgs(args)
			require.NoError(t, root.Execute())
			select {
			case query := <-queries:
				require.Equal(t, "instance-bucket", query.Get("bucket_name"))
				require.Equal(t, "instance/root", query.Get("root_path"))
			default:
				t.Fatal("command never reached the Admin endpoint")
			}
			require.Empty(t, queries)
		})
	}
}

// A newly registered command gets the same defaults without a command-name entry
// in the session loader. Explicit target changes never combine two instances.
func TestSessionStorageSharedDefaultsAndOverrides(t *testing.T) {
	withSessionJSON(t, `{"schema_version":1,"client_storage":{"bucket":"session-bucket","root_path":"session/root"}}`)
	for _, rootFlag := range []string{"root", "root-path"} {
		for _, tc := range []struct {
			name                    string
			args                    []string
			bucket, root, errorText string
		}{
			{name: "defaults", bucket: "session-bucket", root: "session/root"},
			{name: "explicit pair", args: []string{"--bucket=other", "--" + rootFlag + "=other/root"}, bucket: "other", root: "other/root"},
			{name: "partial pair", args: []string{"--bucket=other"}, errorText: "require both"},
			{name: "empty pair", args: []string{"--bucket=", "--" + rootFlag + "="}, errorText: "nonempty"},
			{name: "new metadata", args: []string{"--etcd=new:2379", "--meta-prefix=new/wp"}},
			{name: "new parameter file", args: []string{"--client-parameters-file=new.yaml"}},
			{name: "standalone", args: []string{"--standalone"}},
		} {
			t.Run(rootFlag+"/"+tc.name, func(t *testing.T) {
				root := NewRootCommand()
				var bucket, storageRoot string
				cmd := &cobra.Command{Use: "future-command", RunE: func(cmd *cobra.Command, _ []string) error {
					require.Equal(t, tc.bucket, bucket)
					require.Equal(t, tc.root, storageRoot)
					if len(tc.args) == 0 {
						require.False(t, cmd.Flags().Changed("bucket"))
						require.False(t, cmd.Flags().Changed(rootFlag))
					}
					return nil
				}}
				cmd.Flags().StringVar(&bucket, "bucket", "", "")
				cmd.Flags().StringVar(&storageRoot, rootFlag, "", "")
				var metadata metaEtcdFlags
				metadata.register(cmd)
				root.AddCommand(cmd)
				root.SetOut(io.Discard)
				root.SetErr(io.Discard)
				root.SetArgs(append([]string{"future-command"}, tc.args...))
				err := root.Execute()
				if tc.errorText != "" {
					require.ErrorContains(t, err, tc.errorText)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestSessionStorageInvalidAndMissingFile(t *testing.T) {
	for _, storage := range []string{`{"bucket":"private-value"}`, `{"root_path":"private-value"}`, `{"bucket":" ","root_path":"private-value"}`} {
		withSessionJSON(t, `{"schema_version":1,"client_storage":`+storage+`}`)
		root := NewRootCommand()
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		root.SetArgs([]string{"instance", "data", "--all"})
		err := root.Execute()
		require.ErrorContains(t, err, "client_storage requires")
		require.NotContains(t, err.Error(), "private-value")
	}
	t.Setenv(config.SessionConfigEnv, filepath.Join(t.TempDir(), "missing.json"))
	root := NewRootCommand()
	root.SetArgs([]string{"instance", "data", "--all"})
	require.ErrorContains(t, root.Execute(), config.SessionConfigEnv)
}

func TestSessionShellStorageOnlyStartup(t *testing.T) {
	withSessionJSON(t, `{"schema_version":1,"scope":{"milvus_instance_id":"client-a"},"client_storage":{"bucket":"instance-bucket","root_path":"instance/root"}}`)
	root := NewRootCommand()
	var out bytes.Buffer
	root.SetIn(strings.NewReader("status --check\ndisconnect\nstatus\nexit\n"))
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"shell"})
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), "instance-bucket")
	require.Contains(t, out.String(), "instance/root")
	require.Contains(t, out.String(), "Etcd check: skipped")
	require.Contains(t, out.String(), "Tenant metadata: not configured")
}

func TestSessionShellStorageDefaultsAndDetach(t *testing.T) {
	withSessionJSON(t, `{"schema_version":1,"client_storage":{"bucket":"stale-bucket","root_path":"stale/root"}}`)
	s, _ := newTestShell(t)
	s.sessionConfig = true
	s.tenant = &shellTenant{name: "client-a", bucket: "instance-bucket", rootPath: "instance/root"}
	s.runChild = func(_ context.Context, args []string, path string) error {
		require.Contains(t, args, "--bucket=instance-bucket")
		require.Contains(t, args, "--root=instance/root")
		require.NotContains(t, strings.Join(args, " "), "client-parameters-file")
		return nil
	}
	require.NoError(t, shellExecute(t, s, "instance", "data", "--all"))
	require.ErrorContains(t, shellExecute(t, s, "meta", "log", "list"), "no tenant metadata")
	require.ErrorContains(t, shellExecute(t, s, "instance", "data", "--all", "--bucket=other"), "require both")
	require.NoError(t, shellExecute(t, s, "disconnect"))
	s.runChild = func(_ context.Context, args []string, _ string) error {
		require.NotContains(t, strings.Join(args, " "), "--bucket")
		require.NotContains(t, strings.Join(args, " "), "--root")
		return nil
	}
	require.NoError(t, shellExecute(t, s, "instance", "data", "--all"))
}

func TestSessionShellConnectAttachesStorage(t *testing.T) {
	withSessionJSON(t, `{"schema_version":1,"client_meta":{"endpoints":["localhost:2379"],"meta_prefix":"tenant/wp"},"client_storage":{"bucket":"instance-bucket","root_path":"instance/root"}}`)
	s, _ := newTestShell(t)
	s.sessionConfig = true
	require.NoError(t, shellExecute(t, s, "connect", "client-a"))
	require.Equal(t, "instance-bucket", s.tenant.bucket)
	require.Equal(t, "instance/root", s.tenant.rootPath)
	s.runChild = func(_ context.Context, args []string, _ string) error {
		require.Contains(t, args, "--bucket=instance-bucket")
		require.Contains(t, args, "--root-path=instance/root")
		return nil
	}
	require.NoError(t, shellExecute(t, s, "segment", "inspect", "mylog", "0"))
	require.NoError(t, shellExecute(t, s, "connect", "client-b", "--etcd=other:2379", "--meta-prefix=other/wp"))
	require.Empty(t, s.tenant.bucket)
	require.Empty(t, s.tenant.rootPath)
}

func TestSessionShellPreservesProviderObjects(t *testing.T) {
	path := withSessionJSON(t, `{"schema_version":1,"service_context":{"endpoint":"http://service:9091","observability":{"loki":{"endpoint":"http://logs:3100","match_labels":{"namespace":"wp"},"tenant_env":"LOG_TENANT"},"future_source":{"nested":{"values":[1,2]}}}}}`)
	session, err := config.LoadSession(path)
	require.NoError(t, err)
	s, _ := newTestShell(t)
	s.service = *session.ServiceContext
	s.runChild = func(_ context.Context, _ []string, path string) error {
		f, err := config.Load(path)
		require.NoError(t, err)
		c, err := f.ResolveContext("")
		require.NoError(t, err)
		for source, expected := range session.ServiceContext.Observability.Connections {
			actual := c.Observability.Connections[source]
			var before, after map[string]any
			require.NoError(t, expected.Decode(&before))
			require.NoError(t, actual.Decode(&after))
			beforeJSON, err := json.Marshal(before)
			require.NoError(t, err)
			afterJSON, err := json.Marshal(after)
			require.NoError(t, err)
			require.JSONEq(t, string(beforeJSON), string(afterJSON))
		}
		return nil
	}
	require.NoError(t, shellExecute(t, s, "diag", "catalog"))
}

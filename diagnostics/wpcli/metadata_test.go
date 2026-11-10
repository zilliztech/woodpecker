package wpcli

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestReadMetadataUsesExistingReadOnlyCommands(t *testing.T) {
	for _, tc := range []struct {
		id, body, exit string
		valid          bool
	}{
		{"M01", `{"metadata":{"logId":"9007199254740993"}}`, "0", true},
		{"M02", `{"records":[]}`, "0", true},
		{"M03", `{"records":[]}`, "0", true},
		{"M04", `{"state":"ok","issues":[]}`, "0", true},
		{"M04", `{"state":"invalid","issues":["missing segment"]}`, "5", true},
		{"M04", `{"state":"ok","issues":[]}`, "5", false},
		{"M04", `{"state":"invalid","issues":[]}`, "0", false},
	} {
		t.Run(tc.id+tc.exit+tc.body, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "wp")
			script := "#!/bin/sh\ncase \"$*\" in *'-o json meta '*) ;; *) exit 11;; esac\ncase \"$*\" in *'--etcd http://configured --meta-prefix client/woodpecker'*) ;; *) exit 12;; esac\nprintf '%s\\n' '" + tc.body + "'\nexit " + tc.exit + "\n"
			require.NoError(t, os.WriteFile(path, []byte(script), 0o700))
			o, e := ReadMetadata(context.Background(), nativeConnection(t, path), MetadataRequest{ID: tc.id, LogName: "log 1", Etcd: "http://configured", Prefix: "client/woodpecker"}, 4096)
			if tc.valid {
				require.NoError(t, e)
				require.JSONEq(t, tc.body, string(o.Data))
				require.NotContains(t, o.Query.Expression, "http://configured")
			} else {
				require.Error(t, e)
			}
		})
	}
	_, e := ReadMetadata(context.Background(), nativeConnection(t, "wp"), MetadataRequest{ID: "M01", LogName: "--dangerous", Etcd: "http://configured", Prefix: "client/woodpecker"}, 4096)
	require.Error(t, e)
	_, e = ReadMetadata(context.Background(), nativeConnection(t, "wp"), MetadataRequest{ID: "repair", LogName: "l", Etcd: "http://configured", Prefix: "client/woodpecker"}, 4096)
	require.Error(t, e)
}

func TestMetadataStandaloneHasNoServiceContextAndQuorumsNeedNoLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wp")
	script := "#!/bin/sh\ncase \"$*\" in *--context*) exit 9;; esac\ncase \"$*\" in *'meta quorum list --standalone --etcd http://configured --meta-prefix client/woodpecker'*) ;; *) exit 10;; esac\nprintf '%s\\n' '{\"records\":[]}'\n"
	require.NoError(t, os.WriteFile(path, []byte(script), 0o700))
	conn := nativeConnection(t, path)
	var node yaml.Node
	require.NoError(t, node.Encode(map[string]any{"executable": path}))
	conn.Settings = node
	o, err := ReadMetadata(context.Background(), conn, MetadataRequest{ID: "M03", Etcd: "http://configured", Prefix: "client/woodpecker"}, 4096)
	require.NoError(t, err)
	require.JSONEq(t, `{"records":[]}`, string(o.Data))
	_, err = ReadMetadata(context.Background(), conn, MetadataRequest{ID: "M01", Etcd: "http://configured", Prefix: "client/woodpecker"}, 4096)
	require.Error(t, err)
}

func TestMetadataExplicitAuthorityIgnoresServerContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wp")
	script := "#!/bin/sh\ncase \"$*\" in *--context*|*--client-meta*) exit 9;; esac\ncase \"$*\" in *'meta quorum list --standalone --etcd http://tenant --meta-prefix tenant/woodpecker'*) ;; *) exit 10;; esac\nprintf '%s\\n' '{\"records\":[]}'\n"
	require.NoError(t, os.WriteFile(path, []byte(script), 0o700))
	o, err := ReadMetadata(context.Background(), nativeConnection(t, path), MetadataRequest{ID: "M03", Etcd: "http://tenant", Prefix: "tenant/woodpecker"}, 4096)
	require.NoError(t, err)
	require.JSONEq(t, `{"records":[]}`, string(o.Data))
}

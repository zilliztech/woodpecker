package cmd

import (
	"fmt"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/cmd/wpcli/candidates"
)

// Operational command tests must never add fixtures to the operator's cache.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "wpcli-candidate-tests-")
	if err != nil {
		panic(err)
	}
	_ = os.Setenv("WOODPECKER_RUN_CACHE_DIR", dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func TestCandidateRecordingOnlyAfterSuccessfulExecution(t *testing.T) {
	t.Setenv("WOODPECKER_RUN_CACHE_DIR", t.TempDir())
	old := Globals
	t.Cleanup(func() { Globals = old })
	run := func(fail bool) error {
		root := NewRootCommand()
		fixture := &cobra.Command{Use: "cache-fixture <logName>", RunE: func(*cobra.Command, []string) error {
			if fail {
				return fmt.Errorf("query failed")
			}
			return nil
		}}
		fixture.Flags().String("etcd", "", "")
		fixture.Flags().String("meta-prefix", "", "")
		fixture.Flags().String("etcd-password", "", "")
		root.AddCommand(fixture)
		root.SetArgs([]string{"cache-fixture", "a-log", "--etcd", "http://etcd:2379", "--meta-prefix", "tenant/wp", "--etcd-password", "never-cache-this"})
		return root.Execute()
	}
	require.Error(t, run(true))
	values, err := candidates.List()
	require.NoError(t, err)
	require.Empty(t, values)
	require.NoError(t, run(false))
	values, err = candidates.List()
	require.NoError(t, err)
	require.Equal(t, "tenant/wp", values["metadata.prefix"][0].Value)
	require.Equal(t, "a-log", values["log_name"][0].Value)
	require.Empty(t, values["etcd-password"])
}

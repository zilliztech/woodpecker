package candidates

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndependentCandidatesLimitForgetAndPermissions(t *testing.T) {
	t.Setenv("WOODPECKER_RUN_CACHE_DIR", t.TempDir())
	for i := 0; i < 8; i++ {
		require.NoError(t, Save(Values{"metadata.etcd": {fmt.Sprintf("http://etcd-%d:2379", i)}, "metadata.prefix": {fmt.Sprintf("tenant-%d/wp", i)}, "metadata.bucket": {"shared"}, "password": {"secret"}, "trace_endpoint": {"http://user:secret@trace:16686"}}))
	}
	values, err := List()
	require.NoError(t, err)
	require.Len(t, values["metadata.etcd"], 5)
	require.Len(t, values["metadata.prefix"], 5)
	require.Empty(t, values["password"])
	require.Empty(t, values["trace_endpoint"])
	first := values["metadata.etcd"][0].Value
	require.NoError(t, Forget("metadata.etcd", first))
	values, err = List()
	require.NoError(t, err)
	require.NotEqual(t, first, values["metadata.etcd"][0].Value)
	require.Equal(t, "tenant-7/wp", values["metadata.prefix"][0].Value)
	names, err := files(Directory())
	require.NoError(t, err)
	require.Len(t, names, 8)
	for _, path := range names {
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		body, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NotContains(t, string(body), "secret")
	}
	require.NoError(t, Save(Values{"metadata.etcd": {first}}))
	values, err = List()
	require.NoError(t, err)
	require.Equal(t, first, values["metadata.etcd"][0].Value)
}

func TestConcurrentWritesAndCorruptFile(t *testing.T) {
	t.Setenv("WOODPECKER_RUN_CACHE_DIR", t.TempDir())
	var wg sync.WaitGroup
	errors := make(chan error, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); errors <- Save(Values{"log_id": {fmt.Sprint(i)}}) }(i)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.NoError(t, os.WriteFile(filepath.Join(Directory(), "run-corrupt.json"), []byte("bad"), 0o600))
	values, err := List()
	require.NoError(t, err)
	require.Len(t, values["log_id"], 5)
}

func TestRecentConfigFileMatchesSuggestionsAndForget(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("WOODPECKER_RUN_CACHE_DIR", dir)
	require.NoError(t, Save(Values{"metadata.etcd": {"http://etcd:2379"}, "metadata.prefix": {"a/wp"}, "password": {"secret"}}))
	path := filepath.Join(dir, "recent-inputs.json")
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	var values Values
	require.NoError(t, json.Unmarshal(body, &values))
	require.Equal(t, []string{"a/wp"}, values["metadata.prefix"])
	require.Empty(t, values["password"])
	require.NoError(t, Forget("metadata.prefix", "a/wp"))
	body, err = os.ReadFile(path)
	require.NoError(t, err)
	require.NotContains(t, string(body), "a/wp")
	require.Contains(t, string(body), "http://etcd:2379")
	actual, preview, err := RecentConfig()
	require.NoError(t, err)
	require.Equal(t, path, actual)
	require.Equal(t, string(body), preview)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

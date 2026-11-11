package server

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactedMark_WriteHas_Idempotent(t *testing.T) {
	dir := t.TempDir()
	seg := filepath.Join(dir, "1", "2")
	require.NoError(t, os.MkdirAll(seg, 0o755))

	assert.False(t, hasCompactedMark(seg))
	created, err := writeCompactedMarkCreated(context.Background(), seg)
	require.NoError(t, err)
	assert.True(t, created)
	assert.True(t, hasCompactedMark(seg))
	// idempotent write
	created, err = writeCompactedMarkCreated(context.Background(), seg)
	require.NoError(t, err)
	assert.False(t, created)
	assert.True(t, hasCompactedMark(seg))
	assert.FileExists(t, filepath.Join(seg, compactedMarkFileName))
}

func TestCompactedMark_WriteCreatedConcurrent(t *testing.T) {
	dir := t.TempDir()
	seg := filepath.Join(dir, "1", "2")

	var createdCount atomic.Int32
	errCh := make(chan error, 16)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			created, err := writeCompactedMarkCreated(context.Background(), seg)
			if created {
				createdCount.Add(1)
			}
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
	assert.Equal(t, int32(1), createdCount.Load())
	assert.True(t, hasCompactedMark(seg))
}

// TestCompactedMark_WriteStatError covers writeCompactedMark's non-IsNotExist stat error
// branch: a path component that is a regular file yields ENOTDIR, which must surface as an
// error rather than being treated as "not marked yet".
func TestCompactedMark_WriteStatError(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o644))

	err := writeCompactedMark(context.Background(), filepath.Join(blocker, "seg"))
	assert.Error(t, err)
}

// TestCompactedMark_WriteMkdirError covers the MkdirAll error branch via a read-only parent.
func TestCompactedMark_WriteMkdirError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based test is meaningless as root")
	}
	dir := t.TempDir()
	ro := filepath.Join(dir, "ro")
	require.NoError(t, os.Mkdir(ro, 0o555))
	t.Cleanup(func() { _ = os.Chmod(ro, 0o755) })

	err := writeCompactedMark(context.Background(), filepath.Join(ro, "seg"))
	assert.Error(t, err)
}

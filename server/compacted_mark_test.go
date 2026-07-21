package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactedMark_WriteHas_Idempotent(t *testing.T) {
	dir := t.TempDir()
	seg := filepath.Join(dir, "1", "2")
	require.NoError(t, os.MkdirAll(seg, 0o755))

	assert.False(t, hasCompactedMark(seg))
	require.NoError(t, writeCompactedMark(context.Background(), seg))
	assert.True(t, hasCompactedMark(seg))
	// idempotent write
	require.NoError(t, writeCompactedMark(context.Background(), seg))
	assert.True(t, hasCompactedMark(seg))
	assert.FileExists(t, filepath.Join(seg, compactedMarkFileName))
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

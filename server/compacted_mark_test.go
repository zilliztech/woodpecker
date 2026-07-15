package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactedMark_WriteHasRemove_Idempotent(t *testing.T) {
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

	require.NoError(t, removeCompactedMark(context.Background(), seg))
	assert.False(t, hasCompactedMark(seg))
	// remove absent is fine
	require.NoError(t, removeCompactedMark(context.Background(), seg))
}

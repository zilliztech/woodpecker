package logger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestGetLevel_ReturnsValidLevel(t *testing.T) {
	level := GetLevel()
	assert.Contains(t, []string{"debug", "info", "warn", "error"}, level)
}

func TestSetLevel_Valid(t *testing.T) {
	prev := GetLevel()
	defer func() { _ = SetLevel(prev) }()

	require.NoError(t, SetLevel("debug"))
	assert.Equal(t, "debug", GetLevel())

	require.NoError(t, SetLevel("warn"))
	assert.Equal(t, "warn", GetLevel())

	require.NoError(t, SetLevel("error"))
	assert.Equal(t, "error", GetLevel())

	require.NoError(t, SetLevel("info"))
	assert.Equal(t, "info", GetLevel())
}

func TestSetLevel_Invalid(t *testing.T) {
	err := SetLevel("invalid")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid log level")
}

// TestSetLevel_AffectsGlobalLogger verifies that SetLevel flips the
// enabled level of the global logger instance without replacing it.
func TestSetLevel_AffectsGlobalLogger(t *testing.T) {
	prev := GetLevel()
	defer func() { _ = SetLevel(prev) }()

	require.NoError(t, SetLevel("debug"))
	assert.True(t, globalLogger().Core().Enabled(zap.DebugLevel))

	require.NoError(t, SetLevel("error"))
	assert.False(t, globalLogger().Core().Enabled(zap.DebugLevel))
	assert.True(t, globalLogger().Core().Enabled(zap.ErrorLevel))
}

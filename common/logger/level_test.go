package logger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetLevel_Default(t *testing.T) {
	// Before InitLogger, GetLevel should return "info" as default.
	level := GetLevel()
	assert.Equal(t, "info", level)
}

func TestSetLevel_Valid(t *testing.T) {
	// Save and restore current level.
	original := GetLevel()
	defer func() { _ = SetLevel(original) }()

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

func TestSetLevel_DoesNotAffectCtxLoggers(t *testing.T) {
	// Setting global level shouldn't break context-level overrides.
	original := GetLevel()
	defer func() { _ = SetLevel(original) }()

	require.NoError(t, SetLevel("debug"))
	// The debug logger should be accessible.
	assert.NotNil(t, debugLogger())
}

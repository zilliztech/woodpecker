package logger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetLevel_ReturnsValidLevel(t *testing.T) {
	level := GetLevel()
	assert.Contains(t, []string{"debug", "info", "warn", "error"}, level)
}

func TestSetLevel_Valid(t *testing.T) {
	// We must restore _globalLogger after SetLevel modifies it.
	// In test context, _globalLogger may be nil (InitLogger never called).
	// atomic.Value can't store nil, so we store warnLogger() as the safe fallback
	// matching the original Ctx() behavior when _globalLogger is nil.
	defer func() {
		_globalLogger.Store(warnLogger())
		_currentLevel.Store("warn")
	}()

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
	defer func() {
		_globalLogger.Store(warnLogger())
		_currentLevel.Store("warn")
	}()

	require.NoError(t, SetLevel("debug"))
	assert.NotNil(t, debugLogger())
}

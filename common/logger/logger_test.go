package logger

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/werr"
)

// TestNewLogger tests the NewLogger function
func TestNewLogger(t *testing.T) {
	tests := []struct {
		level    string
		expected zap.AtomicLevel
	}{
		{"debug", zap.NewAtomicLevelAt(zap.DebugLevel)},
		{"info", zap.NewAtomicLevelAt(zap.InfoLevel)},
		{"warn", zap.NewAtomicLevelAt(zap.WarnLevel)},
		{"error", zap.NewAtomicLevelAt(zap.ErrorLevel)},
		{"invalid", zap.NewAtomicLevelAt(zap.WarnLevel)}, // By default, NewLogger returns WarnLevel
	}

	for _, test := range tests {
		logger := Ctx(context.WithValue(context.Background(), "__LogLevel__", test.level))
		assert.True(t, logger.Core().Enabled(test.expected.Level()), fmt.Sprintf("level:%s should enable:%v", test.level, test.expected.Level()))
	}
}

// TestLoggerMethods tests the Logger methods
func TestLoggerMethods(t *testing.T) {
	logger := Ctx(context.WithValue(context.Background(), "__LogLevel__", "debug"))

	// Verify log recording using testify's assert
	assert.NotPanics(t, func() {
		logger.Debug("debug message", zap.String("key", "value"))
		logger.Info("info message", zap.String("key", "value"))
		logger.Warn("warn message", zap.String("key", "value"))
		logger.Error("error message", zap.Error(werr.ErrEntryNotFound))
	})
}

func TestLoggerMethodsWithContext(t *testing.T) {
	// Set the level explicitly
	logger := Ctx(context.WithValue(context.Background(), "__LogLevel__", "info"))
	assert.False(t, logger.Core().Enabled(zap.NewAtomicLevelAt(zap.DebugLevel).Level()))
	assert.True(t, logger.Core().Enabled(zap.NewAtomicLevelAt(zap.InfoLevel).Level()))
	assert.True(t, logger.Core().Enabled(zap.NewAtomicLevelAt(zap.WarnLevel).Level()))
	assert.True(t, logger.Core().Enabled(zap.NewAtomicLevelAt(zap.ErrorLevel).Level()))

	// default level is warn
	defaultLogger := Ctx(context.Background())
	assert.False(t, defaultLogger.Core().Enabled(zap.NewAtomicLevelAt(zap.DebugLevel).Level()))
	assert.False(t, defaultLogger.Core().Enabled(zap.NewAtomicLevelAt(zap.InfoLevel).Level()))
	assert.True(t, defaultLogger.Core().Enabled(zap.NewAtomicLevelAt(zap.WarnLevel).Level()))
	assert.True(t, defaultLogger.Core().Enabled(zap.NewAtomicLevelAt(zap.ErrorLevel).Level()))
}

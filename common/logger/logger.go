package logger

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/werr"
)

var (
	_globalLevelLogger sync.Map
)

func init() {
	levels := []string{
		"debug", "info", "warn", "error",
	}
	for _, level := range levels {
		levelLogger, err := newLogger(level)
		if err != nil {
			continue
		}
		_globalLevelLogger.Store(level, levelLogger)
	}
}

func debugLogger() *zap.Logger {
	v, _ := _globalLevelLogger.Load("debug")
	return v.(*zap.Logger)
}

func infoLogger() *zap.Logger {
	v, _ := _globalLevelLogger.Load("info")
	return v.(*zap.Logger)
}

func warnLogger() *zap.Logger {
	v, _ := _globalLevelLogger.Load("warn")
	return v.(*zap.Logger)
}

func errorLogger() *zap.Logger {
	v, _ := _globalLevelLogger.Load("error")
	return v.(*zap.Logger)
}

func Ctx(ctx context.Context) *zap.Logger {
	if ctx == nil {
		return debugLogger()
	}
	logger := ctx.Value("__Logger__")
	if logger != nil {
		return logger.(*zap.Logger)
	}
	level := ctx.Value("__LogLevel__")
	if level == nil {
		return warnLogger()
	}
	logger, ok := _globalLevelLogger.Load(level)
	if !ok {
		return warnLogger()
	}
	context.WithValue(ctx, "__Logger__", logger)
	return logger.(*zap.Logger)
}

// NewLogger creates a new logger with the specified log level
func newLogger(level string) (*zap.Logger, error) {
	var config zap.Config
	switch level {
	case "debug":
		config = zap.NewDevelopmentConfig()
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		config = zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		config = zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		config = zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		return nil, werr.ErrConfigError.WithCauseErrMsg(fmt.Sprintf("invalid log level: %s", level))
	}

	logger, err := config.Build(zap.AddCallerSkip(1))
	if err != nil {
		return nil, err
	}

	return logger, nil
}

// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logger

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/zilliztech/woodpecker/common/config"
)

type contextKey string

var (
	// _globalAtomicLevel is the shared level controller. All loggers — including
	// those cloned into contexts via WithFields — reference this atomic level,
	// so SetLevel takes effect immediately for every outstanding logger.
	_globalAtomicLevel = zap.NewAtomicLevelAt(zap.InfoLevel)
	_globalLogger      atomic.Value // stores *zap.Logger
	_currentLevel      atomic.Value // stores string: "debug", "info", "warn", "error"
	initLogOnce        sync.Once
	customEncoder      = "_WpCustomTextEncoder_"
	CtxLogKey          = contextKey("_WpLogger_")
	validLevels        = map[string]zapcore.Level{
		"debug": zap.DebugLevel,
		"info":  zap.InfoLevel,
		"warn":  zap.WarnLevel,
		"error": zap.ErrorLevel,
	}
)

func init() {
	encodeFormat := "text"
	registerErr := zap.RegisterEncoder(customEncoder, func(encoderConfig zapcore.EncoderConfig) (zapcore.Encoder, error) {
		return NewTextEncoderByConfig(encodeFormat), nil
	})
	if registerErr != nil {
		panic(registerErr)
	}
	l, err := newLogger(encodeFormat)
	if err != nil {
		panic(err)
	}
	_globalLogger.Store(l)
	_currentLevel.Store("info")
}

func InitLogger(cfg *config.Configuration) {
	initLogOnce.Do(func() {
		logLevel := cfg.Log.Level
		if len(logLevel) == 0 {
			logLevel = "info"
		}
		if lvl, ok := validLevels[logLevel]; ok {
			_globalAtomicLevel.SetLevel(lvl)
			_currentLevel.Store(logLevel)
		}
	})
}

// GetLevel returns the current global log level.
func GetLevel() string {
	v := _currentLevel.Load()
	if v == nil {
		return "info"
	}
	return v.(string)
}

// SetLevel changes the global log level at runtime.
// Valid levels: "debug", "info", "warn", "error".
// The change applies to every logger derived from the global logger,
// including ones previously stored in a context via WithFields.
func SetLevel(level string) error {
	lvl, ok := validLevels[level]
	if !ok {
		return fmt.Errorf("invalid log level %q: must be one of debug, info, warn, error", level)
	}
	_globalAtomicLevel.SetLevel(lvl)
	_currentLevel.Store(level)
	return nil
}

func globalLogger() *zap.Logger {
	return _globalLogger.Load().(*zap.Logger)
}

// ReplaceGlobals swaps the global logger for l and returns a function that
// restores the previous logger.
//
// FOR TESTS ONLY. It exists so tests can capture log output (e.g. via a
// zaptest/observer core), since Ctx/WithFields always derive from the global
// logger. Do not call this from production code.
func ReplaceGlobals(l *zap.Logger) func() {
	prev := _globalLogger.Load()
	_globalLogger.Store(l)
	return func() {
		if prev != nil {
			_globalLogger.Store(prev.(*zap.Logger))
		}
	}
}

func Ctx(ctx context.Context) *zap.Logger {
	if ctx == nil {
		return globalLogger()
	}
	if ctxLogger, ok := ctx.Value(CtxLogKey).(*zap.Logger); ok {
		return ctxLogger
	}
	return globalLogger()
}

// newLogger builds the single global logger whose level is driven by
// _globalAtomicLevel. Runtime level changes go through that atomic, not
// through constructing a new logger.
func newLogger(format string) (*zap.Logger, error) {
	// Use development config for all levels to get console-friendly output
	config := zap.NewDevelopmentConfig()
	config.Level = _globalAtomicLevel

	if format == "json" {
		config.Encoding = "json"
	} else if format == "text" {
		// use custom text encoder
		config.Encoding = customEncoder
	} else {
		// fallback to default console text encoder
		config.Encoding = "console"
	}

	// Configure encoder for better readability
	config.EncoderConfig.EncodeTime = customTimeEncoder
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder
	config.EncoderConfig.EncodeDuration = zapcore.StringDurationEncoder
	config.OutputPaths = []string{"stdout"}

	logger, err := config.Build()
	if err != nil {
		return nil, err
	}

	return logger, nil
}

func customTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006/01/02 15:04:05.000 -07:00")) // custom time format
}

// ============= logger with trace context ===========
func WithFields(ctx context.Context, fields ...zap.Field) context.Context {
	var zLogger *zap.Logger
	// get zap logger template
	if ctxLogger, ok := ctx.Value(CtxLogKey).(*zap.Logger); ok {
		zLogger = ctxLogger
	} else {
		zLogger = Ctx(ctx)
	}
	// clone a new logger with fields
	newZLogger := zLogger.With(fields...)
	// set it to context
	return context.WithValue(ctx, CtxLogKey, newZLogger)
}

// WithFieldsReplace replaces existing fields instead of accumulating them
func WithFieldsReplace(ctx context.Context, fields ...zap.Field) context.Context {
	// Always start from the base logger to avoid field accumulation
	baseLogger := Ctx(context.Background())
	newZLogger := baseLogger.With(fields...)
	return context.WithValue(ctx, CtxLogKey, newZLogger)
}

// NewIntentCtx creates a new context with intent information and returns it along with a span.
func NewIntentCtx(scopeName string, intent string) (context.Context, trace.Span) {
	return NewIntentCtxWithParent(context.Background(), scopeName, intent)
}

func NewIntentCtxWithParent(parent context.Context, scopeName string, intent string) (context.Context, trace.Span) {
	intentCtx, initSpan := otel.Tracer(scopeName).Start(parent, intent)
	// Use WithFieldsReplace to avoid accumulating duplicate scope/intent fields
	intentCtx = WithFieldsReplace(intentCtx,
		zap.String("scope", scopeName),
		zap.String("intent", intent),
		zap.String("traceID", initSpan.SpanContext().TraceID().String()))
	return intentCtx, initSpan
}

// SetupSpan add span into ctx values.
// Also setup logger in context with tracerID field.
func SetupSpan(ctx context.Context, span trace.Span) context.Context {
	ctx = trace.ContextWithSpan(ctx, span)
	ctx = WithFields(ctx, zap.Stringer("traceID", span.SpanContext().TraceID()))
	return ctx
}

// Propagate passes span context into a new ctx with different lifetime.
// Also setup logger in new context with traceID field.
func Propagate(ctx, newRoot context.Context) context.Context {
	spanCtx := trace.SpanContextFromContext(ctx)

	newCtx := trace.ContextWithSpanContext(newRoot, spanCtx)
	return WithFields(newCtx, zap.Stringer("traceID", spanCtx.TraceID()))
}

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
	"github.com/zilliztech/woodpecker/common/werr"
)

var (
	_globalLevelLogger sync.Map
	_globalLogger      atomic.Value
	initLogOnce        sync.Once
	customEncoder      = "_WpCustomTextEncoder_"
	CtxLogKey          = "_WpLogger_"
	CtxLogLevelKey     = "_WpLoggerLevel_"
)

func init() {
	encodeFormat := "text"
	registerErr := zap.RegisterEncoder(customEncoder, func(encoderConfig zapcore.EncoderConfig) (zapcore.Encoder, error) {
		return NewTextEncoderByConfig(encodeFormat), nil
	})
	if registerErr != nil {
		panic(registerErr)
	}
	levels := []string{
		"debug", "info", "warn", "error",
	}
	for _, level := range levels {
		levelLogger, err := newLogger(encodeFormat, level)
		if err != nil {
			continue
		}
		_globalLevelLogger.Store(level, levelLogger)
	}
}

func InitLogger(cfg *config.Configuration) {
	initLogOnce.Do(func() {
		logLevel := cfg.Log.Level
		if len(logLevel) == 0 {
			logLevel = "info"
		}
		v, _ := _globalLevelLogger.Load(logLevel)
		_globalLogger.Store(v)
	})
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
	logger := ctx.Value(CtxLogKey)
	if logger != nil {
		return logger.(*zap.Logger)
	}
	level := ctx.Value(CtxLogLevelKey)
	if level != nil {
		if l, ok := _globalLevelLogger.Load(level); ok {
			return l.(*zap.Logger)
		}
	}
	l := _globalLogger.Load()
	if l != nil {
		return l.(*zap.Logger)
	}
	return warnLogger()
}

// NewLogger creates a new logger with the specified log level
func newLogger(format string, level string) (*zap.Logger, error) {
	// Use development config for all levels to get console-friendly output
	config := zap.NewDevelopmentConfig()

	switch level {
	case "debug":
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		config.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		config.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		return nil, werr.ErrConfigError.WithCauseErrMsg(fmt.Sprintf("invalid log level: %s", level))
	}

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

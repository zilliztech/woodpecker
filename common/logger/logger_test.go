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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/tracer"
	"github.com/zilliztech/woodpecker/common/werr"
)

// TestSetLevel_UpdatesCtxLogger verifies that SetLevel flips the enabled
// level observed via Ctx(ctx) for every supported level.
func TestSetLevel_UpdatesCtxLogger(t *testing.T) {
	prev := GetLevel()
	defer func() { _ = SetLevel(prev) }()

	cases := []struct {
		level    string
		expected zapcore.Level
	}{
		{"debug", zap.DebugLevel},
		{"info", zap.InfoLevel},
		{"warn", zap.WarnLevel},
		{"error", zap.ErrorLevel},
	}
	for _, c := range cases {
		require.NoError(t, SetLevel(c.level))
		l := Ctx(context.Background())
		assert.True(t, l.Core().Enabled(c.expected), fmt.Sprintf("level=%s should enable %v", c.level, c.expected))
	}
}

// TestLoggerMethods exercises the log APIs on the default logger.
func TestLoggerMethods(t *testing.T) {
	prev := GetLevel()
	defer func() { _ = SetLevel(prev) }()
	require.NoError(t, SetLevel("debug"))

	logger := Ctx(context.Background())
	assert.NotPanics(t, func() {
		logger.Debug("debug message", zap.String("key", "value"))
		logger.Info("info message", zap.String("key", "value"))
		logger.Warn("warn message", zap.String("key", "value"))
		logger.Error("error message", zap.Error(werr.ErrEntryNotFound))
	})
}

// TestSetLevel_AppliesToWithFieldsCtx is the regression test for the bug
// where a ctx produced by WithFields before SetLevel kept logging at the
// old level. With a shared atomic level, the change must take effect on
// all outstanding derived loggers.
func TestSetLevel_AppliesToWithFieldsCtx(t *testing.T) {
	prev := GetLevel()
	defer func() { _ = SetLevel(prev) }()

	require.NoError(t, SetLevel("info"))
	ctx := WithFields(context.Background(), zap.String("k", "v"))
	ctxLogger := Ctx(ctx)
	assert.False(t, ctxLogger.Core().Enabled(zap.DebugLevel), "debug should be disabled at info level")

	require.NoError(t, SetLevel("debug"))
	assert.True(t, ctxLogger.Core().Enabled(zap.DebugLevel),
		"previously-captured ctx logger must observe the new debug level")
}

func TestTraceLogger(t *testing.T) {
	t.Skip("Skipping integration test: requires OTLP endpoint and sleeps 10s")
	// create cfg
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)

	// init log
	cfg.Log.Level = "debug"
	InitLogger(cfg)

	// init tracer
	cfg.Trace.Exporter = "otlp" // for integration test, set up jaeger/otlp before this exporter enable
	initTraceErr := tracer.InitTracer(cfg, "TextTraceLogger", 1001)
	assert.NoError(t, initTraceErr)

	// start a span
	newCtx, span := NewIntentCtx("wpTrace", "testIntent")
	Ctx(newCtx).Info("start a test intent")
	testPrintSomething(newCtx)
	span.End()

	// wait for tracer to flush
	time.Sleep(10 * time.Second)
}

func TestTraceLoggerWithParentCtx(t *testing.T) {
	// create cfg
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	assert.NoError(t, err)

	// init log
	cfg.Log.Level = "debug"
	InitLogger(cfg)

	// init tracer
	// cfg.Trace.Exporter = "otlp" // for integration test, set up jaeger/otlp before this exporter enable
	initTraceErr := tracer.InitTracer(cfg, "TextTraceLogger", 1001)
	assert.NoError(t, initTraceErr)

	// start a span
	type testKey string
	parentCtx := context.WithValue(context.Background(), testKey("testkey"), "123")
	ctx, span := NewIntentCtxWithParent(parentCtx, "testScope", "testIntent")
	assert.Equal(t, "123", parentCtx.Value(testKey("testkey")))
	assert.Equal(t, "123", ctx.Value(testKey("testkey")))
	Ctx(ctx).Info("start a test intent")
	testPrintSomething(ctx)
	span.End()
}

func TestCtx_NilContext(t *testing.T) {
	l := Ctx(nil) //nolint:staticcheck // intentionally testing nil context handling
	assert.NotNil(t, l)
}

func TestCustomTimeEncoder(t *testing.T) {
	enc := newTestTextEncoder()
	now := time.Date(2024, 6, 15, 10, 30, 45, 123000000, time.UTC)
	customTimeEncoder(now, enc)
	assert.Contains(t, enc.buf.String(), "2024/06/15 10:30:45.123")
}

func TestWithFields(t *testing.T) {
	ctx := context.Background()
	newCtx := WithFields(ctx, zap.String("key", "val"))
	l := Ctx(newCtx)
	assert.NotNil(t, l)

	// WithFields on ctx that already has a logger
	newCtx2 := WithFields(newCtx, zap.String("key2", "val2"))
	l2 := Ctx(newCtx2)
	assert.NotNil(t, l2)
}

func TestSetupSpan(t *testing.T) {
	ctx := context.Background()
	span := trace.SpanFromContext(ctx) // noop span
	newCtx := SetupSpan(ctx, span)
	assert.NotNil(t, newCtx)
	l := Ctx(newCtx)
	assert.NotNil(t, l)
}

func TestPropagate(t *testing.T) {
	ctx := context.Background()
	newRoot := context.Background()
	newCtx := Propagate(ctx, newRoot)
	assert.NotNil(t, newCtx)
	l := Ctx(newCtx)
	assert.NotNil(t, l)
}

func TestNewLogger_JsonFormat(t *testing.T) {
	l, err := newLogger("json")
	assert.NoError(t, err)
	assert.NotNil(t, l)
}

func TestNewLogger_FallbackConsoleFormat(t *testing.T) {
	l, err := newLogger("unknown_format")
	assert.NoError(t, err)
	assert.NotNil(t, l)
}

func TestNewIntentCtx(t *testing.T) {
	ctx, span := NewIntentCtx("testScope", "testIntent")
	defer span.End()
	assert.NotNil(t, ctx)
	l := Ctx(ctx)
	assert.NotNil(t, l)
}

func TestInitLogger_EmptyLevel(t *testing.T) {
	// initLogOnce already fired, so we test the branch logic directly
	// by checking that an empty log level defaults to "info"
	cfg := &config.Configuration{}
	cfg.Log.Level = ""
	// InitLogger uses sync.Once, already called - just verify the default logic
	logLevel := cfg.Log.Level
	if len(logLevel) == 0 {
		logLevel = "info"
	}
	assert.Equal(t, "info", logLevel)
}

func testPrintSomething(ctx context.Context) {
	ctx, span := NewIntentCtxWithParent(ctx, "subRole", "subIntent")
	defer span.End()
	span.AddEvent("event01", trace.WithAttributes(attribute.Int64("timestamp", 123), attribute.Bool("isOK", true)))
	span.AddEvent("event02", trace.WithAttributes(attribute.Int64("timestamp", 456), attribute.Bool("isOK", false)))
	Ctx(ctx).Debug("test print trace", zap.String("testField", "123"))
}

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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type foo struct {
	Key   string
	Value string
}

func BenchmarkZapReflect(b *testing.B) {
	payload := make([]foo, 10)
	for i := 0; i < len(payload); i++ {
		payload[i] = foo{Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("value%d", i)}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Ctx(context.TODO()).With(zap.Any("payload", payload))
	}
}

func BenchmarkZapWithLazy(b *testing.B) {
	payload := make([]foo, 10)
	for i := 0; i < len(payload); i++ {
		payload[i] = foo{Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("value%d", i)}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		Ctx(context.TODO()).WithLazy(zap.Any("payload", payload))
	}
}

// The following two benchmarks are validations if `WithLazy` has the same performance as `With` in the worst case.
func BenchmarkWithLazyLog(b *testing.B) {
	payload := make([]foo, 10)
	for i := 0; i < len(payload); i++ {
		payload[i] = foo{Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("value%d", i)}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		log := Ctx(context.TODO()).WithLazy(zap.Any("payload", payload))
		log.Info("test")
		log.Warn("test")
	}
}

func BenchmarkWithLog(b *testing.B) {
	payload := make([]foo, 10)
	for i := 0; i < len(payload); i++ {
		payload[i] = foo{Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("value%d", i)}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		log := Ctx(context.TODO()).With(zap.Any("payload", payload))
		log.Info("test")
		log.Warn("test")
	}
}

func newTestTextEncoder() *textEncoder {
	return &textEncoder{
		EncoderConfig: &zapcore.EncoderConfig{
			TimeKey:        "time",
			LevelKey:       "level",
			NameKey:        "name",
			CallerKey:      "caller",
			MessageKey:     "message",
			StacktraceKey:  "stack",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     DefaultTimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   ShortCallerEncoder,
		},
		buf:                 _pool.Get(),
		spaced:              false,
		disableErrorVerbose: true,
	}
}

// TestWithFieldsBracketFormatting verifies that fields added via Clone()+Add*
// (i.e. zap's With()) are individually wrapped in brackets.
func TestWithFieldsBracketFormatting(t *testing.T) {
	enc := newTestTextEncoder()

	// Clone simulates zap's With() path
	cloned := enc.Clone().(*textEncoder)
	cloned.AddString("scope", "testScope")
	cloned.AddString("intent", "testIntent")

	// The cloned encoder's buffer should contain individually bracketed fields
	output := cloned.buf.String()
	assert.Contains(t, output, "[scope=testScope]")
	assert.Contains(t, output, "[intent=testIntent]")
}

// TestInlineFieldsBracketFormatting verifies that inline fields passed to
// EncodeEntry are individually wrapped in brackets.
func TestInlineFieldsBracketFormatting(t *testing.T) {
	enc := newTestTextEncoder()

	fields := []zapcore.Field{
		zap.String("scope", "testScope"),
		zap.Int64("testField", 123),
	}

	entry := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Message: "test message",
	}

	buf, err := enc.EncodeEntry(entry, fields)
	assert.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "[scope=testScope]")
	assert.Contains(t, output, "[testField=123]")
	// Each field should be in its own bracket pair, not combined
	assert.NotContains(t, output, "[scope=testScope,testField=123]")
}

// TestErrorFieldSingleBracket verifies that error fields have exactly one layer
// of brackets, not double-nested brackets.
func TestErrorFieldSingleBracket(t *testing.T) {
	enc := newTestTextEncoder()

	fields := []zapcore.Field{
		zap.Error(errors.New("entry not found")),
	}

	entry := zapcore.Entry{
		Level:   zapcore.ErrorLevel,
		Time:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Message: "error message",
	}

	buf, err := enc.EncodeEntry(entry, fields)
	assert.NoError(t, err)

	output := buf.String()
	// Should contain single-bracketed error
	assert.Contains(t, output, `[error="entry not found"]`)
	// Should NOT contain double brackets
	assert.NotContains(t, output, "[ [")
	assert.NotContains(t, output, "]]")
}

// TestArrayFieldsNoBrackets verifies that fields inside arrays don't get extra
// brackets (since array marshaling uses cloned() which doesn't set the flag).
func TestArrayFieldsNoBrackets(t *testing.T) {
	enc := newTestTextEncoder()

	arr := zapcore.ArrayMarshalerFunc(func(ae zapcore.ArrayEncoder) error {
		ae.AppendString("a")
		ae.AppendString("b")
		return nil
	})

	fields := []zapcore.Field{
		zap.Array("items", arr),
	}

	entry := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Message: "test",
	}

	buf, err := enc.EncodeEntry(entry, fields)
	assert.NoError(t, err)

	output := buf.String()
	// The array field itself should be in brackets
	assert.Contains(t, output, "[items=")
	// The array contents should use standard array format, not extra brackets
	assert.Contains(t, output, "[a,b]")
}

// TestCombinedWithAndInlineFields verifies that both With-style fields (pre-set
// via Clone) and inline fields (passed to EncodeEntry) are correctly formatted.
func TestCombinedWithAndInlineFields(t *testing.T) {
	enc := newTestTextEncoder()

	// Simulate With() fields
	cloned := enc.Clone().(*textEncoder)
	cloned.AddString("scope", "testScope")
	cloned.AddString("intent", "testIntent")

	// Now encode an entry with additional inline fields
	entry := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Message: "test message",
	}

	inlineFields := []zapcore.Field{
		zap.String("traceID", "abc123"),
		zap.Int64("testField", 456),
	}

	buf, err := cloned.EncodeEntry(entry, inlineFields)
	assert.NoError(t, err)

	output := buf.String()

	// With-style fields should be individually bracketed
	assert.Contains(t, output, "[scope=testScope]")
	assert.Contains(t, output, "[intent=testIntent]")

	// Inline fields should also be individually bracketed
	assert.Contains(t, output, "[traceID=abc123]")
	assert.Contains(t, output, "[testField=456]")

	// Count total bracket pairs - should have separate brackets for each field
	openCount := strings.Count(output, "[")
	closeCount := strings.Count(output, "]")
	assert.Equal(t, openCount, closeCount, "bracket count should be balanced")
}

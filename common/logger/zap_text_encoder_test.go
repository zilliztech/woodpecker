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
	"math"
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

func TestNewTextEncoder(t *testing.T) {
	cc := zapcore.EncoderConfig{}
	enc := NewTextEncoder(&cc, true, false)
	assert.NotNil(t, enc)
}

func TestNewTextEncoderByConfig_JSON(t *testing.T) {
	enc := NewTextEncoderByConfig("json")
	assert.NotNil(t, enc)
}

func TestNewTextEncoderByConfig_Panic(t *testing.T) {
	assert.Panics(t, func() {
		NewTextEncoderByConfig("unknown")
	})
}

func TestTextEncoder_AllAddMethods(t *testing.T) {
	enc := newTestTextEncoder()

	enc.AddBinary("bin", []byte{0x01, 0x02})
	enc.AddByteString("bs", []byte("hello"))
	enc.AddBool("flag", true)
	enc.AddComplex128("c128", complex(1.0, 2.0))
	enc.AddComplex64("c64", complex64(complex(1.0, 2.0)))
	enc.AddDuration("dur", 5*time.Second)
	enc.AddFloat64("f64", 3.14)
	enc.AddFloat32("f32", 2.72)
	enc.AddInt("i", 42)
	enc.AddInt64("i64", 64)
	enc.AddInt32("i32", 32)
	enc.AddInt16("i16", 16)
	enc.AddInt8("i8", 8)
	enc.AddUint64("u64", 64)
	enc.AddUint("u", 42)
	enc.AddUint32("u32", 32)
	enc.AddUint16("u16", 16)
	enc.AddUint8("u8", 8)
	enc.AddUintptr("uptr", 0xdeadbeef)
	enc.AddString("str", "value")
	enc.AddTime("t", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	output := enc.buf.String()
	assert.Contains(t, output, "bin=")
	assert.Contains(t, output, "flag=true")
	assert.Contains(t, output, "str=value")
}

func TestTextEncoder_AllAppendMethods(t *testing.T) {
	enc := newTestTextEncoder()

	enc.AppendBool(true)
	enc.AppendComplex128(complex(1.0, 2.0))
	enc.AppendComplex64(complex64(complex(3.0, 4.0)))
	enc.AppendDuration(time.Second)
	enc.AppendFloat64(1.23)
	enc.AppendFloat32(4.56)
	enc.AppendInt(1)
	enc.AppendInt64(2)
	enc.AppendInt32(3)
	enc.AppendInt16(4)
	enc.AppendInt8(5)
	enc.AppendUint64(6)
	enc.AppendUint(7)
	enc.AppendUint32(8)
	enc.AppendUint16(9)
	enc.AppendUint8(10)
	enc.AppendUintptr(11)
	enc.AppendString("test")

	output := enc.buf.String()
	assert.Contains(t, output, "true")
	assert.Contains(t, output, "test")
}

func TestTextEncoder_AddObject(t *testing.T) {
	enc := newTestTextEncoder()
	obj := zapcore.ObjectMarshalerFunc(func(oe zapcore.ObjectEncoder) error {
		oe.AddString("inner", "val")
		return nil
	})
	err := enc.AddObject("obj", obj)
	assert.NoError(t, err)
	assert.Contains(t, enc.buf.String(), "obj=")
}

func TestTextEncoder_AppendObject(t *testing.T) {
	enc := newTestTextEncoder()
	obj := zapcore.ObjectMarshalerFunc(func(oe zapcore.ObjectEncoder) error {
		oe.AddString("k", "v")
		return nil
	})
	err := enc.AppendObject(obj)
	assert.NoError(t, err)
}

func TestTextEncoder_AddReflected(t *testing.T) {
	enc := newTestTextEncoder()
	err := enc.AddReflected("ref", map[string]int{"a": 1})
	assert.NoError(t, err)
	assert.Contains(t, enc.buf.String(), "ref=")
}

func TestTextEncoder_AppendReflected(t *testing.T) {
	enc := newTestTextEncoder()
	err := enc.AppendReflected(42)
	assert.NoError(t, err)
	assert.Contains(t, enc.buf.String(), "42")
}

func TestTextEncoder_OpenNamespace(t *testing.T) {
	enc := newTestTextEncoder()
	enc.OpenNamespace("ns")
	assert.Contains(t, enc.buf.String(), "ns={")
	assert.Equal(t, 1, enc.openNamespaces)
}

func TestTextEncoder_Truncate(t *testing.T) {
	enc := newTestTextEncoder()
	enc.AddString("key", "val")
	assert.True(t, enc.buf.Len() > 0)
	enc.truncate()
	assert.Equal(t, 0, enc.buf.Len())
}

func TestTextEncoder_AppendFloat_SpecialValues(t *testing.T) {
	enc := newTestTextEncoder()
	enc.appendFloat(math.NaN(), 64)
	enc.appendFloat(math.Inf(1), 64)
	enc.appendFloat(math.Inf(-1), 64)
	enc.appendFloat(1.5, 64)
	output := enc.buf.String()
	assert.Contains(t, output, "NaN")
	assert.Contains(t, output, "+Inf")
	assert.Contains(t, output, "-Inf")
}

func TestTextEncoder_SafeAddString_EscapeChars(t *testing.T) {
	enc := newTestTextEncoder()
	enc.safeAddString("hello\nworld\t\"quoted\"\\back")
	output := enc.buf.String()
	assert.Contains(t, output, "\\n")
	assert.Contains(t, output, "\\t")
	assert.Contains(t, output, "\\\"")
	assert.Contains(t, output, "\\\\")
}

func TestTextEncoder_SafeAddString_ControlChar(t *testing.T) {
	enc := newTestTextEncoder()
	enc.safeAddString(string([]byte{0x01})) // control char
	output := enc.buf.String()
	assert.Contains(t, output, "\\u00")
}

func TestTextEncoder_SafeAddString_MultibyteUTF8(t *testing.T) {
	enc := newTestTextEncoder()
	enc.safeAddString("café")
	assert.Contains(t, enc.buf.String(), "caf")
}

func TestTextEncoder_SafeAddByteString_InvalidUTF8(t *testing.T) {
	enc := newTestTextEncoder()
	enc.safeAddByteString([]byte{0xff}) // invalid UTF-8
	assert.Contains(t, enc.buf.String(), "\\ufffd")
}

func TestTextEncoder_DefaultTimeEncoder_NonTextEncoder(t *testing.T) {
	// Test the branch where enc is NOT a *textEncoder
	arr := make(appendStringArr, 0)
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	DefaultTimeEncoder(now, &arr)
	assert.Contains(t, arr[0], "2024/01/01")
}

type appendStringArr []string

func (a *appendStringArr) AppendBool(v bool)              {}
func (a *appendStringArr) AppendByteString(v []byte)      {}
func (a *appendStringArr) AppendComplex128(v complex128)  {}
func (a *appendStringArr) AppendComplex64(v complex64)    {}
func (a *appendStringArr) AppendFloat64(v float64)        {}
func (a *appendStringArr) AppendFloat32(v float32)        {}
func (a *appendStringArr) AppendInt(v int)                {}
func (a *appendStringArr) AppendInt64(v int64)            {}
func (a *appendStringArr) AppendInt32(v int32)            {}
func (a *appendStringArr) AppendInt16(v int16)            {}
func (a *appendStringArr) AppendInt8(v int8)              {}
func (a *appendStringArr) AppendString(v string)          { *a = append(*a, v) }
func (a *appendStringArr) AppendUint(v uint)              {}
func (a *appendStringArr) AppendUint64(v uint64)          {}
func (a *appendStringArr) AppendUint32(v uint32)          {}
func (a *appendStringArr) AppendUint16(v uint16)          {}
func (a *appendStringArr) AppendUint8(v uint8)            {}
func (a *appendStringArr) AppendUintptr(v uintptr)        {}
func (a *appendStringArr) AppendDuration(v time.Duration) {}
func (a *appendStringArr) AppendTime(v time.Time)         {}

func TestTextEncoder_EncodeEntry_WithNameAndCaller(t *testing.T) {
	enc := newTestTextEncoder()
	entry := zapcore.Entry{
		Level:      zapcore.InfoLevel,
		Time:       time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Message:    "msg",
		LoggerName: "mylogger",
		Caller:     zapcore.EntryCaller{Defined: true, File: "test.go", Line: 42},
		Stack:      "goroutine 1\n",
	}
	buf, err := enc.EncodeEntry(entry, nil)
	assert.NoError(t, err)
	output := buf.String()
	assert.Contains(t, output, "mylogger")
	assert.Contains(t, output, "test.go:42")
	assert.Contains(t, output, "goroutine 1")
}

func TestTextEncoder_EncodeEntry_EmptyLineEnding(t *testing.T) {
	enc := newTestTextEncoder()
	enc.LineEnding = ""
	entry := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Message: "msg",
	}
	buf, err := enc.EncodeEntry(entry, nil)
	assert.NoError(t, err)
	assert.True(t, strings.HasSuffix(buf.String(), "\n"))
}

func TestTextEncoder_WrapFieldsInBrackets(t *testing.T) {
	enc := newTestTextEncoder()
	enc.wrapFieldsInBrackets = true

	enc.AddBool("flag", true)
	enc.AddByteString("bs", []byte("hi"))
	enc.AddComplex128("c", complex(1, 2))
	enc.AddDuration("d", time.Second)
	enc.AddFloat64("f", 1.0)
	enc.AddInt64("i", 1)
	enc.AddUint64("u", 1)
	enc.AddString("s", "v")
	enc.AddTime("t", time.Now())

	output := enc.buf.String()
	assert.Contains(t, output, "[flag=true]")
	assert.Contains(t, output, "[s=v]")
}

func TestTextEncoder_ErrorField_Verbose(t *testing.T) {
	enc := newTestTextEncoder()
	enc.disableErrorVerbose = false

	fields := []zapcore.Field{
		zap.Error(errors.New("simple error")),
	}
	entry := zapcore.Entry{
		Level:   zapcore.ErrorLevel,
		Time:    time.Now(),
		Message: "fail",
	}
	buf, err := enc.EncodeEntry(entry, fields)
	assert.NoError(t, err)
	assert.Contains(t, buf.String(), "simple error")
}

func TestPutTextEncoder_WithReflectBuf(t *testing.T) {
	enc := newTestTextEncoder()
	enc.resetReflectBuf()
	assert.NotNil(t, enc.reflectBuf)

	putTextEncoder(enc)
	// After put, fields should be reset — just verify no panic
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

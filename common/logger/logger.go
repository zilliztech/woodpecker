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

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
)

var (
	_globalLevelLogger sync.Map
	_globalLogger      atomic.Value
	initLogOnce        sync.Once
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
	logger := ctx.Value("__Logger__")
	if logger != nil {
		return logger.(*zap.Logger)
	}
	level := ctx.Value("__LogLevel__")
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
	// default text encoder
	config.Encoding = "console"
	config.EncoderConfig.EncodeTime = customTimeEncoder
	logger, err := config.Build()
	if err != nil {
		return nil, err
	}

	return logger, nil
}

func customTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05.000")) // custom time format
}

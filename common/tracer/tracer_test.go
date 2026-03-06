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

package tracer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/zilliztech/woodpecker/common/config"
)

func TestTracer_Init(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Trace.Exporter = "unknown"
	// init failed with unknown exporter
	err = Init(cfg, "TestTracer_Init", 1001)
	assert.Error(t, err)

	cfg.Trace.Exporter = "stdout"
	// init with stdout exporter
	err = Init(cfg, "TestTracer_Init", 1001)
	assert.NoError(t, err)

	cfg.Trace.Exporter = "noop"
	// init with noop exporter
	err = Init(cfg, "TestTracer_Init", 1001)
	assert.NoError(t, err)
}

func TestTracer_CloseProviderFailed(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Trace.Exporter = "stdout"
	// init with stdout exporter
	err = Init(cfg, "TestTracer_CloseProviderFailed", 2001)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = CloseTracerProvider(ctx)
	assert.Error(t, err)
}

func TestCloseTracerProvider_NoSDKProvider(t *testing.T) {
	// Save current provider and set a noop one (not *sdk.TracerProvider)
	origProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(noop.NewTracerProvider())
	defer otel.SetTracerProvider(origProvider)

	err := CloseTracerProvider(context.Background())
	assert.NoError(t, err)
}

func TestCreateTracerExporter_Jaeger(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Trace.Exporter = "jaeger"
	cfg.Trace.Jaeger.URL = "http://localhost:14268/api/traces"

	exp, err := CreateTracerExporter(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exp)
}

func TestCreateTracerExporter_OtlpGrpcDefault(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Trace.Exporter = "otlp"
	cfg.Trace.Otlp.Method = "" // default is grpc
	cfg.Trace.Otlp.Endpoint = "localhost:4317"
	cfg.Trace.Otlp.Secure = false

	exp, err := CreateTracerExporter(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exp)
}

func TestCreateTracerExporter_OtlpGrpcSecure(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Trace.Exporter = "otlp"
	cfg.Trace.Otlp.Method = "grpc"
	cfg.Trace.Otlp.Endpoint = "localhost:4317"
	cfg.Trace.Otlp.Secure = true

	exp, err := CreateTracerExporter(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exp)
}

func TestCreateTracerExporter_OtlpHttp(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Trace.Exporter = "otlp"
	cfg.Trace.Otlp.Method = "http"
	cfg.Trace.Otlp.Endpoint = "localhost:4318"
	cfg.Trace.Otlp.Secure = false

	exp, err := CreateTracerExporter(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exp)
}

func TestCreateTracerExporter_OtlpHttpSecure(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Trace.Exporter = "otlp"
	cfg.Trace.Otlp.Method = "http"
	cfg.Trace.Otlp.Endpoint = "localhost:4318"
	cfg.Trace.Otlp.Secure = true

	exp, err := CreateTracerExporter(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exp)
}

func TestCreateTracerExporter_OtlpUnsupportedMethod(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Trace.Exporter = "otlp"
	cfg.Trace.Otlp.Method = "websocket"

	exp, err := CreateTracerExporter(cfg)
	assert.Error(t, err)
	assert.Nil(t, exp)
	assert.Contains(t, err.Error(), "otlp method not supported")
}

func TestCreateTracerExporter_Noop(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Trace.Exporter = "noop"

	exp, err := CreateTracerExporter(cfg)
	assert.NoError(t, err)
	assert.Nil(t, exp)
}

func TestInitTracer(t *testing.T) {
	// InitTracer uses sync.Once — just verify it returns nil
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Trace.Exporter = "stdout"

	err = InitTracer(cfg, "TestInitTracer", 3001)
	assert.NoError(t, err)
}

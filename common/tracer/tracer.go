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

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	stdout "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"sync"

	"github.com/zilliztech/woodpecker/common/config"
)

var (
	initOnce sync.Once
)

func InitTracer(cfg *config.Configuration, serviceName string, nodeID int64) error {
	initOnce.Do(func() {
		err := Init(cfg, serviceName, nodeID)
		if err != nil {
			panic(err)
		}
	})
	return nil
}

func Init(cfg *config.Configuration, serviceName string, nodeID int64) error {
	exp, err := CreateTracerExporter(cfg)
	if err != nil {
		return err
	}

	SetTracerProvider(exp, cfg.Trace.SampleFraction, serviceName, nodeID)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return nil
}

func CloseTracerProvider(ctx context.Context) error {
	provider, ok := otel.GetTracerProvider().(*sdk.TracerProvider)
	if ok {
		err := provider.Shutdown(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func SetTracerProvider(exp sdk.SpanExporter, traceIDRatio float64, runtimeService string, runtimeNodeID int64) {
	tp := sdk.NewTracerProvider(
		sdk.WithBatcher(exp),
		sdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(runtimeService),
			attribute.Int64("NodeID", runtimeNodeID),
		)),
		sdk.WithSampler(sdk.ParentBased(
			sdk.TraceIDRatioBased(traceIDRatio),
		)),
	)
	otel.SetTracerProvider(tp)
}

func CreateTracerExporter(cfg *config.Configuration) (sdk.SpanExporter, error) {
	var exp sdk.SpanExporter
	var err error

	switch cfg.Trace.Exporter {
	case "jaeger":
		exp, err = jaeger.New(jaeger.WithCollectorEndpoint(
			jaeger.WithEndpoint(cfg.Trace.Jaeger.URL)))
	case "otlp":
		secure := cfg.Trace.Otlp.Secure
		switch cfg.Trace.Otlp.Method {
		case "", "grpc":
			opts := []otlptracegrpc.Option{
				otlptracegrpc.WithEndpoint(cfg.Trace.Otlp.Endpoint),
			}
			if !secure {
				opts = append(opts, otlptracegrpc.WithInsecure())
			}
			exp, err = otlptracegrpc.New(context.Background(), opts...)
		case "http":
			opts := []otlptracehttp.Option{
				otlptracehttp.WithEndpoint(cfg.Trace.Otlp.Endpoint),
			}
			if !secure {
				opts = append(opts, otlptracehttp.WithInsecure())
			}
			exp, err = otlptracehttp.New(context.Background(), opts...)
		default:
			return nil, errors.Newf("otlp method not supported: %s", cfg.Trace.Otlp.Method)
		}
	case "stdout":
		exp, err = stdout.New()
	case "noop":
		return nil, nil
	default:
		err = errors.New("Empty Trace")
	}

	return exp, err
}

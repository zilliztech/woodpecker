package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// Exercise the actual HTTP boundary: a recorded request root must be exported,
// and the receiver must extract that exact identity rather than a random anchor.
func TestRequestExportsTheInjectedParent(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	observed := make(chan trace.SpanContext, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := propagation.TraceContext{}.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		observed <- trace.SpanContextFromContext(ctx)
		_, _ = w.Write([]byte(`{"code":0,"data":{"insertCount":1}}`))
	}))
	defer server.Close()
	w := &worker{client: server.Client(), tracer: provider.Tracer("acceptance-test"), base: server.URL}
	_, err := w.call(context.Background(), "entities/insert", map[string]any{"data": []int{1}})
	require.NoError(t, err)
	parent := <-observed
	require.True(t, parent.IsValid())
	require.Len(t, recorder.Ended(), 1)
	span := recorder.Ended()[0]
	require.Equal(t, parent.TraceID(), span.SpanContext().TraceID())
	require.Equal(t, parent.SpanID(), span.SpanContext().SpanID())
	require.False(t, span.Parent().IsValid())
	require.Len(t, w.rows, 1)
	require.Equal(t, parent.TraceID().String(), w.rows[0].TraceID)
	require.True(t, w.rows[0].OK)
}

func TestFailedResponseRetainsAttemptWithoutLeakingBody(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("private backend details"))
	}))
	defer server.Close()
	w := &worker{client: server.Client(), tracer: provider.Tracer("acceptance-test"), base: server.URL}
	_, err := w.call(context.Background(), "entities/insert", nil)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "private backend details")
	require.Len(t, w.rows, 1)
	require.False(t, w.rows[0].OK)
	require.NotEmpty(t, strings.Trim(w.rows[0].TraceID, "0"))
	require.Len(t, recorder.Ended(), 1)
	span := recorder.Ended()[0]
	require.Equal(t, codes.Error, span.Status().Code)
	require.NotContains(t, span.Status().Description, "private backend details")
}

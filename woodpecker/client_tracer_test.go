package woodpecker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/tracer"
)

func TestClientNoopTracingPreservesHostInstrumentation(t *testing.T) {
	for _, constructor := range []string{"service", "embedded-from-config"} {
		t.Run(constructor, func(t *testing.T) {
			previous, propagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
			recorder := tracetest.NewSpanRecorder()
			host := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			otel.SetTracerProvider(host)
			// A host-defined propagator must not be replaced as a side effect of
			// constructing a client whose own exporter is explicitly disabled.
			otel.SetTextMapPropagator(propagation.Baggage{})
			tracer.ResetForTesting()
			t.Cleanup(func() {
				otel.SetTracerProvider(previous)
				otel.SetTextMapPropagator(propagator)
				tracer.ResetForTesting()
				_ = host.Shutdown(context.Background())
			})
			cfg, err := config.NewConfiguration()
			require.NoError(t, err)
			cfg.Trace.Exporter = "noop"
			if constructor == "service" {
				cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:1"}, DialTimeout: time.Millisecond})
				require.NoError(t, err)
				defer cli.Close()
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // No external etcd or metadata mutation in this constructor probe.
				cfg.Woodpecker.Client.Quorum.SelectStrategy.AffinityMode = config.NewDynamic("invalid-test-mode")
				_, err = NewClient(ctx, cfg, cli, false)
				require.Error(t, err)
			} else {
				cfg.Woodpecker.Storage.Type = "service" // Rejected before creating external clients.
				_, err = NewEmbedClientFromConfig(context.Background(), cfg)
				require.Error(t, err)
			}
			require.Same(t, host, otel.GetTracerProvider())
			require.Equal(t, []string{"baggage"}, otel.GetTextMapPropagator().Fields())
			_, span := otel.Tracer("host.after-client").Start(context.Background(), "still-exported-by-host")
			span.End()
			require.Len(t, recorder.Ended(), 1)
			require.Equal(t, "still-exported-by-host", recorder.Ended()[0].Name())
		})
	}
}

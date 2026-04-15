package opregistry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/zilliztech/woodpecker/common/metrics"
)

func TestUnaryInterceptor_Success(t *testing.T) {
	metrics.ResetObservers()
	defer metrics.ResetObservers()

	reg := NewWithClock(100, 30*time.Second, clockwork.NewFakeClock())
	metrics.RegisterOpObserver(reg)

	interceptor := UnaryInterceptor()
	handler := func(ctx context.Context, req any) (any, error) {
		// During handler execution, the op should be in the registry.
		ops := reg.List(Filter{Types: []OpType{OpTypeGRPC}})
		assert.Len(t, ops, 1)
		return "ok", nil
	}

	resp, err := interceptor(
		context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/test/Method"}, handler,
	)
	require.NoError(t, err)
	require.Equal(t, "ok", resp)

	// After handler completes, op should be deregistered.
	require.Len(t, reg.List(Filter{}), 0)
}

func TestUnaryInterceptor_Error(t *testing.T) {
	metrics.ResetObservers()
	defer metrics.ResetObservers()

	reg := NewWithClock(100, 30*time.Second, clockwork.NewFakeClock())
	metrics.RegisterOpObserver(reg)

	interceptor := UnaryInterceptor()
	handler := func(ctx context.Context, req any) (any, error) {
		return nil, fmt.Errorf("boom")
	}

	_, err := interceptor(
		context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/test/Method"}, handler,
	)
	require.Error(t, err)

	// Op should still be deregistered even on error.
	require.Len(t, reg.List(Filter{}), 0)
}

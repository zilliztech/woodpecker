package opregistry

import (
	"context"

	"google.golang.org/grpc"

	"github.com/zilliztech/woodpecker/common/metrics"
)

// UnaryInterceptor returns a gRPC unary server interceptor that registers
// each RPC as an op in the registry via the metrics.StartOp bridge.
func UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		op := metrics.StartOp(string(OpTypeGRPC), nil, nil)
		resp, err := handler(ctx, req)
		status := "success"
		if err != nil {
			status = "error"
		}
		op.End(status)
		return resp, err
	}
}

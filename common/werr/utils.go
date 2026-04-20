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

package werr

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zilliztech/woodpecker/proto"
)

// ---------------------------------------------
// gRPC Status related error
// ---------------------------------------------

func Success(reason ...string) *proto.Status {
	status := &proto.Status{
		Code: 0,
	}
	// NOLINT
	status.Reason = strings.Join(reason, " ")
	return status
}

func Code(err error) int32 {
	if err == nil {
		return 0
	}

	// First check if it's already a woodpeckerError in the chain
	var wpErr woodpeckerError
	if errors.As(err, &wpErr) {
		return wpErr.Code()
	}

	// Then check for specific error types
	if errors.Is(err, context.Canceled) {
		return ErrCancelError.Code()
	} else if errors.Is(err, context.DeadlineExceeded) {
		return ErrTimeoutError.Code()
	} else {
		return ErrUnknownError.Code()
	}
}

// Status returns a status according to the given err,
// returns Success status if err is nil
func Status(err error) *proto.Status {
	if err == nil {
		return Success()
	}

	code := Code(err)

	// Extract the most relevant error message for Reason
	var reason string
	cause := errors.Cause(err)
	if wpErr, ok := cause.(woodpeckerError); ok {
		// For woodpecker errors, use the core message
		reason = wpErr.Error()
	} else {
		// For other errors, use the root cause
		reason = cause.Error()
	}

	status := &proto.Status{
		Code:      code,
		Reason:    reason,
		Retriable: IsRetryableErr(err),
		Detail:    err.Error(), // Full error chain
	}
	return status
}

func Error(status *proto.Status) error {
	// use code first
	code := status.GetCode()
	if code == 0 {
		return nil
	}

	return newWoodpeckerError(status.GetReason(), code, status.GetRetriable())
}

// ---------------------------------------------
// Segment Writable related error
// ---------------------------------------------

func IsSegmentNotWritableErr(err error) bool {
	if err == nil {
		return false
	}
	return ErrSegmentHandleSegmentClosed.Is(err) || ErrSegmentFenced.Is(err) || ErrStorageNotWritable.Is(err) || ErrFileWriterFinalized.Is(err) || ErrWoodpeckerClientClosed.Is(err)
}

// ---------------------------------------------
// Timeout related error
// ---------------------------------------------

// IsTimeoutError Helper function to check if an error is a timeout error (context deadline exceeded or gRPC deadline exceeded)
func IsTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	// Check for standard context errors first
	if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
		return true
	}

	// Check for gRPC deadline exceeded
	if st, ok := status.FromError(err); ok {
		if st.Code() == codes.DeadlineExceeded || st.Code() == codes.Canceled {
			return true
		}
	}

	// Fallback to string matching for wrapped errors
	errMsg := err.Error()
	return strings.Contains(errMsg, "context deadline exceeded") ||
		strings.Contains(errMsg, "DeadlineExceeded") ||
		strings.Contains(errMsg, "context canceled")
}

// ---------------------------------------------
// Transport related error
// ---------------------------------------------

// IsTransportError reports whether err indicates that the underlying gRPC
// transport to the peer is broken (connection refused, peer unreachable,
// dropped connection, etc.). When true, callers should drop the cached
// connection so that the next call re-resolves DNS and rebuilds the
// subchannel, instead of retrying forever against a stale address.
//
// Rationale: grpc-go's pick_first balancer pins the first resolved address
// to a subchannel and only re-resolves when the balancer calls ResolveNow,
// rate-limited by MinResolutionInterval (30s by default). When a peer pod
// is recreated with a new IP, the cached ClientConn can keep dialing the
// old IP for a long time. Dropping the connection on transport errors
// forces a fresh grpc.NewClient on the next GetLogStoreClient and recovers
// in seconds instead of tens of minutes.
func IsTransportError(err error) bool {
	if err == nil {
		return false
	}

	// Part 1: canonical gRPC status code.
	//
	// Every transport-layer failure in grpc-go converges to codes.Unavailable
	// via toRPCErr (rpc_util.go):
	//
	//     case transport.ConnectionError:
	//         return status.Error(codes.Unavailable, e.Desc)
	//
	// transport.ConnectionError is produced in three places we care about:
	//   1. dial failure — http2_client.go wraps any net.Dial error as
	//      `connectionErrorf(true, err, "transport: Error while dialing: %v", err)`
	//   2. already-open transport closing — transport.go defines
	//      `ErrConnClosing = connectionErrorf(true, nil, "transport is closing")`
	//   3. server GOAWAY / draining — http2_client.go & transport.go emit
	//      codes.Unavailable directly via status.Newf/status.Error.
	//
	// So checking codes.Unavailable catches all "the peer is unreachable"
	// cases as long as the status survives to reach us.
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable:
			return true
		}
	}

	// Part 2: defensive string fallback.
	//
	// Used when the gRPC Status has been stripped along the way — typically
	// by an intermediate `fmt.Errorf("...: %v", err)` (note: %v not %w) that
	// flattens the error into a plain string. Each pattern below corresponds
	// to a real producer, not a guess:
	//
	//   "Error while dialing"       — grpc http2_client.go connectionErrorf
	//                                 template when net.Dial returns an error.
	//   "transport is closing"      — grpc transport.go ErrConnClosing, emitted
	//                                 when an established transport is torn down.
	//   "connection refused"        — Go stdlib: syscall.ECONNREFUSED.Error().
	//                                 Happens when the TCP SYN reaches a host
	//                                 that has nothing listening on that port
	//                                 (exactly our "pod was killed" scenario).
	//   "connection reset by peer"  — Go stdlib: syscall.ECONNRESET.Error().
	//                                 Peer process died mid-connection or sent
	//                                 RST.
	//   "no such host"              — Go stdlib: *net.DNSError.Error() when
	//                                 the hostname does not resolve (e.g. pod
	//                                 FQDN not yet published by k8s DNS).
	//
	// None of these substrings appear in legitimate application errors, so
	// false positives are negligible; the cost of a false positive is at
	// most one avoidable connection rebuild, which is cheap. The cost of a
	// false negative is another 30-minute stall, so we err on the side of
	// dropping the connection.
	errMsg := err.Error()
	return strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "no such host") ||
		strings.Contains(errMsg, "connection reset by peer") ||
		strings.Contains(errMsg, "transport is closing") ||
		strings.Contains(errMsg, "Error while dialing")
}

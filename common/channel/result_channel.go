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

package channel

import (
	"context"
	"errors"

	"github.com/zilliztech/woodpecker/common/werr"
)

// ResultChannel is an abstract interface for handling asynchronous result notifications.
// The local implementation directly uses Go channels, while the remote implementation uses gRPC streams + identifiers.
type ResultChannel interface {
	// GetIdentifier returns the unique identifier of this result channel.
	GetIdentifier() string
	// SendResult sends a result to the channel.
	SendResult(ctx context.Context, result *AppendResult) error
	// ReadResult reads a result from the channel.
	ReadResult(ctx context.Context) (*AppendResult, error)
	// Close closes the channel.
	Close(ctx context.Context) error
	// IsClosed checks if the channel is closed.
	IsClosed() bool
}

type AppendResult struct {
	SyncedId int64
	Err      error
}

// readBudgetExhausted renders a read that ended because its caller's context did.
//
// A deadline means the ack ran late, and every ReadResult has to report that the
// same way, because what consumes it decides an entry's fate: the append path
// asks werr.IsRetryableErr, which extracts a woodpeckerError and so answers
// false for a bare context error. An entry whose ack merely ran late would then
// go terminal with none of its retries spent. Wrapping keeps errors.Is working,
// which is what the read-timeout log branch tests for.
//
// A cancellation is the caller's own decision - the server reads on the gRPC
// stream's context and sees one whenever a client goes away - so it is passed
// through as itself rather than relabelled a timeout.
func readBudgetExhausted(cause error) error {
	if errors.Is(cause, context.DeadlineExceeded) {
		return werr.ErrAppendOpTimeout.WithCauseErr(cause)
	}
	return cause
}

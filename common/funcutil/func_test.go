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

package funcutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_CheckGrpcReady(t *testing.T) {
	errChan := make(chan error, 1)

	// test errChan can receive nil after interval
	go CheckGrpcReady(context.TODO(), errChan)

	err := <-errChan
	assert.NoError(t, err)

	// test CheckGrpcReady finishes when context is done before timer fires
	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Millisecond)
	defer cancel()
	go CheckGrpcReady(ctx, errChan)
	// Wait for context to expire; CheckGrpcReady should return without sending
	<-ctx.Done()
	// Give it a moment to finish
	time.Sleep(20 * time.Millisecond)
	// errChan should be empty (no send because context expired first)
	select {
	case <-errChan:
		t.Error("expected no value on errChan when context is cancelled before timer")
	default:
		// expected
	}
}

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

package segment

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

func TestAppendOpQuorumCountsEachReplicaOnce(t *testing.T) {
	q := &proto.QuorumInfo{Es: 3, Wq: 3, Aq: 2}
	op := NewAppendOp("b", "r", 1, 2, 10, []byte("value"), func(int64, int64, error) {}, q, nil)
	require.Equal(t, "1/2/10", op.Identifier())
	require.Equal(t, int64(5), op.Size())
	require.False(t, op.applyReplicaResult(0, nil))
	require.False(t, op.applyReplicaResult(0, nil))
	require.False(t, op.applyReplicaResult(1, werr.ErrTimeoutError))
	require.True(t, op.applyReplicaResult(2, nil))
	require.NoError(t, op.terminalErr)
}

func TestAppendOpQuorumImpossibleAndCallbackOnce(t *testing.T) {
	var calls atomic.Int32
	op := NewAppendOp("b", "r", 1, 1, 0, []byte("v"), func(int64, int64, error) { calls.Add(1) }, &proto.QuorumInfo{Wq: 3, Aq: 2}, nil)
	require.False(t, op.applyReplicaResult(0, werr.ErrTimeoutError))
	require.False(t, op.applyReplicaResult(0, werr.ErrTimeoutError))
	require.True(t, op.applyReplicaResult(1, werr.ErrTimeoutError))
	require.Error(t, op.terminalErr)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				op.FastSuccess(context.Background())
			} else {
				op.FastFail(context.Background(), werr.ErrTimeoutError)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), calls.Load())
}

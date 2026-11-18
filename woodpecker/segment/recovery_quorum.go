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
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
)

const backgroundRecoveryTimeout = 30 * time.Second

type nodeRecoveryResult struct {
	node        string
	lastEntryId int64
	err         error
}

// One worker per busy replica keeps Fence and Complete in submission order.
// Workers exit when their queue is empty, including on read-only recovery handles.
type recoveryPool struct {
	mu     sync.Mutex
	queues map[string][]func()
}

func (p *recoveryPool) submit(node string, task func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.queues == nil {
		p.queues = make(map[string][]func())
	}
	p.queues[node] = append(p.queues[node], task)
	if len(p.queues[node]) == 1 {
		go p.run(node, task)
	}
}

func (p *recoveryPool) run(node string, task func()) {
	for {
		task()
		p.mu.Lock()
		queue := p.queues[node]
		queue[0] = nil
		if len(queue) == 1 {
			delete(p.queues, node)
			p.mu.Unlock()
			return
		}
		p.queues[node] = queue[1:]
		task = queue[1]
		p.mu.Unlock()
	}
}

// submitRecovery returns one buffered reply per replica. Releasing the foreground
// wait after quorum gives unfinished tasks a bounded grace period.
func (s *segmentHandleImpl) submitRecovery(parent context.Context, nodes []string, allowEarly bool, operation string,
	rpc func(context.Context, string) (int64, error),
) (<-chan nodeRecoveryResult, func()) {
	ctx := parent
	taskDone, release := func() {}, func() {}
	if allowEarly {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.WithoutCancel(parent))
		var remaining atomic.Int32
		remaining.Store(int32(len(nodes)))
		taskDone = func() {
			if remaining.Add(-1) == 0 {
				cancel()
			}
		}
		release = func() {
			if parent.Err() != nil || ctx.Err() != nil {
				cancel()
				return
			}
			timer := time.AfterFunc(backgroundRecoveryTimeout, cancel)
			context.AfterFunc(ctx, func() { timer.Stop() })
		}
	}

	results := make(chan nodeRecoveryResult, len(nodes))
	for _, node := range nodes {
		s.recovery.submit(node, func() {
			defer taskDone()
			tail := int64(-1)
			err := ctx.Err()
			if err == nil {
				tail, err = rpc(ctx, node)
			}
			if err != nil {
				logger.Ctx(ctx).Warn("Segment recovery RPC failed", zap.String("operation", operation),
					zap.Int64("logId", s.logId), zap.Int64("segmentId", s.segmentId), zap.String("node", node), zap.Error(err))
			}
			results <- nodeRecoveryResult{node: node, lastEntryId: tail, err: err}
		})
	}
	return results, release
}

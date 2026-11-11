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

package conc

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/hardware"
)

func TestPool(t *testing.T) {
	pool := NewDefaultPool[any]()

	taskNum := pool.Cap() * 2
	futures := make([]*Future[any], 0, taskNum)
	for i := 0; i < taskNum; i++ {
		res := i
		future := pool.Submit(func() (any, error) {
			time.Sleep(500 * time.Millisecond)
			return res, nil
		})
		futures = append(futures, future)
	}

	// Wait for all tasks to complete
	AwaitAll(futures...)
	for i, future := range futures {
		res, err := future.Await()
		assert.NoError(t, err)
		assert.Equal(t, err, future.Err())
		assert.True(t, future.OK())
		assert.Equal(t, res, future.Value())
		assert.Equal(t, i, res.(int))

		// Await() should be idempotent
		<-future.Inner()
		resDup, errDup := future.Await()
		assert.Equal(t, res, resDup)
		assert.Equal(t, err, errDup)
	}
}

func TestPoolResize(t *testing.T) {
	cpuNum := hardware.GetCPUNum()

	pool := NewPool[any](cpuNum)

	assert.Equal(t, cpuNum, pool.Cap())

	err := pool.Resize(cpuNum * 2)
	assert.NoError(t, err)
	assert.Equal(t, cpuNum*2, pool.Cap())

	err = pool.Resize(0)
	assert.Error(t, err)

	pool = NewDefaultPool[any]()
	err = pool.Resize(cpuNum * 2)
	assert.Error(t, err)
}

func TestPoolWithPanic(t *testing.T) {
	pool := NewPool[any](1, WithConcealPanic(true))

	future := pool.Submit(func() (any, error) {
		panic("mocked panic")
	})

	// make sure error returned when conceal panic
	_, err := future.Await()
	assert.Error(t, err)
}

func TestPoolSubmitWithError(t *testing.T) {
	pool := NewPool[int](2)
	defer pool.Release()

	future := pool.Submit(func() (int, error) {
		return 0, assert.AnError
	})

	_, err := future.Await()
	assert.ErrorIs(t, err, assert.AnError)
}

func TestPoolSubmitWithPreHandler(t *testing.T) {
	var called atomic.Bool
	pool := NewPool[int](2, WithPreHandler(func() {
		called.Store(true)
	}))
	defer pool.Release()

	future := pool.Submit(func() (int, error) {
		return 42, nil
	})

	res, err := future.Await()
	assert.NoError(t, err)
	assert.Equal(t, 42, res)
	assert.True(t, called.Load())
}

func TestPoolSubmitToReleasedPool(t *testing.T) {
	pool := NewPool[int](2)
	pool.Release()

	future := pool.Submit(func() (int, error) {
		return 0, nil
	})

	_, err := future.Await()
	assert.Error(t, err)
}

func TestPoolStatusMethods(t *testing.T) {
	pool := NewPool[int](4)
	defer pool.Release()

	assert.Equal(t, 4, pool.Cap())
	assert.GreaterOrEqual(t, pool.Free(), 0)
	assert.GreaterOrEqual(t, pool.Running(), 0)
	assert.False(t, pool.IsClosed())

	pool.Release()
	assert.True(t, pool.IsClosed())
}

func TestPoolReleaseTimeout(t *testing.T) {
	pool := NewPool[int](2)
	err := pool.ReleaseTimeout(time.Second)
	assert.NoError(t, err)
	assert.True(t, pool.IsClosed())
}

func TestNewPoolInvalidCapPanics(t *testing.T) {
	// ants.NewPool returns error when preAlloc=true with negative size
	assert.Panics(t, func() {
		NewPool[int](-1, WithPreAlloc(true))
	})
}

func TestWarmupPool(t *testing.T) {
	pool := NewPool[int](2)
	defer pool.Release()

	var count atomic.Int32
	WarmupPool(pool, func() {
		count.Add(1)
	})
	assert.Equal(t, int32(2), count.Load())
}

func TestPoolWithExpiryDuration(t *testing.T) {
	pool := NewPool[int](2, WithExpiryDuration(time.Second))
	defer pool.Release()

	future := pool.Submit(func() (int, error) {
		return 1, nil
	})
	res, err := future.Await()
	assert.NoError(t, err)
	assert.Equal(t, 1, res)
}

func TestPoolWithCustomPanicHandler(t *testing.T) {
	var panicCaught atomic.Bool
	pool := NewPool[int](1, WithConcealPanic(true), func(opt *poolOption) {
		opt.panicHandler = func(v any) {
			panicCaught.Store(true)
		}
	})
	defer pool.Release()

	future := pool.Submit(func() (int, error) {
		panic("custom handler test")
	})

	_, _ = future.Await()
	// The panic handler runs in the worker goroutine after future.ch is closed,
	// so we need to poll until it completes.
	assert.Eventually(t, func() bool {
		return panicCaught.Load()
	}, 5*time.Second, 10*time.Millisecond)
}

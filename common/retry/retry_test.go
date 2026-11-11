// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package retry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/werr"
)

func TestDo(t *testing.T) {
	ctx := context.Background()

	n := 0
	testFn := func() error {
		if n < 3 {
			n++
			return errors.New("some error")
		}
		return nil
	}

	err := Do(ctx, testFn)
	assert.NoError(t, err)
}

func TestAttempts(t *testing.T) {
	ctx := context.Background()

	testFn := func() error {
		t.Log("executed")
		return errors.New("some error")
	}

	err := Do(ctx, testFn, Attempts(1))
	assert.Error(t, err)
	t.Log(err)

	ctx = context.Background()
	testOperation := 0
	testFn = func() error {
		testOperation++
		return nil
	}

	err = Do(ctx, testFn, AttemptAlways())
	assert.Equal(t, testOperation, 1)
	assert.NoError(t, err)
}

func TestMaxSleepTime(t *testing.T) {
	ctx := context.Background()

	testFn := func() error {
		return fmt.Errorf("some error")
	}

	err := Do(ctx, testFn, Attempts(3), MaxSleepTime(200*time.Millisecond))
	assert.Error(t, err)
	t.Log(err)

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	err = Do(ctx, testFn, Attempts(10), MaxSleepTime(200*time.Millisecond))
	assert.Error(t, err)
	assert.Nil(t, ctx.Err())
}

func TestSleep(t *testing.T) {
	ctx := context.Background()

	testFn := func() error {
		return fmt.Errorf("some error")
	}

	err := Do(ctx, testFn, Attempts(3), Sleep(500*time.Millisecond))
	assert.Error(t, err)
	t.Log(err)
}

func TestAllError(t *testing.T) {
	ctx := context.Background()

	testFn := func() error {
		return errors.New("some error")
	}

	err := Do(ctx, testFn, Attempts(3))
	assert.Error(t, err)
	t.Log(err)
}

func TestUnRecoveryError(t *testing.T) {
	attempts := 0
	ctx := context.Background()

	mockErr := errors.New("some error")
	testFn := func() error {
		attempts++
		return Unrecoverable(mockErr)
	}

	err := Do(ctx, testFn, Attempts(3))
	assert.Error(t, err)
	assert.Equal(t, 1, attempts)
	assert.True(t, errors.Is(err, mockErr))
}

func TestIsRecoverable(t *testing.T) {
	// Normal errors are recoverable
	normalErr := errors.New("normal error")
	assert.True(t, IsRecoverable(normalErr))

	// Unrecoverable-wrapped errors are not recoverable
	unrecoverableErr := Unrecoverable(normalErr)
	assert.False(t, IsRecoverable(unrecoverableErr))

	// The original error is still accessible through the chain
	assert.True(t, errors.Is(unrecoverableErr, normalErr))

	// Wrapping a woodpecker error as unrecoverable
	wpErr := werr.ErrTimeoutError.WithCauseErrMsg("timeout")
	unrecoverableWpErr := Unrecoverable(wpErr)
	assert.False(t, IsRecoverable(unrecoverableWpErr))
	assert.True(t, errors.Is(unrecoverableWpErr, werr.ErrTimeoutError))
}

func TestUnrecoverableStopsRetry(t *testing.T) {
	attempts := 0
	ctx := context.Background()

	testFn := func() error {
		attempts++
		if attempts == 2 {
			return Unrecoverable(errors.New("fatal error"))
		}
		return errors.New("transient error")
	}

	err := Do(ctx, testFn, Attempts(10), Sleep(time.Millisecond))
	assert.Error(t, err)
	assert.Equal(t, 2, attempts, "retry should stop at the first unrecoverable error")
}

func TestContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	testFn := func() error {
		return fmt.Errorf("some error")
	}

	err := Do(ctx, testFn)
	assert.Error(t, err)
	t.Log(err)
}

func TestContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mockErr := errors.New("mock error")
	testFn := func() error {
		return mockErr
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := Do(ctx, testFn)
	assert.Error(t, err)
	assert.ErrorIs(t, err, mockErr)
	t.Log(err)
}

func TestWrap(t *testing.T) {
	err := werr.ErrTimeoutError.WithCauseErrMsg("timeout to process")
	assert.True(t, errors.Is(err, werr.ErrTimeoutError))
	assert.True(t, IsRecoverable(err))
	err2 := Unrecoverable(err)
	fmt.Println(err2)
	assert.True(t, errors.Is(err2, werr.ErrTimeoutError))
	assert.False(t, IsRecoverable(err2))
}

func TestRetryErrorParam(t *testing.T) {
	{
		mockErr := errors.New("mock not retry error")
		runTimes := 0
		err := Do(context.Background(), func() error {
			runTimes++
			return mockErr
		}, RetryErr(func(err error) bool {
			return err != mockErr
		}))

		assert.Error(t, err)
		assert.Equal(t, 1, runTimes)
	}

	{
		mockErr := errors.New("mock retry error")
		runTimes := 0
		err := Do(context.Background(), func() error {
			runTimes++
			return mockErr
		}, Attempts(3), RetryErr(func(err error) bool {
			return err == mockErr
		}))

		assert.Error(t, err)
		assert.Equal(t, 3, runTimes)
	}
}

func TestDo_ContextAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := Do(ctx, func() error {
		t.Fatal("should not be called")
		return nil
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestDo_ContextAlreadyDeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	time.Sleep(1 * time.Millisecond) // ensure deadline passed

	err := Do(ctx, func() error {
		t.Fatal("should not be called")
		return nil
	})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestDo_UnrecoverableReturnsLastErr(t *testing.T) {
	// When fn returns context.Canceled (unrecoverable) and lastErr exists,
	// Do should return lastErr instead of the context error
	firstErr := errors.New("first failure")
	counter := 0
	err := Do(context.Background(), func() error {
		counter++
		if counter == 1 {
			return firstErr
		}
		return Unrecoverable(context.Canceled)
	}, Attempts(5), Sleep(1*time.Millisecond))
	assert.ErrorIs(t, err, firstErr)
}

func TestDo_DeadlineApproachingReturnsLastErr(t *testing.T) {
	// When deadline is approaching and the error is a context error with lastErr set
	firstErr := errors.New("transient error")
	counter := 0
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := Do(ctx, func() error {
		counter++
		if counter == 1 {
			return firstErr
		}
		// Return context deadline exceeded after first failure
		return Unrecoverable(context.DeadlineExceeded)
	}, Attempts(10), Sleep(1*time.Millisecond))
	// Should get the first error back since the second error is context-related
	assert.ErrorIs(t, err, firstErr)
}

func TestSleep_AdjustsMaxSleepTime(t *testing.T) {
	// When Sleep is set to a large value, maxSleepTime should be auto-adjusted
	c := newDefaultRetryConfig()
	Sleep(2 * time.Second)(c)
	assert.Equal(t, 2*time.Second, c.sleep)
	assert.GreaterOrEqual(t, c.maxSleepTime, 4*time.Second)
}

func TestMaxSleepTime_NormalCase(t *testing.T) {
	// When maxSleepTime >= 2*sleep, it should just be set directly
	c := newDefaultRetryConfig()
	MaxSleepTime(10 * time.Second)(c)
	assert.Equal(t, 10*time.Second, c.maxSleepTime)
}

func TestHandle_ContextAlreadyCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Handle(ctx, func() (bool, error) {
		t.Fatal("should not be called")
		return false, nil
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func TestHandle_ShouldNotRetryWithContextErr(t *testing.T) {
	// When shouldRetry=false and err is context error with lastErr, returns lastErr
	firstErr := errors.New("first err")
	counter := 0
	err := Handle(context.Background(), func() (bool, error) {
		counter++
		if counter == 1 {
			return true, firstErr
		}
		return false, context.Canceled // not retryable, context error
	}, Attempts(5), Sleep(1*time.Millisecond))
	assert.ErrorIs(t, err, firstErr)
}

func TestHandle_SleepCapAtMaxSleepTime(t *testing.T) {
	// Verify exponential backoff caps at maxSleepTime
	counter := 0
	start := time.Now()
	err := Handle(context.Background(), func() (bool, error) {
		counter++
		if counter >= 5 {
			return false, nil
		}
		return true, errors.New("retry")
	}, Attempts(10), Sleep(1*time.Millisecond), MaxSleepTime(5*time.Millisecond))
	assert.NoError(t, err)
	assert.Equal(t, 5, counter)
	elapsed := time.Since(start)
	// With sleep 1,2,4,5 ms (capped at 5), total should be < 100ms
	assert.Less(t, elapsed, 100*time.Millisecond)
}

func TestHandle_ExhaustsAttempts(t *testing.T) {
	mockErr := errors.New("always fail")
	err := Handle(context.Background(), func() (bool, error) {
		return true, mockErr
	}, Attempts(3), Sleep(1*time.Millisecond))
	assert.ErrorIs(t, err, mockErr)
}

func TestHandle(t *testing.T) {
	// test context done
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Handle(ctx, func() (bool, error) {
		return false, nil
	}, Attempts(5))
	assert.ErrorIs(t, err, context.Canceled)

	fakeErr := errors.New("mock retry error")
	// test return error and retry
	counter := 0
	err = Handle(context.Background(), func() (bool, error) {
		counter++
		if counter < 3 {
			return true, fakeErr
		}
		return false, nil
	}, Attempts(10))
	assert.NoError(t, err)

	// test ctx done before return retry success
	counter = 0
	ctx1, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = Handle(ctx1, func() (bool, error) {
		counter++
		if counter < 5 {
			return true, fakeErr
		}
		return false, nil
	}, Attempts(10))
	assert.ErrorIs(t, err, fakeErr)

	// test return error and not retry
	err = Handle(context.Background(), func() (bool, error) {
		return false, fakeErr
	}, Attempts(10))
	assert.ErrorIs(t, err, fakeErr)

	// test return nil
	err = Handle(context.Background(), func() (bool, error) {
		return false, nil
	}, Attempts(10))
	assert.NoError(t, err)
}

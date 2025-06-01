// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/mocks/mocks_server/mocks_storage"
	"github.com/zilliztech/woodpecker/server/storage"
)

func setupMockFragment(t *testing.T, id int64, key string, size int64, lastEntryId int64) storage.Fragment {
	fragment := mocks_storage.NewFragment(t)

	// Setup mock methods - using On() instead of EXPECT() to match mockery's pattern
	fragment.On("GetLogId").Return(int64(1)).Maybe()
	fragment.On("GetSegmentId").Return(int64(0)).Maybe()
	fragment.On("GetFragmentId").Return(id).Maybe()
	fragment.On("GetFragmentKey").Return(key).Maybe()
	fragment.On("GetSize").Return(size).Maybe()
	fragment.On("Release").Return(nil).Maybe()
	fragment.On("GetRawBufSize").Return(size).Maybe()

	return fragment
}

func TestFragmentManager_AddFragment(t *testing.T) {
	fm := newFragmentManager(100)

	fragment := setupMockFragment(t, 0, "0.frag", 20, 10)

	err := fm.AddFragment(context.TODO(), fragment)
	assert.NoError(t, err)
	assert.Equal(t, int64(20), fm.GetUsedMemory())
}

func TestFragmentManager_RemoveFragment(t *testing.T) {
	fm := newFragmentManager(100)

	fragment := setupMockFragment(t, 0, "0.frag", 20, 10)

	err := fm.AddFragment(context.TODO(), fragment)
	assert.NoError(t, err)
	err = fm.RemoveFragment(context.TODO(), fragment)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), fm.GetUsedMemory())
}

func TestFragmentManager_EvictFragments(t *testing.T) {
	fm := newFragmentManager(40)

	fragment1 := setupMockFragment(t, 0, "0.frag", 20, 10)
	fragment2 := setupMockFragment(t, 1, "1.frag", 30, 20)

	err := fm.AddFragment(context.TODO(), fragment1)
	assert.NoError(t, err)
	err = fm.AddFragment(context.TODO(), fragment2)
	assert.NoError(t, err)
	assert.Equal(t, int64(50), fm.GetUsedMemory())

	err = fm.EvictFragments()
	assert.NoError(t, err)
	assert.Equal(t, int64(30), fm.GetUsedMemory())
}

func TestFragmentManager(t *testing.T) {
	fm := newFragmentManager(100)

	fragment1 := setupMockFragment(t, 0, "0.frag", 20, 20)
	fragment2 := setupMockFragment(t, 1, "1.frag", 30, 50)
	fragment3 := setupMockFragment(t, 2, "2.frag", 40, 90)

	err := fm.AddFragment(context.TODO(), fragment1)
	assert.NoError(t, err)
	err = fm.AddFragment(context.TODO(), fragment2)
	assert.NoError(t, err)
	err = fm.AddFragment(context.TODO(), fragment3)
	assert.NoError(t, err)

	assert.Equal(t, int64(90), fm.GetUsedMemory())

	// remove manually
	err = fm.RemoveFragment(context.TODO(), fragment2)
	assert.NoError(t, err)
	assert.Equal(t, int64(60), fm.GetUsedMemory())

	// remove automatically
	err = fm.StartEvictionLoop(1 * time.Second)
	assert.NoError(t, err)
	fragment4 := setupMockFragment(t, 3, "3.frag", 80, 170)
	err = fm.AddFragment(context.TODO(), fragment4)
	assert.NoError(t, err)
	time.Sleep(3 * time.Second)
	assert.Equal(t, int64(80), fm.GetUsedMemory())
	err = fm.StopEvictionLoop()
	assert.NoError(t, err)
}

// TestConcurrentGetAddPattern simulates the scenario where multiple goroutines
// perform the get-then-add pattern that could lead to deadlocks in the original implementation
func TestConcurrentGetAddPattern(t *testing.T) {
	fm := newFragmentManager(1000)
	ctx := context.Background()

	const numGoroutines = 10
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run multiple goroutines that perform get-then-add operations
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("%d.%d.frag", routineID, j)

				// First try to get (this pattern could cause deadlocks in the original implementation)
				fragment, exists := fm.GetFragment(ctx, key)

				if !exists {
					// Fragment doesn't exist, create and add it
					fragment = setupMockFragment(t, int64(routineID*1000+j), key, 10, int64(j))
					err := fm.AddFragment(ctx, fragment)
					assert.NoError(t, err)
				}

				// Simulate some work with the fragment
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Use a timeout to detect potential deadlocks
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Test completed successfully
	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out, possible deadlock detected")
	}
}

// TestConcurrentMixedOperations tests a mix of different operations running concurrently
func TestConcurrentMixedOperations(t *testing.T) {
	fm := newFragmentManager(500)
	ctx := context.Background()

	// Pre-populate the cache with some fragments
	for i := 0; i < 20; i++ {
		fragment := setupMockFragment(t, int64(i), fmt.Sprintf("initial.%d.frag", i), 10, int64(i))
		err := fm.AddFragment(ctx, fragment)
		assert.NoError(t, err)
	}

	const numGoroutines = 5
	const duration = 3 * time.Second

	var wg sync.WaitGroup
	wg.Add(4 * numGoroutines) // 4 different operation types

	// Signal to start and stop the test
	startTime := time.Now()
	endTime := startTime.Add(duration)

	// Goroutines that perform GetFragment operations
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()

			for time.Now().Before(endTime) {
				key := fmt.Sprintf("initial.%d.frag", routineID%20)
				_, _ = fm.GetFragment(ctx, key)
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Goroutines that perform AddFragment operations
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()

			counter := 0
			for time.Now().Before(endTime) {
				key := fmt.Sprintf("add.%d.%d.frag", routineID, counter)
				fragment := setupMockFragment(t, int64(routineID*1000+counter), key, 5, int64(counter))
				_ = fm.AddFragment(ctx, fragment)
				counter++
				time.Sleep(2 * time.Millisecond)
			}
		}(i)
	}

	// Goroutines that perform RemoveFragment operations
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()

			for time.Now().Before(endTime) {
				// Try to remove a fragment that was added by the AddFragment goroutines
				key := fmt.Sprintf("add.%d.%d.frag", routineID, routineID%10)
				fragment := setupMockFragment(t, 0, key, 0, 0) // We only need the key for removal
				_ = fm.RemoveFragment(ctx, fragment)
				time.Sleep(3 * time.Millisecond)
			}
		}(i)
	}

	// Goroutines that perform EvictFragments operations
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			for time.Now().Before(endTime) {
				_ = fm.EvictFragments()
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	// Use a timeout to detect potential deadlocks (slightly longer than the test duration)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Test completed successfully
	case <-time.After(duration + 2*time.Second):
		t.Fatal("Test timed out, possible deadlock detected")
	}

	// We don't check specific state values here because with concurrent operations
	// the exact state is non-deterministic, but the test passing without deadlocks
	// is the main success criteria
}

// TestDeadlockScenario specifically tests the scenario that could cause deadlocks
// in the original implementation where two goroutines try to get and add the same fragments
func TestDeadlockScenario(t *testing.T) {
	fm := newFragmentManager(200)
	ctx := context.Background()

	// Create two fragments that will be operated on
	fragment1 := setupMockFragment(t, 1, "deadlock.1.frag", 30, 10)
	fragment2 := setupMockFragment(t, 2, "deadlock.2.frag", 30, 20)

	var wg sync.WaitGroup
	wg.Add(2)

	// First goroutine tries to get fragment1 then add fragment2
	go func() {
		defer wg.Done()

		// First get fragment1
		_, _ = fm.GetFragment(ctx, "deadlock.1.frag")
		time.Sleep(10 * time.Millisecond) // Ensure the second goroutine has time to get fragment2

		// Then add fragment2
		_ = fm.AddFragment(ctx, fragment2)
	}()

	// Second goroutine tries to get fragment2 then add fragment1
	go func() {
		defer wg.Done()

		// First get fragment2
		_, _ = fm.GetFragment(ctx, "deadlock.2.frag")
		time.Sleep(10 * time.Millisecond) // Ensure the first goroutine has time to get fragment1

		// Then add fragment1
		_ = fm.AddFragment(ctx, fragment1)
	}()

	// Use a timeout to detect potential deadlocks
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Test completed successfully
	case <-time.After(1 * time.Second):
		t.Fatal("Test timed out, deadlock detected in the get-then-add scenario")
	}
}

// TestHighContention simulates high contention on the same fragments
func TestHighContention(t *testing.T) {
	fm := newFragmentManager(300)
	ctx := context.Background()

	// Pre-populate with a few fragments
	for i := 0; i < 5; i++ {
		fragment := setupMockFragment(t, int64(i), fmt.Sprintf("contention.%d.frag", i), 20, int64(i))
		err := fm.AddFragment(ctx, fragment)
		assert.NoError(t, err)
	}

	const numGoroutines = 20
	const duration = 2 * time.Second

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	startTime := time.Now()
	endTime := startTime.Add(duration)

	// All goroutines operate on the same small set of fragments
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			defer wg.Done()

			for time.Now().Before(endTime) {
				// Choose an operation randomly
				op := routineID % 4
				fragIndex := routineID % 5
				key := fmt.Sprintf("contention.%d.frag", fragIndex)

				switch op {
				case 0:
					// Get
					_, _ = fm.GetFragment(ctx, key)
				case 1:
					// Add (trying to add existing fragments is a no-op)
					fragment := setupMockFragment(t, int64(fragIndex), key, 20, int64(fragIndex))
					_ = fm.AddFragment(ctx, fragment)
				case 2:
					// Remove
					fragment := setupMockFragment(t, int64(fragIndex), key, 0, 0)
					_ = fm.RemoveFragment(ctx, fragment)
				case 3:
					// Evict
					_ = fm.EvictFragments()
				}

				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Use a timeout to detect potential deadlocks
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Test completed successfully
	case <-time.After(duration + 2*time.Second):
		t.Fatal("Test timed out, possible deadlock detected in high contention scenario")
	}
}

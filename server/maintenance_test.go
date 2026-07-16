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

package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
)

// fakeTask is a test double for MaintenanceTask. It records each run in a buffered
// channel and, if panicOnce is true, panics on the very first invocation then
// behaves normally afterwards.
type fakeTask struct {
	name      string
	interval  time.Duration
	runs      chan struct{}
	panicOnce bool
	ranOnce   atomic.Bool
}

func (f *fakeTask) Name() string            { return f.name }
func (f *fakeTask) Interval() time.Duration { return f.interval }
func (f *fakeTask) Run(_ context.Context) error {
	if f.panicOnce && f.ranOnce.CompareAndSwap(false, true) {
		panic("deliberate test panic")
	}
	// Non-blocking send: if the channel is full we don't block the task goroutine.
	select {
	case f.runs <- struct{}{}:
	default:
	}
	return nil
}

// TestNodeMaintenanceManager_RunsTasksPeriodically verifies that a registered task
// receives an immediate first run and then continues to run on its ticker interval.
func TestNodeMaintenanceManager_RunsTasksPeriodically(t *testing.T) {
	t.Parallel()

	task := &fakeTask{
		name:     "periodic-task",
		interval: 15 * time.Millisecond,
		runs:     make(chan struct{}, 64),
	}

	mgr := NewNodeMaintenanceManager(context.Background())
	mgr.Register(task)
	mgr.Start()
	defer mgr.Stop()

	// We expect at least 2 runs (immediate + ≥1 tick) within 2 seconds.
	count := 0
	deadline := time.After(2 * time.Second)
	for count < 2 {
		select {
		case <-task.runs:
			count++
		case <-deadline:
			t.Fatalf("timed out waiting for ≥2 runs; got %d", count)
		}
	}
	assert.GreaterOrEqual(t, count, 2)
}

// TestNodeMaintenanceManager_PanicIsolation verifies that a panicking task is
// recovered and does NOT prevent other tasks from running or itself from running
// again on subsequent ticks.
func TestNodeMaintenanceManager_PanicIsolation(t *testing.T) {
	t.Parallel()

	panicTask := &fakeTask{
		name:      "panic-task",
		interval:  10 * time.Millisecond,
		runs:      make(chan struct{}, 64),
		panicOnce: true,
	}
	normalTask := &fakeTask{
		name:     "normal-task",
		interval: 10 * time.Millisecond,
		runs:     make(chan struct{}, 64),
	}

	mgr := NewNodeMaintenanceManager(context.Background())
	mgr.Register(panicTask)
	mgr.Register(normalTask)
	mgr.Start()
	defer mgr.Stop()

	// Both tasks must run within 2 seconds.
	// The panicking task must also recover and run again (i.e., ≥2 runs total).
	var normalRuns, panicRuns int
	deadline := time.After(2 * time.Second)
	for normalRuns < 1 || panicRuns < 2 {
		select {
		case <-normalTask.runs:
			normalRuns++
		case <-panicTask.runs:
			panicRuns++
		case <-deadline:
			t.Fatalf("timed out: normalRuns=%d panicRuns=%d", normalRuns, panicRuns)
		}
	}
	assert.GreaterOrEqual(t, normalRuns, 1, "normal task should have run at least once")
	assert.GreaterOrEqual(t, panicRuns, 2, "panic task should have run again after recovering")
}

// TestNodeMaintenanceManager_StopIsClean verifies that Stop() waits for running
// goroutines and returns promptly without deadlock or panic.
func TestNodeMaintenanceManager_StopIsClean(t *testing.T) {
	t.Parallel()

	task := &fakeTask{
		name:     "stoppable-task",
		interval: 10 * time.Millisecond,
		runs:     make(chan struct{}, 64),
	}

	mgr := NewNodeMaintenanceManager(context.Background())
	mgr.Register(task)
	mgr.Start()

	// Wait until the task has run at least once before stopping.
	require.Eventually(t, func() bool {
		return len(task.runs) >= 1
	}, 2*time.Second, 5*time.Millisecond, "task should have run at least once before Stop")

	// Stop must return without hanging.
	done := make(chan struct{})
	go func() {
		mgr.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return within 2 seconds")
	}
}

// TestLocalLogDataDir_ByStorageMode verifies localLogDataDir returns the correct path
// for service and local storage modes, and "" for minio mode.
func TestLocalLogDataDir_ByStorageMode(t *testing.T) {
	t.Parallel()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	cfg.Woodpecker.Storage.RootPath = "/data"

	cfg.Woodpecker.Storage.Type = "service"
	assert.Equal(t, "/data/buck/rp/9", localLogDataDir(cfg, "buck", "rp", 9))

	cfg.Woodpecker.Storage.Type = "local"
	assert.Equal(t, "/data/rp/9", localLogDataDir(cfg, "buck", "rp", 9))

	cfg.Woodpecker.Storage.Type = "minio"
	assert.Equal(t, "", localLogDataDir(cfg, "buck", "rp", 9))
}

// TestDeletedLogReclaimTask_ReclaimsPastGraceOnly verifies that:
//   - markers older than the grace window have their local data dir, marker file, and
//     in-memory gate entry removed;
//   - markers within the grace window are left completely untouched.
func TestDeletedLogReclaimTask_ReclaimsPastGraceOnly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := createTestLogStore()
	root := t.TempDir()
	store.cfg.Woodpecker.Storage.RootPath = root
	store.cfg.Woodpecker.Storage.Type = "local"

	grace := time.Hour
	now := time.Now()

	// --- OLD log (logId 7, deleted 2 hours ago — past grace) ---
	oldMarker := deleteMarker{Bucket: "b", RootPath: "r", LogId: 7, DeletedAt: now.Add(-2 * time.Hour).Unix()}
	require.NoError(t, writeDeleteMarker(context.Background(), root, oldMarker))
	oldDir := localLogDataDir(store.cfg, "b", "r", 7)
	require.NotEmpty(t, oldDir)
	require.NoError(t, os.MkdirAll(oldDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(oldDir, "data.log"), []byte("old data"), 0o644))
	oldKey := GetLogKey("b", "r", 7)
	store.spMu.Lock()
	store.deletingLogs[oldKey] = struct{}{}
	store.spMu.Unlock()

	// --- FRESH log (logId 8, deleted just now — within grace) ---
	freshMarker := deleteMarker{Bucket: "b", RootPath: "r", LogId: 8, DeletedAt: now.Unix()}
	require.NoError(t, writeDeleteMarker(context.Background(), root, freshMarker))
	freshDir := localLogDataDir(store.cfg, "b", "r", 8)
	require.NotEmpty(t, freshDir)
	require.NoError(t, os.MkdirAll(freshDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(freshDir, "data.log"), []byte("fresh data"), 0o644))
	freshKey := GetLogKey("b", "r", 8)
	store.spMu.Lock()
	store.deletingLogs[freshKey] = struct{}{}
	store.spMu.Unlock()

	// Run the task.
	task := newDeletedLogReclaimTask(store, grace)
	require.NoError(t, task.Run(ctx))

	// OLD log: data dir must be gone.
	_, statErr := os.Stat(oldDir)
	assert.True(t, os.IsNotExist(statErr), "old log data dir should have been removed")

	// OLD log: gate entry must be pruned.
	store.spMu.RLock()
	_, oldPresent := store.deletingLogs[oldKey]
	store.spMu.RUnlock()
	assert.False(t, oldPresent, "old log gate entry should have been pruned")

	// OLD log: marker file must be gone.
	_, markerStatErr := os.Stat(markerPath(root, oldMarker))
	assert.True(t, os.IsNotExist(markerStatErr), "old log marker should have been removed")

	// FRESH log: data dir must still exist.
	_, freshStatErr := os.Stat(freshDir)
	assert.NoError(t, freshStatErr, "fresh log data dir should still exist")

	// FRESH log: gate entry must still be present.
	store.spMu.RLock()
	_, freshPresent := store.deletingLogs[freshKey]
	store.spMu.RUnlock()
	assert.True(t, freshPresent, "fresh log gate entry should still be present")

	// FRESH log: marker must still be present (scanDeleteMarkers returns exactly 1).
	remaining, err := scanDeleteMarkers(context.Background(), root)
	require.NoError(t, err)
	require.Len(t, remaining, 1, "exactly one marker (fresh) should remain")
	assert.Equal(t, int64(8), remaining[0].LogId)
}

// TestDeletedLogReclaimTask_ReclaimsInstance verifies that the reclaim task:
//   - removes the local instance data directory for an instance-level marker,
//   - prunes the deletingInstances gate entry (read under spMu.RLock), and
//   - removes the marker file (scanDeleteMarkers returns empty).
func TestDeletedLogReclaimTask_ReclaimsInstance(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := createTestLogStore()
	root := t.TempDir()
	store.cfg.Woodpecker.Storage.RootPath = root
	store.cfg.Woodpecker.Storage.Type = "local"

	// Write an instance marker deleted 2 hours ago (past any reasonable grace).
	m := deleteMarker{
		Bucket:    "b",
		RootPath:  "r",
		Instance:  true,
		DeletedAt: time.Now().Add(-2 * time.Hour).Unix(),
	}
	require.NoError(t, writeDeleteMarker(context.Background(), root, m))

	// Create the local instance data directory with a data file inside.
	instanceDir := localInstanceDataDir(store.cfg, "b", "r")
	require.NotEmpty(t, instanceDir)
	require.NoError(t, os.MkdirAll(instanceDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(instanceDir, "data.log"), []byte("instance data"), 0o644))

	// Prime the in-memory gate.
	instanceKey := GetInstanceKey("b", "r")
	store.spMu.Lock()
	store.deletingInstances[instanceKey] = struct{}{}
	store.spMu.Unlock()

	// Run the reclaim task with a 1-hour grace (2-hour-old marker is past grace).
	task := newDeletedLogReclaimTask(store, time.Hour)
	require.NoError(t, task.Run(ctx))

	// Instance data directory must be gone.
	_, statErr := os.Stat(instanceDir)
	assert.True(t, os.IsNotExist(statErr), "instance data dir should have been removed")

	// Gate entry must be pruned.
	store.spMu.RLock()
	_, present := store.deletingInstances[instanceKey]
	store.spMu.RUnlock()
	assert.False(t, present, "deletingInstances gate entry should have been pruned")

	// Marker file must be gone.
	remaining, err := scanDeleteMarkers(context.Background(), root)
	require.NoError(t, err)
	assert.Empty(t, remaining, "no markers should remain after reclaim")
}

// TestDeletedLogReclaimTask_MinioModeNoLocalButPrunes verifies that in minio/object-storage
// mode (no local data directory), the reclaim task:
//   - does NOT panic or return an error,
//   - removes the delete marker file, and
//   - prunes the deletingLogs gate entry.
//
// In minio mode localLogDataDir returns "" so there is nothing to os.RemoveAll; the task
// must still advance through the prune logic and clean up the marker + gate.
func TestDeletedLogReclaimTask_MinioModeNoLocalButPrunes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := createTestLogStore()
	root := t.TempDir()
	store.cfg.Woodpecker.Storage.RootPath = root
	store.cfg.Woodpecker.Storage.Type = "minio"

	// Write a LOG marker for logId 5, deleted 2 hours ago (past grace).
	m := deleteMarker{
		Bucket:    "b",
		RootPath:  "r",
		LogId:     5,
		Instance:  false,
		DeletedAt: time.Now().Add(-2 * time.Hour).Unix(),
	}
	require.NoError(t, writeDeleteMarker(context.Background(), root, m))

	// Confirm minio mode returns no local dir (sanity-check of the test assumption).
	dir := localLogDataDir(store.cfg, "b", "r", 5)
	assert.Empty(t, dir, "minio mode must return empty local dir")

	// Prime the in-memory gate.
	logKey := GetLogKey("b", "r", 5)
	store.spMu.Lock()
	store.deletingLogs[logKey] = struct{}{}
	store.spMu.Unlock()

	// Run the reclaim task with a 1-hour grace; must not panic or error.
	task := newDeletedLogReclaimTask(store, time.Hour)
	require.NoError(t, task.Run(ctx))

	// Marker file must be gone.
	remaining, err := scanDeleteMarkers(context.Background(), root)
	require.NoError(t, err)
	assert.Empty(t, remaining, "marker should have been removed even in minio mode")

	// Gate entry must be pruned.
	store.spMu.RLock()
	_, present := store.deletingLogs[logKey]
	store.spMu.RUnlock()
	assert.False(t, present, "deletingLogs gate entry should have been pruned in minio mode")
}

func newDiskWatermarkTestStore(t *testing.T) (*logStore, *diskWatermarkTask) {
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	cfg.Woodpecker.Storage.Type = "service"
	cfg.Woodpecker.Storage.RootPath = t.TempDir()
	ls := NewLogStore(context.Background(), cfg, nil).(*logStore)
	return ls, newDiskWatermarkTask(ls)
}

const testGi = uint64(1024 * 1024 * 1024)

func TestDiskWatermarkTask_Levels(t *testing.T) {
	ls, task := newDiskWatermarkTestStore(t)
	ctx := context.Background()

	// normal: 50% used, plenty free
	task.usageFn = func(string) (uint64, uint64, uint64, error) { return 15 * testGi, 30 * testGi, 15 * testGi, nil }
	require.NoError(t, task.Run(ctx))
	assert.Equal(t, int32(0), ls.diskRejectBps.Load())
	assert.Equal(t, diskLevelNormal, task.lastLevel)

	// warn: 85% used (exact midpoint of the 0.80-0.90 throttle band)
	task.usageFn = func(string) (uint64, uint64, uint64, error) { return 85 * testGi, 100 * testGi, 15 * testGi, nil }
	require.NoError(t, task.Run(ctx))
	assert.InDelta(t, 5000, ls.diskRejectBps.Load(), 2)
	assert.Equal(t, diskLevelWarn, task.lastLevel)

	// blocked: 95% used
	task.usageFn = func(string) (uint64, uint64, uint64, error) { return 95 * testGi, 100 * testGi, 5 * testGi, nil }
	require.NoError(t, task.Run(ctx))
	assert.Equal(t, diskRejectBpsMax, ls.diskRejectBps.Load())
	assert.Equal(t, diskLevelBlocked, task.lastLevel)

	// recovery back to normal clears the rejection probability on the next tick
	task.usageFn = func(string) (uint64, uint64, uint64, error) { return 15 * testGi, 30 * testGi, 15 * testGi, nil }
	require.NoError(t, task.Run(ctx))
	assert.Equal(t, int32(0), ls.diskRejectBps.Load())
	assert.Equal(t, diskLevelNormal, task.lastLevel)
}

func TestDiskWatermarkTask_MinFreeFloorBlocks(t *testing.T) {
	ls, task := newDiskWatermarkTestStore(t)
	// ratio is low (~16%) but free (512Mi) is under the 1Gi floor -> blocked
	task.usageFn = func(string) (uint64, uint64, uint64, error) {
		return 100 * 1024 * 1024, 1 * testGi, 512 * 1024 * 1024, nil
	}
	require.NoError(t, task.Run(context.Background()))
	assert.Equal(t, diskRejectBpsMax, ls.diskRejectBps.Load())
	assert.Equal(t, diskLevelBlocked, task.lastLevel)
}

func TestDiskWatermarkTask_StatErrorKeepsState(t *testing.T) {
	ls, task := newDiskWatermarkTestStore(t)
	ctx := context.Background()

	// drive to blocked
	task.usageFn = func(string) (uint64, uint64, uint64, error) { return 95 * testGi, 100 * testGi, 5 * testGi, nil }
	require.NoError(t, task.Run(ctx))
	require.Equal(t, diskRejectBpsMax, ls.diskRejectBps.Load())

	// transient stat error must NOT unblock (keep last state)
	task.usageFn = func(string) (uint64, uint64, uint64, error) { return 0, 0, 0, fmt.Errorf("statfs boom") }
	require.NoError(t, task.Run(ctx))
	assert.Equal(t, diskRejectBpsMax, ls.diskRejectBps.Load())
}

func TestDiskRejectBpsFor(t *testing.T) {
	p := config.DiskWatermarkPolicyConfig{
		Enabled: true, SoftThresholdRatio: 0.80, HardThresholdRatio: 0.90,
		MinFreeBytes: config.ByteSize(1024), ThrottleEnabled: true,
	}
	assert.Equal(t, int32(0), diskRejectBpsFor(0.50, 1<<30, p))
	assert.Equal(t, int32(0), diskRejectBpsFor(0.7999, 1<<30, p))
	assert.InDelta(t, 0, diskRejectBpsFor(0.80, 1<<30, p), 2)    // ramp start
	assert.InDelta(t, 5000, diskRejectBpsFor(0.85, 1<<30, p), 2) // midpoint
	assert.InDelta(t, 9000, diskRejectBpsFor(0.89, 1<<30, p), 2) // near hard
	assert.Equal(t, diskRejectBpsMax, diskRejectBpsFor(0.90, 1<<30, p))
	assert.Equal(t, diskRejectBpsMax, diskRejectBpsFor(0.95, 1<<30, p))
	assert.Equal(t, diskRejectBpsMax, diskRejectBpsFor(0.10, 1024, p)) // min-free floor wins

	p.ThrottleEnabled = false // two-state mode: nothing in the band, hard still blocks
	assert.Equal(t, int32(0), diskRejectBpsFor(0.85, 1<<30, p))
	assert.Equal(t, diskRejectBpsMax, diskRejectBpsFor(0.90, 1<<30, p))

	p.ThrottleEnabled = true
	p.HardThresholdRatio = 0.80 // soft==hard: empty band, no division by zero
	assert.Equal(t, diskRejectBpsMax, diskRejectBpsFor(0.80, 1<<30, p))
	assert.Equal(t, int32(0), diskRejectBpsFor(0.79, 1<<30, p))
}

func TestDiskWatermarkTask_WarnCooldown(t *testing.T) {
	ls, task := newDiskWatermarkTestStore(t)
	_ = ls
	ctx := context.Background()

	task.usageFn = func(string) (uint64, uint64, uint64, error) { return 85 * testGi, 100 * testGi, 15 * testGi, nil }
	require.NoError(t, task.Run(ctx))
	firstWarnAt := task.lastWarnAt
	assert.False(t, firstWarnAt.IsZero(), "first warn must stamp lastWarnAt")

	// same level within cooldown: no re-log (lastWarnAt unchanged)
	require.NoError(t, task.Run(ctx))
	assert.Equal(t, firstWarnAt, task.lastWarnAt)

	// level escalation logs immediately even within cooldown
	task.usageFn = func(string) (uint64, uint64, uint64, error) { return 95 * testGi, 100 * testGi, 5 * testGi, nil }
	require.NoError(t, task.Run(ctx))
	assert.NotEqual(t, firstWarnAt, task.lastWarnAt, "level change must re-stamp lastWarnAt")
}

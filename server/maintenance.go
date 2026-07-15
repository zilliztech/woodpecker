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
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/hardware"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/metrics"
)

// MaintenanceTask is one periodic, idempotent self-maintenance routine run by the
// NodeMaintenanceManager (e.g. reclaiming deleted logs' local data, idle-processor cleanup).
type MaintenanceTask interface {
	Name() string
	Interval() time.Duration
	Run(ctx context.Context) error
}

// NodeMaintenanceManager runs a set of MaintenanceTasks, each on its own goroutine+ticker
// with panic isolation, tied to the node lifecycle. It is the server's autonomous means to
// act (self-inspection / self-gc) even in a heavy-client architecture.
type NodeMaintenanceManager struct {
	ctx    context.Context
	cancel context.CancelFunc
	tasks  []MaintenanceTask
	wg     sync.WaitGroup
}

func NewNodeMaintenanceManager(ctx context.Context) *NodeMaintenanceManager {
	c, cancel := context.WithCancel(ctx)
	return &NodeMaintenanceManager{ctx: c, cancel: cancel}
}

// Register adds a task. Call before Start.
func (m *NodeMaintenanceManager) Register(t MaintenanceTask) {
	m.tasks = append(m.tasks, t)
}

// Start launches each registered task on its own goroutine. Each task runs once immediately,
// then on its interval.
func (m *NodeMaintenanceManager) Start() {
	for _, t := range m.tasks {
		m.wg.Add(1)
		go m.runTask(t)
	}
}

// Stop cancels all tasks and waits for them to finish.
func (m *NodeMaintenanceManager) Stop() {
	m.cancel()
	m.wg.Wait()
}

func (m *NodeMaintenanceManager) runTask(t MaintenanceTask) {
	defer m.wg.Done()
	m.runOnce(t) // run once immediately at startup
	interval := t.Interval()
	if interval <= 0 {
		logger.Ctx(m.ctx).Warn("maintenance task has non-positive interval; clamping to 1m",
			zap.String("task", t.Name()), zap.Duration("interval", interval))
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.runOnce(t)
		}
	}
}

// runOnce executes one pass with panic recovery so one task cannot crash the others.
func (m *NodeMaintenanceManager) runOnce(t MaintenanceTask) {
	defer func() {
		if r := recover(); r != nil {
			logger.Ctx(m.ctx).Error("maintenance task panicked",
				zap.String("task", t.Name()), zap.Any("panic", r))
		}
	}()
	if err := t.Run(m.ctx); err != nil {
		logger.Ctx(m.ctx).Warn("maintenance task run failed",
			zap.String("task", t.Name()), zap.Error(err))
	}
}

// localLogDataDir returns the node-local data directory for a log, or "" when the
// storage backend keeps no local data (pure object-storage / minio mode).
func localLogDataDir(cfg *config.Configuration, bucket, rootPath string, logId int64) string {
	root := cfg.Woodpecker.Storage.RootPath
	if root == "" {
		return ""
	}
	logIdStr := strconv.FormatInt(logId, 10)
	switch {
	case cfg.Woodpecker.Storage.IsStorageService():
		return path.Join(root, bucket, rootPath, logIdStr)
	case cfg.Woodpecker.Storage.IsStorageLocal():
		return path.Join(root, rootPath, logIdStr)
	default:
		return ""
	}
}

// localSegmentDataDir returns the node-local directory for a single segment, or "" when
// the storage backend keeps no local data (pure object-storage / minio mode).
func localSegmentDataDir(cfg *config.Configuration, bucket, rootPath string, logId, segId int64) string {
	logDir := localLogDataDir(cfg, bucket, rootPath, logId)
	if logDir == "" {
		return ""
	}
	return filepath.Join(logDir, strconv.FormatInt(segId, 10))
}

// localInstanceDataDir returns the node-local data directory for a whole instance.
func localInstanceDataDir(cfg *config.Configuration, bucket, rootPath string) string {
	root := cfg.Woodpecker.Storage.RootPath
	if root == "" {
		return ""
	}
	switch {
	case cfg.Woodpecker.Storage.IsStorageService():
		return path.Join(root, bucket, rootPath)
	case cfg.Woodpecker.Storage.IsStorageLocal():
		return path.Join(root, rootPath)
	default:
		return ""
	}
}

// idleProcessorCleanupTask is the migrated background idle-processor cleanup (formerly
// backgroundCleanupLoop). It evicts segment processors idle longer than MaxIdleTime.
type idleProcessorCleanupTask struct {
	store *logStore
}

func newIdleProcessorCleanupTask(store *logStore) *idleProcessorCleanupTask {
	return &idleProcessorCleanupTask{store: store}
}

func (i *idleProcessorCleanupTask) Name() string { return "idle-processor-cleanup" }

func (i *idleProcessorCleanupTask) Interval() time.Duration {
	return i.store.cfg.Woodpecker.Logstore.ProcessorCleanupPolicy.CleanupInterval.Duration.Duration()
}

func (i *idleProcessorCleanupTask) Run(ctx context.Context) error {
	maxIdle := i.store.cfg.Woodpecker.Logstore.ProcessorCleanupPolicy.MaxIdleTime.Duration.Duration()
	i.store.performBackgroundCleanup(maxIdle)
	return nil
}

// deletedLogReclaimTask reclaims the LOCAL data of logs/instances marked deleted more than
// `grace` ago, then removes the marker and prunes the in-memory deleting-set entry. Object
// storage is never touched here (that is an explicit CleanData operation, a later plan).
type deletedLogReclaimTask struct {
	store *logStore
	grace time.Duration
}

func newDeletedLogReclaimTask(store *logStore, grace time.Duration) *deletedLogReclaimTask {
	return &deletedLogReclaimTask{store: store, grace: grace}
}

func (r *deletedLogReclaimTask) Name() string { return "deleted-log-reclaim" }
func (r *deletedLogReclaimTask) Interval() time.Duration {
	return r.store.cfg.Woodpecker.Logstore.MaintenanceStrategy.DeleteReclaimInterval.Duration.Duration()
}

func (r *deletedLogReclaimTask) Run(ctx context.Context) error {
	root := r.store.cfg.Woodpecker.Storage.RootPath
	if root == "" {
		return nil
	}
	markers, err := scanDeleteMarkers(ctx, root)
	if err != nil {
		return err
	}
	cutoff := time.Now().Add(-r.grace).Unix()
	inGrace := 0
	earliestDue := int64(0)
	for _, m := range markers {
		if m.DeletedAt > cutoff {
			// still within grace
			inGrace++
			if due := m.DeletedAt + int64(r.grace.Seconds()); earliestDue == 0 || due < earliestDue {
				earliestDue = due
			}
			logger.Ctx(ctx).Info("reclaim: marker still within grace period, skipping",
				zap.String("bucket", m.Bucket), zap.String("rootPath", m.RootPath),
				zap.Int64("logId", m.LogId), zap.Bool("instance", m.Instance),
				zap.Int64("deletedAt", m.DeletedAt), zap.Int64("cutoff", cutoff))
			continue
		}
		var dir, key string
		if m.Instance {
			dir = localInstanceDataDir(r.store.cfg, m.Bucket, m.RootPath)
			key = GetInstanceKey(m.Bucket, m.RootPath)
		} else {
			dir = localLogDataDir(r.store.cfg, m.Bucket, m.RootPath, m.LogId)
			key = GetLogKey(m.Bucket, m.RootPath, m.LogId)
		}
		if dir != "" {
			if rmErr := os.RemoveAll(dir); rmErr != nil {
				logger.Ctx(ctx).Warn("reclaim: failed to remove local data; will retry next pass",
					zap.String("dir", dir), zap.Error(rmErr))
				continue // keep the marker so we retry; do not prune the gate
			}
			logger.Ctx(ctx).Info("reclaim: removed local data directory",
				zap.String("dir", dir), zap.String("key", key), zap.Bool("instance", m.Instance))
		}
		// Remove the marker and prune the in-memory gate ATOMICALLY under spMu so a
		// concurrent EvictLog (which re-adds the gate under spMu) cannot interleave and
		// leave a marker-on-disk-but-no-gate state (which would silently resume serving).
		r.store.spMu.Lock()
		if rmErr := removeDeleteMarker(ctx, root, m); rmErr != nil {
			r.store.spMu.Unlock()
			logger.Ctx(ctx).Warn("reclaim: failed to remove marker; will retry next pass", zap.Error(rmErr))
			continue
		}
		if m.Instance {
			delete(r.store.deletingInstances, key)
		} else {
			delete(r.store.deletingLogs, key)
		}
		r.store.spMu.Unlock()
		logger.Ctx(ctx).Info("reclaimed deleted log/instance local data", zap.String("key", key), zap.Bool("instance", m.Instance))
	}
	if inGrace > 0 {
		logger.Ctx(ctx).Info("reclaim pass: markers still within grace period",
			zap.Int("count", inGrace),
			zap.Int64("earliestDueUnix", earliestDue),
			zap.Duration("gracePeriod", r.grace))
	}
	return nil
}

// Disk watermark backpressure levels reported via WpLogStoreWriteBackpressureState.
const (
	diskLevelNormal  = 0
	diskLevelWarn    = 1
	diskLevelBlocked = 2
)

// diskWatermarkWarnCooldown rate-limits repeated warn logs while the node stays
// at or above the soft watermark.
const diskWatermarkWarnCooldown = time.Minute

// diskRejectBpsMax is full rejection in basis points.
const diskRejectBpsMax = int32(10000)

// diskRejectBpsFor maps a disk sample to an append-rejection probability in
// basis points: 0 below soft; a linear ramp across the soft→hard band when
// throttling is enabled; full rejection at/above hard or at/below the
// absolute free floor (regardless of ThrottleEnabled).
func diskRejectBpsFor(ratio float64, free uint64, p config.DiskWatermarkPolicyConfig) int32 {
	if free <= uint64(p.MinFreeBytes.Int64()) || ratio >= p.HardThresholdRatio {
		return diskRejectBpsMax
	}
	if !p.ThrottleEnabled || ratio < p.SoftThresholdRatio || p.HardThresholdRatio <= p.SoftThresholdRatio {
		return 0
	}
	frac := (ratio - p.SoftThresholdRatio) / (p.HardThresholdRatio - p.SoftThresholdRatio)
	return int32(frac * float64(diskRejectBpsMax))
}

// diskWatermarkTask samples the local WAL disk once per interval, evaluates the
// soft/hard watermarks (issue #215) and sets store.diskRejectBps so the append path
// can reject new writes with a retriable error before the disk actually fills.
// The level is a pure function of the current sample (no hysteresis); lastLevel
// and lastWarnAt exist only for log rate-limiting and are touched by a single
// goroutine (the maintenance runner), so they are plain fields.
type diskWatermarkTask struct {
	store      *logStore
	policy     config.DiskWatermarkPolicyConfig
	path       string
	usageFn    func(path string) (used, total, free uint64, err error) // injectable for tests
	lastLevel  int
	lastWarnAt time.Time
}

func newDiskWatermarkTask(store *logStore) *diskWatermarkTask {
	return &diskWatermarkTask{
		store:   store,
		policy:  store.cfg.Woodpecker.Logstore.DiskWatermarkPolicy,
		path:    store.cfg.Woodpecker.Storage.RootPath,
		usageFn: hardware.GetDiskStats,
	}
}

func (t *diskWatermarkTask) Name() string { return "disk-watermark" }

func (t *diskWatermarkTask) Interval() time.Duration {
	return t.policy.SampleInterval.Duration.Duration()
}

func (t *diskWatermarkTask) Run(ctx context.Context) error {
	used, _, free, err := t.usageFn(t.path)
	if err != nil || used+free == 0 {
		// Transient stat failure (or empty stats): keep the previous state rather
		// than flapping the gate; the next successful sample corrects it.
		logger.Ctx(ctx).Debug("disk watermark sample unavailable, keeping previous state",
			zap.String("path", t.path), zap.Error(err))
		return nil
	}
	// df semantics: free is statfs Bavail, so used/(used+free) matches what the
	// operator sees in df/kubectl and accounts for ext4 root-reserved blocks.
	ratio := float64(used) / float64(used+free)
	rejectBps := diskRejectBpsFor(ratio, free, t.policy)
	blocked := rejectBps >= diskRejectBpsMax
	level := diskLevelNormal
	if blocked {
		level = diskLevelBlocked
	} else if ratio >= t.policy.SoftThresholdRatio {
		level = diskLevelWarn
	}

	t.store.diskRejectBps.Store(rejectBps)
	metrics.WpLogStoreDiskUsageRatio.WithLabelValues(metrics.NodeID, t.path).Set(ratio)
	metrics.WpLogStoreDiskFreeBytes.WithLabelValues(metrics.NodeID, t.path).Set(float64(free))
	metrics.WpLogStoreWriteBackpressureState.WithLabelValues(metrics.NodeID).Set(float64(level))
	metrics.WpLogStoreWriteRejectProbability.WithLabelValues(metrics.NodeID).Set(float64(rejectBps) / float64(diskRejectBpsMax))

	now := time.Now()
	switch {
	case level > diskLevelNormal && (level != t.lastLevel || now.Sub(t.lastWarnAt) >= diskWatermarkWarnCooldown):
		logger.Ctx(ctx).Warn("local WAL disk usage high: expand the PVC or scale out logstore nodes to bind new PVCs",
			zap.String("path", t.path),
			zap.Float64("usedRatio", ratio),
			zap.Uint64("freeBytes", free),
			zap.Float64("softThreshold", t.policy.SoftThresholdRatio),
			zap.Float64("hardThreshold", t.policy.HardThresholdRatio),
			zap.Bool("writesBlocked", blocked),
			zap.Int32("rejectBps", rejectBps))
		t.lastWarnAt = now
	case level == diskLevelNormal && t.lastLevel > diskLevelNormal:
		logger.Ctx(ctx).Info("local WAL disk usage recovered below watermarks",
			zap.String("path", t.path),
			zap.Float64("usedRatio", ratio),
			zap.Uint64("freeBytes", free))
	}
	t.lastLevel = level
	return nil
}

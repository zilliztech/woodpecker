// Copyright 2025 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package idempotent

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/objectstorage"
	pb "github.com/zilliztech/woodpecker/proto"
)

const (
	// DedupSnapshotPrefix is the prefix for dedup snapshot objects in object storage.
	DedupSnapshotPrefix = "dedup_snapshot"
	// SnapshotFilePrefix is the prefix for snapshot file names.
	SnapshotFilePrefix = "snapshot_"
	// MaxSnapshotsToRetain is the maximum number of snapshots to keep.
	MaxSnapshotsToRetain = 2
)

// DedupSnapshot represents the dedup state snapshot for persistence.
type DedupSnapshot struct {
	LogId             int64
	SnapshotTimestamp time.Time
	Config            DedupWindowConfig
	SegmentWatermarks map[int64]int64 // segmentId -> lastEntryId
	Entries           []*DedupEntry
}

// DedupSnapshotManager handles snapshot persistence to object storage.
type DedupSnapshotManager interface {
	// Save saves the current dedup window state to object storage.
	Save(ctx context.Context, snapshot *DedupSnapshot) error

	// Load loads the latest snapshot from object storage.
	// Returns nil if no snapshot exists.
	Load(ctx context.Context) (*DedupSnapshot, error)

	// Cleanup removes old snapshots, keeping only the most recent ones.
	Cleanup(ctx context.Context) error

	// GetSnapshotPath returns the base path for snapshots.
	GetSnapshotPath() string
}

// dedupSnapshotManagerImpl is the implementation of DedupSnapshotManager.
type dedupSnapshotManagerImpl struct {
	logId         int64
	bucketName    string
	basePath      string // e.g., "logs/123"
	objectStorage objectstorage.ObjectStorage
	namespace     string
}

// NewDedupSnapshotManager creates a new DedupSnapshotManager.
func NewDedupSnapshotManager(
	logId int64,
	bucketName string,
	basePath string,
	objectStorage objectstorage.ObjectStorage,
) DedupSnapshotManager {
	return &dedupSnapshotManagerImpl{
		logId:         logId,
		bucketName:    bucketName,
		basePath:      basePath,
		objectStorage: objectStorage,
		namespace:     fmt.Sprintf("%s/%s", bucketName, basePath),
	}
}

// GetSnapshotPath returns the base path for snapshots.
func (m *dedupSnapshotManagerImpl) GetSnapshotPath() string {
	return fmt.Sprintf("%s/%s/%d", m.basePath, DedupSnapshotPrefix, m.logId)
}

// getSnapshotObjectKey returns the object key for a snapshot at the given timestamp.
func (m *dedupSnapshotManagerImpl) getSnapshotObjectKey(timestamp time.Time) string {
	return fmt.Sprintf("%s/%s%d", m.GetSnapshotPath(), SnapshotFilePrefix, timestamp.UnixNano())
}

// Save saves the current dedup window state to object storage.
func (m *dedupSnapshotManagerImpl) Save(ctx context.Context, snapshot *DedupSnapshot) error {
	// Convert to protobuf
	pbSnapshot := m.toProto(snapshot)

	// Serialize
	data, err := proto.Marshal(pbSnapshot)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	// Generate object key with timestamp
	objectKey := m.getSnapshotObjectKey(snapshot.SnapshotTimestamp)

	// Upload to object storage
	reader := bytes.NewReader(data)
	err = m.objectStorage.PutObject(ctx, m.bucketName, objectKey, reader, int64(len(data)), m.namespace, fmt.Sprintf("%d", m.logId))
	if err != nil {
		return fmt.Errorf("failed to save snapshot to object storage: %w", err)
	}

	logger.Ctx(ctx).Info("Saved dedup snapshot",
		zap.Int64("logId", m.logId),
		zap.String("objectKey", objectKey),
		zap.Int("entriesCount", len(snapshot.Entries)),
		zap.Int("dataSize", len(data)),
	)

	return nil
}

// Load loads the latest snapshot from object storage.
func (m *dedupSnapshotManagerImpl) Load(ctx context.Context) (*DedupSnapshot, error) {
	// List all snapshots
	snapshotKeys, err := m.listSnapshots(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}

	if len(snapshotKeys) == 0 {
		logger.Ctx(ctx).Info("No dedup snapshot found", zap.Int64("logId", m.logId))
		return nil, nil
	}

	// Find the latest snapshot (sorted by timestamp in filename)
	sort.Strings(snapshotKeys)
	latestKey := snapshotKeys[len(snapshotKeys)-1]

	// Download from object storage
	reader, err := m.objectStorage.GetObject(ctx, m.bucketName, latestKey, 0, -1, m.namespace, fmt.Sprintf("%d", m.logId))
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot from object storage: %w", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot data: %w", err)
	}

	// Deserialize
	pbSnapshot := &pb.DedupSnapshotProto{}
	err = proto.Unmarshal(data, pbSnapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	snapshot := m.fromProto(pbSnapshot)

	logger.Ctx(ctx).Info("Loaded dedup snapshot",
		zap.Int64("logId", m.logId),
		zap.String("objectKey", latestKey),
		zap.Int("entriesCount", len(snapshot.Entries)),
		zap.Time("snapshotTimestamp", snapshot.SnapshotTimestamp),
	)

	return snapshot, nil
}

// Cleanup removes old snapshots, keeping only the most recent ones.
func (m *dedupSnapshotManagerImpl) Cleanup(ctx context.Context) error {
	snapshotKeys, err := m.listSnapshots(ctx)
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %w", err)
	}

	if len(snapshotKeys) <= MaxSnapshotsToRetain {
		return nil
	}

	// Sort by timestamp (older first)
	sort.Strings(snapshotKeys)

	// Remove older snapshots
	toRemove := snapshotKeys[:len(snapshotKeys)-MaxSnapshotsToRetain]
	for _, key := range toRemove {
		err := m.objectStorage.RemoveObject(ctx, m.bucketName, key, m.namespace, fmt.Sprintf("%d", m.logId))
		if err != nil {
			logger.Ctx(ctx).Warn("Failed to remove old snapshot",
				zap.Int64("logId", m.logId),
				zap.String("objectKey", key),
				zap.Error(err),
			)
			// Continue with other deletions
		} else {
			logger.Ctx(ctx).Info("Removed old dedup snapshot",
				zap.Int64("logId", m.logId),
				zap.String("objectKey", key),
			)
		}
	}

	return nil
}

// listSnapshots lists all snapshot object keys for this log.
func (m *dedupSnapshotManagerImpl) listSnapshots(ctx context.Context) ([]string, error) {
	var keys []string
	prefix := m.GetSnapshotPath() + "/"

	walkFn := func(info *objectstorage.ChunkObjectInfo) bool {
		// Filter for snapshot files
		if strings.Contains(info.FilePath, SnapshotFilePrefix) {
			keys = append(keys, info.FilePath)
		}
		return true // Continue walking
	}

	err := m.objectStorage.WalkWithObjects(ctx, m.bucketName, prefix, false, walkFn, m.namespace, fmt.Sprintf("%d", m.logId))
	if err != nil {
		// Check if the error is because the prefix doesn't exist (no snapshots yet)
		if m.objectStorage.IsObjectNotExistsError(err) {
			return nil, nil
		}
		return nil, err
	}

	return keys, nil
}

// toProto converts a DedupSnapshot to its protobuf representation.
func (m *dedupSnapshotManagerImpl) toProto(snapshot *DedupSnapshot) *pb.DedupSnapshotProto {
	// Convert segment watermarks
	watermarks := make([]*pb.SegmentWatermark, 0, len(snapshot.SegmentWatermarks))
	for segId, entryId := range snapshot.SegmentWatermarks {
		watermarks = append(watermarks, &pb.SegmentWatermark{
			SegmentId:   segId,
			LastEntryId: entryId,
		})
	}

	// Convert dedup entries
	entries := make([]*pb.DedupEntryProto, 0, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		entries = append(entries, &pb.DedupEntryProto{
			IdempotencyId:     entry.IdempotencyId,
			SegmentId:         entry.SegmentId,
			EntryId:           entry.EntryId,
			TimestampUnixNano: entry.Timestamp.UnixNano(),
		})
	}

	return &pb.DedupSnapshotProto{
		LogId:                     snapshot.LogId,
		SnapshotTimestampUnixNano: snapshot.SnapshotTimestamp.UnixNano(),
		WindowDurationNano:        snapshot.Config.WindowDuration.Nanoseconds(),
		MaxKeys:                   int32(snapshot.Config.MaxKeys),
		SegmentWatermarks:         watermarks,
		DedupEntries:              entries,
	}
}

// fromProto converts a protobuf DedupSnapshotProto to DedupSnapshot.
func (m *dedupSnapshotManagerImpl) fromProto(pbSnapshot *pb.DedupSnapshotProto) *DedupSnapshot {
	// Convert segment watermarks
	watermarks := make(map[int64]int64, len(pbSnapshot.SegmentWatermarks))
	for _, wm := range pbSnapshot.SegmentWatermarks {
		watermarks[wm.SegmentId] = wm.LastEntryId
	}

	// Convert dedup entries
	entries := make([]*DedupEntry, 0, len(pbSnapshot.DedupEntries))
	for _, pbEntry := range pbSnapshot.DedupEntries {
		entries = append(entries, &DedupEntry{
			IdempotencyId: pbEntry.IdempotencyId,
			SegmentId:     pbEntry.SegmentId,
			EntryId:       pbEntry.EntryId,
			Status:        EntryStatusCommitted, // All persisted entries are committed
			Timestamp:     time.Unix(0, pbEntry.TimestampUnixNano),
		})
	}

	return &DedupSnapshot{
		LogId:             pbSnapshot.LogId,
		SnapshotTimestamp: time.Unix(0, pbSnapshot.SnapshotTimestampUnixNano),
		Config: DedupWindowConfig{
			WindowDuration: time.Duration(pbSnapshot.WindowDurationNano),
			MaxKeys:        int(pbSnapshot.MaxKeys),
		},
		SegmentWatermarks: watermarks,
		Entries:           entries,
	}
}

// CreateSnapshotFromWindow creates a DedupSnapshot from a DedupWindow.
func CreateSnapshotFromWindow(logId int64, window DedupWindow, config DedupWindowConfig) *DedupSnapshot {
	return &DedupSnapshot{
		LogId:             logId,
		SnapshotTimestamp: time.Now(),
		Config:            config,
		SegmentWatermarks: window.GetSegmentWatermarks(),
		Entries:           window.GetCommittedEntries(),
	}
}

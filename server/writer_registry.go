package server

import (
	"context"
	"fmt"

	"github.com/zilliztech/woodpecker/server/processor"
	"github.com/zilliztech/woodpecker/server/storage"
)

// Compile-time assertion.
var _ storage.WriterRegistry = (*logStore)(nil)

// ListWriterSnapshots returns snapshots of all active writers across all segments.
func (l *logStore) ListWriterSnapshots(_ context.Context, filter storage.WriterFilter) []storage.WriterSnapshot {
	l.spMu.RLock()
	defer l.spMu.RUnlock()

	var result []storage.WriterSnapshot
	for _, segMap := range l.segmentProcessors {
		for _, sp := range segMap {
			snap := sp.GetWriterSnapshot()
			if snap == nil {
				continue
			}
			if filter.LogID != nil && snap.LogID != *filter.LogID {
				continue
			}
			if filter.Writable != nil && snap.Writable != *filter.Writable {
				continue
			}
			result = append(result, *snap)
		}
	}
	return result
}

// GetWriterSnapshotDetailed returns detailed snapshot for a specific log/segment.
func (l *logStore) GetWriterSnapshotDetailed(_ context.Context, logID, segmentID int64) (*storage.WriterSnapshotDetailed, error) {
	sp := l.findSegmentProcessor(logID, segmentID)
	if sp == nil {
		return nil, fmt.Errorf("segment %d:%d not found", logID, segmentID)
	}
	snap := sp.GetWriterSnapshotDetailed()
	if snap == nil {
		return nil, fmt.Errorf("writer not active for log %d segment %d", logID, segmentID)
	}
	return snap, nil
}

// ForceFlush forces a sync on the specified writer (or all if logID=0, segmentID=0).
// Note: SegmentProcessor does not currently expose a Sync method, so this
// operation is accepted but has no effect. A future enhancement will wire
// this to the underlying Writer.Sync().
func (l *logStore) ForceFlush(_ context.Context, _, _ int64) error {
	return fmt.Errorf("force-flush is not yet supported (operation accepted but no-op)")
}

// ForceFence forces a fence on the specified writer.
func (l *logStore) ForceFence(ctx context.Context, logID, segmentID int64, _ string) error {
	sp := l.findSegmentProcessor(logID, segmentID)
	if sp == nil {
		return fmt.Errorf("segment %d:%d not found", logID, segmentID)
	}
	_, err := sp.Fence(ctx)
	return err
}

// ForceCompact forces compaction on the specified writer.
func (l *logStore) ForceCompact(ctx context.Context, logID, segmentID int64) error {
	sp := l.findSegmentProcessor(logID, segmentID)
	if sp == nil {
		return fmt.Errorf("segment %d:%d not found", logID, segmentID)
	}
	_, err := sp.Compact(ctx)
	return err
}

// findSegmentProcessor finds a segment processor by logID and segmentID.
func (l *logStore) findSegmentProcessor(logID, segmentID int64) processor.SegmentProcessor {
	l.spMu.RLock()
	defer l.spMu.RUnlock()

	for _, segMap := range l.segmentProcessors {
		if sp, ok := segMap[segmentID]; ok && sp.GetLogId() == logID {
			return sp
		}
	}
	return nil
}

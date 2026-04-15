package storage

import "context"

// WriterRegistry provides access to all active Writers for admin inspection.
type WriterRegistry interface {
	// ListWriterSnapshots returns snapshots of all active writers,
	// optionally filtered by logID and writable flag.
	ListWriterSnapshots(ctx context.Context, filter WriterFilter) []WriterSnapshot

	// GetWriterSnapshotDetailed returns the detailed snapshot for a specific writer.
	GetWriterSnapshotDetailed(ctx context.Context, logID, segmentID int64) (*WriterSnapshotDetailed, error)

	// ForceFlush forces a sync on the specified writer (or all if logID=0, segmentID=0).
	ForceFlush(ctx context.Context, logID, segmentID int64) error

	// ForceFence forces a fence on the specified writer.
	ForceFence(ctx context.Context, logID, segmentID int64, reason string) error

	// ForceCompact forces compaction on the specified writer.
	ForceCompact(ctx context.Context, logID, segmentID int64) error
}

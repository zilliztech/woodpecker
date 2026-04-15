package objectstorage

import (
	"github.com/zilliztech/woodpecker/server/storage"
)

func (f *MinioFileWriter) Snapshot() storage.WriterSnapshot {
	buf := f.buffer.Load()
	var entryCount int64
	if buf != nil {
		entryCount = f.lastEntryID.Load() - f.firstEntryID.Load() + 1
		if entryCount < 0 {
			entryCount = 0
		}
	}
	return storage.WriterSnapshot{
		LogID:      f.logId,
		SegmentID:  f.segmentId,
		Backend:    "objectstorage",
		Writable:   f.storageWritable.Load(),
		Fenced:     f.fenced.Load(),
		Finalized:  f.finalized.Load(),
		Closed:     f.closed.Load(),
		EntryCount: entryCount,
		FirstEntry: f.firstEntryID.Load(),
		LastEntry:  f.lastEntryID.Load(),
		BlockCount: f.lastBlockID.Load(),
	}
}

func (f *MinioFileWriter) SnapshotDetailed() storage.WriterSnapshotDetailed {
	snap := f.Snapshot()

	var bufBytes, bufEntries int64
	if buf := f.buffer.Load(); buf != nil {
		bufBytes = buf.DataSize.Load()
		bufEntries = int64(len(buf.Entries))
	}

	return storage.WriterSnapshotDetailed{
		WriterSnapshot:               snap,
		BufferBytes:                  bufBytes,
		BufferEntries:                bufEntries,
		FlushQueueDepth:              len(f.flushingTaskList),
		FlushQueueCapacity:           cap(f.flushingTaskList),
		LastSubmittedFlushingEntryID: f.lastSubmittedUploadingEntryID.Load(),
		LastSubmittedFlushingBlockID: f.lastSubmittedUploadingBlockID.Load(),
		LastModifiedMS:               f.lastModifiedTime.Load(),
	}
}

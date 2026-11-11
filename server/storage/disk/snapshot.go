package disk

import (
	"github.com/zilliztech/woodpecker/server/storage"
)

func (w *LocalFileWriter) Snapshot() storage.WriterSnapshot {
	entryCount := w.lastEntryID.Load() - w.firstEntryID.Load() + 1
	if entryCount < 0 {
		entryCount = 0
	}
	return storage.WriterSnapshot{
		LogID:      w.logId,
		SegmentID:  w.segmentId,
		Backend:    "disk",
		Writable:   w.storageWritable.Load(),
		Fenced:     w.fenced.Load(),
		Finalized:  w.finalized.Load(),
		Closed:     w.closed.Load(),
		EntryCount: entryCount,
		FirstEntry: w.firstEntryID.Load(),
		LastEntry:  w.lastEntryID.Load(),
		BlockCount: w.currentBlockNumber.Load(),
	}
}

func (w *LocalFileWriter) SnapshotDetailed() storage.WriterSnapshotDetailed {
	snap := w.Snapshot()

	var bufBytes, bufEntries int64
	if buf := w.buffer.Load(); buf != nil {
		bufBytes = buf.DataSize.Load()
		bufEntries = int64(len(buf.Entries))
	}

	var lastModMS int64
	w.mu.Lock()
	if !w.lastModifiedTime.IsZero() {
		lastModMS = w.lastModifiedTime.UnixMilli()
	}
	writtenBytes := w.writtenBytes
	w.mu.Unlock()

	return storage.WriterSnapshotDetailed{
		WriterSnapshot:               snap,
		BufferBytes:                  bufBytes,
		BufferEntries:                bufEntries,
		FlushQueueDepth:              len(w.flushTaskChan),
		FlushQueueCapacity:           cap(w.flushTaskChan),
		WrittenBytes:                 writtenBytes,
		LastSubmittedFlushingEntryID: w.lastSubmittedFlushingEntryID.Load(),
		LastSubmittedFlushingBlockID: w.lastSubmittedFlushingBlockID.Load(),
		LastModifiedMS:               lastModMS,
		Recovered:                    w.recovered.Load(),
	}
}

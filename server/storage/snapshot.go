package storage

// WriterSnapshot is the lightweight state returned by Writer.Snapshot().
type WriterSnapshot struct {
	LogID      int64  `json:"log_id"`
	SegmentID  int64  `json:"segment_id"`
	Backend    string `json:"backend"` // "objectstorage" | "stagedstorage" | "disk"
	Writable   bool   `json:"writable"`
	Fenced     bool   `json:"fenced"`
	Finalized  bool   `json:"finalized"`
	Closed     bool   `json:"closed"`
	SizeBytes  int64  `json:"size_bytes"`
	EntryCount int64  `json:"entry_count"`
	FirstEntry int64  `json:"first_entry_id"`
	LastEntry  int64  `json:"last_entry_id"`
	BlockCount int64  `json:"block_count"`
}

// WriterSnapshotDetailed extends WriterSnapshot with buffer and flush queue info.
type WriterSnapshotDetailed struct {
	WriterSnapshot

	BufferBytes                  int64 `json:"buffer_bytes"`
	BufferEntries                int64 `json:"buffer_entries"`
	FlushQueueDepth              int   `json:"flush_queue_depth"`
	FlushQueueCapacity           int   `json:"flush_queue_capacity"`
	SyncPoolSubmitted            int   `json:"sync_pool_submitted"`
	SyncPoolRunning              int   `json:"sync_pool_running"`
	SyncPoolWaiting              int   `json:"sync_pool_waiting"`
	SyncPoolCapacity             int   `json:"sync_pool_capacity"`
	WrittenBytes                 int64 `json:"written_bytes"`
	LastSubmittedFlushingEntryID int64 `json:"last_submitted_flushing_entry_id"`
	LastSubmittedFlushingBlockID int64 `json:"last_submitted_flushing_block_id"`
	LastModifiedMS               int64 `json:"last_modified_ms"`
	Recovered                    bool  `json:"recovered"`
}

// WriterFilter specifies criteria for listing writers.
type WriterFilter struct {
	LogID    *int64 // nil = all
	Writable *bool  // nil = all
}

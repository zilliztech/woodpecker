package objectstorage

//
//import (
//	"github.com/cockroachdb/errors"
//	"sync"
//)
//
//// DoubleBuffer provides a double buffering mechanism to handle log entries.
//// It maintains two Buffers and switches between them when the active buffer is full.
//// This allows for continuous writing to one buffer while the other is being read and synced.
//type DoubleBuffer struct {
//	mu                sync.Mutex
//	activeIdx         int            // Index of the currently active buffer (0 or 1)
//	buffers           [2]*Buffer     // Two buffers for double buffering
//	readSyncWG        sync.WaitGroup // Synchronization for read and sync
//	bufferSize        int            // Size of each buffer
//	bufferStartOffset uint64         // Start offset of the file, used to calculate the offset of each entry in fragment
//	lastSyncedSeqNum  uint64         // Sequence number of the last entry synced
//}
//
//// NewDoubleBuffer initializes a DoubleBuffer with one buffer and sets up for lazy creation.
//func NewDoubleBuffer(bufferSize int) *DoubleBuffer {
//	return &DoubleBuffer{
//		buffers:    [2]*Buffer{NewBuffer(bufferSize, 0), nil}, // Initialize only one buffer
//		bufferSize: bufferSize,
//	}
//}
//
//// WriteEntry writes an entry to the active buffer. If the buffer is full, it triggers a switch.
//func (db *DoubleBuffer) WriteEntry(payload []byte, seqNum uint64) error {
//	db.mu.Lock()
//	defer db.mu.Unlock()
//
//	activeBuffer := db.getActiveBuffer()
//
//	if err := activeBuffer.WriteEntry(payload); err != nil {
//		if errors.Is(err, ErrIsFull) {
//			db.switchBuffers()
//			// Attempt to write again after switching buffers
//			return db.getActiveBuffer().WriteEntry(payload)
//		}
//		return err
//	}
//
//	return nil
//}
//
//// switchBuffers switches the active buffer, lazily creates the new buffer, and starts syncing the inactive buffer.
//func (db *DoubleBuffer) switchBuffers() {
//	// Lazily create the inactive buffer if it does not exist
//	inactiveIdx := 1 - db.activeIdx
//	if db.buffers[inactiveIdx] == nil {
//		db.buffers[inactiveIdx] = NewBuffer(db.bufferSize, db.bufferStartOffset)
//	}
//
//	// Increment WaitGroup and start syncing the now inactive buffer
//	db.readSyncWG.Add(1)
//	inactiveBuffer := db.buffers[db.activeIdx] // Current active buffer becomes inactive
//	go func(idx int) {
//		defer db.readSyncWG.Done()
//		db.readAndSync(inactiveBuffer)
//		db.mu.Lock()
//		db.buffers[idx] = nil // Release the buffer after syncing is complete
//		db.mu.Unlock()
//	}(db.activeIdx)
//
//	// Switch to the other buffer
//	db.activeIdx = inactiveIdx
//}
//
//// getActiveBuffer returns the current active buffer.
//func (db *DoubleBuffer) getActiveBuffer() *Buffer {
//	return db.buffers[db.activeIdx]
//}
//
//// readAndSync reads all entries from the buffer and performs syncing operations.
//func (db *DoubleBuffer) readAndSync(buffer *Buffer) {
//	for {
//		entries, err := buffer.ReadBytesFromSeqToLast(uint32(db.lastSyncedSeqNum - db.bufferStartOffset))
//		if err != nil {
//			if errors.Is(err, ErrIsEmpty) {
//				break
//			}
//			// Handle read errors as needed
//			return
//		}
//
//		// Sync operation (placeholder for actual syncing logic)
//		db.syncEntry(entries)
//	}
//}
//
//// syncEntry performs the syncing operation for an entry (placeholder for actual implementation).
//// TODO: Implement actual syncing logic, e.g., saving to disk or sending over the network.
//func (db *DoubleBuffer) syncEntry(entry []byte) {
//	// Sync logic, e.g., saving to disk or sending over the network
//	// Placeholder for actual implementation
//}
//
//// WaitForSync waits until all syncing operations are complete.
//func (db *DoubleBuffer) WaitForSync() {
//	db.readSyncWG.Wait()
//}

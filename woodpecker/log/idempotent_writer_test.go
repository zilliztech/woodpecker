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

package log

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/woodpecker/log/idempotent"
)

// mockLogWriter is a mock implementation of LogWriter for testing.
type mockLogWriter struct {
	mu             sync.Mutex
	writeCount     int32
	nextSegmentId  int64
	nextEntryId    int64
	simulateError  error
	writeDelayMs   int
	writtenMsgs    []*WriteMessage
	closeCalled    bool
}

func newMockLogWriter() *mockLogWriter {
	return &mockLogWriter{
		nextSegmentId: 1,
		nextEntryId:   0,
	}
}

func (m *mockLogWriter) Write(ctx context.Context, msg *WriteMessage) *WriteResult {
	ch := m.WriteAsync(ctx, msg)
	return <-ch
}

func (m *mockLogWriter) WriteAsync(ctx context.Context, msg *WriteMessage) <-chan *WriteResult {
	ch := make(chan *WriteResult, 1)

	go func() {
		if m.writeDelayMs > 0 {
			time.Sleep(time.Duration(m.writeDelayMs) * time.Millisecond)
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		atomic.AddInt32(&m.writeCount, 1)
		m.writtenMsgs = append(m.writtenMsgs, msg)

		if m.simulateError != nil {
			ch <- &WriteResult{
				LogMessageId: &LogMessageId{SegmentId: -1, EntryId: -1},
				Err:          m.simulateError,
			}
		} else {
			m.nextEntryId++
			ch <- &WriteResult{
				LogMessageId: &LogMessageId{
					SegmentId: m.nextSegmentId,
					EntryId:   m.nextEntryId,
				},
				Err: nil,
			}
		}
		close(ch)
	}()

	return ch
}

func (m *mockLogWriter) Close(ctx context.Context) error {
	m.closeCalled = true
	return nil
}

func (m *mockLogWriter) getWriteCount() int32 {
	return atomic.LoadInt32(&m.writeCount)
}

// mockSnapshotManager is a mock implementation of DedupSnapshotManager.
type mockSnapshotManager struct {
	savedSnapshot  *idempotent.DedupSnapshot
	loadedSnapshot *idempotent.DedupSnapshot
	loadError      error
	saveError      error
	cleanupCalled  bool
}

func (m *mockSnapshotManager) Save(ctx context.Context, snapshot *idempotent.DedupSnapshot) error {
	if m.saveError != nil {
		return m.saveError
	}
	m.savedSnapshot = snapshot
	return nil
}

func (m *mockSnapshotManager) Load(ctx context.Context) (*idempotent.DedupSnapshot, error) {
	if m.loadError != nil {
		return nil, m.loadError
	}
	return m.loadedSnapshot, nil
}

func (m *mockSnapshotManager) Cleanup(ctx context.Context) error {
	m.cleanupCalled = true
	return nil
}

func (m *mockSnapshotManager) GetSnapshotPath() string {
	return "test/dedup_snapshot"
}

func TestIdempotentWriter_FirstWrite(t *testing.T) {
	mockWriter := newMockLogWriter()
	mockSnapshot := &mockSnapshotManager{}

	config := DefaultIdempotentWriterConfig()
	config.SnapshotInterval = 1 * time.Hour // Disable periodic snapshots for test

	writer := NewIdempotentWriter(1, mockWriter, mockSnapshot, config)
	defer writer.Close(context.Background())

	msg := &WriteMessage{
		Payload:       []byte("test message"),
		IdempotencyId: "test-id-1",
	}

	result := writer.Write(context.Background(), msg)

	require.NoError(t, result.Err)
	assert.Equal(t, int64(1), result.LogMessageId.SegmentId)
	assert.Equal(t, int64(1), result.LogMessageId.EntryId)
	assert.Equal(t, int32(1), mockWriter.getWriteCount())

	// Verify entry is in dedup window
	window := writer.GetDedupWindow()
	entry := window.Get("test-id-1")
	require.NotNil(t, entry)
	assert.Equal(t, idempotent.EntryStatusCommitted, entry.Status)
}

func TestIdempotentWriter_DuplicateWrite(t *testing.T) {
	mockWriter := newMockLogWriter()
	mockSnapshot := &mockSnapshotManager{}

	config := DefaultIdempotentWriterConfig()
	config.SnapshotInterval = 1 * time.Hour

	writer := NewIdempotentWriter(1, mockWriter, mockSnapshot, config)
	defer writer.Close(context.Background())

	msg := &WriteMessage{
		Payload:       []byte("test message"),
		IdempotencyId: "test-id-1",
	}

	// First write
	result1 := writer.Write(context.Background(), msg)
	require.NoError(t, result1.Err)

	// Duplicate write with same idempotencyId
	result2 := writer.Write(context.Background(), msg)
	require.NoError(t, result2.Err)

	// Should return same entryId
	assert.Equal(t, result1.LogMessageId.SegmentId, result2.LogMessageId.SegmentId)
	assert.Equal(t, result1.LogMessageId.EntryId, result2.LogMessageId.EntryId)

	// Inner writer should only be called once
	assert.Equal(t, int32(1), mockWriter.getWriteCount())
}

func TestIdempotentWriter_ConcurrentDuplicateWrites(t *testing.T) {
	mockWriter := newMockLogWriter()
	mockWriter.writeDelayMs = 50 // Add delay to simulate real-world latency
	mockSnapshot := &mockSnapshotManager{}

	config := DefaultIdempotentWriterConfig()
	config.SnapshotInterval = 1 * time.Hour

	writer := NewIdempotentWriter(1, mockWriter, mockSnapshot, config)
	defer writer.Close(context.Background())

	msg := &WriteMessage{
		Payload:       []byte("test message"),
		IdempotencyId: "concurrent-id",
	}

	// Start multiple concurrent writes with same idempotencyId
	var wg sync.WaitGroup
	results := make([]*WriteResult, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = writer.Write(context.Background(), msg)
		}(i)
	}

	wg.Wait()

	// All writes should succeed with the same entryId
	for i := 0; i < 5; i++ {
		require.NoError(t, results[i].Err)
		assert.Equal(t, results[0].LogMessageId.SegmentId, results[i].LogMessageId.SegmentId)
		assert.Equal(t, results[0].LogMessageId.EntryId, results[i].LogMessageId.EntryId)
	}

	// Inner writer should only be called once
	assert.Equal(t, int32(1), mockWriter.getWriteCount())
}

func TestIdempotentWriter_DeriveIdempotencyId(t *testing.T) {
	mockWriter := newMockLogWriter()
	mockSnapshot := &mockSnapshotManager{}

	config := DefaultIdempotentWriterConfig()
	config.SnapshotInterval = 1 * time.Hour

	writer := NewIdempotentWriter(1, mockWriter, mockSnapshot, config)
	defer writer.Close(context.Background())

	// Write without idempotencyId - should be derived from payload hash
	msg := &WriteMessage{
		Payload: []byte("test message"),
		// IdempotencyId not set
	}

	result := writer.Write(context.Background(), msg)
	require.NoError(t, result.Err)

	// The idempotencyId should now be set on the message
	assert.NotEmpty(t, msg.IdempotencyId)

	// Verify it's the MD5 hash of the payload
	expected := deriveIdempotencyId([]byte("test message"))
	assert.Equal(t, expected, msg.IdempotencyId)
}

func TestIdempotentWriter_DifferentIdempotencyIds(t *testing.T) {
	mockWriter := newMockLogWriter()
	mockSnapshot := &mockSnapshotManager{}

	config := DefaultIdempotentWriterConfig()
	config.SnapshotInterval = 1 * time.Hour

	writer := NewIdempotentWriter(1, mockWriter, mockSnapshot, config)
	defer writer.Close(context.Background())

	// Write with different idempotencyIds
	msg1 := &WriteMessage{
		Payload:       []byte("message 1"),
		IdempotencyId: "id-1",
	}
	msg2 := &WriteMessage{
		Payload:       []byte("message 2"),
		IdempotencyId: "id-2",
	}

	result1 := writer.Write(context.Background(), msg1)
	result2 := writer.Write(context.Background(), msg2)

	require.NoError(t, result1.Err)
	require.NoError(t, result2.Err)

	// Should get different entryIds
	assert.NotEqual(t, result1.LogMessageId.EntryId, result2.LogMessageId.EntryId)

	// Inner writer should be called twice
	assert.Equal(t, int32(2), mockWriter.getWriteCount())
}

func TestIdempotentWriter_Close(t *testing.T) {
	mockWriter := newMockLogWriter()
	mockSnapshot := &mockSnapshotManager{}

	config := DefaultIdempotentWriterConfig()
	config.SnapshotInterval = 1 * time.Hour

	writer := NewIdempotentWriter(1, mockWriter, mockSnapshot, config)

	// Write something first
	msg := &WriteMessage{
		Payload:       []byte("test"),
		IdempotencyId: "test-id",
	}
	writer.Write(context.Background(), msg)

	// Close the writer
	err := writer.Close(context.Background())
	require.NoError(t, err)

	// Final snapshot should be saved
	assert.NotNil(t, mockSnapshot.savedSnapshot)

	// Inner writer should be closed
	assert.True(t, mockWriter.closeCalled)
}

func TestDeriveIdempotencyId(t *testing.T) {
	// Same payload should produce same id
	id1 := deriveIdempotencyId([]byte("test payload"))
	id2 := deriveIdempotencyId([]byte("test payload"))
	assert.Equal(t, id1, id2)

	// Different payloads should produce different ids
	id3 := deriveIdempotencyId([]byte("different payload"))
	assert.NotEqual(t, id1, id3)

	// ID should be hex-encoded MD5 (32 chars)
	assert.Equal(t, 32, len(id1))
}

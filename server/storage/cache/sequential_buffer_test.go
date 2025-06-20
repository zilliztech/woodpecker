// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/zilliztech/woodpecker/common/channel"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/werr"
)

// TestNewSequentialBuffer tests the creation of a new SequentialBuffer.
func TestNewSequentialBuffer(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)
	assert.NotNil(t, buffer)
	assert.Equal(t, int64(1), buffer.FirstEntryId)
	assert.Equal(t, int64(1), buffer.ExpectedNextEntryId.Load())
	assert.Len(t, buffer.Entries, 5) // test entry slots
	assert.Equal(t, int64(5), buffer.MaxEntries)

}

// TestNewSequentialBufferWithData tests the creation of a new SequentialBuffer with initial data.
func TestNewSequentialBufferWithData(t *testing.T) {
	// Create BufferEntry data instead of [][]byte
	data := []*BufferEntry{
		{EntryId: 1, Data: []byte("data1"), NotifyChan: nil},
		{EntryId: 2, Data: []byte("data2"), NotifyChan: nil},
		{EntryId: 3, Data: []byte("data3"), NotifyChan: nil},
		{EntryId: 4, Data: []byte("data4"), NotifyChan: nil},
		{EntryId: 5, Data: []byte("data5"), NotifyChan: nil},
	}
	buffer := NewSequentialBufferWithData(1, 0, 1, 5, data)
	assert.NotNil(t, buffer)
	assert.Equal(t, int64(1), buffer.FirstEntryId)
	assert.Equal(t, int64(1), buffer.ExpectedNextEntryId.Load())
	assert.Len(t, buffer.Entries, 5)
	// Check that data is properly converted to BufferEntry format
	for i, expectedData := range data {
		assert.NotNil(t, buffer.Entries[i])
		assert.Equal(t, int64(1+i), buffer.Entries[i].EntryId)
		assert.Equal(t, expectedData.Data, buffer.Entries[i].Data)
		assert.Nil(t, buffer.Entries[i].NotifyChan) // No notification for existing data
	}

	// Create BufferEntry data with nil entries
	data2 := []*BufferEntry{
		nil,
		{EntryId: 3, Data: []byte("data3"), NotifyChan: nil},
		nil,
		{EntryId: 5, Data: []byte("data5"), NotifyChan: nil},
	}
	buffer2 := NewSequentialBufferWithData(1, 0, 2, 5, data2)
	assert.NotNil(t, buffer2)
	assert.Equal(t, int64(2), buffer2.FirstEntryId)
	assert.Equal(t, int64(2), buffer2.ExpectedNextEntryId.Load())
	assert.Len(t, buffer2.Entries, 5)
	// Check entries
	assert.Nil(t, buffer2.Entries[0])
	assert.NotNil(t, buffer2.Entries[1])
	assert.Equal(t, int64(3), buffer2.Entries[1].EntryId)
	assert.Equal(t, []byte("data3"), buffer2.Entries[1].Data)
	assert.Nil(t, buffer2.Entries[2])
	assert.NotNil(t, buffer2.Entries[3])
	assert.Equal(t, int64(5), buffer2.Entries[3].EntryId)
	assert.Equal(t, []byte("data5"), buffer2.Entries[3].Data)
	assert.Nil(t, buffer2.Entries[4])
}

// TestWriteEntry tests the WriteEntry method.
func TestWriteEntry(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)

	// Test writing a valid entry
	id, err := buffer.WriteEntryWithNotify(1, []byte("data1"), nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), id)
	assert.Equal(t, int64(2), buffer.ExpectedNextEntryId.Load())

	// Test writing another valid entry
	id, err = buffer.WriteEntryWithNotify(2, []byte("data2"), nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), id)
	assert.Equal(t, int64(3), buffer.ExpectedNextEntryId.Load())

	// Test writing an entry with an valid ID, but not in sequence
	id, err = buffer.WriteEntryWithNotify(4, []byte("data4"), nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), id)
	assert.Equal(t, int64(3), buffer.ExpectedNextEntryId.Load())

	// Test writing an entry that exceeds the buffer size
	id, err = buffer.WriteEntryWithNotify(7, []byte("data7"), nil)
	assert.Error(t, err)
	assert.Equal(t, int64(-1), id)
	assert.Equal(t, int64(3), buffer.ExpectedNextEntryId.Load())

	// Test writing another valid entry
	id, err = buffer.WriteEntryWithNotify(3, []byte("data3"), nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), id)
	assert.Equal(t, int64(5), buffer.ExpectedNextEntryId.Load())

}

// TestWriteEntryWithNotify tests the WriteEntryWithNotify method.
func TestWriteEntryWithNotify(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)

	// Test writing a valid entry with notification channel
	resultChannel := channel.NewLocalResultChannel("1/0/1")
	id, err := buffer.WriteEntryWithNotify(1, []byte("data1"), resultChannel)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), id)
	assert.Equal(t, int64(2), buffer.ExpectedNextEntryId.Load())

	// Verify the entry was stored correctly
	entry := buffer.Entries[0]
	assert.NotNil(t, entry)
	assert.Equal(t, int64(1), entry.EntryId)
	assert.Equal(t, []byte("data1"), entry.Data)
	assert.NotNil(t, entry.NotifyChan)

	// Test notification
	buffer.NotifyEntriesInRange(context.TODO(), 1, 2, 1, nil) // result >= 0 means success
	result, readErr := resultChannel.ReadResult(context.TODO())
	assert.NoError(t, readErr)
	assert.Equal(t, int64(1), result.SyncedId) // Should receive the entry's own ID
}

// TestReadEntry tests the ReadEntry method.
func TestReadEntry(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)
	buffer.WriteEntryWithNotify(1, []byte("data1"), nil)
	buffer.WriteEntryWithNotify(2, []byte("data2"), nil)
	buffer.WriteEntryWithNotify(4, []byte("data4"), nil)

	// Test reading a valid entry
	entry, err := buffer.ReadEntry(1)
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, int64(1), entry.EntryId)
	assert.Equal(t, []byte("data1"), entry.Data)

	// Test reading another valid entry
	entry, err = buffer.ReadEntry(2)
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, int64(2), entry.EntryId)
	assert.Equal(t, []byte("data2"), entry.Data)

	// Test reading an entry with an invalid ID
	entry, err = buffer.ReadEntry(3)
	assert.Error(t, err)
	assert.True(t, werr.ErrEntryNotFound.Is(err))
	assert.Nil(t, entry)

	// Test reading another valid entry
	entry, err = buffer.ReadEntry(4)
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, int64(4), entry.EntryId)
	assert.Equal(t, []byte("data4"), entry.Data)

	// Test reading an entry that is out of bounds
	entry, err = buffer.ReadEntry(7)
	assert.Error(t, err)
	assert.Nil(t, entry)
}

// TestReadEntriesToLast tests the ReadEntriesToLast method.
func TestReadEntriesToLast(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)
	buffer.WriteEntryWithNotify(1, []byte("data1"), nil)
	buffer.WriteEntryWithNotify(2, []byte("data2"), nil)
	buffer.WriteEntryWithNotify(3, []byte("data3"), nil)
	buffer.WriteEntryWithNotify(5, []byte("data5"), nil)

	// Test reading entries range
	entries, err := buffer.ReadEntriesToLast(3)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(entries))

	// Check entry 3
	assert.NotNil(t, entries[0])
	assert.Equal(t, int64(3), entries[0].EntryId)
	assert.Equal(t, []byte("data3"), entries[0].Data)

	// Check entry 4 (should be nil)
	assert.Nil(t, entries[1])

	// Check entry 5
	assert.NotNil(t, entries[2])
	assert.Equal(t, int64(5), entries[2].EntryId)
	assert.Equal(t, []byte("data5"), entries[2].Data)

	// Test reading entries with an invalid start ID
	entries, err = buffer.ReadEntriesToLast(6)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(entries))

	// Test reading entries with a start ID out of bounds
	entries, err = buffer.ReadEntriesToLast(7)
	assert.Error(t, err)
	assert.Nil(t, entries)

	// Test reading entries to the last entry
	entries, err = buffer.ReadEntriesToLast(1)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(entries))

	// Verify all entries
	assert.NotNil(t, entries[0])
	assert.Equal(t, int64(1), entries[0].EntryId)
	assert.Equal(t, []byte("data1"), entries[0].Data)

	assert.NotNil(t, entries[1])
	assert.Equal(t, int64(2), entries[1].EntryId)
	assert.Equal(t, []byte("data2"), entries[1].Data)

	assert.NotNil(t, entries[2])
	assert.Equal(t, int64(3), entries[2].EntryId)
	assert.Equal(t, []byte("data3"), entries[2].Data)

	assert.Nil(t, entries[3]) // Entry 4 is nil

	assert.NotNil(t, entries[4])
	assert.Equal(t, int64(5), entries[4].EntryId)
	assert.Equal(t, []byte("data5"), entries[4].Data)

	// Test reading entries to the last entry
	entries, err = buffer.ReadEntriesToLast(5)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(entries))
	assert.NotNil(t, entries[0])
	assert.Equal(t, int64(5), entries[0].EntryId)
	assert.Equal(t, []byte("data5"), entries[0].Data)
}

// TestReadEntriesRange tests the ReadEntriesRange method.
func TestReadEntriesRange(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)
	buffer.WriteEntryWithNotify(1, []byte("data1"), nil)
	buffer.WriteEntryWithNotify(2, []byte("data2"), nil)
	buffer.WriteEntryWithNotify(3, []byte("data3"), nil)
	buffer.WriteEntryWithNotify(5, []byte("data5"), nil)

	// Test reading entries in a valid range
	entries, err := buffer.ReadEntriesRange(1, 3)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(entries))

	// Check entry 1
	assert.NotNil(t, entries[0])
	assert.Equal(t, int64(1), entries[0].EntryId)
	assert.Equal(t, []byte("data1"), entries[0].Data)

	// Check entry 2
	assert.NotNil(t, entries[1])
	assert.Equal(t, int64(2), entries[1].EntryId)
	assert.Equal(t, []byte("data2"), entries[1].Data)

	// Test reading a nil entry
	entries, err = buffer.ReadEntriesRange(4, 5)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(entries))
	assert.Nil(t, entries[0]) // Entry 4 is nil

	// Test reading entries with a start ID out of bounds
	entries, err = buffer.ReadEntriesRange(7, 8)
	assert.Error(t, err)
	assert.Nil(t, entries)

	// Test reading all entries
	entries, err = buffer.ReadEntriesRange(1, 6)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(entries))

	// Verify all entries
	assert.NotNil(t, entries[0])
	assert.Equal(t, int64(1), entries[0].EntryId)
	assert.Equal(t, []byte("data1"), entries[0].Data)

	assert.NotNil(t, entries[1])
	assert.Equal(t, int64(2), entries[1].EntryId)
	assert.Equal(t, []byte("data2"), entries[1].Data)

	assert.NotNil(t, entries[2])
	assert.Equal(t, int64(3), entries[2].EntryId)
	assert.Equal(t, []byte("data3"), entries[2].Data)

	assert.Nil(t, entries[3]) // Entry 4 is nil

	assert.NotNil(t, entries[4])
	assert.Equal(t, int64(5), entries[4].EntryId)
	assert.Equal(t, []byte("data5"), entries[4].Data)

	// Test reading single entry
	entries, err = buffer.ReadEntriesRange(5, 6)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(entries))
	assert.NotNil(t, entries[0])
	assert.Equal(t, int64(5), entries[0].EntryId)
	assert.Equal(t, []byte("data5"), entries[0].Data)

	// Test reading single entry
	entries, err = buffer.ReadEntriesRange(1, 2)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(entries))
	assert.NotNil(t, entries[0])
	assert.Equal(t, int64(1), entries[0].EntryId)
	assert.Equal(t, []byte("data1"), entries[0].Data)
}

// TestReset tests the Reset method.
func TestReset(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)

	// Add entries with notification channels
	rc1 := channel.NewLocalResultChannel("1/0/1")
	rc2 := channel.NewLocalResultChannel("1/0/2")

	buffer.WriteEntryWithNotify(1, []byte("data1"), rc1)
	buffer.WriteEntryWithNotify(2, []byte("data2"), rc2)

	buffer.Reset(context.TODO())

	assert.Len(t, buffer.Entries, 5)
	assert.Equal(t, int64(0), buffer.DataSize.Load())
	assert.Equal(t, int64(1), buffer.FirstEntryId)
	assert.Equal(t, int64(1), buffer.ExpectedNextEntryId.Load())

	// Check that all entries are nil after reset
	for _, entry := range buffer.Entries {
		assert.Nil(t, entry)
	}

	// Check that notification channels received error signal
	result1, read1Err := rc1.ReadResult(context.TODO())
	assert.NoError(t, read1Err)
	assert.Equal(t, int64(-1), result1.SyncedId)
	result2, read2Err := rc2.ReadResult(context.TODO())
	assert.NoError(t, read2Err)
	assert.Equal(t, int64(-1), result2.SyncedId)
}

// TestNotifyEntriesInRange tests the NotifyEntriesInRange method.
func TestNotifyEntriesInRange(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)

	// Add entries with notification channels
	rc1 := channel.NewLocalResultChannel("1/0/1")
	rc2 := channel.NewLocalResultChannel("1/0/2")
	rc3 := channel.NewLocalResultChannel("1/0/4")

	buffer.WriteEntryWithNotify(1, []byte("data1"), rc1)
	buffer.WriteEntryWithNotify(2, []byte("data2"), rc2)
	buffer.WriteEntryWithNotify(4, []byte("data4"), rc3)

	// Notify entries 1-3 (should notify rc1 and rc2, but not rc3)
	buffer.NotifyEntriesInRange(context.TODO(), 1, 3, 100, nil) // result >= 0 means success

	// Check notifications - each entry should receive its own ID
	result1, err1 := rc1.ReadResult(context.TODO())
	assert.NoError(t, err1)
	assert.Equal(t, int64(1), result1.SyncedId) // Entry 1 receives its own ID

	result2, err2 := rc2.ReadResult(context.TODO())
	assert.NoError(t, err2)
	assert.Equal(t, int64(2), result2.SyncedId) // Entry 2 receives its own ID

	// rc3 should not have received notification yet (we can't easily test this with the current interface)
	// Instead, we'll test the next notification

	// Notify entry 4
	buffer.NotifyEntriesInRange(context.TODO(), 4, 5, 200, nil) // result >= 0 means success
	result3, err3 := rc3.ReadResult(context.TODO())
	assert.NoError(t, err3)
	assert.Equal(t, int64(4), result3.SyncedId) // Entry 4 receives its own ID
}

// TestNotifyAllPendingEntries tests the NotifyAllPendingEntries method.
func TestNotifyAllPendingEntries(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)

	// Add entries with notification channels
	rc1 := channel.NewLocalResultChannel("1/0/1")
	rc2 := channel.NewLocalResultChannel("1/0/2")
	rc3 := channel.NewLocalResultChannel("1/0/4")

	buffer.WriteEntryWithNotify(1, []byte("data1"), rc1)
	buffer.WriteEntryWithNotify(2, []byte("data2"), rc2)
	buffer.WriteEntryWithNotify(4, []byte("data4"), rc3)

	// Notify all pending entries
	buffer.NotifyAllPendingEntries(context.TODO(), 300, nil) // result >= 0 means success

	// Check all notifications - each entry should receive its own ID
	result1, err1 := rc1.ReadResult(context.TODO())
	assert.NoError(t, err1)
	assert.Equal(t, int64(1), result1.SyncedId) // Entry 1 receives its own ID

	result2, err2 := rc2.ReadResult(context.TODO())
	assert.NoError(t, err2)
	assert.Equal(t, int64(2), result2.SyncedId) // Entry 2 receives its own ID

	result3, err3 := rc3.ReadResult(context.TODO())
	assert.NoError(t, err3)
	assert.Equal(t, int64(4), result3.SyncedId) // Entry 4 receives its own ID
}

// TestEntryIdDebugging demonstrates how the EntryId field helps with debugging
func TestEntryIdDebugging(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 10, 5)

	// Add entries with notification channels
	rc1 := channel.NewLocalResultChannel("1/0/10")
	rc2 := channel.NewLocalResultChannel("1/0/12")
	rc3 := channel.NewLocalResultChannel("1/0/14")

	// Write entries with different IDs
	buffer.WriteEntryWithNotify(10, []byte("data10"), rc1)
	buffer.WriteEntryWithNotify(12, []byte("data12"), rc2)
	buffer.WriteEntryWithNotify(14, []byte("data14"), rc3)

	// Verify EntryId fields are set correctly
	assert.Equal(t, int64(10), buffer.Entries[0].EntryId)
	assert.Equal(t, int64(12), buffer.Entries[2].EntryId)
	assert.Equal(t, int64(14), buffer.Entries[4].EntryId)

	// Test notification with logging (the debug output will show EntryIds)
	buffer.NotifyEntriesInRange(context.TODO(), 10, 13, 100, nil)

	// Check notifications - each entry should receive its own ID
	result1, err1 := rc1.ReadResult(context.TODO())
	assert.NoError(t, err1)
	assert.Equal(t, int64(10), result1.SyncedId) // Entry 10 receives its own ID

	result2, err2 := rc2.ReadResult(context.TODO())
	assert.NoError(t, err2)
	assert.Equal(t, int64(12), result2.SyncedId) // Entry 12 receives its own ID

	// Test NotifyAllPendingEntries with remaining entry
	buffer.NotifyAllPendingEntries(context.TODO(), 200, nil) // result >= 0 means success
	result3, err3 := rc3.ReadResult(context.TODO())
	assert.NoError(t, err3)
	assert.Equal(t, int64(14), result3.SyncedId) // Entry 14 receives its own ID
}

// TestNotifyWithError tests that error notifications send the error result instead of EntryId
func TestNotifyWithError(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)

	// Add entries with notification channels
	rc1 := channel.NewLocalResultChannel("1/0/1")
	rc2 := channel.NewLocalResultChannel("1/0/2")

	buffer.WriteEntryWithNotify(1, []byte("data1"), rc1)
	buffer.WriteEntryWithNotify(2, []byte("data2"), rc2)

	// Test error notification (result < 0)
	buffer.NotifyEntriesInRange(context.TODO(), 1, 3, -1, fmt.Errorf("test error")) // result < 0 means error

	// Check notifications - should receive error result, not EntryId
	result1, err1 := rc1.ReadResult(context.TODO())
	assert.NoError(t, err1)
	assert.Equal(t, int64(-1), result1.SyncedId) // Should receive error result

	result2, err2 := rc2.ReadResult(context.TODO())
	assert.NoError(t, err2)
	assert.Equal(t, int64(-1), result2.SyncedId) // Should receive error result
}

// TestNotifyAllPendingEntriesWithError tests error notifications for all pending entries
func TestNotifyAllPendingEntriesWithError(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)

	// Add entries with notification channels
	rc1 := channel.NewLocalResultChannel("1/0/1")
	rc2 := channel.NewLocalResultChannel("1/0/2")

	buffer.WriteEntryWithNotify(1, []byte("data1"), rc1)
	buffer.WriteEntryWithNotify(2, []byte("data2"), rc2)

	// Test error notification for all pending entries
	buffer.NotifyAllPendingEntries(context.TODO(), -5, fmt.Errorf("test error")) // result < 0 means error

	// Check notifications - should receive error result, not EntryId
	result1, err1 := rc1.ReadResult(context.TODO())
	assert.NoError(t, err1)
	assert.Equal(t, int64(-5), result1.SyncedId) // Should receive error result

	result2, err2 := rc2.ReadResult(context.TODO())
	assert.NoError(t, err2)
	assert.Equal(t, int64(-5), result2.SyncedId) // Should receive error result
}

// TestNotificationBehaviorDemo demonstrates the new notification behavior
func TestNotificationBehaviorDemo(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 100, 10)

	// Add entries with notification channels
	resultChannels := make([]channel.ResultChannel, 5)
	for i := 0; i < 5; i++ {
		resultChannels[i] = channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", 100+i))
		buffer.WriteEntryWithNotify(int64(100+i), []byte(fmt.Sprintf("data%d", 100+i)), resultChannels[i])
	}

	// Test 1: Successful notification - entries should receive their own IDs
	fmt.Println("=== Test 1: Successful Notification ===")
	buffer.NotifyEntriesInRange(context.TODO(), 100, 103, 1000, nil) // result >= 0 means success

	// Verify first 3 entries received their own IDs
	for i := 0; i < 3; i++ {
		result, err := resultChannels[i].ReadResult(context.TODO())
		assert.NoError(t, err)
		expectedId := int64(100 + i)
		assert.Equal(t, expectedId, result.SyncedId, "Entry %d should receive its own ID %d", 100+i, expectedId)
		fmt.Printf("Entry %d received: %d (expected: %d) ✓\n", 100+i, result.SyncedId, expectedId)
	}

	// Test 2: Error notification - entries should receive the error result
	fmt.Println("\n=== Test 2: Error Notification ===")
	buffer.NotifyEntriesInRange(context.TODO(), 103, 105, -1, fmt.Errorf("test error")) // result < 0 means error

	// Verify last 2 entries received the error result
	for i := 3; i < 5; i++ {
		result, err := resultChannels[i].ReadResult(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(-1), result.SyncedId, "Entry %d should receive error result -1", 100+i)
		fmt.Printf("Entry %d received: %d (expected: -1) ✓\n", 100+i, result.SyncedId)
	}

	fmt.Println("\n=== Test Summary ===")
	fmt.Println("✓ Successful entries (result >= 0): Each entry receives its own EntryId")
	fmt.Println("✓ Failed entries (result < 0): All entries receive the same error result")
}

// TestNotifyEntriesInRangeWithClosedChannels tests notification behavior when channels are closed
func TestNotifyEntriesInRangeWithClosedChannels(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)

	// Add entries with notification channels
	rc1 := channel.NewLocalResultChannel("1/0/1")
	rc2 := channel.NewLocalResultChannel("1/0/2")
	rc3 := channel.NewLocalResultChannel("1/0/3")

	buffer.WriteEntryWithNotify(1, []byte("data1"), rc1)
	buffer.WriteEntryWithNotify(2, []byte("data2"), rc2)
	buffer.WriteEntryWithNotify(3, []byte("data3"), rc3)

	// Close some channels before notification
	rc1.Close(context.TODO())
	rc3.Close(context.TODO())

	// This should not panic even with closed channels
	assert.NotPanics(t, func() {
		buffer.NotifyEntriesInRange(context.TODO(), 1, 4, 100, nil)
	})

	// Only rc2 should receive the notification
	result, err := rc2.ReadResult(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result.SyncedId) // Should receive its own ID

	// Verify closed channels behavior - reading from closed channel should return error
	_, err1 := rc1.ReadResult(context.TODO())
	assert.Error(t, err1) // Should return error because channel is closed

	_, err3 := rc3.ReadResult(context.TODO())
	assert.Error(t, err3) // Should return error because channel is closed
}

// TestNotifyAllPendingEntriesWithClosedChannels tests NotifyAllPendingEntries with closed channels
func TestNotifyAllPendingEntriesWithClosedChannels(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)

	// Add entries with notification channels
	rc1 := channel.NewLocalResultChannel("1/0/1")
	rc2 := channel.NewLocalResultChannel("1/0/2")
	rc3 := channel.NewLocalResultChannel("1/0/3")

	buffer.WriteEntryWithNotify(1, []byte("data1"), rc1)
	buffer.WriteEntryWithNotify(2, []byte("data2"), rc2)
	buffer.WriteEntryWithNotify(3, []byte("data3"), rc3)

	// Close some channels before notification
	rc1.Close(context.TODO())
	rc3.Close(context.TODO())

	// This should not panic even with closed channels
	assert.NotPanics(t, func() {
		buffer.NotifyAllPendingEntries(context.TODO(), 200, nil)
	})

	// Only rc2 should receive the notification
	result, err := rc2.ReadResult(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(2), result.SyncedId) // Should receive its own ID

	// Verify closed channels behavior - reading from closed channel should return error
	_, err1 := rc1.ReadResult(context.TODO())
	assert.Error(t, err1) // Should return error because channel is closed

	_, err3 := rc3.ReadResult(context.TODO())
	assert.Error(t, err3) // Should return error because channel is closed
}

// TestResetWithClosedChannels tests Reset method with closed channels
func TestResetWithClosedChannels(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)

	// Add entries with notification channels
	rc1 := channel.NewLocalResultChannel("1/0/1")
	rc2 := channel.NewLocalResultChannel("1/0/2")
	rc3 := channel.NewLocalResultChannel("1/0/3")

	buffer.WriteEntryWithNotify(1, []byte("data1"), rc1)
	buffer.WriteEntryWithNotify(2, []byte("data2"), rc2)
	buffer.WriteEntryWithNotify(3, []byte("data3"), rc3)

	// Close some channels before reset
	rc1.Close(context.TODO())
	rc3.Close(context.TODO())

	// This should not panic even with closed channels
	assert.NotPanics(t, func() {
		buffer.Reset(context.TODO())
	})

	// Only rc2 should receive the error notification
	result, err := rc2.ReadResult(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(-1), result.SyncedId) // Should receive error result

	// Verify buffer is reset
	assert.Equal(t, int64(0), buffer.DataSize.Load())
	assert.Equal(t, int64(1), buffer.ExpectedNextEntryId.Load())

	// Check that all entries are nil after reset
	for _, entry := range buffer.Entries {
		assert.Nil(t, entry)
	}
}

// TestMixedClosedAndFullChannels tests behavior with both closed and full channels
func TestMixedClosedAndFullChannels(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 5)

	// Add entries with different channel states
	closedRc := channel.NewLocalResultChannel("1/0/1")
	closedRc.Close(context.TODO()) // Closed channel
	fullRc := channel.NewLocalResultChannel("1/0/2")
	normalRc := channel.NewLocalResultChannel("1/0/3")

	buffer.WriteEntryWithNotify(1, []byte("data1"), closedRc)
	buffer.WriteEntryWithNotify(2, []byte("data2"), fullRc)
	buffer.WriteEntryWithNotify(3, []byte("data3"), normalRc)

	// This should not panic or block
	assert.NotPanics(t, func() {
		buffer.NotifyEntriesInRange(context.TODO(), 1, 4, 300, nil)
	})

	// Only normalRc should receive the notification
	result, err := normalRc.ReadResult(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result.SyncedId) // Should receive its own ID

	// Verify fullRc didn't receive anything (because it's unbuffered and no reader)
	// We can't easily test this without blocking, so we'll skip this check
}

// TestConcurrentNotificationWithClosedChannels tests concurrent notifications with closed channels
func TestConcurrentNotificationWithClosedChannels(t *testing.T) {
	buffer := NewSequentialBuffer(1, 0, 1, 10)

	// Add multiple entries
	resultChannels := make([]channel.ResultChannel, 5)
	for i := 0; i < 5; i++ {
		resultChannels[i] = channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", i+1))
		buffer.WriteEntryWithNotify(int64(i+1), []byte(fmt.Sprintf("data%d", i+1)), resultChannels[i])
	}

	// Close some channels
	resultChannels[0].Close(context.TODO())
	resultChannels[2].Close(context.TODO())
	resultChannels[4].Close(context.TODO())

	// Run multiple notifications concurrently
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(iteration int) {
			defer wg.Done()
			// This should not panic
			assert.NotPanics(t, func() {
				buffer.NotifyEntriesInRange(context.TODO(), 1, 6, int64(100+iteration), nil)
			})
		}(i)
	}

	wg.Wait()

	// Only open channels should have received notifications
	// Note: Due to concurrent access, we can't predict exact values,
	// but we can verify no panics occurred and channels work
	fmt.Println("Concurrent notification test completed without panics")
}

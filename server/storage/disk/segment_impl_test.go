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

package disk

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/channel"
	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
)

func getTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "disk_log_test_*")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

// TestNewDiskSegmentImpl tests the NewDiskSegmentImpl function.
func TestNewDiskSegmentImpl(t *testing.T) {
	tmpDir := getTempDir(t)
	baseDir := filepath.Join(tmpDir, "1/0")
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, baseDir)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), segmentImpl.logId)
	assert.Equal(t, int64(0), segmentImpl.segmentId)
	assert.Equal(t, int64(0), segmentImpl.GetId())
	assert.Equal(t, baseDir, segmentImpl.logFileDir)
	assert.Equal(t, int64(128*1024*1024), segmentImpl.fragmentSize)
	assert.Equal(t, 100000, segmentImpl.maxEntryPerFile)

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestDiskSegmentImplWithOptions tests creating a DiskSegmentImpl with custom options.
func TestDiskSegmentImplWithOptions(t *testing.T) {
	tmpDir := getTempDir(t)
	baseDir := filepath.Join(tmpDir, "1/0")
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, baseDir,
		WithWriteFragmentSize(1024*1024), // 1MB
		WithWriteMaxEntryPerFile(1000),   // 1000 entries
	)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), segmentImpl.logId)
	assert.Equal(t, int64(0), segmentImpl.segmentId)
	assert.Equal(t, int64(0), segmentImpl.GetId())
	assert.Equal(t, baseDir, segmentImpl.logFileDir)
	assert.Equal(t, int64(1024*1024), segmentImpl.fragmentSize)
	assert.Equal(t, 1000, segmentImpl.maxEntryPerFile)

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestAppend tests the Append function.
func TestAppend(t *testing.T) {
	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)

	// Append data
	err = segmentImpl.Append(context.Background(), []byte("test_data_0"))
	assert.NoError(t, err)
	err = segmentImpl.Append(context.Background(), []byte("test_data_1"))
	assert.NoError(t, err)

	// Get last entry ID
	lastEntryID, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	// DiskSegmentImpl implementation increments the entry ID from 0, first append results in ID 0
	assert.Equal(t, lastEntryID, int64(1))

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestAppendAsync tests the AppendAsync function.
func TestAppendAsync(t *testing.T) {
	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)

	// Test with auto-assigned ID (0 means auto-assign, will use internal incrementing ID)
	rc := channel.NewLocalResultChannel("1/0/0")
	entryID, err := segmentImpl.AppendAsync(context.Background(), 0, []byte("test_data"), rc)
	assert.NoError(t, err)
	assert.Equal(t, entryID, int64(0))

	// Wait for result
	result, readErr := rc.ReadResult(context.TODO())
	assert.NoError(t, readErr)
	assert.Equal(t, entryID, result.SyncedId)

	// Get last entry ID after first append
	firstLastID, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, firstLastID, int64(0))

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestMultipleEntriesAppend tests appending multiple entries.
func TestMultipleEntriesAppend(t *testing.T) {
	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)

	// Append multiple entries
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		err = segmentImpl.Append(context.Background(), []byte(fmt.Sprintf("test_data_%d", i)))
		assert.NoError(t, err)
	}

	// Get last entry ID
	lastEntryID, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, lastEntryID, int64(numEntries-1))

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestAppendAsyncMultipleEntries tests appending multiple entries asynchronously.
func TestAppendAsyncMultipleEntries(t *testing.T) {
	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)

	// Append entries with specific IDs
	numEntries := 100
	resultChannels := make([]channel.ResultChannel, 0, numEntries)
	entryIDs := make([]int64, 0, numEntries)
	for i := 0; i < numEntries; i++ {
		entryID := int64(i)
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", i))
		id, err := segmentImpl.AppendAsync(context.Background(), entryID, []byte(fmt.Sprintf("test_data_%d", i)), rc)
		assert.NoError(t, err)
		assert.Equal(t, entryID, id)
		resultChannels = append(resultChannels, rc)
		entryIDs = append(entryIDs, entryID)
	}

	// Wait for all results
	for i, rc := range resultChannels {
		result, readErr := rc.ReadResult(context.TODO())
		assert.NoError(t, readErr)
		assert.Equal(t, entryIDs[i], result.SyncedId)
	}

	// Get last entry ID
	lastEntryID, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, lastEntryID, int64(numEntries)-1)

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestOutOfOrderAppend tests appending entries out of order.
func TestOutOfOrderAppend(t *testing.T) {
	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)

	// Use a larger starting ID to avoid conflicts with auto-assigned IDs
	entryIDs := []int64{5, 3, 8, 1, 0, 2, 7, 6, 4, 9}
	resultChannels := make([]channel.ResultChannel, 0, len(entryIDs))

	// Record data for each ID for later verification
	entryData := make(map[int64][]byte)
	for _, id := range entryIDs {
		data := []byte(fmt.Sprintf("data-%d", id))
		entryData[id] = data
		t.Logf("Appending entry with ID %d and data '%s'", id, string(data))

		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", id))
		assignedID, err := segmentImpl.AppendAsync(context.Background(), id, data, rc)
		assert.NoError(t, err)
		assert.Equal(t, id, assignedID, "Assigned ID should match requested ID")
		resultChannels = append(resultChannels, rc)
	}

	// Wait for all results
	for i, rc := range resultChannels {
		result, readErr := rc.ReadResult(context.TODO())
		assert.NoError(t, readErr)
		assert.Equal(t, entryIDs[i], result.SyncedId, "Result ID should match original ID")
		t.Logf("Received result for ID %d", result.SyncedId)
	}

	// Print the current ID order and data mapping for debugging
	t.Log("ID to Data mapping:")
	for id, data := range entryData {
		t.Logf("ID %d -> %s", id, string(data))
	}

	// Create reader to verify data
	roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roSegmentImpl)
	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,  // Ensure we start before all IDs
		EndSequenceNum:   10, // Ensure we include all IDs
	})
	assert.NoError(t, err)
	defer reader.Close()

	// Collect all read entries
	readEntries := make(map[int64]*proto.LogEntry)
	for {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext(context.TODO())
		if err != nil {
			t.Logf("Error reading entry: %v", err)
			continue
		}
		t.Logf("Read entry with ID %d and data '%s'", entry.EntryId, string(entry.Values))
		readEntries[entry.EntryId] = entry
	}

	// Verify that all written entries were read back
	assert.Equal(t, len(entryIDs), len(readEntries), "Should read back the same number of entries that were written")

	// Verify that all written IDs and data were correctly read back
	for id, expectedData := range entryData {
		entry, ok := readEntries[id]
		assert.True(t, ok, "Entry with ID %d should be read back", id)
		if ok {
			assert.Equal(t, expectedData, entry.Values, "Data for entry ID %d should match", id)
			t.Logf("Verified entry ID %d with data '%s'", id, string(entry.Values))
		}
	}

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestDelayedAppend tests handling of delayed append requests.
func TestDelayedAppend(t *testing.T) {
	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)

	// Starting ID to use
	startID := int64(0)

	// Prepare data
	entryData := make(map[int64][]byte)

	// First send request for ID 2
	data2 := []byte("data-2")
	entryData[startID+2] = data2
	rc2 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", startID+2))
	_, err = segmentImpl.AppendAsync(context.Background(), startID+2, data2, rc2)
	assert.NoError(t, err)

	// Wait a short time to simulate delay
	time.Sleep(100 * time.Millisecond)

	// Send request for ID 1
	data1 := []byte("data-1")
	entryData[startID+1] = data1
	rc1 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", startID+1))
	_, err = segmentImpl.AppendAsync(context.Background(), startID+1, data1, rc1)
	assert.NoError(t, err)

	// Wait a short time to simulate delay
	time.Sleep(100 * time.Millisecond)

	// Send request for ID 0
	data0 := []byte("data-0")
	entryData[startID] = data0
	rc0 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", startID))
	_, err = segmentImpl.AppendAsync(context.Background(), startID, data0, rc0)
	assert.NoError(t, err)

	// Wait for all results
	result0, readErr0 := rc0.ReadResult(context.TODO())
	assert.NoError(t, readErr0)
	assert.Equal(t, startID, result0.SyncedId)

	result1, readErr1 := rc1.ReadResult(context.TODO())
	assert.NoError(t, readErr1)
	assert.Equal(t, startID+1, result1.SyncedId)

	result2, readErr2 := rc2.ReadResult(context.TODO())
	assert.NoError(t, readErr2)
	assert.Equal(t, startID+2, result2.SyncedId)

	// Ensure all data is written
	lastEntryID, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, startID+2, lastEntryID, "Last entry ID should match the highest ID")

	// Create reader to verify data
	roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roSegmentImpl)
	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: startID,
		EndSequenceNum:   startID + 3,
	})
	assert.NoError(t, err)
	defer reader.Close()

	// Collect all read entries
	readEntries := make(map[int64]*proto.LogEntry)
	for {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext(context.TODO())
		if err != nil {
			t.Logf("Error reading entry: %v", err)
			continue
		}
		t.Logf("Read entry with ID %d and data '%s'", entry.EntryId, string(entry.Values))
		readEntries[entry.EntryId] = entry
	}

	// Verify that all written entries were read back
	assert.Equal(t, 3, len(readEntries), "Should read back all 3 entries")

	// Verify that all written IDs and data were correctly read back
	for id, resultData := range readEntries {
		expectedEntry, ok := entryData[id]
		assert.True(t, ok, "Entry with ID %d should be read back", id)
		if ok {
			assert.Equal(t, expectedEntry, resultData.Values, "Data for entry ID %d should match", id)
			t.Logf("Verified entry ID %d with data '%s'", id, string(resultData.Values))
		}
	}

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestOutOfBoundsAppend tests handling of out-of-bounds append requests.
func TestOutOfBoundsAppend(t *testing.T) {
	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)
	{
		// First write 100 entries
		numEntries := 100
		resultChannels := make([]channel.ResultChannel, 0, numEntries)
		entryIDs := make([]int64, 0, numEntries)
		for i := 0; i < numEntries; i++ {
			entryID := int64(i)
			rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryID))
			id, err := segmentImpl.AppendAsync(context.Background(), entryID, []byte(fmt.Sprintf("test_data_%d", i)), rc)
			assert.NoError(t, err)
			assert.Equal(t, entryID, id)
			resultChannels = append(resultChannels, rc)
			entryIDs = append(entryIDs, entryID)
		}
		err := segmentImpl.Sync(context.TODO())
		assert.NoError(t, err)
		lastEntryId, err := segmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(numEntries-1), lastEntryId)
	}

	// Starting ID to use
	startID := int64(100)
	entryData := make(map[int64][]byte)

	// First send a normal ID request
	data1 := []byte("data-1")
	entryData[startID+1] = data1
	rc1 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", startID+1))
	_, err = segmentImpl.AppendAsync(context.Background(), startID+1, data1, rc1)
	assert.NoError(t, err)
	t.Logf("Appending entry with ID %d and data '%s'", startID+1, string(data1))

	// Send a request with an ID smaller than the current ID
	data0 := []byte("data-0")
	entryData[startID+0] = data0
	rc2 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", startID+0))
	_, err = segmentImpl.AppendAsync(context.Background(), startID+0, data0, rc2)
	assert.NoError(t, err) // This request should be accepted
	t.Logf("Appending entry with ID %d and data '%s'", startID+0, string(data0))

	// Send a request with a very small ID, which has already been persisted
	dataN1 := []byte("data-N1")
	entryData[startID-1] = dataN1
	rc3 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", startID-1))
	syncedId, err := segmentImpl.AppendAsync(context.Background(), startID-1, dataN1, rc3)
	assert.NoError(t, err) // This request should be accepted, indicating it's already been added
	assert.Equal(t, syncedId, startID-1)
	t.Logf("Appending entry with ID %d and data '%s'", startID-1, string(dataN1))

	// Send a request with an ID greater than the buffer window
	dataN2 := []byte("data-N2")
	entryData[startID+1_00000_0000] = dataN1
	rc4 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", startID+1_00000_0000))
	_, err = segmentImpl.AppendAsync(context.Background(), startID+1_00000_0000, dataN2, rc4)
	assert.Error(t, err) // This request should be immediately rejected, indicating it exceeds the buffer window
	assert.True(t, werr.ErrWriteBufferFull.Is(err))
	t.Logf("Appending entry with ID %d and data '%s'", startID+1_00000_0000, string(dataN2))

	// Wait for all results
	result1, readErr1 := rc1.ReadResult(context.TODO())
	assert.NoError(t, readErr1)
	assert.Equal(t, startID+1, result1.SyncedId)
	t.Logf("Received result for ID %d", result1.SyncedId)

	result2, readErr2 := rc2.ReadResult(context.TODO())
	assert.NoError(t, readErr2)
	assert.Equal(t, startID+0, result2.SyncedId)
	t.Logf("Received result for ID %d", result2.SyncedId)

	result3, readErr3 := rc3.ReadResult(context.TODO())
	assert.NoError(t, readErr3)
	assert.Equal(t, startID-1, result3.SyncedId) // This is directly returned as successful because it was already persisted
	t.Logf("Received result for ID %d", result3.SyncedId)

	// Ensure all data is written
	syncErr := segmentImpl.Sync(context.Background())
	assert.NoError(t, syncErr)

	// Create reader to verify data
	roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roSegmentImpl)
	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: startID + 0,
		EndSequenceNum:   startID + 2,
	})
	assert.NoError(t, err)
	defer reader.Close()

	// Collect all read entries
	readEntries := make(map[int64]*proto.LogEntry)
	for {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext(context.TODO())
		if err != nil {
			t.Logf("Error reading entry: %v", err)
			continue
		}
		t.Logf("Read entry with ID %d and data '%s'", entry.EntryId, string(entry.Values))
		readEntries[entry.EntryId] = entry
	}

	// Verify that all written entries were read back
	assert.Equal(t, 2, len(readEntries), "Should read back all 3 entries")

	// Verify that all written IDs and data were correctly read back
	for id, resultData := range readEntries {
		expectedEntry, ok := entryData[id]
		assert.True(t, ok, "Entry with ID %d should be read back", id)
		if ok {
			assert.Equal(t, expectedEntry, resultData.Values, "Data for entry ID %d should match", id)
			t.Logf("Verified entry ID %d with data '%s'", id, string(resultData.Values))
		}
	}

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestMixedAppendScenarios tests various mixed scenarios for append requests.
func TestMixedAppendScenarios(t *testing.T) {
	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)

	{
		// First write 100 entries
		numEntries := 100
		resultChannels := make([]channel.ResultChannel, 0, numEntries)
		entryIDs := make([]int64, 0, numEntries)
		for i := 0; i < numEntries; i++ {
			entryID := int64(i)
			rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryID))
			id, err := segmentImpl.AppendAsync(context.Background(), entryID, []byte(fmt.Sprintf("test_data_%d", i)), rc)
			assert.NoError(t, err)
			assert.Equal(t, entryID, id)
			resultChannels = append(resultChannels, rc)
			entryIDs = append(entryIDs, entryID)
		}
		err := segmentImpl.Sync(context.TODO())
		assert.NoError(t, err)
		lastEntryId, err := segmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(numEntries-1), lastEntryId)
	}

	// Use a larger starting ID
	startID := int64(100)
	entryData := make(map[int64][]byte)
	channels := make(map[int64]channel.ResultChannel)

	// Scenario 1: Normal sequential request
	data0 := []byte("data-0")
	entryData[startID] = data0
	rc1 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", startID))
	_, err = segmentImpl.AppendAsync(context.Background(), startID, data0, rc1)
	assert.NoError(t, err)
	channels[startID] = rc1

	// Scenario 2: Out-of-order request
	data2 := []byte("data-2")
	entryData[startID+2] = data2
	rc2 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", startID+2))
	_, err = segmentImpl.AppendAsync(context.Background(), startID+2, data2, rc2)
	assert.NoError(t, err)
	channels[startID+2] = rc2

	// Scenario 3: Delayed request
	time.Sleep(2000 * time.Millisecond)
	data1 := []byte("data-1")
	entryData[startID+1] = data1
	rc3 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", startID+1))
	_, err = segmentImpl.AppendAsync(context.Background(), startID+1, data1, rc3)
	assert.NoError(t, err)
	channels[startID+1] = rc3

	// Scenario 4: Out-of-bounds request
	data5 := []byte("data-5_000_0000")
	rc4 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", startID+5_000_0000))
	_, err = segmentImpl.AppendAsync(context.Background(), startID+5_000_0000, data5, rc4)
	assert.Error(t, err)
	assert.True(t, werr.ErrWriteBufferFull.Is(err))

	// Scenario 5: Fill missing request
	data3 := []byte("data-3")
	entryData[startID+3] = data3
	rc5 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", startID+3))
	_, err = segmentImpl.AppendAsync(context.Background(), startID+3, data3, rc5)
	assert.NoError(t, err)
	channels[startID+3] = rc5

	data4 := []byte("data-4")
	entryData[startID+4] = data4
	rc6 := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", startID+4))
	_, err = segmentImpl.AppendAsync(context.Background(), startID+4, data4, rc6)
	assert.NoError(t, err)
	channels[startID+4] = rc6

	// Wait for all results
	for id, ch := range channels {
		subCtx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
		result, readErr := ch.ReadResult(subCtx)
		cancel()
		assert.NoError(t, readErr)
		assert.Equal(t, id, result.SyncedId, "Result ID should match requested ID")
	}

	// Ensure all data is written
	err = segmentImpl.Sync(context.Background())
	assert.NoError(t, err)

	// Create reader to verify data
	roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roSegmentImpl)
	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: startID,
		EndSequenceNum:   startID + 5,
	})
	assert.NoError(t, err)
	defer reader.Close()

	// Collect all read entries
	readEntries := make(map[int64]*proto.LogEntry)
	for {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext(context.TODO())
		if err != nil {
			t.Logf("Error reading entry: %v", err)
			continue
		}
		t.Logf("Read entry with ID %d and data '%s'", entry.EntryId, string(entry.Values))
		readEntries[entry.EntryId] = entry
	}

	// Verify that all written entries were read back
	assert.Equal(t, len(entryData), len(readEntries), "Should read back all entries")

	// Verify that all written IDs and data were correctly read back
	for id, resultData := range readEntries {
		expectedEntry, ok := entryData[id]
		assert.True(t, ok, "Entry with ID %d should be read back", id)
		if ok {
			assert.Equal(t, expectedEntry, resultData.Values, "Data for entry ID %d should match", id)
			t.Logf("Verified entry ID %d with data '%s'", id, string(resultData.Values))
		}
	}

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestSync tests the Sync function.
func TestSync(t *testing.T) {
	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)

	// Append multiple entries
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		err = segmentImpl.Append(context.Background(), []byte("test_data"))
		assert.NoError(t, err)
	}

	// Sync data
	err = segmentImpl.Sync(context.Background())
	assert.NoError(t, err)

	// Get last entry ID
	lastEntryID, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, lastEntryID, int64(numEntries-1))

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestFragmentRotation tests the basic rotation of fragment files.
func TestFragmentRotation(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir, WithWriteMaxEntryPerFile(10))
	assert.NoError(t, err)
	assert.NotNil(t, segmentImpl)

	entryData := make(map[int64][]byte)
	// Append a few entries
	{
		// First write 100 entries
		numEntries := 100
		resultChannels := make([]channel.ResultChannel, 0, numEntries)
		entryIDs := make([]int64, 0, numEntries)
		for i := 0; i < numEntries; i++ {
			entryID := int64(i)
			entryValue := []byte(fmt.Sprintf("test_data_%d", i))
			entryData[entryID] = entryValue
			rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryID))
			id, err := segmentImpl.AppendAsync(context.Background(), entryID, entryValue, rc)
			assert.NoError(t, err)
			assert.Equal(t, entryID, id)
			resultChannels = append(resultChannels, rc)
			entryIDs = append(entryIDs, entryID)
		}
		err := segmentImpl.Sync(context.TODO())
		assert.NoError(t, err)
		lastEntryId, err := segmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(numEntries-1), lastEntryId)
	}

	// Ensure all data has been written
	err = segmentImpl.Sync(context.Background())
	assert.NoError(t, err)

	// Confirm that 10 fragments were created through rotation
	roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roSegmentImpl)
	_, _, err = roSegmentImpl.fetchROFragments(context.TODO())
	assert.NoError(t, err)
	assert.NotNil(t, roSegmentImpl.fragments)
	assert.Equal(t, 10, len(roSegmentImpl.fragments))

	// Check that at least one fragment file was created
	files, err := os.ReadDir(segmentImpl.logFileDir)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(files), "Expected 10 fragment files")

	// Verify data from various fragments
	// Create reader to verify data
	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,
		EndSequenceNum:   100,
	})
	assert.NoError(t, err)
	defer reader.Close()

	// Collect all read entries
	readEntries := make(map[int64]*proto.LogEntry)
	for {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext(context.TODO())
		if err != nil {
			t.Logf("Error reading entry: %v", err)
			continue
		}
		t.Logf("Read entry with ID %d and data '%s'", entry.EntryId, string(entry.Values))
		readEntries[entry.EntryId] = entry
	}

	// Verify that all written entries were read back
	assert.Equal(t, len(entryData), len(readEntries), "Should read back all entries")

	// Verify that all written IDs and data were correctly read back
	for id, resultData := range readEntries {
		expectedEntry, ok := entryData[id]
		assert.True(t, ok, "Entry with ID %d should be read back", id)
		if ok {
			assert.Equal(t, expectedEntry, resultData.Values, "Data for entry ID %d should match", id)
			t.Logf("Verified entry ID %d with data '%s'", id, string(resultData.Values))
		}
	}

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestNewReader tests creating and using a reader to read ranges
func TestNewReader(t *testing.T) {
	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)

	// Append entries with known data
	entryData := make(map[int64][]byte)
	// Append a few entries
	{
		// First write 100 entries
		numEntries := 100
		resultChannels := make([]channel.ResultChannel, 0, numEntries)
		entryIDs := make([]int64, 0, numEntries)
		for i := 0; i < numEntries; i++ {
			entryID := int64(i)
			entryValue := []byte(fmt.Sprintf("test_data_%d", i))
			entryData[entryID] = entryValue
			rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryID))
			id, err := segmentImpl.AppendAsync(context.Background(), entryID, entryValue, rc)
			assert.NoError(t, err)
			assert.Equal(t, entryID, id)
			resultChannels = append(resultChannels, rc)
			entryIDs = append(entryIDs, entryID)
		}
		err := segmentImpl.Sync(context.TODO())
		assert.NoError(t, err)
		lastEntryId, err := segmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(numEntries-1), lastEntryId)
	}

	// Reader for middle 80 entries
	{
		roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
		assert.NoError(t, err)
		assert.NotNil(t, roSegmentImpl)
		reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
			StartSequenceNum: 10,
			EndSequenceNum:   90, // Read middle 80 entries
		})
		assert.NoError(t, err)
		defer reader.Close()
		readEntries := make(map[int64]*proto.LogEntry)
		for {
			hasNext, err := reader.HasNext(context.TODO())
			assert.NoError(t, err)
			if !hasNext {
				break
			}
			entry, err := reader.ReadNext(context.TODO())
			if err != nil {
				t.Logf("Error reading entry: %v", err)
				continue
			}
			t.Logf("Read entry with ID %d and data '%s'", entry.EntryId, string(entry.Values))
			readEntries[entry.EntryId] = entry
		}

		// Verify that all expected entries were read back
		assert.Equal(t, 80, len(readEntries), "Should read back 80 entries")

		// Verify that all expected IDs and data were correctly read back
		for id, resultData := range readEntries {
			expectedEntry, ok := entryData[id]
			assert.True(t, ok, "Entry with ID %d should be read back", id)
			if ok {
				assert.Equal(t, expectedEntry, resultData.Values, "Data for entry ID %d should match", id)
				t.Logf("Verified entry ID %d with data '%s'", id, string(resultData.Values))
			}
		}
	}

	// Read first 10 entries
	{
		roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
		assert.NoError(t, err)
		assert.NotNil(t, roSegmentImpl)
		reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
			StartSequenceNum: 0,
			EndSequenceNum:   10, // Read middle 80 entries
		})
		assert.NoError(t, err)
		defer reader.Close()
		readEntries := make(map[int64]*proto.LogEntry)
		for {
			hasNext, err := reader.HasNext(context.TODO())
			assert.NoError(t, err)
			if !hasNext {
				break
			}
			entry, err := reader.ReadNext(context.TODO())
			if err != nil {
				t.Logf("Error reading entry: %v", err)
				continue
			}
			t.Logf("Read entry with ID %d and data '%s'", entry.EntryId, string(entry.Values))
			readEntries[entry.EntryId] = entry
		}

		// Verify if all written entries were read back
		assert.Equal(t, 10, len(readEntries), "Should read back all 3 entries")

		// Verify all written IDs and data were correctly read back
		for id, resultData := range readEntries {
			expectedEntry, ok := entryData[id]
			assert.True(t, ok, "Entry with ID %d should be read back", id)
			if ok {
				assert.Equal(t, expectedEntry, resultData.Values, "Data for entry ID %d should match", id)
				t.Logf("Verified entry ID %d with data '%s'", id, string(resultData.Values))
			}
		}
	}

	// Read last 10 entries
	{
		roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
		assert.NoError(t, err)
		assert.NotNil(t, roSegmentImpl)
		reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
			StartSequenceNum: 90,
			EndSequenceNum:   100, // Read middle 80 entries
		})
		assert.NoError(t, err)
		defer reader.Close()
		readEntries := make(map[int64]*proto.LogEntry)
		for {
			hasNext, err := reader.HasNext(context.TODO())
			assert.NoError(t, err)
			if !hasNext {
				break
			}
			entry, err := reader.ReadNext(context.TODO())
			if err != nil {
				t.Logf("Error reading entry: %v", err)
				continue
			}
			t.Logf("Read entry with ID %d and data '%s'", entry.EntryId, string(entry.Values))
			readEntries[entry.EntryId] = entry
		}

		// Verify if all written entries were read back
		assert.Equal(t, 10, len(readEntries), "Should read back all 3 entries")

		// Verify all written IDs and data were correctly read back
		for id, resultData := range readEntries {
			expectedEntry, ok := entryData[id]
			assert.True(t, ok, "Entry with ID %d should be read back", id)
			if ok {
				assert.Equal(t, expectedEntry, resultData.Values, "Data for entry ID %d should match", id)
				t.Logf("Verified entry ID %d with data '%s'", id, string(resultData.Values))
			}
		}
	}

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestLoad tests loading a log file after restart.
func TestLoad(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)
	dir := getTempDir(t)

	// Initial number of entries written
	const initialEntries = 20
	var initialLastEntryID int64

	// Create log file and write data
	{
		segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
		assert.NoError(t, err)
		// First write 20 entries
		resultChannels := make([]channel.ResultChannel, 0, initialEntries)
		entryIDs := make([]int64, 0, initialEntries)
		for i := 0; i < initialEntries; i++ {
			entryID := int64(i)
			entryValue := []byte(fmt.Sprintf("test_data_%d", i))
			rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryID))
			id, err := segmentImpl.AppendAsync(context.Background(), entryID, entryValue, rc)
			assert.NoError(t, err)
			assert.Equal(t, entryID, id)
			resultChannels = append(resultChannels, rc)
			entryIDs = append(entryIDs, entryID)
		}
		err = segmentImpl.Sync(context.TODO())
		assert.NoError(t, err)
		lastEntryId, err := segmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, int64(initialEntries-1), lastEntryId)
		initialLastEntryID = lastEntryId
	}

	// Reload log file
	{
		roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
		assert.NoError(t, err)
		assert.NotNil(t, roSegmentImpl)

		// Load data
		_, fragment, err := roSegmentImpl.Load(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, fragment)

		// Get last entry ID after loading
		loadedLastEntryID, err := roSegmentImpl.GetLastEntryId(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, initialLastEntryID, loadedLastEntryID, "GetLastEntryId() should return the same value as loaded")
	}

	{
		// open exists log file for write
		segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
		assert.Error(t, err)
		assert.Nil(t, segmentImpl)
	}
}

// TestGetId tests the GetId function.
func TestGetId(t *testing.T) {
	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 42, dir)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), segmentImpl.logId)
	assert.Equal(t, int64(42), segmentImpl.segmentId)
	assert.Equal(t, int64(42), segmentImpl.GetId())

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestInvalidReaderRange tests creating a reader with invalid range.
func TestInvalidReaderRange(t *testing.T) {
	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)

	// Track entry IDs and data
	var entryIDs []int64

	// Append a few entries
	for i := 0; i < 5; i++ {
		data := []byte{byte(i)}
		// Use AppendAsync to get ID
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", i))
		id, err := segmentImpl.AppendAsync(context.Background(), int64(i), data, rc)
		assert.NoError(t, err)
		// Wait for append to complete
		subCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		result, readErr := rc.ReadResult(subCtx)
		cancel()
		assert.NoError(t, readErr)
		assert.Equal(t, id, result.SyncedId)
		entryIDs = append(entryIDs, id)
		t.Logf("Appended entry %d with ID %d and data '%d'", i, id, i)
	}

	// Ensure all data has been written
	err = segmentImpl.Sync(context.Background())
	assert.NoError(t, err)

	// Ensure there are enough entries for testing
	if len(entryIDs) < 5 {
		t.Fatalf("Not enough entries to test read range")
		return
	}

	lastID := entryIDs[len(entryIDs)-1]
	beyondLastID := lastID + 5 // An ID beyond the range

	// Try to create a reader with start beyond available entries
	roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roSegmentImpl)
	beyondReader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: beyondLastID, // Beyond available entries
		EndSequenceNum:   beyondLastID + 5,
	})
	assert.NoError(t, err)
	hasNext, err := beyondReader.HasNext(context.TODO())
	assert.NoError(t, err)
	assert.False(t, hasNext)

	// Create another reader using verified actual ID range, and verify read results
	// Explicitly read only from the 3rd to the 5th entry (indices 2 to 4)
	startID := entryIDs[2]
	endID := entryIDs[4] + 1 // +1 because the range is half-open

	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: startID,
		EndSequenceNum:   endID,
	})
	assert.NoError(t, err)
	defer reader.Close()

	// Verify that expected entries can be read
	for i := 2; i <= 4; i++ {
		hasNextMsg, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		assert.True(t, hasNextMsg, "Should have entry at index %d", i)
		entry, err := reader.ReadNext(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, entryIDs[i], entry.EntryId, "Entry ID should match")
		t.Logf("Successfully read entry at index %d: ID=%d", i, entry.EntryId)
	}

	// After reading all 3 entries, there should be no more entries
	hasNextMsg, err := reader.HasNext(context.TODO())
	assert.NoError(t, err)
	assert.False(t, hasNextMsg, "Should not have more entries after reading all valid ones")

	// Try to read again, should return error
	_, err = reader.ReadNext(context.TODO())
	assert.Error(t, err, "Reading out of range should return error")
	assert.True(t, strings.Contains(err.Error(), "invalid data offset"))
	t.Logf("Reading out of range returned expected error: %v", err)

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestReadAfterClose tests reading from a closed log file.
func TestReadAfterClose(t *testing.T) {
	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)

	// Write a small amount of data
	numEntries := 5
	for i := 0; i < numEntries; i++ {
		entryID := int64(i)
		entryValue := []byte(fmt.Sprintf("test_data_%d", i))
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryID))
		id, err := segmentImpl.AppendAsync(context.Background(), entryID, entryValue, rc)
		assert.NoError(t, err)
		assert.Equal(t, entryID, id)
	}

	// Force sync
	fmt.Println("Force syncing data")
	err = segmentImpl.Sync(context.TODO())
	assert.NoError(t, err)
	lastEntryId, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(numEntries-1), lastEntryId)

	// Close file
	fmt.Println("Closing file")
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)

	roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roSegmentImpl)

	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,
		EndSequenceNum:   2,
	})
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	for i := 0; i < 2; i++ {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		assert.True(t, hasNext)
		entry, err := reader.ReadNext(context.TODO())
		assert.NoError(t, err)
		expectedID := int64(i)
		assert.Equal(t, expectedID, entry.EntryId, i)
		assert.Equal(t, []byte(fmt.Sprintf("test_data_%d", i)), entry.Values)
	}

	// Verify no more entries
	hasNext, err := reader.HasNext(context.TODO())
	assert.NoError(t, err)
	assert.False(t, hasNext)

	// Cleanup
	err = reader.Close()
	assert.NoError(t, err)
	err = roSegmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestBasicReader tests basic reader functionality.
func TestBasicReader(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)

	// Append entries with specific IDs to ensure we know the exact IDs to read later
	startID := int64(0) // Use a larger starting ID to ensure no conflicts
	numEntries := 10

	// Append ordered entries
	for i := 0; i < numEntries; i++ {
		entryID := startID + int64(i)
		data := []byte(fmt.Sprintf("data-%d", i))
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryID))
		_, err := segmentImpl.AppendAsync(context.Background(), entryID, data, rc)
		assert.NoError(t, err)
		// Wait for write to complete
		result, readErr := rc.ReadResult(context.TODO())
		assert.NoError(t, readErr)
		assert.Equal(t, entryID, result.SyncedId)
	}

	// Ensure data has been written
	err = segmentImpl.Sync(context.Background())
	assert.NoError(t, err)

	// Create reader for all entries
	roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roSegmentImpl)
	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: startID,
		EndSequenceNum:   startID + int64(numEntries),
	})
	assert.NoError(t, err)

	// Read and verify all entries
	for i := 0; i < numEntries; i++ {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		assert.True(t, hasNext)
		entry, err := reader.ReadNext(context.TODO())
		assert.NoError(t, err)
		expectedID := startID + int64(i)
		assert.Equal(t, expectedID, entry.EntryId, i)
		assert.Equal(t, []byte(fmt.Sprintf("data-%d", i)), entry.Values)
	}

	// Verify no more entries
	hasNext, err := reader.HasNext(context.TODO())
	assert.NoError(t, err)
	assert.False(t, hasNext)

	// Cleanup
	err = reader.Close()
	assert.NoError(t, err)
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestOnlyFirstAndLast skips read testing, only tests getting first and last entry IDs
func TestOnlyFirstAndLast(t *testing.T) {
	dir := getTempDir(t)
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)

	// Use simple Append method to add data, letting the system assign IDs
	numEntries := 5
	testData := make([][]byte, numEntries)

	for i := 0; i < numEntries; i++ {
		testData[i] = []byte(fmt.Sprintf("data-%d", i))
		err = segmentImpl.Append(context.Background(), testData[i])
		assert.NoError(t, err)
		t.Logf("Appended data: %s", testData[i])
	}

	// Ensure data has been written
	err = segmentImpl.Sync(context.Background())
	assert.NoError(t, err)

	// Get last entry ID
	lastEntryID, err := segmentImpl.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	t.Logf("Last entry ID after writes: %d", lastEntryID)

	// Ensure lastEntryID is valid
	assert.Equal(t, lastEntryID, int64(numEntries-1), "Last entry ID should be at least %d", numEntries-1)

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestSequentialBufferAppend Test based on SequentialBuffer implementation
func TestSequentialBufferAppend(t *testing.T) {
	tempDir := getTempDir(t)
	defer os.RemoveAll(tempDir)

	dlf, err := NewDiskSegmentImpl(context.TODO(), 1, 0, tempDir)
	assert.NoError(t, err)
	defer dlf.Close(context.TODO())

	// Test continuous ordered write
	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", i))
		id, err := dlf.AppendAsync(context.Background(), int64(i), data, rc)
		assert.NoError(t, err)
		assert.Equal(t, int64(i), id)
		result, _ := rc.ReadResult(context.TODO()) // Wait for write to complete
		_ = result
	}

	// Force sync buffer to disk
	err = dlf.Sync(context.Background())
	assert.NoError(t, err)

	// Get last written entryID
	lastID, err := dlf.GetLastEntryId(context.TODO())
	assert.NoError(t, err)
	assert.Equal(t, int64(4), lastID)

	// Verify can read written data
	roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, tempDir)
	assert.NoError(t, err)
	assert.NotNil(t, roSegmentImpl)
	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,
		EndSequenceNum:   5, // Read all entries
	})
	assert.NoError(t, err)

	// Read and verify data
	entryCount := 0
	for {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("data-%d", entry.EntryId)), entry.Values)
		entryCount++
	}

	// Should read 5 entries (0-4)
	assert.Equal(t, 5, entryCount)
}

func TestWrite10kAndReadInOrder(t *testing.T) {
	testEntryCount := 10000
	dir := getTempDir(t)
	// Create a larger fragment size to accommodate all data
	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir, WithWriteFragmentSize(10*1024*1024))
	assert.NoError(t, err)

	// Record write start time
	writeStartTime := time.Now()

	// Use larger starting ID to avoid conflicts with auto-allocated ID
	resultChannels := make([]channel.ResultChannel, testEntryCount)
	// Record data for each ID, for subsequent verification
	entryData := make(map[int][]byte)

	t.Logf("Starting to write %d entries...", testEntryCount)

	for id := 0; id < testEntryCount; id++ {
		// Create data, include ID information for verification
		data := []byte(fmt.Sprintf("data-for-entry-%d", id))
		entryData[id] = data
		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", id))
		assignedID, err := segmentImpl.AppendAsync(context.Background(), int64(id), data, rc)
		assert.NoError(t, err)
		assert.Equal(t, int64(id), assignedID, "Assigned ID should match requested ID")
		resultChannels[id] = rc

		// Print progress every 1000 entries
		if (id+1)%1000 == 0 {
			t.Logf("Wrote %d/%d entries", id+1, testEntryCount)
		}
	}

	// Wait for all write results
	successCount := 0
	failCount := 0
	for i, rc := range resultChannels {
		subCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result, readErr := rc.ReadResult(subCtx)
		cancel()
		if readErr != nil {
			t.Logf("Timeout waiting for append result for ID %d", i)
			failCount++
			continue
		}
		if result.Err != nil {
			t.Logf("Failed to append entry %d: %v", i, result.Err)
			failCount++
			continue
		}
		if result.SyncedId >= 0 {
			successCount++
		} else {
			failCount++
			t.Logf("Failed to write entry %d", i)
		}
		// Print progress every 1000 entries
		if (i+1)%1000 == 0 {
			t.Logf("Processed %d/%d write results", i+1, testEntryCount)
		}
	}

	writeDuration := time.Since(writeStartTime)
	t.Logf("Write completed in %v. Success: %d, Failed: %d", writeDuration, successCount, failCount)

	// Ensure write success rate is 100%
	assert.Equal(t, testEntryCount, successCount, "All entries should be written successfully")
	assert.Equal(t, 0, failCount, "No entries should fail")

	// Sync to ensure all data has been written to disk
	err = segmentImpl.Sync(context.Background())
	assert.NoError(t, err)

	// Record read start time
	readStartTime := time.Now()

	// Create reader to verify data, ensure read from ID 0 to all data
	roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roSegmentImpl)
	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,
		EndSequenceNum:   int64(testEntryCount),
	})
	assert.NoError(t, err)
	defer reader.Close()

	// Collect all read entries
	readEntries := make(map[int64]*proto.LogEntry)
	readSequence := make([]int64, 0, testEntryCount) // Record read order

	t.Logf("Starting to read entries...")
	readCount := 0

	for {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext(context.TODO())
		if err != nil {
			t.Logf("Error reading entry: %v", err)
			continue
		}

		readEntries[entry.EntryId] = entry
		readSequence = append(readSequence, entry.EntryId)
		readCount++

		// Print progress every 1000 entries
		if readCount%1000 == 0 {
			t.Logf("Read %d entries", readCount)
		}
	}

	readDuration := time.Since(readStartTime)
	t.Logf("Read completed in %v. Total entries read: %d", readDuration, len(readEntries))

	// Verify if all written entries were read back
	assert.Equal(t, testEntryCount, len(readEntries), "Should read back the same number of entries that were written")

	// Verify read order is correct
	for i := 0; i < len(readSequence)-1; i++ {
		assert.Equal(t, readSequence[i]+1, readSequence[i+1],
			"Entries should be read in sequential order, but got %d followed by %d",
			readSequence[i], readSequence[i+1])
	}

	// Verify all written IDs and data were correctly read back
	for id, expectedData := range entryData {
		entry, ok := readEntries[int64(id)]
		assert.True(t, ok, "Entry with ID %d should be read back", id)
		if ok {
			assert.Equal(t, expectedData, entry.Values,
				"Data for entry ID %d should match. Expected: %s, Got: %s",
				id, string(expectedData), string(entry.Values))
		}
	}

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

func TestWrite10kWithSmallFragments(t *testing.T) {
	testEntryCount := 10000 // Reduce the number of entries for quicker testing
	dir := getTempDir(t)

	// Use more reasonable fragment sizes that will still force rotation
	// but allow entries to be written correctly
	smallFragmentSize := int64(16 * 1024) // 16KB instead of 4KB
	maxEntriesPerFragment := 100          // 100 entries per fragment instead of 200

	t.Logf("Creating log file with small fragment size: %d bytes, max %d entries per fragment",
		smallFragmentSize, maxEntriesPerFragment)

	segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir,
		WithWriteFragmentSize(smallFragmentSize),
		WithWriteMaxEntryPerFile(maxEntriesPerFragment))
	assert.NoError(t, err)

	// Record write start time
	writeStartTime := time.Now()

	// Use larger starting ID to avoid conflicts with auto-allocated ID
	resultChannels := make([]channel.ResultChannel, testEntryCount)
	// Record data for each ID, for subsequent verification
	entryData := make(map[int][]byte)

	t.Logf("Starting to write %d entries with forced fragment rotations...", testEntryCount)

	for id := 0; id < testEntryCount; id++ {
		// Create data, include ID information for verification
		data := []byte(fmt.Sprintf("data-for-entry-%d", id))
		entryData[id] = data

		rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", id))
		assignedID, err := segmentImpl.AppendAsync(context.Background(), int64(id), data, rc)
		assert.NoError(t, err)
		assert.Equal(t, int64(id), assignedID, "Assigned ID should match requested ID")
		resultChannels[id] = rc

		// Print progress every 100 entries (should trigger fragment rotation)
		if (id+1)%100 == 0 {
			t.Logf("Wrote %d/%d entries (should trigger fragment rotation)", id+1, testEntryCount)

			// Force sync to ensure fragment rotation happens
			err = segmentImpl.Sync(context.Background())
			assert.NoError(t, err, "Failed to sync at entry %d", id+1)

			// Get current fragment information
			roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
			assert.NoError(t, err)
			assert.NotNil(t, roSegmentImpl)
			_, _, err = roSegmentImpl.fetchROFragments(context.TODO())
			assert.NoError(t, err)
			t.Logf("After %d entries, fragment count: %d", id+1, len(roSegmentImpl.fragments))

			if len(roSegmentImpl.fragments) > 0 {
				lastFrag := roSegmentImpl.fragments[len(roSegmentImpl.fragments)-1]
				firstID, _ := lastFrag.GetFirstEntryId(context.TODO())
				lastID, _ := lastFrag.GetLastEntryId(context.TODO())
				t.Logf("Last fragment: ID=%d, first entry=%d, last entry=%d",
					lastFrag.GetFragmentId(), firstID, lastID)
			}
		}
	}

	// Wait for all write results
	successCount := 0
	failCount := 0
	for i, rc := range resultChannels {
		subCtx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		result, readErr := rc.ReadResult(subCtx)
		cancel()
		if readErr != nil {
			t.Errorf("ReadResult failed for entry %d: %v", i, readErr)
			failCount++
			continue
		}
		if result.Err != nil {
			t.Errorf("Append failed for entry %d: %v", i, result.Err)
			failCount++
			continue
		}
		if result.SyncedId < 0 {
			t.Errorf("Append failed for entry %d: %d", i, result.SyncedId)
			failCount++
			continue
		} else {
			logger.Ctx(context.TODO()).Debug("ReadResult: read succeeded",
				zap.Int64("result", result.SyncedId))
			successCount++
		}
		// Print progress every 100 entries
		if (i+1)%100 == 0 {
			t.Logf("Processed %d/%d write results", i+1, testEntryCount)
		}
	}

	writeDuration := time.Since(writeStartTime)
	t.Logf("Write completed in %v. Success: %d, Failed: %d", writeDuration, successCount, failCount)

	// Check write status, allow some failures but not too many
	assert.Greater(t, successCount, failCount, "More successful than failed writes")

	// Final sync to ensure all data is written to disk
	err = segmentImpl.Sync(context.Background())
	assert.NoError(t, err)

	// Get final fragment information
	roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roSegmentImpl)
	_, _, err = roSegmentImpl.fetchROFragments(context.TODO())
	assert.NoError(t, err)
	t.Logf("Final fragment count: %d", len(roSegmentImpl.fragments))

	// Verify multiple fragments were created (confirm rotation occurred)
	assert.Greater(t, len(roSegmentImpl.fragments), 1, "Multiple fragments should be created due to small fragment size")

	for i, frag := range roSegmentImpl.fragments {
		firstID, _ := frag.GetFirstEntryId(context.TODO())
		lastID, _ := frag.GetLastEntryId(context.TODO())
		entryCount := lastID - firstID + 1
		t.Logf("Fragment[%d]: ID=%d, first entry=%d, last entry=%d, entries=%d",
			i, frag.GetFragmentId(), firstID, lastID, entryCount)
	}

	// Record read start time
	readStartTime := time.Now()

	// Create reader to verify data, ensure reading all data from ID 0
	reader, err := roSegmentImpl.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,
		EndSequenceNum:   int64(testEntryCount),
	})
	assert.NoError(t, err)
	defer reader.Close()

	// Collect all read entries
	readEntries := make(map[int64]*proto.LogEntry)
	readSequence := make([]int64, 0, testEntryCount) // Record read order

	t.Logf("Starting to read entries across multiple fragments...")
	readCount := 0

	for {
		hasNext, err := reader.HasNext(context.TODO())
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext(context.TODO())
		if err != nil {
			t.Errorf("Error reading entry: %v", err)
			continue
		}

		readEntries[entry.EntryId] = entry
		readSequence = append(readSequence, entry.EntryId)
		readCount++

		// Print progress every 100 entries (corresponding to fragment capacity)
		if readCount%100 == 0 {
			t.Logf("Read %d entries (crossing fragment boundary)", readCount)
		}
	}

	readDuration := time.Since(readStartTime)
	t.Logf("Read completed in %v. Total entries read: %d", readDuration, len(readEntries))

	// Verify all written entries were read back
	// Note: May not have all entries as some writes may have failed
	t.Logf("Read back %d entries of %d attempted writes (%d successful writes)",
		len(readEntries), testEntryCount, successCount)

	// If there are sequence gaps, record them
	if len(readSequence) > 0 {
		t.Logf("First read ID: %d, Last read ID: %d",
			readSequence[0], readSequence[len(readSequence)-1])

		// Check sequence continuity
		for i := 0; i < len(readSequence)-1; i++ {
			if readSequence[i]+1 != readSequence[i+1] {
				t.Logf("Gap in sequence: %d followed by %d (expected %d)",
					readSequence[i], readSequence[i+1], readSequence[i]+1)
			}
		}
	}

	// Verify correctness of read entries' data
	for id, entry := range readEntries {
		expectedData, ok := entryData[int(id)]
		if ok {
			if !bytes.Equal(expectedData, entry.Values) {
				t.Errorf("Data mismatch for ID %d. Expected: %s, Got: %s",
					id, string(expectedData), string(entry.Values))
			} else {
				// Data matches
				t.Logf("Verified entry ID %d", id)
			}
		} else {
			t.Errorf("Read unexpected entry ID: %d", id)
		}
	}

	// Cleanup
	err = segmentImpl.Close(context.TODO())
	assert.NoError(t, err)
}

// TestFragmentDataValueCheck Debug Test Only
func TestFragmentDataValueCheck(t *testing.T) {
	t.Skipf("just for debug, skip")
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	for i := 0; i <= 14; i++ {
		filePath := fmt.Sprintf("/tmp/TestWriteReadPerf/woodpecker/14/0/fragment_%d", i)
		ff, err := NewFragmentFileReader(context.TODO(), filePath, 128*1024*1024, 14, 0, int64(i))
		assert.NoError(t, err)
		err = ff.IteratorPrint()
		assert.NoError(t, err)
		if err != nil {
			logger.Ctx(context.Background()).Error("iterator failed", zap.Int("fragmentId", i), zap.Error(err))
		}
	}
}

// TestDeleteFragments tests the DeleteFragments function focusing on its ability
// to handle directory and logging operations, rather than actual file operations.
func TestDeleteFragments(t *testing.T) {
	t.Run("EmptyDirectory", func(t *testing.T) {
		// Set up test directory
		testDir := getTempDir(t)
		logId := int64(1)

		// Create a DiskSegmentImpl object with a mock directory
		logDir := testDir
		err := os.MkdirAll(logDir, 0755)
		assert.NoError(t, err)

		// Create read-only log file
		roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), logId, 0, logDir)
		assert.NoError(t, err)
		assert.NotNil(t, roSegmentImpl)

		// Execute deletion operation
		err = roSegmentImpl.DeleteFragments(context.Background(), 0)
		assert.NoError(t, err, "DeleteFragments should not error with empty directory")

		// Verify internal state has been reset
		assert.Equal(t, 0, len(roSegmentImpl.fragments), "fragments should be empty")

		// Close
		err = roSegmentImpl.Close(context.TODO())
		assert.NoError(t, err)
	})

	t.Run("NonExistentDirectory", func(t *testing.T) {
		// Test case where directory does not exist
		nonExistDir := getTempDir(t)
		os.RemoveAll(nonExistDir) // Ensure directory does not exist

		segmentImpl2, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, nonExistDir)
		assert.NoError(t, err)

		err = segmentImpl2.DeleteFragments(context.Background(), 0)
		assert.NoError(t, err, "DeleteFragments should not error when directory doesn't exist")

		// Verify state is also correctly reset
		assert.Equal(t, 0, len(segmentImpl2.fragments), "fragments should be empty")

		err = segmentImpl2.Close(context.TODO())
		assert.NoError(t, err)
	})

	t.Run("WithFragmentFiles", func(t *testing.T) {
		dir := getTempDir(t)

		// first write multi fragments
		{
			cfg, _ := config.NewConfiguration()
			cfg.Log.Level = "debug"
			logger.InitLogger(cfg)

			segmentImpl, err := NewDiskSegmentImpl(context.TODO(), 1, 0, dir, WithWriteMaxEntryPerFile(10))
			assert.NoError(t, err)
			assert.NotNil(t, segmentImpl)

			entryData := make(map[int64][]byte)
			// Append a few entries
			{
				// First write 100 entries
				numEntries := 100
				resultChannels := make([]channel.ResultChannel, 0, numEntries)
				entryIDs := make([]int64, 0, numEntries)
				for i := 0; i < numEntries; i++ {
					entryID := int64(i)
					entryValue := []byte(fmt.Sprintf("test_data_%d", i))
					entryData[entryID] = entryValue
					rc := channel.NewLocalResultChannel(fmt.Sprintf("1/0/%d", entryID))
					id, err := segmentImpl.AppendAsync(context.Background(), entryID, entryValue, rc)
					assert.NoError(t, err)
					assert.Equal(t, entryID, id)
					resultChannels = append(resultChannels, rc)
					entryIDs = append(entryIDs, entryID)
				}
				err := segmentImpl.Sync(context.TODO())
				assert.NoError(t, err)
				lastEntryId, err := segmentImpl.GetLastEntryId(context.TODO())
				assert.NoError(t, err)
				assert.Equal(t, int64(numEntries-1), lastEntryId)
			}

			// Ensure all data has been written
			err = segmentImpl.Sync(context.Background())
			assert.NoError(t, err)

			// Confirm that 10 fragments were created through rotation
			roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
			assert.NoError(t, err)
			assert.NotNil(t, roSegmentImpl)
			_, _, err = roSegmentImpl.fetchROFragments(context.TODO())
			assert.NoError(t, err)
			assert.NotNil(t, roSegmentImpl.fragments)
			assert.Equal(t, 10, len(roSegmentImpl.fragments))

			// Check that at least one fragment file was created
			files, err := os.ReadDir(segmentImpl.logFileDir)
			assert.NoError(t, err)
			assert.Equal(t, 10, len(files), "Expected 10 fragment files")
		}

		// Create read-only log file
		roSegmentImpl, err := NewRODiskSegmentImpl(context.TODO(), 1, 0, dir)
		assert.NoError(t, err)
		assert.NotNil(t, roSegmentImpl)

		_, _, err = roSegmentImpl.fetchROFragments(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, 10, len(roSegmentImpl.fragments))

		// Execute deletion operation
		err = roSegmentImpl.DeleteFragments(context.Background(), 0)
		assert.NoError(t, err, "DeleteFragments should successfully delete fragment files")

		// Verify internal state has been reset
		_, _, err = roSegmentImpl.fetchROFragments(context.TODO())
		assert.NoError(t, err)
		assert.Equal(t, 0, len(roSegmentImpl.fragments), "fragments should be empty")

		err = roSegmentImpl.Close(context.TODO())
		assert.NoError(t, err)
	})
}

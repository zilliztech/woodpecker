package disk

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/gops/agent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

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

// TestNewDiskLogFile tests the NewDiskLogFile function.
func TestNewDiskLogFile(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), logFile.id)
	assert.Equal(t, filepath.Join(dir, "1"), logFile.logFileDir)
	assert.Equal(t, int64(128*1024*1024), logFile.fragmentSize)
	assert.Equal(t, 100000, logFile.maxEntryPerFile)

	// Cleanup
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestDiskLogFileWithOptions tests creating a DiskLogFile with custom options.
func TestDiskLogFileWithOptions(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir,
		WithWriteFragmentSize(1024*1024), // 1MB
		WithWriteMaxEntryPerFile(1000),   // 1000 entries
	)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), logFile.id)
	assert.Equal(t, filepath.Join(dir, "1"), logFile.logFileDir)
	assert.Equal(t, int64(1024*1024), logFile.fragmentSize)
	assert.Equal(t, 1000, logFile.maxEntryPerFile)

	// Cleanup
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestAppend tests the Append function.
func TestAppend(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)

	// Append data
	err = logFile.Append(context.Background(), []byte("test_data_0"))
	assert.NoError(t, err)
	err = logFile.Append(context.Background(), []byte("test_data_1"))
	assert.NoError(t, err)

	// Get last entry ID
	lastEntryID, err := logFile.GetLastEntryId()
	assert.NoError(t, err)
	// DiskLogFile implementation increments the entry ID from 0, first append results in ID 0
	assert.Equal(t, lastEntryID, int64(1))

	// Cleanup
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestAppendAsync tests the AppendAsync function.
func TestAppendAsync(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)

	// Test with auto-assigned ID (0 means auto-assign, will use internal incrementing ID)
	entryID, resultCh, err := logFile.AppendAsync(context.Background(), 0, []byte("test_data"))
	assert.NoError(t, err)
	assert.Equal(t, entryID, int64(0))

	// Wait for result
	select {
	case result := <-resultCh:
		assert.Equal(t, entryID, result)
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for append result")
	}

	// Get last entry ID after first append
	firstLastID, err := logFile.GetLastEntryId()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, firstLastID, int64(0))

	// Cleanup
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestMultipleEntriesAppend tests appending multiple entries.
func TestMultipleEntriesAppend(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)

	// Append multiple entries
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		err = logFile.Append(context.Background(), []byte(fmt.Sprintf("test_data_%d", i)))
		assert.NoError(t, err)
	}

	// Get last entry ID
	lastEntryID, err := logFile.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, lastEntryID, int64(numEntries-1))

	// Cleanup
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestAppendAsyncMultipleEntries tests appending multiple entries asynchronously.
func TestAppendAsyncMultipleEntries(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)

	// Append entries with specific IDs
	numEntries := 100
	resultChannels := make([]<-chan int64, 0, numEntries)
	entryIDs := make([]int64, 0, numEntries)
	for i := 0; i < numEntries; i++ {
		entryID := int64(i)
		id, ch, err := logFile.AppendAsync(context.Background(), entryID, []byte(fmt.Sprintf("test_data_%d", i)))
		assert.NoError(t, err)
		assert.Equal(t, entryID, id)
		resultChannels = append(resultChannels, ch)
		entryIDs = append(entryIDs, entryID)
	}

	// Wait for all results
	for i, ch := range resultChannels {
		select {
		case result := <-ch:
			assert.Equal(t, entryIDs[i], result)
		case <-time.After(2 * time.Second):
			t.Error("Timeout waiting for append result")
		}
	}

	// Get last entry ID
	lastEntryID, err := logFile.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, lastEntryID, int64(numEntries)-1)

	// Cleanup
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestOutOfOrderAppend tests appending entries out of order.
func TestOutOfOrderAppend(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)

	// Use a larger starting ID to avoid conflicts with auto-assigned IDs
	entryIDs := []int64{5, 3, 8, 1, 0, 2, 7, 6, 4, 9}
	resultChannels := make([]<-chan int64, 0, len(entryIDs))

	// Record data for each ID for later verification
	entryData := make(map[int64][]byte)
	for _, id := range entryIDs {
		data := []byte(fmt.Sprintf("data-%d", id))
		entryData[id] = data
		t.Logf("Appending entry with ID %d and data '%s'", id, string(data))

		assignedID, ch, err := logFile.AppendAsync(context.Background(), id, data)
		assert.NoError(t, err)
		assert.Equal(t, id, assignedID, "Assigned ID should match requested ID")
		resultChannels = append(resultChannels, ch)
	}

	// Wait for all results
	for i, ch := range resultChannels {
		select {
		case result := <-ch:
			assert.Equal(t, entryIDs[i], result, "Result ID should match original ID")
			t.Logf("Received result for ID %d", result)
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout waiting for append result for ID %d", entryIDs[i])
		}
	}

	// Print the current ID order and data mapping for debugging
	t.Log("ID to Data mapping:")
	for id, data := range entryData {
		t.Logf("ID %d -> %s", id, string(data))
	}

	// Create reader to verify data
	roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roLogFile)
	reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,  // Ensure we start before all IDs
		EndSequenceNum:   10, // Ensure we include all IDs
	})
	assert.NoError(t, err)
	defer reader.Close()

	// Collect all read entries
	readEntries := make(map[int64]*proto.LogEntry)
	for {
		hasNext, err := reader.HasNext()
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext()
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
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestDelayedAppend tests handling of delayed append requests.
func TestDelayedAppend(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)

	// Starting ID to use
	startID := int64(0)

	// Prepare data
	entryData := make(map[int64][]byte)

	// First send request for ID 2
	data2 := []byte("data-2")
	entryData[startID+2] = data2
	_, ch2, err := logFile.AppendAsync(context.Background(), startID+2, data2)
	assert.NoError(t, err)

	// Wait a short time to simulate delay
	time.Sleep(100 * time.Millisecond)

	// Send request for ID 1
	data1 := []byte("data-1")
	entryData[startID+1] = data1
	_, ch1, err := logFile.AppendAsync(context.Background(), startID+1, data1)
	assert.NoError(t, err)

	// Wait a short time to simulate delay
	time.Sleep(100 * time.Millisecond)

	// Send request for ID 0
	data0 := []byte("data-0")
	entryData[startID] = data0
	_, ch0, err := logFile.AppendAsync(context.Background(), startID, data0)
	assert.NoError(t, err)

	// Wait for all results
	select {
	case result := <-ch0:
		assert.Equal(t, startID, result)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for append result for ID 1000")
	}

	select {
	case result := <-ch1:
		assert.Equal(t, startID+1, result)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for append result for ID 1001")
	}

	select {
	case result := <-ch2:
		assert.Equal(t, startID+2, result)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for append result for ID 1002")
	}

	// Ensure all data is written
	lastEntryID, err := logFile.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, startID+2, lastEntryID, "Last entry ID should match the highest ID")

	// Create reader to verify data
	roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roLogFile)
	reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: startID,
		EndSequenceNum:   startID + 3,
	})
	assert.NoError(t, err)
	defer reader.Close()

	// Collect all read entries
	readEntries := make(map[int64]*proto.LogEntry)
	for {
		hasNext, err := reader.HasNext()
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext()
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
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestOutOfBoundsAppend tests handling of out-of-bounds append requests.
func TestOutOfBoundsAppend(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)
	{
		// First write 100 entries
		numEntries := 100
		resultChannels := make([]<-chan int64, 0, numEntries)
		entryIDs := make([]int64, 0, numEntries)
		for i := 0; i < numEntries; i++ {
			entryID := int64(i)
			id, ch, err := logFile.AppendAsync(context.Background(), entryID, []byte(fmt.Sprintf("test_data_%d", i)))
			assert.NoError(t, err)
			assert.Equal(t, entryID, id)
			resultChannels = append(resultChannels, ch)
			entryIDs = append(entryIDs, entryID)
		}
		err := logFile.Sync(context.TODO())
		assert.NoError(t, err)
		lastEntryId, err := logFile.GetLastEntryId()
		assert.NoError(t, err)
		assert.Equal(t, int64(numEntries-1), lastEntryId)
	}

	// Starting ID to use
	startID := int64(100)
	entryData := make(map[int64][]byte)

	// First send a normal ID request
	data1 := []byte("data-1")
	entryData[startID+1] = data1
	_, ch1, err := logFile.AppendAsync(context.Background(), startID+1, data1)
	assert.NoError(t, err)
	t.Logf("Appending entry with ID %d and data '%s'", startID+1, string(data1))

	// Send a request with an ID smaller than the current ID
	data0 := []byte("data-0")
	entryData[startID+0] = data0
	_, ch2, err := logFile.AppendAsync(context.Background(), startID+0, data0)
	assert.NoError(t, err) // This request should be accepted
	t.Logf("Appending entry with ID %d and data '%s'", startID+0, string(data0))

	// Send a request with a very small ID, which has already been persisted
	dataN1 := []byte("data-N1")
	entryData[startID-1] = dataN1
	syncedId, ch3, err := logFile.AppendAsync(context.Background(), startID-1, dataN1)
	assert.NoError(t, err) // This request should be accepted, indicating it's already been added
	assert.Equal(t, syncedId, startID-1)
	t.Logf("Appending entry with ID %d and data '%s'", startID-1, string(dataN1))

	// Send a request with an ID greater than the buffer window
	dataN2 := []byte("data-N2")
	entryData[startID+1_00000_0000] = dataN1
	_, ch4, err := logFile.AppendAsync(context.Background(), startID+1_00000_0000, dataN2)
	assert.Error(t, err) // This request should be immediately rejected, indicating it exceeds the buffer window
	assert.True(t, werr.ErrInvalidEntryId.Is(err))
	t.Logf("Appending entry with ID %d and data '%s'", startID+1_00000_0000, string(dataN2))

	// Wait for all results
	select {
	case result := <-ch1:
		assert.Equal(t, startID+1, result)
		t.Logf("Received result for ID %d", result)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for append result for ID 1001")
	}

	select {
	case result := <-ch2:
		assert.Equal(t, startID+0, result)
		t.Logf("Received result for ID %d", result)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for append result for ID 1000")
	}

	select {
	case result := <-ch3:
		assert.Equal(t, startID-1, result) // This is directly returned as successful because it was already persisted
		t.Logf("Received result for ID %d", result)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for append result for ID 999")
	}

	select {
	case result := <-ch4:
		assert.Equal(t, int64(-1), result) // This returns an error because it exceeds the window range
		t.Logf("Received result for ID %d", result)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for append result for ID 1001")
	}

	// Ensure all data is written
	syncErr := logFile.Sync(context.Background())
	assert.NoError(t, syncErr)

	// Create reader to verify data
	roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roLogFile)
	reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: startID + 0,
		EndSequenceNum:   startID + 2,
	})
	assert.NoError(t, err)
	defer reader.Close()

	// Collect all read entries
	readEntries := make(map[int64]*proto.LogEntry)
	for {
		hasNext, err := reader.HasNext()
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext()
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
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestMixedAppendScenarios tests various mixed scenarios for append requests.
func TestMixedAppendScenarios(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)

	{
		// First write 100 entries
		numEntries := 100
		resultChannels := make([]<-chan int64, 0, numEntries)
		entryIDs := make([]int64, 0, numEntries)
		for i := 0; i < numEntries; i++ {
			entryID := int64(i)
			id, ch, err := logFile.AppendAsync(context.Background(), entryID, []byte(fmt.Sprintf("test_data_%d", i)))
			assert.NoError(t, err)
			assert.Equal(t, entryID, id)
			resultChannels = append(resultChannels, ch)
			entryIDs = append(entryIDs, entryID)
		}
		err := logFile.Sync(context.TODO())
		assert.NoError(t, err)
		lastEntryId, err := logFile.GetLastEntryId()
		assert.NoError(t, err)
		assert.Equal(t, int64(numEntries-1), lastEntryId)
	}

	// Use a larger starting ID
	startID := int64(100)
	entryData := make(map[int64][]byte)
	channels := make(map[int64]<-chan int64)

	// Scenario 1: Normal sequential request
	data0 := []byte("data-0")
	entryData[startID] = data0
	_, ch1, err := logFile.AppendAsync(context.Background(), startID, data0)
	assert.NoError(t, err)
	channels[startID] = ch1

	// Scenario 2: Out-of-order request
	data2 := []byte("data-2")
	entryData[startID+2] = data2
	_, ch2, err := logFile.AppendAsync(context.Background(), startID+2, data2)
	assert.NoError(t, err)
	channels[startID+2] = ch2

	// Scenario 3: Delayed request
	time.Sleep(2000 * time.Millisecond)
	data1 := []byte("data-1")
	entryData[startID+1] = data1
	_, ch3, err := logFile.AppendAsync(context.Background(), startID+1, data1)
	assert.NoError(t, err)
	channels[startID+1] = ch3

	// Scenario 4: Out-of-bounds request
	data5 := []byte("data-5_000_0000")
	_, ch4, err := logFile.AppendAsync(context.Background(), startID+5_000_0000, data5)
	assert.Error(t, err)
	assert.True(t, werr.ErrInvalidEntryId.Is(err))
	assert.Equal(t, int64(-1), <-ch4) // This returns an error because it exceeds the window range

	// Scenario 5: Fill missing request
	data3 := []byte("data-3")
	entryData[startID+3] = data3
	_, ch5, err := logFile.AppendAsync(context.Background(), startID+3, data3)
	assert.NoError(t, err)
	channels[startID+3] = ch5

	data4 := []byte("data-4")
	entryData[startID+4] = data4
	_, ch6, err := logFile.AppendAsync(context.Background(), startID+4, data4)
	assert.NoError(t, err)
	channels[startID+4] = ch6

	// Wait for all results
	for id, ch := range channels {
		select {
		case result := <-ch:
			assert.Equal(t, id, result, "Result ID should match requested ID")
			t.Logf("Received result for ID %d", result)
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for append result for ID %d", id)
		}
	}

	// Ensure all data is written
	err = logFile.Sync(context.Background())
	assert.NoError(t, err)

	// Create reader to verify data
	roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roLogFile)
	reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: startID,
		EndSequenceNum:   startID + 5,
	})
	assert.NoError(t, err)
	defer reader.Close()

	// Collect all read entries
	readEntries := make(map[int64]*proto.LogEntry)
	for {
		hasNext, err := reader.HasNext()
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext()
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
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestSync tests the Sync function.
func TestSync(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)

	// Append multiple entries
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		err = logFile.Append(context.Background(), []byte("test_data"))
		assert.NoError(t, err)
	}

	// Sync data
	err = logFile.Sync(context.Background())
	assert.NoError(t, err)

	// Get last entry ID
	lastEntryID, err := logFile.GetLastEntryId()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, lastEntryID, int64(numEntries-1))

	// Cleanup
	err = logFile.Close()
	assert.NoError(t, err)
}

// Test Only
func startGopsAgent() {
	// start gops agent
	if err := agent.Listen(agent.Options{}); err != nil {
		panic(err)
	}
	http.HandleFunc("/pprof/cmdline", pprof.Cmdline)
	http.HandleFunc("/pprof/profile", pprof.Profile)
	http.HandleFunc("/pprof/symbol", pprof.Symbol)
	http.HandleFunc("/pprof/trace", pprof.Trace)
	go func() {
		fmt.Println("Starting gops agent on :6060")
		http.ListenAndServe(":6060", nil)
	}()
}

// TestFragmentRotation tests the basic rotation of fragment files.
func TestFragmentRotation(t *testing.T) {
	startGopsAgent()

	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir, WithWriteMaxEntryPerFile(10))
	assert.NoError(t, err)
	assert.NotNil(t, logFile)

	entryData := make(map[int64][]byte)
	// Append a few entries
	{
		// First write 100 entries
		numEntries := 100
		resultChannels := make([]<-chan int64, 0, numEntries)
		entryIDs := make([]int64, 0, numEntries)
		for i := 0; i < numEntries; i++ {
			entryID := int64(i)
			entryValue := []byte(fmt.Sprintf("test_data_%d", i))
			entryData[entryID] = entryValue
			id, ch, err := logFile.AppendAsync(context.Background(), entryID, entryValue)
			assert.NoError(t, err)
			assert.Equal(t, entryID, id)
			resultChannels = append(resultChannels, ch)
			entryIDs = append(entryIDs, entryID)
		}
		err := logFile.Sync(context.TODO())
		assert.NoError(t, err)
		lastEntryId, err := logFile.GetLastEntryId()
		assert.NoError(t, err)
		assert.Equal(t, int64(numEntries-1), lastEntryId)
	}

	// Ensure all data has been written
	err = logFile.Sync(context.Background())
	assert.NoError(t, err)

	// Confirm that 10 fragments were created through rotation
	roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roLogFile)
	_, _, err = roLogFile.fetchROFragments()
	assert.NoError(t, err)
	assert.NotNil(t, roLogFile.fragments)
	assert.Equal(t, 10, len(roLogFile.fragments))

	// Check that at least one fragment file was created
	files, err := os.ReadDir(logFile.logFileDir)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(files), "Expected 10 fragment files")

	// Verify data from various fragments
	// Create reader to verify data
	reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,
		EndSequenceNum:   100,
	})
	assert.NoError(t, err)
	defer reader.Close()

	// Collect all read entries
	readEntries := make(map[int64]*proto.LogEntry)
	for {
		hasNext, err := reader.HasNext()
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext()
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
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestNewReader tests creating and using a reader to read ranges
func TestNewReader(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)

	// Append entries with known data
	entryData := make(map[int64][]byte)
	// Append a few entries
	{
		// First write 100 entries
		numEntries := 100
		resultChannels := make([]<-chan int64, 0, numEntries)
		entryIDs := make([]int64, 0, numEntries)
		for i := 0; i < numEntries; i++ {
			entryID := int64(i)
			entryValue := []byte(fmt.Sprintf("test_data_%d", i))
			entryData[entryID] = entryValue
			id, ch, err := logFile.AppendAsync(context.Background(), entryID, entryValue)
			assert.NoError(t, err)
			assert.Equal(t, entryID, id)
			resultChannels = append(resultChannels, ch)
			entryIDs = append(entryIDs, entryID)
		}
		err := logFile.Sync(context.TODO())
		assert.NoError(t, err)
		lastEntryId, err := logFile.GetLastEntryId()
		assert.NoError(t, err)
		assert.Equal(t, int64(numEntries-1), lastEntryId)
	}

	// Reader for middle 80 entries
	{
		roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
		assert.NoError(t, err)
		assert.NotNil(t, roLogFile)
		reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
			StartSequenceNum: 10,
			EndSequenceNum:   90, // Read middle 80 entries
		})
		assert.NoError(t, err)
		defer reader.Close()
		readEntries := make(map[int64]*proto.LogEntry)
		for {
			hasNext, err := reader.HasNext()
			assert.NoError(t, err)
			if !hasNext {
				break
			}
			entry, err := reader.ReadNext()
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
		roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
		assert.NoError(t, err)
		assert.NotNil(t, roLogFile)
		reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
			StartSequenceNum: 0,
			EndSequenceNum:   10, // Read middle 80 entries
		})
		assert.NoError(t, err)
		defer reader.Close()
		readEntries := make(map[int64]*proto.LogEntry)
		for {
			hasNext, err := reader.HasNext()
			assert.NoError(t, err)
			if !hasNext {
				break
			}
			entry, err := reader.ReadNext()
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
		roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
		assert.NoError(t, err)
		assert.NotNil(t, roLogFile)
		reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
			StartSequenceNum: 90,
			EndSequenceNum:   100, // Read middle 80 entries
		})
		assert.NoError(t, err)
		defer reader.Close()
		readEntries := make(map[int64]*proto.LogEntry)
		for {
			hasNext, err := reader.HasNext()
			assert.NoError(t, err)
			if !hasNext {
				break
			}
			entry, err := reader.ReadNext()
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
	err = logFile.Close()
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
		logFile, err := NewDiskLogFile(1, 0, 1, dir)
		assert.NoError(t, err)
		// First write 20 entries
		resultChannels := make([]<-chan int64, 0, initialEntries)
		entryIDs := make([]int64, 0, initialEntries)
		for i := 0; i < initialEntries; i++ {
			entryID := int64(i)
			entryValue := []byte(fmt.Sprintf("test_data_%d", i))
			id, ch, err := logFile.AppendAsync(context.Background(), entryID, entryValue)
			assert.NoError(t, err)
			assert.Equal(t, entryID, id)
			resultChannels = append(resultChannels, ch)
			entryIDs = append(entryIDs, entryID)
		}
		err = logFile.Sync(context.TODO())
		assert.NoError(t, err)
		lastEntryId, err := logFile.GetLastEntryId()
		assert.NoError(t, err)
		assert.Equal(t, int64(initialEntries-1), lastEntryId)
		initialLastEntryID = lastEntryId
	}

	// Reload log file
	{
		roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
		assert.NoError(t, err)
		assert.NotNil(t, roLogFile)

		// Load data
		_, fragment, err := roLogFile.Load(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, fragment)

		// Get last entry ID after loading
		loadedLastEntryID, err := roLogFile.GetLastEntryId()
		assert.NoError(t, err)
		assert.Equal(t, initialLastEntryID, loadedLastEntryID, "GetLastEntryId() should return the same value as loaded")
	}

	{
		// open exists log file for write
		logFile, err := NewDiskLogFile(1, 0, 1, dir)
		assert.Error(t, err)
		assert.Nil(t, logFile)
	}
}

// TestGetId tests the GetId function.
func TestGetId(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 42, dir)
	assert.NoError(t, err)
	assert.Equal(t, int64(42), logFile.GetId())

	// Cleanup
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestInvalidReaderRange tests creating a reader with invalid range.
func TestInvalidReaderRange(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)

	// Track entry IDs and data
	var entryIDs []int64

	// Append a few entries
	for i := 0; i < 5; i++ {
		data := []byte{byte(i)}
		// Use AppendAsync to get ID
		id, resultCh, err := logFile.AppendAsync(context.Background(), int64(i), data)
		assert.NoError(t, err)
		// Wait for append to complete
		select {
		case result := <-resultCh:
			assert.Equal(t, id, result)
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout waiting for append result")
		}
		entryIDs = append(entryIDs, id)
		t.Logf("Appended entry %d with ID %d and data '%d'", i, id, i)
	}

	// Ensure all data has been written
	err = logFile.Sync(context.Background())
	assert.NoError(t, err)

	// Ensure there are enough entries for testing
	if len(entryIDs) < 5 {
		t.Fatalf("Not enough entries to test read range")
		return
	}

	lastID := entryIDs[len(entryIDs)-1]
	beyondLastID := lastID + 5 // An ID beyond the range

	// Try to create a reader with start beyond available entries
	roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roLogFile)
	beyondReader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: beyondLastID, // Beyond available entries
		EndSequenceNum:   beyondLastID + 5,
	})
	assert.NoError(t, err)
	hasNext, err := beyondReader.HasNext()
	assert.NoError(t, err)
	assert.False(t, hasNext)

	// Create another reader using verified actual ID range, and verify read results
	// Explicitly read only from the 3rd to the 5th entry (indices 2 to 4)
	startID := entryIDs[2]
	endID := entryIDs[4] + 1 // +1 because the range is half-open

	reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: startID,
		EndSequenceNum:   endID,
	})
	assert.NoError(t, err)
	defer reader.Close()

	// Verify that expected entries can be read
	for i := 2; i <= 4; i++ {
		hasNextMsg, err := reader.HasNext()
		assert.NoError(t, err)
		assert.True(t, hasNextMsg, "Should have entry at index %d", i)
		entry, err := reader.ReadNext()
		assert.NoError(t, err)
		assert.Equal(t, entryIDs[i], entry.EntryId, "Entry ID should match")
		t.Logf("Successfully read entry at index %d: ID=%d", i, entry.EntryId)
	}

	// After reading all 3 entries, there should be no more entries
	hasNextMsg, err := reader.HasNext()
	assert.NoError(t, err)
	assert.False(t, hasNextMsg, "Should not have more entries after reading all valid ones")

	// Try to read again, should return error
	_, err = reader.ReadNext()
	assert.Error(t, err, "Reading out of range should return error")
	assert.True(t, strings.Contains(err.Error(), "invalid data offset"))
	t.Logf("Reading out of range returned expected error: %v", err)

	// Cleanup
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestReadAfterClose tests reading from a closed log file.
func TestReadAfterClose(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)

	// Write a small amount of data
	numEntries := 5
	for i := 0; i < numEntries; i++ {
		entryID := int64(i)
		entryValue := []byte(fmt.Sprintf("test_data_%d", i))
		id, _, err := logFile.AppendAsync(context.Background(), entryID, entryValue)
		assert.NoError(t, err)
		assert.Equal(t, entryID, id)
	}

	// Force sync
	fmt.Println("Force syncing data")
	err = logFile.Sync(context.TODO())
	assert.NoError(t, err)
	lastEntryId, err := logFile.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, int64(numEntries-1), lastEntryId)

	// Close file
	fmt.Println("Closing file")
	err = logFile.Close()
	assert.NoError(t, err)

	roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roLogFile)

	reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,
		EndSequenceNum:   2,
	})
	assert.NoError(t, err)
	assert.NotNil(t, reader)
	for i := 0; i < 2; i++ {
		hasNext, err := reader.HasNext()
		assert.NoError(t, err)
		assert.True(t, hasNext)
		entry, err := reader.ReadNext()
		assert.NoError(t, err)
		expectedID := int64(i)
		assert.Equal(t, expectedID, entry.EntryId, i)
		assert.Equal(t, []byte(fmt.Sprintf("test_data_%d", i)), entry.Values)
	}

	// Verify no more entries
	hasNext, err := reader.HasNext()
	assert.NoError(t, err)
	assert.False(t, hasNext)

	// Cleanup
	err = reader.Close()
	assert.NoError(t, err)
	err = roLogFile.Close()
	assert.NoError(t, err)
}

// TestBasicReader tests basic reader functionality.
func TestBasicReader(t *testing.T) {
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)

	// Append entries with specific IDs to ensure we know the exact IDs to read later
	startID := int64(0) // Use a larger starting ID to ensure no conflicts
	numEntries := 10

	// Append ordered entries
	for i := 0; i < numEntries; i++ {
		entryID := startID + int64(i)
		data := []byte(fmt.Sprintf("data-%d", i))
		_, ch, err := logFile.AppendAsync(context.Background(), entryID, data)
		assert.NoError(t, err)
		<-ch // Wait for write to complete
	}

	// Ensure data has been written
	err = logFile.Sync(context.Background())
	assert.NoError(t, err)

	// Create reader for all entries
	roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roLogFile)
	reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: startID,
		EndSequenceNum:   startID + int64(numEntries),
	})
	assert.NoError(t, err)

	// Read and verify all entries
	for i := 0; i < numEntries; i++ {
		hasNext, err := reader.HasNext()
		assert.NoError(t, err)
		assert.True(t, hasNext)
		entry, err := reader.ReadNext()
		assert.NoError(t, err)
		expectedID := startID + int64(i)
		assert.Equal(t, expectedID, entry.EntryId, i)
		assert.Equal(t, []byte(fmt.Sprintf("data-%d", i)), entry.Values)
	}

	// Verify no more entries
	hasNext, err := reader.HasNext()
	assert.NoError(t, err)
	assert.False(t, hasNext)

	// Cleanup
	err = reader.Close()
	assert.NoError(t, err)
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestOnlyFirstAndLast skips read testing, only tests getting first and last entry IDs
func TestOnlyFirstAndLast(t *testing.T) {
	dir := getTempDir(t)
	logFile, err := NewDiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)

	// Use simple Append method to add data, letting the system assign IDs
	numEntries := 5
	testData := make([][]byte, numEntries)

	for i := 0; i < numEntries; i++ {
		testData[i] = []byte(fmt.Sprintf("data-%d", i))
		err = logFile.Append(context.Background(), testData[i])
		assert.NoError(t, err)
		t.Logf("Appended data: %s", testData[i])
	}

	// Ensure data has been written
	err = logFile.Sync(context.Background())
	assert.NoError(t, err)

	// Get last entry ID
	lastEntryID, err := logFile.GetLastEntryId()
	assert.NoError(t, err)
	t.Logf("Last entry ID after writes: %d", lastEntryID)

	// Ensure lastEntryID is valid
	assert.Equal(t, lastEntryID, int64(numEntries-1), "Last entry ID should be at least %d", numEntries-1)

	// Cleanup
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestSequentialBufferAppend Test based on SequentialBuffer implementation
func TestSequentialBufferAppend(t *testing.T) {
	tempDir := getTempDir(t)
	defer os.RemoveAll(tempDir)

	dlf, err := NewDiskLogFile(1, 0, 1, tempDir)
	assert.NoError(t, err)
	defer dlf.Close()

	// Test continuous ordered write
	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf("data-%d", i))
		id, ch, err := dlf.AppendAsync(context.Background(), int64(i), data)
		assert.NoError(t, err)
		assert.Equal(t, int64(i), id)
		<-ch // Wait for write to complete
	}

	// Force sync buffer to disk
	err = dlf.Sync(context.Background())
	assert.NoError(t, err)

	// Get last written entryID
	lastID, err := dlf.GetLastEntryId()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), lastID)

	// Verify can read written data
	roLogFile, err := NewRODiskLogFile(1, 0, 1, tempDir)
	assert.NoError(t, err)
	assert.NotNil(t, roLogFile)
	reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
		StartSequenceNum: 0,
		EndSequenceNum:   5, // Read all entries
	})
	assert.NoError(t, err)

	// Read and verify data
	entryCount := 0
	for {
		hasNext, err := reader.HasNext()
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext()
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
	logFile, err := NewDiskLogFile(1, 0, 1, dir, WithWriteFragmentSize(10*1024*1024))
	assert.NoError(t, err)

	// Record write start time
	writeStartTime := time.Now()

	// Use larger starting ID to avoid conflicts with auto-allocated ID
	resultChannels := make([]<-chan int64, testEntryCount)
	// Record data for each ID, for subsequent verification
	entryData := make(map[int][]byte)

	t.Logf("Starting to write %d entries...", testEntryCount)

	for id := 0; id < testEntryCount; id++ {
		// Create data, include ID information for verification
		data := []byte(fmt.Sprintf("data-for-entry-%d", id))
		entryData[id] = data

		assignedID, ch, err := logFile.AppendAsync(context.Background(), int64(id), data)
		assert.NoError(t, err)
		assert.Equal(t, int64(id), assignedID, "Assigned ID should match requested ID")
		resultChannels[id] = ch

		// Print progress every 1000 entries
		if (id+1)%1000 == 0 {
			t.Logf("Wrote %d/%d entries", id+1, testEntryCount)
		}
	}

	// Wait for all write results
	successCount := 0
	failCount := 0
	for i, ch := range resultChannels {
		select {
		case result := <-ch:
			if result >= 0 {
				successCount++
			} else {
				failCount++
				t.Logf("Failed to write entry %d", i)
			}
		case <-time.After(10 * time.Second): // Increase timeout
			t.Logf("Timeout waiting for append result for ID %d", i)
			failCount++
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
	err = logFile.Sync(context.Background())
	assert.NoError(t, err)

	// Record read start time
	readStartTime := time.Now()

	// Create reader to verify data, ensure read from ID 0 to all data
	roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
	assert.NoError(t, err)
	assert.NotNil(t, roLogFile)
	reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
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
		hasNext, err := reader.HasNext()
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext()
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
	err = logFile.Close()
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

	logFile, err := NewDiskLogFile(1, 0, 1, dir,
		WithWriteFragmentSize(smallFragmentSize),
		WithWriteMaxEntryPerFile(maxEntriesPerFragment))
	assert.NoError(t, err)

	// Record write start time
	writeStartTime := time.Now()

	// Use larger starting ID to avoid conflicts with auto-allocated ID
	resultChannels := make([]<-chan int64, testEntryCount)
	// Record data for each ID, for subsequent verification
	entryData := make(map[int][]byte)

	t.Logf("Starting to write %d entries with forced fragment rotations...", testEntryCount)

	for id := 0; id < testEntryCount; id++ {
		// Create data, include ID information for verification
		data := []byte(fmt.Sprintf("data-for-entry-%d", id))
		entryData[id] = data

		assignedID, ch, err := logFile.AppendAsync(context.Background(), int64(id), data)
		assert.NoError(t, err)
		assert.Equal(t, int64(id), assignedID, "Assigned ID should match requested ID")
		resultChannels[id] = ch

		// Print progress every 100 entries (should trigger fragment rotation)
		if (id+1)%100 == 0 {
			t.Logf("Wrote %d/%d entries (should trigger fragment rotation)", id+1, testEntryCount)

			// Force sync to ensure fragment rotation happens
			err = logFile.Sync(context.Background())
			assert.NoError(t, err, "Failed to sync at entry %d", id+1)

			// Get current fragment information
			roLogFile, err := NewRODiskLogFile(1, 0, logFile.GetId(), dir)
			assert.NoError(t, err)
			assert.NotNil(t, roLogFile)
			_, _, err = roLogFile.fetchROFragments()
			assert.NoError(t, err)
			t.Logf("After %d entries, fragment count: %d", id+1, len(roLogFile.fragments))

			if len(roLogFile.fragments) > 0 {
				lastFrag := roLogFile.fragments[len(roLogFile.fragments)-1]
				firstID, _ := lastFrag.GetFirstEntryId()
				lastID, _ := lastFrag.GetLastEntryId()
				t.Logf("Last fragment: ID=%d, first entry=%d, last entry=%d",
					lastFrag.GetFragmentId(), firstID, lastID)
			}
		}
	}

	// Wait for all write results
	successCount := 0
	failCount := 0
	for i, ch := range resultChannels {
		select {
		case result := <-ch:
			if result >= 0 {
				successCount++
			} else {
				failCount++
				t.Logf("Failed to write entry %d", i)
			}
		case <-time.After(2 * time.Second): // Shorter timeout for faster testing
			t.Logf("Timeout waiting for append result for ID %d", i)
			failCount++
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
	err = logFile.Sync(context.Background())
	assert.NoError(t, err)

	// Get final fragment information
	roLogFile, err := NewRODiskLogFile(1, 0, logFile.GetId(), dir)
	assert.NoError(t, err)
	assert.NotNil(t, roLogFile)
	_, _, err = roLogFile.fetchROFragments()
	assert.NoError(t, err)
	t.Logf("Final fragment count: %d", len(roLogFile.fragments))

	// Verify multiple fragments were created (confirm rotation occurred)
	assert.Greater(t, len(roLogFile.fragments), 1, "Multiple fragments should be created due to small fragment size")

	for i, frag := range roLogFile.fragments {
		firstID, _ := frag.GetFirstEntryId()
		lastID, _ := frag.GetLastEntryId()
		entryCount := lastID - firstID + 1
		t.Logf("Fragment[%d]: ID=%d, first entry=%d, last entry=%d, entries=%d",
			i, frag.GetFragmentId(), firstID, lastID, entryCount)
	}

	// Record read start time
	readStartTime := time.Now()

	// Create reader to verify data, ensure reading all data from ID 0
	reader, err := roLogFile.NewReader(context.Background(), storage.ReaderOpt{
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
		hasNext, err := reader.HasNext()
		assert.NoError(t, err)
		if !hasNext {
			break
		}
		entry, err := reader.ReadNext()
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
	err = logFile.Close()
	assert.NoError(t, err)
}

// TestFragmentDataValueCheck Debug Test Only
func TestFragmentDataValueCheck(t *testing.T) {
	t.Skipf("just for debug, skip")
	cfg, _ := config.NewConfiguration()
	cfg.Log.Level = "debug"
	logger.InitLogger(cfg)

	for i := 0; i <= 14; i++ {
		filePath := fmt.Sprintf("/tmp/TestWriteReadPerf/woodpecker/14/0/log_0/fragment_%d", i)
		ff, err := NewFragmentFileReader(filePath, 128*1024*1024, 14, 0, int64(i))
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

		// Create a DiskLogFile object with a mock directory
		logDir := filepath.Join(testDir, fmt.Sprintf("log_%d", logId))
		err := os.MkdirAll(logDir, 0755)
		assert.NoError(t, err)

		// Create read-only log file
		roLogFile, err := NewRODiskLogFile(logId, 0, logId, testDir)
		assert.NoError(t, err)
		assert.NotNil(t, roLogFile)

		// Execute deletion operation
		err = roLogFile.DeleteFragments(context.Background(), 0)
		assert.NoError(t, err, "DeleteFragments should not error with empty directory")

		// Verify internal state has been reset
		assert.Equal(t, 0, len(roLogFile.fragments), "fragments should be empty")

		// Close
		err = roLogFile.Close()
		assert.NoError(t, err)
	})

	t.Run("NonExistentDirectory", func(t *testing.T) {
		// Test case where directory does not exist
		nonExistDir := getTempDir(t)
		os.RemoveAll(nonExistDir) // Ensure directory does not exist

		logFile2, err := NewRODiskLogFile(1, 0, 2, nonExistDir)
		assert.NoError(t, err)

		err = logFile2.DeleteFragments(context.Background(), 0)
		assert.NoError(t, err, "DeleteFragments should not error when directory doesn't exist")

		// Verify state is also correctly reset
		assert.Equal(t, 0, len(logFile2.fragments), "fragments should be empty")

		err = logFile2.Close()
		assert.NoError(t, err)
	})

	t.Run("WithFragmentFiles", func(t *testing.T) {
		dir := getTempDir(t)

		// first write multi fragments
		{
			cfg, _ := config.NewConfiguration()
			cfg.Log.Level = "debug"
			logger.InitLogger(cfg)

			logFile, err := NewDiskLogFile(1, 0, 1, dir, WithWriteMaxEntryPerFile(10))
			assert.NoError(t, err)
			assert.NotNil(t, logFile)

			entryData := make(map[int64][]byte)
			// Append a few entries
			{
				// First write 100 entries
				numEntries := 100
				resultChannels := make([]<-chan int64, 0, numEntries)
				entryIDs := make([]int64, 0, numEntries)
				for i := 0; i < numEntries; i++ {
					entryID := int64(i)
					entryValue := []byte(fmt.Sprintf("test_data_%d", i))
					entryData[entryID] = entryValue
					id, ch, err := logFile.AppendAsync(context.Background(), entryID, entryValue)
					assert.NoError(t, err)
					assert.Equal(t, entryID, id)
					resultChannels = append(resultChannels, ch)
					entryIDs = append(entryIDs, entryID)
				}
				err := logFile.Sync(context.TODO())
				assert.NoError(t, err)
				lastEntryId, err := logFile.GetLastEntryId()
				assert.NoError(t, err)
				assert.Equal(t, int64(numEntries-1), lastEntryId)
			}

			// Ensure all data has been written
			err = logFile.Sync(context.Background())
			assert.NoError(t, err)

			// Confirm that 10 fragments were created through rotation
			roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
			assert.NoError(t, err)
			assert.NotNil(t, roLogFile)
			_, _, err = roLogFile.fetchROFragments()
			assert.NoError(t, err)
			assert.NotNil(t, roLogFile.fragments)
			assert.Equal(t, 10, len(roLogFile.fragments))

			// Check that at least one fragment file was created
			files, err := os.ReadDir(logFile.logFileDir)
			assert.NoError(t, err)
			assert.Equal(t, 10, len(files), "Expected 10 fragment files")
		}

		// Create read-only log file
		roLogFile, err := NewRODiskLogFile(1, 0, 1, dir)
		assert.NoError(t, err)
		assert.NotNil(t, roLogFile)

		_, _, err = roLogFile.fetchROFragments()
		assert.NoError(t, err)
		assert.Equal(t, 10, len(roLogFile.fragments))

		// Execute deletion operation
		err = roLogFile.DeleteFragments(context.Background(), 0)
		assert.NoError(t, err, "DeleteFragments should successfully delete fragment files")

		// Verify internal state has been reset
		_, _, err = roLogFile.fetchROFragments()
		assert.NoError(t, err)
		assert.Equal(t, 0, len(roLogFile.fragments), "fragments should be empty")

		err = roLogFile.Close()
		assert.NoError(t, err)
	})
}

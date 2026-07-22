package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalSegmentCleanupStatus_Roundtrip(t *testing.T) {
	original := &SegmentCleanupStatus{
		LogId:               7,
		SegmentId:           3,
		State:               SegmentCleanupState_CLEANUP_FAILED,
		StartTime:           111,
		LastUpdateTime:      222,
		QuorumCleanupStatus: map[string]bool{"node1": true, "node2": false},
		ErrorMessage:        "boom",
	}

	data, err := MarshalSegmentCleanupStatus(original)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	got := &SegmentCleanupStatus{}
	require.NoError(t, UnmarshalSegmentCleanupStatus(data, got))
	assert.Equal(t, original.LogId, got.LogId)
	assert.Equal(t, original.SegmentId, got.SegmentId)
	assert.Equal(t, original.State, got.State)
	assert.Equal(t, original.StartTime, got.StartTime)
	assert.Equal(t, original.LastUpdateTime, got.LastUpdateTime)
	assert.Equal(t, original.QuorumCleanupStatus, got.QuorumCleanupStatus)
	assert.Equal(t, original.ErrorMessage, got.ErrorMessage)
}

func TestMarshalSegmentCleanupStatus_Errors(t *testing.T) {
	_, err := MarshalSegmentCleanupStatus(nil)
	assert.Error(t, err)

	assert.Error(t, UnmarshalSegmentCleanupStatus([]byte{0x0a, 0x0a}, nil), "nil target")
	assert.Error(t, UnmarshalSegmentCleanupStatus([]byte{0x0a, 0x0a}, &SegmentCleanupStatus{}), "invalid bytes")
}

func TestMarshalSegmentCompactedNotifyStatus_Roundtrip(t *testing.T) {
	original := &SegmentCompactedNotifyStatus{
		LogId:              7,
		SegmentId:          3,
		State:              SegmentCompactedNotifyState_NOTIFY_PENDING_MANUAL,
		StartTime:          111,
		LastUpdateTime:     222,
		QuorumNotifyStatus: map[string]bool{"node1": true, "node2": false},
		ErrorMessage:       "nodes unacked after 30m0s of retries: node2",
	}

	data, err := MarshalSegmentCompactedNotifyStatus(original)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	got := &SegmentCompactedNotifyStatus{}
	require.NoError(t, UnmarshalSegmentCompactedNotifyStatus(data, got))
	assert.Equal(t, original.LogId, got.LogId)
	assert.Equal(t, original.SegmentId, got.SegmentId)
	assert.Equal(t, original.State, got.State)
	assert.Equal(t, original.StartTime, got.StartTime)
	assert.Equal(t, original.LastUpdateTime, got.LastUpdateTime)
	assert.Equal(t, original.QuorumNotifyStatus, got.QuorumNotifyStatus)
	assert.Equal(t, original.ErrorMessage, got.ErrorMessage)
}

func TestMarshalSegmentCompactedNotifyStatus_Errors(t *testing.T) {
	// Marshal rejects a nil message.
	_, err := MarshalSegmentCompactedNotifyStatus(nil)
	assert.Error(t, err)

	// Unmarshal rejects a nil target.
	assert.Error(t, UnmarshalSegmentCompactedNotifyStatus([]byte{0x0a, 0x0a}, nil), "nil target")

	// Unmarshal surfaces a decode error on malformed bytes (field 1, length-delimited,
	// claims length 10 with no data following).
	assert.Error(t, UnmarshalSegmentCompactedNotifyStatus([]byte{0x0a, 0x0a}, &SegmentCompactedNotifyStatus{}), "invalid bytes")
}

// TestMarshalSegmentCompactedNotifyStatus_EmptyMessage verifies a zero-value message
// roundtrips (a valid, empty encoding — distinct from the nil-pointer error case).
func TestMarshalSegmentCompactedNotifyStatus_EmptyMessage(t *testing.T) {
	data, err := MarshalSegmentCompactedNotifyStatus(&SegmentCompactedNotifyStatus{})
	require.NoError(t, err)

	got := &SegmentCompactedNotifyStatus{}
	require.NoError(t, UnmarshalSegmentCompactedNotifyStatus(data, got))
	assert.Equal(t, int64(0), got.LogId)
	assert.Equal(t, SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS, got.State)
}

package workload

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecorderRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "acked.jsonl")
	r, err := openRecorder(path, true)
	require.NoError(t, err)
	in := []AckRecord{
		{LogName: "chaos-n152-0", LogId: 1, SegmentId: 0, EntryId: 0, PayloadHash: "aa", Seq: 1},
		{LogName: "chaos-n152-0", LogId: 1, SegmentId: 0, EntryId: 1, PayloadHash: "bb", Seq: 2},
	}
	for _, rec := range in {
		require.NoError(t, r.append(rec))
	}
	require.NoError(t, r.close())
	out, err := loadRecords(path)
	require.NoError(t, err)
	require.Equal(t, in, out)
	_ = os.Remove(path)
}

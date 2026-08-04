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

func TestEarliestAck(t *testing.T) {
	for _, tc := range []struct {
		name             string
		recs             []AckRecord
		wantSeg, wantEnt int64
	}{
		{"single", []AckRecord{{SegmentId: 7, EntryId: 3}}, 7, 3},
		{"ordered", []AckRecord{{SegmentId: 4, EntryId: 0}, {SegmentId: 4, EntryId: 1}, {SegmentId: 5, EntryId: 0}}, 4, 0},
		// Records arrive per-writer, so a phase's slice is not guaranteed to be sorted by position.
		{"unordered", []AckRecord{{SegmentId: 9, EntryId: 2}, {SegmentId: 4, EntryId: 8}, {SegmentId: 6, EntryId: 0}}, 4, 8},
		{"same segment picks lower entry", []AckRecord{{SegmentId: 3, EntryId: 9}, {SegmentId: 3, EntryId: 2}}, 3, 2},
		// A later scenario's acks start deep into the reused log — the point of #259.
		{"offset into reused log", []AckRecord{{SegmentId: 120, EntryId: 5}, {SegmentId: 121, EntryId: 0}}, 120, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seg, ent := earliestAck(tc.recs)
			require.Equal(t, tc.wantSeg, seg)
			require.Equal(t, tc.wantEnt, ent)
		})
	}
}

// The verify reader must start at or before every ack it has to observe, otherwise read-back
// silently misses entries and the I1 assertion can never be satisfied.
func TestEarliestAckIsAtOrBeforeEveryRecord(t *testing.T) {
	recs := []AckRecord{
		{SegmentId: 12, EntryId: 4}, {SegmentId: 9, EntryId: 7},
		{SegmentId: 12, EntryId: 0}, {SegmentId: 30, EntryId: 1}, {SegmentId: 9, EntryId: 9},
	}
	seg, ent := earliestAck(recs)
	for _, r := range recs {
		require.True(t, seg < r.SegmentId || (seg == r.SegmentId && ent <= r.EntryId),
			"start (%d,%d) is after record (%d,%d)", seg, ent, r.SegmentId, r.EntryId)
	}
}

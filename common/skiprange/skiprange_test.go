package skiprange

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSpanFor_ScansWhateverOrderTheRangesCameIn pins the one thing the reader's lookup may not
// depend on. The write path sorts and coalesces, but a record written by an older command, or edited
// by hand, may arrive unsorted or overlapping -- and a reader that answered wrongly for such a
// record would skip the wrong entries rather than merely take an extra hop.
func TestSpanFor_ScansWhateverOrderTheRangesCameIn(t *testing.T) {
	ranges := BySegment{3: []Span{
		{FromEntryID: 40, ToEntryID: 49},
		{FromEntryID: 10, ToEntryID: 19},
		{FromEntryID: 15, ToEntryID: 25}, // overlaps the one before it
	}}

	for _, tc := range []struct {
		entry int64
		found bool
		to    int64
	}{
		{entry: 9, found: false},
		{entry: 10, found: true, to: 19},
		{entry: 22, found: true, to: 25},
		{entry: 30, found: false},
		{entry: 45, found: true, to: 49},
		{entry: 50, found: false},
	} {
		span, found := SpanFor(ranges, 3, tc.entry)
		require.Equal(t, tc.found, found, "entry %d", tc.entry)
		if tc.found {
			require.Equal(t, tc.to, span.ToEntryID, "entry %d", tc.entry)
		}
	}

	_, found := SpanFor(ranges, 4, 10)
	require.False(t, found, "another segment's entry 10 is not this segment's")
	_, found = SpanFor(nil, 3, 10)
	require.False(t, found, "no ranges declared at all")
}

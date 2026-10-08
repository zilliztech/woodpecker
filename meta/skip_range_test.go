// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/proto"
)

// TestSkipRangeCovering_ScansWhateverOrderTheRangesCameIn pins the one thing the reader's lookup may
// not depend on. The write path sorts and coalesces, but a record written by an older command, or
// edited by hand, may arrive unsorted or overlapping -- and a reader that answered wrongly for such a
// record would pass over the wrong entries rather than merely take an extra hop.
func TestSkipRangeCovering_ScansWhateverOrderTheRangesCameIn(t *testing.T) {
	ranges := &proto.LogSkipRanges{BySegmentId: map[int64]*proto.SegmentSkipRanges{3: {Ranges: []*proto.SkipRange{
		{FromEntryId: 40, ToEntryId: 49},
		{FromEntryId: 10, ToEntryId: 19},
		{FromEntryId: 15, ToEntryId: 25}, // overlaps the one before it
	}}}}

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
		skip := SkipRangeCovering(ranges, 3, tc.entry)
		require.Equal(t, tc.found, skip != nil, "entry %d", tc.entry)
		if tc.found {
			require.Equal(t, tc.to, skip.GetToEntryId(), "entry %d", tc.entry)
		}
	}

	require.Nil(t, SkipRangeCovering(ranges, 4, 10), "another segment's entry 10 is not this segment's")
	require.Nil(t, SkipRangeCovering(nil, 3, 10), "no ranges declared at all")
	require.Nil(t, SkipRangeCovering(&proto.LogSkipRanges{}, 3, 10), "a log present in the record with nothing for it")
}

// TestAllSkipRangesFor covers the wrapper's nil-safety, which the read path relies on: a provider
// that has not read anything yet, and a record that has nothing for this log, both read as nothing
// declared rather than panicking inside a reader.
func TestAllSkipRangesFor(t *testing.T) {
	var absent *AllSkipRanges
	require.Nil(t, absent.For(7))
	require.Nil(t, (&AllSkipRanges{}).For(7), "no metadata at all")
	require.Nil(t, (&AllSkipRanges{Metadata: &proto.AllSkipRanges{}}).For(7), "an empty record")

	held := &AllSkipRanges{Metadata: &proto.AllSkipRanges{ByLogId: map[int64]*proto.LogSkipRanges{
		7: {BySegmentId: map[int64]*proto.SegmentSkipRanges{3: {Ranges: []*proto.SkipRange{{FromEntryId: 1, ToEntryId: 2}}}}},
	}}}
	require.NotNil(t, held.For(7))
	require.Nil(t, held.For(8), "another log")
}

// TestTruncateSkipRangeReason pins the UTF-8-safe cut: protobuf refuses to marshal a string that is
// not valid UTF-8, so a byte-count cut through a multi-byte rune fails the write after an operator
// has already confirmed it. Short reasons pass through, over-budget ones are cut, and a cut that
// lands mid-rune backs off to the last whole rune.
func TestTruncateSkipRangeReason(t *testing.T) {
	// Under budget: unchanged.
	require.Equal(t, "short", TruncateSkipRangeReason("short"))

	// Exactly at the budget: unchanged.
	exact := string(make([]byte, MaxSkipRangeReasonBytes))
	require.Len(t, exact, MaxSkipRangeReasonBytes)
	require.Equal(t, exact, TruncateSkipRangeReason(exact))

	// Over budget by ASCII: cut to the budget, still valid UTF-8.
	longASCII := string(make([]byte, MaxSkipRangeReasonBytes+20))
	got := TruncateSkipRangeReason(longASCII)
	require.Len(t, got, MaxSkipRangeReasonBytes)

	// Over budget with a multi-byte rune straddling the cut: back off to a whole-rune boundary.
	// '中' is 3 bytes; fill so the byte budget ends one byte into a rune, so a naive cut would split it.
	prefix := make([]byte, MaxSkipRangeReasonBytes-2)
	for i := range prefix {
		prefix[i] = 'x'
	}
	over := string(prefix) + "中文"
	got = TruncateSkipRangeReason(over)
	require.LessOrEqual(t, len(got), MaxSkipRangeReasonBytes)
	require.True(t, utf8.ValidString(got), "the cut must not split a rune")
	require.True(t, strings.HasPrefix(got, string(prefix)), "the whole prefix before the cut is kept")
}

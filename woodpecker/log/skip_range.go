package log

import (
	"context"

	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

// SkipSpan is an inclusive range of entry ids a reader should pass over. Entry ids restart at 0 in
// every segment, so a span means nothing without the segment it belongs to.
type SkipSpan struct {
	FromEntryID int64
	ToEntryID   int64
}

// LogSkipRanges is one log's spans indexed by segment id, so a reader that has just resolved a
// segment answers "is any of this skipped" with one lookup.
type LogSkipRanges map[int64][]SkipSpan

// SkipRangeSource answers what a log's readers should pass over.
//
// It is an injected object rather than a configuration field because it is not configuration: the
// ranges are a record of data an operator has established is gone, only readers consult them, and
// a configuration value travels to every part of the system that takes one. Woodpecker's own
// metadata backs it by default; an embedding application that manages these itself supplies its own
// through WithSkipRangeSource, and then the client never reads the record at all.
type SkipRangeSource interface {
	// For returns the ranges declared for one log, or nil when there are none -- which is the normal
	// case, and has to cost the caller nothing, since a reader consults this while stalled.
	For(ctx context.Context, logID int64) LogSkipRanges
}

// metadataSkipRanges reads the ranges from woodpecker's own record, through the provider's
// short-lived cache: the read path never waits on it, and an elapsed interval only starts a refresh
// behind the caller.
type metadataSkipRanges struct {
	provider meta.MetadataProvider
}

func (m metadataSkipRanges) For(ctx context.Context, logID int64) LogSkipRanges {
	if m.provider == nil {
		return nil
	}
	return skipRangesOf(m.provider.GetAllSkipRangesCached(ctx).For(logID))
}

// skipRangesOf converts the stored record into the shape a reader consults, so both sources hand
// the reader the same thing.
func skipRangesOf(held *proto.LogSkipRanges) LogSkipRanges {
	bySegment := held.GetBySegmentId()
	if len(bySegment) == 0 {
		return nil
	}
	out := make(LogSkipRanges, len(bySegment))
	for segmentID, ranges := range bySegment {
		spans := make([]SkipSpan, 0, len(ranges.GetRanges()))
		for _, r := range ranges.GetRanges() {
			spans = append(spans, SkipSpan{FromEntryID: r.GetFromEntryId(), ToEntryID: r.GetToEntryId()})
		}
		out[segmentID] = spans
	}
	return out
}

// skipSpanFor returns the span covering an entry, if any. Ranges within a segment are kept sorted
// and coalesced by the write path, but correctness does not rest on that: scanning a handful
// answers the same however they are ordered, and an overlap only costs a second hop.
func skipSpanFor(ranges LogSkipRanges, segmentID, entryID int64) (SkipSpan, bool) {
	for _, span := range ranges[segmentID] {
		if entryID >= span.FromEntryID && entryID <= span.ToEntryID {
			return span, true
		}
	}
	return SkipSpan{}, false
}

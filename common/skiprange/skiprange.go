// Package skiprange holds the entry ranges an operator has declared unreadable, and the interface a
// reader gets them through.
//
// It sits in common because the two sides cannot see each other: the reader lives in woodpecker/log,
// the record lives in etcd behind the metadata provider, and that provider's package is already
// imported by the reader's. Putting the types and the interface here is what lets the metadata layer
// implement them.
package skiprange

import "context"

// Span is an inclusive range of entry ids a reader should pass over. Entry ids restart at 0 in every
// segment, so a span means nothing without the segment it belongs to.
type Span struct {
	FromEntryID int64
	ToEntryID   int64
}

// BySegment is one log's spans indexed by segment id, so a reader that has just resolved a segment
// answers "is any of this skipped" with one lookup.
type BySegment map[int64][]Span

// Source answers what a log's readers should pass over.
//
// An interface rather than a configuration value because these are not settings: they are a record
// of data an operator has established is gone, only readers consult them, and a configuration value
// travels to every part of the system that takes one. Woodpecker's own metadata implements this; an
// application that manages the ranges itself supplies its own, and then the record is never read.
type Source interface {
	// For returns the ranges declared for one log, or nil when there are none -- which is the normal
	// case, and must cost the caller nothing: a reader consults this while it is stalled.
	For(ctx context.Context, logID int64) BySegment
}

// SpanFor returns the span covering an entry, if any.
//
// The ranges within a segment are kept sorted and non-overlapping by whatever writes the record, but
// this does not depend on that: scanning a handful answers the same however they are ordered, and an
// overlap only costs the caller a second hop. A record written by an older tool, or edited by hand,
// must not make a reader skip the wrong entries.
func SpanFor(ranges BySegment, segmentID, entryID int64) (Span, bool) {
	for _, span := range ranges[segmentID] {
		if entryID >= span.FromEntryID && entryID <= span.ToEntryID {
			return span, true
		}
	}
	return Span{}, false
}

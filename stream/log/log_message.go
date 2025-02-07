package log

// LogMessageId represents the unique identifier for a log message.
type LogMessageId struct {
	SegmentId int64 // The ID of the segment to which this log message belongs.
	EntryId   int64 // The ID of the entry within the segment.
}

// LogMessage represents a log message with an ID and a payload.
type LogMessage struct {
	Id      *LogMessageId // The unique identifier for this log message.
	Payload []byte        // The payload of the log message.
}
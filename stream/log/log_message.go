package log

type LogMessageId struct {
	SegmentId int64
	EntryId   int64
	// TODO SlotId int64
}

type LogMessage struct {
	Id      *LogMessageId
	Payload []byte
}

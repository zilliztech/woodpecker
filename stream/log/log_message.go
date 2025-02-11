package log

import (
	pb "github.com/zilliztech/woodpecker/proto"
	"google.golang.org/protobuf/proto"
	"math"
)

// LogMessageId represents the unique identifier for a log message.
type LogMessageId struct {
	SegmentId int64 // The ID of the segment to which this log message belongs.
	EntryId   int64 // The ID of the entry within the segment.
}

func (i *LogMessageId) Serialize() []byte {
	logMsgId := &pb.LogMessageIdData{
		SegId:   i.SegmentId,
		EntryId: i.EntryId,
	}
	data, _ := proto.Marshal(logMsgId)
	return data
}

func DeserializeLogMessageId(data []byte) (*LogMessageId, error) {
	logMsgId := &pb.LogMessageIdData{}
	err := proto.Unmarshal(data, logMsgId)
	if err != nil {
		return nil, err
	}
	id := &LogMessageId{
		SegmentId: logMsgId.GetSegId(),
		EntryId:   logMsgId.GetEntryId(),
	}
	return id, nil
}

var EarliestLogMessageID = &LogMessageId{
	SegmentId: 0,
	EntryId:   0,
}

var LatestLogMessageID = &LogMessageId{
	SegmentId: math.MaxInt64,
	EntryId:   math.MaxInt64,
}

// LogMessage represents a log message with an ID and a payload.
type LogMessage struct {
	Id      *LogMessageId // The unique identifier for this log message.
	Payload []byte        // The payload of the log message.
}

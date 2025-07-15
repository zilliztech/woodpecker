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

package log

import (
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/common/werr"
	pb "github.com/zilliztech/woodpecker/proto"
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

func EarliestLogMessageID() LogMessageId {
	return LogMessageId{
		SegmentId: 0,
		EntryId:   0,
	}
}

func LatestLogMessageID() LogMessageId {
	return LogMessageId{
		SegmentId: math.MaxInt64,
		EntryId:   math.MaxInt64,
	}
}

// LogMessage represents a log message with an ID and a payload.
type LogMessage struct {
	Id         *LogMessageId     // The unique identifier for this log message.
	Payload    []byte            // The payload of the log message.
	Properties map[string]string // Properties attach application defined properties on the message
}

// WriterMessage abstraction used in LogWriter
type WriterMessage struct {
	Payload    []byte
	Properties map[string]string
}

func MarshalMessage(m *WriterMessage) ([]byte, error) {
	if invalidErr := ValidateMsg(m); invalidErr != nil {
		return nil, invalidErr
	}

	msgLayout := &pb.LogMessageLayout{
		Payload:    m.Payload,
		Properties: m.Properties,
	}
	data, err := proto.Marshal(msgLayout)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func UnmarshalMessage(data []byte) (*LogMessage, error) {
	msgLayout := &pb.LogMessageLayout{}
	err := proto.Unmarshal(data, msgLayout)
	if err != nil {
		return nil, err
	}
	m := &LogMessage{
		Payload:    msgLayout.Payload,
		Properties: msgLayout.Properties,
	}
	return m, nil
}

func ValidateMsg(msg *WriterMessage) error {
	if msg == nil {
		return werr.ErrInvalidMessage.WithCauseErrMsg("message is nil")
	}

	if len(msg.Properties) == 0 && len(msg.Payload) == 0 {
		return werr.ErrInvalidMessage.WithCauseErrMsg("can not set Properties and Payload both")
	}

	return nil
}

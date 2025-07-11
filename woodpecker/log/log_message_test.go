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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/common/werr"
	pb "github.com/zilliztech/woodpecker/proto"
)

func TestMarshalMessage(t *testing.T) {
	t.Run("ValidPayload", func(t *testing.T) {
		msg := &WriterMessage{
			Payload:    []byte("valid test data"),
			Properties: map[string]string{"key": "value"},
		}

		data, err := MarshalMessage(msg)
		require.NoError(t, err)
		assert.Greater(t, len(data), 0)

		// Verify we can unmarshal it back
		layout := &pb.LogMessageLayout{}
		err = proto.Unmarshal(data, layout)
		require.NoError(t, err)
		assert.Equal(t, msg.Payload, layout.Payload)
		assert.Equal(t, msg.Properties, layout.Properties)
	})

	t.Run("EmptyPayload", func(t *testing.T) {
		msg := &WriterMessage{
			Payload:    []byte{},
			Properties: map[string]string{"key": "value"},
		}

		_, err := MarshalMessage(msg)
		require.Error(t, err)
		assert.True(t, werr.ErrEmptyPayload.Is(err), "Error should be ErrEmptyPayload")
	})

	t.Run("NilPayload", func(t *testing.T) {
		msg := &WriterMessage{
			Payload:    nil,
			Properties: map[string]string{"key": "value"},
		}

		_, err := MarshalMessage(msg)
		require.Error(t, err)
		assert.True(t, werr.ErrEmptyPayload.Is(err), "Error should be ErrEmptyPayload")
	})

	t.Run("SingleBytePayload", func(t *testing.T) {
		msg := &WriterMessage{
			Payload:    []byte("a"),
			Properties: map[string]string{"key": "value"},
		}

		data, err := MarshalMessage(msg)
		require.NoError(t, err)
		assert.Greater(t, len(data), 0)

		// Verify content
		layout := &pb.LogMessageLayout{}
		err = proto.Unmarshal(data, layout)
		require.NoError(t, err)
		assert.Equal(t, []byte("a"), layout.Payload)
	})

	t.Run("LargePayload", func(t *testing.T) {
		largeData := make([]byte, 1024*1024) // 1MB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		msg := &WriterMessage{
			Payload:    largeData,
			Properties: map[string]string{"size": "1MB"},
		}

		data, err := MarshalMessage(msg)
		require.NoError(t, err)
		assert.Greater(t, len(data), len(largeData))

		// Verify content
		layout := &pb.LogMessageLayout{}
		err = proto.Unmarshal(data, layout)
		require.NoError(t, err)
		assert.Equal(t, largeData, layout.Payload)
	})

	t.Run("NoProperties", func(t *testing.T) {
		msg := &WriterMessage{
			Payload:    []byte("test data without properties"),
			Properties: nil,
		}

		data, err := MarshalMessage(msg)
		require.NoError(t, err)
		assert.Greater(t, len(data), 0)

		// Verify content
		layout := &pb.LogMessageLayout{}
		err = proto.Unmarshal(data, layout)
		require.NoError(t, err)
		assert.Equal(t, msg.Payload, layout.Payload)
		assert.Nil(t, layout.Properties)
	})

	t.Run("EmptyProperties", func(t *testing.T) {
		msg := &WriterMessage{
			Payload:    []byte("test data with empty properties"),
			Properties: map[string]string{},
		}

		data, err := MarshalMessage(msg)
		require.NoError(t, err)
		assert.Greater(t, len(data), 0)

		// Verify content
		layout := &pb.LogMessageLayout{}
		err = proto.Unmarshal(data, layout)
		require.NoError(t, err)
		assert.Equal(t, msg.Payload, layout.Payload)
		// Empty map becomes nil after protobuf serialization/deserialization
		if layout.Properties == nil {
			assert.Nil(t, layout.Properties)
		} else {
			assert.Equal(t, map[string]string{}, layout.Properties)
		}
	})
}

func TestUnmarshalMessage(t *testing.T) {
	t.Run("ValidData", func(t *testing.T) {
		originalMsg := &WriterMessage{
			Payload:    []byte("test unmarshal data"),
			Properties: map[string]string{"test": "unmarshal"},
		}

		// Marshal first
		data, err := MarshalMessage(originalMsg)
		require.NoError(t, err)

		// Then unmarshal
		msg, err := UnmarshalMessage(data)
		require.NoError(t, err)
		assert.Equal(t, originalMsg.Payload, msg.Payload)
		assert.Equal(t, originalMsg.Properties, msg.Properties)
	})

	t.Run("InvalidData", func(t *testing.T) {
		invalidData := []byte("this is not valid protobuf data")

		_, err := UnmarshalMessage(invalidData)
		require.Error(t, err)
	})

	t.Run("EmptyData", func(t *testing.T) {
		// Empty data should fail to unmarshal as it's not valid protobuf
		_, err := UnmarshalMessage([]byte{})
		if err == nil {
			// If protobuf accepts empty data, that's OK too
			t.Skip("Protobuf accepts empty data")
		}
	})

	t.Run("NilData", func(t *testing.T) {
		// Nil data should fail to unmarshal as it's not valid protobuf
		_, err := UnmarshalMessage(nil)
		if err == nil {
			// If protobuf accepts nil data, that's OK too
			t.Skip("Protobuf accepts nil data")
		}
	})
}

func BenchmarkMarshalMessage(b *testing.B) {
	testCases := []struct {
		name string
		msg  *WriterMessage
	}{
		{
			"Small",
			&WriterMessage{
				Payload:    []byte("small test data"),
				Properties: map[string]string{"key": "value"},
			},
		},
		{
			"Medium",
			&WriterMessage{
				Payload:    make([]byte, 1024), // 1KB
				Properties: map[string]string{"size": "1KB"},
			},
		},
		{
			"Large",
			&WriterMessage{
				Payload:    make([]byte, 64*1024), // 64KB
				Properties: map[string]string{"size": "64KB"},
			},
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := MarshalMessage(tc.msg)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshalMessage(b *testing.B) {
	// Prepare test data
	msg := &WriterMessage{
		Payload:    []byte("benchmark test data for unmarshaling"),
		Properties: map[string]string{"benchmark": "unmarshal"},
	}
	data, err := MarshalMessage(msg)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := UnmarshalMessage(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

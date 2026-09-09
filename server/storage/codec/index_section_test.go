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

package codec

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func indexRecords(n int) []*IndexRecord {
	out := make([]*IndexRecord, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, &IndexRecord{
			BlockNumber: int32(i), StartOffset: int64(100 * (i + 1)), BlockSize: 100,
			FirstEntryID: int64(10 * i), LastEntryID: int64(10*i + 9),
		})
	}
	return out
}

func encodeIndex(records []*IndexRecord) []byte {
	var buf bytes.Buffer
	for _, r := range records {
		buf.Write(EncodeRecord(r))
	}
	return buf.Bytes()
}

func TestValidateIndexSection(t *testing.T) {
	headerSize := uint64(RecordHeaderSize + HeaderRecordSize)

	assert.Error(t, ValidateIndexSection(nil))
	assert.Error(t, ValidateIndexSection(&FooterRecord{IndexOffset: 0, IndexLength: 41, TotalBlocks: 1}), "index at offset 0 is impossible")
	assert.Error(t, ValidateIndexSection(&FooterRecord{IndexOffset: headerSize, IndexLength: 0, TotalBlocks: 3}), "zero-length index cannot describe 3 blocks")

	// The footer Finalize writes for a segment completed with zero blocks.
	assert.NoError(t, ValidateIndexSection(&FooterRecord{IndexOffset: headerSize, IndexLength: 0, TotalBlocks: 0, TotalSize: headerSize}))
	assert.NoError(t, ValidateIndexSection(&FooterRecord{IndexOffset: 500, IndexLength: 41, TotalBlocks: 1}))
}

func TestParseIndexRecords(t *testing.T) {
	headerSize := uint64(RecordHeaderSize + HeaderRecordSize)

	t.Run("empty finalized segment", func(t *testing.T) {
		footer := &FooterRecord{IndexOffset: headerSize, IndexLength: 0, TotalBlocks: 0, TotalSize: headerSize}
		records, err := ParseIndexRecords(nil, footer)
		require.NoError(t, err)
		require.NotNil(t, records)
		assert.Empty(t, records)
	})

	t.Run("round trip", func(t *testing.T) {
		want := indexRecords(3)
		data := encodeIndex(want)
		footer := &FooterRecord{IndexOffset: 1000, IndexLength: uint32(len(data)), TotalBlocks: 3}
		got, err := ParseIndexRecords(data, footer)
		require.NoError(t, err)
		require.Len(t, got, 3)
		for i := range want {
			assert.Equal(t, want[i].BlockNumber, got[i].BlockNumber)
			assert.Equal(t, want[i].StartOffset, got[i].StartOffset)
			assert.Equal(t, want[i].BlockSize, got[i].BlockSize)
			assert.Equal(t, want[i].FirstEntryID, got[i].FirstEntryID)
			assert.Equal(t, want[i].LastEntryID, got[i].LastEntryID)
		}
	})

	t.Run("length mismatch with footer", func(t *testing.T) {
		data := encodeIndex(indexRecords(2))
		footer := &FooterRecord{IndexOffset: 1000, IndexLength: uint32(len(data)) + 1, TotalBlocks: 2}
		_, err := ParseIndexRecords(data, footer)
		assert.Error(t, err)
	})

	t.Run("wrong record type", func(t *testing.T) {
		data := EncodeRecord(&HeaderRecord{Version: FormatVersion, FirstEntryID: 0})
		footer := &FooterRecord{IndexOffset: 1000, IndexLength: uint32(len(data)), TotalBlocks: 1}
		_, err := ParseIndexRecords(data, footer)
		assert.ErrorContains(t, err, "expected index record type")
	})

	t.Run("corrupt record", func(t *testing.T) {
		data := encodeIndex(indexRecords(1))
		data[len(data)-1] ^= 0xff // break the payload, CRC no longer matches
		footer := &FooterRecord{IndexOffset: 1000, IndexLength: uint32(len(data)), TotalBlocks: 1}
		_, err := ParseIndexRecords(data, footer)
		assert.ErrorContains(t, err, "failed to decode index record")
	})

	t.Run("invalid footer is rejected before parsing", func(t *testing.T) {
		_, err := ParseIndexRecords(encodeIndex(indexRecords(1)), &FooterRecord{IndexOffset: 0, IndexLength: 41, TotalBlocks: 1})
		assert.Error(t, err)
	})
}

func TestReadIndexSection(t *testing.T) {
	headerSize := RecordHeaderSize + HeaderRecordSize
	header := EncodeRecord(&HeaderRecord{Version: FormatVersion, FirstEntryID: 0})
	require.Len(t, header, headerSize)

	t.Run("empty finalized file: header + footer only", func(t *testing.T) {
		footer := &FooterRecord{IndexOffset: uint64(headerSize), IndexLength: 0, TotalBlocks: 0, TotalSize: uint64(headerSize), Version: FormatVersion, LAC: -1}
		file := append(append([]byte{}, header...), EncodeRecord(footer)...)
		assert.Len(t, file, headerSize+RecordHeaderSize+FooterRecordSize)

		records, err := ReadIndexSection(bytes.NewReader(file), footer)
		require.NoError(t, err)
		assert.Empty(t, records)
	})

	t.Run("file with blocks", func(t *testing.T) {
		want := indexRecords(2)
		fakeBlocks := bytes.Repeat([]byte{0xab}, 200)
		index := encodeIndex(want)
		indexOffset := uint64(len(header) + len(fakeBlocks))
		footer := &FooterRecord{IndexOffset: indexOffset, IndexLength: uint32(len(index)), TotalBlocks: 2, Version: FormatVersion, LAC: want[1].LastEntryID}
		file := bytes.Join([][]byte{header, fakeBlocks, index, EncodeRecord(footer)}, nil)

		records, err := ReadIndexSection(bytes.NewReader(file), footer)
		require.NoError(t, err)
		require.Len(t, records, 2)
		assert.Equal(t, want[1].LastEntryID, records[1].LastEntryID)
	})

	t.Run("short read", func(t *testing.T) {
		footer := &FooterRecord{IndexOffset: 1000, IndexLength: 41, TotalBlocks: 1}
		_, err := ReadIndexSection(bytes.NewReader(header), footer)
		assert.ErrorContains(t, err, "failed to read index section")
	})
}

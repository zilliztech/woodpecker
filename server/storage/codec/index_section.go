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
	"fmt"
	"io"
)

// ValidateIndexSection checks that a parsed footer describes a readable index
// section. It is the single definition of which footers are acceptable, shared
// by every storage backend that recovers a finalized segment from its footer.
//
// A segment that was finalized with zero blocks is valid: Finalize writes the
// header and the footer with nothing in between, so IndexOffset equals the
// header size and IndexLength is 0. Such a file is complete, it just carries no
// entries; it must not be reported as corrupt, or the segment can never be
// fenced or read again.
func ValidateIndexSection(footer *FooterRecord) error {
	if footer == nil {
		return fmt.Errorf("invalid footer record: nil")
	}
	if footer.IndexOffset == 0 {
		// Every finalized file starts with a header record, so the index
		// section can never begin at offset 0.
		return fmt.Errorf("invalid footer record: IndexOffset=%d, IndexLength=%d",
			footer.IndexOffset, footer.IndexLength)
	}
	if footer.IndexLength == 0 && footer.TotalBlocks != 0 {
		return fmt.Errorf("invalid footer record: IndexLength=0 but TotalBlocks=%d", footer.TotalBlocks)
	}
	return nil
}

// ParseIndexRecords parses the index section of a finalized segment. indexData
// is exactly the IndexLength bytes starting at IndexOffset. Every record must be
// a well-formed IndexRecord; a trailing fragment shorter than a record header is
// ignored. For an empty index section (a segment finalized with zero blocks)
// the result is an empty, non-nil slice.
func ParseIndexRecords(indexData []byte, footer *FooterRecord) ([]*IndexRecord, error) {
	if err := ValidateIndexSection(footer); err != nil {
		return nil, err
	}
	records := make([]*IndexRecord, 0, footer.TotalBlocks)
	if footer.IndexLength == 0 {
		return records, nil
	}
	if len(indexData) != int(footer.IndexLength) {
		return nil, fmt.Errorf("index section is %d bytes, footer says %d", len(indexData), footer.IndexLength)
	}

	recordSize := RecordHeaderSize + IndexRecordSize
	offset := 0
	for offset < len(indexData) {
		if offset+RecordHeaderSize > len(indexData) {
			break
		}
		record, err := DecodeRecord(indexData[offset:])
		if err != nil {
			return nil, fmt.Errorf("failed to decode index record at offset %d: %w", offset, err)
		}
		if record.Type() != IndexRecordType {
			return nil, fmt.Errorf("expected index record type %d, got %d at offset %d",
				IndexRecordType, record.Type(), offset)
		}
		records = append(records, record.(*IndexRecord))
		offset += recordSize
	}
	return records, nil
}

// ReadIndexSection reads the index section described by footer from r and
// parses it with ParseIndexRecords. It is what a local-file backend calls when
// it reopens a finalized segment for recovery.
func ReadIndexSection(r io.ReaderAt, footer *FooterRecord) ([]*IndexRecord, error) {
	if err := ValidateIndexSection(footer); err != nil {
		return nil, err
	}
	if footer.IndexLength == 0 {
		return make([]*IndexRecord, 0), nil
	}
	indexData := make([]byte, footer.IndexLength)
	if _, err := r.ReadAt(indexData, int64(footer.IndexOffset)); err != nil {
		return nil, fmt.Errorf("failed to read index section: %w", err)
	}
	return ParseIndexRecords(indexData, footer)
}

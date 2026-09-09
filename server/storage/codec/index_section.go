// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package codec

import (
	"fmt"
	"io"
)

// ValidateIndexSection checks that a parsed footer describes a readable index
// section of a segment file that stores its index inline -- the local-file
// backends, disk and stagedstorage, whose data.log is
// header | blocks | index records | footer.
//
// It is NOT valid for footers whose index lives in a separate object. In
// objectstorage, and for stagedstorage's compacted footer, the index records
// start at offset 0 of footer.blk, so those footers carry IndexOffset = 0 as
// their normal value and the readers never consult IndexOffset at all
// (objectstorage.recoverFromFooter and stagedstorage's parseIndexDataUnsafe
// slice the object instead). Routing them through this function would reject
// every object-storage segment as corrupt.
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
		// An inline index always follows the header record, so it can never
		// begin at offset 0. (A footer describing a separate index object may
		// legitimately say 0 -- such footers do not belong here, see above.)
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

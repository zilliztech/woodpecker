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
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// What a survey found at one block. A read path collapses all of these into "stop here"; telling
// them apart is the point of surveying, because they differ in what can be done next.
//
// There is no "undecodable" status because DecodeRecordList does not report corruption that way:
// on a checksum mismatch, a short buffer or an unparseable payload it stops and returns the records
// it already has, without an error. A damaged block header therefore presents as no block header
// record being found at that offset.
const (
	BlockOK                 = "ok"
	BlockHeaderUnreadable   = "header_unreadable"
	BlockHeaderMissing      = "header_missing"
	BlockDataUnreadable     = "data_unreadable"
	BlockChecksumFailed     = "checksum_failed"
	BlockRecordsUndecodable = "records_undecodable"
)

// Why a survey ended.
// blockIsTrailer is internal: the offset holds the index or footer rather than a block, so the walk
// has reached the end of the data region.
const blockIsTrailer = "trailer"

const (
	SurveyStopEnd         = "end_of_segment"
	SurveyStopBound       = "bound"
	SurveyStopChainBroken = "chain_broken"
	SurveyStopCancelled   = "cancelled"
)

// Why a record-by-record walk stopped.
const (
	RecordStopEnd         = "end_of_buffer"
	RecordStopTruncated   = "record_truncated"
	RecordStopChecksum    = "record_checksum_failed"
	RecordStopUnparseable = "record_unparseable"
)

// RecordSurvey is what a record-by-record walk found. DecodeRecordList answers all of these the
// same way -- it stops and returns the records it already has -- which is enough to serve data and
// not enough to say what is wrong or where.
type RecordSurvey struct {
	Records       int    `json:"records"`
	DataRecords   int    `json:"data_records"`
	BytesConsumed int    `json:"bytes_consumed"`
	Stopped       bool   `json:"stopped"`
	StopOffset    int    `json:"stop_offset"`
	StopReason    string `json:"stop_reason"`
	StopDetail    string `json:"stop_detail,omitempty"`
}

// InspectRecords walks a buffer record by record and reports how far it is readable and what ended
// the walk. A block's checksum covers all of it, so one damaged byte condemns the whole block;
// every record carries its own checksum, so the damage can be placed at a record -- the difference
// between losing a block's worth of entries and losing the tail of one.
//
// It does not step over a damaged record: a record's length is part of what the checksum protects,
// so after a failure nothing reliable says where the next record begins.
func InspectRecords(buf []byte) RecordSurvey {
	survey := RecordSurvey{StopReason: RecordStopEnd}
	offset := 0
	for offset < len(buf) {
		if offset+RecordHeaderSize > len(buf) {
			survey.Stopped, survey.StopOffset = true, offset
			survey.StopReason = RecordStopTruncated
			survey.StopDetail = fmt.Sprintf("%d bytes left, a record header needs %d", len(buf)-offset, RecordHeaderSize)
			return survey
		}
		crc := binary.LittleEndian.Uint32(buf[offset : offset+4])
		recordType := buf[offset+4]
		payloadLength := binary.LittleEndian.Uint32(buf[offset+5 : offset+9])
		total := RecordHeaderSize + int(payloadLength)
		if offset+total > len(buf) {
			survey.Stopped, survey.StopOffset = true, offset
			survey.StopReason = RecordStopTruncated
			survey.StopDetail = fmt.Sprintf("record claims %d bytes, %d remain", total, len(buf)-offset)
			return survey
		}
		if crc != crc32.ChecksumIEEE(buf[offset+4:offset+total]) {
			survey.Stopped, survey.StopOffset = true, offset
			survey.StopReason = RecordStopChecksum
			survey.StopDetail = "record checksum mismatch"
			return survey
		}
		if _, err := ParseRecord(recordType, buf[offset+RecordHeaderSize:offset+total]); err != nil {
			survey.Stopped, survey.StopOffset = true, offset
			survey.StopReason = RecordStopUnparseable
			survey.StopDetail = err.Error()
			return survey
		}
		survey.Records++
		if recordType == DataRecordType {
			survey.DataRecords++
		}
		offset += total
		survey.BytesConsumed = offset
	}
	return survey
}

// BlockReport is one block as the survey found it.
type BlockReport struct {
	Number       int64  `json:"block"`
	Offset       int64  `json:"offset"`
	Bytes        int64  `json:"bytes"`
	FirstEntryID int64  `json:"first_entry_id"`
	LastEntryID  int64  `json:"last_entry_id"`
	Status       string `json:"status"`
	Detail       string `json:"detail,omitempty"`
	// RecordsOK and LastGoodEntryID come from walking the block's own records, which survive a
	// block checksum failure that makes a reader abandon the block whole.
	RecordsOK       int   `json:"records_ok"`
	LastGoodEntryID int64 `json:"last_good_entry_id"`
}

// SegmentSurvey is every block the walk reached, and why it ended.
type SegmentSurvey struct {
	Blocks           []BlockReport `json:"blocks"`
	Sealed           bool          `json:"sealed"`
	TotalBlocksKnown int32         `json:"total_blocks_known"`
	// IndexUsable is false when a sealed segment's index could not be read in full. The blocks are
	// then located by walking the chain, which reaches the same blocks as long as their headers are
	// intact -- surveying only the index records that decoded would report a partial walk as a
	// complete one.
	IndexUsable  bool   `json:"index_usable"`
	LAC          int64  `json:"lac"`
	StoppedEarly bool   `json:"stopped_early"`
	StopReason   string `json:"stop_reason"`
}

// InspectBlocks walks a segment's blocks and reports what it finds at each one, continuing past a
// block it could not read. A reader stops at the first failure and returns what it already had,
// which is right for serving data and useless for telling one bad block from a segment that is
// unreadable from there on.
//
// How far it can continue depends on what locates the next block. A sealed segment carries index
// records giving every block's offset independently, so no single damaged block can hide the rest.
// An active segment has only the chain: block N+1 begins where block N's header says it ends. A
// damaged body is stepped over there too, but a header that cannot be read ends the walk -- and
// that is reported as the walk stopping, never as the blocks beyond it being damaged, which the
// survey has not looked at.
func InspectBlocks(ctx context.Context, r io.ReaderAt, size, fromBlock, maxBlocks int64) SegmentSurvey {
	survey := SegmentSurvey{
		Blocks:           make([]BlockReport, 0, 16),
		TotalBlocksKnown: -1,
		LAC:              -1,
		StopReason:       SurveyStopEnd,
	}
	if maxBlocks <= 0 {
		maxBlocks = 1
	}

	if footer, indexes := readFooterAndIndexes(r, size); footer != nil {
		survey.Sealed = true
		survey.TotalBlocksKnown = footer.TotalBlocks
		survey.LAC = footer.LAC
		// A partially decoded index locates only the blocks before its own damage; walking just
		// those and calling it a survey would report blocks nobody looked at as fine.
		survey.IndexUsable = int32(len(indexes)) == footer.TotalBlocks
		if survey.IndexUsable {
			surveyByIndex(ctx, r, size, indexes, fromBlock, maxBlocks, &survey)
			return survey
		}
	}
	surveyByChain(ctx, r, size, fromBlock, maxBlocks, &survey)
	return survey
}

// surveyByIndex walks the blocks a sealed segment's index locates, which is every block regardless
// of what any one of them contains.
func surveyByIndex(ctx context.Context, r io.ReaderAt, size int64, indexes []*IndexRecord, fromBlock, maxBlocks int64, survey *SegmentSurvey) {
	for _, idx := range indexes {
		if int64(idx.BlockNumber) < fromBlock {
			continue
		}
		if int64(len(survey.Blocks)) >= maxBlocks {
			survey.StoppedEarly, survey.StopReason = true, SurveyStopBound
			return
		}
		if ctx.Err() != nil {
			survey.StoppedEarly, survey.StopReason = true, SurveyStopCancelled
			return
		}
		// The file header sits at offset 0 only; a block's index entry already points past it.
		report, _ := inspectOneBlock(r, size, idx.StartOffset, int64(idx.BlockNumber), idx.StartOffset == 0)
		if report.FirstEntryID < 0 {
			// The header could not be read; the index still knows what the block should hold.
			report.FirstEntryID, report.LastEntryID = idx.FirstEntryID, idx.LastEntryID
			report.Bytes = int64(idx.BlockSize)
		}
		survey.Blocks = append(survey.Blocks, report)
	}
}

// surveyByChain walks an active segment, where block N+1 is found only through block N's header.
func surveyByChain(ctx context.Context, r io.ReaderAt, size, fromBlock, maxBlocks int64, survey *SegmentSurvey) {
	offset := int64(0)
	for blockNumber := int64(0); offset < size; blockNumber++ {
		if size-offset < int64(RecordHeaderSize+BlockHeaderRecordSize) {
			// Too few bytes left for a block header: the file ends here. On a segment still being
			// written that is the ordinary state, not damage.
			return
		}
		if ctx.Err() != nil {
			survey.StoppedEarly, survey.StopReason = true, SurveyStopCancelled
			return
		}
		if int64(len(survey.Blocks)) >= maxBlocks {
			survey.StoppedEarly, survey.StopReason = true, SurveyStopBound
			return
		}
		report, next := inspectOneBlock(r, size, offset, blockNumber, offset == 0)
		if report.Status == blockIsTrailer {
			// The index and footer follow the last block. They are not blocks, and reporting them
			// as damaged ones would invent damage.
			return
		}
		if blockNumber >= fromBlock {
			survey.Blocks = append(survey.Blocks, report)
		}
		if next <= offset {
			// Nothing says where the next block begins. The blocks beyond this one have not been
			// looked at, which is not the same as their being damaged.
			survey.StoppedEarly, survey.StopReason = true, SurveyStopChainBroken
			return
		}
		offset = next
	}
}

// inspectOneBlock reads and verifies one block, and returns where the next one begins -- or an
// offset no greater than this one when that cannot be known.
func inspectOneBlock(r io.ReaderAt, size, offset, blockNumber int64, first bool) (BlockReport, int64) {
	report := BlockReport{
		Number: blockNumber, Offset: offset,
		Bytes: -1, FirstEntryID: -1, LastEntryID: -1, LastGoodEntryID: -1,
	}

	headersLen := RecordHeaderSize + BlockHeaderRecordSize
	if first {
		// The file header record precedes the first block's own header.
		headersLen += RecordHeaderSize + HeaderRecordSize
	}
	// Read enough to recognise an index record too: on a segment whose footer could not be read,
	// the walk arrives at the trailer and has to know it for what it is rather than call it a
	// damaged block. An index record is longer than a block header, so the window is the larger of
	// the two, clamped to what is left of the file.
	window := int64(headersLen)
	if trailer := int64(RecordHeaderSize + IndexRecordSize); trailer > window {
		window = trailer
	}
	if offset+window > size {
		window = size - offset
	}
	headers := make([]byte, window)
	if _, err := r.ReadAt(headers, offset); err != nil {
		report.Status, report.Detail = BlockHeaderUnreadable, err.Error()
		return report, offset
	}
	records, _ := DecodeRecordList(headers)
	var header *BlockHeaderRecord
	for _, record := range records {
		if record.Type() == BlockHeaderRecordType {
			header = record.(*BlockHeaderRecord)
			break
		}
	}
	if header == nil {
		for _, record := range records {
			if record.Type() == IndexRecordType || record.Type() == FooterRecordType {
				// The trailer, not a block: the data region ended at this offset.
				report.Status = blockIsTrailer
				return report, offset
			}
		}
		detail := "no block header record at this offset"
		if headerSurvey := InspectRecords(headers); headerSurvey.Stopped {
			detail = fmt.Sprintf("%s: %s at offset %d (%s)", detail,
				headerSurvey.StopReason, offset+int64(headerSurvey.StopOffset), headerSurvey.StopDetail)
		}
		report.Status, report.Detail = BlockHeaderMissing, detail
		return report, offset
	}
	report.FirstEntryID, report.LastEntryID = header.FirstEntryID, header.LastEntryID
	report.Bytes = int64(header.BlockLength)
	// The header is intact, so the next block's position is known even if this block's body is not.
	next := offset + int64(headersLen) + int64(header.BlockLength)

	data := make([]byte, header.BlockLength)
	if _, err := r.ReadAt(data, offset+int64(headersLen)); err != nil {
		report.Status, report.Detail = BlockDataUnreadable, err.Error()
		return report, next
	}
	// Walk the records whatever the block's checksum says. They carry their own checksums, so a
	// damaged block still reports how far into it the data is readable.
	body := InspectRecords(data)
	report.RecordsOK = body.DataRecords
	if body.DataRecords > 0 {
		report.LastGoodEntryID = header.FirstEntryID + int64(body.DataRecords) - 1
	}

	if err := VerifyBlockDataIntegrity(header, data); err != nil {
		report.Status, report.Detail = BlockChecksumFailed, err.Error()
		if body.Stopped {
			report.Detail = fmt.Sprintf("%s; %s at record offset %d (%s)",
				report.Detail, body.StopReason, body.StopOffset, body.StopDetail)
		}
		return report, next
	}
	// The checksum matched, so the bytes are the ones that were written; records that still do not
	// decode mean the block is internally inconsistent rather than damaged in transit.
	if body.Records == 0 && header.BlockLength > 0 {
		report.Status, report.Detail = BlockRecordsUndecodable, "no records decoded from a block whose checksum matched"
		return report, next
	}
	report.Status = BlockOK
	return report, next
}

// readFooterAndIndexes returns a sealed segment's footer and index records, or nil when there is no
// footer to read -- which is how an active segment presents.
func readFooterAndIndexes(r io.ReaderAt, size int64) (*FooterRecord, []*IndexRecord) {
	footerSize := int64(RecordHeaderSize + GetFooterRecordSize(FormatVersion))
	if size < footerSize {
		return nil, nil
	}
	tail := make([]byte, footerSize)
	if _, err := r.ReadAt(tail, size-footerSize); err != nil {
		return nil, nil
	}
	records, err := DecodeRecordList(tail)
	if err != nil || len(records) == 0 {
		return nil, nil
	}
	footer, ok := records[len(records)-1].(*FooterRecord)
	if !ok || footer.TotalBlocks <= 0 {
		return nil, nil
	}

	indexSize := int64(footer.TotalBlocks) * int64(RecordHeaderSize+IndexRecordSize)
	indexStart := size - footerSize - indexSize
	if indexStart < 0 {
		return footer, nil
	}
	indexData := make([]byte, indexSize)
	if _, err := r.ReadAt(indexData, indexStart); err != nil {
		return footer, nil
	}
	indexRecords, err := DecodeRecordList(indexData)
	if err != nil {
		return footer, nil
	}
	indexes := make([]*IndexRecord, 0, len(indexRecords))
	for _, record := range indexRecords {
		if idx, ok := record.(*IndexRecord); ok {
			indexes = append(indexes, idx)
		}
	}
	if len(indexes) == 0 {
		return footer, nil
	}
	return footer, indexes
}

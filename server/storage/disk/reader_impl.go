package disk

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/cockroachdb/errors"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

var (
	SegmentReaderScope = "LocalReader"
)

var _ storage.Reader = (*LocalFileReader)(nil)

// LocalFileReader implements AbstractFileReader for local filesystem storage
type LocalFileReader struct {
	filePath     string
	file         *os.File
	size         int64
	footer       *codec.FooterRecord
	blockIndexes []*codec.IndexRecord
	closed       bool
}

// NewLocalFileReader creates a new local filesystem reader
func NewLocalFileReader(filePath string) (*LocalFileReader, error) {
	// Open file for reading
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("stat file: %w", err)
	}

	reader := &LocalFileReader{
		filePath: filePath,
		file:     file,
		size:     stat.Size(),
		closed:   false,
	}

	// Parse footer and indexes
	if err := reader.parseFooterAndIndexes(); err != nil {
		file.Close()
		return nil, fmt.Errorf("parse footer and indexes: %w", err)
	}

	return reader, nil
}

// parseFooterAndIndexes reads and parses the footer and index records
func (r *LocalFileReader) parseFooterAndIndexes() error {
	if r.size < codec.RecordHeaderSize+codec.FooterRecordSize {
		return errors.New("file too small to contain footer")
	}

	// Read the footer record from the end of the file
	footerRecordSize := int64(codec.RecordHeaderSize + codec.FooterRecordSize)
	footerData, err := r.readAt(r.size-footerRecordSize, int(footerRecordSize))
	if err != nil {
		return fmt.Errorf("read footer data: %w", err)
	}

	// Decode footer record
	footerRecord, err := codec.DecodeRecord(footerData)
	if err != nil {
		return fmt.Errorf("decode footer record: %w", err)
	}

	if footerRecord.Type() != codec.FooterRecordType {
		return fmt.Errorf("expected footer record type %d, got %d", codec.FooterRecordType, footerRecord.Type())
	}

	r.footer = footerRecord.(*codec.FooterRecord)

	// Parse index records - they are located before the footer
	if r.footer.TotalBlocks > 0 {
		if err := r.parseIndexRecords(); err != nil {
			return fmt.Errorf("parse index records: %w", err)
		}
	}

	return nil
}

// parseIndexRecords parses all index records from the file
func (r *LocalFileReader) parseIndexRecords() error {
	// Calculate where index records start
	// Index records are located before the footer record
	footerRecordSize := int64(codec.RecordHeaderSize + codec.FooterRecordSize)
	indexRecordSize := int64(codec.RecordHeaderSize + 36) // IndexRecord payload size: BlockNumber(4) + StartOffset(8) + FirstRecordOffset(8) + FirstEntryID(8) + LastEntryID(8) = 36
	totalIndexSize := int64(r.footer.TotalBlocks) * indexRecordSize

	indexStartOffset := r.size - footerRecordSize - totalIndexSize

	// Read all index data
	indexData, err := r.readAt(indexStartOffset, int(totalIndexSize))
	if err != nil {
		return fmt.Errorf("read index data: %w", err)
	}

	// Parse index records sequentially
	r.blockIndexes = make([]*codec.IndexRecord, 0, r.footer.TotalBlocks)
	offset := 0

	for i := int32(0); i < r.footer.TotalBlocks; i++ {
		if offset+codec.RecordHeaderSize+36 > len(indexData) {
			return fmt.Errorf("incomplete index record at offset %d", offset)
		}

		recordData := indexData[offset : offset+codec.RecordHeaderSize+36]
		record, err := codec.DecodeRecord(recordData)
		if err != nil {
			return fmt.Errorf("decode index record %d: %w", i, err)
		}

		if record.Type() != codec.IndexRecordType {
			return fmt.Errorf("expected index record type %d, got %d at record %d", codec.IndexRecordType, record.Type(), i)
		}

		indexRecord := record.(*codec.IndexRecord)
		r.blockIndexes = append(r.blockIndexes, indexRecord)

		offset += codec.RecordHeaderSize + 36
	}

	// Sort index records by block number to ensure correct order
	sort.Slice(r.blockIndexes, func(i, j int) bool {
		return r.blockIndexes[i].BlockNumber < r.blockIndexes[j].BlockNumber
	})

	return nil
}

// readAt reads data from the file at the specified offset
func (r *LocalFileReader) readAt(offset int64, length int) ([]byte, error) {
	// Seek to offset
	if _, err := r.file.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to offset %d: %w", offset, err)
	}

	// Read data
	data := make([]byte, length)
	n, err := io.ReadFull(r.file, data)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("read data: %w", err)
	}

	return data[:n], nil
}

// GetLastEntryID returns the last entry ID in the file
func (r *LocalFileReader) GetLastEntryID(ctx context.Context) (int64, error) {
	if r.closed {
		return -1, werr.ErrReaderClosed
	}

	if len(r.blockIndexes) == 0 {
		return -1, werr.ErrEntryNotFound
	}

	// Return the last entry ID from the last block
	lastBlock := r.blockIndexes[len(r.blockIndexes)-1]
	return lastBlock.LastEntryID, nil
}

// ReadNextBatch reads the next batch of entries
func (r *LocalFileReader) ReadNextBatch(ctx context.Context, opt storage.ReaderOpt) ([]*proto.LogEntry, error) {
	if r.closed {
		return nil, werr.ErrReaderClosed
	}

	if len(r.blockIndexes) == 0 {
		return nil, werr.ErrEntryNotFound
	}

	// Find the starting block that contains the start sequence number
	startBlockIndex := -1
	for i, block := range r.blockIndexes {
		if block.FirstEntryID <= opt.StartSequenceNum && opt.StartSequenceNum <= block.LastEntryID {
			startBlockIndex = i
			break
		}
	}

	if startBlockIndex == -1 {
		return nil, werr.ErrEntryNotFound
	}

	if opt.BatchSize == -1 {
		// Auto batch mode: return all data records from the single block containing the start sequence number
		return r.readSingleBlock(ctx, r.blockIndexes[startBlockIndex], opt.StartSequenceNum)
	} else {
		// Specified batch size mode: read across multiple blocks if necessary
		return r.readMultipleBlocks(ctx, r.blockIndexes, startBlockIndex, opt.StartSequenceNum, opt.BatchSize)
	}
}

// readSingleBlock reads all data records from a single block starting from the specified entry ID
func (r *LocalFileReader) readSingleBlock(ctx context.Context, blockInfo *codec.IndexRecord, startSequenceNum int64) ([]*proto.LogEntry, error) {
	// Calculate the end offset of this block
	var blockEndOffset int64
	blockIndex := int(blockInfo.BlockNumber)
	if blockIndex+1 < len(r.blockIndexes) {
		// Not the last block, end at the start of the next block
		blockEndOffset = r.blockIndexes[blockIndex+1].StartOffset
	} else {
		// Last block, end at the start of index records
		footerRecordSize := int64(codec.RecordHeaderSize + codec.FooterRecordSize)
		indexRecordSize := int64(codec.RecordHeaderSize + 36) // IndexRecord payload size
		totalIndexSize := int64(r.footer.TotalBlocks) * indexRecordSize
		blockEndOffset = r.size - footerRecordSize - totalIndexSize
	}

	blockSize := int(blockEndOffset - blockInfo.StartOffset)
	blockData, err := r.readAt(blockInfo.StartOffset, blockSize)
	if err != nil {
		return nil, fmt.Errorf("read block data: %w", err)
	}

	// Decode all records in the block
	records, err := codec.DecodeRecordList(blockData)
	if err != nil {
		return nil, fmt.Errorf("decode block records: %w", err)
	}

	// Extract data records and build log entries
	entries := make([]*proto.LogEntry, 0)
	currentEntryID := blockInfo.FirstEntryID

	for _, record := range records {
		if record.Type() == codec.DataRecordType {
			// Only include entries from the start sequence number onwards
			if currentEntryID >= startSequenceNum {
				dataRecord := record.(*codec.DataRecord)
				entry := &proto.LogEntry{
					EntryId: currentEntryID,
					Values:  dataRecord.Payload,
				}
				entries = append(entries, entry)
			}
			currentEntryID++
		}
	}

	return entries, nil
}

// readMultipleBlocks reads across multiple blocks to get the specified number of entries
func (r *LocalFileReader) readMultipleBlocks(ctx context.Context, allBlocks []*codec.IndexRecord, startBlockIndex int, startSequenceNum int64, batchSize int64) ([]*proto.LogEntry, error) {
	entries := make([]*proto.LogEntry, 0, batchSize)
	entriesCollected := int64(0)

	for i := startBlockIndex; i < len(allBlocks) && entriesCollected < batchSize; i++ {
		blockInfo := allBlocks[i]

		// Calculate the end offset of this block
		var blockEndOffset int64
		if i+1 < len(allBlocks) {
			// Not the last block, end at the start of the next block
			blockEndOffset = allBlocks[i+1].StartOffset
		} else {
			// Last block, end at the start of index records
			footerRecordSize := int64(codec.RecordHeaderSize + codec.FooterRecordSize)
			indexRecordSize := int64(codec.RecordHeaderSize + 36) // IndexRecord payload size
			totalIndexSize := int64(r.footer.TotalBlocks) * indexRecordSize
			blockEndOffset = r.size - footerRecordSize - totalIndexSize
		}

		blockSize := int(blockEndOffset - blockInfo.StartOffset)
		blockData, err := r.readAt(blockInfo.StartOffset, blockSize)
		if err != nil {
			return nil, fmt.Errorf("read block %d data: %w", i, err)
		}

		// Decode all records in the block
		records, err := codec.DecodeRecordList(blockData)
		if err != nil {
			return nil, fmt.Errorf("decode block %d records: %w", i, err)
		}

		// Extract data records and build log entries
		currentEntryID := blockInfo.FirstEntryID

		for _, record := range records {
			if record.Type() == codec.DataRecordType && entriesCollected < batchSize {
				// Only include entries from the start sequence number onwards
				if currentEntryID >= startSequenceNum {
					dataRecord := record.(*codec.DataRecord)
					entry := &proto.LogEntry{
						EntryId: currentEntryID,
						Values:  dataRecord.Payload,
					}
					entries = append(entries, entry)
					entriesCollected++
				}
				currentEntryID++
			}
		}
	}

	return entries, nil
}

// GetBlockIndexes returns all block indexes
func (r *LocalFileReader) GetBlockIndexes() []*codec.IndexRecord {
	return r.blockIndexes
}

// GetFooter returns the footer record
func (r *LocalFileReader) GetFooter() *codec.FooterRecord {
	return r.footer
}

// GetTotalRecords returns the total number of records
func (r *LocalFileReader) GetTotalRecords() uint32 {
	if r.footer != nil {
		return r.footer.TotalRecords
	}
	return 0
}

// GetTotalBlocks returns the total number of blocks
func (r *LocalFileReader) GetTotalBlocks() int32 {
	if r.footer != nil {
		return r.footer.TotalBlocks
	}
	return 0
}

// Close closes the reader
func (r *LocalFileReader) Close(ctx context.Context) error {
	if r.closed {
		return nil
	}

	if r.file != nil {
		if err := r.file.Close(); err != nil {
			return fmt.Errorf("close file: %w", err)
		}
		r.file = nil
	}

	r.closed = true
	return nil
}

func SearchBlock(list []*codec.IndexRecord, entryId int64) (*codec.IndexRecord, error) {
	low, high := 0, len(list)-1
	var candidate *codec.IndexRecord

	for low <= high {
		mid := (low + high) / 2
		block := list[mid]

		firstEntryID := block.FirstEntryID
		if firstEntryID > entryId {
			high = mid - 1
		} else {
			lastEntryID := block.LastEntryID
			if lastEntryID >= entryId {
				candidate = block
				return candidate, nil
			} else {
				low = mid + 1
			}
		}
	}
	return candidate, nil
}

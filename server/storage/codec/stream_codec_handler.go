package codec

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/zilliztech/woodpecker/common/werr"
)

// StreamDecodeReader is for disk fragment scenario - sequential reading from append-only files
// Suitable for disk fragments that use append-only file operations
type StreamDecodeReader struct {
	reader io.ReadSeeker
	header *HeaderRecord
	footer *FooterRecord
	index  *IndexRecord
}

// NewStreamDecodeReader creates a new stream reader for disk fragment scenario
func NewStreamDecodeReader(reader io.ReadSeeker) (*StreamDecodeReader, error) {
	sr := &StreamDecodeReader{
		reader: reader,
	}

	// Try to read complete file structure (header + data + index + footer)
	// This supports both complete files and incomplete (growing) files
	if err := sr.initialize(); err != nil {
		return nil, err
	}

	return sr, nil
}

// initialize attempts to read the file structure
func (sr *StreamDecodeReader) initialize() error {
	// Read header first
	header, err := sr.readHeader()
	if err != nil {
		return werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to read header: " + err.Error())
	}
	sr.header = header

	// Try to read footer from the end (for complete files)
	footer, err := sr.tryReadFooter()
	if err == nil {
		sr.footer = footer
		// If footer exists, read index
		index, err := sr.readIndexFromFooter()
		if err == nil {
			sr.index = index
		}
	}
	// If footer doesn't exist, it's an incomplete/growing file - that's okay

	return nil
}

// GetHeader returns the file header
func (sr *StreamDecodeReader) GetHeader() *HeaderRecord {
	return sr.header
}

// GetFooter returns the file footer (may be nil for incomplete files)
func (sr *StreamDecodeReader) GetFooter() *FooterRecord {
	return sr.footer
}

// GetIndex returns the file index (may be nil for incomplete files)
func (sr *StreamDecodeReader) GetIndex() *IndexRecord {
	return sr.index
}

// IsComplete returns whether the file has a complete structure (footer + index)
func (sr *StreamDecodeReader) IsComplete() bool {
	return sr.footer != nil && sr.index != nil
}

// ReadNext reads the next record from the current position
// Useful for sequential reading of incomplete files
func (sr *StreamDecodeReader) ReadNext() (Record, error) {
	// Read record header: CRC(4) + Type(1) + Length(4)
	recordHeader := make([]byte, 9)
	n, err := io.ReadFull(sr.reader, recordHeader)
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to read record header: " + err.Error())
	}
	if n != 9 {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("incomplete record header")
	}

	// Extract length
	length := binary.LittleEndian.Uint32(recordHeader[5:9])

	// Read payload
	payload := make([]byte, length)
	if length > 0 {
		n, err := io.ReadFull(sr.reader, payload)
		if err != nil {
			return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to read record payload: " + err.Error())
		}
		if n != int(length) {
			return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("incomplete record payload")
		}
	}

	// Combine header and payload for decoding
	fullRecord := append(recordHeader, payload...)
	return DecodeRecord(fullRecord)
}

// GetRecordByEntryID retrieves a specific record by its entry ID using the index
// Only works for complete files that have an index
func (sr *StreamDecodeReader) GetRecordByEntryID(entryID int64) (Record, error) {
	if sr.index == nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("no index available - file may be incomplete")
	}

	if sr.header == nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("no header available")
	}

	// Calculate offset within the index using header's FirstEntryID
	if entryID < sr.header.FirstEntryID {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("entry ID too small")
	}

	offsetIndex := entryID - sr.header.FirstEntryID
	if offsetIndex >= int64(len(sr.index.Offsets)) {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("entry ID too large")
	}

	// Get the physical offset
	physicalOffset := sr.index.Offsets[offsetIndex]

	return sr.readRecordAtOffset(uint64(physicalOffset))
}

// GetAllDataRecords retrieves all data records
// For complete files, uses index for efficient access
// For incomplete files, reads sequentially
func (sr *StreamDecodeReader) GetAllDataRecords() ([]*DataRecord, error) {
	if sr.IsComplete() {
		return sr.getAllDataRecordsFromIndex()
	}
	return sr.getAllDataRecordsSequentially()
}

// ReadAllRecords reads all records from the current position
// Useful for processing incomplete files
func (sr *StreamDecodeReader) ReadAllRecords() ([]Record, error) {
	var records []Record

	for {
		record, err := sr.ReadNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			return records, err
		}
		records = append(records, record)
	}

	return records, nil
}

// IterateRecords calls the provided function for each record
func (sr *StreamDecodeReader) IterateRecords(fn func(Record) error) error {
	// Reset to beginning to iterate from the start
	_, err := sr.reader.Seek(0, io.SeekStart)
	if err != nil {
		return werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to seek to beginning: " + err.Error())
	}

	for {
		record, err := sr.ReadNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := fn(record); err != nil {
			return err
		}
	}
	return nil
}

// readHeader reads and validates the header record
func (sr *StreamDecodeReader) readHeader() (*HeaderRecord, error) {
	// Reset to beginning
	_, err := sr.reader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to seek to beginning: " + err.Error())
	}

	// Read first record which should be header
	record, err := sr.ReadNext()
	if err != nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to read first record: " + err.Error())
	}

	header, ok := record.(*HeaderRecord)
	if !ok {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("first record is not a header")
	}

	return header, nil
}

// tryReadFooter attempts to read footer from the end
func (sr *StreamDecodeReader) tryReadFooter() (*FooterRecord, error) {
	// Get file size
	fileSize, err := sr.reader.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	if fileSize < 41 { // Minimum: CRC(4) + Type(1) + Length(4) + FooterPayload(32)
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("file too short for footer")
	}

	// Try to read footer from the end
	footerSize := int64(41) // CRC(4) + Type(1) + Length(4) + FooterPayload(32)
	_, err = sr.reader.Seek(-footerSize, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	footerData := make([]byte, footerSize)
	n, err := io.ReadFull(sr.reader, footerData)
	if err != nil || n != int(footerSize) {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to read footer data")
	}

	record, err := DecodeRecord(footerData)
	if err != nil {
		return nil, err
	}

	footer, ok := record.(*FooterRecord)
	if !ok {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("last record is not a footer")
	}

	return footer, nil
}

// readIndexFromFooter reads the index using footer information
func (sr *StreamDecodeReader) readIndexFromFooter() (*IndexRecord, error) {
	if sr.footer == nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("footer not available")
	}

	record, err := sr.readRecordAtOffset(sr.footer.IndexOffset)
	if err != nil {
		return nil, err
	}

	index, ok := record.(*IndexRecord)
	if !ok {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("record at index offset is not an index record")
	}

	return index, nil
}

// readRecordAtOffset reads a record at the specified offset
func (sr *StreamDecodeReader) readRecordAtOffset(offset uint64) (Record, error) {
	// Seek to offset
	_, err := sr.reader.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to seek to offset: " + err.Error())
	}

	// Read record
	return sr.ReadNext()
}

// getAllDataRecordsFromIndex gets all data records using index
func (sr *StreamDecodeReader) getAllDataRecordsFromIndex() ([]*DataRecord, error) {
	var dataRecords []*DataRecord

	if sr.header == nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("no header available")
	}

	firstEntryID := sr.header.FirstEntryID
	for i := range sr.index.Offsets {
		entryID := firstEntryID + int64(i)
		record, err := sr.GetRecordByEntryID(entryID)
		if err != nil {
			return dataRecords, werr.ErrInvalidRecordSize.WithCauseErrMsg(fmt.Sprintf("failed to get record at entry ID %d: %s", entryID, err.Error()))
		}

		// Only collect data records
		if dataRecord, ok := record.(*DataRecord); ok {
			dataRecords = append(dataRecords, dataRecord)
		}
	}

	return dataRecords, nil
}

// getAllDataRecordsSequentially reads all data records sequentially
func (sr *StreamDecodeReader) getAllDataRecordsSequentially() ([]*DataRecord, error) {
	var dataRecords []*DataRecord

	// Reset to after header
	_, err := sr.reader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Skip header
	_, err = sr.ReadNext()
	if err != nil {
		return nil, err
	}

	// Read all remaining records
	for {
		record, err := sr.ReadNext()
		if err == io.EOF {
			break
		}
		if err != nil {
			return dataRecords, err
		}

		// Only collect data records, skip index and footer
		if dataRecord, ok := record.(*DataRecord); ok {
			dataRecords = append(dataRecords, dataRecord)
		}
	}

	return dataRecords, nil
}

// GetFileInfo returns summary information about the file
func (sr *StreamDecodeReader) GetFileInfo() *FileInfo {
	info := &FileInfo{
		HasFooter:  sr.footer != nil,
		IsComplete: sr.IsComplete(),
	}

	if sr.header != nil {
		info.Version = sr.header.Version
		info.Flags = sr.header.Flags
	}

	if sr.footer != nil {
		info.FirstEntryID = sr.footer.FirstEntryID
		info.RecordCount = sr.footer.Count
		info.IndexOffset = sr.footer.IndexOffset
		info.IndexLength = sr.footer.IndexLength
	} else if sr.header != nil {
		info.FirstEntryID = sr.header.FirstEntryID
		if sr.index != nil {
			info.RecordCount = uint32(len(sr.index.Offsets))
		}
	}

	return info
}

// StreamEncodeWriter is for disk fragment scenario - supports incremental writing
// Suitable for disk fragment writer that uses mmap and writes data incrementally
type StreamEncodeWriter struct {
	writer       io.WriteSeeker
	header       *HeaderRecord
	dataRecords  []*DataRecord
	offsets      []uint32
	firstEntryID int64
	nextEntryID  int64
	isFinalized  bool
	dataWritten  bool // Track if any data has been written
}

// NewStreamEncodeWriter creates a new stream writer for disk fragment scenario
func NewStreamEncodeWriter(writer io.WriteSeeker, firstEntryID int64, version uint16, flags uint16) (*StreamEncodeWriter, error) {
	sw := &StreamEncodeWriter{
		writer:       writer,
		firstEntryID: firstEntryID,
		nextEntryID:  firstEntryID,
		dataRecords:  make([]*DataRecord, 0),
		offsets:      make([]uint32, 0),
		dataWritten:  false,
	}

	// Create and write header
	header := &HeaderRecord{
		Version:      version,
		Flags:        flags,
		FirstEntryID: firstEntryID,
	}
	sw.header = header

	// Write header to file
	headerData, err := EncodeRecord(header)
	if err != nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to encode header: " + err.Error())
	}

	_, err = sw.writer.Write(headerData)
	if err != nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to write header: " + err.Error())
	}

	return sw, nil
}

// WriteData writes a data record to the stream
// Returns the assigned entry ID
func (sw *StreamEncodeWriter) WriteData(payload []byte) (int64, error) {
	if sw.isFinalized {
		return -1, werr.ErrInvalidRecordSize.WithCauseErrMsg("writer is already finalized")
	}

	// Get current position for offset tracking
	currentPos, err := sw.writer.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to get current position: " + err.Error())
	}

	// Create data record
	dataRecord := &DataRecord{Payload: payload}
	dataBytes, err := EncodeRecord(dataRecord)
	if err != nil {
		return -1, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to encode data record: " + err.Error())
	}

	// Write data record
	_, err = sw.writer.Write(dataBytes)
	if err != nil {
		return -1, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to write data record: " + err.Error())
	}

	// Track record and offset
	sw.dataRecords = append(sw.dataRecords, dataRecord)
	sw.offsets = append(sw.offsets, uint32(currentPos))
	sw.dataWritten = true

	entryID := sw.nextEntryID
	sw.nextEntryID++

	return entryID, nil
}

// Finalize completes the file by writing index and footer
// This is called when the fragment is complete and no more data will be written
func (sw *StreamEncodeWriter) Finalize() error {
	if sw.isFinalized {
		return werr.ErrInvalidRecordSize.WithCauseErrMsg("writer is already finalized")
	}

	// Get current position for index offset
	indexOffset, err := sw.writer.Seek(0, io.SeekCurrent)
	if err != nil {
		return werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to get index offset: " + err.Error())
	}

	// Create and write index record
	index := &IndexRecord{
		Offsets: sw.offsets,
	}

	indexBytes, err := EncodeRecord(index)
	if err != nil {
		return werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to encode index: " + err.Error())
	}

	_, err = sw.writer.Write(indexBytes)
	if err != nil {
		return werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to write index: " + err.Error())
	}

	// Create and write footer record
	footer := &FooterRecord{
		IndexOffset:  uint64(indexOffset),
		IndexLength:  uint32(len(indexBytes)),
		FirstEntryID: sw.firstEntryID,
		Count:        uint32(len(sw.dataRecords)),
		Version:      sw.header.Version,
		Flags:        sw.header.Flags,
	}

	footerBytes, err := EncodeRecord(footer)
	if err != nil {
		return werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to encode footer: " + err.Error())
	}

	_, err = sw.writer.Write(footerBytes)
	if err != nil {
		return werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to write footer: " + err.Error())
	}

	sw.isFinalized = true
	return nil
}

// GetHeader returns the header record
func (sw *StreamEncodeWriter) GetHeader() *HeaderRecord {
	return sw.header
}

// GetDataRecordCount returns the number of data records written
func (sw *StreamEncodeWriter) GetDataRecordCount() int {
	return len(sw.dataRecords)
}

// GetFirstEntryID returns the first entry ID
func (sw *StreamEncodeWriter) GetFirstEntryID() int64 {
	return sw.firstEntryID
}

// GetNextEntryID returns the next entry ID to be assigned
func (sw *StreamEncodeWriter) GetNextEntryID() int64 {
	return sw.nextEntryID
}

// GetLastEntryID returns the last assigned entry ID, -1 if no data written
func (sw *StreamEncodeWriter) GetLastEntryID() int64 {
	if !sw.dataWritten {
		return -1
	}
	return sw.nextEntryID - 1
}

// IsFinalized returns whether the writer has been finalized
func (sw *StreamEncodeWriter) IsFinalized() bool {
	return sw.isFinalized
}

// HasData returns whether any data has been written
func (sw *StreamEncodeWriter) HasData() bool {
	return sw.dataWritten
}

// FileInfo provides summary information about the file
type FileInfo struct {
	Version      uint16
	Flags        uint16
	FirstEntryID int64
	RecordCount  uint32
	IndexOffset  uint64
	IndexLength  uint32
	HasFooter    bool
	IsComplete   bool
}

package codec

import (
	"encoding/binary"
	"fmt"

	"github.com/zilliztech/woodpecker/common/werr"
)

// BatchCodecHandler is for object storage scenario - handles complete file data in memory
// Suitable for object fragments that are created/downloaded as complete objects
// Supports both reading from existing data and building new data
type BatchCodecHandler struct {
	// For reading existing data
	data   []byte
	footer *FooterRecord
	index  *IndexRecord
	header *HeaderRecord

	// For building new data
	dataRecords  []*DataRecord
	firstEntryID int64
	nextEntryID  int64
	version      uint16
	flags        uint16
}

// NewBatchCodecHandlerFromData creates a new batch handler from existing data (read mode)
func NewBatchCodecHandlerFromData(data []byte) (*BatchCodecHandler, error) {
	bh := &BatchCodecHandler{
		data: data,
	}

	// Read footer from the end
	footer, err := bh.readFooter()
	if err != nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to read footer: " + err.Error())
	}
	bh.footer = footer

	// Read index using footer information
	index, err := bh.readIndex()
	if err != nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to read index: " + err.Error())
	}
	bh.index = index

	// Read header for completeness
	header, err := bh.readHeader()
	if err != nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to read header: " + err.Error())
	}
	bh.header = header

	return bh, nil
}

// NewBatchCodecHandlerForWriting creates a new batch handler for building data (write mode)
func NewBatchCodecHandlerForWriting(firstEntryID int64, version uint16, flags uint16) *BatchCodecHandler {
	return &BatchCodecHandler{
		header: &HeaderRecord{
			Version:      version,
			Flags:        flags,
			FirstEntryID: firstEntryID,
		},
		dataRecords:  make([]*DataRecord, 0),
		firstEntryID: firstEntryID,
		nextEntryID:  firstEntryID,
		version:      version,
		flags:        flags,
	}
}

// ========== Read Methods ==========

// GetFooter returns the file footer (only available in read mode)
func (bh *BatchCodecHandler) GetFooter() *FooterRecord {
	return bh.footer
}

// GetIndex returns the file index (only available in read mode)
func (bh *BatchCodecHandler) GetIndex() *IndexRecord {
	return bh.index
}

// GetHeader returns the file header
func (bh *BatchCodecHandler) GetHeader() *HeaderRecord {
	return bh.header
}

// GetRecordByEntryID retrieves a specific record by its entry ID using the index
// Only works when initialized from existing data
func (bh *BatchCodecHandler) GetRecordByEntryID(entryID int64) (Record, error) {
	if bh.index == nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("no index available - not initialized from data")
	}

	if bh.header == nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("no header available")
	}

	// Calculate offset within the index using header's FirstEntryID
	if entryID < bh.header.FirstEntryID {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("entry ID too small")
	}

	offsetIndex := entryID - bh.header.FirstEntryID
	if offsetIndex >= int64(len(bh.index.Offsets)) {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("entry ID too large")
	}

	// Get the physical offset
	physicalOffset := bh.index.Offsets[offsetIndex]

	return bh.readRecordAtOffset(uint64(physicalOffset))
}

// GetRecordsByRange retrieves multiple records by entry ID range
func (bh *BatchCodecHandler) GetRecordsByRange(startEntryID, endEntryID int64) ([]Record, error) {
	var records []Record

	for entryID := startEntryID; entryID <= endEntryID; entryID++ {
		record, err := bh.GetRecordByEntryID(entryID)
		if err != nil {
			return records, werr.ErrInvalidRecordSize.WithCauseErrMsg(fmt.Sprintf("failed to get record at entry ID %d: %s", entryID, err.Error()))
		}
		records = append(records, record)
	}

	return records, nil
}

// GetAllDataRecords retrieves all data records using the index
func (bh *BatchCodecHandler) GetAllDataRecords() ([]*DataRecord, error) {
	// If in write mode, return the data records being built
	if bh.data == nil {
		result := make([]*DataRecord, len(bh.dataRecords))
		copy(result, bh.dataRecords)
		return result, nil
	}

	// If in read mode, use index to get all data records
	var dataRecords []*DataRecord

	if bh.header == nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("no header available")
	}

	firstEntryID := bh.header.FirstEntryID
	for i := range bh.index.Offsets {
		entryID := firstEntryID + int64(i)
		record, err := bh.GetRecordByEntryID(entryID)
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

// ========== Write Methods ==========

// AddData adds a data record to the batch
// Returns the assigned entry ID
func (bh *BatchCodecHandler) AddData(payload []byte) int64 {
	if bh.data != nil {
		// This handler was initialized from existing data, cannot add more data
		return -1
	}

	dataRecord := &DataRecord{Payload: payload}
	bh.dataRecords = append(bh.dataRecords, dataRecord)

	entryID := bh.nextEntryID
	bh.nextEntryID++

	return entryID
}

// ToBytes serializes the complete object to bytes
// This creates a complete file format that can be used by other BatchCodecHandlers
func (bh *BatchCodecHandler) ToBytes() ([]byte, error) {
	if bh.data != nil {
		// If initialized from data, return the original data
		return bh.data, nil
	}

	// Build the complete file format
	var result []byte

	// Write header
	headerBytes, err := EncodeRecord(bh.header)
	if err != nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to encode header: " + err.Error())
	}
	result = append(result, headerBytes...)

	// Write data records and track offsets
	var offsets []uint32
	for _, dataRecord := range bh.dataRecords {
		offsets = append(offsets, uint32(len(result)))

		dataBytes, err := EncodeRecord(dataRecord)
		if err != nil {
			return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to encode data record: " + err.Error())
		}
		result = append(result, dataBytes...)
	}

	// Write index record
	indexOffset := uint32(len(result))
	index := &IndexRecord{
		Offsets: offsets,
	}

	indexBytes, err := EncodeRecord(index)
	if err != nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to encode index: " + err.Error())
	}
	result = append(result, indexBytes...)

	// Write footer record
	footer := &FooterRecord{
		IndexOffset:  uint64(indexOffset),
		IndexLength:  uint32(len(indexBytes)),
		FirstEntryID: bh.firstEntryID,
		Count:        uint32(len(bh.dataRecords)),
		Version:      bh.header.Version,
		Flags:        bh.header.Flags,
	}

	footerBytes, err := EncodeRecord(footer)
	if err != nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to encode footer: " + err.Error())
	}
	result = append(result, footerBytes...)

	return result, nil
}

// ========== Common Methods ==========

// GetDataRecordCount returns the number of data records
func (bh *BatchCodecHandler) GetDataRecordCount() int {
	if bh.data != nil {
		// Read mode: use footer count
		if bh.footer != nil {
			return int(bh.footer.Count)
		}
		return 0
	}
	// Write mode: use dataRecords length
	return len(bh.dataRecords)
}

// GetFirstEntryID returns the first entry ID
func (bh *BatchCodecHandler) GetFirstEntryID() int64 {
	if bh.header != nil {
		return bh.header.FirstEntryID
	}
	return bh.firstEntryID
}

// GetNextEntryID returns the next entry ID to be assigned (write mode only)
func (bh *BatchCodecHandler) GetNextEntryID() int64 {
	return bh.nextEntryID
}

// GetLastEntryID returns the last assigned entry ID, -1 if no data
func (bh *BatchCodecHandler) GetLastEntryID() int64 {
	if bh.data != nil {
		// Read mode
		if bh.footer != nil && bh.footer.Count > 0 {
			return bh.footer.FirstEntryID + int64(bh.footer.Count) - 1
		}
		return -1
	}
	// Write mode
	if len(bh.dataRecords) == 0 {
		return -1
	}
	return bh.nextEntryID - 1
}

// HasData returns whether any data is available
func (bh *BatchCodecHandler) HasData() bool {
	if bh.data != nil {
		// Read mode: check footer count
		return bh.footer != nil && bh.footer.Count > 0
	}
	// Write mode: check dataRecords
	return len(bh.dataRecords) > 0
}

// IsReadMode returns whether this handler is in read mode (initialized from data)
func (bh *BatchCodecHandler) IsReadMode() bool {
	return bh.data != nil
}

// IsWriteMode returns whether this handler is in write mode (building data)
func (bh *BatchCodecHandler) IsWriteMode() bool {
	return bh.data == nil
}

// GetFileInfo returns summary information about the file
func (bh *BatchCodecHandler) GetFileInfo() *FileInfo {
	info := &FileInfo{
		HasFooter:  bh.footer != nil,
		IsComplete: true, // BatchCodecHandler always works with complete files
	}

	if bh.header != nil {
		info.Version = bh.header.Version
		info.Flags = bh.header.Flags
		info.FirstEntryID = bh.header.FirstEntryID
	}

	if bh.footer != nil {
		info.RecordCount = bh.footer.Count
		info.IndexOffset = bh.footer.IndexOffset
		info.IndexLength = bh.footer.IndexLength
	} else {
		// Write mode
		info.RecordCount = uint32(len(bh.dataRecords))
	}

	return info
}

// ========== Private Methods ==========

// readFooter reads the footer from the end of the data
func (bh *BatchCodecHandler) readFooter() (*FooterRecord, error) {
	if len(bh.data) < 41 { // Minimum: CRC(4) + Type(1) + Length(4) + FooterPayload(32)
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("data too short for footer")
	}

	// Footer is the last record
	footerSize := 41 // CRC(4) + Type(1) + Length(4) + FooterPayload(32)
	footerData := bh.data[len(bh.data)-footerSize:]

	record, err := DecodeRecord(footerData)
	if err != nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to decode footer record: " + err.Error())
	}

	footer, ok := record.(*FooterRecord)
	if !ok {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("last record is not a footer")
	}

	return footer, nil
}

// readIndex reads the index record using footer information
func (bh *BatchCodecHandler) readIndex() (*IndexRecord, error) {
	if bh.footer == nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("footer not available")
	}

	record, err := bh.readRecordAtOffset(bh.footer.IndexOffset)
	if err != nil {
		return nil, err
	}

	index, ok := record.(*IndexRecord)
	if !ok {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("record at index offset is not an index record")
	}

	return index, nil
}

// readHeader reads the header record from the beginning
func (bh *BatchCodecHandler) readHeader() (*HeaderRecord, error) {
	record, err := bh.readRecordAtOffset(0)
	if err != nil {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("failed to read header: " + err.Error())
	}

	header, ok := record.(*HeaderRecord)
	if !ok {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("first record is not a header")
	}

	return header, nil
}

// readRecordAtOffset reads a record at the specified offset
func (bh *BatchCodecHandler) readRecordAtOffset(offset uint64) (Record, error) {
	if offset >= uint64(len(bh.data)) {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("offset beyond data length")
	}

	// Read record header first to get the length
	if offset+9 > uint64(len(bh.data)) {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("insufficient data for record header")
	}

	recordHeader := bh.data[offset : offset+9]
	length := binary.LittleEndian.Uint32(recordHeader[5:9])

	// Read the complete record
	totalLength := 9 + uint64(length)
	if offset+totalLength > uint64(len(bh.data)) {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("insufficient data for complete record")
	}

	recordData := bh.data[offset : offset+totalLength]
	return DecodeRecord(recordData)
}

package disk

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/edsrzf/mmap-go"

	"github.com/zilliztech/woodpecker/server/storage"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

var _ storage.Fragment = (*FragmentFile)(nil)

// FragmentFile manages file-based fragments.
type FragmentFile struct {
	mu       sync.RWMutex
	filePath string
	basePath string
	txnFile  *os.File
	mmap     mmap.MMap // Memory-mapped file
	entries  []storage.LogEntry
	footer   storage.Footer

	fileLastOffset  uint32 // Last offset written to the fragment file
	fileStartOffset uint64 // Start offset of the file, used to calculate the offset of each entry in fragment
	lastSequenceNum uint32 // Last entry sequence number, starts from 0 that corresponds to the first index item
	fragmentSize    int    // total size of the fragment file
}

func (ff *FragmentFile) GetFragmentKey() string {
	//TODO implement me
	panic("implement me")
}

func (ff *FragmentFile) GetFragmentId() int64 {
	//TODO implement me
	panic("implement me")
}

func (ff *FragmentFile) GetSize() int64 {
	//TODO implement me
	panic("implement me")
}

// NewFileFragment initializes a new FragmentFile.
func NewFileFragment(basePath string, fileStartOffset uint64, fragmentSize int) (*FragmentFile, error) {
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		if err = os.MkdirAll(basePath, 0755); err != nil {
			return nil, errors.Wrapf(err, "failed to create wal directory %s", basePath)
		}
	}

	filePath := fmt.Sprintf("%s/fragment_%d", basePath, fileStartOffset)
	ff := &FragmentFile{
		filePath:        filePath,
		basePath:        basePath,
		fileStartOffset: fileStartOffset,
		fragmentSize:    fragmentSize,
	}

	// check if the fragment file exists
	var err error
	if _, err = os.Stat(ff.filePath); err == nil {
		if err = ff.openAlreadyExistsFragment(); err != nil {
			return nil, err
		}
	} else {
		// create a new fragment file
		if err = ff.openNewFragment(); err != nil {
			return nil, err
		}
	}

	if ff.mmap, err = mmap.MapRegion(ff.txnFile, ff.fragmentSize, mmap.RDWR, 0, 0); err != nil {
		return nil, errors.Wrapf(err, "failed to map fragment file %s", ff.txnFile)
	}
	return ff, nil
}

// openAlreadyExistsFragment opens an existing file and recovery the data.
func (ff *FragmentFile) openAlreadyExistsFragment() error {
	return nil
}

// Create a new file if it doesn't exist
func (ff *FragmentFile) openNewFragment() error {
	var err error
	if ff.txnFile, err = os.OpenFile(ff.filePath, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return errors.Wrapf(err, "failed to open fragement file %s", ff.filePath)
	}

	if _, err := ff.txnFile.Seek(int64(ff.fragmentSize), 0); err != nil {
		return err
	}

	// Write the Header (magic string and version)
	if _, err := ff.txnFile.Write([]byte{0}); err != nil {
		return err
	}

	_, err = ff.txnFile.WriteAt([]byte(codec.MagicString), 0)
	if err != nil {
		return err
	}
	err = binary.Write(ff.txnFile, binary.LittleEndian, codec.VERSION)
	ff.fileLastOffset = uint32(codec.FileHeaderSize)
	return ff.txnFile.Sync()
}

// NewReader reads data from a file.
func (ff *FragmentFile) Read(ctx context.Context, opt storage.ReaderOpt) ([]*storage.LogEntry, error) {
	ff.mu.RLock()
	defer ff.mu.RUnlock()

	// Validate the sequence range
	if opt.StartSequenceNum > opt.EndSequenceNum {
		return nil, fmt.Errorf("invalid sequence range: start %d, end %d", opt.StartSequenceNum, opt.EndSequenceNum)
	}

	// Ensure the range is within the available sequences
	// If the end sequence is greater than the last sequence, adjust the end sequence
	if opt.EndSequenceNum >= int64(ff.lastSequenceNum)+int64(ff.fileStartOffset) {
		opt.EndSequenceNum = int64(ff.lastSequenceNum) + int64(ff.fileStartOffset) - 1
	}

	// Ensure the start sequence is within the available sequences
	if opt.StartSequenceNum >= int64(ff.lastSequenceNum)+int64(ff.fileStartOffset) {
		return nil, fmt.Errorf("start sequence %d exceeds the available range", opt.StartSequenceNum)
	}

	// Determine the starting and ending index
	startIndex := opt.StartSequenceNum - int64(ff.fileStartOffset)
	endIndex := opt.EndSequenceNum - int64(ff.fileStartOffset) + 1

	// Collect the entries within the range
	entries := make([]*storage.LogEntry, 0, endIndex-startIndex)
	for i := startIndex; i < endIndex; i++ {
		if i >= int64(len(ff.footer.EntryOffset)) {
			panic("index out of range, should not happen")
		}

		offset := ff.footer.EntryOffset[i]

		// Read payload size
		if offset+codec.PayloadSize > uint32(len(ff.mmap)) {
			return nil, fmt.Errorf("corrupted data: offset %d exceeds file size", offset)
		}
		payloadSize := binary.LittleEndian.Uint32(ff.mmap[offset : offset+codec.PayloadSize])

		if offset+payloadSize+codec.EntryHeaderSize > uint32(len(ff.mmap)) {
			return nil, fmt.Errorf("corrupted data: offset %d exceeds file size", offset)
		}

		// Read CRC (stored 4 bytes after the payload size)
		crc := binary.LittleEndian.Uint32(ff.mmap[offset+codec.PayloadSize : offset+codec.EntryHeaderSize])

		// Read payload
		payload := ff.mmap[offset+codec.EntryHeaderSize : offset+codec.EntryHeaderSize+payloadSize]

		// Verify CRC
		if crc != crc32.ChecksumIEEE(payload) {
			return nil, fmt.Errorf("CRC mismatch at sequence %d", int64(ff.fileStartOffset)+i)
		}

		// Create the log entry
		entry := &storage.LogEntry{
			Payload:     payload,
			SequenceNum: ff.fileStartOffset + uint64(i),
			CRC:         crc,
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// Write writes data to a file.
func (ff *FragmentFile) Write(ctx context.Context, data []byte) error {
	ff.mu.Lock()
	defer ff.mu.Unlock()

	// Calculate the entry size
	payloadSize := uint32(len(data))
	crc := crc32.ChecksumIEEE(data)
	entrySize := 4 + 4 + len(data)

	// Check if there is enough space
	if len(ff.mmap)-int(ff.fileLastOffset) < entrySize {
		return errors.New("not enough space in memory-mapped file")
	}

	// Write entry to mmap
	// Construct the entry: [payloadSize (4 bytes)] + [CRC (4 bytes)] + [Payload (variable)]
	binary.LittleEndian.PutUint32(ff.mmap[ff.fileLastOffset:], payloadSize)
	ff.fileLastOffset += 4
	binary.LittleEndian.PutUint32(ff.mmap[ff.fileLastOffset:], crc)
	ff.fileLastOffset += 4
	copy(ff.mmap[ff.fileLastOffset:], data)
	ff.fileLastOffset += uint32(len(data))

	// Update footer
	ff.footer.EntryOffset = append(ff.footer.EntryOffset, ff.fileLastOffset)
	ff.footer.IndexSize += codec.IndexItemSize
	ff.lastSequenceNum++
	return nil
}

// Flush writes the footer and ensures all data is persisted to the file.
func (ff *FragmentFile) Flush(ctx context.Context) error {
	ff.mu.Lock()
	defer ff.mu.Unlock()

	// Sync changes to disk
	if err := ff.mmap.Flush(); err != nil {
		return err
	}
	return nil
}

func (ff *FragmentFile) writeFooter() {
	// Write the footer at the end of the file
	for _, entryOffset := range ff.footer.EntryOffset {
		binary.LittleEndian.PutUint32(ff.mmap[ff.fileLastOffset:], entryOffset)
		ff.fileLastOffset += 4
	}
	binary.LittleEndian.PutUint32(ff.mmap[ff.fileLastOffset:], ff.footer.CRC)
	ff.fileLastOffset += 4
	binary.LittleEndian.PutUint32(ff.mmap[ff.fileLastOffset:], ff.footer.IndexSize)
	ff.fileLastOffset += 4
}

func (ff *FragmentFile) Load(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (ff *FragmentFile) GetLastEntryId() (int64, error) {
	//TODO implement me
	panic("implement me")
}

func (ff *FragmentFile) GetFirstEntryIdDirectly() int64 {
	//TODO implement me
	panic("implement me")
}

func (ff *FragmentFile) GetLastEntryIdDirectly() int64 {
	//TODO implement me
	panic("implement me")
}

func (ff *FragmentFile) GetLastModified() int64 {
	//TODO implement me
	panic("implement me")
}

func (ff *FragmentFile) GetEntry(entryId int64) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (ff *FragmentFile) Release() error {
	//TODO implement me
	panic("implement me")
}

// Close writes the footer if it hasn't been flushed and closes the file.
func (ff *FragmentFile) Close() error {
	ff.mu.Lock()
	defer ff.mu.Unlock()

	// Write the footer to the end of the file
	ff.writeFooter()

	err := ff.mmap.Unmap()
	if err != nil {
		return err
	}

	return ff.txnFile.Close()
}

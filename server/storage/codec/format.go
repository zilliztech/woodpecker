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
	"time"
)

const (
	FormatVersion         = 4
	RecordHeaderSize      = 9  // CRC32(4) + Type(1) + Length(4)
	HeaderRecordSize      = 16 // Version(2) + Flags(2) + FirstEntryID(8) + Magic(4)
	BlockHeaderRecordSize = 24 // FirstEntryID(8) + LastEntryID(8) + BlockLength(4) + BlockCrc(4)
	IndexRecordSize       = 32 // BlockNumber(4) + StartOffset(8) + BlockSize(4) + FirstEntryID(8) + LastEntryID(8)
	FooterRecordSize      = 36 // TotalBlocks(4) + TotalRecords(4) + TotalSize(8) + IndexOffset(8) + IndexLength(4) + Version(2) + Flags(2) + Magic(4)
)

// Record types
const (
	HeaderRecordType      byte = 1
	DataRecordType        byte = 2
	IndexRecordType       byte = 3
	FooterRecordType      byte = 4
	BlockHeaderRecordType byte = 5
)

// Magic numbers
var (
	HeaderMagic = [4]byte{0x48, 0x44, 0x52, 0x01} // HDR\x01
	FooterMagic = [4]byte{0x46, 0x54, 0x52, 0x01} // FTR\x01
)

// Record interface
type Record interface {
	Type() byte
}

// HeaderRecord represents file header
type HeaderRecord struct {
	Version      uint16
	Flags        uint16
	FirstEntryID int64
}

func (h *HeaderRecord) Type() byte { return HeaderRecordType }

// DataRecord represents data payload
type DataRecord struct {
	Payload []byte
}

func (d *DataRecord) Type() byte { return DataRecordType }

// BlockHeaderRecord represents block metadata at the end of each block
// This record is placed at the end of each 2MB block to facilitate
// efficient recovery in object storage scenarios
type BlockHeaderRecord struct {
	FirstEntryID int64  // First entry ID in this block
	LastEntryID  int64  // Last entry ID in this block
	BlockLength  uint32 // Length of the block data (excluding this header record)
	BlockCrc     uint32 // CRC32 checksum of the block data (excluding this header record)
}

func (b *BlockHeaderRecord) Type() byte { return BlockHeaderRecordType }

// IndexRecord represents block-level index (one entry per 2MB block)
type IndexRecord struct {
	BlockNumber  int32  // Which 2MB block this refers to
	StartOffset  int64  // Start offset of this block in the complete file
	BlockSize    uint32 // Size of this block, including this block header record+data records of this block
	FirstEntryID int64  // First entry ID of this first data record
	LastEntryID  int64  // Last entry ID of this first data record
}

func (i *IndexRecord) Type() byte { return IndexRecordType }

// FooterRecord represents file footer
type FooterRecord struct {
	TotalBlocks  int32  // Total number of 2MB blocks
	TotalRecords uint32 // Total number of records
	TotalSize    uint64 // Total size of the file
	IndexOffset  uint64 // Offset where index section starts
	IndexLength  uint32 // Length of index section
	Version      uint16
	Flags        uint16 // [compacted:1][reserve:15]
}

func (f *FooterRecord) Type() byte { return FooterRecordType }

// FileInfo represents a file in storage
type FileInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

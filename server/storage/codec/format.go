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

// Here defines a self-describing, append-only fragment file format,
// designed for high-throughput sequential writes, concurrent multi-reader access,
// and efficient recovery or index-based random access.
//
// File Format Layout:
// -------------------
// The file consists of a sequence of variable-length records:
//     [CRC32:4][Type:1][Length:4][Payload:Length bytes]
//
// Field Definitions:
//   - CRC32  (4 bytes): CRC32 checksum of the record starting from Type.
//                       i.e., CRC32(Type + Length + Payload)
//   - Type   (1 byte) : Record type:
//       0x01 = Header
//       0x02 = Data
//       0x03 = Index
//       0x04 = Footer
//   - Length (4 bytes): Length of the payload in bytes.
//   - Payload: Type-specific content, detailed below.
//
// Record Types:
// -------------
//
// 1. Header Record (Type = 0x01)
//    Appears at the beginning of the file. Describes the file version and feature flags.
//
//    Payload:
//      [Version:2][Flags:2][FirstEntryID:8][Magic:4]
//        - Version		: format version.
//        - Flags		: feature flags (e.g. compression)
//        - FirstEntryID: logical entry ID of the first record in this file.
//        - Magic       : 4-byte constant (e.g. "WPHD")
//
// 2. Data Record (Type = 0x02)
//    Carries application-defined content.
//
//    Payload:
//      [UserPayload...]
//
// 3. Index Record (Type = 0x03)
//    Stores offsets of selected Data Records for fast lookup.
//
//    Payload:
//      [Offset1:8][Offset2:8]...[OffsetN:8]
//        - Offsets: list of physical file offsets for data records in order.
//          The N-th entry maps to EntryID = StartEntryID + N.
//
//    This enables direct mapping from logical entry ID to file offset and supports segmented indexes for scalable seeking.
//
// 4. Footer Record (Type = 0x04)
//    Appears only when the file is fully flushed. Enables fast index lookup and version discovery
//    without requiring a scan of the file or reading the header.
//
//    Payload:
//      [IndexOffset:8][IndexSize:4][FirstEntryID:8][Count:4][Version:2][Flags:2][Magic:4]
//        - IndexOffset: absolute offset of the Index Record
//        - IndexSize  : size in bytes of the Index block
//        - FirstEntryID  : copy of the logical entry ID of the first record in this file.
//        - Count  	   :  record count
//        - Version    : copy of format version (same as header)
//        - Flags      : copy of feature flags (e.g. compression)
//        - Magic      : 4-byte constant (e.g. "WPFT")
//
//    Footer Record is typically the last record in a complete file, and can be used to directly locate and parse the Index block.
//
// Application Scenarios:
// ----------------------
//
// 1. Real-time Streaming (Concurrent Write + Read):
//    - During file writing, the Footer may not be present yet.
//    - Readers can scan the file sequentially from the beginning:
//        a) Read Header to get [Version, Flags] for decoding context.
//        b) Then iterate over all subsequent records:
//              → Validate each via CRC32
//              → Use Type to distinguish Data vs Index
//    - If a record is incomplete (e.g., during concurrent append crash), CRC will fail.
//      This provides safe tailing and partial recovery.
//
// 2. Offline Read of Complete File:
//    - When the file is fully written (flush complete), a Footer is appended.
//    - Readers can seek to the end of the file (e.g., last 20 bytes), read the Footer payload,
//      and extract the Index location, version, and decoding flags.
//    - This allows:
//        a) Skipping the Header entirely
//        b) Jumping directly to the Index block
//        c) Performing fast random access to data via index
//    - Very efficient for large file scanning or analytics applications.
//
// Notes:
// ------
//   - All numeric fields are little-endian.
//   - CRC32 uses IEEE polynomial (Go: crc32.ChecksumIEEE).
//   - The format is forward-compatible:
//       - New record types can be introduced with higher Type codes.
//       - Unknown record types can be skipped safely via Length.
//   - Only one writer should append to a file; multiple concurrent readers are supported.
//   - The format is compact, robust against partial writes, and supports efficient forward and reverse traversal.

var (
	HeaderRecordType byte   = 0x1
	DataRecordType   byte   = 0x2
	IndexRecordType  byte   = 0x3
	FooterRecordType byte   = 0x4
	HeaderMagic             = [4]byte{'W', 'P', 'H', 'D'} // header record magic code
	FooterMagic             = [4]byte{'W', 'P', 'F', 'T'} // footer record magic code
	FormatVersion    uint16 = 2                           // Current Format Version
)

type Record interface {
	Type() byte
}

type HeaderRecord struct {
	Version      uint16
	Flags        uint16 //   used for compress,encryption...
	FirstEntryID int64
}

func (hr *HeaderRecord) Type() byte {
	return HeaderRecordType
}

type DataRecord struct {
	Payload []byte
}

func (dr *DataRecord) Type() byte {
	return DataRecordType
}

type IndexRecord struct {
	Offsets []uint32
}

func (ir *IndexRecord) Type() byte {
	return IndexRecordType
}

type FooterRecord struct {
	IndexOffset  uint64
	IndexLength  uint32
	FirstEntryID int64
	Count        uint32
	Version      uint16
	Flags        uint16 //  reserved flags, used for compress,encryption...
}

func (fr *FooterRecord) Type() byte {
	return FooterRecordType
}

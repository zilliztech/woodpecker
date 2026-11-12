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

// Package checkpoint encodes optional intermediate segment recovery hints for
// the direct object-storage backend used by embedded MinIO-compatible mode.
// An unavailable or invalid hint can be discarded in favor of full block-scan
// recovery; checkpoint persistence is not part of data acknowledgment.
package checkpoint

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"google.golang.org/protobuf/proto"

	pb "github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

var magic = [4]byte{'W', 'P', 'C', 'P'}

const (
	formatVersion = 1
	checksumSize  = 4
)

// New snapshots the committed prefix into the protobuf checkpoint schema.
func New(blocks []*codec.IndexRecord) *pb.SegmentCheckpoint {
	cp := &pb.SegmentCheckpoint{
		FormatVersion: formatVersion,
		BlockIndexes:  make([]*pb.CheckpointBlockIndex, len(blocks)),
		LastEntryId:   -1,
	}
	for i, block := range blocks {
		if block == nil {
			continue
		} // Serialize will reject an invalid snapshot.
		cp.BlockIndexes[i] = &pb.CheckpointBlockIndex{
			BlockNumber:  block.BlockNumber,
			StartOffset:  block.StartOffset,
			BlockSize:    block.BlockSize,
			FirstEntryId: block.FirstEntryID,
			LastEntryId:  block.LastEntryID,
		}
		cp.TotalSize += uint64(block.BlockSize)
		cp.LastEntryId = block.LastEntryID
	}
	return cp
}

// Serialize encodes [WPCP magic][protobuf payload][CRC32 IEEE, little endian].
// Only the thin framing is custom; all snapshot fields use protobuf encoding.
func Serialize(cp *pb.SegmentCheckpoint) ([]byte, error) {
	if err := validate(cp); err != nil {
		return nil, err
	}
	payload, err := proto.Marshal(cp)
	if err != nil {
		return nil, fmt.Errorf("marshal checkpoint: %w", err)
	}
	out := make([]byte, len(magic)+len(payload)+checksumSize)
	copy(out, magic[:])
	copy(out[len(magic):], payload)
	binary.LittleEndian.PutUint32(out[len(out)-checksumSize:], crc32.ChecksumIEEE(payload))
	return out, nil
}

// Parse verifies framing, integrity and recovery invariants before exposing a
// snapshot. Unsupported or invalid hints are safe to discard and scan in full.
// Unknown protobuf fields are retained, including when Serialize is called again.
func Parse(raw []byte) (*pb.SegmentCheckpoint, error) {
	if len(raw) < len(magic)+checksumSize || !bytes.Equal(raw[:len(magic)], magic[:]) {
		return nil, fmt.Errorf("invalid checkpoint framing")
	}
	payload := raw[len(magic) : len(raw)-checksumSize]
	checksum := binary.LittleEndian.Uint32(raw[len(raw)-checksumSize:])
	if crc32.ChecksumIEEE(payload) != checksum {
		return nil, fmt.Errorf("checkpoint checksum mismatch")
	}
	cp := &pb.SegmentCheckpoint{}
	if err := proto.Unmarshal(payload, cp); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint: %w", err)
	}
	if err := validate(cp); err != nil {
		return nil, err
	}
	return cp, nil
}

// Indexes converts a validated checkpoint to the existing in-memory WAL index.
func Indexes(cp *pb.SegmentCheckpoint) []*codec.IndexRecord {
	blocks := make([]*codec.IndexRecord, len(cp.BlockIndexes))
	for i, block := range cp.BlockIndexes {
		blocks[i] = &codec.IndexRecord{
			BlockNumber:  block.BlockNumber,
			StartOffset:  block.StartOffset,
			BlockSize:    block.BlockSize,
			FirstEntryID: block.FirstEntryId,
			LastEntryID:  block.LastEntryId,
		}
	}
	return blocks
}

func validate(cp *pb.SegmentCheckpoint) error {
	if cp == nil {
		return fmt.Errorf("nil checkpoint")
	}
	if cp.FormatVersion != formatVersion {
		return fmt.Errorf("unsupported checkpoint version %d", cp.FormatVersion)
	}
	var totalSize uint64
	var lastEntryID int64 = -1
	for i, block := range cp.BlockIndexes {
		if block == nil || int64(block.BlockNumber) != int64(i) || block.StartOffset != int64(i) ||
			block.BlockSize == 0 || block.FirstEntryId < 0 || block.LastEntryId < block.FirstEntryId ||
			(i > 0 && block.FirstEntryId != lastEntryID+1) {
			return fmt.Errorf("invalid or non-contiguous checkpoint index at block %d", i)
		}
		totalSize += uint64(block.BlockSize)
		lastEntryID = block.LastEntryId
	}
	if cp.TotalSize != totalSize || cp.LastEntryId != lastEntryID {
		return fmt.Errorf("inconsistent checkpoint size or last entry ID")
	}
	return nil
}

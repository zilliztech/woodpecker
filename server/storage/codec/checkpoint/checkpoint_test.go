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

package checkpoint

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	pb "github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage/codec"
)

func testIndexes(count int) []*codec.IndexRecord {
	blocks := make([]*codec.IndexRecord, count)
	for i := range blocks {
		blocks[i] = &codec.IndexRecord{
			BlockNumber: int32(i), StartOffset: int64(i), BlockSize: 1024,
			FirstEntryID: int64(i * 100), LastEntryID: int64(i*100 + 99),
		}
	}
	return blocks
}

// Make independently framed fixtures, including invalid protobuf snapshots
// that Serialize intentionally refuses to write.
func frameTestPayload(payload []byte) []byte {
	raw := append([]byte("WPCP"), payload...)
	return binary.LittleEndian.AppendUint32(raw, crc32.ChecksumIEEE(payload))
}

func TestProtobufCheckpointRoundTrip(t *testing.T) {
	for _, count := range []int{0, 1, 635} {
		blocks := testIndexes(count)
		raw, err := Serialize(New(blocks))
		require.NoError(t, err)
		cp, err := Parse(raw)
		require.NoError(t, err)
		require.Equal(t, blocks, Indexes(cp))
		require.Equal(t, uint64(count*1024), cp.TotalSize)
		require.Equal(t, int64(count*100-1), cp.LastEntryId)
		// The payload is an ordinary typed protobuf message, not encoded WAL records.
		decoded := &pb.SegmentCheckpoint{}
		require.NoError(t, proto.Unmarshal(raw[4:len(raw)-4], decoded))
		require.True(t, proto.Equal(cp, decoded))
	}
}

func TestProtobufCheckpointPreservesUnknownFields(t *testing.T) {
	cp := New(testIndexes(2))
	extra := protowire.AppendTag(nil, 100, protowire.BytesType)
	extra = protowire.AppendString(extra, "future checkpoint metadata")
	cp.ProtoReflect().SetUnknown(extra)
	blockExtra := protowire.AppendTag(nil, 101, protowire.VarintType)
	blockExtra = protowire.AppendVarint(blockExtra, 42)
	cp.BlockIndexes[0].ProtoReflect().SetUnknown(blockExtra)
	raw, err := Serialize(cp)
	require.NoError(t, err)
	parsed, err := Parse(raw)
	require.NoError(t, err)
	require.Equal(t, extra, []byte(parsed.ProtoReflect().GetUnknown()))
	require.Equal(t, blockExtra, []byte(parsed.BlockIndexes[0].ProtoReflect().GetUnknown()))
	rewritten, err := Serialize(parsed)
	require.NoError(t, err)
	parsedAgain, err := Parse(rewritten)
	require.NoError(t, err)
	require.True(t, proto.Equal(cp, parsedAgain))
}

func TestProtobufCheckpointRejectsCorruption(t *testing.T) {
	valid, err := Serialize(New(testIndexes(2)))
	require.NoError(t, err)
	corrupted := append([]byte(nil), valid...)
	corrupted[5] ^= 1
	badChecksum := append([]byte(nil), valid...)
	badChecksum[len(badChecksum)-1] ^= 1
	cases := map[string][]byte{
		"empty":              nil,
		"short framing":      []byte("WPCP"),
		"wrong magic":        append([]byte("NOPE"), valid[4:]...),
		"truncated":          valid[:len(valid)-1],
		"corrupted payload":  corrupted,
		"corrupted checksum": badChecksum,
		"invalid protobuf":   frameTestPayload([]byte{0xff}),
		"empty protobuf":     frameTestPayload(nil),
		"trailing bytes":     append(append([]byte(nil), valid...), 0),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			require.NotPanics(t, func() { _, err := Parse(data); require.Error(t, err) })
		})
	}
}

func TestProtobufCheckpointRejectsInvalidSnapshots(t *testing.T) {
	cases := map[string]func(*pb.SegmentCheckpoint){
		"unsupported version": func(cp *pb.SegmentCheckpoint) { cp.FormatVersion++ },
		"missing version":     func(cp *pb.SegmentCheckpoint) { cp.FormatVersion = 0 },
		"nil block":           func(cp *pb.SegmentCheckpoint) { cp.BlockIndexes[0] = nil },
		"block gap":           func(cp *pb.SegmentCheckpoint) { cp.BlockIndexes[1].BlockNumber++ },
		"duplicate block":     func(cp *pb.SegmentCheckpoint) { cp.BlockIndexes[1].BlockNumber = 0 },
		"negative block":      func(cp *pb.SegmentCheckpoint) { cp.BlockIndexes[0].BlockNumber = -1 },
		"wrong offset":        func(cp *pb.SegmentCheckpoint) { cp.BlockIndexes[1].StartOffset++ },
		"zero size":           func(cp *pb.SegmentCheckpoint) { cp.BlockIndexes[0].BlockSize = 0 },
		"entry gap":           func(cp *pb.SegmentCheckpoint) { cp.BlockIndexes[1].FirstEntryId++ },
		"entry overlap":       func(cp *pb.SegmentCheckpoint) { cp.BlockIndexes[1].FirstEntryId-- },
		"negative entry":      func(cp *pb.SegmentCheckpoint) { cp.BlockIndexes[0].FirstEntryId = -1 },
		"invalid entry range": func(cp *pb.SegmentCheckpoint) { cp.BlockIndexes[1].LastEntryId = 0 },
		"wrong size":          func(cp *pb.SegmentCheckpoint) { cp.TotalSize++ },
		"wrong LAC":           func(cp *pb.SegmentCheckpoint) { cp.LastEntryId++ },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cp := New(testIndexes(2))
			mutate(cp)
			_, err := Serialize(cp)
			require.Error(t, err)
			payload, err := proto.Marshal(cp)
			require.NoError(t, err)
			_, err = Parse(frameTestPayload(payload))
			require.Error(t, err)
		})
	}
	_, err := Serialize(nil)
	require.Error(t, err)
	_, err = Serialize(New([]*codec.IndexRecord{nil}))
	require.Error(t, err)
}

func TestProtobufCheckpointRequiresExplicitEmptyLAC(t *testing.T) {
	// An omitted scalar defaults to zero in proto3, which must not fabricate an
	// entry in an empty segment. Our writer persists -1 explicitly.
	payload, err := proto.Marshal(&pb.SegmentCheckpoint{FormatVersion: 1})
	require.NoError(t, err)
	_, err = Parse(frameTestPayload(payload))
	require.Error(t, err)
}

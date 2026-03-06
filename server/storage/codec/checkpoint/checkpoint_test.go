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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// buildBlockIndexesSection replicates the output of serializeFooterAndIndexes
// using only the public codec API, so the checkpoint package tests have no
// dependency on objectstorage.
func buildBlockIndexesSection(records []*codec.IndexRecord) []byte {
	var out []byte
	var totalSize int64
	lastEntryID := int64(-1)
	for _, r := range records {
		out = append(out, codec.EncodeRecord(r)...)
		totalSize += int64(r.BlockSize)
		lastEntryID = r.LastEntryID
	}
	footer := &codec.FooterRecord{
		TotalBlocks:  int32(len(records)),
		TotalRecords: uint32(len(records)),
		TotalSize:    uint64(totalSize),
		IndexOffset:  0,
		IndexLength:  uint32(len(out)),
		Version:      codec.FormatVersion,
		Flags:        0,
		LAC:          lastEntryID,
	}
	out = append(out, codec.EncodeRecord(footer)...)
	return out
}

func makeTestIndexRecords(n int) []*codec.IndexRecord {
	records := make([]*codec.IndexRecord, n)
	for i := 0; i < n; i++ {
		records[i] = &codec.IndexRecord{
			BlockNumber:  int32(i),
			StartOffset:  int64(i),
			BlockSize:    1024,
			FirstEntryID: int64(i * 100),
			LastEntryID:  int64(i*100 + 99),
		}
	}
	return records
}

func TestSerializeAndParse(t *testing.T) {
	cp := New()
	cp.SetSection(SectionBlockIndexes, []byte{10, 20, 30, 40, 50})
	raw := cp.Serialize()

	require.True(t, IsFormat(raw))

	cp2, err := Parse(raw)
	require.NoError(t, err)
	assert.Equal(t, 1, cp2.Meta.Version)
	require.Len(t, cp2.Meta.Sections, 1)
	assert.Equal(t, SectionBlockIndexes, cp2.Meta.Sections[0].Type)

	got, err := cp2.GetSectionData(SectionBlockIndexes)
	require.NoError(t, err)
	assert.Equal(t, []byte{10, 20, 30, 40, 50}, got)
}

func TestIsFormat(t *testing.T) {
	t.Run("new format", func(t *testing.T) {
		cp := New()
		cp.SetSection(SectionBlockIndexes, []byte{1})
		assert.True(t, IsFormat(cp.Serialize()))
	})

	t.Run("random data", func(t *testing.T) {
		assert.False(t, IsFormat([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}))
	})

	t.Run("too small", func(t *testing.T) {
		assert.False(t, IsFormat([]byte{1, 2, 3}))
	})

	t.Run("empty", func(t *testing.T) {
		assert.False(t, IsFormat(nil))
	})
}

func TestParse_Errors(t *testing.T) {
	t.Run("too small", func(t *testing.T) {
		_, err := Parse([]byte{1, 2, 3})
		assert.Error(t, err)
	})

	t.Run("bad magic", func(t *testing.T) {
		data := make([]byte, 16)
		copy(data[12:], []byte("NOPE"))
		_, err := Parse(data)
		assert.Error(t, err)
	})

	t.Run("metaLength exceeds data", func(t *testing.T) {
		data := make([]byte, trailerSize)
		data[0] = 0xE7
		data[1] = 0x03
		copy(data[4:], magic[:])
		_, err := Parse(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds data size")
	})
}

func TestGetSectionData_Errors(t *testing.T) {
	cp := New()
	cp.SetSection(SectionBlockIndexes, []byte{1, 2, 3})

	t.Run("not found", func(t *testing.T) {
		_, err := cp.GetSectionData(99)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("found", func(t *testing.T) {
		got, err := cp.GetSectionData(SectionBlockIndexes)
		require.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3}, got)
	})
}

func TestSetSection_Replace(t *testing.T) {
	const (
		typeA SectionType = 10
		typeB SectionType = 20
	)

	cp := New()
	cp.SetSection(typeA, []byte{1, 2})
	cp.SetSection(typeB, []byte{3, 4, 5})
	cp.SetSection(typeA, []byte{6, 7, 8, 9}) // replace typeA

	require.Len(t, cp.Meta.Sections, 2)

	// typeB should come first (preserved order), then new typeA appended at end.
	assert.Equal(t, typeB, cp.Meta.Sections[0].Type)
	assert.Equal(t, typeA, cp.Meta.Sections[1].Type)

	got, err := cp.GetSectionData(typeA)
	require.NoError(t, err)
	assert.Equal(t, []byte{6, 7, 8, 9}, got)

	got, err = cp.GetSectionData(typeB)
	require.NoError(t, err)
	assert.Equal(t, []byte{3, 4, 5}, got)

	// Round-trip through serialize/parse.
	raw := cp.Serialize()
	cp2, err := Parse(raw)
	require.NoError(t, err)

	got, err = cp2.GetSectionData(typeA)
	require.NoError(t, err)
	assert.Equal(t, []byte{6, 7, 8, 9}, got)

	got, err = cp2.GetSectionData(typeB)
	require.NoError(t, err)
	assert.Equal(t, []byte{3, 4, 5}, got)
}

func TestFindSection(t *testing.T) {
	meta := &Meta{
		Version: 1,
		Sections: []Section{
			{Type: SectionBlockIndexes, Offset: 0, Length: 100},
		},
	}

	assert.NotNil(t, meta.FindSection(SectionBlockIndexes))
	assert.Nil(t, meta.FindSection(99))
}

func TestCheckpointWithBlockIndexes(t *testing.T) {
	blocks := makeTestIndexRecords(5)
	sectionBytes := buildBlockIndexesSection(blocks)

	cp := New()
	cp.SetSection(SectionBlockIndexes, sectionBytes)
	raw := cp.Serialize()

	require.True(t, IsFormat(raw))

	// Parse back and extract section.
	cp2, err := Parse(raw)
	require.NoError(t, err)

	got, err := cp2.GetSectionData(SectionBlockIndexes)
	require.NoError(t, err)
	assert.Equal(t, sectionBytes, got)

	// Parse footer from section data.
	maxFooterSize := codec.GetMaxFooterReadSize()
	footerData := got[len(got)-maxFooterSize:]
	footer, err := codec.ParseFooterFromBytes(footerData)
	require.NoError(t, err)
	assert.Equal(t, int32(5), footer.TotalBlocks)

	// Parse index records.
	actualFooterSize := codec.RecordHeaderSize + codec.GetFooterRecordSize(footer.Version)
	idxData := got[:len(got)-actualFooterSize]
	recordSize := codec.RecordHeaderSize + codec.IndexRecordSize
	assert.Equal(t, 5*recordSize, len(idxData))

	for i := 0; i < 5; i++ {
		rec, decErr := codec.DecodeRecord(idxData[i*recordSize:])
		require.NoError(t, decErr)
		idx := rec.(*codec.IndexRecord)
		assert.Equal(t, int32(i), idx.BlockNumber)
		assert.Equal(t, int64(i*100), idx.FirstEntryID)
		assert.Equal(t, int64(i*100+99), idx.LastEntryID)
	}
}

func TestBackwardCompatibility(t *testing.T) {
	// Old-format data: raw footer+indexes with no CKPT trailer.
	oldData := buildBlockIndexesSection(makeTestIndexRecords(3))
	require.False(t, IsFormat(oldData))

	// Verify old data can still be parsed by the same footer+index logic.
	minFooterSize := codec.RecordHeaderSize + codec.FooterRecordSizeV5
	require.Greater(t, len(oldData), minFooterSize)

	maxFooterSize := codec.GetMaxFooterReadSize()
	footerData := oldData[len(oldData)-maxFooterSize:]
	footer, err := codec.ParseFooterFromBytes(footerData)
	require.NoError(t, err)
	assert.Equal(t, int32(3), footer.TotalBlocks)

	actualFooterSize := codec.RecordHeaderSize + codec.GetFooterRecordSize(footer.Version)
	idxData := oldData[:len(oldData)-actualFooterSize]
	recordSize := codec.RecordHeaderSize + codec.IndexRecordSize
	assert.Equal(t, 3*recordSize, len(idxData))

	for i := 0; i < 3; i++ {
		rec, decErr := codec.DecodeRecord(idxData[i*recordSize:])
		require.NoError(t, decErr)
		idx := rec.(*codec.IndexRecord)
		assert.Equal(t, int32(i), idx.BlockNumber)
	}
}

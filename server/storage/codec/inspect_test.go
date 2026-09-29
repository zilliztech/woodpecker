package codec

import (
	"bytes"
	"context"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
)

// segmentBuilder lays out a segment file the way the writer does: a file header, then one block
// header followed by its data records per block, then (for a sealed file) the index records and the
// footer.
type segmentBuilder struct {
	buf     bytes.Buffer
	indexes []*IndexRecord
	blocks  int32
	lastID  int64
}

func (b *segmentBuilder) addBlock(entries int) {
	data := make([]byte, 0, entries*16)
	first := b.lastID
	for i := 0; i < entries; i++ {
		data = append(data, EncodeRecord(&DataRecord{Payload: []byte{byte(b.lastID)}})...)
		b.lastID++
	}
	start := int64(b.buf.Len())
	header := EncodeRecord(&BlockHeaderRecord{
		BlockNumber:  b.blocks,
		FirstEntryID: first,
		LastEntryID:  b.lastID - 1,
		BlockLength:  uint32(len(data)),
		BlockCrc:     crc32.ChecksumIEEE(data),
	})
	b.buf.Write(header)
	b.buf.Write(data)
	b.indexes = append(b.indexes, &IndexRecord{
		BlockNumber:  b.blocks,
		StartOffset:  start,
		BlockSize:    uint32(len(header) + len(data)),
		FirstEntryID: first,
		LastEntryID:  b.lastID - 1,
	})
	b.blocks++
}

// seal appends the index records and the footer, as completing a segment does.
func (b *segmentBuilder) seal() {
	indexStart := int64(b.buf.Len())
	for _, idx := range b.indexes {
		b.buf.Write(EncodeRecord(idx))
	}
	b.buf.Write(EncodeRecord(&FooterRecord{
		TotalBlocks: b.blocks,
		IndexOffset: uint64(indexStart),
		IndexLength: uint32(int64(b.buf.Len()) - indexStart),
		Version:     FormatVersion,
		LAC:         b.lastID - 1,
	}))
}

func newSegment(t *testing.T, blocks, entriesPerBlock int, sealed bool) *segmentBuilder {
	t.Helper()
	b := &segmentBuilder{}
	b.buf.Write(EncodeRecord(&HeaderRecord{Version: FormatVersion, FirstEntryID: 0}))
	for i := 0; i < blocks; i++ {
		b.addBlock(entriesPerBlock)
	}
	if sealed {
		b.seal()
	}
	return b
}

func (b *segmentBuilder) survey(fromBlock, maxBlocks int64) SegmentSurvey {
	data := b.buf.Bytes()
	return InspectBlocks(context.Background(), bytes.NewReader(data), int64(len(data)), fromBlock, maxBlocks)
}

// corruptAt flips a byte, which is what a damaged block looks like from here.
func (b *segmentBuilder) corruptAt(offset int) {
	b.buf.Bytes()[offset] ^= 0xFF
}

// TestInspectBlocks_HealthySealedSegment covers the baseline: every block verifies, and the survey
// knows how many there should be.
func TestInspectBlocks_HealthySealedSegment(t *testing.T) {
	b := newSegment(t, 4, 3, true)

	got := b.survey(0, 100)

	require.Len(t, got.Blocks, 4)
	for i, blk := range got.Blocks {
		require.Equal(t, BlockOK, blk.Status, "block %d", i)
	}
	require.True(t, got.Sealed)
	require.Equal(t, int32(4), got.TotalBlocksKnown)
	require.False(t, got.StoppedEarly)
}

// TestInspectBlocks_DamageIsBounded is what the command exists for. A read stops at the first bad
// block, so nothing today can say whether the blocks after it are readable -- which is the
// difference between skipping a few entries and skipping the rest of the segment.
func TestInspectBlocks_DamageIsBounded(t *testing.T) {
	b := newSegment(t, 4, 3, true)
	// Flip a byte inside block 1's data, leaving its header intact.
	b.corruptAt(int(b.indexes[1].StartOffset) + RecordHeaderSize + BlockHeaderRecordSize + 2)

	got := b.survey(0, 100)

	require.Len(t, got.Blocks, 4, "the survey must not stop where a read would")
	require.Equal(t, BlockOK, got.Blocks[0].Status)
	require.Equal(t, BlockChecksumFailed, got.Blocks[1].Status)
	require.Equal(t, BlockOK, got.Blocks[2].Status, "the damage is bounded, and that is the finding")
	require.Equal(t, BlockOK, got.Blocks[3].Status)
	require.Contains(t, got.Blocks[1].Detail, "CRC")
}

// TestInspectBlocks_ActiveSegmentSurvivesABadBody covers the same damage with no footer: the block's
// own header still gives its length, so the walk can step over it.
func TestInspectBlocks_ActiveSegmentSurvivesABadBody(t *testing.T) {
	b := newSegment(t, 3, 3, false)
	b.corruptAt(int(b.indexes[1].StartOffset) + RecordHeaderSize + BlockHeaderRecordSize + 1)

	got := b.survey(0, 100)

	require.Len(t, got.Blocks, 3)
	require.Equal(t, BlockChecksumFailed, got.Blocks[1].Status)
	require.Equal(t, BlockOK, got.Blocks[2].Status)
	require.False(t, got.Sealed)
}

// TestInspectBlocks_ActiveSegmentStopsAtABrokenChain is the boundary of what a survey can see. An
// active segment has no index, so block N+1 is found only through block N's header. With that
// header unreadable the walk cannot continue -- and saying the rest is damaged would be reporting
// "I cannot see it" as "it is broken".
func TestInspectBlocks_ActiveSegmentStopsAtABrokenChain(t *testing.T) {
	b := newSegment(t, 4, 3, false)
	b.corruptAt(int(b.indexes[1].StartOffset) + 2) // inside block 1's header record

	got := b.survey(0, 100)

	require.Equal(t, BlockOK, got.Blocks[0].Status)
	require.Len(t, got.Blocks, 2, "block 1 is reported; nothing past it can be located")
	// DecodeRecordList answers corruption by stopping rather than erroring, so a damaged header
	// presents as none being found at that offset.
	require.Equal(t, BlockHeaderMissing, got.Blocks[1].Status)
	require.True(t, got.StoppedEarly)
	require.Equal(t, SurveyStopChainBroken, got.StopReason)
}

// TestInspectBlocks_SealedSegmentWalksPastABrokenHeader covers the same damage on a sealed segment,
// where the index locates every block independently of the broken one.
func TestInspectBlocks_SealedSegmentWalksPastABrokenHeader(t *testing.T) {
	b := newSegment(t, 4, 3, true)
	b.corruptAt(int(b.indexes[1].StartOffset) + 2)

	got := b.survey(0, 100)

	require.Len(t, got.Blocks, 4, "the index does not depend on the damaged header")
	require.Equal(t, BlockOK, got.Blocks[2].Status)
	require.Equal(t, BlockOK, got.Blocks[3].Status)
	require.False(t, got.StoppedEarly)
}

// TestInspectBlocks_BoundIsReportedAsTheBound keeps a truncated survey from reading as a truncated
// segment.
func TestInspectBlocks_BoundIsReportedAsTheBound(t *testing.T) {
	b := newSegment(t, 5, 2, true)

	got := b.survey(1, 2)

	require.Len(t, got.Blocks, 2)
	require.Equal(t, int64(1), got.Blocks[0].Number, "the survey starts where it was asked to")
	require.True(t, got.StoppedEarly)
	require.Equal(t, SurveyStopBound, got.StopReason)
}

// recordsOf builds a buffer of n data records, as a block's body is.
func recordsOf(n int) []byte {
	out := make([]byte, 0, n*16)
	for i := 0; i < n; i++ {
		out = append(out, EncodeRecord(&DataRecord{Payload: []byte{byte(i), byte(i + 1)}})...)
	}
	return out
}

// TestInspectRecords_CleanBuffer is the baseline: every record decodes and the walk consumes the
// whole buffer.
func TestInspectRecords_CleanBuffer(t *testing.T) {
	buf := recordsOf(5)

	got := InspectRecords(buf)

	require.Equal(t, 5, got.Records)
	require.Equal(t, 5, got.DataRecords)
	require.Equal(t, len(buf), got.BytesConsumed)
	require.False(t, got.Stopped)
}

// TestInspectRecords_FindsTheDamagedRecord is what this adds over the block checksum. A block's CRC
// covers all of it, so one flipped byte condemns the whole block; each record carries its own
// checksum, so the damage can be placed at a record -- which is the difference between losing a
// block's worth of entries and losing the tail of one.
func TestInspectRecords_FindsTheDamagedRecord(t *testing.T) {
	buf := recordsOf(5)
	recordSize := RecordHeaderSize + 2
	buf[recordSize*2+RecordHeaderSize] ^= 0xFF // inside the third record's payload

	got := InspectRecords(buf)

	require.Equal(t, 2, got.DataRecords, "two records are readable before the damage")
	require.True(t, got.Stopped)
	require.Equal(t, RecordStopChecksum, got.StopReason)
	require.Equal(t, recordSize*2, got.StopOffset)
}

// TestInspectRecords_TruncatedTail covers a buffer that ends mid-record, which is what a torn write
// leaves behind. The two cuts are different code paths: one leaves too little for a record header at
// all, the other leaves a header whose record claims more bytes than remain.
func TestInspectRecords_TruncatedTail(t *testing.T) {
	recordSize := RecordHeaderSize + 2

	t.Run("not even a header left", func(t *testing.T) {
		buf := recordsOf(3)
		got := InspectRecords(buf[:len(buf)-3])

		require.Equal(t, 2, got.DataRecords)
		require.True(t, got.Stopped)
		require.Equal(t, RecordStopTruncated, got.StopReason)
		require.Contains(t, got.StopDetail, "a record header needs")
	})

	t.Run("header left, payload cut", func(t *testing.T) {
		buf := recordsOf(3)
		require.Greater(t, recordSize-1, RecordHeaderSize, "the cut must leave a whole header")
		got := InspectRecords(buf[:len(buf)-1])

		require.Equal(t, 2, got.DataRecords)
		require.True(t, got.Stopped)
		require.Equal(t, RecordStopTruncated, got.StopReason)
		require.Contains(t, got.StopDetail, "remain")
	})
}

// TestInspectRecords_UnknownType covers a record whose checksum matches but whose type nothing can
// parse -- a file written by something else, or a version this build does not know.
func TestInspectRecords_UnknownType(t *testing.T) {
	buf := recordsOf(2)
	odd := EncodeRecord(&DataRecord{Payload: []byte{9}})
	odd[4] = 0x7F                      // type byte
	crc := crc32.ChecksumIEEE(odd[4:]) // re-checksum so only the type is wrong
	odd[0], odd[1], odd[2], odd[3] = byte(crc), byte(crc>>8), byte(crc>>16), byte(crc>>24)
	buf = append(buf, odd...)

	got := InspectRecords(buf)

	require.Equal(t, 2, got.DataRecords)
	require.True(t, got.Stopped)
	require.Equal(t, RecordStopUnparseable, got.StopReason)
	require.Contains(t, got.StopDetail, "unknown record type")
}

// TestInspectBlocks_DamagedBlockNamesItsLastGoodEntry is the payoff. The block's checksum fails, so
// a reader gives up on all of it; the records inside still say how far it is readable, and that is
// what a skip range has to cover.
func TestInspectBlocks_DamagedBlockNamesItsLastGoodEntry(t *testing.T) {
	b := &segmentBuilder{}
	b.buf.Write(EncodeRecord(&HeaderRecord{Version: FormatVersion, FirstEntryID: 0}))
	b.addBlock(5) // entries 0..4
	b.addBlock(5) // entries 5..9
	b.seal()
	// Damage the fourth record of the second block: entries 5, 6, 7 remain readable.
	recordSize := RecordHeaderSize + 1
	start := int(b.indexes[1].StartOffset) + RecordHeaderSize + BlockHeaderRecordSize
	b.corruptAt(start + recordSize*3 + RecordHeaderSize)

	got := b.survey(0, 100)

	require.Equal(t, BlockChecksumFailed, got.Blocks[1].Status)
	require.Equal(t, 3, got.Blocks[1].RecordsOK, "three records survive the damage")
	require.Equal(t, int64(7), got.Blocks[1].LastGoodEntryID,
		"entries 5, 6 and 7 are still readable inside a block a reader would abandon whole")
}

package codec

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
)

// segmentBuilder lays out a segment file the way the writer does: a file header, then one block
// header followed by its data records per block, then (for a sealed file) the index records and the
// footer.
type segmentBuilder struct {
	buf             bytes.Buffer
	indexes         []*IndexRecord
	blocks          int32
	lastID          int64
	entriesPerBlock int
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
	b := &segmentBuilder{entriesPerBlock: entriesPerBlock}
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

	require.Len(t, got.Blocks, 1, "only a block that was actually read may be reported")
	require.Equal(t, BlockOK, got.Blocks[0].Status)
	require.True(t, got.StoppedEarly)
	require.Equal(t, SurveyStopChainBroken, got.StopReason)
	require.Equal(t, b.indexes[1].StartOffset, got.SurveyStopOffset,
		"where the walk gave up is what an operator needs, not a block asserted to be there")
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

// truncateTo cuts the file short, which is what a torn write or a partial copy leaves.
func (b *segmentBuilder) truncateTo(n int) {
	b.buf.Truncate(n)
}

// --- damage matrix -----------------------------------------------------------------------------
//
// Two dimensions decide what a survey should do: how big the damage is, and what it lands on.
// Enumerating raw byte offsets would be endless and mostly redundant, so the location is named by
// the structures the format is made of -- the file header, a block header, a data record, an index
// record, the footer -- each taken at its first, middle and last occurrence, because a survey
// navigates by exactly those. Segment form multiplies both: a sealed segment can locate blocks
// through its index, an active one only through the chain.

type damageKind string

const (
	damageFlip     damageKind = "flip"     // bytes corrupted, every length still intact
	damageZero     damageKind = "zero"     // bytes cleared, which takes the lengths with them
	damageTruncate damageKind = "truncate" // bytes gone
)

type damageSite struct {
	where   string // file_header | block_header | data_record | index_record | footer
	ordinal string // first | middle | last -- which occurrence of that structure
	record  string // for data_record: which record inside the chosen block
}

type damageSpec struct {
	site  damageSite
	kind  damageKind
	bytes int
}

// locate resolves a named structure to its offset in the built file.
func (b *segmentBuilder) locate(t *testing.T, site damageSite) int {
	t.Helper()
	pick := func(n int) int {
		switch site.ordinal {
		case "first":
			return 0
		case "last":
			return n - 1
		default:
			return n / 2
		}
	}
	switch site.where {
	case "file_header":
		return 2 // inside the file header record's payload
	case "block_header":
		return int(b.indexes[pick(len(b.indexes))].StartOffset) + 2
	case "data_record":
		block := pick(len(b.indexes))
		blockStart := int(b.indexes[block].StartOffset) + RecordHeaderSize + BlockHeaderRecordSize
		// Which record inside the block matters as much as which block: damage to the first record
		// leaves nothing decodable, while damage to a later one leaves the entries before it
		// readable -- which is the difference between losing a block and losing its tail.
		perRecord := RecordHeaderSize + 1 // the builder writes one-byte payloads
		index := 0
		switch site.record {
		case "last":
			index = b.entriesPerBlock - 1
		case "middle":
			index = b.entriesPerBlock / 2
		}
		return blockStart + index*perRecord + RecordHeaderSize
	case "index_record":
		indexStart := b.buf.Len() - (RecordHeaderSize + GetFooterRecordSize(FormatVersion)) -
			len(b.indexes)*(RecordHeaderSize+IndexRecordSize)
		return indexStart + pick(len(b.indexes))*(RecordHeaderSize+IndexRecordSize) + 2
	case "footer":
		return b.buf.Len() - GetFooterRecordSize(FormatVersion) + 2
	}
	t.Fatalf("unknown damage site %q", site.where)
	return 0
}

// apply damages the file as described, clamped to what is there.
func (b *segmentBuilder) apply(t *testing.T, spec damageSpec) {
	t.Helper()
	at := b.locate(t, spec.site)
	switch spec.kind {
	case damageTruncate:
		if at < b.buf.Len() {
			b.truncateTo(at)
		}
	default:
		raw := b.buf.Bytes()
		for i := at; i < at+spec.bytes && i < len(raw); i++ {
			if spec.kind == damageZero {
				raw[i] = 0
			} else {
				raw[i] ^= 0xFF
			}
		}
	}
}

// assertSurveyInvariants holds for every damaged file, whatever was damaged. They are what makes a
// report worth acting on: it may not invent damage, may not pass over what it did not look at, and
// may not call a block readable without it being so.
func assertSurveyInvariants(t *testing.T, b *segmentBuilder, got SegmentSurvey, bound int64) {
	t.Helper()
	raw := b.buf.Bytes()

	// I4: bounded and terminating. Reaching here at all covers termination.
	require.LessOrEqual(t, int64(len(got.Blocks)), bound, "the survey exceeded the bound it was given")

	// The end of the data region: anything at or past it is the index and footer, which are not
	// blocks and must never be reported as damaged ones.
	dataEnd := len(raw)
	if len(b.indexes) > 0 {
		last := b.indexes[len(b.indexes)-1]
		if end := int(last.StartOffset) + int(last.BlockSize); end < dataEnd {
			dataEnd = end
		}
	}

	for _, block := range got.Blocks {
		// I1: a block in the report was actually looked at, so it must lie inside the file, and
		// inside the part of it that holds blocks.
		require.GreaterOrEqual(t, block.Offset, int64(0))
		require.Less(t, block.Offset, int64(len(raw)), "block %d is reported from beyond the file", block.Number)
		require.Less(t, block.Offset, int64(dataEnd),
			"block %d is reported from the index or footer region, which holds no blocks", block.Number)

		if block.Status != BlockOK {
			continue
		}
		// I3: a block called readable must be readable, verified here rather than believed.
		start := block.Offset + RecordHeaderSize + BlockHeaderRecordSize
		if block.Offset == 0 {
			start += RecordHeaderSize + HeaderRecordSize
		}
		require.LessOrEqual(t, start+block.Bytes, int64(len(raw)),
			"block %d is called readable but runs past the file", block.Number)
		body := raw[start : start+block.Bytes]
		survey := InspectRecords(body)
		require.False(t, survey.Stopped,
			"block %d is called readable but its records stop: %s", block.Number, survey.StopDetail)
	}

	// I2: no silent gaps. Calling it the end of the segment is only allowed when what follows
	// cannot be a block at all -- a tail too short to hold a block header, which on a segment still
	// being written is where the writer has got to. Anything longer than that was passed over.
	if got.StopReason == SurveyStopEnd && len(got.Blocks) < len(b.indexes) {
		next := int(b.indexes[len(got.Blocks)].StartOffset)
		require.Less(t, len(raw)-next, RecordHeaderSize+BlockHeaderRecordSize,
			"the survey stopped after block %d and called it the end, while a whole block header still follows at %d of %d bytes",
			len(got.Blocks)-1, next, len(raw))
	}
}

func recordPart(site damageSite) string {
	if site.record == "" {
		return ""
	}
	return " record:" + site.record
}

// TestInspectBlocks_DamageMatrix runs every (size, location) pair over both segment forms and holds
// the invariants for all of them.
func TestInspectBlocks_DamageMatrix(t *testing.T) {
	const blocks, entries = 5, 4
	sites := []damageSite{
		{"file_header", "first", ""},
		{"block_header", "first", ""},
		{"block_header", "middle", ""},
		{"block_header", "last", ""},
		{"data_record", "first", "first"},
		{"data_record", "middle", "first"},
		{"data_record", "last", "first"},
		{"data_record", "middle", "middle"},
		{"data_record", "middle", "last"},
		{"data_record", "last", "last"},
		{"index_record", "first", ""},
		{"index_record", "middle", ""},
		{"index_record", "last", ""},
		{"footer", "first", ""},
	}
	sizes := []struct {
		name  string
		kind  damageKind
		bytes int
	}{
		{"1 byte flipped", damageFlip, 1},
		{"64 bytes flipped", damageFlip, 64},
		{"4KB flipped", damageFlip, 4096},
		{"4KB cleared", damageZero, 4096},
		{"truncated here", damageTruncate, 0},
	}

	for _, sealed := range []bool{true, false} {
		form := "active"
		if sealed {
			form = "sealed"
		}
		for _, site := range sites {
			if !sealed && (site.where == "index_record" || site.where == "footer") {
				continue // an active segment has neither
			}
			for _, size := range sizes {
				name := fmt.Sprintf("%s/%s %s%s/%s", form, site.where, site.ordinal, recordPart(site), size.name)
				t.Run(name, func(t *testing.T) {
					b := newSegment(t, blocks, entries, sealed)
					b.apply(t, damageSpec{site: site, kind: size.kind, bytes: size.bytes})

					assertSurveyInvariants(t, b, b.survey(0, 100), 100)
					// Again under a bound that binds: damage must not let a survey run past what
					// the caller allowed it to read.
					assertSurveyInvariants(t, b, b.survey(0, 2), 2)
				})
			}
		}
	}
}

// TestInspectBlocks_MixedDamage covers damage that is not a single event: a corrupted record in one
// block, a cleared header in another, healthy blocks in between. Interleaving is what tells whether
// the accounting survives repeated failures rather than one.
func TestInspectBlocks_MixedDamage(t *testing.T) {
	cases := []struct {
		name   string
		sealed bool
		specs  []damageSpec
	}{
		{
			name: "a record in one block and a header in another", sealed: true,
			specs: []damageSpec{
				{damageSite{"data_record", "first", "first"}, damageFlip, 1},
				{damageSite{"block_header", "last", ""}, damageZero, 64},
			},
		},
		{
			name: "every block header cleared", sealed: true,
			specs: []damageSpec{
				{damageSite{"block_header", "first", ""}, damageZero, 40},
				{damageSite{"block_header", "middle", ""}, damageZero, 40},
				{damageSite{"block_header", "last", ""}, damageZero, 40},
			},
		},
		{
			name: "a record and the index", sealed: true,
			specs: []damageSpec{
				{damageSite{"data_record", "middle", "middle"}, damageFlip, 1},
				{damageSite{"index_record", "middle", ""}, damageFlip, 1},
			},
		},
		{
			name: "a record in one block and a header in another, active",
			specs: []damageSpec{
				{damageSite{"data_record", "first", "first"}, damageFlip, 1},
				{damageSite{"block_header", "last", ""}, damageZero, 64},
			},
		},
		{
			name: "4KB across a block boundary, active",
			specs: []damageSpec{
				{damageSite{"data_record", "middle", "middle"}, damageFlip, 4096},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newSegment(t, 5, 4, tc.sealed)
			for _, spec := range tc.specs {
				b.apply(t, spec)
			}

			assertSurveyInvariants(t, b, b.survey(0, 100), 100)
			assertSurveyInvariants(t, b, b.survey(0, 2), 2)
		})
	}
}

// TestInspectBlocks_ZeroBlockSealedSegment covers a segment finalized with no entries at all. The
// format documents it as valid -- Finalize writes the header and the footer with nothing between --
// and reporting it as corrupt would leave a segment that can never be fenced or read again.
func TestInspectBlocks_ZeroBlockSealedSegment(t *testing.T) {
	b := &segmentBuilder{}
	b.buf.Write(EncodeRecord(&HeaderRecord{Version: FormatVersion, FirstEntryID: 0}))
	b.seal() // no blocks between the header and the footer

	data := b.buf.Bytes()
	got := InspectBlocks(context.Background(), bytes.NewReader(data), int64(len(data)), 0, 100)

	require.True(t, got.Sealed, "the footer decoded, so the segment is sealed")
	require.Empty(t, got.Blocks)
	require.Equal(t, SurveyStopEnd, got.StopReason, "an empty sealed segment is complete, not broken")
	require.False(t, got.StoppedEarly)
}

// TestInspectBlocks_ActiveTailBetweenTheTwoWrites covers the window the writer leaves open: the block
// header goes out in one write and the block data in the next, so a survey can arrive between them,
// and a crash leaves that state on disk for good. Those entries are not written yet, which is not
// the same as damaged.
func TestInspectBlocks_ActiveTailBetweenTheTwoWrites(t *testing.T) {
	b := newSegment(t, 3, 4, false)
	// Keep the last block's header, drop its data.
	b.truncateTo(int(b.indexes[2].StartOffset) + RecordHeaderSize + BlockHeaderRecordSize)

	data := b.buf.Bytes()
	got := InspectBlocks(context.Background(), bytes.NewReader(data), int64(len(data)), 0, 100)

	require.Len(t, got.Blocks, 3, "the partial block is still worth reporting")
	require.Equal(t, BlockOK, got.Blocks[1].Status)
	require.Equal(t, BlockDataIncomplete, got.Blocks[2].Status,
		"a header whose data has not been written yet is a tail, not corruption")
	require.Equal(t, SurveyStopEnd, got.StopReason)
}

// TestInspectBlocks_SealedDamagedDataIsDamage is the same shape on a sealed segment, where the
// writer is finished: data that fails its checksum is damage, with no question of it arriving later.
func TestInspectBlocks_SealedDamagedDataIsDamage(t *testing.T) {
	b := newSegment(t, 3, 4, true)
	b.corruptAt(int(b.indexes[2].StartOffset) + RecordHeaderSize + BlockHeaderRecordSize + 1)

	got := b.survey(0, 100)

	require.Equal(t, BlockChecksumFailed, got.Blocks[2].Status)
	require.NotEqual(t, BlockDataIncomplete, got.Blocks[2].Status,
		"a finalized segment is not waiting for more data")
}

// TestInspectBlocks_FromBlockIsAPositionOnBothPaths keeps --from-block selecting the same block
// whether the index or the chain located it. The recorded block number may differ from the position
// after a recovery, so the two paths must agree on which one they count.
func TestInspectBlocks_FromBlockIsAPositionOnBothPaths(t *testing.T) {
	sealed := newSegment(t, 5, 2, true)
	active := newSegment(t, 5, 2, false)

	fromIndex := sealed.survey(2, 100)
	fromChain := active.survey(2, 100)

	require.Len(t, fromIndex.Blocks, 3)
	require.Len(t, fromChain.Blocks, 3)
	require.Equal(t, fromChain.Blocks[0].FirstEntryID, fromIndex.Blocks[0].FirstEntryID,
		"--from-block 2 must mean the same block on both paths")
}

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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/zilliztech/woodpecker/common/werr"
)

func init() {
	RegisterRecordCodec(HeaderRecordType, encodeHeader, decodeHeader)
	RegisterRecordCodec(DataRecordType, encodeData, decodeData)
	RegisterRecordCodec(IndexRecordType, encodeIndex, decodeIndex)
	RegisterRecordCodec(FooterRecordType, encodeFooter, decodeFooter)
}

type EncoderFunc func(Record) ([]byte, error)
type DecoderFunc func([]byte) (Record, error)

var encoders = map[byte]EncoderFunc{}
var decoders = map[byte]DecoderFunc{}

func RegisterRecordCodec(
	typ byte,
	encode EncoderFunc,
	decode DecoderFunc) {
	encoders[typ] = encode
	decoders[typ] = decode
}

func EncodeRecord(r Record) ([]byte, error) {
	encodeFn := encoders[r.Type()]
	if encodeFn == nil {
		return nil, werr.ErrCodecNotFound.WithCauseErrMsg(fmt.Sprintf("no encoder for type: %x", r.Type()))
	}

	payload, err := encodeFn(r)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 1+4+len(payload))
	buf[0] = r.Type()
	binary.LittleEndian.PutUint32(buf[1:], uint32(len(payload)))
	copy(buf[5:], payload)

	crc := crc32.ChecksumIEEE(buf)
	final := make([]byte, 4+len(buf))
	binary.LittleEndian.PutUint32(final, crc)
	copy(final[4:], buf)
	return final, nil
}

func DecodeRecord(data []byte) (Record, error) {
	if len(data) < 9 {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("too short")
	}

	expectedCRC := binary.LittleEndian.Uint32(data[:4])
	crc := crc32.ChecksumIEEE(data[4:])
	if expectedCRC != crc {
		return nil, werr.ErrCRCMismatch.WithCauseErrMsg("crc mismatch")
	}

	typ := data[4]
	length := binary.LittleEndian.Uint32(data[5:])
	if int(9+length) > len(data) {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg("truncated payload")
	}

	decoder := decoders[typ]
	if decoder == nil {
		return nil, werr.ErrCodecNotFound.WithCauseErrMsg(fmt.Sprintf("no decoder for type: %x", typ))
	}

	return decoder(data[9 : 9+length])
}

func encodeHeader(r Record) ([]byte, error) {
	h, ok := r.(*HeaderRecord)
	if !ok {
		return nil, werr.ErrUnknownRecord.WithCauseErrMsg(fmt.Sprintf("invalid record type: %T", r))
	}
	buf := make([]byte, 16) // Version(2) + Flags(2) + FirstEntryID(8) + Magic(4)
	binary.LittleEndian.PutUint16(buf[0:], FormatVersion)
	binary.LittleEndian.PutUint16(buf[2:], h.Flags)
	binary.LittleEndian.PutUint64(buf[4:], uint64(h.FirstEntryID))
	copy(buf[12:], HeaderMagic[:])
	return buf, nil
}

func decodeHeader(data []byte) (Record, error) {
	if len(data) != 16 {
		return nil, werr.ErrInvalidRecordSize.WithCauseErrMsg(fmt.Sprintf("invalid header length: %d", len(data)))
	}
	h := &HeaderRecord{}
	h.Version = binary.LittleEndian.Uint16(data[0:])
	if h.Version != FormatVersion {
		return nil, werr.ErrNotSupport.WithCauseErrMsg(fmt.Sprintf("unsupport file format v%d", h.Version))
	}
	h.Flags = binary.LittleEndian.Uint16(data[2:])
	h.FirstEntryID = int64(binary.LittleEndian.Uint64(data[4:]))
	headerMagic := make([]byte, 4)
	copy(headerMagic[:], data[12:])
	if !bytes.Equal(headerMagic, HeaderMagic[:]) {
		return nil, werr.ErrInvalidMagicCode.WithCauseErrMsg("invalid header magic")
	}
	return h, nil
}

func encodeData(r Record) ([]byte, error) {
	d, ok := r.(*DataRecord)
	if !ok {
		return nil, fmt.Errorf("invalid record type: %T", r)
	}
	return d.Payload, nil
}

func decodeData(data []byte) (Record, error) {
	return &DataRecord{Payload: append([]byte(nil), data...)}, nil
}

func encodeIndex(r Record) ([]byte, error) {
	idx, ok := r.(*IndexRecord)
	if !ok {
		return nil, fmt.Errorf("invalid record type: %T", r)
	}
	buf := make([]byte, 4*len(idx.Offsets))
	for i, offset := range idx.Offsets {
		binary.LittleEndian.PutUint32(buf[i*4:], offset)
	}
	return buf, nil
}

func decodeIndex(data []byte) (Record, error) {
	if len(data)%4 != 0 {
		return nil, fmt.Errorf("invalid index payload length: %d", len(data))
	}
	idx := &IndexRecord{}
	count := len(data) / 4
	idx.Offsets = make([]uint32, count)
	for i := 0; i < count; i++ {
		idx.Offsets[i] = binary.LittleEndian.Uint32(data[i*4:])
	}
	return idx, nil
}

func encodeFooter(r Record) ([]byte, error) {
	f, ok := r.(*FooterRecord)
	if !ok {
		return nil, fmt.Errorf("invalid record type: %T", r)
	}
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint64(buf[0:], f.IndexOffset)
	binary.LittleEndian.PutUint32(buf[8:], f.IndexLength)
	binary.LittleEndian.PutUint64(buf[12:], uint64(f.FirstEntryID))
	binary.LittleEndian.PutUint32(buf[20:], f.Count)
	binary.LittleEndian.PutUint16(buf[24:], FormatVersion)
	binary.LittleEndian.PutUint16(buf[26:], f.Flags)
	copy(buf[28:], FooterMagic[:])
	return buf, nil
}

func decodeFooter(data []byte) (Record, error) {
	if len(data) != 32 {
		return nil, fmt.Errorf("invalid footer length: %d", len(data))
	}
	f := &FooterRecord{}
	f.IndexOffset = binary.LittleEndian.Uint64(data[0:])
	f.IndexLength = binary.LittleEndian.Uint32(data[8:])
	f.FirstEntryID = int64(binary.LittleEndian.Uint64(data[12:]))
	f.Count = binary.LittleEndian.Uint32(data[20:])
	f.Version = binary.LittleEndian.Uint16(data[24:])
	if f.Version != FormatVersion {
		return nil, werr.ErrNotSupport.WithCauseErrMsg(fmt.Sprintf("unsupport file format v%d", f.Version))
	}

	f.Flags = binary.LittleEndian.Uint16(data[26:])
	footerMagic := make([]byte, 4)
	copy(footerMagic[:], data[28:])
	if !bytes.Equal(footerMagic, FooterMagic[:]) {
		return nil, werr.ErrInvalidMagicCode.WithCauseErrMsg("invalid footer magic")
	}
	return f, nil
}

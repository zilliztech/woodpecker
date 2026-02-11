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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

var magic = [4]byte{'C', 'K', 'P', 'T'}

const trailerSize = 8 // 4 bytes metaLength + 4 bytes magic

// SectionType identifies a data section inside a Checkpoint.
type SectionType byte

const (
	SectionBlockIndexes SectionType = 1
)

// Meta describes the contents of a checkpoint file.
type Meta struct {
	Version  int       `json:"version"`
	Sections []Section `json:"sections"`
}

// Section describes a single data section inside the checkpoint.
type Section struct {
	Type   SectionType `json:"type"`
	Offset int         `json:"offset"`
	Length int         `json:"length"`
}

// FindSection returns the first section with the given type, or nil.
func (m *Meta) FindSection(sectionType SectionType) *Section {
	for i := range m.Sections {
		if m.Sections[i].Type == sectionType {
			return &m.Sections[i]
		}
	}
	return nil
}

// Checkpoint is a self-describing container that pairs metadata with
// concatenated section data. The binary on-disk format is tail-anchored:
//
//	[section data...][JSON metadata][metaLength (4B LE)][magic "CKPT" (4B)]
type Checkpoint struct {
	Meta Meta
	data []byte // concatenated section data; offsets in Meta.Sections index into this
}

// New creates an empty Checkpoint ready for sections to be added.
func New() *Checkpoint {
	return &Checkpoint{Meta: Meta{Version: 1}}
}

// SetSection adds or replaces a section's data. When replacing an existing
// section the internal data buffer is rebuilt so that all offsets remain
// consistent.
func (c *Checkpoint) SetSection(sectionType SectionType, sectionData []byte) {
	// Collect existing section payloads, skipping the one being replaced.
	type entry struct {
		typ  SectionType
		data []byte
	}
	var entries []entry
	for _, s := range c.Meta.Sections {
		if s.Type != sectionType {
			entries = append(entries, entry{s.Type, c.data[s.Offset : s.Offset+s.Length]})
		}
	}
	entries = append(entries, entry{sectionType, sectionData})

	// Rebuild data buffer and section descriptors.
	c.data = nil
	c.Meta.Sections = nil
	for _, e := range entries {
		c.Meta.Sections = append(c.Meta.Sections, Section{
			Type:   e.typ,
			Offset: len(c.data),
			Length: len(e.data),
		})
		c.data = append(c.data, e.data...)
	}
}

// GetSectionData returns the raw bytes for the given section type.
func (c *Checkpoint) GetSectionData(sectionType SectionType) ([]byte, error) {
	sec := c.Meta.FindSection(sectionType)
	if sec == nil {
		return nil, fmt.Errorf("section type %d not found", sectionType)
	}
	if sec.Offset+sec.Length > len(c.data) {
		return nil, fmt.Errorf("section type %d out of range: offset=%d length=%d dataSize=%d",
			sectionType, sec.Offset, sec.Length, len(c.data))
	}
	return c.data[sec.Offset : sec.Offset+sec.Length], nil
}

// Serialize produces the binary checkpoint format:
//
//	[section data][JSON metadata][metaLength (4B LE)][magic "CKPT" (4B)]
func (c *Checkpoint) Serialize() []byte {
	metaJSON, _ := json.Marshal(c.Meta)

	trailer := make([]byte, trailerSize)
	binary.LittleEndian.PutUint32(trailer[0:4], uint32(len(metaJSON)))
	copy(trailer[4:8], magic[:])

	out := make([]byte, 0, len(c.data)+len(metaJSON)+trailerSize)
	out = append(out, c.data...)
	out = append(out, metaJSON...)
	out = append(out, trailer...)
	return out
}

// Parse deserializes a Checkpoint from its binary format.
func Parse(raw []byte) (*Checkpoint, error) {
	if len(raw) < trailerSize {
		return nil, fmt.Errorf("checkpoint data too small: %d bytes", len(raw))
	}

	trailer := raw[len(raw)-trailerSize:]
	if !bytes.Equal(trailer[4:8], magic[:]) {
		return nil, fmt.Errorf("invalid checkpoint magic")
	}

	metaLen := int(binary.LittleEndian.Uint32(trailer[0:4]))
	dataEnd := len(raw) - trailerSize - metaLen
	if dataEnd < 0 {
		return nil, fmt.Errorf("checkpoint metaLength %d exceeds data size %d", metaLen, len(raw))
	}

	var meta Meta
	if err := json.Unmarshal(raw[dataEnd:len(raw)-trailerSize], &meta); err != nil {
		return nil, fmt.Errorf("failed to parse checkpoint metadata: %w", err)
	}

	data := make([]byte, dataEnd)
	copy(data, raw[:dataEnd])

	return &Checkpoint{Meta: meta, data: data}, nil
}

// IsFormat returns true if data ends with the checkpoint magic bytes.
func IsFormat(data []byte) bool {
	if len(data) < trailerSize {
		return false
	}
	return bytes.Equal(data[len(data)-4:], magic[:])
}

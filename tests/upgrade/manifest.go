// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package upgrade defines the shared, version-agnostic manifest types exchanged
// between the v0.1.13 embed writer and the HEAD verifier in the upgrade
// compatibility test. The file imports only the Go standard library so that it
// compiles unchanged in both the branch-v0.1.13 worktree and HEAD.
package upgrade

import (
	"encoding/json"
	"hash/crc32"
	"io"
	"os"
)

// EntryRecord captures the identity and payload fingerprint of a written entry.
type EntryRecord struct {
	SegmentId  int64  `json:"segmentId"`
	EntryId    int64  `json:"entryId"`
	CRC32      uint32 `json:"crc32"` // CRC32 (IEEE) of the payload bytes
	PayloadLen int    `json:"payloadLen"`
}

// LogRecord describes a single log and all entries written to it during the
// pre-upgrade phase.
type LogRecord struct {
	Name             string        `json:"name"`
	EntryCount       int           `json:"entryCount"`
	PayloadPrefix    string        `json:"payloadPrefix"` // Used to reconstruct expected payloads
	FirstId          *EntryRecord  `json:"firstId"`
	LastId           *EntryRecord  `json:"lastId"`
	Entries          []EntryRecord `json:"entries"`          // Full ordered list of all entries
	SegmentCount     int64         `json:"segmentCount"`     // Distinct segment ids observed
	MaxSeqNoObserved int64         `json:"maxSeqNoObserved"` // Highest segment id observed (>=1 => rolled)
}

// Manifest is the root structure written by the writer. It captures all
// pre-upgrade state required for verification after the upgrade.
type Manifest struct {
	CreatedAt     string      `json:"createdAt"`     // RFC3339 timestamp
	WriterVersion string      `json:"writerVersion"` // e.g., "v0.1.13"
	StorageType   string      `json:"storageType"`   // "local" or "minio"
	RootPath      string      `json:"rootPath"`
	EtcdEndpoints string      `json:"etcdEndpoints"` // Comma-separated
	Logs          []LogRecord `json:"logs"`
	Notes         string      `json:"notes"` // Human-readable summary
}

// ComputePayloadCRC32 computes the CRC32 (IEEE) checksum of a payload byte slice.
func ComputePayloadCRC32(payload []byte) uint32 {
	return crc32.ChecksumIEEE(payload)
}

// LoadManifest reads and unmarshals a manifest JSON file.
func LoadManifest(filePath string) (*Manifest, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// SaveManifest writes a manifest to a JSON file.
func SaveManifest(m *Manifest, filePath string) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, data, 0o600)
}

// WriteManifestTo writes the manifest as formatted JSON to an io.Writer.
func WriteManifestTo(m *Manifest, w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(m)
}

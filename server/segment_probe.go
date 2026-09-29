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

package server

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
)

// How the read ended, in the read path's own words. A node cannot tell "nothing has been written
// yet" from "this copy is damaged": both surface as ErrEntryNotFound, from a missing data.log
// (stagedstorage/reader_impl.go:120), from a block whose data or checksum could not be read
// (:1199, disk/reader_impl.go:783), and from a caught-up tail. So the node reports what it saw and
// what it holds, and the caller -- which has the segment's metadata -- decides what it means.
const (
	probeOutcomeNoLocalData   = "no_local_data"
	probeOutcomeCapReached    = "cap_reached"
	probeOutcomeEntryNotFound = "entry_not_found"
	probeOutcomeEndOfFile     = "end_of_file"
	probeOutcomeError         = "error"
)

// ProbeMaxEntriesDefault bounds a probe that did not ask for a bound, and ProbeMaxEntriesLimit
// bounds one that asked for too much: a diagnostic cannot turn into a full scan of a segment that
// may hold millions of entries, whatever the caller asked for.
const (
	ProbeMaxEntriesDefault = int64(1000)
	ProbeMaxEntriesLimit   = int64(100_000)
)

// probeBatchSize is how much one read asks for. The reader returns what a block holds, so this is
// an upper bound per call rather than a promise.
const probeBatchSize = int64(200)

// SegmentProbeRequest is one node's part of a probe: bucket and root path identify the instance and
// are resolved from this node's own state when the caller does not know them.
type SegmentProbeRequest struct {
	Bucket     string
	RootPath   string
	LogID      int64
	SegmentID  int64
	FromEntry  int64
	MaxEntries int64
}

// SegmentLocalFacts is what this node holds for the segment, read with stat calls alone. These are
// facts rather than conclusions, and they are what separates "nothing was written" from "this copy
// is gone": a segment with no data.log and no compacted mark has no data on this node at all.
type SegmentLocalFacts struct {
	DataLog       bool  `json:"data_log"`
	DataLogBytes  int64 `json:"data_log_bytes"`
	CompactedMark bool  `json:"compacted_mark"`
	DeleteMarked  bool  `json:"delete_marked"`
}

// SegmentProbeResult is what this node holds, what it could serve, and how the read ended.
type SegmentProbeResult struct {
	NodeID      string            `json:"node_id"`
	Bucket      string            `json:"bucket_name"`
	RootPath    string            `json:"root_path"`
	LogID       int64             `json:"log_id"`
	SegmentID   int64             `json:"segment_id"`
	Local       SegmentLocalFacts `json:"local"`
	Source      string            `json:"source"`
	FromEntry   int64             `json:"from_entry"`
	FirstEntry  int64             `json:"first_entry"`
	LastEntry   int64             `json:"last_entry"`
	EntriesRead int64             `json:"entries_read"`
	Outcome     string            `json:"outcome"`
	Error       string            `json:"error,omitempty"`
	ElapsedMs   int64             `json:"elapsed_ms"`
}

// Where the data this node served came from. A segment whose local copy has been reclaimed after
// compaction is served from object storage, which every replica shares: three such answers are one
// copy answering three times, not three independent confirmations.
const (
	probeSourceLocalStaged  = "local_staged"
	probeSourceLocalDisk    = "local_disk"
	probeSourceObjectStore  = "object_storage"
	probeSourceNone         = "none"
	probeSourceUnknownStore = "unknown"
)

// probeBatchReader reads one batch, exactly as the data plane does.
type probeBatchReader func(ctx context.Context, fromEntry, maxEntries int64, state *proto.LastReadState) (*proto.BatchReadResult, error)

// probeRead reads forward from fromEntry until the cap is reached, the data runs out, or a read
// fails, and reports both halves: how far it got, and what stopped it.
func probeRead(ctx context.Context, read probeBatchReader, fromEntry, maxEntries int64) SegmentProbeResult {
	result := SegmentProbeResult{
		FromEntry: fromEntry,
		// Seeded from where the read starts, so "the entry after the last one served" is the
		// position asked for when nothing could be served, not entry zero.
		FirstEntry: -1,
		LastEntry:  fromEntry - 1,
		Outcome:    probeOutcomeEntryNotFound,
	}
	next := fromEntry
	var state *proto.LastReadState
	for result.EntriesRead < maxEntries {
		want := maxEntries - result.EntriesRead
		if want > probeBatchSize {
			want = probeBatchSize
		}
		batch, err := read(ctx, next, want, state)
		if err != nil {
			switch {
			case werr.ErrEntryNotFound.Is(err):
				// Not a verdict: this is also what a damaged block and a missing file look like.
				result.Outcome = probeOutcomeEntryNotFound
				result.Error = err.Error()
			case werr.ErrFileReaderEndOfFile.Is(err):
				result.Outcome = probeOutcomeEndOfFile
			default:
				result.Outcome = probeOutcomeError
				result.Error = err.Error()
			}
			return result
		}
		if batch == nil || len(batch.Entries) == 0 {
			// An answer with no entries and no error: there is nothing more to ask for, and asking
			// again would spin to the cap with nothing to show.
			result.Outcome = probeOutcomeEntryNotFound
			return result
		}
		for _, entry := range batch.Entries {
			if result.FirstEntry < 0 {
				result.FirstEntry = entry.EntryId
			}
			result.LastEntry = entry.EntryId
			result.EntriesRead++
		}
		state = batch.LastReadState
		next = result.LastEntry + 1
	}
	result.Outcome = probeOutcomeCapReached
	return result
}

// ProbeSegment reports what this node holds for a segment and how far it can read it. It answers
// for this node and never asks a peer: the caller assembles the quorum's view.
//
// The local facts are gathered with stat calls before anything is opened, and a node holding
// neither a data.log nor a compacted mark returns there. That is not an optimisation: opening a
// reader creates the segment directory (stagedstorage/reader_impl.go:101, disk:80) before it
// discovers there is nothing to read, and this command resolves an instance by which directories
// exist -- so a probe that opened a reader for a segment the node does not hold would leave behind
// the very evidence the next probe reads.
func (l *logStore) ProbeSegment(ctx context.Context, req SegmentProbeRequest) (*SegmentProbeResult, error) {
	if l.stopped.Load() {
		// Not a statement about the segment: this node cannot answer at all.
		return nil, werr.ErrLogStoreShutdown
	}
	req.MaxEntries = probeBound(req.MaxEntries)
	if req.FromEntry < 0 {
		req.FromEntry = 0
	}
	bucket, rootPath, err := l.resolveProbeInstance(req)
	if err != nil {
		return nil, err
	}

	facts := l.localSegmentFacts(bucket, rootPath, req.LogID, req.SegmentID)
	result := SegmentProbeResult{
		NodeID: l.GetAddress(), Bucket: bucket, RootPath: rootPath,
		LogID: req.LogID, SegmentID: req.SegmentID,
		Local: facts, FromEntry: req.FromEntry,
		FirstEntry: -1, LastEntry: req.FromEntry - 1,
	}
	if source, readable := l.probeSource(facts); !readable {
		result.Source, result.Outcome = source, probeOutcomeNoLocalData
		return &result, nil
	} else {
		result.Source = source
	}

	start := time.Now()
	read := probeRead(ctx, func(ctx context.Context, fromEntry, maxEntries int64, state *proto.LastReadState) (*proto.BatchReadResult, error) {
		return l.GetBatchEntriesAdv(ctx, bucket, rootPath, req.LogID, req.SegmentID, fromEntry, maxEntries, state)
	}, req.FromEntry, req.MaxEntries)

	read.NodeID, read.Bucket, read.RootPath = result.NodeID, result.Bucket, result.RootPath
	read.LogID, read.SegmentID = result.LogID, result.SegmentID
	read.Local, read.Source = result.Local, result.Source
	read.ElapsedMs = time.Since(start).Milliseconds()
	return &read, nil
}

// probeBound keeps a probe within bounds the node sets, whatever the caller asked for.
func probeBound(asked int64) int64 {
	switch {
	case asked <= 0:
		return ProbeMaxEntriesDefault
	case asked > ProbeMaxEntriesLimit:
		return ProbeMaxEntriesLimit
	default:
		return asked
	}
}

// localSegmentFacts reads what this node holds for the segment with stat calls alone: nothing here
// creates, opens or caches anything.
func (l *logStore) localSegmentFacts(bucket, rootPath string, logID, segmentID int64) SegmentLocalFacts {
	if !l.cfg.Woodpecker.Storage.IsStorageService() && !l.cfg.Woodpecker.Storage.IsStorageLocal() {
		return SegmentLocalFacts{} // no local segment data in pure object storage
	}
	segmentDir := l.localSegmentDir(bucket, rootPath, logID, segmentID)
	facts := SegmentLocalFacts{
		CompactedMark: hasCompactedMark(segmentDir),
		DeleteMarked:  l.hasDeleteMarker(bucket, rootPath, logID),
	}
	if info, err := os.Stat(filepath.Join(segmentDir, "data.log")); err == nil {
		facts.DataLog, facts.DataLogBytes = true, info.Size()
	}
	return facts
}

// hasDeleteMarker reports whether this node has been told the log, or its whole instance, is
// deleted. A segment with no data under a deleted log is expected rather than damaged.
func (l *logStore) hasDeleteMarker(bucket, rootPath string, logID int64) bool {
	root := l.cfg.Woodpecker.Storage.RootPath
	for _, m := range []deleteMarker{
		{Bucket: bucket, RootPath: rootPath, LogId: logID},
		{Bucket: bucket, RootPath: rootPath, Instance: true},
	} {
		if _, err := os.Stat(markerPath(root, m)); err == nil {
			return true
		}
	}
	return false
}

// resolveProbeInstance answers which instance the caller means. A caller that knows says so; one
// that does not gets the instance this node associates with the segment, and an answer that could
// be more than one instance is reported rather than picked -- answering for the wrong tenant is
// worse than answering nothing.
func (l *logStore) resolveProbeInstance(req SegmentProbeRequest) (string, string, error) {
	if req.Bucket != "" && req.RootPath != "" {
		return req.Bucket, req.RootPath, nil
	}
	if req.Bucket != "" || req.RootPath != "" {
		return "", "", werr.ErrInvalidMessage.WithCauseErrMsg(
			"bucket_name and root_path identify an instance together; one without the other does not")
	}
	candidates := l.instancesOfLiveProcessors(req.LogID, req.SegmentID)
	if len(candidates) == 0 {
		candidates = l.localInstancesHoldingSegment(req.LogID, req.SegmentID)
	}
	switch len(candidates) {
	case 1:
		return candidates[0].bucket, candidates[0].rootPath, nil
	case 0:
		return "", "", werr.ErrSegmentNotFound.WithCauseErrMsg(fmt.Sprintf(
			"this node holds no data for log %d segment %d and serves no writer for it; name the instance with bucket_name and root_path if it should be here",
			req.LogID, req.SegmentID))
	default:
		named := make([]string, 0, len(candidates))
		for _, in := range candidates {
			named = append(named, GetInstanceKey(in.bucket, in.rootPath))
		}
		return "", "", werr.ErrInvalidMessage.WithCauseErrMsg(fmt.Sprintf(
			"log %d segment %d is held by %d instances on this node (%s); pass bucket_name and root_path",
			req.LogID, req.SegmentID, len(candidates), strings.Join(named, ", ")))
	}
}

// instancesOfLiveProcessors returns every instance this node holds a segment processor for. Map
// order is random, so returning the first match would answer for a different tenant from one probe
// to the next.
func (l *logStore) instancesOfLiveProcessors(logID, segmentID int64) []probeInstance {
	l.spMu.RLock()
	defer l.spMu.RUnlock()

	suffix := "/" + strconv.FormatInt(logID, 10)
	found := make([]probeInstance, 0, 1)
	for logKey, segMap := range l.segmentProcessors {
		if !strings.HasSuffix(logKey, suffix) {
			continue
		}
		sp, ok := segMap[segmentID]
		if !ok || sp.GetLogId() != logID {
			continue
		}
		// logKey is GetLogKey(bucket, rootPath, logId): the instance key with the log id appended.
		instanceKey := strings.TrimSuffix(logKey, suffix)
		bucket, rootPath, _ := strings.Cut(instanceKey, "/")
		found = append(found, probeInstance{bucket: bucket, rootPath: rootPath})
	}
	sortProbeInstances(found)
	return found
}

type probeInstance struct {
	bucket   string
	rootPath string
}

// localInstancesHoldingSegment finds the instances whose local tree holds this segment's directory.
// The layout is the staged writer's: <storage root>/<bucket>/<instance root>/<logId>/<segId> in
// service mode, and without the bucket component in local mode.
func (l *logStore) localInstancesHoldingSegment(logID, segmentID int64) []probeInstance {
	root := l.cfg.Woodpecker.Storage.RootPath
	if root == "" {
		return nil
	}
	var bucketDepth int
	switch {
	case l.cfg.Woodpecker.Storage.IsStorageService():
		bucketDepth = 1
	case l.cfg.Woodpecker.Storage.IsStorageLocal():
		bucketDepth = 0
	default:
		return nil // pure object storage keeps no local segment data
	}

	logDir := strconv.FormatInt(logID, 10)
	segDir := strconv.FormatInt(segmentID, 10)
	markerRoot := filepath.Join(root, deleteMarkerDir)
	found := make([]probeInstance, 0, 1)

	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || !d.IsDir() {
			return nil //nolint:nilerr // an unreadable subtree is not an answer about the segment
		}
		if p == markerRoot {
			return filepath.SkipDir
		}
		if d.Name() != segDir || filepath.Base(filepath.Dir(p)) != logDir {
			return nil
		}
		rel, relErr := filepath.Rel(root, filepath.Dir(filepath.Dir(p)))
		if relErr != nil || rel == "." {
			return filepath.SkipDir
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) < bucketDepth+1 {
			return filepath.SkipDir
		}
		found = append(found, probeInstance{
			bucket:   strings.Join(parts[:bucketDepth], "/"),
			rootPath: strings.Join(parts[bucketDepth:], "/"),
		})
		return filepath.SkipDir
	})
	sortProbeInstances(found)
	return found
}

func sortProbeInstances(in []probeInstance) {
	sort.Slice(in, func(i, j int) bool {
		if in[i].bucket != in[j].bucket {
			return in[i].bucket < in[j].bucket
		}
		return in[i].rootPath < in[j].rootPath
	})
}

// probeSource names where a read would be served from, and whether there is anything to read at
// all. A segment whose local copy was reclaimed after compaction is served from the object-storage
// copy every replica shares, so agreement between replicas reading it is one copy answering
// repeatedly. With neither a local copy nor a compacted mark, the staged reader would only report
// that it found nothing (stagedstorage/reader_impl.go:120) -- there is nothing to open.
func (l *logStore) probeSource(facts SegmentLocalFacts) (string, bool) {
	switch {
	case l.cfg.Woodpecker.Storage.IsStorageService():
		switch {
		case facts.DataLog:
			return probeSourceLocalStaged, true
		case facts.CompactedMark:
			return probeSourceObjectStore, true
		default:
			return probeSourceNone, false
		}
	case l.cfg.Woodpecker.Storage.IsStorageLocal():
		if !facts.DataLog {
			return probeSourceNone, false
		}
		return probeSourceLocalDisk, true
	case l.cfg.Woodpecker.Storage.IsStorageMinio():
		return probeSourceObjectStore, true
	default:
		return probeSourceUnknownStore, true
	}
}

// localSegmentDir is the node-local directory a segment's staged data lives in.
func (l *logStore) localSegmentDir(bucket, rootPath string, logID, segmentID int64) string {
	base := l.cfg.Woodpecker.Storage.RootPath
	if l.cfg.Woodpecker.Storage.IsStorageService() {
		base = filepath.Join(base, bucket, rootPath)
	} else {
		base = filepath.Join(base, rootPath)
	}
	return filepath.Join(base, strconv.FormatInt(logID, 10), strconv.FormatInt(segmentID, 10))
}

func dataLogExists(segmentDir string) bool {
	info, err := os.Stat(filepath.Join(segmentDir, "data.log"))
	return err == nil && info.Size() > 0
}

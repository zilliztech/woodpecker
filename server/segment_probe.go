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

// Why a bounded read stopped. The three that are not failures are separate answers: a reader
// waiting at a position it cannot read is a fault, a reader waiting for data nobody has written is
// not, and a reader at the end of a sealed segment is neither.
const (
	probeStopCapReached    = "cap_reached"
	probeStopNotYetWritten = "not_yet_written"
	probeStopEndOfSegment  = "end_of_segment"
	probeStopError         = "error"
)

// ProbeMaxEntriesDefault bounds a probe that did not ask for a bound, so a diagnostic cannot turn
// into a full scan of a segment that may hold millions of entries.
const ProbeMaxEntriesDefault = int64(1000)

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

// SegmentProbeResult is what this node can serve, and why it stopped.
type SegmentProbeResult struct {
	NodeID      string `json:"node_id"`
	Bucket      string `json:"bucket_name"`
	RootPath    string `json:"root_path"`
	LogID       int64  `json:"log_id"`
	SegmentID   int64  `json:"segment_id"`
	Source      string `json:"source"`
	FromEntry   int64  `json:"from_entry"`
	FirstEntry  int64  `json:"first_entry"`
	LastEntry   int64  `json:"last_entry"`
	EntriesRead int64  `json:"entries_read"`
	StopReason  string `json:"stop_reason"`
	Error       string `json:"error,omitempty"`
	ElapsedMs   int64  `json:"elapsed_ms"`
}

// Where the data this node served came from. A segment whose local copy has been reclaimed after
// compaction is served from object storage, which every replica shares: three such answers are one
// copy answering three times, not three independent confirmations.
const (
	probeSourceLocalStaged  = "local_staged"
	probeSourceLocalDisk    = "local_disk"
	probeSourceObjectStore  = "object_storage"
	probeSourceUnknownStore = "unknown"
)

// probeBatchReader reads one batch, exactly as the data plane does.
type probeBatchReader func(ctx context.Context, fromEntry, maxEntries int64, state *proto.LastReadState) (*proto.BatchReadResult, error)

// probeRead reads forward from fromEntry until the cap is reached, the data runs out, or a read
// fails, and reports both halves: how far it got, and what stopped it.
func probeRead(ctx context.Context, read probeBatchReader, fromEntry, maxEntries int64) SegmentProbeResult {
	result := SegmentProbeResult{
		FromEntry:  fromEntry,
		FirstEntry: -1,
		LastEntry:  -1,
		StopReason: probeStopNotYetWritten,
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
				result.StopReason = probeStopNotYetWritten
			case werr.ErrFileReaderEndOfFile.Is(err):
				result.StopReason = probeStopEndOfSegment
			default:
				result.StopReason = probeStopError
				result.Error = err.Error()
			}
			return result
		}
		if batch == nil || len(batch.Entries) == 0 {
			// An answer with no entries and no error: there is nothing more to ask for, and asking
			// again would spin to the cap with nothing to show.
			result.StopReason = probeStopNotYetWritten
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
	result.StopReason = probeStopCapReached
	return result
}

// ProbeSegment attempts a bounded read of one segment on this node alone. It answers for this node
// and never asks a peer: the caller assembles the quorum's view.
func (l *logStore) ProbeSegment(ctx context.Context, req SegmentProbeRequest) (*SegmentProbeResult, error) {
	if req.MaxEntries <= 0 {
		req.MaxEntries = ProbeMaxEntriesDefault
	}
	if req.FromEntry < 0 {
		req.FromEntry = 0
	}
	bucket, rootPath, err := l.resolveProbeInstance(req)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	result := probeRead(ctx, func(ctx context.Context, fromEntry, maxEntries int64, state *proto.LastReadState) (*proto.BatchReadResult, error) {
		return l.GetBatchEntriesAdv(ctx, bucket, rootPath, req.LogID, req.SegmentID, fromEntry, maxEntries, state)
	}, req.FromEntry, req.MaxEntries)

	result.ElapsedMs = time.Since(start).Milliseconds()
	result.NodeID = l.GetAddress()
	result.Bucket, result.RootPath = bucket, rootPath
	result.LogID, result.SegmentID = req.LogID, req.SegmentID
	result.Source = l.probeSource(bucket, rootPath, req.LogID, req.SegmentID)
	return &result, nil
}

// resolveProbeInstance answers which instance the caller means. A caller that knows says so; one
// that does not gets the instance this node already associates with the segment, and an ambiguous
// answer is reported rather than picked.
func (l *logStore) resolveProbeInstance(req SegmentProbeRequest) (string, string, error) {
	if req.Bucket != "" {
		return req.Bucket, req.RootPath, nil
	}
	if req.RootPath != "" {
		return "", "", werr.ErrInvalidMessage.WithCauseErrMsg(
			"root_path without bucket_name does not identify an instance")
	}
	if bucket, rootPath, found := l.instanceOfLiveProcessor(req.LogID, req.SegmentID); found {
		return bucket, rootPath, nil
	}
	instances := l.localInstancesHoldingSegment(req.LogID, req.SegmentID)
	switch len(instances) {
	case 1:
		return instances[0].bucket, instances[0].rootPath, nil
	case 0:
		return "", "", werr.ErrSegmentNotFound.WithCauseErrMsg(fmt.Sprintf(
			"this node holds no local data for log %d segment %d and serves no writer for it; pass bucket_name and root_path to probe object storage",
			req.LogID, req.SegmentID))
	default:
		named := make([]string, 0, len(instances))
		for _, in := range instances {
			named = append(named, GetInstanceKey(in.bucket, in.rootPath))
		}
		return "", "", werr.ErrInvalidMessage.WithCauseErrMsg(fmt.Sprintf(
			"log %d segment %d is held by %d instances on this node (%s); pass bucket_name and root_path",
			req.LogID, req.SegmentID, len(instances), strings.Join(named, ", ")))
	}
}

// instanceOfLiveProcessor returns the instance of a segment processor this node already holds.
func (l *logStore) instanceOfLiveProcessor(logID, segmentID int64) (string, string, bool) {
	l.spMu.RLock()
	defer l.spMu.RUnlock()

	suffix := "/" + strconv.FormatInt(logID, 10)
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
		return bucket, rootPath, true
	}
	return "", "", false
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
	sort.Slice(found, func(i, j int) bool {
		if found[i].bucket != found[j].bucket {
			return found[i].bucket < found[j].bucket
		}
		return found[i].rootPath < found[j].rootPath
	})
	return found
}

// probeSource names where the served data came from, which decides whether this node's answer is
// its own or the shared one every replica reads.
func (l *logStore) probeSource(bucket, rootPath string, logID, segmentID int64) string {
	switch {
	case l.cfg.Woodpecker.Storage.IsStorageService():
		if dataLogExists(l.localSegmentDir(bucket, rootPath, logID, segmentID)) {
			return probeSourceLocalStaged
		}
		return probeSourceObjectStore
	case l.cfg.Woodpecker.Storage.IsStorageLocal():
		return probeSourceLocalDisk
	case l.cfg.Woodpecker.Storage.IsStorageMinio():
		return probeSourceObjectStore
	default:
		return probeSourceUnknownStore
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

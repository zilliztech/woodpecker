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
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/metrics"
)

// Delete states reported per instance by the local-data inventory.
const (
	deleteStateLog      = "log"
	deleteStateInstance = "instance"
)

// segmentDataFileName is the per-segment WAL file on local disk.
const segmentDataFileName = "data.log"

// isLiveSegmentData reports whether a segment's data.log still genuinely occupies the
// node: non-empty, and not already superseded by a durable compacted copy in object
// storage. A compacted-marked file is scheduled for physical GC and must not be counted
// as data that still has to be drained.
//
// HasLocalSegmentData and the local-data inventory share this predicate deliberately —
// the decommission gate and the inventory an operator reads must never disagree about
// what "this node still holds data" means.
func isLiveSegmentData(segmentDir string, info fs.FileInfo) bool {
	return info.Size() > 0 && !hasCompactedMark(segmentDir)
}

// InstanceDataEntry is one instance's node-local data footprint.
type InstanceDataEntry struct {
	BucketName string `json:"bucket_name"`
	RootPath   string `json:"root_path"`
	LogCount   int    `json:"log_count"`
	// SegmentCount counts every segment directory holding a data.log, including those
	// already marked compacted; LiveSegmentCount counts only the subset that
	// HasLocalSegmentData would treat as not yet drained.
	SegmentCount     int   `json:"segment_count"`
	LiveSegmentCount int   `json:"live_segment_count"`
	SizeBytes        int64 `json:"size_bytes"`
	LastModifiedMS   int64 `json:"last_modified_ms"`
	// ActiveProcessors is the number of segment processors this node currently holds
	// for the instance. Together with LastModifiedMS it is what separates a genuine
	// orphan from an instance that was just created.
	ActiveProcessors int `json:"active_processors"`
	// DeleteState is "" (not marked), "log" (some logs marked) or "instance".
	DeleteState       string `json:"delete_state"`
	MarkedDeletedAtMS int64  `json:"marked_deleted_at_ms"`
}

// scanLocalInstances walks the storage root once and attributes every segment data.log
// to the (bucket, rootPath) instance that owns it, keyed by GetInstanceKey. The second
// return value counts paths that could not be read: while it is non-zero, an instance
// missing from the result does not prove the instance has no data here.
//
// Identity is derived from the path tail (<logId>/<segId>/data.log) rather than from a
// fixed directory depth, so a rootPath containing separators is still attributed to one
// instance instead of being split into several.
func scanLocalInstances(cfg *config.Configuration) (map[string]*InstanceDataEntry, int) {
	entries := make(map[string]*InstanceDataEntry)
	root := cfg.Woodpecker.Storage.RootPath
	if root == "" {
		return entries, 0
	}
	// bucketDepth is how many leading path components carry the bucket name.
	var bucketDepth int
	switch {
	case cfg.Woodpecker.Storage.IsStorageService():
		bucketDepth = 1
	case cfg.Woodpecker.Storage.IsStorageLocal():
		bucketDepth = 0
	default:
		return entries, 0 // pure object storage keeps no local segment data
	}

	scanErrors := 0
	logsSeen := make(map[string]map[int64]struct{})
	markerRoot := filepath.Join(root, deleteMarkerDir)

	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if p == root && os.IsNotExist(walkErr) {
				return filepath.SkipAll // nothing written yet: clean, not unreadable
			}
			scanErrors++
			return nil
		}
		if d.IsDir() {
			// Skip ONLY the top-level marker dir, not a user dir deeper under root
			// that happens to be named ".deleted" — same rule as HasLocalSegmentData.
			if p == markerRoot {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Name() != segmentDataFileName {
			return nil
		}
		rel, relErr := filepath.Rel(root, p)
		if relErr != nil {
			scanErrors++
			return nil
		}
		// <bucket?>/<rootPath...>/<logId>/<segId>/data.log
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) < bucketDepth+4 {
			return nil
		}
		logIdx, segIdx := len(parts)-3, len(parts)-2
		logId, logErr := strconv.ParseInt(parts[logIdx], 10, 64)
		if logErr != nil {
			return nil // not a segment tree
		}
		if _, segErr := strconv.ParseInt(parts[segIdx], 10, 64); segErr != nil {
			return nil
		}
		bucket := ""
		if bucketDepth == 1 {
			bucket = parts[0]
		}
		rootPath := strings.Join(parts[bucketDepth:logIdx], "/")
		if rootPath == "" {
			return nil
		}
		info, infoErr := d.Info()
		if infoErr != nil {
			scanErrors++
			return nil
		}

		key := GetInstanceKey(bucket, rootPath)
		entry := entries[key]
		if entry == nil {
			entry = &InstanceDataEntry{BucketName: bucket, RootPath: rootPath}
			entries[key] = entry
			logsSeen[key] = make(map[int64]struct{})
		}
		entry.SegmentCount++
		entry.SizeBytes += info.Size()
		if isLiveSegmentData(filepath.Dir(p), info) {
			entry.LiveSegmentCount++
		}
		if ms := info.ModTime().UnixMilli(); ms > entry.LastModifiedMS {
			entry.LastModifiedMS = ms
		}
		if _, seen := logsSeen[key][logId]; !seen {
			logsSeen[key][logId] = struct{}{}
			entry.LogCount++
		}
		return nil
	})
	return entries, scanErrors
}

// InstanceDataReport is the node-local payload of GET /admin/instance/data.
//
// The shape follows /admin/log-health, the other node-level filterable read: node id,
// the filter that actually took effect, a timestamp, the node-level config that
// explains the result, aggregate counters, then the detail list.
type InstanceDataReport struct {
	NodeID      string `json:"node_id"`
	StorageMode string `json:"storage_mode"`
	StorageRoot string `json:"storage_root"`
	// FilterBucketName/FilterRootPath echo the filter that took effect, so a caller
	// that misspelled a param can tell it received an unfiltered answer instead of
	// mistaking it for one instance's data.
	FilterBucketName string `json:"filter_bucket_name,omitempty"`
	FilterRootPath   string `json:"filter_root_path,omitempty"`
	TimestampMS      int64  `json:"timestamp_ms"`
	InstanceCount    int    `json:"instance_count"`
	TotalSizeBytes   int64  `json:"total_size_bytes"`
	// ScanErrors counts paths that could not be read. While it is non-zero, an
	// instance missing from Instances does NOT prove the instance has no data here.
	ScanErrors int                 `json:"scan_errors"`
	Instances  []InstanceDataEntry `json:"instances"`
}

// LocalInstanceData reports the instances holding node-local data, optionally filtered
// to one (bucketName, rootPath). Empty strings mean "all instances".
func (l *logStore) LocalInstanceData(bucketName string, rootPath string) *InstanceDataReport {
	return l.localInstanceData(bucketName, rootPath, metrics.NodeID, time.Now())
}

// localInstanceData is LocalInstanceData with the node id and clock injected.
//
// The instance list is driven by what is on disk. An instance that is being served but
// has not flushed a byte yet is deliberately absent: the caller only ever deletes what
// the inventory shows it, so an instance it cannot see is one it cannot delete.
func (l *logStore) localInstanceData(bucketName string, rootPath string, nodeID string, now time.Time) *InstanceDataReport {
	report := &InstanceDataReport{
		NodeID:      nodeID,
		StorageMode: l.cfg.Woodpecker.Storage.Type,
		StorageRoot: l.cfg.Woodpecker.Storage.RootPath,
		TimestampMS: now.UnixMilli(),
		Instances:   []InstanceDataEntry{}, // never null: an empty list is a real answer
	}
	if bucketName != "" && rootPath != "" {
		report.FilterBucketName = bucketName
		report.FilterRootPath = rootPath
	}

	entries, scanErrors := scanLocalInstances(l.cfg)
	report.ScanErrors = scanErrors
	l.annotateActiveProcessors(entries)
	report.ScanErrors += annotateDeleteMarkers(l.ctx, l.cfg.Woodpecker.Storage.RootPath, entries)

	for _, entry := range entries {
		if report.FilterBucketName != "" &&
			(entry.BucketName != bucketName || entry.RootPath != rootPath) {
			continue
		}
		report.Instances = append(report.Instances, *entry)
		report.TotalSizeBytes += entry.SizeBytes
	}
	sort.Slice(report.Instances, func(i, j int) bool {
		if report.Instances[i].BucketName != report.Instances[j].BucketName {
			return report.Instances[i].BucketName < report.Instances[j].BucketName
		}
		return report.Instances[i].RootPath < report.Instances[j].RootPath
	})
	report.InstanceCount = len(report.Instances)
	return report
}

// annotateActiveProcessors fills ActiveProcessors from the in-memory processor map.
// The instance key is the log key minus its trailing logId, which stays exact even when
// a rootPath contains separators.
func (l *logStore) annotateActiveProcessors(entries map[string]*InstanceDataEntry) {
	l.spMu.RLock()
	defer l.spMu.RUnlock()
	for logKey, procs := range l.segmentProcessors {
		sep := strings.LastIndex(logKey, "/")
		if sep < 0 {
			continue
		}
		if entry := entries[logKey[:sep]]; entry != nil {
			entry.ActiveProcessors += len(procs)
		}
	}
}

// annotateDeleteMarkers fills DeleteState/MarkedDeletedAtMS from the durable markers,
// and returns the number of scan errors it hit. An instance-level mark outranks any
// per-log mark; among per-log marks the oldest wins, since that is the one that has
// been waiting to be reclaimed the longest.
func annotateDeleteMarkers(ctx context.Context, root string, entries map[string]*InstanceDataEntry) int {
	if root == "" || len(entries) == 0 {
		return 0
	}
	markers, err := scanDeleteMarkers(ctx, root)
	if err != nil {
		return 1
	}
	for _, m := range markers {
		entry := entries[GetInstanceKey(m.Bucket, m.RootPath)]
		if entry == nil {
			continue // a mark for data this node does not hold belongs in no report
		}
		markedAtMS := m.DeletedAt * 1000
		if m.Instance {
			entry.DeleteState = deleteStateInstance
			entry.MarkedDeletedAtMS = markedAtMS
			continue
		}
		if entry.DeleteState == deleteStateInstance {
			continue
		}
		entry.DeleteState = deleteStateLog
		if entry.MarkedDeletedAtMS == 0 || markedAtMS < entry.MarkedDeletedAtMS {
			entry.MarkedDeletedAtMS = markedAtMS
		}
	}
	return 0
}

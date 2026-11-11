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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/server/processor"
)

// writeSegmentData creates <dir>/data.log with the given contents.
func writeSegmentData(t *testing.T, dir string, contents string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.log"), []byte(contents), 0o644))
}

func serviceModeCfg(t *testing.T) *config.Configuration {
	t.Helper()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	cfg.Woodpecker.Storage.Type = "service"
	cfg.Woodpecker.Storage.RootPath = t.TempDir()
	return cfg
}

// TestScanLocalInstances_ServiceModeCountsLogsSegmentsAndBytes verifies that a
// service-mode tree is attributed to the right (bucket, rootPath) instance and that
// logs, segments and bytes are counted across the whole instance.
func TestScanLocalInstances_ServiceModeCountsLogsSegmentsAndBytes(t *testing.T) {
	t.Parallel()

	cfg := serviceModeCfg(t)
	writeSegmentData(t, localSegmentDataDir(cfg, "bkt", "inst-a", 1, 0), "aaaa")   // 4 bytes
	writeSegmentData(t, localSegmentDataDir(cfg, "bkt", "inst-a", 1, 1), "bb")     // 2 bytes
	writeSegmentData(t, localSegmentDataDir(cfg, "bkt", "inst-a", 2, 0), "cccccc") // 6 bytes
	writeSegmentData(t, localSegmentDataDir(cfg, "bkt", "inst-b", 9, 0), "d")      // 1 byte

	entries, scanErrors := scanLocalInstances(cfg)

	require.Zero(t, scanErrors)
	require.Len(t, entries, 2)

	a := entries[GetInstanceKey("bkt", "inst-a")]
	require.NotNil(t, a)
	require.Equal(t, "bkt", a.BucketName)
	require.Equal(t, "inst-a", a.RootPath)
	require.Equal(t, 2, a.LogCount)
	require.Equal(t, 3, a.SegmentCount)
	require.Equal(t, 3, a.LiveSegmentCount)
	require.Equal(t, int64(12), a.SizeBytes)

	b := entries[GetInstanceKey("bkt", "inst-b")]
	require.NotNil(t, b)
	require.Equal(t, 1, b.LogCount)
	require.Equal(t, int64(1), b.SizeBytes)
}

// TestScanLocalInstances_CompactedSegmentCountsButIsNotLive pins the distinction the
// decommission gate relies on: a data.log whose data is already durable in object
// storage is still on disk, but it is not data the node still has to drain.
func TestScanLocalInstances_CompactedSegmentCountsButIsNotLive(t *testing.T) {
	t.Parallel()

	cfg := serviceModeCfg(t)
	compacted := localSegmentDataDir(cfg, "bkt", "inst", 1, 0)
	writeSegmentData(t, compacted, "already-in-object-storage")
	require.NoError(t, writeCompactedMark(t.Context(), compacted))
	writeSegmentData(t, localSegmentDataDir(cfg, "bkt", "inst", 1, 1), "still-here")

	entries, scanErrors := scanLocalInstances(cfg)

	require.Zero(t, scanErrors)
	e := entries[GetInstanceKey("bkt", "inst")]
	require.NotNil(t, e)
	require.Equal(t, 2, e.SegmentCount)
	require.Equal(t, 1, e.LiveSegmentCount)
}

// TestScanLocalInstances_EmptyDataLogIsNotLive mirrors HasLocalSegmentData, which
// treats a zero-byte data.log as no data at all.
func TestScanLocalInstances_EmptyDataLogIsNotLive(t *testing.T) {
	t.Parallel()

	cfg := serviceModeCfg(t)
	writeSegmentData(t, localSegmentDataDir(cfg, "bkt", "inst", 1, 0), "")

	entries, _ := scanLocalInstances(cfg)

	e := entries[GetInstanceKey("bkt", "inst")]
	require.NotNil(t, e)
	require.Equal(t, 1, e.SegmentCount)
	require.Zero(t, e.LiveSegmentCount)
}

// TestScanLocalInstances_SkipsTopLevelDeleteMarkerDir keeps the marker tree from being
// reported as an instance of its own. Without the skip, ".deleted" would be read as a
// bucket name.
func TestScanLocalInstances_SkipsTopLevelDeleteMarkerDir(t *testing.T) {
	t.Parallel()

	cfg := serviceModeCfg(t)
	root := cfg.Woodpecker.Storage.RootPath
	writeSegmentData(t, filepath.Join(root, deleteMarkerDir, "bkt", "inst", "1", "0"), "marker-tree")
	writeSegmentData(t, localSegmentDataDir(cfg, "bkt", "inst", 1, 0), "real-data")

	entries, scanErrors := scanLocalInstances(cfg)

	require.Zero(t, scanErrors)
	require.Len(t, entries, 1)
	require.NotNil(t, entries[GetInstanceKey("bkt", "inst")])
}

// TestScanLocalInstances_LocalModeHasNoBucketLevel covers the local-storage layout,
// which is one directory shallower than service mode.
func TestScanLocalInstances_LocalModeHasNoBucketLevel(t *testing.T) {
	t.Parallel()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	cfg.Woodpecker.Storage.Type = "local"
	cfg.Woodpecker.Storage.RootPath = t.TempDir()
	writeSegmentData(t, localSegmentDataDir(cfg, "ignored", "inst", 3, 0), "data")

	entries, _ := scanLocalInstances(cfg)

	e := entries[GetInstanceKey("", "inst")]
	require.NotNil(t, e)
	require.Empty(t, e.BucketName)
	require.Equal(t, "inst", e.RootPath)
	require.Equal(t, 1, e.SegmentCount)
}

// TestScanLocalInstances_MinioModeReportsNothing: pure object storage keeps no local
// segment data, so an empty result means "not applicable", not "clean".
func TestScanLocalInstances_MinioModeReportsNothing(t *testing.T) {
	t.Parallel()

	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	cfg.Woodpecker.Storage.Type = "minio"
	cfg.Woodpecker.Storage.RootPath = t.TempDir()
	writeSegmentData(t, filepath.Join(cfg.Woodpecker.Storage.RootPath, "bkt", "inst", "1", "0"), "data")

	entries, scanErrors := scanLocalInstances(cfg)

	require.Empty(t, entries)
	require.Zero(t, scanErrors)
}

// TestScanLocalInstances_NonSegmentTreesAreIgnored: a data.log that is not sitting at
// <logId>/<segId>/ is not segment data and must not invent an instance.
func TestScanLocalInstances_NonSegmentTreesAreIgnored(t *testing.T) {
	t.Parallel()

	cfg := serviceModeCfg(t)
	root := cfg.Woodpecker.Storage.RootPath
	writeSegmentData(t, filepath.Join(root, "bkt", "inst", "not-a-log-id", "0"), "x")
	writeSegmentData(t, filepath.Join(root, "bkt", "inst", "1", "not-a-seg-id"), "x")
	writeSegmentData(t, filepath.Join(root, "bkt"), "x") // too shallow

	entries, scanErrors := scanLocalInstances(cfg)

	require.Empty(t, entries)
	require.Zero(t, scanErrors)
}

// TestScanLocalInstances_RootPathWithSeparatorStaysOneInstance: identity comes from the
// path tail, so a multi-segment rootPath is not split into several instances.
func TestScanLocalInstances_RootPathWithSeparatorStaysOneInstance(t *testing.T) {
	t.Parallel()

	cfg := serviceModeCfg(t)
	writeSegmentData(t, localSegmentDataDir(cfg, "bkt", "by-dev/inst", 1, 0), "data")

	entries, _ := scanLocalInstances(cfg)

	require.Len(t, entries, 1)
	e := entries[GetInstanceKey("bkt", "by-dev/inst")]
	require.NotNil(t, e)
	require.Equal(t, "by-dev/inst", e.RootPath)
}

// TestScanLocalInstances_UnreadableDirIsCountedAsScanError: an unreadable directory
// must surface, otherwise the caller would read "instance absent" as "instance clean"
// and delete on a blind spot.
func TestScanLocalInstances_UnreadableDirIsCountedAsScanError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not restrict the walk")
	}
	t.Parallel()

	cfg := serviceModeCfg(t)
	blocked := filepath.Join(cfg.Woodpecker.Storage.RootPath, "bkt", "inst")
	writeSegmentData(t, filepath.Join(blocked, "1", "0"), "data")
	require.NoError(t, os.Chmod(blocked, 0o000))
	t.Cleanup(func() { _ = os.Chmod(blocked, 0o755) })

	_, scanErrors := scanLocalInstances(cfg)

	require.Positive(t, scanErrors)
}

// storeWithRoot returns a test logStore rooted at a fresh service-mode temp dir.
func storeWithRoot(t *testing.T) *logStore {
	t.Helper()
	store := createTestLogStore()
	store.cfg.Woodpecker.Storage.Type = "service"
	store.cfg.Woodpecker.Storage.RootPath = t.TempDir()
	return store
}

// TestLocalInstanceData_MergesDiskProcessorsAndInstanceMarker is the whole point of
// the endpoint: on-disk residue alone cannot tell an orphan from a live instance, so
// the report has to carry the in-memory processor count and the delete marker too.
func TestLocalInstanceData_MergesDiskProcessorsAndInstanceMarker(t *testing.T) {
	t.Parallel()

	store := storeWithRoot(t)
	root := store.cfg.Woodpecker.Storage.RootPath
	writeSegmentData(t, localSegmentDataDir(store.cfg, "bkt", "live", 1, 0), "live-data")
	writeSegmentData(t, localSegmentDataDir(store.cfg, "bkt", "orphan", 7, 0), "orphan-data")

	// "live" has two processors across two logs; "orphan" has none.
	store.segmentProcessors[GetLogKey("bkt", "live", 1)] = map[int64]processor.SegmentProcessor{0: nil, 1: nil}
	store.segmentProcessors[GetLogKey("bkt", "live", 2)] = map[int64]processor.SegmentProcessor{0: nil}

	marker := deleteMarker{Bucket: "bkt", RootPath: "orphan", Instance: true, DeletedAt: 1700000000}
	require.NoError(t, writeDeleteMarker(t.Context(), root, marker))
	store.deletingInstances[GetInstanceKey("bkt", "orphan")] = struct{}{}

	report := store.localInstanceData("", "", "woodpecker-0", time.UnixMilli(1758300000123))

	require.Equal(t, "woodpecker-0", report.NodeID)
	require.Equal(t, "service", report.StorageMode)
	require.Equal(t, root, report.StorageRoot)
	require.Equal(t, int64(1758300000123), report.TimestampMS)
	require.Equal(t, 2, report.InstanceCount)
	require.Zero(t, report.ScanErrors)
	require.Empty(t, report.FilterBucketName)
	require.Len(t, report.Instances, 2)

	byRoot := map[string]InstanceDataEntry{}
	for _, e := range report.Instances {
		byRoot[e.RootPath] = e
	}
	require.Equal(t, 3, byRoot["live"].ActiveProcessors)
	require.Empty(t, byRoot["live"].DeleteState)
	require.Zero(t, byRoot["live"].MarkedDeletedAtMS)

	require.Zero(t, byRoot["orphan"].ActiveProcessors)
	require.Equal(t, "instance", byRoot["orphan"].DeleteState)
	require.Equal(t, int64(1700000000000), byRoot["orphan"].MarkedDeletedAtMS)
}

// TestLocalInstanceData_FilterSelectsOneInstanceAndIsEchoed: the echo is what lets a
// caller that misspelled a param notice it received an unfiltered answer.
func TestLocalInstanceData_FilterSelectsOneInstanceAndIsEchoed(t *testing.T) {
	t.Parallel()

	store := storeWithRoot(t)
	writeSegmentData(t, localSegmentDataDir(store.cfg, "bkt", "wanted", 1, 0), "aa")
	writeSegmentData(t, localSegmentDataDir(store.cfg, "bkt", "other", 1, 0), "bbbb")

	report := store.localInstanceData("bkt", "wanted", "n", time.UnixMilli(1))

	require.Equal(t, "bkt", report.FilterBucketName)
	require.Equal(t, "wanted", report.FilterRootPath)
	require.Equal(t, 1, report.InstanceCount)
	require.Equal(t, int64(2), report.TotalSizeBytes)
	require.Len(t, report.Instances, 1)
	require.Equal(t, "wanted", report.Instances[0].RootPath)
}

// TestLocalInstanceData_FilterMissesEverythingYieldsEmptyList is the "is it gone yet?"
// answer the reconciliation loop polls for.
func TestLocalInstanceData_FilterMissesEverythingYieldsEmptyList(t *testing.T) {
	t.Parallel()

	store := storeWithRoot(t)
	writeSegmentData(t, localSegmentDataDir(store.cfg, "bkt", "present", 1, 0), "aa")

	report := store.localInstanceData("bkt", "absent", "n", time.UnixMilli(1))

	require.Zero(t, report.InstanceCount)
	require.Empty(t, report.Instances)
	require.NotNil(t, report.Instances, "empty list must encode as [] rather than null")
}

// TestLocalInstanceData_LogMarkerReportsOldestLogDeleteState: with only per-log marks,
// the instance is not marked as a whole, and the oldest mark is the one still waiting.
func TestLocalInstanceData_LogMarkerReportsOldestLogDeleteState(t *testing.T) {
	t.Parallel()

	store := storeWithRoot(t)
	root := store.cfg.Woodpecker.Storage.RootPath
	writeSegmentData(t, localSegmentDataDir(store.cfg, "bkt", "inst", 1, 0), "aa")
	require.NoError(t, writeDeleteMarker(t.Context(), root,
		deleteMarker{Bucket: "bkt", RootPath: "inst", LogId: 1, DeletedAt: 1700000500}))
	require.NoError(t, writeDeleteMarker(t.Context(), root,
		deleteMarker{Bucket: "bkt", RootPath: "inst", LogId: 2, DeletedAt: 1700000100}))

	report := store.localInstanceData("", "", "n", time.UnixMilli(1))

	require.Len(t, report.Instances, 1)
	require.Equal(t, "log", report.Instances[0].DeleteState)
	require.Equal(t, int64(1700000100000), report.Instances[0].MarkedDeletedAtMS)
}

// TestLocalInstanceData_InstanceMarkerOutranksLogMarker: once the whole instance is
// marked, per-log marks are noise.
func TestLocalInstanceData_InstanceMarkerOutranksLogMarker(t *testing.T) {
	t.Parallel()

	store := storeWithRoot(t)
	root := store.cfg.Woodpecker.Storage.RootPath
	writeSegmentData(t, localSegmentDataDir(store.cfg, "bkt", "inst", 1, 0), "aa")
	require.NoError(t, writeDeleteMarker(t.Context(), root,
		deleteMarker{Bucket: "bkt", RootPath: "inst", LogId: 1, DeletedAt: 1700000500}))
	require.NoError(t, writeDeleteMarker(t.Context(), root,
		deleteMarker{Bucket: "bkt", RootPath: "inst", Instance: true, DeletedAt: 1700009999}))

	report := store.localInstanceData("", "", "n", time.UnixMilli(1))

	require.Equal(t, "instance", report.Instances[0].DeleteState)
	require.Equal(t, int64(1700009999000), report.Instances[0].MarkedDeletedAtMS)
}

// TestLocalInstanceData_SortedByBucketThenRootPath keeps the output stable so a
// control plane diffing successive polls does not see phantom churn.
func TestLocalInstanceData_SortedByBucketThenRootPath(t *testing.T) {
	t.Parallel()

	store := storeWithRoot(t)
	for _, inst := range []struct{ bucket, root string }{
		{"b2", "z"}, {"b1", "z"}, {"b2", "a"}, {"b1", "a"},
	} {
		writeSegmentData(t, localSegmentDataDir(store.cfg, inst.bucket, inst.root, 1, 0), "x")
	}

	report := store.localInstanceData("", "", "n", time.UnixMilli(1))

	got := make([]string, 0, len(report.Instances))
	for _, e := range report.Instances {
		got = append(got, e.BucketName+"/"+e.RootPath)
	}
	require.Equal(t, []string{"b1/a", "b1/z", "b2/a", "b2/z"}, got)
}

// TestLocalInstanceData_ConvergesAfterInstanceDeleteAndReclaim walks the whole loop the
// endpoint exists to enable: an orphan shows up in the inventory, the delete marks it,
// the inventory reports it as marked while the grace window runs, and after the reclaim
// pass the instance is gone while the live instance beside it is untouched.
func TestLocalInstanceData_ConvergesAfterInstanceDeleteAndReclaim(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := storeWithRoot(t)
	writeSegmentData(t, localSegmentDataDir(store.cfg, "bkt", "live", 1, 0), "live-data")
	writeSegmentData(t, localSegmentDataDir(store.cfg, "bkt", "orphan", 7, 0), "orphan-data")

	// 1. Query: both instances are visible, neither is marked.
	before := store.localInstanceData("", "", "n", time.UnixMilli(1))
	require.Equal(t, 2, before.InstanceCount)
	for _, e := range before.Instances {
		require.Empty(t, e.DeleteState, e.RootPath)
	}

	// 2. Decide + delete: the orphan is marked, and is still on disk during grace.
	require.NoError(t, store.EvictInstance(ctx, "bkt", "orphan"))
	marked := store.localInstanceData("bkt", "orphan", "n", time.UnixMilli(1))
	require.Equal(t, 1, marked.InstanceCount)
	require.Equal(t, deleteStateInstance, marked.Instances[0].DeleteState)
	require.Positive(t, marked.Instances[0].MarkedDeletedAtMS)

	// 3. Reclaim: with the grace window elapsed the local data goes.
	require.NoError(t, newDeletedLogReclaimTask(store, 0).Run(ctx))

	// 4. Re-query: the orphan is absent, the live instance is untouched.
	after := store.localInstanceData("", "", "n", time.UnixMilli(2))
	require.Equal(t, 1, after.InstanceCount)
	require.Equal(t, "live", after.Instances[0].RootPath)

	gone := store.localInstanceData("bkt", "orphan", "n", time.UnixMilli(2))
	require.Zero(t, gone.InstanceCount)
	require.Empty(t, gone.Instances)
}

// TestLocalInstanceData_ReportsDataOnANodeThatNeverGotTheDelete is the gap that
// motivated the endpoint: a node that missed the delete broadcast has no marker, so
// nothing on it will ever reclaim the data, and only an external query can notice.
func TestLocalInstanceData_ReportsDataOnANodeThatNeverGotTheDelete(t *testing.T) {
	t.Parallel()

	store := storeWithRoot(t)
	writeSegmentData(t, localSegmentDataDir(store.cfg, "bkt", "missed", 1, 0), "stranded")

	// Restarting rebuilds the deleting-set from markers alone — of which there are none.
	require.NoError(t, store.rebuildDeletingSetsFromMarkers())
	require.NoError(t, newDeletedLogReclaimTask(store, 0).Run(context.Background()))

	report := store.localInstanceData("bkt", "missed", "n", time.UnixMilli(1))

	require.Equal(t, 1, report.InstanceCount)
	require.Empty(t, report.Instances[0].DeleteState, "no marker: nothing here will ever reclaim it")
	require.Equal(t, 1, report.Instances[0].LiveSegmentCount)
}

// TestInstanceDataReport_JSONContract pins the wire field names of
// GET /admin/instance/data. The control plane and common/http/README.md both key off
// these exact names, so a rename here is a breaking change and has to be deliberate.
func TestInstanceDataReport_JSONContract(t *testing.T) {
	t.Parallel()

	report := InstanceDataReport{
		NodeID: "woodpecker-0", StorageMode: "service", StorageRoot: "/var/lib/woodpecker",
		FilterBucketName: "a-bucket", FilterRootPath: "in01-abc",
		TimestampMS: 1758300000123, InstanceCount: 1, TotalSizeBytes: 1234567, ScanErrors: 0,
		Instances: []InstanceDataEntry{{
			BucketName: "a-bucket", RootPath: "in01-abc",
			LogCount: 12, SegmentCount: 37, LiveSegmentCount: 30,
			SizeBytes: 1234567, LastModifiedMS: 1758299000456,
			ActiveProcessors: 0, DeleteState: "", MarkedDeletedAtMS: 0,
		}},
	}

	raw, err := json.Marshal(report)
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))

	require.ElementsMatch(t, []string{
		"node_id", "storage_mode", "storage_root",
		"filter_bucket_name", "filter_root_path",
		"timestamp_ms", "instance_count", "total_size_bytes", "scan_errors", "instances",
	}, keysOf(decoded))

	entry, ok := decoded["instances"].([]any)[0].(map[string]any)
	require.True(t, ok)
	require.ElementsMatch(t, []string{
		"bucket_name", "root_path", "log_count", "segment_count", "live_segment_count",
		"size_bytes", "last_modified_ms", "active_processors",
		"delete_state", "marked_deleted_at_ms",
	}, keysOf(entry))
}

// TestInstanceDataReport_FilterFieldsOmittedWhenUnfiltered: the echo must be absent
// rather than empty, so its presence alone tells a caller a filter took effect.
func TestInstanceDataReport_FilterFieldsOmittedWhenUnfiltered(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(InstanceDataReport{NodeID: "n", Instances: []InstanceDataEntry{}})
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))

	require.NotContains(t, decoded, "filter_bucket_name")
	require.NotContains(t, decoded, "filter_root_path")
	require.Equal(t, []any{}, decoded["instances"], "empty inventory must be [] not null")
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

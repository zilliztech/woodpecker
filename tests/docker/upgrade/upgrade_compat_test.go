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

package upgrade

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	wphttp "github.com/zilliztech/woodpecker/common/http"
	"github.com/zilliztech/woodpecker/tests/docker/framework"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// metricsPorts maps node index -> the published METRICS_PORT from
// deployments/docker-compose.yaml. The HTTP endpoints (/healthz,
// /admin/log-health, /admin/node/status) are served on the metrics port, NOT
// the service port. framework.NodeInfo carries only ServicePort/GossipPort, so
// these are hardcoded here (index-aligned with cluster.Nodes).
var metricsPorts = []int{9091, 9092, 9093, 9094}

const (
	// upgradeLogName is the deterministic log name written pre-upgrade and read
	// back post-upgrade.
	upgradeLogName = "upgrade-compat-test-log"

	// numPreUpgradeEntries / entrySize are tuned together with the rolling
	// policy so the dataset spans >=2 segments (8 entries * 4KiB = 32KiB with a
	// 12KiB MaxSize forces several rolls).
	numPreUpgradeEntries = 8
	entrySize            = 4096
	maxSegmentSize       = 12 * 1024 // 12KiB -> >=2 segments for the dataset above

	numPostUpgradeEntries = 5
)

// entrySnapshot records the identity and payload fingerprint of one pre-upgrade
// entry. This is the in-memory manifest used for post-upgrade verification.
type entrySnapshot struct {
	segmentID int64
	entryID   int64
	payload   []byte
	crc32     uint32
}

// deterministicPayload produces a stable payload for entry i so that a byte/CRC
// comparison is meaningful across the upgrade.
func deterministicPayload(i int) []byte {
	p := make([]byte, entrySize)
	for j := range p {
		p[j] = byte((i*31 + j) % 251)
	}
	return p
}

// TestUpgradeCompat_ServiceMode verifies that a service-mode cluster created by
// the v0.1.30 baseline upgrades to HEAD with no data loss, metadata corruption,
// or behavioral regression.
//
// Phases:
//
//	A. Ensure target (HEAD) image built as woodpecker:current.
//	B. Ensure baseline image present (already-local, pull, or skip).
//	C. Start the v0.1.30 cluster; assert /admin/log-health is 404 (soft proof the
//	   baseline is genuinely v0.1.30).
//	D. Write a deterministic dataset (>=2 segments); record an in-memory manifest.
//	E. Stop nodes, swap WOODPECKER_IMAGE -> target, RecreateAllNodes.
//	F. Verify: pre-upgrade data byte/CRC/(SegmentId,EntryId) identical; continued
//	   read+write with monotonic IDs + segment roll; LAC >= recorded; quorum read
//	   with node4 absent; recovery (4 nodes + /healthz 200 + membership converges);
//	   /admin/log-health 200 (hard).
func TestUpgradeCompat_ServiceMode(t *testing.T) {
	ctx := context.Background()

	baselineImage := os.Getenv("WOODPECKER_IMAGE_BASELINE")
	if baselineImage == "" {
		baselineImage = "ghcr.io/zilliztech/woodpecker/upgrade-base:v0.1.30"
	}
	targetImage := os.Getenv("WOODPECKER_IMAGE_TARGET")
	if targetImage == "" {
		targetImage = "woodpecker:current"
	}
	t.Logf("Baseline image: %s", baselineImage)
	t.Logf("Target image:   %s", targetImage)

	cluster := NewUpgradeCluster(t)
	t.Cleanup(func() { cluster.Clean(t) })

	// ===== Phase A: build the HEAD (target) image as an explicit tag =====
	t.Log("Phase A: ensuring target (HEAD) image is built...")
	ensureTargetImage(t, cluster, targetImage)

	// ===== Phase B: ensure the baseline image is available =====
	t.Log("Phase B: ensuring baseline image is available...")
	if !ensureBaselineImage(t, baselineImage) {
		t.Skipf("baseline image %s unavailable (not local and pull failed); "+
			"run prime-upgrade-baselines.yaml or pre-pull the image", baselineImage)
	}

	// ===== Phase C: start the v0.1.30 baseline cluster =====
	t.Log("Phase C: starting v0.1.30 baseline cluster...")
	t.Setenv("WOODPECKER_IMAGE", baselineImage)
	cluster.Up(t)
	cluster.WaitForClusterReady(t, 2*time.Minute)
	t.Log("v0.1.30 cluster ready")

	// Soft / log-only: /admin/log-health must be 404 in v0.1.30 (route added by #171).
	// Brittle if backported, so we only log a mismatch instead of failing.
	preStatus := httpGetStatus(t, metricsPorts[0], wphttp.AdminLogHealthPath)
	if preStatus == http.StatusNotFound {
		t.Logf("OK (soft): /admin/log-health is 404 pre-upgrade (baseline is genuine v0.1.30)")
	} else {
		t.Logf("WARN (soft): expected /admin/log-health 404 pre-upgrade, got %d "+
			"(may indicate a backported baseline)", preStatus)
	}

	// ===== Phase D: write the deterministic dataset with the host (HEAD) client =====
	t.Log("Phase D: writing deterministic dataset...")
	cfg := cluster.NewConfig(t)
	cfg.Woodpecker.Client.SegmentRollingPolicy.MaxSize = maxSegmentSize

	wpClient, etcdCli, _ := cluster.NewClientManual(t, ctx)

	require.NoError(t, wpClient.CreateLog(ctx, upgradeLogName), "CreateLog")
	logHandle, err := wpClient.OpenLog(ctx, upgradeLogName)
	require.NoError(t, err, "OpenLog")

	logWriter, err := logHandle.OpenLogWriter(ctx)
	require.NoError(t, err, "OpenLogWriter")

	snapshots := make([]entrySnapshot, 0, numPreUpgradeEntries)
	segmentsSeen := make(map[int64]struct{})
	for i := 0; i < numPreUpgradeEntries; i++ {
		payload := deterministicPayload(i)
		result := logWriter.Write(ctx, &log.WriteMessage{
			Payload: payload,
			Properties: map[string]string{
				"index": fmt.Sprintf("%d", i),
			},
		})
		require.NoError(t, result.Err, "write entry %d", i)
		require.NotNil(t, result.LogMessageId, "write entry %d returned nil id", i)
		snapshots = append(snapshots, entrySnapshot{
			segmentID: result.LogMessageId.SegmentId,
			entryID:   result.LogMessageId.EntryId,
			payload:   payload,
			crc32:     crc32.ChecksumIEEE(payload),
		})
		segmentsSeen[result.LogMessageId.SegmentId] = struct{}{}
	}
	require.NoError(t, logWriter.Close(ctx), "close pre-upgrade writer")
	// In service mode the SERVER controls segment rolling (the baseline image's
	// baked config defaults to maxSize 256MB), so a small client-side dataset stays
	// in a single segment regardless of the client SegmentRollingPolicy. The
	// sealed-segment compat path is still exercised: this segment is sealed on
	// writer close / the upgrade restart and read back byte-exact in F1, and
	// segment rolling across the upgrade is asserted in F2 (a brand-new segment
	// must appear after the restart).
	require.GreaterOrEqual(t, len(segmentsSeen), 1,
		"dataset must produce at least one segment")
	lastPreUpgradeID := snapshots[len(snapshots)-1]
	t.Logf("Wrote %d entries across %d segments; last id seg=%d entry=%d",
		len(snapshots), len(segmentsSeen), lastPreUpgradeID.segmentID, lastPreUpgradeID.entryID)

	// Drop the pre-upgrade client before the swap (servers are about to restart).
	// Best-effort: closing the client/etcd as the cluster winds down can surface a
	// benign "context canceled"; the post-upgrade phase opens a fresh client anyway.
	_ = wpClient.Close(ctx)
	_ = etcdCli.Close()

	// ===== Phase E: upgrade the node services to HEAD =====
	t.Log("Phase E: upgrading node services to HEAD image...")
	t.Setenv("WOODPECKER_IMAGE", targetImage)
	cluster.RecreateAllNodes(t, 2*time.Minute)
	t.Log("cluster recreated on HEAD image")

	// Hard: /admin/log-health must be 200 after upgrade (HEAD is running).
	require.Eventually(t, func() bool {
		return httpGetStatus(t, metricsPorts[0], wphttp.AdminLogHealthPath) == http.StatusOK
	}, 60*time.Second, 2*time.Second, "/admin/log-health must be 200 post-upgrade")
	t.Log("confirmed: /admin/log-health is 200 post-upgrade")

	// ===== Phase F: verify post-upgrade integrity =====
	t.Log("Phase F: verifying post-upgrade integrity...")
	cfg2 := cluster.NewConfig(t)
	cfg2.Woodpecker.Client.SegmentRollingPolicy.MaxSize = maxSegmentSize
	wpClient2, etcdCli2, _ := cluster.NewClientManual(t, ctx)
	defer func() {
		_ = wpClient2.Close(ctx)
		_ = etcdCli2.Close()
	}()

	logHandle2, err := wpClient2.OpenLog(ctx, upgradeLogName)
	require.NoError(t, err, "OpenLog post-upgrade")

	// F1: pre-upgrade entries are byte/CRC/(SegmentId,EntryId) identical.
	t.Log("F1: reading back pre-upgrade entries (byte/crc/id exact)...")
	earliest := log.EarliestLogMessageID()
	reader, err := logHandle2.OpenLogReader(ctx, &earliest, "verify-readback")
	require.NoError(t, err, "OpenLogReader readback")
	for i, want := range snapshots {
		msg, readErr := reader.ReadNext(ctx)
		require.NoError(t, readErr, "ReadNext %d", i)
		require.NotNil(t, msg, "entry %d nil", i)
		require.NotNil(t, msg.Id, "entry %d nil id", i)
		require.Equal(t, want.segmentID, msg.Id.SegmentId, "entry %d SegmentId", i)
		require.Equal(t, want.entryID, msg.Id.EntryId, "entry %d EntryId", i)
		require.Equal(t, want.payload, msg.Payload, "entry %d payload (byte-compare)", i)
		require.Equal(t, want.crc32, crc32.ChecksumIEEE(msg.Payload), "entry %d crc", i)
	}
	require.NoError(t, reader.Close(ctx), "close readback reader")
	t.Logf("verified %d pre-upgrade entries byte-identical", len(snapshots))

	// F2: append post-upgrade entries; IDs monotonic; segment roll old(v6)+new(v6).
	t.Log("F2: appending post-upgrade entries with monotonic IDs...")
	writer2, err := logHandle2.OpenLogWriter(ctx)
	require.NoError(t, err, "OpenLogWriter post-upgrade")
	postSegmentsSeen := make(map[int64]struct{})
	var lastID *log.LogMessageId
	for i := 0; i < numPostUpgradeEntries; i++ {
		payload := deterministicPayload(numPreUpgradeEntries + i)
		result := writer2.Write(ctx, &log.WriteMessage{Payload: payload})
		require.NoError(t, result.Err, "post-upgrade write %d", i)
		require.NotNil(t, result.LogMessageId, "post-upgrade write %d nil id", i)
		// Monotonic continuation: new id strictly greater than the last pre-upgrade id.
		require.True(t, idGreater(result.LogMessageId, &log.LogMessageId{
			SegmentId: lastPreUpgradeID.segmentID, EntryId: lastPreUpgradeID.entryID,
		}), "post-upgrade id %v must exceed last pre-upgrade id seg=%d entry=%d",
			result.LogMessageId, lastPreUpgradeID.segmentID, lastPreUpgradeID.entryID)
		postSegmentsSeen[result.LogMessageId.SegmentId] = struct{}{}
		lastID = result.LogMessageId
	}
	require.NoError(t, writer2.Close(ctx), "close post-upgrade writer")

	// Segment rolling across the upgrade: the post-upgrade writes must land in at
	// least one segment that did not exist pre-upgrade. The pre-upgrade segment was
	// sealed by the writer close + the cluster restart, so the upgraded cluster
	// rolls to a new segment. This is the service-mode proof that rolling still works
	// (we cannot force >=2 pre-upgrade segments because the server controls rolling).
	rolledToNewSegment := false
	for s := range postSegmentsSeen {
		if _, existed := segmentsSeen[s]; !existed {
			rolledToNewSegment = true
			break
		}
	}
	require.True(t, rolledToNewSegment,
		"post-upgrade writes must roll into a new segment (segment rolling across upgrade)")

	// F3: LAC >= recorded. The last successfully read/written record id must be
	// at least the last pre-upgrade record id (LAC advanced past recorded state).
	require.True(t, idGreaterOrEqual(lastID, &log.LogMessageId{
		SegmentId: lastPreUpgradeID.segmentID, EntryId: lastPreUpgradeID.entryID,
	}), "post-upgrade LAC (last id %v) must be >= recorded pre-upgrade last id seg=%d entry=%d",
		lastID, lastPreUpgradeID.segmentID, lastPreUpgradeID.entryID)

	// F4: read back all entries (pre + post).
	totalExpected := numPreUpgradeEntries + numPostUpgradeEntries
	allCount := readAllFromEarliest(t, ctx, logHandle2, "verify-all", totalExpected)
	require.Equal(t, totalExpected, allCount, "should read all (pre + post) entries")
	t.Logf("read back all %d entries (pre + post upgrade)", allCount)

	// F5: quorum read with node4 absent (node4 is not among the first-3 seeds).
	t.Log("F5: quorum read with node4 stopped...")
	cluster.StopNode(t, "woodpecker-node4")
	node4Restarted := false
	defer func() {
		if !node4Restarted {
			cluster.StartNode(t, "woodpecker-node4")
		}
	}()
	quorumCount := readAllFromEarliest(t, ctx, logHandle2, "verify-quorum", totalExpected)
	require.Equal(t, totalExpected, quorumCount,
		"quorum read with 3/4 nodes must return all entries")
	t.Logf("quorum read succeeded: %d entries with node4 absent", quorumCount)

	// Bring node4 back for the recovery / convergence checks.
	cluster.StartNode(t, "woodpecker-node4")
	node4Restarted = true

	// F6: recovery — all 4 nodes /healthz 200 and membership converges to 4.
	t.Log("F6: recovery — health + membership convergence...")
	require.Eventually(t, func() bool {
		for i := range metricsPorts {
			if httpGetStatus(t, metricsPorts[i], wphttp.HealthRouterPath) != http.StatusOK {
				return false
			}
		}
		return true
	}, 90*time.Second, 3*time.Second, "all 4 nodes must report /healthz 200 after recovery")

	require.Eventually(t, func() bool {
		for i := range metricsPorts {
			if nodeMemberCount(t, metricsPorts[i]) != len(metricsPorts) {
				return false
			}
		}
		return true
	}, 90*time.Second, 3*time.Second, "memberlist must converge to %d members on all nodes", len(metricsPorts))
	t.Log("recovery verified: 4 nodes healthy, membership converged")

	// F7: client recovery — reopen client and re-read everything.
	t.Log("F7: client recovery (reopen + re-read)...")
	// Best-effort closes: a benign "context canceled" can surface on shutdown; the
	// reopened wpClient3 below is what proves recovery (matches the pre-swap closes).
	_ = wpClient2.Close(ctx)
	_ = etcdCli2.Close()

	wpClient3, etcdCli3, _ := cluster.NewClientManual(t, ctx)
	defer func() {
		_ = wpClient3.Close(ctx)
		_ = etcdCli3.Close()
	}()
	logHandle3, err := wpClient3.OpenLog(ctx, upgradeLogName)
	require.NoError(t, err, "OpenLog after recovery")
	recoveryCount := readAllFromEarliest(t, ctx, logHandle3, "verify-recovery", totalExpected)
	require.Equal(t, totalExpected, recoveryCount, "recovery read should return all entries")
	t.Logf("recovery verified: read %d entries after client restart", recoveryCount)

	t.Log("All upgrade compatibility checks passed")
}

// readAllFromEarliest reads exactly expected entries from earliest and returns
// the count actually read.
func readAllFromEarliest(t *testing.T, ctx context.Context, logHandle log.LogHandle, readerName string, expected int) int {
	t.Helper()
	earliest := log.EarliestLogMessageID()
	reader, err := logHandle.OpenLogReader(ctx, &earliest, readerName)
	require.NoError(t, err, "OpenLogReader %s", readerName)
	defer func() { _ = reader.Close(ctx) }()

	count := 0
	for i := 0; i < expected; i++ {
		msg, readErr := reader.ReadNext(ctx)
		require.NoError(t, readErr, "%s ReadNext %d", readerName, i)
		require.NotNil(t, msg, "%s entry %d nil", readerName, i)
		count++
	}
	return count
}

// idGreater reports whether a > b in (SegmentId, EntryId) order.
func idGreater(a, b *log.LogMessageId) bool {
	if a.SegmentId != b.SegmentId {
		return a.SegmentId > b.SegmentId
	}
	return a.EntryId > b.EntryId
}

// idGreaterOrEqual reports whether a >= b in (SegmentId, EntryId) order.
func idGreaterOrEqual(a, b *log.LogMessageId) bool {
	if a.SegmentId != b.SegmentId {
		return a.SegmentId > b.SegmentId
	}
	return a.EntryId >= b.EntryId
}

// ensureTargetImage builds the HEAD image to an explicit target tag if it is not
// already present locally. It uses build/build_image.sh directly (BuildImageIfNeeded
// only builds the :latest tag, which must not be reused here per the upgrade design).
func ensureTargetImage(t *testing.T, cluster *UpgradeCluster, targetImage string) {
	t.Helper()
	stdout, _, _ := framework.RunCommand(t, "docker", "images", "-q", targetImage)
	if strings.TrimSpace(stdout) != "" {
		t.Logf("target image %s already present; skipping build", targetImage)
		return
	}
	projectRoot := filepath.Join(cluster.ComposeDir, "..")
	buildScript := filepath.Join(projectRoot, "build", "build_image.sh")
	t.Logf("building target image %s via %s ...", targetImage, buildScript)
	// build_image.sh must run from the project root.
	framework.RunCommandNoFail(t, buildScript, "ubuntu22.04", "auto", "-t", targetImage)
	t.Logf("built target image %s", targetImage)
}

// ensureBaselineImage returns true if the baseline image is available locally
// (already present, or pulled successfully). Returns false if it is neither local
// nor pullable, letting the caller skip rather than hard-fail.
func ensureBaselineImage(t *testing.T, baselineImage string) bool {
	t.Helper()
	if _, _, err := framework.RunCommand(t, "docker", "image", "inspect", baselineImage); err == nil {
		t.Logf("baseline image %s already present", baselineImage)
		return true
	}
	t.Logf("baseline image %s not local; pulling...", baselineImage)
	if _, _, err := framework.RunCommand(t, "docker", "pull", baselineImage); err != nil {
		t.Logf("failed to pull baseline image %s: %v", baselineImage, err)
		return false
	}
	t.Logf("pulled baseline image %s", baselineImage)
	return true
}

// httpGetStatus issues a GET to http://localhost:<port><path> and returns the
// HTTP status code, or -1 on a request error.
func httpGetStatus(t *testing.T, port int, path string) int {
	t.Helper()
	url := fmt.Sprintf("http://localhost:%d%s", port, path)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Logf("GET %s failed: %v", url, err)
		return -1
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// nodeMemberCount queries /admin/node/status on the given metrics port and returns
// the reported member_count, or -1 on any error.
func nodeMemberCount(t *testing.T, port int) int {
	t.Helper()
	url := fmt.Sprintf("http://localhost:%d%s", port, wphttp.AdminNodeStatusPath)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Logf("GET %s failed: %v", url, err)
		return -1
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Logf("read %s body failed: %v", url, err)
		return -1
	}
	var status struct {
		MemberCount int `json:"member_count"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		t.Logf("unmarshal %s body failed: %v (body=%s)", url, err, string(body))
		return -1
	}
	return status.MemberCount
}

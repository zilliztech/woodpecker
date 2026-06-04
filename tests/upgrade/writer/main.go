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

// Command writer is the pre-upgrade data generator for the embed upgrade
// compatibility test. It is built inside the branch-v0.1.13 worktree, so it
// must use ONLY the API subset that is byte-for-byte identical between
// branch-v0.1.13 and HEAD:
//
//	config.NewConfiguration(files...)
//	woodpecker.NewEmbedClientFromConfig(ctx, cfg)
//	client.CreateLog / client.OpenLog / client.Close
//	logHandle.OpenLogWriter
//	logWriter.Write (returns *log.WriteResult{LogMessageId, Err}) / logWriter.Close
//	woodpecker.StopEmbedLogStore()
//
// To stay portable across the two trees (whose config struct field *types*
// differ — e.g. Etcd.Endpoints is string in v0.1.13 but []string in HEAD, and
// SegmentRollingPolicy.MaxSize is int64 in v0.1.13 but a ByteSize wrapper in
// HEAD) the writer never assigns those typed fields directly. Instead it emits
// a small YAML overlay at runtime and feeds it to config.NewConfiguration,
// which both trees parse with yaml.Unmarshal regardless of the underlying Go
// type. The writer is only ever *run* from the v0.1.13 binary; HEAD only needs
// it to *compile* (verification check #1).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/tests/upgrade"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

func main() {
	storage := flag.String("storage", "local", "Storage type: local or minio")
	dataDir := flag.String("data-dir", "/tmp/woodpecker_data", "Data root directory (for local storage)")
	etcdEndpoints := flag.String("etcd", "localhost:2379", "Etcd endpoints (comma-separated)")
	minioEndpoint := flag.String("minio-endpoint", "localhost:9000", "MinIO endpoint host:port (minio storage)")
	minioBucket := flag.String("bucket", "a-bucket", "MinIO bucket name (minio storage)")
	logs := flag.Int("logs", 2, "Number of logs to create")
	entries := flag.Int("entries", 100, "Entries per log")
	payloadPrefix := flag.String("payload-prefix", "entry_", "Prefix for payload content")
	rollAtBytes := flag.Int64("roll-at-bytes", 50000, "Force segment roll at this many bytes")
	maxBlocks := flag.Int64("max-blocks", 4, "Force segment roll after this many blocks")
	compactionWait := flag.Int("compaction-wait-seconds", 8, "Seconds to wait after the main logs so the auditor compacts their completed segments into merged m_*.blk objects")
	freshEntries := flag.Int("fresh-entries", 2, "Entries for a final 'fresh' log left UNcompacted (must be < max-blocks so its segment stays active and is never compacted)")
	manifestOut := flag.String("manifest-out", "/tmp/manifest.json", "Output manifest JSON file")
	flag.Parse()

	ctx := context.Background()

	if *storage == "local" {
		if err := os.MkdirAll(*dataDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create data directory: %v\n", err)
			os.Exit(1)
		}
	}

	cfg, err := buildConfig(*storage, *dataDir, *etcdEndpoints, *minioEndpoint, *minioBucket, *rollAtBytes, *maxBlocks)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to build config: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Creating embed client...")
	client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err)
		os.Exit(1)
	}

	manifest := upgrade.Manifest{
		CreatedAt:     time.Now().Format(time.RFC3339),
		WriterVersion: "v0.1.13",
		StorageType:   *storage,
		RootPath:      *dataDir,
		EtcdEndpoints: *etcdEndpoints,
		Logs:          make([]upgrade.LogRecord, 0, *logs),
	}

	// Phase 1: main logs. Each is written and CLOSED, so all of its segments
	// become Completed. With auditor.maxInterval lowered in the config overlay,
	// the auditor will then compact these completed segments into merged m_*.blk
	// objects during the wait below.
	fmt.Printf("Creating %d logs with %d entries each...\n", *logs, *entries)
	for logIdx := 0; logIdx < *logs; logIdx++ {
		logName := fmt.Sprintf("test_log_%d", logIdx)
		logRecord, writeErr := writeLog(ctx, client, logName, *entries, *payloadPrefix, true)
		if writeErr != nil {
			fmt.Fprintf(os.Stderr, "Failed writing log %s: %v\n", logName, writeErr)
			closeAll(ctx, client)
			os.Exit(1)
		}
		manifest.Logs = append(manifest.Logs, logRecord)
		fmt.Printf("Log %s: %d entries across %d segments (max segment id %d)\n",
			logName, logRecord.EntryCount, logRecord.SegmentCount, logRecord.MaxSeqNoObserved)
	}

	// Phase 2: wait for the auditor to compact the completed segments above into
	// merged m_*.blk objects, so the upgrade test exercises reading COMPACTED v5
	// data (the path that surfaced the ParseHeader v5 regression).
	if *compactionWait > 0 {
		fmt.Printf("Waiting %ds for the auditor to compact completed segments...\n", *compactionWait)
		time.Sleep(time.Duration(*compactionWait) * time.Second)
	}

	// Phase 3: a final 'fresh' log left UNcompacted, so the test also exercises
	// reading a plain (non-merged) v5 block. Its single segment is kept ACTIVE
	// (fresh-entries < max-blocks ⇒ it never rolls/completes), and the auditor only
	// compacts Completed segments — so it is never merged. The embed logstore seals
	// it (writing the v5 footer) on shutdown in closeAll.
	freshName := "test_log_fresh_uncompacted"
	freshRecord, writeErr := writeLog(ctx, client, freshName, *freshEntries, *payloadPrefix, false)
	if writeErr != nil {
		fmt.Fprintf(os.Stderr, "Failed writing fresh log %s: %v\n", freshName, writeErr)
		closeAll(ctx, client)
		os.Exit(1)
	}
	manifest.Logs = append(manifest.Logs, freshRecord)
	fmt.Printf("Fresh log %s: %d entries (left uncompacted)\n", freshName, freshRecord.EntryCount)

	// Close client (flushes/closes metadata) then stop the embed logstore
	// singleton, which seals all active segments (writing the v5 footer on disk)
	// and stops the auditor so the fresh segment is never compacted.
	closeAll(ctx, client)

	manifest.Notes = fmt.Sprintf("Created %d logs with %d entries each. Storage: %s. "+
		"Segment rolling: %d bytes / %d blocks. All segments sealed with v5 footer.",
		*logs, *entries, *storage, *rollAtBytes, *maxBlocks)

	if err := upgrade.SaveManifest(&manifest, *manifestOut); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write manifest: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Manifest written to %s\n", *manifestOut)

	fmt.Println("\n=== Pre-Upgrade Snapshot Summary ===")
	totalEntries := 0
	totalSegments := int64(0)
	for _, lr := range manifest.Logs {
		totalEntries += lr.EntryCount
		totalSegments += lr.SegmentCount
		fmt.Printf("  %s: %d entries in %d segments\n", lr.Name, lr.EntryCount, lr.SegmentCount)
	}
	fmt.Printf("Total: %d entries in %d segments across %d logs\n", totalEntries, totalSegments, len(manifest.Logs))
	fmt.Printf("Storage type: %s, created at: %s\n", manifest.StorageType, manifest.CreatedAt)
}

// writeLog creates a log, writes deterministic entries, and records each entry's
// fingerprint. When closeWriter is true the writer is closed (sealing its last
// segment); when false the writer is left open so its final segment stays ACTIVE
// (never auto-compacted) and is sealed later by StopEmbedLogStore.
func writeLog(ctx context.Context, client woodpecker.Client, logName string, entries int, payloadPrefix string, closeWriter bool) (upgrade.LogRecord, error) {
	if err := client.CreateLog(ctx, logName); err != nil {
		return upgrade.LogRecord{}, fmt.Errorf("create log: %w", err)
	}

	logHandle, err := client.OpenLog(ctx, logName)
	if err != nil {
		return upgrade.LogRecord{}, fmt.Errorf("open log: %w", err)
	}

	logWriter, err := logHandle.OpenLogWriter(ctx)
	if err != nil {
		return upgrade.LogRecord{}, fmt.Errorf("open writer: %w", err)
	}

	logRecord := upgrade.LogRecord{
		Name:          logName,
		EntryCount:    entries,
		PayloadPrefix: payloadPrefix,
		Entries:       make([]upgrade.EntryRecord, 0, entries),
	}

	segmentIDs := make(map[int64]struct{})
	for entryIdx := 0; entryIdx < entries; entryIdx++ {
		payload := []byte(fmt.Sprintf("%s%d", payloadPrefix, entryIdx))
		result := logWriter.Write(ctx, &log.WriteMessage{
			Payload: payload,
			Properties: map[string]string{
				"index": fmt.Sprintf("%d", entryIdx),
			},
		})
		if result.Err != nil {
			_ = logWriter.Close(ctx)
			return upgrade.LogRecord{}, fmt.Errorf("write entry %d: %w", entryIdx, result.Err)
		}

		entryRec := upgrade.EntryRecord{
			SegmentId:  result.LogMessageId.SegmentId,
			EntryId:    result.LogMessageId.EntryId,
			CRC32:      upgrade.ComputePayloadCRC32(payload),
			PayloadLen: len(payload),
		}
		logRecord.Entries = append(logRecord.Entries, entryRec)

		if entryIdx == 0 {
			first := entryRec
			logRecord.FirstId = &first
		}
		last := entryRec
		logRecord.LastId = &last
		segmentIDs[result.LogMessageId.SegmentId] = struct{}{}

		if entryIdx%20 == 0 {
			fmt.Printf("  wrote entry %d/%d (seg:%d, entry:%d)\n",
				entryIdx+1, entries, result.LogMessageId.SegmentId, result.LogMessageId.EntryId)
		}
	}

	logRecord.SegmentCount = int64(len(segmentIDs))
	if n := len(logRecord.Entries); n > 0 {
		logRecord.MaxSeqNoObserved = logRecord.Entries[n-1].SegmentId
	}

	if closeWriter {
		if err := logWriter.Close(ctx); err != nil {
			return upgrade.LogRecord{}, fmt.Errorf("close writer: %w", err)
		}
	}
	return logRecord, nil
}

// closeAll closes the client and stops the embed logstore singleton, sealing
// all active segments. Errors are logged but not fatal during cleanup.
func closeAll(ctx context.Context, client woodpecker.Client) {
	if err := client.Close(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "warning: failed to close client: %v\n", err)
	}
	fmt.Println("Stopping embed logstore to seal segments...")
	if err := woodpecker.StopEmbedLogStore(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: failed to stop embed logstore: %v\n", err)
	}
}

// buildConfig writes a YAML overlay carrying only the fields the writer needs to
// override, then loads it through config.NewConfiguration. This is the portable
// path across branch-v0.1.13 and HEAD (their config struct field *types* differ,
// but both unmarshal the same YAML keys). The overlay is layered on top of the
// built-in defaults that NewConfiguration seeds first.
func buildConfig(storage, dataDir, etcdEndpoints, minioEndpoint, minioBucket string, rollAtBytes, maxBlocks int64) (*config.Configuration, error) {
	overlayPath, err := writeConfigOverlay(storage, dataDir, etcdEndpoints, minioEndpoint, minioBucket, rollAtBytes, maxBlocks)
	if err != nil {
		return nil, err
	}
	defer os.Remove(overlayPath)

	cfg, err := config.NewConfiguration(overlayPath)
	if err != nil {
		return nil, fmt.Errorf("load configuration overlay: %w", err)
	}
	return cfg, nil
}

// writeConfigOverlay renders the runtime YAML overlay to a temp file and returns
// its path. etcd.endpoints is emitted as a YAML scalar, which matches the
// branch-v0.1.13 string field (the only tree this binary runs in).
func writeConfigOverlay(storage, dataDir, etcdEndpoints, minioEndpoint, minioBucket string, rollAtBytes, maxBlocks int64) (string, error) {
	minioHost, minioPort := splitHostPort(minioEndpoint, "localhost", 9000)

	overlay := fmt.Sprintf(`woodpecker:
  meta:
    type: etcd
    prefix: woodpecker
  client:
    segmentRollingPolicy:
      maxSize: %d
      maxBlocks: %d
    auditor:
      maxInterval: 1
  storage:
    type: %s
    rootPath: %s
etcd:
  endpoints: %s
minio:
  address: %s
  port: %d
  bucketName: %s
`, rollAtBytes, maxBlocks, storage, dataDir, etcdEndpoints, minioHost, minioPort, minioBucket)

	f, err := os.CreateTemp("", "wp-upgrade-writer-*.yaml")
	if err != nil {
		return "", fmt.Errorf("create overlay file: %w", err)
	}
	path := f.Name()
	if _, err := f.WriteString(overlay); err != nil {
		f.Close()
		os.Remove(path)
		return "", fmt.Errorf("write overlay file: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return "", fmt.Errorf("close overlay file: %w", err)
	}
	return path, nil
}

// splitHostPort splits "host:port" into its parts, falling back to defaults.
func splitHostPort(endpoint, defaultHost string, defaultPort int) (string, int) {
	host := defaultHost
	port := defaultPort
	if endpoint == "" {
		return host, port
	}
	for i := len(endpoint) - 1; i >= 0; i-- {
		if endpoint[i] == ':' {
			host = endpoint[:i]
			p := 0
			if _, err := fmt.Sscanf(endpoint[i+1:], "%d", &p); err == nil && p > 0 {
				port = p
			}
			return host, port
		}
	}
	return endpoint, port
}

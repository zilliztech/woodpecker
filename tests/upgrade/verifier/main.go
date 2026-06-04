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

// Command verifier is the post-upgrade reader/validator for the embed upgrade
// compatibility test. It is built from HEAD and reads the dataset written by the
// branch-v0.1.13 writer (see tests/upgrade/writer). It validates that:
//
//  1. the legacy metadata prefix bridge fired (pre-existing logs are found);
//  2. every pre-upgrade entry reads back byte/CRC/id-identical to the manifest
//     (this exercises HEAD parsing of v5 segment footers with LAC defaulting
//     to -1);
//  3. new entries can be appended to an existing log, forcing a segment roll so
//     a v6 footer sits beside the old v5 footers, and old+new read back;
//  4. the client recovers after a full close/reopen and re-reads everything.
//
// Any mismatch exits non-zero with a clear message.
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
	dataDir := flag.String("data-dir", "/tmp/woodpecker_data", "Data directory for local storage")
	etcdEndpoint := flag.String("etcd", "localhost:2379", "etcd endpoint host:port")
	minioEndpoint := flag.String("minio-endpoint", "localhost:9000", "MinIO endpoint host:port")
	minioBucket := flag.String("bucket", "a-bucket", "MinIO bucket name")
	manifestIn := flag.String("manifest-in", "", "Path to input manifest JSON (required)")
	flag.Parse()

	if *manifestIn == "" {
		fmt.Fprintln(os.Stderr, "ERROR: --manifest-in is required")
		os.Exit(1)
	}

	ctx := context.Background()

	manifest, err := upgrade.LoadManifest(*manifestIn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: failed to load manifest: %v\n", err)
		os.Exit(1)
	}
	if len(manifest.Logs) == 0 {
		fmt.Fprintln(os.Stderr, "FATAL: manifest contains no logs")
		os.Exit(1)
	}

	cfg, err := buildConfig(*storage, *dataDir, *etcdEndpoint, *minioEndpoint, *minioBucket)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: failed to build config: %v\n", err)
		os.Exit(1)
	}

	phases := []struct {
		name string
		run  func(context.Context, *config.Configuration, *upgrade.Manifest) error
	}{
		{"Phase 1: legacy metadata prefix detection", testLegacyPrefixDetection},
		{"Phase 2: byte/CRC/id read-back of all pre-upgrade entries", validateReadBack},
		{"Phase 3: v5 segment footer parsing (LAC defaults)", validateV5SegmentParsing},
		{"Phase 4: post-upgrade append + segment roll", testPostUpgradeAppendAndRoll},
		{"Phase 5: recovery (close/reopen/re-read)", testRecovery},
	}

	for _, p := range phases {
		fmt.Printf("[VERIFIER] %s ...\n", p.name)
		if err := p.run(ctx, cfg, manifest); err != nil {
			fmt.Fprintf(os.Stderr, "FAIL: %s: %v\n", p.name, err)
			os.Exit(1)
		}
		fmt.Printf("[VERIFIER] PASS: %s\n", p.name)
	}

	fmt.Println("[VERIFIER] ALL PHASES PASSED")
}

// buildConfig constructs the HEAD Configuration from CLI flags, starting from
// built-in defaults (so the metadata prefix matches the writer's "woodpecker"
// default and the legacy-prefix bridge has a chance to fire).
func buildConfig(storageType, dataDir, etcdEndpoint, minioEndpoint, minioBucket string) (*config.Configuration, error) {
	cfg, err := config.NewConfiguration()
	if err != nil {
		return nil, fmt.Errorf("new configuration: %w", err)
	}

	cfg.Woodpecker.Storage.Type = storageType
	if storageType == "local" {
		cfg.Woodpecker.Storage.RootPath = dataDir
	}

	cfg.Etcd.Endpoints = []string{etcdEndpoint}

	if storageType == "minio" {
		host, port := splitHostPort(minioEndpoint, "localhost", 9000)
		cfg.Minio.Address = host
		cfg.Minio.Port = port
		cfg.Minio.BucketName = minioBucket
	}

	return cfg, nil
}

// splitHostPort splits "host:port", falling back to defaults on parse failure.
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

// newClient creates an embed client and returns a cleanup func that closes the
// client and stops the embed logstore singleton (required between phases since
// the logstore is a process-level singleton).
func newClient(ctx context.Context, cfg *config.Configuration) (woodpecker.Client, func(), error) {
	client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("create embed client: %w", err)
	}
	cleanup := func() {
		if closeErr := client.Close(ctx); closeErr != nil {
			fmt.Fprintf(os.Stderr, "[WARN] close client: %v\n", closeErr)
		}
		if stopErr := woodpecker.StopEmbedLogStore(); stopErr != nil {
			fmt.Fprintf(os.Stderr, "[WARN] stop embed logstore: %v\n", stopErr)
		}
	}
	return client, cleanup, nil
}

// readAll reads every entry of a log from the earliest position until the reader
// stops returning messages, returning the entries in order.
func readAll(ctx context.Context, logHandle log.LogHandle, readerName string, expected int) ([]*log.LogMessage, error) {
	earliest := log.EarliestLogMessageID()
	reader, err := logHandle.OpenLogReader(ctx, &earliest, readerName)
	if err != nil {
		return nil, fmt.Errorf("open reader: %w", err)
	}
	defer func() { _ = reader.Close(ctx) }()

	msgs := make([]*log.LogMessage, 0, expected)
	for {
		// Bound each ReadNext so a tail-blocking read past the last entry does
		// not hang the verifier forever.
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		msg, readErr := reader.ReadNext(readCtx)
		cancel()
		if readErr != nil {
			break
		}
		if msg == nil {
			break
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// findFirstLog returns a deterministic "first" log name from the manifest
// (manifest.Logs preserves writer order).
func firstLogName(manifest *upgrade.Manifest) string {
	return manifest.Logs[0].Name
}

// testLegacyPrefixDetection proves the legacy auto-detect bridge fired: the
// writer wrote bare-key metadata under the hardcoded "woodpecker" prefix, and
// HEAD must still discover those logs and read at least one entry.
func testLegacyPrefixDetection(ctx context.Context, cfg *config.Configuration, manifest *upgrade.Manifest) error {
	client, cleanup, err := newClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	logs, err := client.GetAllLogs(ctx)
	if err != nil {
		return fmt.Errorf("get all logs: %w", err)
	}
	if len(logs) == 0 {
		return fmt.Errorf("no logs found (legacy prefix bridge did not surface v0.1.13 data)")
	}

	logHandle, err := client.OpenLog(ctx, logs[0])
	if err != nil {
		return fmt.Errorf("open log %q: %w", logs[0], err)
	}
	defer func() { _ = logHandle.Close(ctx) }()

	earliest := log.EarliestLogMessageID()
	reader, err := logHandle.OpenLogReader(ctx, &earliest, "verifier-legacy")
	if err != nil {
		return fmt.Errorf("open log reader: %w", err)
	}
	defer func() { _ = reader.Close(ctx) }()

	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	msg, err := reader.ReadNext(readCtx)
	if err != nil {
		return fmt.Errorf("read first entry: %w", err)
	}
	if msg == nil {
		return fmt.Errorf("read first entry returned nil")
	}
	return nil
}

// validateReadBack reads all entries from all logs and verifies each entry is
// byte-length, CRC, segment id and entry id identical to the manifest, and that
// the payload matches the deterministic prefix+index rule.
func validateReadBack(ctx context.Context, cfg *config.Configuration, manifest *upgrade.Manifest) error {
	client, cleanup, err := newClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	for _, expectedLog := range manifest.Logs {
		if err := validateLogReadBack(ctx, client, expectedLog); err != nil {
			return fmt.Errorf("log %q: %w", expectedLog.Name, err)
		}
	}
	return nil
}

func validateLogReadBack(ctx context.Context, client woodpecker.Client, expectedLog upgrade.LogRecord) error {
	logHandle, err := client.OpenLog(ctx, expectedLog.Name)
	if err != nil {
		return fmt.Errorf("open log: %w", err)
	}
	defer func() { _ = logHandle.Close(ctx) }()

	msgs, err := readAll(ctx, logHandle, "verifier-readback", len(expectedLog.Entries))
	if err != nil {
		return err
	}
	if len(msgs) < len(expectedLog.Entries) {
		return fmt.Errorf("read %d entries, expected %d", len(msgs), len(expectedLog.Entries))
	}

	for idx, expected := range expectedLog.Entries {
		msg := msgs[idx]
		if msg == nil || msg.Id == nil {
			return fmt.Errorf("entry %d: nil message/id", idx)
		}
		if len(msg.Payload) != expected.PayloadLen {
			return fmt.Errorf("entry %d: payload length mismatch (got %d, expected %d)", idx, len(msg.Payload), expected.PayloadLen)
		}
		if crc := upgrade.ComputePayloadCRC32(msg.Payload); crc != expected.CRC32 {
			return fmt.Errorf("entry %d: CRC mismatch (got %d, expected %d)", idx, crc, expected.CRC32)
		}
		wantPayload := fmt.Sprintf("%s%d", expectedLog.PayloadPrefix, idx)
		if string(msg.Payload) != wantPayload {
			return fmt.Errorf("entry %d: payload mismatch (got %q, expected %q)", idx, string(msg.Payload), wantPayload)
		}
		if msg.Id.SegmentId != expected.SegmentId {
			return fmt.Errorf("entry %d: SegmentId mismatch (got %d, expected %d)", idx, msg.Id.SegmentId, expected.SegmentId)
		}
		if msg.Id.EntryId != expected.EntryId {
			return fmt.Errorf("entry %d: EntryId mismatch (got %d, expected %d)", idx, msg.Id.EntryId, expected.EntryId)
		}
	}
	return nil
}

// validateV5SegmentParsing reads back the first log to exercise HEAD's v5 footer
// parser (the dataset was sealed by v0.1.13 with FormatVersion 5 footers).
func validateV5SegmentParsing(ctx context.Context, cfg *config.Configuration, manifest *upgrade.Manifest) error {
	client, cleanup, err := newClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	name := firstLogName(manifest)
	logHandle, err := client.OpenLog(ctx, name)
	if err != nil {
		return fmt.Errorf("open log %q: %w", name, err)
	}
	defer func() { _ = logHandle.Close(ctx) }()

	msgs, err := readAll(ctx, logHandle, "verifier-v5", 1)
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		return fmt.Errorf("log %q: read 0 entries (v5 footer parsing may have failed)", name)
	}
	return nil
}

// testPostUpgradeAppendAndRoll appends new entries to an existing log (forcing a
// segment roll so a v6 footer is written beside the old v5 footers), then reads
// the whole log back and confirms old + new entries are present.
func testPostUpgradeAppendAndRoll(ctx context.Context, cfg *config.Configuration, manifest *upgrade.Manifest) error {
	client, cleanup, err := newClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	name := firstLogName(manifest)
	originalCount := len(manifest.Logs[0].Entries)
	const newEntries = 2

	logHandle, err := client.OpenLog(ctx, name)
	if err != nil {
		return fmt.Errorf("open log %q: %w", name, err)
	}

	writer, err := logHandle.OpenLogWriter(ctx)
	if err != nil {
		_ = logHandle.Close(ctx)
		return fmt.Errorf("open writer: %w", err)
	}

	for i := 0; i < newEntries; i++ {
		result := writer.Write(ctx, &log.WriteMessage{
			Payload: []byte(fmt.Sprintf("post-upgrade-entry-%d", i)),
		})
		if result.Err != nil {
			_ = writer.Close(ctx)
			_ = logHandle.Close(ctx)
			return fmt.Errorf("write new entry %d: %w", i, result.Err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		_ = logHandle.Close(ctx)
		return fmt.Errorf("close writer: %w", err)
	}
	_ = logHandle.Close(ctx)

	// Re-open the log and read all entries (old + new).
	logHandle2, err := client.OpenLog(ctx, name)
	if err != nil {
		return fmt.Errorf("reopen log after append: %w", err)
	}
	defer func() { _ = logHandle2.Close(ctx) }()

	expectedTotal := originalCount + newEntries
	msgs, err := readAll(ctx, logHandle2, "verifier-append", expectedTotal)
	if err != nil {
		return err
	}
	if len(msgs) < expectedTotal {
		return fmt.Errorf("post-upgrade read %d entries, expected at least %d (orig %d + %d new)",
			len(msgs), expectedTotal, originalCount, newEntries)
	}
	return nil
}

// testRecovery closes the client fully (including the embed logstore singleton),
// reopens a fresh client, and re-reads every log to prove durable recovery.
func testRecovery(ctx context.Context, cfg *config.Configuration, manifest *upgrade.Manifest) error {
	// First pass.
	if err := readAllLogs(ctx, cfg, manifest, "verifier-recovery-1"); err != nil {
		return fmt.Errorf("first pass: %w", err)
	}

	// Allow the singleton shutdown to settle before reopening.
	time.Sleep(500 * time.Millisecond)

	// Recovery pass with a brand-new client.
	if err := readAllLogs(ctx, cfg, manifest, "verifier-recovery-2"); err != nil {
		return fmt.Errorf("recovery pass: %w", err)
	}
	return nil
}

func readAllLogs(ctx context.Context, cfg *config.Configuration, manifest *upgrade.Manifest, readerName string) error {
	client, cleanup, err := newClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	for _, expectedLog := range manifest.Logs {
		logHandle, err := client.OpenLog(ctx, expectedLog.Name)
		if err != nil {
			return fmt.Errorf("open log %q: %w", expectedLog.Name, err)
		}
		msgs, readErr := readAll(ctx, logHandle, readerName, len(expectedLog.Entries))
		_ = logHandle.Close(ctx)
		if readErr != nil {
			return fmt.Errorf("log %q: %w", expectedLog.Name, readErr)
		}
		if len(msgs) < len(expectedLog.Entries) {
			return fmt.Errorf("log %q: read %d entries, expected at least %d", expectedLog.Name, len(msgs), len(expectedLog.Entries))
		}
	}
	return nil
}

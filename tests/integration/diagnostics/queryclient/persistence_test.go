// Licensed to the LF AI & Data foundation under the Apache License, Version 2.0.
package main

import (
	"context"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPersistenceUsesActualFlushBoundaryAndStrongReads(t *testing.T) {
	f := &fixture{}
	w := worker{api: client(t, f), options: options{collection: "persist_accept_20261027T000000Z_aabbccdd", timeout: time.Second, startID: 64, batches: 2, rowsPerBatch: 4, duration: time.Millisecond}}
	if err := w.flushPhase(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(w.records) != 8 || len(f.flushes) != 2 || len(f.queries) != 2 || len(f.search) != 2 {
		t.Fatal("missing actual insert, Strong reads or flush RPCs")
	}
	for _, r := range f.flushes {
		if r.GetDbName() != "default" || len(r.GetCollectionNames()) != 1 || r.GetCollectionNames()[0] != w.options.collection {
			t.Fatal("flush broadened to another collection")
		}
	}
	for _, r := range w.records {
		if !r.OK || (r.Operation != "insert" && r.AckTimestamp != serverTS) {
			t.Fatal("actual acknowledgement lost")
		}
		if r.Operation == "flush" && (r.FlushTimestamp != serverTS+1 || len(r.SegmentIDs) != 1 || len(r.SegmentIDs[0]) < 16) {
			t.Fatal("server flush boundary or large segment identity lost")
		}
		if r.Operation == "insert" && (r.Rows != 4 || len(r.IDs) != 4) {
			t.Fatal("actual batched acknowledgement lost")
		}
		if (r.Operation == "query" || r.Operation == "search") && (r.Consistency != "Strong" || r.GuaranteeTimestamp != 0) {
			t.Fatal("persistence read was given a future fence")
		}
	}
}

func TestPersistenceRejectsInvalidBoundaryTransportAndDeadline(t *testing.T) {
	for _, scenario := range []string{"boundary", "transport", "deadline"} {
		t.Run(scenario, func(t *testing.T) {
			f := &fixture{badFlush: scenario == "boundary", fail: scenario == "transport", block: scenario == "deadline"}
			w := worker{api: client(t, f), options: options{collection: "owned", timeout: 20 * time.Millisecond}}
			if err := w.flush(context.Background(), 64, serverTS); err == nil || strings.Contains(err.Error(), "sentinel-private") {
				t.Fatal("bad boundary, transport or deadline was accepted or leaked")
			}
			if len(w.records) != 1 || w.records[0].OK {
				t.Fatal("failed flush was not retained")
			}
		})
	}
}

func TestPersistenceCannotFlushQueryCollectionOrUseFutureFence(t *testing.T) {
	for _, o := range []options{
		{collection: "query_accept_20261027T000000Z_aabbccdd", mode: "flush", phase: "baseline", parallel: 1},
		{collection: "persist_accept_20261027T000000Z_aabbccdd", mode: "flush", phase: "future", parallel: 1},
		{collection: "persist_accept_20261027T000000Z_aabbccdd", mode: "flush", phase: "baseline", parallel: 2},
		{collection: "persist_accept_20261027T000000Z_aabbccdd", mode: "phase", phase: "baseline", parallel: 1},
	} {
		o.endpoint, o.output, o.startID, o.batches, o.rows = "127.0.0.1:1", "unused", 64, 24, 64
		o.duration, o.timeout, o.offsetMS = 30*time.Second, time.Second, 2000
		if err := execute(o); err == nil || !strings.Contains(err.Error(), "persistence") {
			t.Fatal("invalid persistence selection was not rejected locally", err)
		}
	}
}

func TestExtremeMarkerIDCannotOverflowTheOwnedRowBound(t *testing.T) {
	for _, mode := range []string{"phase", "flush"} {
		collection := "query_accept_20261027T000000Z_aabbccdd"
		if mode == "flush" {
			collection = "persist_accept_20261027T000000Z_aabbccdd"
		}
		o := options{endpoint: "127.0.0.1:1", collection: collection, output: filepath.Join(t.TempDir(), "new.json"), execute: true,
			mode: mode, phase: "baseline", startID: math.MaxInt, batches: 24, parallel: 1, rows: 64, rowsPerBatch: 1,
			duration: 30 * time.Second, timeout: time.Second, offsetMS: 2000}
		if err := execute(o); err == nil || !strings.Contains(err.Error(), "bounds") {
			t.Fatal("overflowing marker ID was not rejected before I/O", err)
		}
	}
}

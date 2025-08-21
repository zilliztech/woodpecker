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

package integration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// testCases builder per-test to align with client_test.go style

// TestInternalLogWriter_BasicOpenWriteCloseReopen validates open->write->close->reopen flow
func TestInternalLogWriter_BasicOpenWriteCloseReopen(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestInternalLogWriter_BasicOpenWriteCloseReopen")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "",
			rootPath:    "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			cfg.Log.Level = "debug"
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)
			t.Cleanup(func() { _ = client.Close(ctx) })

			logName := "test-internal-writer-basic-" + tc.name + time.Now().Format("20060102150405")
			err = client.CreateLog(ctx, logName)
			if err != nil && !werr.ErrLogHandleLogAlreadyExists.Is(err) {
				assert.NoError(t, err)
			}

			lh, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			w1, err := lh.OpenInternalLogWriter(ctx)
			assert.NoError(t, err)

			res := w1.Write(ctx, &log.WriteMessage{Payload: []byte("m1")})
			assert.NoError(t, res.Err)
			first := res.LogMessageId
			assert.NotNil(t, first)

			assert.NoError(t, w1.Close(ctx))

			w2, err := lh.OpenInternalLogWriter(ctx)
			assert.NoError(t, err)
			defer w2.Close(ctx)

			res2 := w2.Write(ctx, &log.WriteMessage{Payload: []byte("m2")})
			assert.NoError(t, res2.Err)
			assert.NotNil(t, res2.LogMessageId)

			flushInterval := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval
			time.Sleep(time.Duration(1000 + flushInterval*int(time.Millisecond)))

			r, err := lh.OpenLogReader(ctx, &log.LogMessageId{SegmentId: first.SegmentId, EntryId: first.EntryId}, "basic")
			assert.NoError(t, err)
			defer r.Close(ctx)

			m1, err := r.ReadNext(ctx)
			assert.NoError(t, err)
			assert.Equal(t, "m1", string(m1.Payload))

			m2, err := r.ReadNext(ctx)
			assert.NoError(t, err)
			assert.Equal(t, "m2", string(m2.Payload))

			// shutdown embed server
			err = woodpecker.StopEmbedLogStore()
			assert.NoError(t, err)
		})
	}
}

// TestInternalLogWriter_PreemptionByNewOpen ensures new internal writer fences current active segment and invalidates old writer
func TestInternalLogWriter_PreemptionByNewOpen(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestInternalLogWriter_PreemptionByNewOpen")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "",
			rootPath:    "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)
			t.Cleanup(func() { _ = client.Close(ctx) })

			logName := "test-internal-writer-preempt-" + tc.name + time.Now().Format("20060102150405")
			err = client.CreateLog(ctx, logName)
			if err != nil && !werr.ErrLogHandleLogAlreadyExists.Is(err) {
				assert.NoError(t, err)
			}

			lh, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			w1, err := lh.OpenInternalLogWriter(ctx)
			assert.NoError(t, err)
			res := w1.Write(ctx, &log.WriteMessage{Payload: []byte("a1")})
			assert.NoError(t, res.Err)

			lh2, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			w2, err := lh2.OpenInternalLogWriter(ctx)
			assert.NoError(t, err)
			defer w2.Close(ctx)

			resOld1 := w1.Write(ctx, &log.WriteMessage{Payload: []byte("a2-should-fail")})
			assert.Error(t, resOld1.Err)
			resOld2 := w1.Write(ctx, &log.WriteMessage{Payload: []byte("a3-should-fail-fast")})
			assert.Error(t, resOld2.Err)
			assert.True(t, werr.ErrLogWriterLockLost.Is(resOld2.Err) || werr.ErrSegmentFenced.Is(resOld2.Err) || werr.ErrStorageNotWritable.Is(resOld2.Err))

			resNew := w2.Write(ctx, &log.WriteMessage{Payload: []byte("b1")})
			assert.NoError(t, resNew.Err)

			assert.NoError(t, w1.Close(ctx))

			flushInterval := cfg.Woodpecker.Logstore.SegmentSyncPolicy.MaxInterval
			time.Sleep(time.Duration(1000 + flushInterval*int(time.Millisecond)))

			r, err := lh.OpenLogReader(ctx, &log.LogMessageId{SegmentId: res.LogMessageId.SegmentId, EntryId: res.LogMessageId.EntryId}, "preempt")
			assert.NoError(t, err)
			defer r.Close(ctx)

			m1, err := r.ReadNext(ctx)
			assert.NoError(t, err)
			assert.Equal(t, "a1", string(m1.Payload))

			m2, err := r.ReadNext(ctx)
			assert.NoError(t, err)
			assert.Equal(t, "b1", string(m2.Payload))

			// shutdown embed server
			err = woodpecker.StopEmbedLogStore()
			assert.NoError(t, err)
		})
	}
}

// TestInternalLogWriter_FinalizeIdempotency validates calling Complete on the active segment multiple times
func TestInternalLogWriter_FinalizeIdempotency(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestInternalLogWriter_FinalizeIdempotency")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "",
			rootPath:    "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)
			t.Cleanup(func() { _ = client.Close(ctx) })

			logName := "test-internal-writer-finalize-idem-" + tc.name + time.Now().Format("20060102150405")
			err = client.CreateLog(ctx, logName)
			if err != nil && !werr.ErrLogHandleLogAlreadyExists.Is(err) {
				assert.NoError(t, err)
			}

			lh, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			w, err := lh.OpenInternalLogWriter(ctx)
			assert.NoError(t, err)
			defer w.Close(ctx)

			for i := 0; i < 3; i++ {
				r := w.Write(ctx, &log.WriteMessage{Payload: []byte("x")})
				assert.NoError(t, r.Err)
			}

			seg, err := lh.GetOrCreateWritableSegmentHandle(ctx, func(context.Context, string) {})
			assert.NoError(t, err)
			last1, err := seg.Complete(ctx)
			assert.NoError(t, err)
			last2, err := seg.Complete(ctx)
			assert.NoError(t, err)
			assert.Equal(t, last1, last2)

			res := w.Write(ctx, &log.WriteMessage{Payload: []byte("after-finalize")})
			assert.Error(t, res.Err)
			res2 := w.Write(ctx, &log.WriteMessage{Payload: []byte("after-finalize-2")})
			assert.Error(t, res2.Err)

			// shutdown embed server
			err = woodpecker.StopEmbedLogStore()
			assert.NoError(t, err)
		})
	}
}

// TestInternalLogWriter_FinalizeIdempotency_AcrossProcesses
// finalize with writer1, close it, then open a new writer (simulating another process) and finalize again
func TestInternalLogWriter_FinalizeIdempotency_AcrossProcesses(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestInternalLogWriter_FinalizeIdempotency_AcrossProcesses")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "",
			rootPath:    "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client1, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)
			t.Cleanup(func() { _ = client1.Close(ctx) })

			logName := "test-internal-writer-finalize-idem-xproc-" + tc.name + time.Now().Format("20060102150405")
			err = client1.CreateLog(ctx, logName)
			if err != nil && !werr.ErrLogHandleLogAlreadyExists.Is(err) {
				assert.NoError(t, err)
			}

			lh1, err := client1.OpenLog(ctx, logName)
			assert.NoError(t, err)

			w1, err := lh1.OpenInternalLogWriter(ctx)
			assert.NoError(t, err)

			for i := 0; i < 2; i++ {
				r := w1.Write(ctx, &log.WriteMessage{Payload: []byte("p")})
				assert.NoError(t, r.Err)
			}

			seg1, err := lh1.GetOrCreateWritableSegmentHandle(ctx, func(context.Context, string) {})
			assert.NoError(t, err)
			segId := seg1.GetId(ctx)

			last1, err := seg1.Complete(ctx)
			assert.NoError(t, err)
			assert.Equal(t, last1, int64(1))

			assert.NoError(t, w1.Close(ctx))

			err = woodpecker.StopEmbedLogStore()
			assert.NoError(t, err)

			client2, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)
			t.Cleanup(func() { _ = client2.Close(ctx) })

			lh2, err := client2.OpenLog(ctx, logName)
			assert.NoError(t, err)

			w2, err := lh2.OpenInternalLogWriter(ctx)
			assert.NoError(t, err)
			defer w2.Close(ctx)

			segRO2, err := lh2.GetExistsReadonlySegmentHandle(ctx, segId)
			assert.NoError(t, err)
			assert.NotNil(t, segRO2)

			// idempotent finalize
			last2, err := segRO2.Complete(ctx)
			assert.NoError(t, err)
			assert.Equal(t, last2, int64(1))

			// shutdown embed server
			err = woodpecker.StopEmbedLogStore()
			assert.NoError(t, err)
		})
	}
}

// TestInternalLogWriter_FinalizeIdempotency_AcrossWriters_NoRestart
// finalize with writer1, close it, then open a new writer in a different client without stopping embed server
func TestInternalLogWriter_FinalizeIdempotency_AcrossWriters_NoRestart(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestInternalLogWriter_FinalizeIdempotency_AcrossWriters_NoRestart")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "",
			rootPath:    "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client1, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)
			t.Cleanup(func() { _ = client1.Close(ctx) })

			logName := "test-internal-writer-finalize-idem-xproc-norestart-" + tc.name + time.Now().Format("20060102150405")
			err = client1.CreateLog(ctx, logName)
			if err != nil && !werr.ErrLogHandleLogAlreadyExists.Is(err) {
				assert.NoError(t, err)
			}

			lh1, err := client1.OpenLog(ctx, logName)
			assert.NoError(t, err)

			w1, err := lh1.OpenInternalLogWriter(ctx)
			assert.NoError(t, err)

			for i := 0; i < 2; i++ {
				r := w1.Write(ctx, &log.WriteMessage{Payload: []byte("q")})
				assert.NoError(t, r.Err)
			}

			seg1, err := lh1.GetOrCreateWritableSegmentHandle(ctx, func(context.Context, string) {})
			assert.NoError(t, err)
			segId := seg1.GetId(ctx)

			last1, err := seg1.Complete(ctx)
			assert.NoError(t, err)

			assert.NoError(t, w1.Close(ctx))

			client2, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)
			t.Cleanup(func() { _ = client2.Close(ctx) })

			lh2, err := client2.OpenLog(ctx, logName)
			assert.NoError(t, err)

			w2, err := lh2.OpenInternalLogWriter(ctx)
			assert.NoError(t, err)
			defer w2.Close(ctx)

			segRO2, err := lh2.GetExistsReadonlySegmentHandle(ctx, segId)
			assert.NoError(t, err)
			assert.NotNil(t, segRO2)

			last2, err := segRO2.Complete(ctx)
			assert.NoError(t, err)
			assert.Equal(t, last1, last2)

			// shutdown embed server
			err = woodpecker.StopEmbedLogStore()
			assert.NoError(t, err)
		})
	}
}

// TestInternalLogWriter_CloseThenFinalize ensures finalizing after close is harmless
func TestInternalLogWriter_CloseThenFinalize(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestInternalLogWriter_CloseThenFinalize")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "",
			rootPath:    "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)

			logName := "test-internal-writer-close-then-finalize-" + tc.name + time.Now().Format("20060102150405")
			err = client.CreateLog(ctx, logName)
			if err != nil && !werr.ErrLogHandleLogAlreadyExists.Is(err) {
				assert.NoError(t, err)
			}

			lh, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			w, err := lh.OpenInternalLogWriter(ctx)
			assert.NoError(t, err)

			r := w.Write(ctx, &log.WriteMessage{Payload: []byte("y1")})
			assert.NoError(t, r.Err)
			r2 := w.Write(ctx, &log.WriteMessage{Payload: []byte("y2")})
			assert.NoError(t, r2.Err)

			seg, err := lh.GetOrCreateWritableSegmentHandle(ctx, func(context.Context, string) {})
			assert.NoError(t, err)
			segId := seg.GetId(ctx)

			assert.NoError(t, w.Close(ctx))

			segRO, err := lh.GetExistsReadonlySegmentHandle(ctx, segId)
			assert.NoError(t, err)

			// can normally complete after close
			if segRO != nil {
				lastId, err := segRO.Complete(ctx)
				assert.NoError(t, err)
				assert.Equal(t, lastId, int64(1))
			}

			// can normally complete again
			if segRO != nil {
				lastId, err := segRO.Complete(ctx)
				assert.NoError(t, err)
				assert.Equal(t, lastId, int64(1))
			}

			// shutdown embed server
			err = woodpecker.StopEmbedLogStore()
			assert.NoError(t, err)
		})
	}
}

// TestInternalLogWriter_FinalizeThenClose ensures closing after finalize works
func TestInternalLogWriter_FinalizeThenClose(t *testing.T) {
	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestInternalLogWriter_FinalizeThenClose")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
	}{
		{
			name:        "LocalFsStorage",
			storageType: "local",
			rootPath:    rootPath,
		},
		{
			name:        "ObjectStorage",
			storageType: "",
			rootPath:    "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
			assert.NoError(t, err)

			logName := "test-internal-writer-finalize-then-close-" + tc.name + time.Now().Format("20060102150405")
			err = client.CreateLog(ctx, logName)
			if err != nil && !werr.ErrLogHandleLogAlreadyExists.Is(err) {
				assert.NoError(t, err)
			}

			lh, err := client.OpenLog(ctx, logName)
			assert.NoError(t, err)

			w, err := lh.OpenInternalLogWriter(ctx)
			assert.NoError(t, err)

			for i := 0; i < 2; i++ {
				r := w.Write(ctx, &log.WriteMessage{Payload: []byte("z")})
				assert.NoError(t, r.Err)
			}

			seg, err := lh.GetOrCreateWritableSegmentHandle(ctx, func(context.Context, string) {})
			assert.NoError(t, err)
			{
				// complete success
				lastId, err := seg.Complete(ctx)
				assert.NoError(t, err)
				assert.Equal(t, lastId, int64(1))
			}
			{
				// complete again
				lastId, err := seg.Complete(ctx)
				assert.NoError(t, err)
				assert.Equal(t, lastId, int64(1))
			}

			err = w.Close(ctx)
			assert.NoError(t, err)

			// shutdown embed server
			err = woodpecker.StopEmbedLogStore()
			assert.NoError(t, err)
		})
	}
}

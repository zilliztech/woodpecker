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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/common/logger"
	"github.com/zilliztech/woodpecker/tests/utils"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// manualLogVolumeEnv gates the log-volume inspection harnesses in this file.
// They are NOT assertion tests — they emit debug logs bracketed by
// WP_IDLE_START / WP_IDLE_END so idle-spin log volume can be eyeballed/grepped
// (see milvus-io/milvus#48976, zilliztech/woodpecker#189 & #190). They need
// etcd + minio (+ a mini-cluster for service mode) and are skipped by default;
// run one explicitly with:
//
//	WP_LOG_VOLUME_MANUAL=1 go test ./tests/integration/ -run TestIdleLogVolume/ObjectStorage -v
const manualLogVolumeEnv = "WP_LOG_VOLUME_MANUAL"

func skipUnlessManualLogVolume(t *testing.T) {
	if os.Getenv(manualLogVolumeEnv) == "" {
		t.Skip("manual log-volume inspection harness; set " + manualLogVolumeEnv + "=1 to run")
	}
}

// TestIdleLogVolume reproduces the "idle-spin" debug logging problem reported in
// milvus-io/milvus#48976. It writes only a handful of entries and then leaves the
// writer idle for a fixed window. The body of the idle window is bracketed by
// WP_IDLE_START / WP_IDLE_END markers so the number of debug lines emitted while
// NOTHING is being written can be counted precisely.
//
// Run per mode and grep between the markers, e.g.:
//
//	go test ./tests/integration/ -run 'TestIdleLogVolume/ObjectStorage' -v 2>&1 | tee /tmp/wp_object.log
func TestIdleLogVolume(t *testing.T) {
	skipUnlessManualLogVolume(t)
	const (
		writeCount = 5
		idleWindow = 8 * time.Second
	)

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestIdleLogVolume")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
		needCluster bool
	}{
		{name: "LocalFsStorage", storageType: "local", rootPath: rootPath, needCluster: false},
		{name: "ObjectStorage", storageType: "", rootPath: "", needCluster: false},
		{name: "ServiceStorage", storageType: "service", rootPath: rootPath + "_service", needCluster: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			cfg.Log.Level = "debug"
			logger.InitLogger(cfg)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			var client woodpecker.Client
			if tc.needCluster {
				const nodeCount = 3
				cluster, ccfg, _, serviceSeeds := utils.StartMiniClusterWithCfg(t, nodeCount, tc.rootPath, cfg)
				ccfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = serviceSeeds
				cfg = ccfg
				defer cluster.StopMultiNodeCluster(t)

				etcdCli, etcdErr := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
				assert.NoError(t, etcdErr)
				defer etcdCli.Close()

				client, err = woodpecker.NewClient(ctx, cfg, etcdCli, true)
				assert.NoError(t, err)
			} else {
				client, err = woodpecker.NewEmbedClientFromConfig(ctx, cfg)
				assert.NoError(t, err)
				defer func() {
					_ = woodpecker.StopEmbedLogStore()
				}()
			}
			defer client.Close(context.TODO())

			logName := "TestIdleLogVolume_" + t.Name() + "_" + time.Now().Format("20060102150405")
			_ = client.CreateLog(ctx, logName)
			logHandle, openErr := client.OpenLog(ctx, logName)
			assert.NoError(t, openErr)

			logWriter, openWriterErr := logHandle.OpenLogWriter(ctx)
			assert.NoError(t, openWriterErr)

			// --- brief active burst: write a few entries ---
			resultChan := make([]<-chan *log.WriteResult, writeCount)
			for i := 0; i < writeCount; i++ {
				resultChan[i] = logWriter.WriteAsync(ctx, &log.WriteMessage{
					Payload: []byte(fmt.Sprintf("hello world %d", i)),
				})
			}
			for i := 0; i < writeCount; i++ {
				res := <-resultChan[i]
				assert.NoError(t, res.Err)
			}
			// let the burst settle / flush
			time.Sleep(1 * time.Second)

			// --- idle window: write NOTHING, just let the sync loop tick ---
			logger.Ctx(ctx).Info("WP_IDLE_START", zap.String("mode", tc.name), zap.Int("writeCount", writeCount), zap.Duration("idleWindow", idleWindow))
			time.Sleep(idleWindow)
			logger.Ctx(ctx).Info("WP_IDLE_END", zap.String("mode", tc.name))

			closeErr := logWriter.Close(ctx)
			assert.NoError(t, closeErr)
		})
	}
}

// TestIdleTailReadLogVolume reproduces the read-side "idle-spin" logging from
// milvus-io/milvus#48976 (reader_impl.go: "submitted reading task for block",
// "processed block data", "block data integrity verified", ~670K each).
//
// It writes a few entries, keeps the writer OPEN (so the segment stays active /
// incomplete — the tail-read case), catches a reader up to the tail, then leaves
// the reader polling the idle tail (no new writes) for a fixed window. The body
// of the idle window is bracketed by WP_IDLE_START / WP_IDLE_END so the DEBUG
// lines emitted while there is NOTHING new to read can be counted.
//
//	go test ./tests/integration/ -run 'TestIdleTailReadLogVolume/ObjectStorage' -v 2>&1 | tee /tmp/wp_tailread_object.log
func TestIdleTailReadLogVolume(t *testing.T) {
	skipUnlessManualLogVolume(t)
	const (
		writeCount = 5
		idleWindow = 8 * time.Second
	)

	tmpDir := t.TempDir()
	rootPath := filepath.Join(tmpDir, "TestIdleTailReadLogVolume")
	testCases := []struct {
		name        string
		storageType string
		rootPath    string
		needCluster bool
	}{
		{name: "LocalFsStorage", storageType: "local", rootPath: rootPath, needCluster: false},
		{name: "ObjectStorage", storageType: "", rootPath: "", needCluster: false},
		{name: "ServiceStorage", storageType: "service", rootPath: rootPath + "_service", needCluster: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
			assert.NoError(t, err)
			cfg.Log.Level = "debug"
			logger.InitLogger(cfg)

			if tc.storageType != "" {
				cfg.Woodpecker.Storage.Type = tc.storageType
			}
			if tc.rootPath != "" {
				cfg.Woodpecker.Storage.RootPath = tc.rootPath
			}

			var client woodpecker.Client
			if tc.needCluster {
				const nodeCount = 3
				cluster, ccfg, _, serviceSeeds := utils.StartMiniClusterWithCfg(t, nodeCount, tc.rootPath, cfg)
				ccfg.Woodpecker.Client.Quorum.BufferPools[0].Seeds = serviceSeeds
				cfg = ccfg
				defer cluster.StopMultiNodeCluster(t)

				etcdCli, etcdErr := etcd.GetRemoteEtcdClient(cfg.Etcd.GetEndpoints())
				assert.NoError(t, etcdErr)
				defer etcdCli.Close()

				client, err = woodpecker.NewClient(ctx, cfg, etcdCli, true)
				assert.NoError(t, err)
			} else {
				client, err = woodpecker.NewEmbedClientFromConfig(ctx, cfg)
				assert.NoError(t, err)
				defer func() {
					_ = woodpecker.StopEmbedLogStore()
				}()
			}
			defer client.Close(context.TODO())

			logName := "TestIdleTailReadLogVolume_" + t.Name() + "_" + time.Now().Format("20060102150405")
			_ = client.CreateLog(ctx, logName)
			logHandle, openErr := client.OpenLog(ctx, logName)
			assert.NoError(t, openErr)

			// writer stays OPEN for the whole test -> segment stays active (tail-read case)
			logWriter, openWriterErr := logHandle.OpenLogWriter(ctx)
			assert.NoError(t, openWriterErr)
			defer logWriter.Close(ctx)

			// --- write a few entries and confirm them ---
			resultChan := make([]<-chan *log.WriteResult, writeCount)
			for i := 0; i < writeCount; i++ {
				resultChan[i] = logWriter.WriteAsync(ctx, &log.WriteMessage{
					Payload: []byte(fmt.Sprintf("hello world %d", i)),
				})
			}
			for i := 0; i < writeCount; i++ {
				res := <-resultChan[i]
				assert.NoError(t, res.Err)
			}

			// --- open a tail reader and catch up to the tail ---
			earliest := log.EarliestLogMessageID()
			logReader, openReaderErr := logHandle.OpenLogReader(ctx, &earliest, "idle-tail-reader")
			assert.NoError(t, openReaderErr)
			defer logReader.Close(ctx)
			for i := 0; i < writeCount; i++ {
				readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				_, rErr := logReader.ReadNext(readCtx)
				cancel()
				assert.NoError(t, rErr)
			}

			// --- idle window: reader polls the tail, NOTHING new is written ---
			logger.Ctx(ctx).Info("WP_IDLE_START", zap.String("mode", tc.name), zap.Duration("idleWindow", idleWindow))
			idleCtx, idleCancel := context.WithTimeout(ctx, idleWindow)
			_, _ = logReader.ReadNext(idleCtx) // blocks, polling the idle tail until the deadline
			idleCancel()
			logger.Ctx(ctx).Info("WP_IDLE_END", zap.String("mode", tc.name))
		})
	}
}

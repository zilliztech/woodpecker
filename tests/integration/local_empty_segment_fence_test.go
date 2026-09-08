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
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage/codec"
	"github.com/zilliztech/woodpecker/server/storage/disk"
	"github.com/zilliztech/woodpecker/woodpecker"
	"github.com/zilliztech/woodpecker/woodpecker/log"
)

// A segment that was finalized while empty (header + footer, zero blocks) is a
// valid file: Finalize writes IndexOffset = header size and IndexLength = 0 for
// it. If that segment's metadata is still Active when the next writer opens the
// log -- the state left behind when the previous holder finished completion
// phase 1 (finalize on the log store) but never wrote the metadata update --
// OpenLogWriter fences the segment, which reopens the file in recovery mode.
// The recovery path has to accept the empty finalized file; rejecting it makes
// the log unopenable for every later writer.
//
// The local backend is the one that reopens the same file, so this runs with
// storage type "local". It needs etcd only.
func TestLocalReopenFencesFinalizedEmptySegment(t *testing.T) {
	ctx := context.Background()
	cfg, err := config.NewConfiguration("../../config/woodpecker.yaml")
	require.NoError(t, err)
	cfg.Woodpecker.Storage.Type = "local"
	cfg.Woodpecker.Storage.RootPath = t.TempDir()
	if ep := os.Getenv("WP_TEST_ETCD_ENDPOINTS"); ep != "" {
		cfg.Etcd.Endpoints = strings.Split(ep, ",")
	}
	cfg.Etcd.RootPath = fmt.Sprintf("wp-empty-seg-%d", time.Now().UnixNano())
	require.NoError(t, woodpecker.StopEmbedLogStore())
	logName := fmt.Sprintf("empty_seg_%d", time.Now().UnixNano())

	// --- previous holder: creates the segment and never writes to it
	client1, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, client1.CreateLog(ctx, logName))
	lh1, err := client1.OpenLog(ctx, logName)
	require.NoError(t, err)
	sh, err := lh1.GetOrCreateWritableSegmentHandle(ctx, func(context.Context, string) {})
	require.NoError(t, err)
	logId := lh1.GetId()
	segId := sh.GetId(ctx)

	// The holder is gone before completing the segment in metadata: drop the
	// client and the in-memory log store (a new process has neither).
	require.NoError(t, client1.Close(ctx))
	require.NoError(t, woodpecker.StopEmbedLogStore())

	// Its log store had finalized the segment file: header + footer, zero blocks.
	// Write that state directly, at the path the local log store uses for
	// storage type "local": <storage.rootPath>/<minio.rootPath>/<logId>/<segmentId>
	// (see segmentProcessor.getOrCreateSegmentWriter, local branch).
	baseDir := path.Join(cfg.Woodpecker.Storage.RootPath, cfg.Minio.RootPath)
	w, err := disk.NewLocalFileWriter(ctx, baseDir, logId, segId, cfg)
	require.NoError(t, err)
	_, err = w.Finalize(ctx, -1)
	require.NoError(t, err)
	require.NoError(t, w.Close(ctx))
	st, err := os.Stat(path.Join(baseDir, fmt.Sprintf("%d/%d/data.log", logId, segId)))
	require.NoError(t, err)
	require.EqualValues(t, codec.RecordHeaderSize+codec.HeaderRecordSize+codec.RecordHeaderSize+codec.FooterRecordSize, st.Size(),
		"header + footer only: a finalized empty segment")

	// --- next holder opens the log and must fence the still-Active empty segment
	client2, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer func() {
		_ = client2.Close(ctx)
		_ = woodpecker.StopEmbedLogStore()
	}()
	lh2, err := client2.OpenLog(ctx, logName)
	require.NoError(t, err)
	segMeta, err := lh2.GetMetadataProvider().GetSegmentMetadata(ctx, logName, segId)
	require.NoError(t, err)
	require.Equal(t, proto.SegmentState_Active, segMeta.Metadata.State, "metadata still says Active")

	writer, err := lh2.OpenLogWriter(ctx)
	require.NoError(t, err, "fencing a finalized empty segment on reopen must succeed")
	res := writer.Write(ctx, &log.WriteMessage{Payload: []byte("first write after reopen")})
	require.NoError(t, res.Err)
	require.NoError(t, writer.Close(ctx))
}

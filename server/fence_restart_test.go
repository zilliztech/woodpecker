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
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/werr"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
)

// startFenceRestartNode runs the real RPC handlers, log store and disk writer.
// Replacing it rebuilds all volatile server state while keeping the same volume.
func startFenceRestartNode(t *testing.T, root string) (proto.LogStoreClient, func()) {
	t.Helper()
	ctx := context.Background()
	cfg, err := config.NewConfiguration()
	require.NoError(t, err)
	cfg.Woodpecker.Storage.Type = "service"
	cfg.Woodpecker.Storage.RootPath = root
	cfg.Woodpecker.Logstore.SyncScheduler.MaxWorkers = 2
	// Exercise fence recovery independently of the host's current free space.
	cfg.Woodpecker.Logstore.DiskWatermarkPolicy.Enabled = false
	store := NewLogStore(ctx, cfg, nil)
	require.NoError(t, store.Start())
	listener := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	proto.RegisterLogStoreServer(srv, &Server{cfg: cfg, logStore: store})
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = srv.Serve(listener)
	}()
	conn, err := grpc.NewClient("passthrough:///fence-restart",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}))
	require.NoError(t, err)
	var once sync.Once
	stop := func() {
		once.Do(func() {
			srv.Stop()
			_ = conn.Close()
			require.NoError(t, store.Stop())
			_ = listener.Close()
			<-served
		})
	}
	t.Cleanup(stop)
	return proto.NewLogStoreClient(conn), stop
}

func TestServiceFenceRestartBeforeFinalizeRejectsStaleQuorumWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rootA, rootB, rootC := t.TempDir(), t.TempDir(), t.TempDir()
	a, stopA := startFenceRestartNode(t, rootA)
	b, _ := startFenceRestartNode(t, rootB)
	c, _ := startFenceRestartNode(t, rootC)
	const bucket, root = "bucket", "root"
	appendEntry := func(cli proto.LogStoreClient, segmentID int64, payload string) *proto.AddEntryResponse {
		t.Helper()
		stream, err := cli.AddEntry(ctx, &proto.AddEntryRequest{
			BucketName: bucket, RootPath: root, LogId: 1,
			Entry: &proto.LogEntry{SegId: segmentID, EntryId: 0, Values: []byte(payload)},
		})
		require.NoError(t, err)
		response, err := stream.Recv()
		require.NoError(t, err)
		if response.State == proto.AddEntryState_Buffered {
			response, err = stream.Recv()
			require.NoError(t, err)
		}
		return response
	}
	fence := func(cli proto.LogStoreClient, segmentID int64) int64 {
		t.Helper()
		response, err := cli.FenceSegment(ctx, &proto.FenceSegmentRequest{
			BucketName: bucket, RootPath: root, LogId: 1, SegmentId: segmentID,
		})
		require.NoError(t, err)
		require.NoError(t, werr.Error(response.Status))
		return response.LastEntryId
	}
	complete := func(cli proto.LogStoreClient, segmentID, lac int64) {
		t.Helper()
		response, err := cli.CompleteSegment(ctx, &proto.CompleteSegmentRequest{
			BucketName: bucket, RootPath: root, LogId: 1, SegmentId: segmentID, LastAddConfirmed: lac,
		})
		require.NoError(t, err)
		require.NoError(t, werr.Error(response.Status))
	}

	// A previous acknowledged entry must remain readable across the same restart.
	for _, cli := range []proto.LogStoreClient{a, b, c} {
		response := appendEntry(cli, 0, "previously-acknowledged")
		require.Equal(t, proto.AddEntryState_Synced, response.State)
		require.EqualValues(t, 0, fence(cli, 0))
		complete(cli, 0, 0)
	}

	// The old writer has only C's durable answer for entry 0 in segment 1.
	// C is not contacted by the recovery coordinator from this point on.
	response := appendEntry(c, 1, "unconfirmed-tail")
	require.Equal(t, proto.AddEntryState_Synced, response.State)
	const durableAnswers = 1
	require.EqualValues(t, -1, fence(a, 1))
	require.EqualValues(t, -1, fence(b, 1))
	marker := filepath.Join(rootA, bucket, root, "1", "1", storage.FenceMarkerName)
	info, err := os.Stat(filepath.Join(filepath.Dir(marker), "data.log"))
	require.NoError(t, err)
	require.Zero(t, info.Size(), "no data or finalized footer may mask a lost fence")

	// Pause completion after the two Fence replies, replace A, then deliver the
	// old append before either Complete RPC. No timing-dependent sleeps are needed.
	stopA()
	a, _ = startFenceRestartNode(t, rootA)
	lateAnswers := 0
	for _, cli := range []proto.LogStoreClient{a, b} {
		response := appendEntry(cli, 1, "unconfirmed-tail")
		if response.State == proto.AddEntryState_Synced {
			lateAnswers++
		}
		require.Equal(t, proto.AddEntryState_Failed, response.State)
		require.ErrorIs(t, werr.Error(response.Status), werr.ErrSegmentFenced)
	}
	require.Less(t, durableAnswers+lateAnswers, 2, "stale writes must not gain an ACK quorum after Fence")
	complete(a, 1, -1)
	complete(b, 1, -1)

	for _, cli := range []proto.LogStoreClient{a, b} {
		read, err := cli.GetBatchEntriesAdv(ctx, &proto.GetBatchEntriesAdvRequest{
			BucketName: bucket, RootPath: root, LogId: 1, SegmentId: 0, FromEntryId: 0, MaxEntries: 10,
		})
		require.NoError(t, err)
		require.NoError(t, werr.Error(read.Status))
		require.Len(t, read.Result.Entries, 1)
		require.Equal(t, []byte("previously-acknowledged"), read.Result.Entries[0].Values)
		empty, err := cli.GetBatchEntriesAdv(ctx, &proto.GetBatchEntriesAdvRequest{
			BucketName: bucket, RootPath: root, LogId: 1, SegmentId: 1, FromEntryId: 0, MaxEntries: 10,
		})
		require.NoError(t, err)
		require.ErrorIs(t, werr.Error(empty.Status), werr.ErrFileReaderEndOfFile)
	}
}

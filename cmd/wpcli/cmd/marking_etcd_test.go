package cmd

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"

	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

// startTestEtcd starts a self-contained embedded etcd on random free ports (so it cannot
// collide with other packages' embedded etcd singletons under parallel `go test ./...`)
// and returns an in-process client.
func startTestEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	freePort := func() int {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer l.Close()
		return l.Addr().(*net.TCPAddr).Port
	}
	clientURL, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", freePort()))
	require.NoError(t, err)
	peerURL, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", freePort()))
	require.NoError(t, err)

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.InitialCluster = cfg.Name + "=" + peerURL.String()
	cfg.LogLevel = "error"

	srv, err := embed.StartEtcd(cfg)
	require.NoError(t, err)
	select {
	case <-srv.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		srv.Close()
		t.Fatal("embedded etcd did not become ready")
	}
	t.Cleanup(srv.Close)
	return v3client.New(srv.Server)
}

func putMarkingRecord(t *testing.T, cli *clientv3.Client, kb *meta.KeyBuilder, s *proto.SegmentCompactedNotifyStatus) {
	t.Helper()
	bytes, err := proto.MarshalSegmentCompactedNotifyStatus(s)
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), kb.BuildSegmentCompactedNotifyStatusKey(s.LogId, s.SegmentId), string(bytes))
	require.NoError(t, err)
}

func markingTestCmd() (*cobra.Command, *bytes.Buffer, *bytes.Buffer) {
	cmd := &cobra.Command{}
	out, errOut := new(bytes.Buffer), new(bytes.Buffer)
	cmd.SetOut(out)
	cmd.SetErr(errOut)
	return cmd, out, errOut
}

// TestMarkingCommands_EmbeddedEtcd drives the etcd-facing cores of `wp marking list` and
// `wp marking confirm` against an embedded etcd, covering: state filtering, log scoping,
// json output, empty results, unparseable-record skip, confirm's PENDING_MANUAL gate,
// --force, idempotent absence, and unparseable-record refusal.
func TestMarkingCommands_EmbeddedEtcd(t *testing.T) {
	cli := startTestEtcd(t)

	oldGlobals := Globals
	defer func() { Globals = oldGlobals }()
	Globals = GlobalFlags{Timeout: 5 * time.Second}

	kb := meta.NewKeyBuilder("wptest")
	nowMs := uint64(time.Now().UnixMilli())

	// Fixtures: log 7 has one PENDING_MANUAL and one COMPLETED; log 8 has one IN_PROGRESS.
	putMarkingRecord(t, cli, kb, &proto.SegmentCompactedNotifyStatus{
		LogId: 7, SegmentId: 1,
		State:     proto.SegmentCompactedNotifyState_NOTIFY_PENDING_MANUAL,
		StartTime: nowMs, LastUpdateTime: nowMs,
		QuorumNotifyStatus: map[string]bool{"node1": true, "node2": false},
		ErrorMessage:       "nodes unacked after 30m0s of retries: node2",
	})
	putMarkingRecord(t, cli, kb, &proto.SegmentCompactedNotifyStatus{
		LogId: 7, SegmentId: 2,
		State:     proto.SegmentCompactedNotifyState_NOTIFY_COMPLETED,
		StartTime: nowMs, LastUpdateTime: nowMs,
		QuorumNotifyStatus: map[string]bool{"node1": true, "node2": true},
	})
	putMarkingRecord(t, cli, kb, &proto.SegmentCompactedNotifyStatus{
		LogId: 8, SegmentId: 1,
		State:     proto.SegmentCompactedNotifyState_NOTIFY_IN_PROGRESS,
		StartTime: nowMs, LastUpdateTime: nowMs,
		QuorumNotifyStatus: map[string]bool{"node1": false},
	})

	t.Run("list default shows only PENDING_MANUAL", func(t *testing.T) {
		cmd, out, _ := markingTestCmd()
		require.NoError(t, runMarkingList(cmd, cli, kb, 0, false, false))
		s := out.String()
		assert.Contains(t, s, "PENDING_MANUAL")
		assert.Contains(t, s, "node2")
		assert.NotContains(t, s, "COMPLETED")
		assert.NotContains(t, s, "IN_PROGRESS")
	})

	t.Run("list all-states shows everything", func(t *testing.T) {
		cmd, out, _ := markingTestCmd()
		require.NoError(t, runMarkingList(cmd, cli, kb, 0, false, true))
		s := out.String()
		assert.Contains(t, s, "PENDING_MANUAL")
		assert.Contains(t, s, "COMPLETED")
		assert.Contains(t, s, "IN_PROGRESS")
	})

	t.Run("list scoped to one log", func(t *testing.T) {
		cmd, out, _ := markingTestCmd()
		require.NoError(t, runMarkingList(cmd, cli, kb, 8, true, true))
		s := out.String()
		assert.Contains(t, s, "IN_PROGRESS")
		assert.NotContains(t, s, "PENDING_MANUAL", "log 7 records must not appear when scoped to log 8")
	})

	t.Run("list json output", func(t *testing.T) {
		Globals.Output = "json"
		defer func() { Globals.Output = "" }()
		cmd, out, _ := markingTestCmd()
		require.NoError(t, runMarkingList(cmd, cli, kb, 0, false, false))
		assert.Contains(t, out.String(), `"log_id"`)
		assert.Contains(t, out.String(), `"unacked_nodes"`)
	})

	t.Run("list empty result message", func(t *testing.T) {
		cmd, out, _ := markingTestCmd()
		require.NoError(t, runMarkingList(cmd, cli, kb, 999, true, false))
		assert.Contains(t, out.String(), "no records pending manual handling")
		cmd2, out2, _ := markingTestCmd()
		require.NoError(t, runMarkingList(cmd2, cli, kb, 999, true, true))
		assert.Contains(t, out2.String(), "no marking records")
	})

	t.Run("list skips unparseable record with warning", func(t *testing.T) {
		_, err := cli.Put(context.Background(), kb.BuildSegmentCompactedNotifyStatusKey(7, 99), string([]byte{0x0a, 0x0a}))
		require.NoError(t, err)
		defer cli.Delete(context.Background(), kb.BuildSegmentCompactedNotifyStatusKey(7, 99))
		cmd, out, errOut := markingTestCmd()
		require.NoError(t, runMarkingList(cmd, cli, kb, 0, false, false))
		assert.Contains(t, errOut.String(), "unparseable")
		assert.Contains(t, out.String(), "PENDING_MANUAL", "valid records still render")
	})

	t.Run("confirm refuses non-PENDING_MANUAL without force", func(t *testing.T) {
		cmd, _, _ := markingTestCmd()
		err := runMarkingConfirm(cmd, cli, kb, 7, 2, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not PENDING_MANUAL")
	})

	t.Run("confirm transitions PENDING_MANUAL to OPERATOR_CONFIRMED (durable, not deleted)", func(t *testing.T) {
		cmd, out, _ := markingTestCmd()
		require.NoError(t, runMarkingConfirm(cmd, cli, kb, 7, 1, false))
		assert.Contains(t, out.String(), "set to OPERATOR_CONFIRMED")

		// Record is retained (durable) with the new terminal state — NOT physically deleted.
		resp, err := cli.Get(context.Background(), kb.BuildSegmentCompactedNotifyStatusKey(7, 1))
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1, "record must be retained, not deleted")
		st := &proto.SegmentCompactedNotifyStatus{}
		require.NoError(t, proto.UnmarshalSegmentCompactedNotifyStatus(resp.Kvs[0].Value, st))
		assert.Equal(t, proto.SegmentCompactedNotifyState_NOTIFY_OPERATOR_CONFIRMED, st.State)
	})

	t.Run("confirm on an already-confirmed record is idempotent", func(t *testing.T) {
		cmd, out, _ := markingTestCmd()
		require.NoError(t, runMarkingConfirm(cmd, cli, kb, 7, 1, false)) // 7/1 is now OPERATOR_CONFIRMED
		assert.Contains(t, out.String(), "already confirmed")
	})

	t.Run("confirm hides the record from the default list", func(t *testing.T) {
		cmd, out, _ := markingTestCmd()
		require.NoError(t, runMarkingList(cmd, cli, kb, 7, true, false)) // default view = PENDING_MANUAL only
		assert.NotContains(t, out.String(), "OPERATOR_CONFIRMED", "confirmed records are hidden from the default view")
	})

	t.Run("confirm missing record is a target-not-found error", func(t *testing.T) {
		cmd, _, _ := markingTestCmd()
		err := runMarkingConfirm(cmd, cli, kb, 7, 999, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "marking record 7/999")
	})

	t.Run("confirm force transitions a COMPLETED record", func(t *testing.T) {
		cmd, out, _ := markingTestCmd()
		require.NoError(t, runMarkingConfirm(cmd, cli, kb, 7, 2, true))
		assert.Contains(t, out.String(), "was NOTIFY_COMPLETED")
	})

	t.Run("confirm refuses unparseable record", func(t *testing.T) {
		_, err := cli.Put(context.Background(), kb.BuildSegmentCompactedNotifyStatusKey(9, 1), string([]byte{0x0a, 0x0a}))
		require.NoError(t, err)
		defer cli.Delete(context.Background(), kb.BuildSegmentCompactedNotifyStatusKey(9, 1))
		cmd, _, _ := markingTestCmd()
		err = runMarkingConfirm(cmd, cli, kb, 9, 1, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unparseable")
	})
}

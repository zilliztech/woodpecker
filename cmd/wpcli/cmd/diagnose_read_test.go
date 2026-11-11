package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/diagnosis"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/common/http/management"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
)

func readMetadataFixture(t *testing.T) (*clientv3.Client, string, *meta.KeyBuilder, clientv3.LeaseID) {
	t.Helper()
	cli := startTestEtcd(t)
	members, err := cli.MemberList(context.Background())
	require.NoError(t, err)
	endpoint := members.Members[0].ClientURLs[0]
	kb := meta.NewKeyBuilder("explicit-read-client/woodpecker")
	put := func(key string, msg pb.Message) {
		raw, err := pb.Marshal(msg)
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), key, string(raw))
		require.NoError(t, err)
	}
	put(kb.BuildLogKey("test"), &proto.LogMeta{LogId: 2, TruncatedSegmentId: -1, TruncatedEntryId: -1})
	put(kb.BuildSegmentInstanceKey("test", "7"), &proto.SegmentMetadata{SegNo: 7, State: proto.SegmentState_Active, LastEntryId: -1, Quorum: &proto.QuorumInfo{Es: 1, Wq: 1, Aq: 1, Nodes: []string{"node-1"}}})
	lease, err := cli.Grant(context.Background(), 120)
	require.NoError(t, err)
	return cli, endpoint, kb, lease.ID
}

func putDiagnoseReader(t *testing.T, cli *clientv3.Client, kb *meta.KeyBuilder, lease clientv3.LeaseID, name string, next int64, reported int64, opened uint64) {
	t.Helper()
	info := &proto.ReaderTempInfo{ReaderName: name, LogId: 2, OpenTimestamp: opened, OpenSegmentId: 7, OpenEntryId: next, RecentReadSegmentId: 7, RecentReadEntryId: next, RecentReadTimestamp: uint64(reported)}
	raw, err := pb.Marshal(info)
	require.NoError(t, err)
	_, err = cli.Put(context.Background(), kb.BuildLogReaderTempInfoKey(2, name), string(raw), clientv3.WithLease(lease))
	require.NoError(t, err)
}

func TestDiagnoseReadMetadataSelectionAndLimits(t *testing.T) {
	cli, endpoint, kb, lease := readMetadataFixture(t)
	withCliYAML(t, "http://127.0.0.1:1")
	now := time.Now().UnixMilli()
	putDiagnoseReader(t, cli, kb, lease, "reader-a", 5, now, uint64(now-60000))
	putDiagnoseReader(t, cli, kb, lease, "reader-b", 11, now, uint64(now-60000))
	newSampler := func(extra ...string) (diagnosis.Options, func()) {
		cmd := NewRootCommand()
		cmd.SetArgs(nil)
		f := readDiagnosisFlags{}
		o := diagnosis.Options{Scenario: "read", RequestTimeout: time.Second, Limits: diagnosis.Limits{ResponseBytes: 4096}}
		f.register(cmd, &o)
		require.NoError(t, cmd.ParseFlags(append([]string{"--log-name", "test", "--segment", "7", "--etcd", endpoint, "--meta-prefix", kb.Prefix()}, extra...)))
		require.NoError(t, f.validate("read"))
		cleanup := f.configure(context.Background(), &o, client.New("http://127.0.0.1:1", client.ClientOpts{}), &client.Memberlist{Members: []client.Member{{ID: "node-1"}}})
		return o, cleanup
	}
	o, closeSampler := newSampler()
	obs := o.ReadMetadata(context.Background())
	closeSampler()
	require.Equal(t, "observed", obs.Status)
	require.Len(t, obs.ReadState.Readers, 2)
	require.Positive(t, obs.ReadState.Readers[0].LeaseTTL)
	require.Equal(t, []string{"node-1"}, obs.ReadState.NodeIDs)
	raw, err := json.Marshal(obs)
	require.NoError(t, err)
	require.NotContains(t, string(raw), endpoint)
	require.NotContains(t, string(raw), kb.Prefix())
	o, closeSampler = newSampler("--max-readers", "1")
	obs = o.ReadMetadata(context.Background())
	closeSampler()
	require.Equal(t, "truncated", obs.Status)
	require.Equal(t, "reader_record_limit", obs.Code)
	require.Len(t, obs.ReadState.Readers, 1)
	o, closeSampler = newSampler("--reader", "reader-b", "--max-readers", "1")
	obs = o.ReadMetadata(context.Background())
	require.Equal(t, "observed", obs.Status)
	require.Len(t, obs.ReadState.Readers, 1)
	require.Equal(t, "reader-b", obs.ReadState.Readers[0].Name)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Equal(t, "unavailable", o.ReadMetadata(ctx).Status)
	closeSampler()
	_, err = cli.Revoke(context.Background(), lease)
	require.NoError(t, err)
	o, closeSampler = newSampler()
	obs = o.ReadMetadata(context.Background())
	closeSampler()
	require.Equal(t, "observed", obs.Status)
	require.Empty(t, obs.ReadState.Readers)
	_, err = cli.Put(context.Background(), kb.BuildLogReaderTempInfoKey(2, "malformed"), "sentinel-secret")
	require.NoError(t, err)
	o, closeSampler = newSampler()
	obs = o.ReadMetadata(context.Background())
	closeSampler()
	require.Equal(t, "invalid_reader_metadata_identity", obs.Code)
	raw, err = json.Marshal(obs)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "sentinel-secret")
}

func TestDiagnoseReadFlagValidation(t *testing.T) {
	for _, args := range [][]string{
		{"--scenario", "read", "--log-name", "test"},
		{"--scenario", "stuck-read", "--log-name", "test", "--segment", "7", "--etcd", "localhost:2379", "--meta-prefix", "tenant"},
		{"--scenario", "read", "--reader", "reader"},
		{"--scenario", "read", "--read-mode", "historical"},
		{"--scenario", "read", "--max-readers", "65"},
		{"--scenario", "write", "--read-mode", "catch-up"},
		{"--scenario", "write", "--reader", "reader"},
		{"--scenario", "read", "--log-name", "test", "--segment", "7", "--etcd", "localhost:2379", "--meta-prefix", "tenant", "--reader", "bad/name"},
	} {
		root := NewRootCommand()
		root.SetArgs(append([]string{"diagnose"}, args...))
		require.Equal(t, 2, wperrors.ExitCodeFor(root.Execute()))
	}
}

func TestDiagnoseReadHumanOutputNamesReaderAndExpectation(t *testing.T) {
	cmd := NewRootCommand()
	out := new(bytes.Buffer)
	cmd.SetOut(out)
	report := diagnosis.Report{ReadMode: "catch-up", Findings: []diagnosis.Finding{{RuleID: "reader_progress", Scope: diagnosis.Scope{Reader: "reader-observed", Tenant: "operator-selected namespace"}, Outcome: "suspected_catchup_no_progress"}}}
	require.NoError(t, renderDiagnosis(cmd, report))
	require.Contains(t, out.String(), "Reader: reader-observed")
	require.Contains(t, out.String(), "Reader expectation: catch-up")
}

// One real 30-second reporting window demonstrates three reader states in the
// same collection. Faster synthetic time would conceal the cadence exclusion.
func TestDiagnoseReadLiveCatchupAndIdleControls(t *testing.T) {
	cli, endpoint, kb, lease := readMetadataFixture(t)
	opened := uint64(time.Now().Add(-time.Minute).UnixMilli())
	names := []string{"catchup-stuck", "moving", "idle-at-tail"}
	for i, name := range names {
		next := int64(5)
		if i == 2 {
			next = 11
		}
		putDiagnoseReader(t, cli, kb, lease, name, next, time.Now().Add(-time.Second).UnixMilli(), opened)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				for i, name := range names {
					next := int64(5)
					if i == 1 {
						next = 6
					}
					if i == 2 {
						next = 11
					}
					openEntry := int64(5)
					if i == 2 {
						openEntry = 11
					}
					info := &proto.ReaderTempInfo{ReaderName: name, LogId: 2, OpenTimestamp: opened, OpenSegmentId: 7, OpenEntryId: openEntry, RecentReadSegmentId: 7, RecentReadEntryId: next, RecentReadTimestamp: uint64(now.UnixMilli())}
					raw, err := pb.Marshal(info)
					if err != nil {
						return
					}
					if _, err = cli.Put(ctx, kb.BuildLogReaderTempInfoKey(2, name), string(raw), clientv3.WithLease(lease)); err != nil {
						return
					}
				}
			}
		}
	}()
	defer func() { cancel(); <-done }()
	server := healthTestServer(t, map[string]func(http.ResponseWriter, *http.Request){
		"/metrics": func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `# TYPE process_start_time_seconds gauge
process_start_time_seconds 100
# TYPE woodpecker_server_logstore_operations_total counter
woodpecker_server_logstore_operations_total{node_id="node-1",log_id="2",log_ns="tenant-a",operation="get_batch_entries",status="success"} 5
`)
		},
		"/admin/runtime/ops": func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `[{"op_id":"pending-read","op_type":"logstore.get_batch_entries","log_id":2,"segment_id":7,"started_at":"2026-10-01T00:00:00Z"}]`)
		},
		"/admin/runtime/ops/stats": func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"capacity":1024,"in_use":1,"evicted_total":0}`)
		},
		"/admin/env": func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"build":{"version":"v0.1.46"},"env":{"TOKEN":"sentinel-secret"}}`)
		},
		"/admin/logstore/segments": management.NewLogstoreSegmentsHandler(func(logID *int64, _ *bool) []any {
			if logID == nil || *logID != 2 {
				return nil
			}
			return []any{storage.WriterSnapshot{LogID: 2, SegmentID: 7, Backend: "stagedstorage", LastEntry: 10}}
		}),
	})
	defer server.Close()
	r, out, err := runDiagnose(t, server, context.Background(), "--scenario", "read", "--read-mode", "catch-up", "--log-name", "test", "--segment", "7", "--etcd", endpoint, "--meta-prefix", kb.Prefix(), "--window", "30100ms", "--interval", "15s", "--max-samples", "4", "--budget", "40s", "--fail-on-findings")
	require.Equal(t, 8, wperrors.ExitCodeFor(err))
	require.Equal(t, "completed", r.Status)
	outcomes := map[string]string{}
	for _, f := range r.Findings {
		if f.RuleID == "reader_progress" {
			outcomes[f.Scope.Reader] = f.Outcome
		}
	}
	require.Equal(t, map[string]string{"catchup-stuck": "suspected_catchup_no_progress", "moving": "reader_forward_progress_observed", "idle-at-tail": "caught_up_or_awaiting_data"}, outcomes)
	require.NotContains(t, out, "sentinel-secret")
	require.NotContains(t, out, endpoint)
	for _, f := range r.Findings {
		if f.RuleID == "read_activity" {
			require.Equal(t, "pending_read_expectation_unknown", f.Outcome)
			require.True(t, strings.Contains(f.Summary, "reader association"))
		}
	}
}

func TestDiagnoseSharedMetadataFlagsSupportBothFamilies(t *testing.T) {
	for _, scenario := range []string{"write", "read", "all"} {
		cmd := NewRootCommand()
		flags := readDiagnosisFlags{}
		o := diagnosis.Options{Scenario: scenario}
		flags.register(cmd, &o)
		require.NoError(t, cmd.ParseFlags([]string{"--log-name", "channel", "--segment", "7", "--etcd", "http://127.0.0.1:2379", "--meta-prefix", "milvus-client/woodpecker"}))
		require.NoError(t, flags.validate(scenario))
	}
}

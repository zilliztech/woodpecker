package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	"github.com/zilliztech/woodpecker/cmd/wpcli/diagnosis"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/common/http/management"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
	"github.com/zilliztech/woodpecker/server/storage"
)

func TestDiagnoseWriteThroughExistingHandlers(t *testing.T) {
	for _, progressing := range []bool{false, true} {
		t.Run(fmt.Sprint("progress=", progressing), func(t *testing.T) {
			reads := int64(0)
			server := healthTestServer(t, map[string]func(http.ResponseWriter, *http.Request){
				"/metrics": func(w http.ResponseWriter, _ *http.Request) {
					fmt.Fprint(w, `# TYPE process_start_time_seconds gauge
process_start_time_seconds 100
# TYPE woodpecker_server_logstore_operations_total counter
woodpecker_server_logstore_operations_total{node_id="node-1",log_id="2",operation="add_entry",status="success"} 0
# TYPE woodpecker_server_logstore_disk_usage_ratio gauge
woodpecker_server_logstore_disk_usage_ratio{node_id="node-1",path="/wal"} 0.6
# TYPE woodpecker_server_logstore_disk_watermark_soft gauge
woodpecker_server_logstore_disk_watermark_soft{node_id="node-1"} 0.5
# TYPE woodpecker_server_logstore_disk_watermark_hard gauge
woodpecker_server_logstore_disk_watermark_hard{node_id="node-1"} 0.8
# TYPE woodpecker_server_logstore_write_backpressure_state gauge
woodpecker_server_logstore_write_backpressure_state{node_id="node-1"} 1
`)
				},
				"/admin/runtime/ops": func(w http.ResponseWriter, _ *http.Request) {
					fmt.Fprint(w, `[{"op_id":"pending","op_type":"logstore.add_entry","log_id":2,"segment_id":7,"started_at":"2026-10-01T00:00:00Z"},{"op_id":"flush","op_type":"file.flush","log_id":2,"segment_id":7,"started_at":"2026-10-01T00:00:00Z"}]`)
				},
				"/admin/runtime/ops/stats": management.NewOpsStatsHandler(func() any { return map[string]any{"capacity": 1024, "in_use": 2, "evicted_total": 0} }),
				"/admin/env":               management.NewEnvHandler(),
				"/admin/logstore/segments": management.NewLogstoreSegmentsHandler(func(_ *int64, _ *bool) []any {
					return []any{storage.WriterSnapshot{LogID: 2, SegmentID: 7, Backend: "stagedstorage", Writable: true}}
				}),
				"/admin/logstore/segments/detail": management.NewLogstoreSegmentShowHandler(func(log, segment int64) (any, error) {
					require.EqualValues(t, 2, log)
					require.EqualValues(t, 7, segment)
					if progressing {
						reads++
					}
					return storage.WriterSnapshotDetailed{WriterSnapshot: storage.WriterSnapshot{LogID: log, SegmentID: segment, Backend: "stagedstorage", Writable: true, LastEntry: reads}, FlushQueueDepth: 2, FlushQueueCapacity: 8, BufferBytes: 1024}, nil
				}),
			})
			defer server.Close()
			r, _, err := runDiagnose(t, server, context.Background(), "--scenario", "write")
			require.NoError(t, err)
			require.Len(t, r.Rules, 4)
			outcomes := map[string]string{}
			for _, finding := range r.Findings {
				outcomes[finding.RuleID] = finding.Outcome
			}
			require.Equal(t, "suspected_stall", outcomes["write_stall"])
			if progressing {
				require.Equal(t, "no_buildup_observed", outcomes["flush_buildup"])
			} else {
				require.Equal(t, "no_durable_progress_observed", outcomes["flush_buildup"])
			}
			require.Equal(t, "observed_admission_pressure", outcomes["disk_admission"])
			require.Equal(t, "inconclusive", outcomes["replica_participation"])
			require.Equal(t, 16, r.Limits.MaxWriters)
			r, _, err = runDiagnose(t, server, context.Background(), "--scenario", "write", "--mode", "snapshot", "--fail-on-findings")
			require.Equal(t, 8, wperrors.ExitCodeFor(err))
			for _, f := range r.Findings {
				if f.RuleID == "flush_buildup" {
					require.Equal(t, "inconclusive", f.Outcome)
				}
			}
		})
	}
}

func TestDiagnosisReadsExplicitMetadataWithoutDiscovery(t *testing.T) {
	cli := startTestEtcd(t)
	members, err := cli.MemberList(context.Background())
	require.NoError(t, err)
	endpoint := members.Members[0].ClientURLs[0]
	kb := meta.NewKeyBuilder("explicit-client/woodpecker")
	put := func(key string, msg pb.Message) {
		raw, err := pb.Marshal(msg)
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), key, string(raw))
		require.NoError(t, err)
	}
	put(kb.BuildLogKey("test"), &proto.LogMeta{LogId: 469986968496707423})
	put(kb.BuildSegmentInstanceKey("test", "7"), &proto.SegmentMetadata{SegNo: 7, State: proto.SegmentState_Active, Quorum: &proto.QuorumInfo{Es: 2, Wq: 2, Aq: 1, Nodes: []string{"node-1", "missing-node"}}})
	flags := &metaEtcdFlags{etcdEndpoints: endpoint, metaPrefix: kb.Prefix()}
	// Fresh command globals select a real CLI context; explicit metadata avoids
	// any /admin/config lookup against the deliberately unusable service origin.
	withCliYAML(t, "http://127.0.0.1:1")
	o := diagnosis.Options{RequestTimeout: time.Second, Limits: diagnosis.Limits{ResponseBytes: 4096}}
	r := diagnosis.Report{RunID: "metadata-test"}
	ac := client.New("http://127.0.0.1:1", client.ClientOpts{Timeout: time.Second})
	collectDiagnosisQuorum(context.Background(), &r, &o, flags, ac, &client.Memberlist{Members: []client.Member{{ID: "node-1"}}}, "test", 7)
	require.Len(t, r.Observations, 1)
	obs := r.Observations[0]
	require.Equal(t, "observed", obs.Status)
	require.Equal(t, []string{"node-1", ""}, obs.Quorum.NodeIDs)
	require.EqualValues(t, 469986968496707423, *o.LogID)
	require.EqualValues(t, 7, *o.SegmentID)
	require.Positive(t, obs.Quorum.Revision)
	raw, err := json.Marshal(r)
	require.NoError(t, err)
	require.NotContains(t, string(raw), endpoint)
	require.NotContains(t, string(raw), kb.Prefix())
	collectDiagnosisQuorum(context.Background(), &r, &o, flags, ac, nil, "absent", 7)
	require.Equal(t, "quorum_metadata_unavailable", r.Observations[1].Code)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	collectDiagnosisQuorum(ctx, &r, &o, flags, ac, nil, "test", 7)
	require.Equal(t, "unavailable", r.Observations[2].Status)
	collectDiagnosisQuorum(context.Background(), &r, &o, flags, ac, &client.Memberlist{Members: []client.Member{{ID: "node-1"}, {ID: "node-2", ServiceAddr: "node-1"}}}, "test", 7)
	require.Empty(t, r.Observations[3].Quorum.NodeIDs[0], "an ambiguous advertised address must not establish quorum identity")
}

func TestDiagnoseRequiresExplicitMetadataSelection(t *testing.T) {
	for _, args := range [][]string{
		{"--scenario", "write", "--log-name", "log"},
		{"--scenario", "stuck-write", "--log-name", "log", "--segment", "0", "--etcd", "localhost:2379", "--meta-prefix", "tenant"},
		{"--scenario", "write", "--log-name", "log", "--segment", "-1", "--etcd", "localhost:2379", "--meta-prefix", "tenant"},
		{"--etcd", "localhost:2379"},
	} {
		root := NewRootCommand()
		root.SetArgs(append([]string{"diagnose"}, args...))
		require.Equal(t, 2, wperrors.ExitCodeFor(root.Execute()))
	}
}

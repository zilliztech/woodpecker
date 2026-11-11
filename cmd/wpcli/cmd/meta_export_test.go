package cmd

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	pb "google.golang.org/protobuf/proto"

	"github.com/zilliztech/woodpecker/proto"
)

func TestMetaReadableBackup(t *testing.T) {
	for _, tc := range []struct {
		path, kind string
		msg        pb.Message
	}{
		{"logs/a", "log", &proto.LogMeta{LogId: 2, TruncatedEntryId: 10}},
		{"logs/a/segments/0", "segment", &proto.SegmentMetadata{LastEntryId: 99, State: proto.SegmentState_Completed}},
		{"logs-deleted/a-123/segments/0", "segment", &proto.SegmentMetadata{State: proto.SegmentState_Sealed}},
		{"quorums/1", "quorum", &proto.QuorumInfo{Es: 3}},
		{"readers/2/reader", "reader", &proto.ReaderTempInfo{}},
		{"cleaning/2/0", "cleanup", &proto.SegmentCleanupStatus{}},
		{"marking/2/0", "compacted_notify", &proto.SegmentCompactedNotifyStatus{}},
		{"version", "version", &proto.Version{}},
		{"skipranges", "skip_ranges", &proto.AllSkipRanges{}},
	} {
		t.Run(tc.path, func(t *testing.T) {
			raw, err := pb.Marshal(tc.msg)
			require.NoError(t, err)
			record := rawBackup(&mvccpb.KeyValue{Key: []byte("root/" + tc.path), Value: raw, ModRevision: 42, Lease: 17}, "root")
			require.Equal(t, tc.kind, record.Kind)
			require.Empty(t, record.DecodeError)
			require.True(t, json.Valid(record.Metadata))
			decoded, err := base64.StdEncoding.DecodeString(record.ValueBase64)
			require.NoError(t, err)
			require.Equal(t, raw, decoded)
			require.EqualValues(t, 42, record.Revision)
			require.EqualValues(t, 17, record.Lease)
			serialized, err := json.Marshal(record)
			require.NoError(t, err)
			require.Contains(t, string(serialized), `"metadata":{`)
		})
	}
	text := rawBackup(&mvccpb.KeyValue{Key: []byte("root/instance"), Value: []byte("uuid-readable")}, "root")
	require.Equal(t, "uuid-readable", text.Value)
	require.Equal(t, "text", text.ValueEncoding)
	audit := rawBackup(&mvccpb.KeyValue{Key: []byte("root/ops-audit/1"), Value: []byte(`{"reason":"repair"}`)}, "root")
	require.JSONEq(t, `{"reason":"repair"}`, string(audit.Metadata))
	corrupt := rawBackup(&mvccpb.KeyValue{Key: []byte("root/logs/a"), Value: []byte{255}}, "root")
	require.NotEmpty(t, corrupt.DecodeError)
	require.Equal(t, "/w==", corrupt.ValueBase64)
	unknown := rawBackup(&mvccpb.KeyValue{Key: []byte("root/unknown"), Value: []byte{255}}, "root")
	require.Equal(t, "base64", unknown.ValueEncoding)
}

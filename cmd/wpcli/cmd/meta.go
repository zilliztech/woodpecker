package cmd

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/spf13/cobra"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/encoding/protojson"
	pb "google.golang.org/protobuf/proto"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

type metaRecord struct {
	Kind     string          `json:"kind"`
	Key      string          `json:"key"`
	Revision int64           `json:"revision"`
	Lease    int64           `json:"lease"`
	Metadata json.RawMessage `json:"metadata"`
}

func (r metaRecord) MarshalYAML() (any, error) {
	var metadata any
	if err := json.Unmarshal(r.Metadata, &metadata); err != nil {
		return nil, err
	}
	return map[string]any{"kind": r.Kind, "key": r.Key, "revision": r.Revision, "lease": r.Lease, "metadata": metadata}, nil
}

func newMetaCommand() *cobra.Command {
	c := &cobra.Command{Use: "meta", Short: "Inspect and repair authoritative etcd metadata"}
	for _, kind := range []string{"log", "segment", "quorum"} {
		group := &cobra.Command{Use: kind, Short: "Inspect " + kind + " metadata"}
		group.AddCommand(newMetaReadCommand(kind, false), newMetaReadCommand(kind, true))
		if kind != "quorum" {
			group.AddCommand(newMetaPatchCommand(kind))
		}
		c.AddCommand(group)
	}
	c.AddCommand(newMetaExportCommand(), newMetaCheckCommand())
	return c
}

func openMeta(flags *metaEtcdFlags) (*clientv3.Client, *meta.KeyBuilder, error) {
	conn, err := resolveMetaEtcd(flags)
	if err != nil {
		return nil, nil, err
	}
	cli, err := metaEtcdClient(conn)
	return cli, conn.kb, err
}

func metaMessage(kind string) pb.Message {
	switch kind {
	case "log":
		return &proto.LogMeta{}
	case "segment":
		return &proto.SegmentMetadata{}
	default:
		return &proto.QuorumInfo{}
	}
}

func decodeMeta(kind string, kv *mvccpb.KeyValue) (metaRecord, pb.Message, error) {
	msg := metaMessage(kind)
	if err := pb.Unmarshal(kv.Value, msg); err != nil {
		return metaRecord{}, nil, wperrors.NewConfigError(fmt.Sprintf("invalid %s metadata at %s: %v", kind, kv.Key, err))
	}
	data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(msg)
	return metaRecord{Kind: kind, Key: string(kv.Key), Revision: kv.ModRevision, Lease: kv.Lease, Metadata: data}, msg, err
}

func renderMeta(cmd *cobra.Command, value any) error {
	if Globals.Output == "yaml" {
		return output.RenderYAML(cmd.OutOrStdout(), value)
	}
	return output.RenderJSON(cmd.OutOrStdout(), value)
}

func validateMetaTarget(kind string, args []string) error {
	want := 1
	if kind == "segment" {
		want = 2
	}
	if len(args) != want {
		return wperrors.NewUsageError("invalid metadata selector")
	}
	if kind == "log" || kind == "segment" {
		if args[0] == "" || strings.Contains(args[0], "/") {
			return wperrors.NewUsageError("logName must be nonempty and contain no '/'")
		}
	}
	if kind != "log" {
		id, err := strconv.ParseInt(args[len(args)-1], 10, 64)
		if err != nil || id < 0 {
			return wperrors.NewUsageError("ID must be a nonnegative integer")
		}
	}
	return nil
}

func metaTargetKey(kind string, kb *meta.KeyBuilder, args []string) (string, error) {
	if err := validateMetaTarget(kind, args); err != nil {
		return "", err
	}
	if kind == "log" {
		return kb.BuildLogKey(args[0]), nil
	}
	idArg := args[len(args)-1]
	id, err := strconv.ParseInt(idArg, 10, 64)
	if err != nil || id < 0 {
		return "", wperrors.NewUsageError("ID must be a nonnegative integer")
	}
	if kind == "quorum" {
		return kb.BuildQuorumInfoKey(strconv.FormatInt(id, 10)), nil
	}
	logKey, err := metaTargetKey("log", kb, args[:1])
	if err != nil {
		return "", err
	}
	return logKey + "/segments/" + strconv.FormatInt(id, 10), nil
}

func metaGet(cli *clientv3.Client, key string) (*mvccpb.KeyValue, error) {
	ctx, cancel := metaCtx()
	defer cancel()
	r, err := cli.Get(ctx, key)
	if err != nil {
		return nil, wperrors.NewNetworkError(err.Error())
	}
	if len(r.Kvs) != 1 {
		return nil, wperrors.NewTargetNotFoundError(key)
	}
	return r.Kvs[0], nil
}

// canonicalMetaSegmentID checks both the key's spelling and the immutable payload identity.
func canonicalMetaSegmentID(key, logKey string, segment *proto.SegmentMetadata) (int64, error) {
	prefix := logKey + "/segments/"
	suffix := strings.TrimPrefix(key, prefix)
	id, err := strconv.ParseInt(suffix, 10, 64)
	if !strings.HasPrefix(key, prefix) || err != nil || id < 0 || suffix != strconv.FormatInt(id, 10) || id != segment.SegNo {
		return 0, fmt.Errorf("%s: key must use canonical segment ID %d matching segNo", key, segment.SegNo)
	}
	return id, nil
}

func newMetaReadCommand(kind string, show bool) *cobra.Command {
	var flags metaEtcdFlags
	verb, use, nargs := "list", "list", 0
	if kind == "segment" {
		use = "list <logName>"
		nargs = 1
	}
	if show {
		verb = "show"
		use = "show <ID>"
		nargs = 1
		if kind == "log" {
			use = "show <logName>"
		}
		if kind == "segment" {
			use = "show <logName> <segmentId>"
			nargs = 2
		}
	}
	c := &cobra.Command{Use: use, Short: verb + " " + kind + " metadata and etcd revision", Args: cobra.ExactArgs(nargs), RunE: func(cmd *cobra.Command, args []string) error {
		if show || kind == "segment" {
			selectorKind := kind
			if !show {
				selectorKind = "log"
			}
			if err := validateMetaTarget(selectorKind, args); err != nil {
				return err
			}
		}
		cli, kb, err := openMeta(&flags)
		if err != nil {
			return err
		}
		defer cli.Close()
		if show {
			key, err := metaTargetKey(kind, kb, args)
			if err != nil {
				return err
			}
			kv, err := metaGet(cli, key)
			if err != nil {
				return err
			}
			record, _, err := decodeMeta(kind, kv)
			if err != nil {
				return err
			}
			return renderMeta(cmd, record)
		}
		prefix := kb.LogsPrefix() + "/"
		if kind == "quorum" {
			prefix = kb.QuorumsPrefix() + "/"
		}
		if kind == "segment" {
			key, err := metaTargetKey("log", kb, args)
			if err != nil {
				return err
			}
			if _, err := metaGet(cli, key); err != nil {
				return err
			}
			prefix = key + "/segments/"
		}
		ctx, cancel := metaCtx()
		defer cancel()
		resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
		if err != nil {
			return wperrors.NewNetworkError(err.Error())
		}
		records := []metaRecord{}
		for _, kv := range resp.Kvs {
			suffix := strings.TrimPrefix(string(kv.Key), prefix)
			if suffix == "" || strings.Contains(suffix, "/") {
				continue
			}
			if kind != "log" {
				if _, err := strconv.ParseInt(suffix, 10, 64); err != nil {
					continue
				}
			}
			record, _, err := decodeMeta(kind, kv)
			if err != nil {
				return err
			}
			records = append(records, record)
		}
		return renderMeta(cmd, map[string]any{"prefix": kb.Prefix(), "snapshot_revision": resp.Header.Revision, "records": records})
	}}
	flags.register(c)
	return c
}

type metaRawBackup struct {
	Key           string          `json:"key"`
	Revision      int64           `json:"revision"`
	Lease         int64           `json:"lease"`
	ValueBase64   string          `json:"value_base64"`
	Kind          string          `json:"kind"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
	Value         string          `json:"value,omitempty"`
	ValueEncoding string          `json:"value_encoding,omitempty"`
	DecodeError   string          `json:"decode_error,omitempty"`
}

func rawBackup(kv *mvccpb.KeyValue, prefix string) metaRawBackup {
	record := metaRawBackup{Key: string(kv.Key), Revision: kv.ModRevision, Lease: kv.Lease, ValueBase64: base64.StdEncoding.EncodeToString(kv.Value), Kind: "raw"}
	path := strings.TrimPrefix(string(kv.Key), prefix+"/")
	parts := strings.Split(path, "/")
	var msg pb.Message
	switch {
	case (parts[0] == "logs" || parts[0] == "logs-deleted") && len(parts) == 2:
		record.Kind = "log"
		msg = &proto.LogMeta{}
	case (parts[0] == "logs" || parts[0] == "logs-deleted") && len(parts) == 4 && parts[2] == "segments":
		record.Kind = "segment"
		msg = &proto.SegmentMetadata{}
	case parts[0] == "quorums" && len(parts) == 2:
		record.Kind = "quorum"
		msg = &proto.QuorumInfo{}
	case parts[0] == "readers" && len(parts) == 3:
		record.Kind = "reader"
		msg = &proto.ReaderTempInfo{}
	case parts[0] == "cleaning" && len(parts) == 3:
		record.Kind = "cleanup"
		msg = &proto.SegmentCleanupStatus{}
	case parts[0] == "marking" && len(parts) == 3:
		record.Kind = "compacted_notify"
		msg = &proto.SegmentCompactedNotifyStatus{}
	case path == "version":
		record.Kind = "version"
		msg = &proto.Version{}
	case path == "skipranges":
		record.Kind = "skip_ranges"
		msg = &proto.AllSkipRanges{}
	}
	if msg != nil {
		err := pb.Unmarshal(kv.Value, msg)
		if err == nil {
			record.Metadata, err = (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(msg)
		}
		if err != nil {
			record.DecodeError = err.Error()
		}
		return record
	}
	// Other namespace records may be JSON, text, locks, counters or opaque bytes.
	if json.Valid(kv.Value) {
		record.ValueEncoding = "json"
		record.Metadata = append(json.RawMessage(nil), kv.Value...)
	} else {
		record.Value, record.ValueEncoding = record.ValueBase64, "base64"
		if utf8.Valid(kv.Value) && strings.IndexFunc(string(kv.Value), func(r rune) bool {
			return unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t'
		}) < 0 {
			record.Value, record.ValueEncoding = string(kv.Value), "text"
		}
	}
	return record
}

func writeMetaBackup(file string, data any) error {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return wperrors.NewConfigError("create new backup file: " + err.Error())
	}
	_, writeErr := f.Write(append(b, '\n'))
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	// Sync the new directory entry before allowing the caller to commit an etcd update.
	dir, err := os.Open(filepath.Dir(file))
	if err != nil {
		return wperrors.NewConfigError("open backup directory: " + err.Error())
	}
	syncErr = dir.Sync()
	closeErr = dir.Close()
	if syncErr != nil {
		return wperrors.NewConfigError("sync backup directory: " + syncErr.Error())
	}
	return closeErr
}

func newMetaExportCommand() *cobra.Command {
	var flags metaEtcdFlags
	var file string
	c := &cobra.Command{Use: "export", Short: "Export one metadata namespace at a consistent etcd revision", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if file == "" {
			return wperrors.NewUsageError("--output-file must be nonempty")
		}
		cli, kb, err := openMeta(&flags)
		if err != nil {
			return err
		}
		defer cli.Close()
		ctx, cancel := metaCtx()
		defer cancel()
		r, err := cli.Get(ctx, kb.Prefix()+"/", clientv3.WithPrefix())
		if err != nil {
			return wperrors.NewNetworkError(err.Error())
		}
		records := []metaRawBackup{}
		for _, kv := range r.Kvs {
			records = append(records, rawBackup(kv, kb.Prefix()))
		}
		if err := writeMetaBackup(file, map[string]any{"prefix": kb.Prefix(), "snapshot_revision": r.Header.Revision, "exported_at": time.Now().UTC(), "records": records}); err != nil {
			return err
		}
		return renderMeta(cmd, map[string]any{"file": file, "keys": len(records), "snapshot_revision": r.Header.Revision})
	}}
	flags.register(c)
	c.Flags().StringVar(&file, "output-file", "", "New snapshot file (required; never overwritten)")
	_ = c.MarkFlagRequired("output-file")
	return c
}

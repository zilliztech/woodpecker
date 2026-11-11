package cmd

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/spf13/cobra"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
)

func newEtcdCommand() *cobra.Command {
	c := &cobra.Command{Use: "etcd", Short: "Explore raw etcd keys and values (read-only; no metadata prefix required)"}
	c.AddCommand(newEtcdListCommand(), newEtcdValueCommand(false), newEtcdValueCommand(true))
	return c
}

func openRawEtcd(flags *metaEtcdFlags) (*clientv3.Client, error) {
	conn, err := resolveEtcd(flags, false)
	if err != nil {
		return nil, err
	}
	return metaEtcdClient(conn)
}

func registerRawEtcd(flags *metaEtcdFlags, c *cobra.Command) {
	flags.register(c)
	_ = c.Flags().MarkHidden("meta-prefix")
}

func displayEtcdKey(key []byte) string {
	if utf8.Valid(key) && strings.IndexFunc(string(key), unicode.IsControl) < 0 {
		return string(key)
	}
	return fmt.Sprintf("%q", key)
}

type etcdKeyRecord struct {
	Key            string `json:"key" yaml:"key"`
	KeyBase64      string `json:"key_base64" yaml:"key_base64"`
	CreateRevision int64  `json:"create_revision" yaml:"create_revision"`
	ModRevision    int64  `json:"mod_revision" yaml:"mod_revision"`
	Version        int64  `json:"version" yaml:"version"`
	Lease          int64  `json:"lease" yaml:"lease"`
}

func etcdRecord(kv *mvccpb.KeyValue) etcdKeyRecord {
	return etcdKeyRecord{displayEtcdKey(kv.Key), base64.StdEncoding.EncodeToString(kv.Key), kv.CreateRevision, kv.ModRevision, kv.Version, kv.Lease}
}

func newEtcdListCommand() *cobra.Command {
	var flags metaEtcdFlags
	var limit, revision int64
	var afterKey, afterKeyBase64 string
	c := &cobra.Command{Use: "list [prefix]", Short: "List raw keys by literal prefix; omit prefix to explore all keys", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if limit <= 0 || revision < 0 {
			return wperrors.NewUsageError("--limit must be positive and --revision nonnegative")
		}
		if cmd.Flags().Changed("after-key") && cmd.Flags().Changed("after-key-base64") {
			return wperrors.NewUsageError("--after-key and --after-key-base64 are mutually exclusive")
		}
		prefix := ""
		if len(args) > 0 {
			prefix = args[0]
		}
		if afterKeyBase64 != "" {
			decoded, err := base64.StdEncoding.DecodeString(afterKeyBase64)
			if err != nil {
				return wperrors.NewUsageError("invalid --after-key-base64: " + err.Error())
			}
			afterKey = string(decoded)
		}
		if afterKey != "" && prefix != "" && !strings.HasPrefix(afterKey, prefix) {
			return wperrors.NewUsageError("--after-key must lie within the requested prefix")
		}
		cli, err := openRawEtcd(&flags)
		if err != nil {
			return err
		}
		defer cli.Close()
		return runEtcdList(cmd, cli, prefix, afterKey, limit, revision)
	}}
	registerRawEtcd(&flags, c)
	c.Flags().Int64Var(&limit, "limit", 100, "Maximum keys per page")
	c.Flags().Int64Var(&revision, "revision", 0, "Read at this etcd snapshot revision (0: latest)")
	c.Flags().StringVar(&afterKey, "after-key", "", "Exclusive page start; use the previous response's next_key")
	c.Flags().StringVar(&afterKeyBase64, "after-key-base64", "", "Exclusive page start from next_key_base64; preserves binary keys")
	return c
}

func runEtcdList(cmd *cobra.Command, cli *clientv3.Client, prefix, afterKey string, limit, revision int64) error {
	key := prefix
	opts := []clientv3.OpOption{clientv3.WithKeysOnly(), clientv3.WithLimit(limit), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)}
	if prefix == "" {
		key = "\x00"
		opts = append(opts, clientv3.WithFromKey())
	} else {
		opts = append(opts, clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)))
	}
	if afterKey != "" {
		key = afterKey + "\x00"
	}
	if revision > 0 {
		opts = append(opts, clientv3.WithRev(revision))
	}
	ctx, cancel := metaCtx()
	defer cancel()
	r, err := cli.Get(ctx, key, opts...)
	if err != nil {
		return wperrors.NewNetworkError(err.Error())
	}
	snapshotRevision := revision
	if snapshotRevision == 0 {
		snapshotRevision = r.Header.Revision
	}
	records := []etcdKeyRecord{}
	for _, kv := range r.Kvs {
		records = append(records, etcdRecord(kv))
	}
	next := ""
	if r.More && len(r.Kvs) > 0 {
		next = string(r.Kvs[len(r.Kvs)-1].Key)
	}
	if Globals.Output == "json" || Globals.Output == "yaml" {
		return renderEtcd(cmd, map[string]any{"prefix": prefix, "snapshot_revision": snapshotRevision, "count": r.Count, "more": r.More, "next_key": next, "next_key_base64": base64.StdEncoding.EncodeToString([]byte(next)), "keys": records})
	}
	rows := [][]string{}
	for _, kv := range records {
		rows = append(rows, []string{kv.Key, fmt.Sprint(kv.ModRevision), fmt.Sprint(kv.Version), fmt.Sprint(kv.Lease)})
	}
	if err := output.RenderRowTable(cmd.OutOrStdout(), []string{"KEY", "MOD_REV", "VERSION", "LEASE"}, rows); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "\n%d keys shown; snapshot revision %d; more=%v\n", len(records), snapshotRevision, r.More)
	if r.More {
		fmt.Fprintf(cmd.OutOrStdout(), "Next key: %q (use JSON next_key_base64 with --after-key-base64 and --revision for consistent pagination)\n", next)
	}
	return nil
}

func encodeEtcdValue(value []byte, encoding string) (string, string, error) {
	if encoding == "auto" {
		encoding = "text"
		if !utf8.Valid(value) || strings.IndexFunc(string(value), func(r rune) bool { return unicode.IsControl(r) && r != '\n' && r != '\r' && r != '\t' }) >= 0 {
			encoding = "base64"
		}
	}
	switch encoding {
	case "text":
		if !utf8.Valid(value) {
			return "", "", wperrors.NewUsageError("value is not UTF-8; use --encoding base64, hex or raw")
		}
		return string(value), encoding, nil
	case "base64":
		return base64.StdEncoding.EncodeToString(value), encoding, nil
	case "hex":
		return hex.EncodeToString(value), encoding, nil
	default:
		return "", "", wperrors.NewUsageError("--encoding must be auto, text, base64, hex or raw (cat only)")
	}
}

func newEtcdValueCommand(cat bool) *cobra.Command {
	var flags metaEtcdFlags
	var encoding string
	var revision int64
	verb := "get"
	short := "Show one exact key, revisions, lease and encoded value"
	if cat {
		verb = "cat"
		short = "Print only one key's value; use --encoding raw for exact bytes"
	}
	c := &cobra.Command{Use: verb + " <key>", Short: short, Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if args[0] == "" || revision < 0 {
			return wperrors.NewUsageError("key must be nonempty and --revision nonnegative")
		}
		if !cat && encoding == "raw" {
			return wperrors.NewUsageError("raw output is supported by etcd cat only")
		}
		if encoding != "raw" {
			if _, _, err := encodeEtcdValue(nil, encoding); err != nil {
				return err
			}
		}
		cli, err := openRawEtcd(&flags)
		if err != nil {
			return err
		}
		defer cli.Close()
		return runEtcdValue(cmd, cli, args[0], revision, encoding, cat)
	}}
	registerRawEtcd(&flags, c)
	c.Flags().StringVar(&encoding, "encoding", "auto", "Value encoding: auto|text|base64|hex; cat also supports raw")
	c.Flags().Int64Var(&revision, "revision", 0, "Read at this etcd revision (0: latest)")
	return c
}

func runEtcdValue(cmd *cobra.Command, cli *clientv3.Client, key string, revision int64, encoding string, cat bool) error {
	opts := []clientv3.OpOption{}
	if revision > 0 {
		opts = append(opts, clientv3.WithRev(revision))
	}
	ctx, cancel := metaCtx()
	defer cancel()
	r, err := cli.Get(ctx, key, opts...)
	if err != nil {
		return wperrors.NewNetworkError(err.Error())
	}
	if len(r.Kvs) == 0 {
		return wperrors.NewTargetNotFoundError(key)
	}
	kv := r.Kvs[0]
	if cat && encoding == "raw" {
		_, err := cmd.OutOrStdout().Write(kv.Value)
		return err
	}
	value, encoding, err := encodeEtcdValue(kv.Value, encoding)
	if err != nil {
		return err
	}
	if cat {
		if encoding == "base64" {
			fmt.Fprintln(cmd.ErrOrStderr(), "wp: binary value printed as base64; use --encoding raw for exact bytes")
		}
		_, err := fmt.Fprintln(cmd.OutOrStdout(), value)
		return err
	}
	snapshotRevision := revision
	if snapshotRevision == 0 {
		snapshotRevision = r.Header.Revision
	}
	return renderEtcd(cmd, map[string]any{"key": etcdRecord(kv), "snapshot_revision": snapshotRevision, "value_encoding": encoding, "value": value})
}

// Raw etcd output is independent of typed metadata commands.
func renderEtcd(cmd *cobra.Command, value any) error {
	if Globals.Output == "yaml" {
		return output.RenderYAML(cmd.OutOrStdout(), value)
	}
	return output.RenderJSON(cmd.OutOrStdout(), value)
}

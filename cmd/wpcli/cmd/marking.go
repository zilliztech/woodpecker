package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/cmd/wpcli/output"
	"github.com/zilliztech/woodpecker/common/etcd"
	"github.com/zilliztech/woodpecker/meta"
	"github.com/zilliztech/woodpecker/proto"
)

// The marking command family operates on the compacted-mark distribution records
// (root/marking/<logId>/<segId> in etcd — the Sealed-phase sibling of root/cleaning).
// Unlike the rest of wp, these records live in cluster metadata, not on a node, so the
// commands connect to etcd directly; the etcd endpoints and meta prefix are discovered
// from any node's /admin/config (zero-config), overridable via --etcd / --meta-prefix.

func newMarkingCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "marking",
		Short: "Inspect and resolve compacted-mark distribution records (root/marking)",
		Long: `Inspect and resolve compacted-mark distribution records.

After a segment is compacted, the writer's auditor distributes a "compacted" mark to
every quorum node so each can reclaim its local data.log. Per-node progress is durable
in etcd under root/marking/<logId>/<segId>. A node that keeps failing past the retry
budget parks its record as NOTIFY_PENDING_MANUAL — excluded from auto-retry and waiting
for an operator: verify the node (usually dead or removed), then 'confirm' to delete the
record. Unhandled records are also reaped automatically when the segment is truncated.
A confirmed-away mark costs little: data-holding nodes self-heal via the server-side
pull reconcile; nodes that never held the segment only lose a read optimization.`,
	}
	cmd.AddCommand(newMarkingListCommand(), newMarkingConfirmCommand())
	return cmd
}

// markingEtcdFlags are shared discovery overrides for the marking subcommands.
type markingEtcdFlags struct {
	etcdEndpoints string // comma-separated override; with --meta-prefix, skips /admin/config discovery
	metaPrefix    string // full meta prefix override (e.g. "woodpecker" or "by-dev/woodpecker")
	// TLS/auth overrides. Defaults are discovered from a node's /admin/config (its cert PATHS
	// are server-side paths — valid when wp runs in-pod; override from a remote machine).
	etcdCert          string
	etcdKey           string
	etcdCACert        string
	etcdTLSMinVersion string
	etcdUsername      string
	etcdPassword      string
}

func (f *markingEtcdFlags) register(cmd *cobra.Command) {
	cmd.Flags().StringVar(&f.etcdEndpoints, "etcd", "", "etcd endpoints (comma-separated); default: discovered from a node's /admin/config")
	cmd.Flags().StringVar(&f.metaPrefix, "meta-prefix", "", "metadata key prefix; default: discovered from a node's /admin/config (etcd.rootPath + woodpecker.meta.prefix)")
	cmd.Flags().StringVar(&f.etcdCert, "etcd-cert", "", "etcd TLS client cert file; default: discovered (server-side path, valid in-pod)")
	cmd.Flags().StringVar(&f.etcdKey, "etcd-key", "", "etcd TLS client key file; default: discovered")
	cmd.Flags().StringVar(&f.etcdCACert, "etcd-cacert", "", "etcd TLS CA cert file; default: discovered")
	cmd.Flags().StringVar(&f.etcdTLSMinVersion, "etcd-tls-min-version", "", "etcd TLS min version (1.0/1.1/1.2/1.3); default: discovered")
	cmd.Flags().StringVar(&f.etcdUsername, "etcd-username", "", "etcd auth username; default: discovered")
	cmd.Flags().StringVar(&f.etcdPassword, "etcd-password", "", "etcd auth password; default: discovered")
}

// adminConfigSnapshot is the minimal shape of GET /admin/config we need. The server
// encodes the Go config struct without json tags, so field names match Go's.
type adminConfigSnapshot struct {
	Etcd struct {
		Endpoints []string
		RootPath  string
		Ssl       struct {
			Enabled       bool
			TlsCert       string
			TlsKey        string
			TlsCACert     string
			TlsMinVersion string
		}
		Auth struct {
			Enabled  bool
			UserName string
			Password string
		}
	}
	Woodpecker struct {
		Meta struct {
			Prefix string
		}
	}
}

// markingEtcdConn is the fully resolved etcd connection spec for the marking commands:
// endpoints + key prefix + the TLS/auth material the cluster's own nodes use.
type markingEtcdConn struct {
	endpoints                             []string
	kb                                    *meta.KeyBuilder
	useSSL                                bool
	tlsCert, tlsKey, tlsCACert, tlsMinVer string
	username, password                    string
}

// resolveMarkingEtcd resolves the connection spec from flags or, when endpoints/prefix are not
// both given, from the first reachable node's /admin/config — including the etcd TLS and auth
// settings the cluster itself uses, so `wp marking` works against a secured etcd. Flags always
// override discovered values.
func resolveMarkingEtcd(f *markingEtcdFlags) (*markingEtcdConn, error) {
	conn := &markingEtcdConn{}
	prefix := f.metaPrefix

	if f.etcdEndpoints != "" {
		conn.endpoints = strings.Split(f.etcdEndpoints, ",")
	}

	if len(conn.endpoints) == 0 || prefix == "" {
		res, err := resolveAndDiscover()
		if err != nil {
			return nil, err
		}
		var snap *adminConfigSnapshot
		var lastErr error
		for _, m := range res.Members.Members {
			body, fetchErr := fetchAdminJSON(res.Client.PeerAdminURL(m), "/admin/config")
			if fetchErr != nil {
				lastErr = fetchErr
				continue
			}
			s := &adminConfigSnapshot{}
			if jsonErr := json.Unmarshal(body, s); jsonErr != nil {
				lastErr = jsonErr
				continue
			}
			snap = s
			break
		}
		if snap == nil {
			return nil, wperrors.NewNetworkError(fmt.Sprintf("could not fetch /admin/config from any node: %v", lastErr))
		}
		if len(conn.endpoints) == 0 {
			conn.endpoints = snap.Etcd.Endpoints
		}
		if prefix == "" {
			prefix = path.Join(snap.Etcd.RootPath, snap.Woodpecker.Meta.Prefix)
		}
		// Adopt the cluster's own TLS/auth settings (the admin config endpoint is unredacted by
		// design; the admin plane is an internal network per the wpcli security model).
		if snap.Etcd.Ssl.Enabled {
			conn.useSSL = true
			conn.tlsCert = snap.Etcd.Ssl.TlsCert
			conn.tlsKey = snap.Etcd.Ssl.TlsKey
			conn.tlsCACert = snap.Etcd.Ssl.TlsCACert
			conn.tlsMinVer = snap.Etcd.Ssl.TlsMinVersion
		}
		if snap.Etcd.Auth.Enabled {
			conn.username = snap.Etcd.Auth.UserName
			conn.password = snap.Etcd.Auth.Password
		}
	}

	// Flag overrides (also enable SSL/auth when only the flags are given, e.g. with discovery
	// skipped via --etcd + --meta-prefix).
	if f.etcdCert != "" || f.etcdKey != "" || f.etcdCACert != "" {
		conn.useSSL = true
	}
	if f.etcdCert != "" {
		conn.tlsCert = f.etcdCert
	}
	if f.etcdKey != "" {
		conn.tlsKey = f.etcdKey
	}
	if f.etcdCACert != "" {
		conn.tlsCACert = f.etcdCACert
	}
	if f.etcdTLSMinVersion != "" {
		conn.tlsMinVer = f.etcdTLSMinVersion
	}
	if f.etcdUsername != "" {
		conn.username = f.etcdUsername
	}
	if f.etcdPassword != "" {
		conn.password = f.etcdPassword
	}

	if len(conn.endpoints) == 0 {
		return nil, wperrors.NewUsageError("no etcd endpoints (discovery returned none; pass --etcd)")
	}
	conn.kb = meta.NewKeyBuilder(prefix)
	return conn, nil
}

// markingEtcdClient dials etcd with the resolved TLS/auth material, reusing the same
// common/etcd constructors the server uses.
func markingEtcdClient(conn *markingEtcdConn) (*clientv3.Client, error) {
	timeout := Globals.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	var cli *clientv3.Client
	var err error
	switch {
	case conn.useSSL:
		cli, err = etcd.GetRemoteEtcdSSLClientWithCfg(conn.endpoints, conn.tlsCert, conn.tlsKey, conn.tlsCACert, conn.tlsMinVer,
			clientv3.Config{Username: conn.username, Password: conn.password})
	case conn.username != "":
		cli, err = etcd.GetRemoteEtcdClientWithAuth(conn.endpoints, conn.username, conn.password)
	default:
		cli, err = clientv3.New(clientv3.Config{
			Endpoints:   conn.endpoints,
			DialTimeout: timeout,
		})
	}
	if err != nil {
		return nil, wperrors.NewNetworkError(fmt.Sprintf("connect etcd %v: %v", conn.endpoints, err))
	}
	return cli, nil
}

func markingCtx() (context.Context, context.CancelFunc) {
	timeout := Globals.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
}

// markingRow is one record rendered by `wp marking list`.
type markingRow struct {
	LogId        int64    `json:"log_id"`
	SegmentId    int64    `json:"segment_id"`
	State        string   `json:"state"`
	UnackedNodes []string `json:"unacked_nodes"`
	StartTime    string   `json:"start_time"`
	LastUpdate   string   `json:"last_update"`
	Error        string   `json:"error,omitempty"`
}

func newMarkingListCommand() *cobra.Command {
	var flags markingEtcdFlags
	var logID int64
	var allStates bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List compacted-mark distribution records (default: only NOTIFY_PENDING_MANUAL)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, err := resolveMarkingEtcd(&flags)
			if err != nil {
				return err
			}
			cli, err := markingEtcdClient(conn)
			if err != nil {
				return err
			}
			defer cli.Close()
			return runMarkingList(cmd, cli, conn.kb, logID, cmd.Flags().Changed("log"), allStates)
		},
	}
	flags.register(cmd)
	cmd.Flags().Int64Var(&logID, "log", 0, "restrict to one log id (default: all logs)")
	cmd.Flags().BoolVar(&allStates, "all-states", false, "include IN_PROGRESS and COMPLETED records, not just PENDING_MANUAL")
	return cmd
}

// runMarkingList is the etcd-facing core of `wp marking list`, separated from the
// resolve/dial plumbing so tests can drive it against an embedded etcd client.
func runMarkingList(cmd *cobra.Command, cli *clientv3.Client, kb *meta.KeyBuilder, logID int64, logScoped bool, allStates bool) error {
	// One prefix scan covers all logs; --log narrows to marking/<logId>/.
	prefix := kb.SegmentCompactedNotifyStatusPrefix() + "/"
	if logScoped {
		prefix = kb.BuildLogCompactedNotifyStatusPrefix(logID)
	}

	ctx, cancel := markingCtx()
	defer cancel()
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return wperrors.NewNetworkError(fmt.Sprintf("etcd get %s: %v", prefix, err))
	}

	rows := make([]markingRow, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		status := &proto.SegmentCompactedNotifyStatus{}
		if err := proto.UnmarshalSegmentCompactedNotifyStatus(kv.Value, status); err != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "warn: skipping unparseable record %s: %v\n", string(kv.Key), err)
			continue
		}
		if !allStates && status.State != proto.SegmentCompactedNotifyState_NOTIFY_PENDING_MANUAL {
			continue
		}
		var unacked []string
		for node, acked := range status.QuorumNotifyStatus {
			if !acked {
				unacked = append(unacked, node)
			}
		}
		sort.Strings(unacked)
		rows = append(rows, markingRow{
			LogId:        status.LogId,
			SegmentId:    status.SegmentId,
			State:        strings.TrimPrefix(status.State.String(), "NOTIFY_"),
			UnackedNodes: unacked,
			StartTime:    formatMarkingTime(status.StartTime),
			LastUpdate:   formatMarkingTime(status.LastUpdateTime),
			Error:        status.ErrorMessage,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].LogId != rows[j].LogId {
			return rows[i].LogId < rows[j].LogId
		}
		return rows[i].SegmentId < rows[j].SegmentId
	})

	w := cmd.OutOrStdout()
	switch Globals.Output {
	case "json":
		return output.RenderJSON(w, rows)
	case "yaml":
		return output.RenderYAML(w, rows)
	default:
		if len(rows) == 0 {
			if allStates {
				fmt.Fprintln(w, "no marking records")
			} else {
				fmt.Fprintln(w, "no records pending manual handling (use --all-states to list every record)")
			}
			return nil
		}
		headers := []string{"LOG", "SEGMENT", "STATE", "UNACKED_NODES", "START", "LAST_UPDATE", "ERROR"}
		table := make([][]string, len(rows))
		for i, r := range rows {
			table[i] = []string{
				strconv.FormatInt(r.LogId, 10),
				strconv.FormatInt(r.SegmentId, 10),
				r.State,
				strings.Join(r.UnackedNodes, ","),
				r.StartTime,
				r.LastUpdate,
				r.Error,
			}
		}
		return output.RenderRowTable(w, headers, table)
	}
}

func newMarkingConfirmCommand() *cobra.Command {
	var flags markingEtcdFlags
	var force bool
	cmd := &cobra.Command{
		Use:   "confirm <logId> <segmentId>",
		Short: "Confirm and delete a PENDING_MANUAL marking record after operator triage",
		Long: `Confirm and delete one compacted-mark distribution record.

Meant for records parked as NOTIFY_PENDING_MANUAL: after verifying the unacked node
(typically permanently dead or removed from the cluster), confirm to delete the record.
Refuses records still IN_PROGRESS or COMPLETED unless --force is given (those are
managed automatically and normally need no operator action).`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			logID, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				return wperrors.NewUsageError(fmt.Sprintf("invalid logId %q", args[0]))
			}
			segID, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return wperrors.NewUsageError(fmt.Sprintf("invalid segmentId %q", args[1]))
			}

			conn, err := resolveMarkingEtcd(&flags)
			if err != nil {
				return err
			}
			cli, err := markingEtcdClient(conn)
			if err != nil {
				return err
			}
			defer cli.Close()
			return runMarkingConfirm(cmd, cli, conn.kb, logID, segID, force)
		},
	}
	flags.register(cmd)
	cmd.Flags().BoolVar(&force, "force", false, "delete even if the record is not PENDING_MANUAL")
	return cmd
}

// runMarkingConfirm is the etcd-facing core of `wp marking confirm`, separated from the
// resolve/dial plumbing so tests can drive it against an embedded etcd client.
func runMarkingConfirm(cmd *cobra.Command, cli *clientv3.Client, kb *meta.KeyBuilder, logID int64, segID int64, force bool) error {
	key := kb.BuildSegmentCompactedNotifyStatusKey(logID, segID)
	ctx, cancel := markingCtx()
	defer cancel()

	resp, err := cli.Get(ctx, key)
	if err != nil {
		return wperrors.NewNetworkError(fmt.Sprintf("etcd get %s: %v", key, err))
	}
	if len(resp.Kvs) == 0 {
		return wperrors.NewTargetNotFoundError(fmt.Sprintf("marking record %d/%d", logID, segID))
	}
	status := &proto.SegmentCompactedNotifyStatus{}
	if err := proto.UnmarshalSegmentCompactedNotifyStatus(resp.Kvs[0].Value, status); err != nil {
		return fmt.Errorf("unparseable marking record %s: %w", key, err)
	}
	if status.State == proto.SegmentCompactedNotifyState_NOTIFY_OPERATOR_CONFIRMED {
		fmt.Fprintf(cmd.OutOrStdout(), "already confirmed: marking record log %d segment %d\n", logID, segID)
		return nil
	}
	if status.State != proto.SegmentCompactedNotifyState_NOTIFY_PENDING_MANUAL && !force {
		return wperrors.NewStateConflictError(fmt.Sprintf(
			"record %d/%d is %s, not PENDING_MANUAL — it is managed automatically; use --force to confirm anyway",
			logID, segID, status.State.String()))
	}

	// Confirm by transitioning to a durable terminal state, NOT by physical delete: the segment
	// is still Sealed, so a physical delete would let the writer's auditor rebuild the record on
	// restart and re-enter PENDING_MANUAL after another retry cycle. OPERATOR_CONFIRMED is
	// re-seeded as settled across restarts; physical deletion is left to truncate-reap.
	prevState := status.State
	status.State = proto.SegmentCompactedNotifyState_NOTIFY_OPERATOR_CONFIRMED
	status.LastUpdateTime = uint64(time.Now().UnixMilli())
	newVal, err := proto.MarshalSegmentCompactedNotifyStatus(status)
	if err != nil {
		return fmt.Errorf("marshal marking record %s: %w", key, err)
	}
	// Compare-and-put against the revision we read: if a concurrent deleter (orphan sweep /
	// truncate reap) or the manager touched the key in between, the delete/update wins and we
	// must NOT resurrect a reaped record with an unconditional Put.
	ok, err := casPutMarkingRecord(ctx, cli, key, string(newVal), resp.Kvs[0].ModRevision)
	if err != nil {
		return wperrors.NewNetworkError(fmt.Sprintf("etcd txn %s: %v", key, err))
	}
	if !ok {
		return wperrors.NewStateConflictError(fmt.Sprintf(
			"marking record %d/%d changed or was reaped concurrently; re-run `wp marking list` and retry if it still shows",
			logID, segID))
	}
	fmt.Fprintf(cmd.OutOrStdout(), "confirmed: marking record log %d segment %d set to OPERATOR_CONFIRMED (was %s); it will be reaped when the segment is truncated\n",
		logID, segID, prevState.String())
	return nil
}

// casPutMarkingRecord writes val to key only if the key still exists at exactly modRev — a
// compare-and-put honoring the manager's update-if-present invariant. Returns false when the
// key was deleted or rewritten concurrently (the other writer wins).
func casPutMarkingRecord(ctx context.Context, cli *clientv3.Client, key, val string, modRev int64) (bool, error) {
	txnResp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", modRev)).
		Then(clientv3.OpPut(key, val)).
		Commit()
	if err != nil {
		return false, err
	}
	return txnResp.Succeeded, nil
}

func formatMarkingTime(unixMilli uint64) string {
	if unixMilli == 0 {
		return "-"
	}
	return time.UnixMilli(int64(unixMilli)).UTC().Format("2006-01-02T15:04:05Z")
}

package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "google.golang.org/protobuf/proto"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/proto"
)

func validateMetaMessage(msg pb.Message) []string {
	issues := []string{}
	switch m := msg.(type) {
	case *proto.LogMeta:
		if m.LogId < 0 {
			issues = append(issues, "logId must be nonnegative")
		}
		if m.MaxSegmentRollTimeSeconds < 0 || m.MaxSegmentRollSizeBytes < 0 || m.CompactionBufferSizeBytes < 0 || m.MaxCompactionFileCount < 0 {
			issues = append(issues, "policy fields must be nonnegative")
		}
		if m.TruncatedSegmentId < -1 || m.TruncatedEntryId < -1 {
			issues = append(issues, "invalid truncation frontier")
		}
		if m.TruncatedSegmentId == -1 && m.TruncatedEntryId != -1 {
			issues = append(issues, "unset truncation segment requires entry -1")
		}
	case *proto.SegmentMetadata:
		if m.SegNo < 0 || m.LastEntryId < -1 || m.Size < 0 || m.CreateTime <= 0 {
			issues = append(issues, "invalid segment ID, lastEntryId, size or createTime")
		}
		if _, ok := proto.SegmentState_name[int32(m.State)]; !ok {
			issues = append(issues, "unknown segment state")
		}
		if m.CompletionTime < 0 || m.SealedTime < 0 {
			issues = append(issues, "timestamps must be nonnegative")
		}
		if m.State == proto.SegmentState_Active && (m.CompletionTime != 0 || m.SealedTime != 0) {
			issues = append(issues, "Active segment must not have completionTime or sealedTime")
		}
		if m.State != proto.SegmentState_Active && m.CompletionTime == 0 {
			issues = append(issues, "finalized segment requires completionTime")
		}
		if m.CompletionTime != 0 && m.CompletionTime < m.CreateTime {
			issues = append(issues, "completionTime precedes createTime")
		}
		if m.State == proto.SegmentState_Sealed && m.SealedTime == 0 {
			issues = append(issues, "Sealed segment requires sealedTime")
		}
		if m.SealedTime != 0 && (m.CompletionTime == 0 || m.SealedTime < m.CompletionTime) {
			issues = append(issues, "sealedTime precedes completionTime")
		}
		if m.Quorum != nil {
			issues = append(issues, validateMetaMessage(m.Quorum)...)
		}
	case *proto.QuorumInfo:
		if m.Id < 0 || m.Es <= 0 || m.Wq <= 0 || m.Aq <= 0 || m.Wq > m.Es || m.Aq > m.Wq || int(m.Es) != len(m.Nodes) {
			issues = append(issues, "invalid quorum sizes or node count")
		}
		seen := map[string]bool{}
		for _, node := range m.Nodes {
			if strings.TrimSpace(node) == "" || seen[node] {
				issues = append(issues, "empty or duplicate quorum endpoint")
			}
			seen[node] = true
		}
		if len(m.Replicas) > 0 {
			if len(m.Replicas) != len(m.Nodes) {
				issues = append(issues, "replica topology count differs from nodes")
			}
			for _, replica := range m.Replicas {
				if !seen[replica.Endpoint] {
					issues = append(issues, "replica endpoint is not in quorum nodes")
				}
			}
		}
	}
	return issues
}

func newMetaCheckCommand() *cobra.Command {
	var flags metaEtcdFlags
	c := &cobra.Command{Use: "check <logName>", Short: "Validate log/segment metadata constraints (does not verify physical data)", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if err := validateMetaTarget("log", args); err != nil {
			return err
		}
		cli, kb, err := openMeta(&flags)
		if err != nil {
			return err
		}
		defer cli.Close()
		key, err := metaTargetKey("log", kb, args)
		if err != nil {
			return err
		}
		ctx, cancel := metaCtx()
		defer cancel()
		resp, err := cli.Get(ctx, key, clientv3.WithPrefix())
		if err != nil {
			return wperrors.NewNetworkError(err.Error())
		}
		issues := []string{}
		found := false
		segmentsChecked := 0
		segments := map[int64]*proto.SegmentMetadata{}
		var logMeta *proto.LogMeta
		for _, kv := range resp.Kvs {
			k := string(kv.Key)
			kind := "log"
			if k != key {
				if !strings.HasPrefix(k, key+"/segments/") {
					continue
				}
				kind = "segment"
				segmentsChecked++
			} else {
				found = true
			}
			_, msg, err := decodeMeta(kind, kv)
			if err != nil {
				issues = append(issues, err.Error())
				continue
			}
			for _, issue := range validateMetaMessage(msg) {
				issues = append(issues, k+": "+issue)
			}
			if kind == "log" {
				logMeta = msg.(*proto.LogMeta)
			} else {
				m := msg.(*proto.SegmentMetadata)
				id, err := strconv.ParseInt(strings.TrimPrefix(k, key+"/segments/"), 10, 64)
				if err != nil || id != m.SegNo {
					issues = append(issues, k+": key and segNo differ")
				}
				segments[m.SegNo] = m
			}
		}
		if !found {
			return wperrors.NewTargetNotFoundError(key)
		}
		if logMeta != nil && logMeta.TruncatedSegmentId >= 0 {
			m := segments[logMeta.TruncatedSegmentId]
			if m == nil {
				issues = append(issues, "truncation frontier segment is missing")
			} else if m.State != proto.SegmentState_Active && logMeta.TruncatedEntryId > m.LastEntryId {
				issues = append(issues, "truncation frontier exceeds segment LastEntryID")
			}
		}
		state := "ok"
		if len(issues) > 0 {
			state = "invalid"
		}
		if err := renderMeta(cmd, map[string]any{"log_name": args[0], "snapshot_revision": resp.Header.Revision, "state": state, "segments_checked": segmentsChecked, "issues": issues, "scope": "metadata only; use segment inspect/log scan for physical data"}); err != nil {
			return err
		}
		if len(issues) > 0 {
			return wperrors.NewRedFindingError(fmt.Sprintf("%d metadata constraint violations", len(issues)))
		}
		return nil
	}}
	flags.register(c)
	return c
}

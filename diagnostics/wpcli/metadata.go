package wpcli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

// MetadataRequest binds the existing read-only CLI commands to an explicit
// client metadata namespace, independently of any WP server context.
type MetadataRequest struct{ ID, LogName, Etcd, Prefix string }

func ReadMetadata(ctx context.Context, connection d.Connection, q MetadataRequest, limit int64) (d.Observation, error) {
	o := d.Observation{ID: "metadata:" + q.ID, SourceID: "wpcli:meta", Kind: "wpcli_metadata", StartedAt: time.Now().UTC(), Status: "unavailable"}
	s, e := connectionSettings(connection, false)
	if e != nil {
		return o, fmt.Errorf("invalid existing wpcli connection")
	}
	if q.ID != "M03" && q.LogName == "" || strings.HasPrefix(q.LogName, "-") || q.Etcd == "" || q.Prefix == "" || len(q.LogName+q.Etcd+q.Prefix) > 2048 || strings.ContainsAny(q.LogName+q.Etcd+q.Prefix, "\x00\r\n") {
		return o, fmt.Errorf("configure explicit log name and client metadata authority")
	}
	command := map[string][]string{"M01": {"meta", "log", "show"}, "M02": {"meta", "segment", "list"}, "M03": {"meta", "quorum", "list"}, "M04": {"meta", "check"}}[q.ID]
	if command == nil {
		return o, fmt.Errorf("unsupported read-only metadata view")
	}
	args := append([]string{"-o", "json"}, command...)
	args = append(args, "--standalone")
	args = append(args, "--etcd", q.Etcd, "--meta-prefix", q.Prefix)
	if q.ID != "M03" {
		args = append(args, "--", q.LogName)
	}
	o.Query = &d.SourceQuery{Language: "wpcli", Expression: strings.Join(command, " ") + " (configured metadata authority; credentials omitted)", Executed: true}
	x := execute(ctx, s, args, min(limit, s.NativeLimits.ResponseBytes))
	o.FinishedAt = time.Now().UTC()
	if x.StartError {
		o.Query.Executed = false
	}
	if x.StartError || x.Overflow || ctx.Err() != nil {
		return o, fmt.Errorf("wpcli unavailable, cancelled, or output exceeded its bound")
	}
	if !json.Valid(x.Data) {
		return o, fmt.Errorf("wpcli did not return valid JSON; check its version and metadata connection")
	}
	if x.ExitCode != 0 && q.ID != "M04" {
		return o, fmt.Errorf("existing wpcli metadata command failed")
	}
	if q.ID == "M04" {
		var check struct {
			State  string   `json:"state"`
			Issues []string `json:"issues"`
		}
		if json.Unmarshal(x.Data, &check) != nil || !((check.State == "ok" && x.ExitCode == 0) || (check.State == "invalid" && len(check.Issues) > 0 && x.ExitCode != 0)) {
			return o, fmt.Errorf("wpcli meta check result and exit status disagree")
		}
	}
	o.Data = append(json.RawMessage(nil), x.Data...)
	o.Status = "observed"
	o.Query = &d.SourceQuery{Language: "wpcli", Expression: strings.Join(command, " ") + " (configured metadata authority; credentials omitted)", Executed: true}
	return o, nil
}

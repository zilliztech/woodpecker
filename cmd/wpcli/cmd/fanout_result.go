package cmd

import (
	"fmt"
	"io"

	"github.com/zilliztech/woodpecker/cmd/wpcli/client"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

// checkFanout turns an incomplete fan-out into the right outcome, so every command that queries
// several nodes agrees on what a missing answer means.
//
// Two cases are not the same. A run where nothing answered produced no view of the cluster, and
// reporting success for it invites a caller to read an empty table as an empty cluster; that is
// an error whether or not strict mode is on. A run where some nodes answered is a real but
// partial view, useful to look at and unsafe to act on, so it stays a warning until --strict
// says otherwise.
func checkFanout(res *client.FanoutResult, total int) error {
	if res.NoneReachable() {
		return wperrors.NewNetworkError(fmt.Sprintf("no node answered: all %d unreachable", total))
	}
	if res.StrictFailure() {
		return wperrors.NewStrictPartialFailureError(res.Unreachable, total)
	}
	return nil
}

// warnIfPartial tells the reader that what follows is missing nodes. Without it an unreachable
// node is just a row among others, and the table reads as the whole truth.
func warnIfPartial(w io.Writer, unreachable, total int) {
	if unreachable > 0 {
		fmt.Fprintf(w, "\nWARNING: %d/%d nodes unreachable — this view is incomplete\n", unreachable, total)
	}
}

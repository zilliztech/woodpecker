// Package cli retains an explicit migration notice for the retired executable.
package cli

import (
	"context"
	"fmt"
	"io"
)

// Execute never queries a source or starts the retired Milvus/Loki/combined UI.
func Execute(_ context.Context, args []string, out, errout io.Writer) int {
	message := "wpdiag has been retired. Use wp diag metrics check, wp diag trace check, wp diag scenario, wp report, or wp webui. Milvus and Loki diagnosis are outside this tool's scope."
	if len(args) == 0 || len(args) == 1 && (args[0] == "--help" || args[0] == "-h") {
		fmt.Fprintln(out, message)
		return 0
	}
	fmt.Fprintln(errout, message)
	return 2
}

package main

import (
	"fmt"
	"os"

	"github.com/zilliztech/woodpecker/cmd/wpcli/cmd"
	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
	"github.com/zilliztech/woodpecker/common/logger"
)

func main() {
	root := cmd.NewRootCommand()
	err := root.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "wp:", err)
	}
	logger.Shutdown()
	os.Exit(wperrors.ExitCodeFor(err))
}

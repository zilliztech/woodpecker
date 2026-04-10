package main

import (
	"fmt"
	"os"

	"github.com/zilliztech/woodpecker/cmd/wpcli/cmd"
)

func main() {
	root := cmd.NewRootCommand()
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "wp:", err)
		os.Exit(1)
	}
}

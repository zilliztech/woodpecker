package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/zilliztech/woodpecker/cmd/wpdiag/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Execute(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/chirino/kube-rsync-machine/internal/cli"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	app := cli.New(version, commit, date)
	if err := app.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

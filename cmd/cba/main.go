// Command cba verifies, inspects and exports the Celestia blob archive.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/noders-team/celestia-blob-archive/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	code, msg := cli.ExitStatus(cli.NewRoot().ExecuteContext(ctx))
	if msg != "" {
		fmt.Fprintln(os.Stderr, msg)
	}
	stop()
	os.Exit(code)
}

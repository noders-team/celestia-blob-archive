// Package cli implements the cba commands.
package cli

import (
	"encoding/base64"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/noders-team/celestia-blob-archive/internal/client"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/verify"
	"github.com/noders-team/celestia-blob-archive/internal/version"
)

func b64Flag(name, s string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("--%s: invalid base64: %w", name, err)
	}
	return b, nil
}

// NewRoot returns the cba command tree.
func NewRoot() *cobra.Command {
	root := &cobra.Command{
		Use:           "cba",
		Short:         "Verify and read the Celestia blob archive without trusting it",
		Version:       version.Version,
		SilenceUsage:  true,
		SilenceErrors: true, // main prints the error via ExitStatus
	}
	root.AddCommand(verifyCmd(), inspectCmd(), exportCmd(), fibreCmd())
	return root
}

func verifyCmd() *cobra.Command {
	var (
		archiveURL, ns, commitment, rpc string
		height                          int64
		rpcTLS                          bool
	)
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify one archived blob against a Celestia node",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			nsB, err := b64Flag("namespace", ns)
			if err != nil {
				return err
			}
			cB, err := b64Flag("commitment", commitment)
			if err != nil {
				return err
			}
			src, err := source.NewGRPC(rpc, rpcTLS)
			if err != nil {
				return err
			}
			defer src.Close()
			err = verify.Verify(cmd.Context(), client.New(archiveURL), src, verify.Params{Height: height, Namespace: nsB, Commitment: cB})
			if err != nil {
				return err // ExitStatus tells a failed check from a failed run
			}
			fmt.Fprintln(cmd.OutOrStdout(), "OK")
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&archiveURL, "archive", "", "archive base URL")
	f.Int64Var(&height, "height", 0, "block height")
	f.StringVar(&ns, "namespace", "", "namespace, base64 (29 bytes)")
	f.StringVar(&commitment, "commitment", "", "blob share commitment, base64")
	f.StringVar(&rpc, "rpc", "", "Celestia consensus node gRPC address host:port")
	f.BoolVar(&rpcTLS, "rpc-tls", false, "use TLS for --rpc")
	for _, n := range []string{"archive", "height", "namespace", "commitment", "rpc"} {
		_ = cmd.MarkFlagRequired(n)
	}
	return cmd
}

func inspectCmd() *cobra.Command {
	var (
		archiveURL, rpc string
		rpcTLS          bool
	)
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Show archive manifest, committed ranges, gaps and lag",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var head HeadSource
			if rpc != "" {
				src, err := source.NewGRPC(rpc, rpcTLS)
				if err != nil {
					return err
				}
				defer src.Close()
				head = src
			}
			return Inspect(cmd.Context(), client.New(archiveURL), head, cmd.OutOrStdout())
		},
	}
	f := cmd.Flags()
	f.StringVar(&archiveURL, "archive", "", "archive base URL")
	f.StringVar(&rpc, "rpc", "", "optional node gRPC address, to report lag")
	f.BoolVar(&rpcTLS, "rpc-tls", false, "use TLS for --rpc")
	_ = cmd.MarkFlagRequired("archive")
	return cmd
}

func exportCmd() *cobra.Command {
	var (
		archiveURL, ns, out string
		from, to            int64
		force               bool
	)
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export the blobs of one namespace to files",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			nsB, err := b64Flag("namespace", ns)
			if err != nil {
				return err
			}
			n, err := Export(cmd.Context(), client.New(archiveURL), ExportOptions{
				Namespace: nsB, From: from, To: to, Out: out, Force: force,
			})
			if err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "exported %d blobs to %s\n", n, out)
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&archiveURL, "archive", "", "archive base URL")
	f.StringVar(&ns, "namespace", "", "namespace, base64 (29 bytes)")
	f.Int64Var(&from, "from", 0, "first height")
	f.Int64Var(&to, "to", 0, "last height")
	f.StringVar(&out, "out", "", "output directory")
	f.BoolVar(&force, "force", false, "overwrite existing files")
	for _, n := range []string{"archive", "namespace", "from", "to", "out"} {
		_ = cmd.MarkFlagRequired(n)
	}
	return cmd
}

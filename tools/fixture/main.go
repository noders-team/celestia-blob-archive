// Command fixture captures a block and its successor from a consensus gRPC
// endpoint into testdata/blocks/<network>-<height>.json for offline tests.
//
// The file holds {"cur": source.Block, "next": source.Block}; byte fields are
// base64 (encoding/json default).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	blobtx "github.com/celestiaorg/go-square/v4/tx"

	"github.com/noders-team/celestia-blob-archive/internal/source"
)

type fixture struct {
	Cur  *source.Block `json:"cur"`
	Next *source.Block `json:"next"`
}

func main() {
	addr := flag.String("grpc", "", "consensus gRPC address host:port")
	useTLS := flag.Bool("tls", false, "use TLS")
	network := flag.String("network", "", "network name used in the file name (e.g. mainnet, mocha)")
	heights := flag.String("heights", "", "comma separated heights to capture")
	emptyFrom := flag.Int64("empty-from", 0, "also capture the first block at or after this height that has no BlobTx")
	out := flag.String("out", "testdata/blocks", "output directory")
	flag.Parse()

	if *addr == "" || *network == "" || (*heights == "" && *emptyFrom == 0) {
		fmt.Fprintln(os.Stderr, "usage: fixture -grpc addr [-tls] -network name (-heights h1,h2 | -empty-from h) [-out dir]")
		os.Exit(2)
	}
	if err := run(*addr, *useTLS, *network, *heights, *emptyFrom, *out); err != nil {
		fmt.Fprintln(os.Stderr, "fixture:", err)
		os.Exit(1)
	}
}

func run(addr string, useTLS bool, network, heights string, emptyFrom int64, out string) error {
	src, err := source.NewGRPC(addr, useTLS)
	if err != nil {
		return err
	}
	defer src.Close()
	ctx := context.Background()

	var hs []int64
	for _, s := range strings.Split(heights, ",") {
		if s = strings.TrimSpace(s); s == "" {
			continue
		}
		h, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("bad height %q: %w", s, err)
		}
		hs = append(hs, h)
	}
	if emptyFrom > 0 {
		h, err := findEmpty(ctx, src, emptyFrom)
		if err != nil {
			return err
		}
		hs = append(hs, h)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	for _, h := range hs {
		if err := capture(ctx, src, network, h, out); err != nil {
			return fmt.Errorf("height %d: %w", h, err)
		}
	}
	return nil
}

// fetch gets one block with a per-call timeout so a stuck node fails fast.
func fetch(ctx context.Context, src *source.GRPC, h int64) (*source.Block, error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	return src.Block(ctx, h)
}

func findEmpty(ctx context.Context, src *source.GRPC, from int64) (int64, error) {
	for h := from; h < from+1000; h++ {
		b, err := fetch(ctx, src, h)
		if err != nil {
			return 0, fmt.Errorf("scan height %d: %w", h, err)
		}
		n := 0
		for _, tx := range b.Txs {
			if _, isBlobTx, _ := blobtx.UnmarshalBlobTx(tx); isBlobTx {
				n++
			}
		}
		if n == 0 {
			return h, nil
		}
	}
	return 0, fmt.Errorf("no block without BlobTx within 1000 blocks of %d", from)
}

func capture(ctx context.Context, src *source.GRPC, network string, h int64, out string) error {
	cur, err := fetch(ctx, src, h)
	if err != nil {
		return err
	}
	next, err := fetch(ctx, src, h+1)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(fixture{Cur: cur, Next: next}, "", " ")
	if err != nil {
		return err
	}
	path := filepath.Join(out, fmt.Sprintf("%s-%d.json", network, h))
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	fmt.Printf("%s: %d bytes, app %d, %d txs\n", path, len(data), cur.AppVersion, len(cur.Txs))
	return nil
}

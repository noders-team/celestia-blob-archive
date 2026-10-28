//go:build integration

package fibrecapture

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/noders-team/celestia-blob-archive/internal/source"
)

// TestFibreCaptureIntegration is read-only: it starts the fibre client and
// reads the params, it downloads nothing. CBA_IT_GRPC is a plaintext app
// gRPC address; CBA_IT_CHAIN_ID defaults to mocha-5.
func TestFibreCaptureIntegration(t *testing.T) {
	addr := os.Getenv("CBA_IT_GRPC")
	if addr == "" {
		t.Skip("CBA_IT_GRPC not set")
	}
	wantChain := os.Getenv("CBA_IT_CHAIN_ID")
	if wantChain == "" {
		wantChain = "mocha-5"
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	d, chain, err := NewClientDownloader(ctx, addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(context.Background())
	if chain != wantChain {
		t.Errorf("chain id %q, want %q", chain, wantChain)
	}

	src, err := source.NewGRPC(addr, false)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	ret, timeout, err := NewGRPCParams(src.Conn()).Fibre(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ret != 4*time.Hour {
		t.Errorf("shard_retention %v, want 4h", ret)
	}
	if timeout <= 0 {
		t.Errorf("payment_promise_timeout %v", timeout)
	}
}

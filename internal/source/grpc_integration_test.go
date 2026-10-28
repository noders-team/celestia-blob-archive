//go:build integration

package source

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestGRPCMochaBlock(t *testing.T) {
	addr := os.Getenv("CBA_TEST_GRPC")
	if addr == "" {
		t.Skip("CBA_TEST_GRPC not set")
	}
	src, err := NewGRPC(addr, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	head, err := src.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if head < 1834013 {
		t.Fatalf("head = %d, want >= 1834013", head)
	}

	const h = 1834013
	b, err := src.Block(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if b.Height != h {
		t.Errorf("Height = %d", b.Height)
	}
	if b.AppVersion != 10 {
		t.Errorf("AppVersion = %d, want 10", b.AppVersion)
	}
	if len(b.Txs) != 3 {
		t.Errorf("len(Txs) = %d, want 3", len(b.Txs))
	}
	if b.SquareSize != 8 {
		t.Errorf("SquareSize = %d, want 8", b.SquareSize)
	}
	if len(b.DataHash) != 32 {
		t.Errorf("len(DataHash) = %d, want 32", len(b.DataHash))
	}
	if len(b.HeaderProto) == 0 || len(b.LastCommitProto) == 0 {
		t.Errorf("empty protos: header=%d commit=%d", len(b.HeaderProto), len(b.LastCommitProto))
	}
	if b.Time.IsZero() || b.Time.Location() != time.UTC {
		t.Errorf("Time = %v, want non-zero UTC", b.Time)
	}
}

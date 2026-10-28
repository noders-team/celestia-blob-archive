// Package source reads blocks from a Celestia consensus node.
package source

import (
	"context"
	"time"
)

// Block is the subset of a consensus block needed for blob archiving.
type Block struct {
	Height          int64
	Time            time.Time
	AppVersion      uint64
	DataHash        []byte
	SquareSize      uint64
	Txs             [][]byte
	HeaderProto     []byte // marshaled tendermint.types.Header
	LastCommitProto []byte // marshaled tendermint.types.Commit
}

// Source provides blocks by height.
type Source interface {
	// Head returns the latest block height.
	Head(ctx context.Context) (int64, error)
	// Block returns the block at the given height.
	Block(ctx context.Context, height int64) (*Block, error)
}

package source

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/cosmos/cosmos-sdk/client/grpc/cmtservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const maxRecvMsgSize = 256 << 20 // 256 MiB

// callTimeout bounds one source call. A connection that stops answering
// otherwise blocks every caller on it forever: on 28.10 the mocha ingester
// sat for 95 minutes with backfill, tail and audit all waiting on one stalled
// connection. A timed-out call is an ordinary source error and is retried.
const callTimeout = 2 * time.Minute

// Keepalive pings let a dead connection be dropped and redialled instead of
// timing out every call on it. grpc-go servers, the Cosmos SDK's included,
// close connections that ping more often than every 5 minutes.
var keepaliveParams = keepalive.ClientParameters{Time: 5 * time.Minute, Timeout: 20 * time.Second}

// GRPC is a Source backed by the cosmos tendermint service.
type GRPC struct {
	conn    *grpc.ClientConn
	client  cmtservice.ServiceClient
	timeout time.Duration
}

var _ Source = (*GRPC)(nil)

// NewGRPC dials addr (host:port). With useTLS the system root CAs are used.
// The connection is lazy: errors surface on the first call.
func NewGRPC(addr string, useTLS bool) (*GRPC, error) {
	creds := insecure.NewCredentials()
	if useTLS {
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvMsgSize)),
		grpc.WithKeepaliveParams(keepaliveParams),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &GRPC{conn: conn, client: cmtservice.NewServiceClient(conn), timeout: callTimeout}, nil
}

// Conn returns the underlying connection, for other query clients on the
// same node. Close closes it.
func (g *GRPC) Conn() *grpc.ClientConn { return g.conn }

// Close releases the underlying connection.
func (g *GRPC) Close() error { return g.conn.Close() }

func (g *GRPC) Head(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	resp, err := g.client.GetLatestBlock(ctx, &cmtservice.GetLatestBlockRequest{})
	if err != nil {
		return 0, fmt.Errorf("get latest block: %w", err)
	}
	if resp.Block == nil {
		return 0, errors.New("get latest block: empty block in response")
	}
	return resp.Block.Header.Height, nil
}

func (g *GRPC) Block(ctx context.Context, height int64) (*Block, error) {
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	resp, err := g.client.GetBlockByHeight(ctx, &cmtservice.GetBlockByHeightRequest{Height: height})
	if err != nil {
		return nil, fmt.Errorf("get block %d: %w", height, err)
	}
	b := resp.Block
	if b == nil {
		return nil, fmt.Errorf("get block %d: empty block in response", height)
	}
	if resp.BlockId == nil {
		return nil, fmt.Errorf("get block %d: no block id in response", height)
	}
	if err := checkHeaderHash(&b.Header, resp.BlockId.Hash); err != nil {
		return nil, fmt.Errorf("get block %d: %w", height, err)
	}
	hdr, err := b.Header.Marshal()
	if err != nil {
		return nil, fmt.Errorf("block %d: marshal header: %w", height, err)
	}
	var commit []byte
	if b.LastCommit != nil {
		if commit, err = b.LastCommit.Marshal(); err != nil {
			return nil, fmt.Errorf("block %d: marshal last commit: %w", height, err)
		}
	}
	return &Block{
		Height:          b.Header.Height,
		Time:            b.Header.Time.UTC(),
		AppVersion:      b.Header.Version.App,
		DataHash:        b.Header.DataHash,
		SquareSize:      b.Data.SquareSize,
		Txs:             b.Data.Txs,
		HeaderProto:     hdr,
		LastCommitProto: commit,
	}, nil
}

// checkHeaderHash requires the CometBFT hash of h to equal want (the block id
// hash the node returned), so a header that does not belong to that block id
// is rejected.
func checkHeaderHash(h *cmtproto.Header, want []byte) error {
	hdr, err := cmttypes.HeaderFromProto(h)
	if err != nil {
		return fmt.Errorf("invalid header: %w", err)
	}
	got := hdr.Hash()
	if len(got) == 0 || !bytes.Equal(got, want) {
		return fmt.Errorf("header hash %X does not match block id %X", []byte(got), want)
	}
	return nil
}

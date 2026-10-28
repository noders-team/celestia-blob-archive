package fibrecapture

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/celestiaorg/celestia-app/v10/fibre"
	"github.com/celestiaorg/celestia-app/v10/fibre/state"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	"google.golang.org/grpc"
)

const (
	valsetCacheTTL  = 30 * time.Second
	startupBudget   = 5 * time.Minute
	startupBackoff0 = time.Second
	startupBackoffN = 15 * time.Second
)

// ClientDownloader is the production Downloader: a download-only fibre.Client.
type ClientDownloader struct {
	c *fibre.Client
}

var _ Downloader = (*ClientDownloader)(nil)

// NewClientDownloader builds a download-only fibre.Client whose state client
// talks to the app node at stateAddr, starts it (retrying for up to 5 min),
// and returns it with the chain ID the node reports.
func NewClientDownloader(ctx context.Context, stateAddr string, log *slog.Logger) (*ClientDownloader, string, error) {
	cfg := fibre.DefaultClientConfig()
	cfg.StateAddress = stateAddr
	if log != nil {
		cfg.Log = log
	}
	if err := cfg.Validate(); err != nil {
		return nil, "", fmt.Errorf("fibre client config: %w", err)
	}
	cfg.StateClientFn = state.WithCachedValset(cfg.StateClientFn, valsetCacheTTL)
	c, err := fibre.NewClient(nil, cfg)
	if err != nil {
		return nil, "", fmt.Errorf("fibre client: %w", err)
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	err = retryStartup(ctx, log, startupBudget, startupBackoff0, startupBackoffN, c.Start)
	if err != nil {
		return nil, "", fmt.Errorf("start fibre client on %s: %w", stateAddr, err)
	}
	return &ClientDownloader{c: c}, c.ChainID(), nil
}

// Download fetches the blob through the client, copies its bytes and frees
// the client's buffers.
func (d *ClientDownloader) Download(ctx context.Context, id [33]byte, promiseHeight uint64) ([]byte, error) {
	blob, err := d.c.Download(ctx, fibre.BlobID(id[:]), fibre.WithHeight(promiseHeight))
	if err != nil {
		return nil, err
	}
	defer blob.Free()
	return append([]byte(nil), blob.Data()...), nil
}

// Close stops the client.
func (d *ClientDownloader) Close(ctx context.Context) error { return d.c.Stop(ctx) }

// retryStartup calls start until it succeeds, ctx ends or budget elapses,
// sleeping first, 2*first, ... capped at maxDelay between attempts.
func retryStartup(ctx context.Context, log *slog.Logger, budget, first, maxDelay time.Duration, start func(context.Context) error) error {
	deadline := time.Now().Add(budget)
	delay := first
	for {
		err := start(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().Add(delay).After(deadline) {
			return err
		}
		log.Warn("fibre client start failed; retrying", "err", err, "in", delay)
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
		delay = min(2*delay, maxDelay)
	}
}

// GRPCParams reads the Fibre module params over the app node's gRPC.
type GRPCParams struct {
	q fibretypes.QueryClient
}

var _ Params = (*GRPCParams)(nil)

// NewGRPCParams queries the x/fibre module over conn.
func NewGRPCParams(conn *grpc.ClientConn) *GRPCParams {
	return &GRPCParams{q: fibretypes.NewQueryClient(conn)}
}

// Fibre returns shard_retention and payment_promise_timeout. A retention
// outside [MinShardRetention, MaxShardRetention] is an error, so Run keeps
// the previous value.
func (p *GRPCParams) Fibre(ctx context.Context) (time.Duration, time.Duration, error) {
	resp, err := p.q.Params(ctx, &fibretypes.QueryParamsRequest{})
	if err != nil {
		return 0, 0, fmt.Errorf("fibre params: %w", err)
	}
	ret := resp.Params.ShardRetention
	if ret < fibretypes.MinShardRetention || ret > fibretypes.MaxShardRetention {
		return 0, 0, fmt.Errorf("fibre params: shard_retention %v outside [%v, %v]",
			ret, fibretypes.MinShardRetention, fibretypes.MaxShardRetention)
	}
	return ret, resp.Params.PaymentPromiseTimeout, nil
}

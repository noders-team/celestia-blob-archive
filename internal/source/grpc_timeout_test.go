package source

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/client/grpc/cmtservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// stalledNode accepts calls and never answers, like the node connection that
// hung the mocha ingester.
type stalledNode struct {
	cmtservice.UnimplementedServiceServer
	release chan struct{}
}

func (n *stalledNode) GetLatestBlock(ctx context.Context, _ *cmtservice.GetLatestBlockRequest) (*cmtservice.GetLatestBlockResponse, error) {
	<-n.release
	return nil, ctx.Err()
}

func (n *stalledNode) GetBlockByHeight(ctx context.Context, _ *cmtservice.GetBlockByHeightRequest) (*cmtservice.GetBlockByHeightResponse, error) {
	<-n.release
	return nil, ctx.Err()
}

func TestGRPCCallsTimeOutOnStalledNode(t *testing.T) {
	ln := bufconn.Listen(1 << 20)
	node := &stalledNode{release: make(chan struct{})}
	s := grpc.NewServer()
	cmtservice.RegisterServiceServer(s, node)
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(func() { close(node.release); s.Stop() })

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return ln.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	g := &GRPC{conn: conn, client: cmtservice.NewServiceClient(conn), timeout: 200 * time.Millisecond}

	for name, call := range map[string]func(context.Context) error{
		"head":  func(ctx context.Context) error { _, err := g.Head(ctx); return err },
		"block": func(ctx context.Context) error { _, err := g.Block(ctx, 1); return err },
	} {
		t.Run(name, func(t *testing.T) {
			// The caller's context has no deadline, as in the ingester.
			done := make(chan error, 1)
			go func() { done <- call(context.Background()) }()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("stalled call returned no error")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("call on a stalled node did not time out")
			}
		})
	}
}

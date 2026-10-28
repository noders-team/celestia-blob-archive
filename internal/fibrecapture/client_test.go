package fibrecapture

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"testing"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// TestClientDownloaderCopiesAndFrees is not written: a fibre.Client over fake
// validators needs fibre's unexported test helpers (makeDownloadMockClientFn)
// and its internal grpc.NewClientFn type. ClientDownloader is covered by the
// integration test and the Task 12 acceptance run.

type fakeFibreQuery struct {
	fibretypes.UnimplementedQueryServer
	params fibretypes.Params
}

func (f *fakeFibreQuery) Params(context.Context, *fibretypes.QueryParamsRequest) (*fibretypes.QueryParamsResponse, error) {
	return &fibretypes.QueryParamsResponse{Params: f.params}, nil
}

func newFakeParams(t *testing.T, srv *fakeFibreQuery) *GRPCParams {
	t.Helper()
	ln := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	fibretypes.RegisterQueryServer(s, srv)
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(s.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return ln.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return NewGRPCParams(conn)
}

func TestGRPCParamsRejectsOutOfBounds(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		retention time.Duration
		ok        bool
	}{
		{4 * time.Hour, true},
		{fibretypes.MinShardRetention, true},
		{fibretypes.MaxShardRetention, true},
		{0, false},
		{fibretypes.MinShardRetention - time.Second, false},
		{fibretypes.MaxShardRetention + time.Second, false},
	}
	srv := &fakeFibreQuery{}
	p := newFakeParams(t, srv)
	for _, c := range cases {
		srv.params = fibretypes.Params{ShardRetention: c.retention, PaymentPromiseTimeout: time.Hour}
		ret, timeout, err := p.Fibre(ctx)
		if !c.ok {
			if err == nil {
				t.Errorf("retention %v: accepted", c.retention)
			}
			continue
		}
		if err != nil {
			t.Errorf("retention %v: %v", c.retention, err)
			continue
		}
		if ret != c.retention || timeout != time.Hour {
			t.Errorf("retention %v: got (%v, %v)", c.retention, ret, timeout)
		}
	}
}

func TestRetryStartup(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	ctx := context.Background()
	calls := 0
	err := retryStartup(ctx, log, time.Second, time.Millisecond, 4*time.Millisecond, func(context.Context) error {
		calls++
		if calls < 4 {
			return errors.New("not yet")
		}
		return nil
	})
	if err != nil || calls != 4 {
		t.Fatalf("err %v after %d calls, want nil after 4", err, calls)
	}

	boom := errors.New("down")
	calls = 0
	err = retryStartup(ctx, log, 20*time.Millisecond, time.Millisecond, 2*time.Millisecond, func(context.Context) error {
		calls++
		return boom
	})
	if !errors.Is(err, boom) || calls < 2 {
		t.Fatalf("err %v after %d calls, want the start error after several tries", err, calls)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	err = retryStartup(cctx, log, time.Minute, time.Second, time.Second, func(context.Context) error { return boom })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: err %v", err)
	}
}

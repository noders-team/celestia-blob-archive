// Command api serves celestia-node's blob and header JSON-RPC methods from
// the public archive. It holds no credentials: it reads the archive over
// HTTP from its public base URL. See docs/api.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/noders-team/celestia-blob-archive/internal/api"
	"github.com/noders-team/celestia-blob-archive/internal/cachestore"
	"github.com/noders-team/celestia-blob-archive/internal/client"
	"github.com/noders-team/celestia-blob-archive/internal/version"
)

// shutdownGrace bounds the drain of in-flight requests on SIGTERM; it is
// longer than the 30 s per-call timeout and shorter than the unit's
// TimeoutStopSec.
const shutdownGrace = 35 * time.Second

// HTTP server timeouts (Global Constraints, request limits).
const (
	readHeaderTimeout = 10 * time.Second
	writeTimeout      = 60 * time.Second
	idleTimeout       = 120 * time.Second
)

func main() {
	err := run(os.Args[1:], nil)
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "api:", err)
	}
	os.Exit(exitCode(err))
}

// usageError is a bad command line; its usage has been printed.
type usageError struct{ err error }

func (e usageError) Error() string { return e.err.Error() }
func (e usageError) Unwrap() error { return e.err }

// exitCode maps run's error to the exit status: 0 on a signal or -h, 2 on a
// bad command line (as package flag does), 1 on any start error.
func exitCode(err error) int {
	var ue usageError
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return 0
	case errors.As(err, &ue):
		return 2
	default:
		return 1
	}
}

type config struct {
	archive, chainID            string
	listen, metricsAddr         string
	cacheBytes                  int64
	openTTL                     time.Duration
	manifestPoll, tombstonePoll time.Duration
	maxBody                     int64
	maxBatch, maxNamespaces     int
	requestTimeout              time.Duration
	maxInflightBytes            int64
}

// parseFlags parses args; on -h or a bad flag it prints the usage to out.
func parseFlags(args []string, out io.Writer) (config, error) {
	c := config{cacheBytes: 512 << 20, maxBody: 1 << 20, maxInflightBytes: 1 << 30}
	fs := flag.NewFlagSet("api", flag.ContinueOnError)
	fs.SetOutput(out)
	fs.StringVar(&c.archive, "archive", os.Getenv("CBA_API_ARCHIVE"), "archive base URL (env CBA_API_ARCHIVE)")
	fs.StringVar(&c.chainID, "chain-id", os.Getenv("CBA_CHAIN_ID"), "chain id the archive must hold (env CBA_CHAIN_ID)")
	fs.StringVar(&c.listen, "listen", "127.0.0.1:8471", "serve JSON-RPC and /health here")
	fs.StringVar(&c.metricsAddr, "metrics-addr", "127.0.0.1:9469", "serve /metrics here; empty disables")
	fs.Var((*byteSize)(&c.cacheBytes), "cache-bytes", "read cache size (e.g. 512MiB)")
	fs.DurationVar(&c.openTTL, "open-ttl", 6*time.Second, "how long an open dir/ bucket or its absence is cached")
	fs.DurationVar(&c.manifestPoll, "manifest-poll", 6*time.Second, "manifest.json reload interval")
	fs.DurationVar(&c.tombstonePoll, "tombstone-poll", time.Minute, "tombstones.json reload interval")
	fs.Var((*byteSize)(&c.maxBody), "max-body", "largest request body (e.g. 1MiB)")
	fs.IntVar(&c.maxBatch, "max-batch", 16, "most calls in one batch")
	fs.IntVar(&c.maxNamespaces, "max-namespaces", 16, "most namespaces in one blob.GetAll")
	fs.DurationVar(&c.requestTimeout, "request-timeout", 30*time.Second, "per-call timeout")
	fs.Var((*byteSize)(&c.maxInflightBytes), "max-inflight-bytes", "memory budget of blob calls across requests (e.g. 1GiB)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return config{}, err
		}
		return config{}, usageError{err} // fs has printed the error and usage
	}
	if fs.NArg() > 0 {
		err := fmt.Errorf("unexpected arguments: %v", fs.Args())
		fmt.Fprintln(out, err)
		fs.Usage()
		return config{}, usageError{err}
	}
	if c.archive == "" {
		return config{}, errors.New("-archive (or CBA_API_ARCHIVE) is required")
	}
	if c.chainID == "" {
		return config{}, errors.New("-chain-id (or CBA_CHAIN_ID) is required")
	}
	for name, d := range map[string]time.Duration{"open-ttl": c.openTTL, "manifest-poll": c.manifestPoll,
		"tombstone-poll": c.tombstonePoll, "request-timeout": c.requestTimeout} {
		if d <= 0 {
			return config{}, fmt.Errorf("-%s must be positive", name)
		}
	}
	if c.maxBatch <= 0 || c.maxNamespaces <= 0 {
		return config{}, errors.New("-max-batch and -max-namespaces must be positive")
	}
	return c, nil
}

// run serves the API until SIGTERM or SIGINT, then drains for up to
// shutdownGrace. ready, if not nil, gets the listen addresses once both
// servers accept connections ("" for a disabled metrics server).
func run(args []string, ready func(apiAddr, metricsAddr string)) error {
	c, err := parseFlags(args, os.Stderr)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := api.NewMetrics(reg)

	// The cache fetches detached from the caller's context, so the client
	// timeout is what bounds a stalled upstream read; without it one hung
	// fetch would pin its key in the cache's singleflight until a restart.
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 64
	tr.ResponseHeaderTimeout = c.requestTimeout
	upstream := m.CountUpstream(client.NewWithClient(c.archive,
		&http.Client{Transport: tr, Timeout: c.requestTimeout}))

	// The cache needs the server's contiguous_until and the server reads
	// through the cache: bind the closure to srv after New.
	var srv *api.Server
	cache := cachestore.New(upstream, func() int64 {
		if srv == nil {
			return 0
		}
		return srv.ContiguousUntil()
	}, cachestore.Options{MaxBytes: c.cacheBytes, OpenTTL: c.openTTL})
	m.WatchCache(cache.Stats)
	srv, err = api.New(cache, api.Config{
		ChainID: c.chainID, MaxBodyBytes: c.maxBody, MaxBatch: c.maxBatch, MaxNamespaces: c.maxNamespaces,
		RequestTimeout: c.requestTimeout, MaxInflightBytes: c.maxInflightBytes,
		ManifestPoll: c.manifestPoll, TombstonePoll: c.tombstonePoll,
	}, m, slog.Default())
	if err != nil {
		return err
	}
	// Start loads the state and returns; the reloads run until ctx is done.
	if err := srv.Start(ctx); err != nil {
		return err
	}

	ln, err := net.Listen("tcp", c.listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", c.listen, err)
	}
	servers := []*http.Server{{Handler: srv.Handler(), ReadHeaderTimeout: readHeaderTimeout,
		WriteTimeout: writeTimeout, IdleTimeout: idleTimeout}}
	listeners := []net.Listener{ln}
	metricsAddr := ""
	if c.metricsAddr != "" {
		mln, err := net.Listen("tcp", c.metricsAddr)
		if err != nil {
			ln.Close()
			return fmt.Errorf("metrics listen %s: %w", c.metricsAddr, err)
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		servers = append(servers, &http.Server{Handler: mux, ReadHeaderTimeout: readHeaderTimeout,
			WriteTimeout: writeTimeout, IdleTimeout: idleTimeout})
		listeners = append(listeners, mln)
		metricsAddr = mln.Addr().String()
	}

	slog.Info("api", "version", version.Version, "chain_id", c.chainID, "archive", c.archive,
		"listen", ln.Addr().String(), "metrics_addr", metricsAddr)
	serveErr := make(chan error, len(servers))
	for i, s := range servers {
		go func() {
			if err := s.Serve(listeners[i]); err != nil && !errors.Is(err, http.ErrServerClosed) {
				serveErr <- err
			}
		}()
	}
	if ready != nil {
		ready(ln.Addr().String(), metricsAddr)
	}

	select {
	case <-ctx.Done():
		slog.Info("api stopping on signal; draining requests", "grace", shutdownGrace)
	case err = <-serveErr:
		err = fmt.Errorf("serve: %w", err)
	}
	// Restore default signal handling: a second SIGTERM or Ctrl-C during
	// the drain ends the process at once.
	stop()
	sctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	for _, s := range servers {
		if serr := s.Shutdown(sctx); serr != nil {
			slog.Warn("shutdown did not drain every request", "err", serr)
		}
	}
	return err
}

// byteSize is a flag.Value of a positive byte count: an integer, optionally
// followed by KiB, MiB or GiB.
type byteSize int64

func (b *byteSize) String() string { return strconv.FormatInt(int64(*b), 10) }

func (b *byteSize) Set(s string) error {
	num, mult := s, int64(1)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}} {
		if n, ok := strings.CutSuffix(s, u.suffix); ok {
			num, mult = n, u.mult
			break
		}
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil || n <= 0 || n > (1<<62)/mult {
		return fmt.Errorf("%q is not a positive byte size (e.g. 1048576, 64KiB, 512MiB, 1GiB)", s)
	}
	*b = byteSize(n * mult)
	return nil
}

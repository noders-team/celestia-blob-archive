// Command ingester archives every Celestia blob with its inclusion proof.
package main

import (
	"context"
	"encoding/hex"
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
	"sync"
	"syscall"
	"time"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"

	"github.com/noders-team/celestia-blob-archive/internal/archive"
	"github.com/noders-team/celestia-blob-archive/internal/audit"
	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
	"github.com/noders-team/celestia-blob-archive/internal/fibrecapture"
	"github.com/noders-team/celestia-blob-archive/internal/ingest"
	"github.com/noders-team/celestia-blob-archive/internal/nsindex"
	"github.com/noders-team/celestia-blob-archive/internal/opbackfill"
	"github.com/noders-team/celestia-blob-archive/internal/oplayer"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
	"github.com/noders-team/celestia-blob-archive/internal/version"
)

// exitStop is the exit status for errors a restart cannot fix (a block that
// cannot be processed, including a data-root mismatch, or a catalog overlap). The systemd unit sets RestartPreventExitStatus=3
// so the service stays failed instead of re-fetching blocks every RestartSec.
const exitStop = 3

func main() {
	err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "ingester:", err)
	}
	os.Exit(exitCode(err))
}

// exitCode maps run's error to the process exit status.
func exitCode(err error) int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, ingest.ErrFatalBlock), errors.Is(err, process.ErrDataRootMismatch), errors.Is(err, archive.ErrOverlap),
		errors.Is(err, store.ErrExists), errors.Is(err, fibrecapture.ErrChainMismatch),
		errors.Is(err, fibrecapture.ErrRecordConflict):
		// ErrExists: an immutable key already holds different bytes; a restart
		// would hit the same key again. So would a record conflict, and a
		// state or node of another chain.
		return exitStop
	default:
		return 1
	}
}

func run() error {
	if len(os.Args) > 1 && os.Args[1] == "tombstone" {
		return runTombstone(os.Args[2:], os.Stdout)
	}
	if len(os.Args) > 1 && os.Args[1] == "migrate-v2" {
		return runMigrate(os.Args[2:], os.Stdout)
	}
	if len(os.Args) > 1 && os.Args[1] == "op-backfill" {
		return runOPBackfill(os.Args[2:], os.Stdout)
	}
	if len(os.Args) > 1 && os.Args[1] == "fibre-capture" {
		return runFibreCapture(os.Args[2:])
	}
	defTLS, err := envBool("CBA_GRPC_TLS", false)
	if err != nil {
		return err
	}
	defCond, err := envBool("CBA_CONDITIONAL_PUT", true)
	if err != nil {
		return err
	}
	defAudit, err := envDuration("CBA_AUDIT_EVERY", time.Minute)
	if err != nil {
		return err
	}
	defNsIndex, err := envDuration("CBA_NSINDEX_EVERY", 10*time.Minute)
	if err != nil {
		return err
	}
	var (
		grpcAddr      = flag.String("grpc", os.Getenv("CBA_GRPC"), "consensus node gRPC address host:port (env CBA_GRPC)")
		grpcTLS       = flag.Bool("grpc-tls", defTLS, "use TLS for gRPC (env CBA_GRPC_TLS)")
		chainID       = flag.String("chain-id", os.Getenv("CBA_CHAIN_ID"), "chain id recorded in the manifest (env CBA_CHAIN_ID)")
		fsRoot        = flag.String("fs-root", os.Getenv("CBA_FS_ROOT"), "write to this directory instead of R2 (env CBA_FS_ROOT)")
		bucket        = flag.String("bucket", os.Getenv("CBA_BUCKET"), "R2 bucket (env CBA_BUCKET); credentials from R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
		condPut       = flag.Bool("conditional-put", defCond, "write immutable objects with PUT If-None-Match: * instead of HEAD+PUT; R2 honours it (env CBA_CONDITIONAL_PUT)")
		workers       = flag.Int("workers", 4, "backfill workers and initial source concurrency")
		maxWorkers    = flag.Int("max-workers", 8, "maximum source concurrency")
		latencyTarget = flag.Duration("latency-target", 2*time.Second, "source p95 latency above which concurrency is halved")
		auditEvery    = flag.Duration("audit-every", defAudit, "audit one random archived height this often; 0 disables (env CBA_AUDIT_EVERY)")
		nsIndexEvery  = flag.Duration("nsindex-every", defNsIndex, "update the namespace index this often; 0 disables (env CBA_NSINDEX_EVERY)")
		metricsAddr   = flag.String("metrics-addr", "127.0.0.1:9466", "serve /metrics here; empty disables")
		opNamespaces  = flag.String("op-namespaces", os.Getenv("CBA_OP_NAMESPACES"), "comma-separated 29-byte hex namespaces to write op-alt-da fallback objects for; empty disables (env CBA_OP_NAMESPACES)")
	)
	flag.Parse()
	if flag.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", flag.Args())
	}
	opNSs, err := oplayer.ParseNamespaces(*opNamespaces)
	if err != nil {
		return fmt.Errorf("-op-namespaces (or CBA_OP_NAMESPACES): %w", err)
	}
	if *grpcAddr == "" {
		return errors.New("-grpc (or CBA_GRPC) is required")
	}
	if *chainID == "" {
		return errors.New("-chain-id (or CBA_CHAIN_ID) is required")
	}

	st, storeDesc, err := openStore(*fsRoot, *bucket, *condPut)
	if err != nil {
		return err
	}

	src, err := source.NewGRPC(*grpcAddr, *grpcTLS)
	if err != nil {
		return fmt.Errorf("connect %s: %w", *grpcAddr, err)
	}
	defer src.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := ingest.NewMetrics(reg)
	am := audit.NewMetrics(reg)
	nm := nsindex.NewMetrics(reg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stopMetrics, err := serveMetrics(*metricsAddr, reg)
	if err != nil {
		return err
	}
	defer stopMetrics()

	slog.Info("ingester", "version", version.Version, "chain_id", *chainID, "grpc", *grpcAddr,
		"grpc_tls", *grpcTLS, "store", storeDesc, "workers", *workers, "max_workers", *maxWorkers,
		"latency_target", *latencyTarget, "metrics_addr", *metricsAddr, "op_namespaces", len(opNSs))

	cfg := ingest.Config{
		ChainID: *chainID, Workers: *workers, MaxWorkers: *maxWorkers, TailDelay: 2,
		RangeSize: 10000, LatencyTarget: *latencyTarget, MetricsAddr: *metricsAddr,
		OPNamespaces: opNSs,
	}
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return ingest.Run(gctx, cfg, ingest.Deps{Source: src, Store: st, Metrics: m})
	})
	// The auditor and the index only log their problems; they end with the
	// ingester (gctx cancelled) and never fail the process.
	if *auditEvery > 0 {
		g.Go(func() error {
			if err := audit.Run(gctx, src, st, *auditEvery, am); err != nil && gctx.Err() == nil {
				slog.Error("auditor stopped", "err", err)
			}
			return nil
		})
	}
	if *nsIndexEvery > 0 {
		g.Go(func() error {
			if err := nsindex.Run(gctx, st, *nsIndexEvery, nm); err != nil && gctx.Err() == nil {
				slog.Error("namespace indexer stopped", "err", err)
			}
			return nil
		})
	}
	err = g.Wait()
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		slog.Info("ingester stopped by signal")
		return nil
	}
	return err
}

// publicURLs maps the archive buckets to their public custom domains.
var publicURLs = map[string]string{
	"celestia-blob-archive-mainnet": "https://celestia-blobs.noders.services",
	"celestia-blob-archive-mocha5":  "https://celestia-t-blobs.noders.services",
}

const tombstoneUsage = "usage: ingester tombstone add -height H -namespace HEX -commitment HEX -reason TEXT [-date YYYY-MM-DD]\n" +
	"       ingester tombstone add-fibre -blob-id HEX -reason TEXT [-date YYYY-MM-DD] [-height H -namespace HEX]"

// runTombstone runs "tombstone add" or "tombstone add-fibre".
func runTombstone(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New(tombstoneUsage)
	}
	switch args[0] {
	case "add":
		return runTombstoneAdd(args[1:], out)
	case "add-fibre":
		return runTombstoneAddFibre(args[1:], out)
	}
	return errors.New(tombstoneUsage)
}

// publicLink returns a function mapping an object key to its public URL:
// publicURL, else the bucket's custom domain, else the bare key.
func publicLink(publicURL, fsRoot, bucket string) func(key string) string {
	base := strings.TrimRight(publicURL, "/")
	if base == "" && fsRoot == "" {
		base = publicURLs[bucket]
	}
	return func(key string) string {
		if base == "" {
			return key
		}
		return base + "/" + key
	}
}

// runTombstoneAddFibre runs "tombstone add-fibre": it appends one entry to the
// fibre array of tombstones.json, then deletes the blob's fibre/data object.
func runTombstoneAddFibre(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("tombstone add-fibre", flag.ContinueOnError)
	var (
		fsRoot    = fs.String("fs-root", os.Getenv("CBA_FS_ROOT"), "archive directory instead of R2 (env CBA_FS_ROOT)")
		bucket    = fs.String("bucket", os.Getenv("CBA_BUCKET"), "R2 bucket (env CBA_BUCKET); credentials from R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
		publicURL = fs.String("public-url", os.Getenv("CBA_PUBLIC_URL"), "public base URL for the printed links (env CBA_PUBLIC_URL; default by bucket)")
		blobID    = fs.String("blob-id", "", "Fibre blob ID, 33 bytes hex")
		height    = fs.Int64("height", 0, "height of one tx carrying the ID (informational; with -namespace)")
		namespace = fs.String("namespace", "", "namespace of that tx, 29 bytes hex (informational; with -height)")
		reason    = fs.String("reason", "", "public reason for the takedown")
		date      = fs.String("date", time.Now().UTC().Format(time.DateOnly), "date of the takedown, YYYY-MM-DD")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if (*height != 0) != (*namespace != "") {
		return errors.New("-height and -namespace go together")
	}
	st, _, err := openStore(*fsRoot, *bucket, true)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	e := tombstone.FibreEntry{BlobID: *blobID, Height: *height, Namespace: *namespace, Date: *date, Reason: *reason}
	return addFibre(ctx, st, publicLink(*publicURL, *fsRoot, *bucket), e, out)
}

// addFibre saves e, then deletes the blob's payload. A failed delete is
// reported after the entry is saved: the running fibre-capture retries it on
// its next tombstone reload, and a rerun of the command (the entry already
// listed) retries it too.
func addFibre(ctx context.Context, st store.Store, link func(string) string, e tombstone.FibreEntry, out io.Writer) error {
	err := tombstone.AddFibre(ctx, st, e)
	dup := errors.Is(err, tombstone.ErrDuplicate)
	if err != nil && !dup {
		return err
	}
	id, err := hex.DecodeString(e.BlobID)
	if err != nil || len(id) != fibrearchive.BlobIDSize {
		return fmt.Errorf("blob id %q: AddFibre accepted an invalid ID", e.BlobID) // unreachable
	}
	dataKey := fibrearchive.DataKey([fibrearchive.BlobIDSize]byte(id))
	if dup {
		fmt.Fprintf(out, "fibre tombstone for blob %x already exists in %s; deleting the data again\n", id, link(tombstone.Key))
	} else {
		fmt.Fprintf(out, "added fibre tombstone for blob %x to %s\n", id, link(tombstone.Key))
	}
	if err := st.Delete(ctx, dataKey); err != nil {
		fmt.Fprintf(out, "could not delete %s: %v\n", link(dataKey), err)
		fmt.Fprintln(out, "the entry is saved; the running fibre-capture retries the delete within 1m")
		return fmt.Errorf("delete %s: %w", dataKey, err)
	}
	fmt.Fprintf(out, "deleted %s; it must now answer 404\n", link(dataKey))
	fmt.Fprintln(out, "the running fibre-capture also deletes it within 1m and never writes it again")
	return nil
}

// runTombstoneAdd runs "tombstone add": it appends one entry to tombstones.json
// and prints the public URLs it affects. It never touches segments; the
// running ingester scrubs them within its tombstone poll, or at its next start.
func runTombstoneAdd(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("tombstone add", flag.ContinueOnError)
	var (
		fsRoot     = fs.String("fs-root", os.Getenv("CBA_FS_ROOT"), "archive directory instead of R2 (env CBA_FS_ROOT)")
		bucket     = fs.String("bucket", os.Getenv("CBA_BUCKET"), "R2 bucket (env CBA_BUCKET); credentials from R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
		publicURL  = fs.String("public-url", os.Getenv("CBA_PUBLIC_URL"), "public base URL for the printed links (env CBA_PUBLIC_URL; default by bucket)")
		height     = fs.Int64("height", 0, "height of the blob")
		namespace  = fs.String("namespace", "", "namespace, 29 bytes hex")
		commitment = fs.String("commitment", "", "share commitment, 32 bytes hex")
		reason     = fs.String("reason", "", "public reason for the takedown")
		date       = fs.String("date", time.Now().UTC().Format(time.DateOnly), "date of the takedown, YYYY-MM-DD")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	st, _, err := openStore(*fsRoot, *bucket, true)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	e := tombstone.Entry{Height: *height, Namespace: *namespace, Commitment: *commitment, Date: *date, Reason: *reason}
	if err := tombstone.Add(ctx, st, e); err != nil {
		return err
	}

	link := publicLink(*publicURL, *fsRoot, *bucket)
	fmt.Fprintf(out, "added tombstone for height %d to %s\n", *height, link(tombstone.Key))
	keys, err := affectedSegments(ctx, st, *height)
	switch {
	case err != nil:
		fmt.Fprintf(out, "could not list the affected segments: %v\n", err)
	case len(keys) == 0:
		fmt.Fprintf(out, "height %d is not archived yet; the ingester writes it scrubbed\n", *height)
	default:
		fmt.Fprintln(out, "segments to be scrubbed by the running ingester:")
		for _, k := range keys {
			fmt.Fprintf(out, "  %s\n", link(k))
		}
	}
	fmt.Fprintln(out, "the ingester applies the tombstone within its poll interval (1m), or at its next start")
	return nil
}

// runMigrate runs "migrate-v2": it converts a format v1 archive to format v2
// in place (see archive.MigrateV2). The ingester must be stopped meanwhile.
func runMigrate(args []string, out io.Writer) error {
	defCond, err := envBool("CBA_CONDITIONAL_PUT", true)
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("migrate-v2", flag.ContinueOnError)
	var (
		fsRoot  = fs.String("fs-root", os.Getenv("CBA_FS_ROOT"), "archive directory instead of R2 (env CBA_FS_ROOT)")
		bucket  = fs.String("bucket", os.Getenv("CBA_BUCKET"), "R2 bucket (env CBA_BUCKET); credentials from R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
		chainID = fs.String("chain-id", os.Getenv("CBA_CHAIN_ID"), "chain id the manifest must hold (env CBA_CHAIN_ID)")
		condPut = fs.Bool("conditional-put", defCond, "write immutable objects with PUT If-None-Match: * (env CBA_CONDITIONAL_PUT)")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if *chainID == "" {
		return errors.New("-chain-id (or CBA_CHAIN_ID) is required")
	}
	st, storeDesc, err := openStore(*fsRoot, *bucket, *condPut)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	stats, err := archive.MigrateV2(ctx, st, *chainID)
	if err != nil {
		return fmt.Errorf("migrate-v2 %s: %w", storeDesc, err)
	}
	if stats.AlreadyV2 {
		fmt.Fprintf(out, "%s: nothing to migrate (already format %d)\n", storeDesc, archive.FormatVersion)
		return nil
	}
	fmt.Fprintf(out, "migrated %s to format %d: %d segments, %d directory buckets; v1 manifest and catalog saved under %s\n",
		storeDesc, archive.FormatVersion, stats.Segments, stats.Buckets, archive.MigrateBackupPrefix)
	return nil
}

// progressEvery is how often op-backfill prints its progress.
const progressEvery = 10 * time.Second

// putPrice is the R2 price of one Class A operation (PUT), in dollars.
const putPrice = 4.50 / 1e6

// runOPBackfill runs "op-backfill": it writes the OP objects of the archived
// history of one namespace (see opbackfill.Backfill). The namespace must
// already be in CBA_OP_NAMESPACES of the running ingester.
func runOPBackfill(args []string, out io.Writer) error {
	defCond, err := envBool("CBA_CONDITIONAL_PUT", true)
	if err != nil {
		return err
	}
	fs := flag.NewFlagSet("op-backfill", flag.ContinueOnError)
	var (
		fsRoot    = fs.String("fs-root", os.Getenv("CBA_FS_ROOT"), "archive directory instead of R2 (env CBA_FS_ROOT)")
		bucket    = fs.String("bucket", os.Getenv("CBA_BUCKET"), "R2 bucket (env CBA_BUCKET); credentials from R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
		condPut   = fs.Bool("conditional-put", defCond, "write objects with PUT If-None-Match: * (env CBA_CONDITIONAL_PUT)")
		namespace = fs.String("namespace", "", "namespace to backfill, 29 bytes hex; must be in op/_namespaces.json")
		from      = fs.Int64("from", 1, "first height")
		to        = fs.Int64("to", 0, "last height; 0 means the manifest's highest_height")
		workers   = fs.Int("workers", opbackfill.DefaultWorkers, "parallel reads and writes")
		dryRun    = fs.Bool("dry-run", false, "read and check everything, write nothing, print the estimate")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	nss, err := oplayer.ParseNamespaces(*namespace)
	if err != nil {
		return fmt.Errorf("-namespace: %w", err)
	}
	if len(nss) != 1 {
		return errors.New("-namespace takes exactly one 29-byte hex namespace")
	}
	if *from < 1 || *to < 0 || (*to > 0 && *from > *to) {
		return fmt.Errorf("-from %d -to %d is not a valid height range", *from, *to)
	}
	if *workers < 1 {
		return fmt.Errorf("-workers %d must be at least 1", *workers)
	}
	st, storeDesc, err := openStore(*fsRoot, *bucket, *condPut)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var (
		mu   sync.Mutex
		last opbackfill.BackfillStats
		done = make(chan struct{})
		wg   sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(progressEvery)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				mu.Lock()
				s := last
				mu.Unlock()
				fmt.Fprintf(out, "progress: blobs %d, written %d, already present %d, skipped %d, withdrawn %d, bytes %d\n",
					s.Blobs, s.Written, s.Present, s.Skipped, s.Withdrawn, s.Bytes)
			}
		}
	}()
	stats, err := opbackfill.Backfill(ctx, st, nss[0], opbackfill.BackfillOptions{
		From: *from, To: *to, Workers: *workers, DryRun: *dryRun,
		Progress: func(s opbackfill.BackfillStats) {
			mu.Lock()
			last = s
			mu.Unlock()
		},
	})
	close(done)
	wg.Wait()

	// An error before the range was resolved leaves nothing to summarise.
	if stats.Heights[1] > 0 {
		mode := ""
		if *dryRun {
			mode = " (dry run, nothing written)"
		}
		fmt.Fprintf(out, "op-backfill %s namespace %s heights %d-%d%s: blobs %d, written %d, already present %d, skipped %d (share version), withdrawn %d, deleted %d (taken down during the run), bytes %d\n",
			storeDesc, hex.EncodeToString(nss[0][:]), stats.Heights[0], stats.Heights[1], mode,
			stats.Blobs, stats.Written, stats.Present, stats.Skipped, stats.Withdrawn, stats.Deleted, stats.Bytes)
		fmt.Fprintf(out, "estimate: PUTs = %d, cost ≈ $%s (PUTs * $4.50/1e6), storage = %d bytes\n",
			stats.Written, strconv.FormatFloat(float64(stats.Written)*putPrice, 'f', -1, 64), stats.Bytes)
	}
	if err != nil {
		return fmt.Errorf("op-backfill %s: %w", storeDesc, err)
	}
	return nil
}

// fibreCaptureOpts are the parsed flags of "fibre-capture".
type fibreCaptureOpts struct {
	grpcAddr        string
	chainID         string
	fsRoot          string
	bucket          string
	condPut         bool
	stateDir        string
	dailyCapBytes   uint64
	downloadWorkers int
	metricsAddr     string
}

func parseFibreCaptureFlags(args []string) (fibreCaptureOpts, error) {
	var o fibreCaptureOpts
	defTLS, err := envBool("CBA_GRPC_TLS", false)
	if err != nil {
		return o, err
	}
	defCond, err := envBool("CBA_CONDITIONAL_PUT", true)
	if err != nil {
		return o, err
	}
	defCap := uint64(50_000_000_000)
	if v := os.Getenv("CBA_FIBRE_DAILY_CAP_BYTES"); v != "" {
		if defCap, err = strconv.ParseUint(v, 10, 64); err != nil {
			return o, fmt.Errorf("CBA_FIBRE_DAILY_CAP_BYTES=%q is not a byte count", v)
		}
	}
	defState := os.Getenv("CBA_FIBRE_STATE_DIR")
	if defState == "" {
		defState = os.Getenv("STATE_DIRECTORY") // set by systemd's StateDirectory=
	}
	fs := flag.NewFlagSet("fibre-capture", flag.ContinueOnError)
	fs.StringVar(&o.grpcAddr, "grpc", os.Getenv("CBA_GRPC"), "consensus node gRPC address host:port, also the fibre client's state address (env CBA_GRPC)")
	grpcTLS := fs.Bool("grpc-tls", defTLS, "not supported: the fibre client dials the app gRPC in plaintext (env CBA_GRPC_TLS)")
	fs.StringVar(&o.chainID, "chain-id", os.Getenv("CBA_CHAIN_ID"), "chain id the node and state.json must hold (env CBA_CHAIN_ID)")
	fs.StringVar(&o.fsRoot, "fs-root", os.Getenv("CBA_FS_ROOT"), "write to this directory instead of R2 (env CBA_FS_ROOT)")
	fs.StringVar(&o.bucket, "bucket", os.Getenv("CBA_BUCKET"), "R2 bucket (env CBA_BUCKET); credentials from R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
	fs.BoolVar(&o.condPut, "conditional-put", defCond, "write immutable objects with PUT If-None-Match: * (env CBA_CONDITIONAL_PUT)")
	fs.StringVar(&o.stateDir, "state-dir", defState, "directory of state.json and the spool (env CBA_FIBRE_STATE_DIR, default $STATE_DIRECTORY)")
	fs.Uint64Var(&o.dailyCapBytes, "daily-cap-bytes", defCap, "bytes downloaded per UTC day at most (env CBA_FIBRE_DAILY_CAP_BYTES)")
	fs.IntVar(&o.downloadWorkers, "download-workers", 2, "parallel downloads")
	fs.StringVar(&o.metricsAddr, "metrics-addr", "127.0.0.1:9470", "serve /metrics here; empty disables")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	switch {
	case fs.NArg() > 0:
		return o, fmt.Errorf("unexpected arguments: %v", fs.Args())
	case o.grpcAddr == "":
		return o, errors.New("-grpc (or CBA_GRPC) is required")
	case *grpcTLS:
		return o, errors.New("-grpc-tls is not supported by fibre-capture: the fibre client dials the app gRPC in plaintext")
	case o.chainID == "":
		return o, errors.New("-chain-id (or CBA_CHAIN_ID) is required")
	case o.stateDir == "":
		return o, errors.New("-state-dir (or CBA_FIBRE_STATE_DIR, or systemd's STATE_DIRECTORY) is required")
	case o.dailyCapBytes == 0:
		return o, errors.New("-daily-cap-bytes must be positive")
	case o.downloadWorkers < 1:
		return o, fmt.Errorf("-download-workers %d must be at least 1", o.downloadWorkers)
	}
	return o, nil
}

// runFibreCapture runs "fibre-capture": it archives Fibre blobs from the
// chain head onward (see fibrecapture.Run) until a signal arrives.
func runFibreCapture(args []string) error {
	o, err := parseFibreCaptureFlags(args)
	if err != nil {
		return err
	}
	dir, err := fibrecapture.OpenDir(o.stateDir)
	if err != nil {
		return err
	}
	st, storeDesc, err := openStore(o.fsRoot, o.bucket, o.condPut)
	if err != nil {
		return err
	}
	src, err := source.NewGRPC(o.grpcAddr, false)
	if err != nil {
		return fmt.Errorf("connect %s: %w", o.grpcAddr, err)
	}
	defer src.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Metrics first: a slow or unreachable node then shows as a live target.
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := fibrecapture.NewMetrics(reg)
	stopMetrics, err := serveMetrics(o.metricsAddr, reg)
	if err != nil {
		return err
	}
	defer stopMetrics()

	slog.Info("fibre-capture", "version", version.Version, "chain_id", o.chainID, "grpc", o.grpcAddr,
		"store", storeDesc, "state_dir", o.stateDir, "daily_cap_bytes", o.dailyCapBytes,
		"download_workers", o.downloadWorkers, "metrics_addr", o.metricsAddr)

	dl, nodeChain, err := fibrecapture.NewClientDownloader(ctx, o.grpcAddr, slog.Default().With("component", "fibre-client"))
	if err != nil {
		if ctx.Err() != nil {
			slog.Info("fibre-capture stopped by signal")
			return nil
		}
		return err
	}
	defer closeDownloader(dl)
	if nodeChain != o.chainID {
		return fmt.Errorf("%w: fibre client reports %q, want %q", fibrecapture.ErrChainMismatch, nodeChain, o.chainID)
	}
	if err := checkHeadChainID(ctx, src, o.chainID); err != nil {
		if ctx.Err() != nil {
			slog.Info("fibre-capture stopped by signal")
			return nil
		}
		return err
	}

	err = fibrecapture.Run(ctx, fibrecapture.Config{
		ChainID: o.chainID, DailyCapBytes: o.dailyCapBytes, DownloadWorkers: o.downloadWorkers,
	}, fibrecapture.Deps{
		Source: src, Downloader: dl, Params: fibrecapture.NewGRPCParams(src.Conn()),
		Store: st, Dir: dir, Metrics: m, Log: slog.Default(),
	})
	if err == nil && ctx.Err() != nil {
		slog.Info("fibre-capture stopped by signal")
	}
	return err
}

// fibreClientStopTimeout bounds the fibre client's stop, so that with the
// capture's own shutdown (cancelled downloads, a save, a flush of up to 20 s)
// the stop path fits the unit's TimeoutStopSec=60.
const fibreClientStopTimeout = 10 * time.Second

// closeDownloader stops the fibre client within fibreClientStopTimeout.
func closeDownloader(dl interface{ Close(context.Context) error }) {
	ctx, cancel := context.WithTimeout(context.Background(), fibreClientStopTimeout)
	defer cancel()
	if err := dl.Close(ctx); err != nil {
		slog.Warn("fibre client stop", "err", err)
	}
}

// checkHeadChainID requires the chain ID in the header of the node's head
// block to be want. It retries source errors until ctx ends; a header that
// does not decode is returned at once.
func checkHeadChainID(ctx context.Context, src source.Source, want string) error {
	for delay := time.Second; ; delay = min(2*delay, 15*time.Second) {
		got, err := headChainID(ctx, src)
		if errors.Is(err, errBadHeader) {
			return err
		}
		if err == nil {
			if got != want {
				return fmt.Errorf("%w: block header has %q, want %q", fibrecapture.ErrChainMismatch, got, want)
			}
			return nil
		}
		slog.Warn("reading the head block header failed; retrying", "err", err, "in", delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// errBadHeader marks a head block header that does not decode; retrying
// would read the same bytes.
var errBadHeader = errors.New("undecodable block header")

func headChainID(ctx context.Context, src source.Source) (string, error) {
	h, err := src.Head(ctx)
	if err != nil {
		return "", err
	}
	b, err := src.Block(ctx, h)
	if err != nil {
		return "", err
	}
	var hdr cmtproto.Header
	if err := hdr.Unmarshal(b.HeaderProto); err != nil {
		return "", fmt.Errorf("block %d: %w: %w", h, errBadHeader, err)
	}
	return hdr.ChainID, nil
}

// serveMetrics serves reg on addr until the returned stop is called. An
// empty addr serves nothing.
func serveMetrics(addr string, reg *prometheus.Registry) (stop func(), err error) {
	if addr == "" {
		return func() {}, nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics listen: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", "err", err)
		}
	}()
	return func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}, nil
}

// affectedSegments returns every stored segment whose range covers height,
// catalogued or orphaned.
func affectedSegments(ctx context.Context, st store.Store, height int64) ([]string, error) {
	keys, err := st.List(ctx, "segments/")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, k := range keys {
		if first, last, ok := archive.ParseSegmentKey(k); ok && first <= height && height <= last {
			out = append(out, k)
		}
	}
	return out, nil
}

// openStore picks the FS store if fsRoot is set, else R2 from the environment.
// The returned description never contains credentials.
func openStore(fsRoot, bucket string, conditionalPut bool) (store.Store, string, error) {
	if fsRoot != "" {
		return store.NewFS(fsRoot), "fs:" + fsRoot, nil
	}
	vals := map[string]string{
		"R2_ACCOUNT_ID":        os.Getenv("R2_ACCOUNT_ID"),
		"R2_ACCESS_KEY_ID":     os.Getenv("R2_ACCESS_KEY_ID"),
		"R2_SECRET_ACCESS_KEY": os.Getenv("R2_SECRET_ACCESS_KEY"),
		"-bucket/CBA_BUCKET":   bucket,
	}
	var missing []string
	for _, k := range []string{"R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "-bucket/CBA_BUCKET"} {
		if vals[k] == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return nil, "", fmt.Errorf("no store configured: set -fs-root (CBA_FS_ROOT), or R2 config; missing: %s",
			strings.Join(missing, ", "))
	}
	st, err := store.NewR2(vals["R2_ACCOUNT_ID"], vals["R2_ACCESS_KEY_ID"], vals["R2_SECRET_ACCESS_KEY"], bucket,
		store.Options{UseConditionalPut: conditionalPut})
	if err != nil {
		return nil, "", err
	}
	return st, "r2:" + bucket, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("%s=%q is not a non-negative duration", key, v)
	}
	return d, nil
}

func envBool(key string, def bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s=%q is not a boolean", key, v)
	}
	return b, nil
}

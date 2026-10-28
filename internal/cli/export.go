package cli

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/noders-team/celestia-blob-archive/internal/nsindex"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
)

// IndexLine is one line of index.jsonl.
type IndexLine struct {
	Height       int64  `json:"height"`
	Namespace    string `json:"namespace"`  // base64
	Commitment   string `json:"commitment"` // base64
	ShareVersion uint32 `json:"share_version"`
	Signer       string `json:"signer"` // base64
	File         string `json:"file"`
}

// ExportOptions selects what Export writes.
type ExportOptions struct {
	Namespace []byte // 29 bytes
	From, To  int64  // inclusive
	Out       string
	Force     bool
}

const indexFile = "index.jsonl"

// Export writes the blobs of one namespace in [From, To] to Out and returns
// how many files it wrote. Blobs listed in tombstones.json are skipped. A
// scrubbed blob not listed there makes Export reload the file once (the
// takedown may have landed meanwhile); if it is still unlisted, Export fails.
func Export(ctx context.Context, st store.Store, o ExportOptions) (int, error) {
	if len(o.Namespace) != 29 {
		return 0, fmt.Errorf("namespace must be 29 bytes, got %d", len(o.Namespace))
	}
	if o.From > o.To {
		return 0, fmt.Errorf("--from %d is after --to %d", o.From, o.To)
	}
	nsHex := hex.EncodeToString(o.Namespace)

	skip, err := tombstone.Load(ctx, st)
	if err != nil {
		return 0, err
	}
	indexed, err := indexedHeight(ctx, st)
	if err != nil {
		return 0, err
	}
	if o.To > indexed {
		return 0, fmt.Errorf("namespace index covers heights up to %d; --to %d is above it (the index follows the contiguous archive)", indexed, o.To)
	}
	entries, err := selectEntries(ctx, st, nsHex, indexed, o.From, o.To)
	if err != nil {
		return 0, err
	}

	if err := os.MkdirAll(o.Out, 0o755); err != nil {
		return 0, fmt.Errorf("create %s: %w", o.Out, err)
	}
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	if o.Force {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}
	idx, err := os.OpenFile(filepath.Join(o.Out, indexFile), flags, 0o644)
	if err != nil {
		return 0, openErr(err, indexFile)
	}
	defer idx.Close()
	enc := json.NewEncoder(idx)

	written := map[string]bool{}
	n := 0
	reloaded := false
	for _, e := range entries {
		c, err := hex.DecodeString(e.Commitment)
		if err != nil {
			return n, fmt.Errorf("namespace index entry at height %d: bad commitment %q", e.Height, e.Commitment)
		}
		if skip.Has(e.Height, o.Namespace, c) {
			continue
		}
		rec, withdrawn, err := readBlob(ctx, st, o.Namespace, e)
		if err != nil {
			return n, err
		}
		if withdrawn {
			// Scrubbed after tombstones.json was loaded: reload it once.
			if !reloaded {
				if skip, err = tombstone.Load(ctx, st); err != nil {
					return n, err
				}
				reloaded = true
			}
			if !skip.Has(e.Height, o.Namespace, c) {
				return n, fmt.Errorf("blob at height %d commitment %s (%s offset %d): archive holds an unlisted tombstone (a takedown may have landed during the export; run the export again)", e.Height, e.Commitment, e.SegmentKey, e.Offset)
			}
			continue
		}
		name := fmt.Sprintf("%d-%s.bin", e.Height, e.Commitment)
		if written[name] {
			continue // identical blob twice in one block
		}
		f, err := os.OpenFile(filepath.Join(o.Out, name), flags, 0o644)
		if err != nil {
			return n, openErr(err, name)
		}
		_, werr := f.Write(rec.Data)
		if cerr := f.Close(); werr == nil {
			werr = cerr
		}
		if werr != nil {
			return n, fmt.Errorf("write %s: %w", name, werr)
		}
		written[name] = true
		if err := enc.Encode(IndexLine{
			Height:       e.Height,
			Namespace:    base64.StdEncoding.EncodeToString(rec.Namespace),
			Commitment:   base64.StdEncoding.EncodeToString(rec.Commitment),
			ShareVersion: rec.ShareVersion,
			Signer:       base64.StdEncoding.EncodeToString(rec.Signer),
			File:         name,
		}); err != nil {
			return n, fmt.Errorf("write %s: %w", indexFile, err)
		}
		n++
	}
	if err := idx.Close(); err != nil {
		return n, fmt.Errorf("close %s: %w", indexFile, err)
	}
	return n, nil
}

func openErr(err error, name string) error {
	if errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("%s already exists (use --force to overwrite)", name)
	}
	return fmt.Errorf("create %s: %w", name, err)
}

// indexedHeight returns the last height whose blobs are in the namespace
// index. A missing ns/_progress.json means nothing is indexed yet.
func indexedHeight(ctx context.Context, st store.Store) (int64, error) {
	b, err := st.Get(ctx, nsindex.ProgressKey)
	if errors.Is(err, store.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read namespace index progress: %w", err)
	}
	var p nsindex.Progress
	if err := json.Unmarshal(b, &p); err != nil {
		return 0, fmt.Errorf("decode namespace index progress: %w", err)
	}
	return p.LastHeight, nil
}

// selectEntries reads the namespace index (honouring head.json as the
// authority on how many entries are valid) and returns entries in [from, to].
// indexed is the index progress, quoted when the namespace is absent.
func selectEntries(ctx context.Context, st store.Store, nsHex string, indexed, from, to int64) ([]nsindex.Entry, error) {
	all, ok, err := nsindex.ReadEntries(ctx, st, nsHex)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("namespace %s is not in the namespace index (index covers heights up to %d): %w", nsHex, indexed, store.ErrNotFound)
	}
	var out []nsindex.Entry
	for _, e := range all {
		if e.Height >= from && e.Height <= to {
			out = append(out, e)
		}
	}
	return out, nil
}

// readBlob fetches one blob record with a single range request and checks it
// is the one the index entry promised. If the blob was scrubbed it returns a
// nil record and withdrawn true.
func readBlob(ctx context.Context, st store.Store, ns []byte, e nsindex.Entry) (b *archivev1.BlobRecord, withdrawn bool, err error) {
	buf, err := st.GetRange(ctx, e.SegmentKey, int64(e.Offset), int64(e.Length))
	if err != nil {
		return nil, false, fmt.Errorf("read blob at height %d: %w", e.Height, err)
	}
	rec, err := segment.ReadAt(buf, 0, e.Length)
	if err != nil {
		return nil, false, fmt.Errorf("decode blob at height %d: %w", e.Height, err)
	}
	var (
		height uint64
		rns, c []byte
	)
	b, t := rec.GetBlob(), rec.GetTombstone()
	switch {
	case b != nil:
		height, rns, c = b.Height, b.Namespace, b.Commitment
	case t != nil:
		height, rns, c = t.Height, t.Namespace, t.Commitment
	}
	if (b == nil && t == nil) || int64(height) != e.Height || hex.EncodeToString(c) != e.Commitment || !bytes.Equal(rns, ns) {
		return nil, false, fmt.Errorf("blob at height %d (%s offset %d): record does not match namespace index entry", e.Height, e.SegmentKey, e.Offset)
	}
	if t != nil {
		return nil, true, nil
	}
	b, err = checkCommitment(b, e)
	return b, false, err
}

// checkCommitment requires b's data to hash to the commitment e promised.
func checkCommitment(b *archivev1.BlobRecord, e nsindex.Entry) (*archivev1.BlobRecord, error) {
	c, err := process.BlobCommitment(b.Namespace, b.Data, b.ShareVersion, b.Signer)
	if err != nil {
		return nil, fmt.Errorf("blob at height %d: %w", e.Height, err)
	}
	if hex.EncodeToString(c) != e.Commitment {
		return nil, fmt.Errorf("blob at height %d: data does not match commitment %s", e.Height, e.Commitment)
	}
	return b, nil
}

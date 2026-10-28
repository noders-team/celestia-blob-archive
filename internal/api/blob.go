package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/celestiaorg/celestia-app/v10/pkg/proof"
	"github.com/celestiaorg/nmt"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/nsrules"
	"github.com/noders-team/celestia-blob-archive/internal/verify"
)

const (
	namespaceSize  = 29
	commitmentSize = 32
)

func parseHeight(raw json.RawMessage) (uint64, *rpcError) {
	var h uint64
	if bytes.Equal(raw, nullID) || json.Unmarshal(raw, &h) != nil {
		return 0, errParams("height must be a positive integer")
	}
	if h == 0 {
		return 0, errHeight0
	}
	return h, nil
}

// checkNamespace requires ns to be a 29-byte namespace that can hold blobs.
func checkNamespace(ns []byte) *rpcError {
	if len(ns) != namespaceSize {
		return errParams("namespace must be %d bytes of base64, got %d bytes", namespaceSize, len(ns))
	}
	if nsrules.CheckBlobNamespace(ns) != nil {
		return errParams("namespace %x is reserved or unusable for blobs", ns)
	}
	return nil
}

func parseNamespace(raw json.RawMessage) ([]byte, *rpcError) {
	var ns []byte
	if err := json.Unmarshal(raw, &ns); err != nil {
		return nil, errParams("namespace must be base64")
	}
	return ns, checkNamespace(ns)
}

func parseCommitment(raw json.RawMessage) ([]byte, *rpcError) {
	var cm []byte
	if err := json.Unmarshal(raw, &cm); err != nil {
		return nil, errParams("commitment must be base64")
	}
	if len(cm) != commitmentSize {
		return nil, errParams("commitment must be %d bytes of base64, got %d bytes", commitmentSize, len(cm))
	}
	return cm, nil
}

// blobParams parses (height, namespace, commitment).
func blobParams(height, ns, cm json.RawMessage) (uint64, []byte, []byte, *rpcError) {
	h, err := parseHeight(height)
	if err != nil {
		return 0, nil, nil, err
	}
	n, err := parseNamespace(ns)
	if err != nil {
		return 0, nil, nil, err
	}
	c, err := parseCommitment(cm)
	if err != nil {
		return 0, nil, nil, err
	}
	return h, n, c, nil
}

// archived maps h to an archive height. A height above the manifest's
// highest_height is answered as in the future before any upstream read, so
// far-future heights cannot make the API fetch a new directory bucket each.
// Right after the ingester commits, a height may be "future" for up to one
// manifest poll; ev-node retries those.
func (s *Server) archived(h uint64) (int64, *rpcError) {
	highest, _ := s.state().heights()
	if h > math.MaxInt64 || int64(h) > highest {
		return 0, errFuture(h, highest)
	}
	return int64(h), nil
}

// reserveFactor is how many times its raw segment bytes a call reserves of
// MaxInflightBytes: while it is held the call keeps the decoded records,
// the base64 JSON result and the encoded reply, roughly three copies.
const reserveFactor = 3

// reserver returns an archiveread reserve hook that holds reserveFactor ×
// the bytes to be read of MaxInflightBytes, waiting for them under ctx, and
// the func that releases them (a no-op if nothing was reserved). serveRPC
// releases only after the HTTP reply is written.
func (s *Server) reserver(ctx context.Context) (reserve reserveFunc, release func()) {
	var held int64
	release = func() {
		if held > 0 {
			s.sem.Release(held)
			held = 0
		}
	}
	reserve = func(n int64) (func(), error) {
		// A call larger than the whole budget still runs, alone.
		if n > s.cfg.MaxInflightBytes/reserveFactor {
			n = s.cfg.MaxInflightBytes
		} else {
			n *= reserveFactor
		}
		if err := s.sem.Acquire(ctx, n); err != nil {
			return nil, err
		}
		held = n
		return release, nil
	}
	return reserve, release
}

// readErr maps an archiveread error. Only ErrBlobNotFound, an archived
// height without the blob, becomes "blob: not found".
func (s *Server) readErr(height int64, ns, cm []byte, err error) *rpcError {
	switch {
	case errors.Is(err, archiveread.ErrCorrupt):
		return s.verifyFailed(height, ns, cm, err)
	case errors.Is(err, archiveread.ErrNotArchived):
		highest, contiguous := s.state().heights()
		if height > highest {
			return errFuture(uint64(height), highest)
		}
		return errGap(uint64(height), contiguous, highest)
	case errors.Is(err, archiveread.ErrBlobNotFound):
		return errNotFound
	}
	s.log.Warn("archive read failed", "height", height, "err", err)
	return errUpstream
}

// verifyFailed counts and logs a stored record that must not be served.
func (s *Server) verifyFailed(height int64, ns, cm []byte, err error) *rpcError {
	s.m.verifyFailed()
	s.log.Error("stored record failed verification", "height", height,
		"namespace", fmt.Sprintf("%x", ns), "commitment", fmt.Sprintf("%x", cm), "err", err)
	return errVerify(height)
}

// lookup returns the verified record of the blob (ns, cm) at height h with
// its height record. Its segment bytes are reserved with reserve.
func (s *Server) lookup(ctx context.Context, h uint64, ns, cm []byte,
	reserve reserveFunc) (*archivev1.BlobRecord, *archivev1.HeightRecord, *rpcError) {
	height, rerr := s.archived(h)
	if rerr != nil {
		return nil, nil, rerr
	}
	if s.state().tombstones().Has(height, ns, cm) {
		return nil, nil, errWithdrawn
	}
	hr, rec, err := archiveread.ReadBlobReserve(ctx, s.st, height, ns, cm, reserve)
	if err != nil {
		return nil, nil, s.readErr(height, ns, cm, err)
	}
	if rec.GetTombstone() != nil {
		// Scrubbed but not in the loaded set: the set is stale.
		s.state().reloadTombstones(ctx)
		return nil, nil, errWithdrawn
	}
	b := rec.GetBlob()
	if err := verify.CheckRecord(b, hr); err != nil {
		return nil, nil, s.verifyFailed(height, ns, cm, err)
	}
	return b, hr, nil
}

func encode(v any) (json.RawMessage, *rpcError) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, errUpstream
	}
	return b, nil
}

func (s *Server) blobGet(ctx context.Context, p []json.RawMessage, reserve reserveFunc) (json.RawMessage, *rpcError) {
	h, ns, cm, rerr := blobParams(p[0], p[1], p[2])
	if rerr != nil {
		return nil, rerr
	}
	rec, hr, rerr := s.lookup(ctx, h, ns, cm, reserve)
	if rerr != nil {
		return nil, rerr
	}
	b, err := BlobFrom(rec, hr.SquareSize)
	if err != nil {
		return nil, s.verifyFailed(int64(h), ns, cm, err)
	}
	return encode(b)
}

// proofOf returns the row proofs of a record CheckRecord accepted.
func (s *Server) proofOf(rec *archivev1.BlobRecord) ([]*nmt.Proof, error) {
	var sp proof.ShareProof
	if err := sp.Unmarshal(rec.ShareProof); err != nil {
		return nil, fmt.Errorf("decode share proof: %w", err)
	}
	return ProofFrom(&sp)
}

func (s *Server) blobGetProof(ctx context.Context, p []json.RawMessage, reserve reserveFunc) (json.RawMessage, *rpcError) {
	h, ns, cm, rerr := blobParams(p[0], p[1], p[2])
	if rerr != nil {
		return nil, rerr
	}
	rec, _, rerr := s.lookup(ctx, h, ns, cm, reserve)
	if rerr != nil {
		return nil, rerr
	}
	pr, err := s.proofOf(rec)
	if err != nil {
		return nil, s.verifyFailed(int64(h), ns, cm, err)
	}
	return encode(pr)
}

// blobIncluded answers false for an archived height without the blob and
// compares the given proof with the stored one otherwise; a height that is
// not archived and a withdrawn blob are errors.
func (s *Server) blobIncluded(ctx context.Context, p []json.RawMessage, reserve reserveFunc) (json.RawMessage, *rpcError) {
	h, ns, cm, rerr := blobParams(p[0], p[1], p[3])
	if rerr != nil {
		return nil, rerr
	}
	var given []*nmt.Proof
	if err := json.Unmarshal(p[2], &given); err != nil {
		return nil, errParams("proof must be a list of NMT proofs")
	}
	rec, _, rerr := s.lookup(ctx, h, ns, cm, reserve)
	if rerr == errNotFound {
		return encode(false)
	}
	if rerr != nil {
		return nil, rerr
	}
	pr, err := s.proofOf(rec)
	if err != nil {
		return nil, s.verifyFailed(int64(h), ns, cm, err)
	}
	return encode(ProofsEqual(pr, given))
}

// blobGetAll returns the blobs of the namespaces at a height, concatenated
// in request order, or null if there are none. A withdrawn blob in any of
// the namespaces fails the whole call, so a client never gets a list with a
// silent hole.
func (s *Server) blobGetAll(ctx context.Context, p []json.RawMessage, reserve reserveFunc) (json.RawMessage, *rpcError) {
	h, rerr := parseHeight(p[0])
	if rerr != nil {
		return nil, rerr
	}
	var nss [][]byte
	if err := json.Unmarshal(p[1], &nss); err != nil {
		return nil, errParams("namespaces must be a list of base64 namespaces")
	}
	switch {
	case len(nss) == 0:
		return nil, errParams("no namespaces")
	case len(nss) > s.cfg.MaxNamespaces:
		return nil, errParams("more than %d namespaces", s.cfg.MaxNamespaces)
	}
	keys := make([][namespaceSize]byte, len(nss))
	seen := make(map[[namespaceSize]byte]bool, len(nss))
	for i, ns := range nss {
		if rerr := checkNamespace(ns); rerr != nil {
			return nil, rerr
		}
		keys[i] = [namespaceSize]byte(ns)
		if seen[keys[i]] {
			return nil, errParams("duplicate namespace %x", ns)
		}
		seen[keys[i]] = true
	}
	height, rerr := s.archived(h)
	if rerr != nil {
		return nil, rerr
	}

	// The records' bytes count against MaxInflightBytes from before they
	// are read until the reply holding them is written.
	hr, recs, err := archiveread.ReadNamespaces(ctx, s.st, height, keys, reserve)
	if err != nil {
		return nil, s.readErr(height, nil, nil, err)
	}

	set := s.state().tombstones()
	for _, rs := range recs {
		for _, rec := range rs {
			if t := rec.GetTombstone(); t != nil {
				if !set.Has(height, t.Namespace, t.Commitment) {
					s.state().reloadTombstones(ctx)
				}
				return nil, errWithdrawn
			}
			if b := rec.GetBlob(); set.Has(height, b.GetNamespace(), b.GetCommitment()) {
				return nil, errWithdrawn
			}
		}
	}
	var out []Blob // nil encodes as null, as celestia-node sends "none"
	for _, rs := range recs {
		for _, rec := range rs {
			b := rec.GetBlob()
			if err := verify.CheckRecord(b, hr); err != nil {
				return nil, s.verifyFailed(height, b.GetNamespace(), b.GetCommitment(), err)
			}
			wb, err := BlobFrom(b, hr.SquareSize)
			if err != nil {
				return nil, s.verifyFailed(height, b.GetNamespace(), b.GetCommitment(), err)
			}
			out = append(out, wb)
		}
	}
	return encode(out)
}

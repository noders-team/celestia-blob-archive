// Package verify checks that a blob is in the archive and that nothing about
// it has to be taken on trust: the archived shares decode to exactly the blob,
// the blob's commitment is recomputed, the share proof is validated against
// the stored data root and that data root is compared with a live node's
// header.
package verify

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/celestiaorg/celestia-app/v10/pkg/proof"
	"github.com/celestiaorg/go-square/v4/share"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archiveread"
	"github.com/noders-team/celestia-blob-archive/internal/nsrules"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/source"
	"github.com/noders-team/celestia-blob-archive/internal/square"
	"github.com/noders-team/celestia-blob-archive/internal/store"
)

// One sentinel per failed check.
var (
	// ErrNotInArchive: no blob record with that height, namespace and commitment.
	ErrNotInArchive = errors.New("blob not in archive")
	// ErrSharesMismatch: the proof's shares do not decode to exactly the archived blob.
	ErrSharesMismatch = errors.New("proof shares do not match archived blob")
	// ErrCommitmentMismatch: the commitment recomputed from the data differs.
	ErrCommitmentMismatch = errors.New("commitment mismatch")
	// ErrProofInvalid: the share proof does not validate against the stored data root.
	ErrProofInvalid = errors.New("share proof invalid")
	// ErrDataRootMismatch: the stored data root differs from the node's header.
	ErrDataRootMismatch = errors.New("data root differs from node header")
	// ErrProofLayout: the proof's row/share positions are not the contiguous
	// share range of the blob.
	ErrProofLayout = errors.New("share proof layout invalid")
	// ErrReservedNamespace: the namespace is reserved for the protocol (tx,
	// PayForBlob, padding, parity, ...) and so cannot hold a blob. Without this
	// a proven prefix of the compact tx shares could be passed off as a blob.
	ErrReservedNamespace = nsrules.ErrReserved
	// ErrWithdrawn: the blob was taken down; its record is a TombstoneRecord
	// and its bytes are no longer in the archive.
	ErrWithdrawn = errors.New("blob withdrawn, see tombstones.json")
)

// ErrNotArchived: the height is outside the committed ranges (in a gap, above
// the highest or below the first committed height). Nothing was verified, so
// it is not a failure; errors wrapping it are a notArchivedError.
var ErrNotArchived = errors.New("height is not archived")

// commitmentSize is the byte length of a share commitment.
const commitmentSize = 32

type notArchivedError struct {
	height    int64
	committed []archiveread.Range
	cause     error // archiveread.ErrNotArchived from the directory lookup
}

func (e notArchivedError) Error() string {
	return fmt.Sprintf("height %d is not archived (committed ranges: %s)", e.height, archiveread.JoinRanges(e.committed))
}

func (e notArchivedError) Is(target error) bool { return target == ErrNotArchived }

func (e notArchivedError) Unwrap() error { return e.cause }

// IsFailure reports whether err is a failed verification check (it wraps one
// of the sentinels above), as opposed to a problem running the checks.
func IsFailure(err error) bool {
	for _, s := range []error{
		ErrNotInArchive, ErrSharesMismatch, ErrCommitmentMismatch, ErrProofInvalid,
		ErrDataRootMismatch, ErrProofLayout, ErrReservedNamespace, ErrWithdrawn,
	} {
		if errors.Is(err, s) {
			return true
		}
	}
	return false
}

// HeaderSource provides the header data hash of a block, e.g. a gRPC node.
type HeaderSource interface {
	Block(ctx context.Context, height int64) (*source.Block, error)
}

// Params identifies the blob to verify.
type Params struct {
	Height     int64
	Namespace  []byte
	Commitment []byte
}

// Verify returns nil only if all checks pass (record lookup, reserved
// namespace rejection, shares, commitment, data root vs node, proof layout and
// proof validity); otherwise an error wrapping
// the sentinel of the first failed check. Other errors (network, corrupt
// archive, node unreachable) are not wrapped in a sentinel.
func Verify(ctx context.Context, st store.Store, hs HeaderSource, p Params) error {
	if err := nsrules.CheckBlobNamespace(p.Namespace); err != nil {
		return err
	}
	rec, hr, err := find(ctx, st, p)
	if err != nil {
		return err
	}
	return verifyRecord(ctx, hs, p, rec, hr)
}

// verifyRecord runs the checks after the record lookup on rec, the archived
// record find returned for p, and hr, its height record.
func verifyRecord(ctx context.Context, hs HeaderSource, p Params, rec *archivev1.BlobRecord, hr *archivev1.HeightRecord) error {
	// Checks 2 and 3, then the request's commitment.
	sp, err := checkContent(rec)
	if err != nil {
		return err
	}
	if !bytes.Equal(rec.Commitment, p.Commitment) {
		return fmt.Errorf("%w: record %x, requested %x", ErrCommitmentMismatch, rec.Commitment, p.Commitment)
	}

	// 4. the stored data root is the one in the node's header. This runs
	// before the proof check so a forged stored root is reported as such.
	blk, err := hs.Block(ctx, p.Height)
	if err != nil {
		return fmt.Errorf("fetch header %d: %w", p.Height, err)
	}
	if blk.Height != p.Height {
		return fmt.Errorf("node returned block %d for height %d", blk.Height, p.Height)
	}
	if !bytes.Equal(blk.DataHash, hr.DataHash) {
		return fmt.Errorf("%w: archive %x, node %x", ErrDataRootMismatch, hr.DataHash, blk.DataHash)
	}

	return checkProof(sp, rec, hr)
}

// CheckRecord checks the archived blob record rec against its height record
// hr without a node: rec's namespace may hold blobs, its proof's shares decode
// exactly to it, its commitment is recomputed from the data, and its proof
// covers its share range and validates against hr's data root. It does not
// check hr's data root against the chain; Verify does. A failure wraps the
// sentinel of the first failed check. A nil rec (e.g. GetBlob of a
// TombstoneRecord) is ErrNotInArchive.
func CheckRecord(rec *archivev1.BlobRecord, hr *archivev1.HeightRecord) error {
	if rec == nil {
		return fmt.Errorf("%w: no blob record", ErrNotInArchive)
	}
	sp, err := checkContent(rec)
	if err != nil {
		return err
	}
	return checkProof(sp, rec, hr)
}

// checkContent runs the checks on rec alone: its namespace (repeating the
// request check in Verify, so the record is never trusted on the request's
// word), its proof's shares (2) and its recomputed commitment (3). It returns
// the decoded proof.
func checkContent(rec *archivev1.BlobRecord) (*proof.ShareProof, error) {
	if err := nsrules.CheckBlobNamespace(rec.Namespace); err != nil {
		return nil, err
	}

	// 2. the proof's shares decode exactly to this blob.
	var sp proof.ShareProof
	if err := sp.Unmarshal(rec.ShareProof); err != nil {
		return nil, fmt.Errorf("%w: decode share proof: %v", ErrProofInvalid, err)
	}
	if err := checkShares(&sp, rec); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSharesMismatch, err)
	}

	// 3. the commitment recomputed from the data matches the record.
	got, err := process.BlobCommitment(rec.Namespace, rec.Data, rec.ShareVersion, rec.Signer)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCommitmentMismatch, err)
	}
	if !bytes.Equal(got, rec.Commitment) {
		return nil, fmt.Errorf("%w: recomputed %x, record %x", ErrCommitmentMismatch, got, rec.Commitment)
	}
	return &sp, nil
}

// checkProof requires sp's positions to be rec's contiguous share range and
// sp to validate against hr's data root (5). Validate alone does not bind
// positions. hr's square_size must be the proof's width, so that the blob
// index derived from it is bound to the data root too.
func checkProof(sp *proof.ShareProof, rec *archivev1.BlobRecord, hr *archivev1.HeightRecord) error {
	if err := checkLayout(sp, rec); err != nil {
		return fmt.Errorf("%w: %v", ErrProofLayout, err)
	}
	if w, err := square.WidthFromProof(sp); err != nil || w != hr.GetSquareSize() {
		return fmt.Errorf("%w: height record square size %d, proof width %d", ErrProofLayout, hr.GetSquareSize(), w)
	}
	if err := sp.Validate(hr.GetDataHash()); err != nil {
		return fmt.Errorf("%w: %v", ErrProofInvalid, err)
	}
	return nil
}

// find loads the height record and the blob record matching p (check 1)
// through the height directory, with range requests only. The catalog is
// read only to describe the committed ranges of a height that is not
// archived. A height that is not archived is reported as such before any
// other mismatch, even if the commitment is malformed.
func find(ctx context.Context, st store.Store, p Params) (*archivev1.BlobRecord, *archivev1.HeightRecord, error) {
	var (
		hr  *archivev1.HeightRecord
		rec *archivev1.Record
		err error
	)
	if len(p.Commitment) != commitmentSize {
		// No blob can match; only the directory says which error applies.
		if _, err = archiveread.Locate(ctx, st, p.Height); err == nil {
			return nil, nil, fmt.Errorf("%w: commitment is %d bytes, want %d", ErrNotInArchive, len(p.Commitment), commitmentSize)
		}
	} else {
		hr, rec, err = archiveread.ReadBlob(ctx, st, p.Height, p.Namespace, p.Commitment)
	}
	switch {
	case errors.Is(err, archiveread.ErrNotArchived):
		refs, cerr := archiveread.LoadCatalog(ctx, st)
		if cerr != nil {
			return nil, nil, fmt.Errorf("height %d not archived; load catalog: %w", p.Height, cerr)
		}
		committed, _ := archiveread.Ranges(refs)
		return nil, nil, notArchivedError{p.Height, committed, err}
	case errors.Is(err, archiveread.ErrBlobNotFound):
		return nil, nil, fmt.Errorf("%w: height %d has no blob with that namespace and commitment", ErrNotInArchive, p.Height)
	case err != nil:
		return nil, nil, fmt.Errorf("read height %d: %w", p.Height, err)
	}
	b, err := matchRecord(p, rec)
	if err != nil {
		return nil, nil, err
	}
	return b, hr, nil
}

// matchRecord returns the BlobRecord in rec if it is the blob p asks for, or
// ErrWithdrawn if rec is that blob's TombstoneRecord. The reader already
// matched the row; this matches the decoded record too, so a record is never
// accepted on its row's word.
func matchRecord(p Params, rec *archivev1.Record) (*archivev1.BlobRecord, error) {
	var (
		height uint64
		ns, cm []byte
	)
	b, t := rec.GetBlob(), rec.GetTombstone()
	switch {
	case b != nil:
		height, ns, cm = b.Height, b.Namespace, b.Commitment
	case t != nil:
		height, ns, cm = t.Height, t.Namespace, t.Commitment
	default:
		return nil, fmt.Errorf("%w: height %d: archived record is neither a blob nor a tombstone", ErrNotInArchive, p.Height)
	}
	if int64(height) != p.Height || !bytes.Equal(ns, p.Namespace) || !bytes.Equal(cm, p.Commitment) {
		return nil, fmt.Errorf("%w: height %d: archived record is height %d, namespace %x, commitment %x", ErrNotInArchive, p.Height, height, ns, cm)
	}
	if t != nil {
		return nil, fmt.Errorf("height %d, namespace %x, commitment %x: %w", p.Height, ns, cm, ErrWithdrawn)
	}
	return b, nil
}

// checkShares requires sp.Data to be the shares of exactly one blob equal to rec.
func checkShares(sp *proof.ShareProof, rec *archivev1.BlobRecord) error {
	ns, err := share.NewNamespaceFromBytes(rec.Namespace)
	if err != nil {
		return fmt.Errorf("record namespace: %w", err)
	}
	if uint32(ns.Version()) != sp.NamespaceVersion || !bytes.Equal(ns.ID(), sp.NamespaceId) {
		return fmt.Errorf("proof namespace %d/%x, record %d/%x", sp.NamespaceVersion, sp.NamespaceId, ns.Version(), ns.ID())
	}
	if len(sp.Data) == 0 {
		return errors.New("proof has no shares")
	}
	shares := make([]share.Share, len(sp.Data))
	for i, raw := range sp.Data {
		s, err := share.NewShare(raw)
		if err != nil {
			return fmt.Errorf("share %d: %w", i, err)
		}
		if s.IsPadding() {
			return fmt.Errorf("share %d is padding", i)
		}
		if !s.Namespace().Equals(ns) {
			return fmt.Errorf("share %d has namespace %x, want %x", i, s.Namespace().Bytes(), ns.Bytes())
		}
		if (i == 0) != s.IsSequenceStart() {
			return fmt.Errorf("share %d: unexpected sequence-start flag %t", i, s.IsSequenceStart())
		}
		shares[i] = s
	}
	blobs, err := share.ParseBlobs(shares)
	if err != nil {
		return fmt.Errorf("parse shares: %w", err)
	}
	if len(blobs) != 1 {
		return fmt.Errorf("shares hold %d blobs, want 1", len(blobs))
	}
	b := blobs[0]
	switch {
	case uint32(b.ShareVersion()) != rec.ShareVersion:
		return fmt.Errorf("share version %d, record %d", b.ShareVersion(), rec.ShareVersion)
	case !bytes.Equal(b.Signer(), rec.Signer):
		return fmt.Errorf("signer %x, record %x", b.Signer(), rec.Signer)
	case !bytes.Equal(b.Data(), rec.Data):
		return errors.New("blob data in shares differs from record data")
	}
	return nil
}

// checkLayout derives the share positions from the proof and requires them to
// be one contiguous run of ODS shares equal to the record's
// [share_start, share_end). ShareProof.Validate only checks that each row's
// range is under some row root under the data root; without this check shares
// could be spliced from elsewhere in the namespace, reordered or repeated.
func checkLayout(sp *proof.ShareProof, rec *archivev1.BlobRecord) error {
	width, err := square.WidthFromProof(sp)
	if err != nil {
		return err
	}
	rp := sp.RowProof
	rows := len(rp.Proofs)
	if len(sp.ShareProofs) != rows || len(rp.RowRoots) != rows {
		return fmt.Errorf("%d row proofs, %d row roots, %d share proofs", rows, len(rp.RowRoots), len(sp.ShareProofs))
	}
	w, total := int64(width), rp.Proofs[0].Total
	start := int64(rp.StartRow)
	if int64(rp.EndRow) != start+int64(rows)-1 || int64(rp.EndRow) >= w {
		return fmt.Errorf("rows [%d,%d] with %d proofs in a square of width %d", rp.StartRow, rp.EndRow, rows, w)
	}
	var shares int64
	for i, rpf := range rp.Proofs {
		if rpf == nil || rpf.Total != total || rpf.Index != start+int64(i) {
			return fmt.Errorf("row proof %d: not row %d of %d", i, start+int64(i), total)
		}
		np := sp.ShareProofs[i]
		if np == nil {
			return fmt.Errorf("row %d: nil share proof", i)
		}
		s, e := int64(np.Start), int64(np.End)
		if s < 0 || e <= s || e > w {
			return fmt.Errorf("row %d: share range [%d,%d) outside width %d", i, s, e, w)
		}
		if i > 0 && s != 0 {
			return fmt.Errorf("row %d: range starts at %d, not 0", i, s)
		}
		if i < rows-1 && e != w {
			return fmt.Errorf("row %d: range ends at %d, not the row end %d", i, e, w)
		}
		shares += e - s
	}
	if int64(len(sp.Data)) != shares {
		return fmt.Errorf("%d shares for ranges totalling %d", len(sp.Data), shares)
	}
	if want := share.SparseSharesNeeded(uint32(len(rec.Data)), len(rec.Signer) > 0); int64(want) != shares {
		return fmt.Errorf("blob needs %d shares, proof has %d", want, shares)
	}
	first := start*w + int64(sp.ShareProofs[0].Start)
	last := int64(rp.EndRow)*w + int64(sp.ShareProofs[rows-1].End)
	if first != int64(rec.ShareStart) || last != int64(rec.ShareEnd) {
		return fmt.Errorf("proof covers shares [%d,%d), record says [%d,%d)", first, last, rec.ShareStart, rec.ShareEnd)
	}
	return nil
}

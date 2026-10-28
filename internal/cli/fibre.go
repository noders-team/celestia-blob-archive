package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"

	"github.com/celestiaorg/celestia-app/v10/fibre"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	cosmostx "github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/spf13/cobra"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/client"
	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
	"github.com/noders-team/celestia-blob-archive/internal/fibrecapture"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
	"github.com/noders-team/celestia-blob-archive/internal/verify"
)

// ErrFibreCheck: a check of cba fibre verify failed (errors wrapping it map to
// ExitVerifyFailed).
var ErrFibreCheck = errors.New("fibre check failed")

// timeoutTypeURL is the SDK type URL of MsgPaymentPromiseTimeout.
const timeoutTypeURL = "/celestia.fibre.v1.MsgPaymentPromiseTimeout"

// Result is what VerifyFibre found for one Fibre message.
type Result struct {
	Status   string // data_status name
	BlobID   string // hex
	DataSize uint64 // payload bytes checked; 0 unless archived
	// PromiseOK: blob_id and the escrow owner's signature were checked. False
	// only for an unrecoverable record whose promise cannot be decoded or
	// validated (the reason it is unrecoverable).
	PromiseOK bool
	Reason    string // record error of an unrecoverable record
}

func fibreFail(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrFibreCheck, fmt.Sprintf(format, args...))
}

// VerifyFibre checks the Fibre record of message msgIndex of tx txIndex at
// height against the archive alone: the record's blob_id against the promise
// in its tx, the promise's escrow owner signature, and for an archived blob
// the payload against the commitment, blob_size and data_sha256. A failed
// check wraps ErrFibreCheck; a blob listed in tombstones.json wraps
// verify.ErrWithdrawn. Inclusion and validator quorum are not checked.
func VerifyFibre(ctx context.Context, st store.Store, height int64, txIndex, msgIndex uint32) (Result, error) {
	m, err := fibrearchive.LoadManifest(ctx, st)
	if err != nil {
		return Result{}, err
	}
	if m == nil {
		return Result{}, fmt.Errorf("archive has no %s", fibrearchive.ManifestKey)
	}
	if m.FibreFormat != fibrearchive.FibreFormat {
		return Result{}, fmt.Errorf("%s: fibre_format %d, this cba reads %d", fibrearchive.ManifestKey, m.FibreFormat, fibrearchive.FibreFormat)
	}
	if height < m.FirstHeight || height > m.ScannedUntil {
		return Result{}, fmt.Errorf("height %d is not scanned for Fibre messages (scanned %d-%d)", height, m.FirstHeight, m.ScannedUntil)
	}

	key := fibrearchive.RecordKey(height, txIndex, msgIndex)
	raw, err := st.Get(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return Result{}, fibreFail("no Fibre record at height %d tx %d msg %d (%s)", height, txIndex, msgIndex, key)
	}
	if err != nil {
		return Result{}, fmt.Errorf("read %s: %w", key, err)
	}
	r, err := fibrearchive.DecodeRecord(raw)
	if err != nil {
		return Result{}, fibreFail("%s: %v", key, err)
	}
	if r.Height != uint64(height) || r.TxIndex != txIndex || r.MsgIndex != msgIndex {
		return Result{}, fibreFail("%s: record of height %d tx %d msg %d stored under it", key, r.Height, r.TxIndex, r.MsgIndex)
	}
	status := fibrearchive.Status(r.DataStatus)
	res := Result{Status: status.String(), BlobID: hex.EncodeToString(r.BlobId)}
	if status == fibrearchive.StatusUnrecoverable {
		res.Reason = r.Error
	}

	p, err := recordPromise(r)
	if err == nil {
		err = p.Validate()
		if err != nil {
			err = fibreFail("%s: payment promise: %v", key, err)
		}
	}
	if err != nil {
		// Capture marks a message whose promise is malformed unrecoverable
		// without downloading; there is nothing more to check.
		if status == fibrearchive.StatusUnrecoverable {
			return res, nil
		}
		return res, err
	}
	if p.BlobVersion > math.MaxUint8 {
		if status == fibrearchive.StatusUnrecoverable {
			return res, nil
		}
		return res, fibreFail("%s: blob version %d does not fit a blob ID", key, p.BlobVersion)
	}
	if want := fibre.NewBlobID(uint8(p.BlobVersion), p.Commitment); !bytes.Equal(want, r.BlobId) {
		return res, fibreFail("%s: blob_id %x, promise in tx gives %x", key, r.BlobId, []byte(want))
	}
	res.PromiseOK = true
	var id [fibrearchive.BlobIDSize]byte
	copy(id[:], r.BlobId)

	ts, err := tombstone.Load(ctx, st)
	if err != nil {
		return res, err
	}
	if ts.HasFibre(id[:]) {
		return res, fmt.Errorf("fibre blob %x: %w", id, verify.ErrWithdrawn)
	}
	if status != fibrearchive.StatusArchived {
		return res, nil
	}

	dkey := fibrearchive.DataKey(id)
	data, err := store.GetLimit(ctx, st, dkey, int64(p.UploadSize))
	if errors.Is(err, store.ErrNotFound) {
		// A takedown deletes the data after listing the ID: re-read the
		// list once before calling the data missing.
		if ts, err = tombstone.Load(ctx, st); err != nil {
			return res, err
		}
		if ts.HasFibre(id[:]) {
			return res, fmt.Errorf("fibre blob %x: %w", id, verify.ErrWithdrawn)
		}
		return res, fibreFail("%s: record is archived but %s is missing", key, dkey)
	}
	if errors.Is(err, store.ErrTooLarge) {
		return res, fibreFail("%s: %v (promise blob_size %d)", dkey, err, p.UploadSize)
	}
	if err != nil {
		return res, fmt.Errorf("read %s: %w", dkey, err)
	}
	if err := fibrecapture.VerifyPayload(data, id, p.UploadSize); err != nil {
		if errors.Is(err, fibrecapture.ErrMismatch) {
			return res, fibreFail("%s does not match the paid commitment: %v", dkey, err)
		}
		return res, fmt.Errorf("verify %s: %w", dkey, err)
	}
	if uint64(len(data)) != r.DataSize {
		return res, fibreFail("%s: %s is %d bytes, record data_size %d", key, dkey, len(data), r.DataSize)
	}
	if sum := sha256.Sum256(data); !bytes.Equal(sum[:], r.DataSha256) {
		return res, fibreFail("%s: sha256 of %s is %x, record data_sha256 %x", key, dkey, sum, r.DataSha256)
	}
	res.DataSize = uint64(len(data))
	return res, nil
}

// recordPromise decodes r.Tx the way the SDK does and returns the payment
// promise of the message at r.MsgIndex, which must be of r's kind.
func recordPromise(r *archivev1.FibreRecord) (*fibre.PaymentPromise, error) {
	at := fmt.Sprintf("record at height %d tx %d msg %d", r.Height, r.TxIndex, r.MsgIndex)
	var txRaw cosmostx.TxRaw
	if err := txRaw.Unmarshal(r.Tx); err != nil {
		return nil, fibreFail("%s: tx does not decode: %v", at, err)
	}
	var body cosmostx.TxBody
	if err := body.Unmarshal(txRaw.BodyBytes); err != nil {
		return nil, fibreFail("%s: tx body does not decode: %v", at, err)
	}
	if uint64(r.MsgIndex) >= uint64(len(body.Messages)) || body.Messages[r.MsgIndex] == nil {
		return nil, fibreFail("%s: tx has %d messages", at, len(body.Messages))
	}
	msg := body.Messages[r.MsgIndex]
	var pb *fibretypes.PaymentPromise
	switch fibrearchive.Kind(r.Kind) {
	case fibrearchive.KindPayForFibre:
		if msg.TypeUrl != fibretypes.MsgPayForFibreTypeURL || r.MsgIndex != 0 || len(body.Messages) != 1 {
			return nil, fibreFail("%s: kind pff, but the message is %s (msg %d of %d), not a single MsgPayForFibre", at, msg.TypeUrl, r.MsgIndex, len(body.Messages))
		}
		var m fibretypes.MsgPayForFibre
		if err := m.Unmarshal(msg.Value); err != nil {
			return nil, fibreFail("%s: decode MsgPayForFibre: %v", at, err)
		}
		pb = &m.PaymentPromise
	case fibrearchive.KindTimeout:
		if msg.TypeUrl != timeoutTypeURL {
			return nil, fibreFail("%s: kind timeout, but the message is %s", at, msg.TypeUrl)
		}
		var m fibretypes.MsgPaymentPromiseTimeout
		if err := m.Unmarshal(msg.Value); err != nil {
			return nil, fibreFail("%s: decode MsgPaymentPromiseTimeout: %v", at, err)
		}
		pb = &m.PaymentPromise
	}
	var p fibre.PaymentPromise
	if err := p.FromProto(pb); err != nil {
		return nil, fibreFail("%s: payment promise: %v", at, err)
	}
	return &p, nil
}

func fibreCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fibre",
		Short: "Read and verify the Fibre blobs of the archive",
	}
	cmd.AddCommand(fibreVerifyCmd())
	return cmd
}

func fibreVerifyCmd() *cobra.Command {
	var (
		archiveURL        string
		height            int64
		txIndex, msgIndex uint32
	)
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Check one archived Fibre message against its paid commitment and promise signature",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			res, err := VerifyFibre(cmd.Context(), client.New(archiveURL), height, txIndex, msgIndex)
			if err != nil {
				return err // ExitStatus tells a failed check from a failed run
			}
			w := cmd.OutOrStdout()
			switch {
			case res.Status == fibrearchive.StatusArchived.String():
				fmt.Fprintln(w, "OK: data matches the paid commitment; promise signed by its escrow owner (inclusion and validator quorum not checked yet)")
			case res.PromiseOK:
				fmt.Fprintf(w, "%s: no data archived for blob %s; promise signed by its escrow owner (inclusion and validator quorum not checked yet)\n", res.Status, res.BlobID)
			default:
				fmt.Fprintf(w, "%s: %s\n", res.Status, res.Reason)
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&archiveURL, "archive", "", "archive base URL")
	f.Int64Var(&height, "height", 0, "block height of the tx")
	f.Uint32Var(&txIndex, "tx-index", 0, "index of the tx in the block")
	f.Uint32Var(&msgIndex, "msg-index", 0, "index of the message in the tx body")
	for _, n := range []string{"archive", "height", "tx-index"} {
		_ = cmd.MarkFlagRequired(n)
	}
	return cmd
}

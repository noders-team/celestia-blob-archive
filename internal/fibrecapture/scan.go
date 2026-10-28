// Package fibrecapture captures Fibre blobs referenced by PayForFibre and
// payment-promise-timeout messages into the fibre/ space of the archive.
package fibrecapture

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	"github.com/celestiaorg/go-square/v4/share"
	squaretx "github.com/celestiaorg/go-square/v4/tx"
	cosmostx "github.com/cosmos/cosmos-sdk/types/tx"

	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
	"github.com/noders-team/celestia-blob-archive/internal/source"
)

// timeoutTypeURL is the SDK type URL of MsgPaymentPromiseTimeout. Unlike PFF,
// a timeout is not restricted to single-message txs (app/check_tx.go ranges
// over all messages), so every position is scanned.
const timeoutTypeURL = "/celestia.fibre.v1.MsgPaymentPromiseTimeout"

// supportedBlobVersion is the only Fibre blob version v10.5.0 can decode
// (fibre.BlobConfigForVersion).
const supportedBlobVersion = 0

// Item is one Fibre message found in a block, with its payment promise.
type Item struct {
	Height        int64
	TxIndex       uint32
	MsgIndex      uint32
	Kind          fibrearchive.Kind
	Tx            []byte
	BlockTime     time.Time
	BlobID        [33]byte // blob version byte || 32-byte commitment
	Namespace     [29]byte
	BlobSize      uint32
	BlobVersion   uint32
	PromiseHeight uint64
	Creation      time.Time
	// Err is non-empty when the promise is malformed or its blob version is
	// unsupported; such an item is unrecoverable without a download.
	Err string
}

// Key returns the item's fibre/records key.
func (it Item) Key() string { return fibrearchive.RecordKey(it.Height, it.TxIndex, it.MsgIndex) }

// Extract returns the PayForFibre and payment-promise-timeout messages of b in
// (tx_index, msg_index) order. BlobTxs and non-SDK bytes are skipped.
func Extract(b *source.Block) ([]Item, error) {
	if b == nil {
		return nil, errors.New("fibrecapture: nil block")
	}
	if uint64(len(b.Txs)) > math.MaxUint32 {
		return nil, fmt.Errorf("fibrecapture: block %d has %d txs", b.Height, len(b.Txs))
	}
	var items []Item
	for i, tx := range b.Txs {
		base := Item{Height: b.Height, TxIndex: uint32(i), BlockTime: b.Time}

		_, isFibre, parseErr := fibretypes.TryParseFibreTx(tx)
		if isFibre {
			// TryParseFibreTx only reports isFibre for a decodable
			// single-message body, so this decode succeeds.
			body, _ := decodeBody(tx)
			it := base
			it.Kind = fibrearchive.KindPayForFibre
			it.Tx = bytes.Clone(tx)
			var msg fibretypes.MsgPayForFibre
			if err := msg.Unmarshal(body.Messages[0].Value); err != nil {
				it.Err = fmt.Sprintf("decoding MsgPayForFibre: %v", err)
			} else {
				fillPromise(&it, &msg.PaymentPromise)
			}
			if it.Err == "" && parseErr != nil {
				it.Err = parseErr.Error()
			}
			items = append(items, it)
			continue
		}

		if _, isBlobTx, _ := squaretx.UnmarshalBlobTx(tx); isBlobTx {
			continue
		}
		body, ok := decodeBody(tx)
		if !ok {
			continue
		}
		if uint64(len(body.Messages)) > math.MaxUint32 {
			continue
		}
		for j, m := range body.Messages {
			if m == nil || m.TypeUrl != timeoutTypeURL {
				continue
			}
			it := base
			it.MsgIndex = uint32(j)
			it.Kind = fibrearchive.KindTimeout
			it.Tx = bytes.Clone(tx)
			var msg fibretypes.MsgPaymentPromiseTimeout
			if err := msg.Unmarshal(m.Value); err != nil {
				it.Err = fmt.Sprintf("decoding MsgPaymentPromiseTimeout: %v", err)
			} else {
				fillPromise(&it, &msg.PaymentPromise)
			}
			items = append(items, it)
		}
	}
	return items, nil
}

// decodeBody decodes tx the way the SDK does: TxRaw, then body_bytes as TxBody.
func decodeBody(tx []byte) (*cosmostx.TxBody, bool) {
	var raw cosmostx.TxRaw
	if err := raw.Unmarshal(tx); err != nil {
		return nil, false
	}
	var body cosmostx.TxBody
	if err := body.Unmarshal(raw.BodyBytes); err != nil {
		return nil, false
	}
	return &body, true
}

// fillPromise copies p into it and sets it.Err for the first defect found.
func fillPromise(it *Item, p *fibretypes.PaymentPromise) {
	it.BlobSize = p.BlobSize
	it.BlobVersion = p.BlobVersion
	it.Creation = p.CreationTimestamp
	fail := func(format string, args ...any) {
		if it.Err == "" {
			it.Err = fmt.Sprintf(format, args...)
		}
	}
	if p.Height < 0 {
		fail("negative promise height %d", p.Height)
	} else {
		it.PromiseHeight = uint64(p.Height)
	}
	if _, err := share.NewNamespaceFromBytes(p.Namespace); err != nil {
		fail("invalid namespace: %v", err)
	} else {
		copy(it.Namespace[:], p.Namespace)
	}
	if len(p.Commitment) != len(it.BlobID)-1 {
		fail("commitment is %d bytes, want %d", len(p.Commitment), len(it.BlobID)-1)
	} else {
		it.BlobID[0] = uint8(p.BlobVersion)
		copy(it.BlobID[1:], p.Commitment)
	}
	if p.BlobVersion != supportedBlobVersion {
		fail("unsupported blob version %d", p.BlobVersion)
	}
}

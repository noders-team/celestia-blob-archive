package fibrecapture

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	"github.com/celestiaorg/go-square/v4/share"
	squaretx "github.com/celestiaorg/go-square/v4/tx"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cosmostx "github.com/cosmos/cosmos-sdk/types/tx"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	gogoproto "github.com/cosmos/gogoproto/proto"

	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
	"github.com/noders-team/celestia-blob-archive/internal/source"
)

// fixturePFF returns the real mocha-5 PFF tx (height 1497140) and its decoded
// message.
func fixturePFF(t *testing.T) ([]byte, *fibretypes.MsgPayForFibre) {
	t.Helper()
	b64, err := os.ReadFile(filepath.Join("..", "..", "testdata", "fibre", "pff_tx_5DA67B2A.b64"))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b64)))
	if err != nil {
		t.Fatal(err)
	}
	var raw cosmostx.TxRaw
	if err := raw.Unmarshal(tx); err != nil {
		t.Fatal(err)
	}
	var body cosmostx.TxBody
	if err := body.Unmarshal(raw.BodyBytes); err != nil {
		t.Fatal(err)
	}
	var msg fibretypes.MsgPayForFibre
	if err := msg.Unmarshal(body.Messages[0].Value); err != nil {
		t.Fatal(err)
	}
	return tx, &msg
}

type marshaler interface{ Marshal() ([]byte, error) }

func anyMsg(t *testing.T, typeURL string, m marshaler) *codectypes.Any {
	t.Helper()
	v, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return &codectypes.Any{TypeUrl: typeURL, Value: v}
}

// buildTx encodes msgs as an unsigned SDK TxRaw.
func buildTx(t *testing.T, msgs ...*codectypes.Any) []byte {
	t.Helper()
	body, err := (&cosmostx.TxBody{Messages: msgs}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := (&cosmostx.TxRaw{BodyBytes: body}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func bankSend(t *testing.T) *codectypes.Any {
	return anyMsg(t, "/cosmos.bank.v1beta1.MsgSend", &banktypes.MsgSend{FromAddress: "a", ToAddress: "b"})
}

func block(txs ...[]byte) *source.Block {
	return &source.Block{Height: 1497140, Time: time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC), Txs: txs}
}

func TestExtractRealPFF(t *testing.T) {
	tx, _ := fixturePFF(t)
	b := block(buildTx(t, bankSend(t)), buildTx(t, bankSend(t)), tx, buildTx(t, bankSend(t)))
	items, err := Extract(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("got %d items, want 1", len(items))
	}
	it := items[0]
	if it.Err != "" {
		t.Fatalf("Err = %q", it.Err)
	}
	if it.Height != b.Height || it.TxIndex != 2 || it.MsgIndex != 0 || it.Kind != fibrearchive.KindPayForFibre {
		t.Errorf("position = %d/%d/%d kind %v", it.Height, it.TxIndex, it.MsgIndex, it.Kind)
	}
	if it.BlobSize != 17039360 || it.PromiseHeight != 1497137 || it.BlobID[0] != 0 || it.BlobVersion != 0 {
		t.Errorf("promise: size %d height %d id[0] %d version %d", it.BlobSize, it.PromiseHeight, it.BlobID[0], it.BlobVersion)
	}
	if !bytes.Equal(it.Tx, tx) {
		t.Error("Tx differs from the fixture bytes")
	}
	if !it.BlockTime.Equal(b.Time) || it.Creation.IsZero() {
		t.Errorf("BlockTime %v Creation %v", it.BlockTime, it.Creation)
	}
	if _, err := share.NewNamespaceFromBytes(it.Namespace[:]); err != nil {
		t.Errorf("namespace: %v", err)
	}
	if it.Key() != fibrearchive.RecordKey(b.Height, 2, 0) {
		t.Errorf("Key = %q", it.Key())
	}
}

func TestExtractTimeoutInMultiMsgTx(t *testing.T) {
	_, pff := fixturePFF(t)
	timeout := &fibretypes.MsgPaymentPromiseTimeout{Signer: pff.Signer, PaymentPromise: pff.PaymentPromise}
	tx := buildTx(t, bankSend(t), anyMsg(t, timeoutTypeURL, timeout))
	items, err := Extract(block(tx))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("got %d items, want 1", len(items))
	}
	it := items[0]
	if it.Kind != fibrearchive.KindTimeout || it.TxIndex != 0 || it.MsgIndex != 1 || it.Err != "" {
		t.Fatalf("item = kind %v tx %d msg %d err %q", it.Kind, it.TxIndex, it.MsgIndex, it.Err)
	}
	if it.BlobSize != 17039360 || it.PromiseHeight != 1497137 || !bytes.Equal(it.BlobID[1:], pff.PaymentPromise.Commitment) {
		t.Errorf("promise fields not copied: size %d height %d", it.BlobSize, it.PromiseHeight)
	}
	if !bytes.Equal(it.Namespace[:], pff.PaymentPromise.Namespace) || !it.Creation.Equal(pff.PaymentPromise.CreationTimestamp) {
		t.Error("namespace or creation not copied")
	}
}

func TestTimeoutTypeURLMatchesRegistry(t *testing.T) {
	if got := "/" + gogoproto.MessageName(&fibretypes.MsgPaymentPromiseTimeout{}); got != timeoutTypeURL {
		t.Fatalf("registered %q, const %q", got, timeoutTypeURL)
	}
}

func TestExtractIgnoresBlobTxAndGarbage(t *testing.T) {
	pffTx, _ := fixturePFF(t)
	ns := share.MustNewV0Namespace(bytes.Repeat([]byte{1}, share.NamespaceVersionZeroIDSize))
	blob, err := share.NewBlob(ns, []byte("data"), share.ShareVersionZero, nil)
	if err != nil {
		t.Fatal(err)
	}
	blobTx, err := squaretx.MarshalBlobTx(pffTx, blob)
	if err != nil {
		t.Fatal(err)
	}
	garbage := []byte{0xff, 0x13, 0x37, 0x00, 0x42, 0x99, 0xde, 0xad}
	items, err := Extract(block(blobTx, garbage, nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("got %d items, want 0: %+v", len(items), items)
	}
}

func TestExtractMalformedPromise(t *testing.T) {
	cases := map[string]func(*fibretypes.PaymentPromise){
		"31-byte commitment": func(p *fibretypes.PaymentPromise) { p.Commitment = p.Commitment[:31] },
		"blob version 1":     func(p *fibretypes.PaymentPromise) { p.BlobVersion = 1 },
		"bad namespace":      func(p *fibretypes.PaymentPromise) { p.Namespace = []byte{1, 2, 3} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			_, pff := fixturePFF(t)
			mutate(&pff.PaymentPromise)
			tx := buildTx(t, anyMsg(t, fibretypes.MsgPayForFibreTypeURL, pff))
			timeout := &fibretypes.MsgPaymentPromiseTimeout{Signer: pff.Signer, PaymentPromise: pff.PaymentPromise}
			ttx := buildTx(t, anyMsg(t, timeoutTypeURL, timeout))
			items, err := Extract(block(tx, ttx))
			if err != nil {
				t.Fatal(err)
			}
			if len(items) != 2 {
				t.Fatalf("got %d items, want 2", len(items))
			}
			for _, it := range items {
				if it.Err == "" {
					t.Errorf("%v item: Err empty", it.Kind)
				}
			}
			if items[0].Kind != fibrearchive.KindPayForFibre || items[1].Kind != fibrearchive.KindTimeout {
				t.Errorf("kinds %v %v", items[0].Kind, items[1].Kind)
			}
		})
	}
}

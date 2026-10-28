package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/celestiaorg/celestia-app/v10/fibre"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	cosmostx "github.com/cosmos/cosmos-sdk/types/tx"
	"google.golang.org/protobuf/types/known/timestamppb"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/fibrearchive"
	"github.com/noders-team/celestia-blob-archive/internal/store"
	"github.com/noders-team/celestia-blob-archive/internal/tombstone"
	"github.com/noders-team/celestia-blob-archive/internal/verify"
)

const (
	fHeight  = 1497140
	fTxIndex = 2
)

// fibreEnv is an FS archive holding Fibre records and payloads.
type fibreEnv struct {
	t   *testing.T
	dir string
	st  store.Store
	key *secp256k1.PrivKey
}

func newFibreEnv(t *testing.T) *fibreEnv {
	t.Helper()
	dir := t.TempDir()
	e := &fibreEnv{t: t, dir: dir, st: store.NewFS(dir), key: secp256k1.GenPrivKey()}
	e.manifest(fibrearchive.FibreFormat)
	return e
}

func (e *fibreEnv) manifest(format int) {
	e.t.Helper()
	m := fibrearchive.Manifest{FibreFormat: format, ChainID: "mocha-5", FirstHeight: 1490001, ScannedUntil: 1497200, UpdatedAt: time.Now()}
	if err := fibrearchive.WriteManifest(context.Background(), e.st, m); err != nil {
		e.t.Fatal(err)
	}
}

// promise returns a promise for data signed by the env's escrow key, the
// blob ID derived from it and its upload size.
func (e *fibreEnv) promise(data []byte) (*fibretypes.PaymentPromise, [33]byte) {
	e.t.Helper()
	cfg, err := fibre.BlobConfigForVersion(0)
	if err != nil {
		e.t.Fatal(err)
	}
	b, err := fibre.NewBlob(bytes.Clone(data), cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	defer b.Free()
	var id [33]byte
	copy(id[:], b.ID())
	pb := &fibretypes.PaymentPromise{
		ChainId:           "mocha-5",
		Height:            fHeight - 3,
		Namespace:         append(make([]byte, 19), bytes.Repeat([]byte{0x0a}, 10)...),
		BlobSize:          uint32(cfg.UploadSize(len(data))),
		BlobVersion:       0,
		Commitment:        bytes.Clone(id[1:]),
		CreationTimestamp: time.Date(2026, 10, 27, 11, 59, 0, 0, time.UTC),
		SignerPublicKey:   *e.key.PubKey().(*secp256k1.PubKey),
	}
	e.sign(pb)
	return pb, id
}

func (e *fibreEnv) sign(pb *fibretypes.PaymentPromise) {
	e.t.Helper()
	var p fibre.PaymentPromise
	if err := p.FromProto(pb); err != nil {
		e.t.Fatal(err)
	}
	sb, err := p.SignBytes()
	if err != nil {
		e.t.Fatal(err)
	}
	sig, err := e.key.Sign(sb)
	if err != nil {
		e.t.Fatal(err)
	}
	pb.Signature = sig
}

func fibreTx(t *testing.T, msgs ...*codectypes.Any) []byte {
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

func anyOf(t *testing.T, url string, m interface{ Marshal() ([]byte, error) }) *codectypes.Any {
	t.Helper()
	v, err := m.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return &codectypes.Any{TypeUrl: url, Value: v}
}

func pffAny(t *testing.T, pb *fibretypes.PaymentPromise) *codectypes.Any {
	return anyOf(t, fibretypes.MsgPayForFibreTypeURL, &fibretypes.MsgPayForFibre{Signer: "celestia1x", PaymentPromise: *pb})
}

func timeoutAny(t *testing.T, pb *fibretypes.PaymentPromise) *codectypes.Any {
	return anyOf(t, timeoutTypeURL, &fibretypes.MsgPaymentPromiseTimeout{Signer: "celestia1x", PaymentPromise: *pb})
}

func (e *fibreEnv) putRecord(r *archivev1.FibreRecord) {
	e.t.Helper()
	b, err := fibrearchive.EncodeRecord(r)
	if err != nil {
		e.t.Fatal(err)
	}
	if err := e.st.Put(context.Background(), fibrearchive.RecordKey(int64(r.Height), r.TxIndex, r.MsgIndex), b); err != nil {
		e.t.Fatal(err)
	}
}

func baseRecord(tx []byte, id [33]byte, kind fibrearchive.Kind, msgIndex uint32, status fibrearchive.Status) *archivev1.FibreRecord {
	return &archivev1.FibreRecord{
		Height: fHeight, TxIndex: fTxIndex, MsgIndex: msgIndex, Kind: uint32(kind), Tx: tx,
		DataStatus: uint32(status), BlobId: id[:],
		CapturedAt: timestamppb.Now(), BlockTime: timestamppb.New(time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)),
	}
}

// archived writes an archived PFF record and its payload; it returns the
// payload and blob ID.
func (e *fibreEnv) archived() ([]byte, [33]byte) {
	e.t.Helper()
	data := make([]byte, 3000)
	if _, err := rand.Read(data); err != nil {
		e.t.Fatal(err)
	}
	pb, id := e.promise(data)
	r := baseRecord(fibreTx(e.t, pffAny(e.t, pb)), id, fibrearchive.KindPayForFibre, 0, fibrearchive.StatusArchived)
	sum := sha256.Sum256(data)
	r.DataSize, r.DataSha256 = uint64(len(data)), sum[:]
	e.putRecord(r)
	if err := e.st.Put(context.Background(), fibrearchive.DataKey(id), data); err != nil {
		e.t.Fatal(err)
	}
	return data, id
}

func wantFailure(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil {
		t.Fatal("verify passed, want a failed check")
	}
	if code, msg := ExitStatus(err); code != ExitVerifyFailed {
		t.Fatalf("ExitStatus = %d %q, want %d (a failed check, not a run error)", code, msg, ExitVerifyFailed)
	}
	if !strings.Contains(err.Error(), substr) {
		t.Fatalf("error %q, want it to mention %q", err, substr)
	}
}

func runCLI(t *testing.T, dir string, args ...string) (string, error) {
	t.Helper()
	srv := httptest.NewServer(http.FileServer(http.Dir(dir)))
	t.Cleanup(srv.Close)
	var out bytes.Buffer
	root := NewRoot()
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(append([]string{"fibre", "verify", "--archive", srv.URL}, args...))
	err := root.ExecuteContext(context.Background())
	return out.String(), err
}

var fibreArgs = []string{"--height", strconv.Itoa(fHeight), "--tx-index", strconv.Itoa(fTxIndex)}

func TestFibreVerifyOK(t *testing.T) {
	e := newFibreEnv(t)
	data, id := e.archived()
	res, err := VerifyFibre(context.Background(), e.st, fHeight, fTxIndex, 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != "archived" || res.BlobID != hex.EncodeToString(id[:]) || res.DataSize != uint64(len(data)) {
		t.Fatalf("result = %+v", res)
	}
	out, err := runCLI(t, e.dir, fibreArgs...)
	if err != nil {
		t.Fatal(err)
	}
	if want := "OK: data matches the paid commitment; promise signed by its escrow owner (inclusion and validator quorum not checked yet)\n"; out != want {
		t.Fatalf("output %q, want %q", out, want)
	}
}

func TestFibreVerifyTamperedData(t *testing.T) {
	ctx := context.Background()
	t.Run("payload", func(t *testing.T) {
		e := newFibreEnv(t)
		data, id := e.archived()
		data[100] ^= 1
		if err := e.st.Put(ctx, fibrearchive.DataKey(id), data); err != nil {
			t.Fatal(err)
		}
		_, err := VerifyFibre(ctx, e.st, fHeight, fTxIndex, 0)
		wantFailure(t, err, "does not match the paid commitment")
		if _, err := runCLI(t, e.dir, fibreArgs...); err == nil {
			t.Fatal("cli passed")
		}
	})
	t.Run("data_sha256", func(t *testing.T) {
		e := newFibreEnv(t)
		e.archived()
		raw, _ := e.st.Get(ctx, fibrearchive.RecordKey(fHeight, fTxIndex, 0))
		r, _ := fibrearchive.DecodeRecord(raw)
		r.DataSha256[0] ^= 1
		e.putRecord(r)
		_, err := VerifyFibre(ctx, e.st, fHeight, fTxIndex, 0)
		wantFailure(t, err, "data_sha256")
	})
	t.Run("missing payload", func(t *testing.T) {
		e := newFibreEnv(t)
		_, id := e.archived()
		if err := e.st.Delete(ctx, fibrearchive.DataKey(id)); err != nil {
			t.Fatal(err)
		}
		_, err := VerifyFibre(ctx, e.st, fHeight, fTxIndex, 0)
		wantFailure(t, err, "missing")
	})
}

func TestFibreVerifyRecordBlobIDMismatch(t *testing.T) {
	ctx := context.Background()
	e := newFibreEnv(t)
	e.archived()
	raw, _ := e.st.Get(ctx, fibrearchive.RecordKey(fHeight, fTxIndex, 0))
	r, _ := fibrearchive.DecodeRecord(raw)
	r.BlobId[5] ^= 1
	e.putRecord(r)
	_, err := VerifyFibre(ctx, e.st, fHeight, fTxIndex, 0)
	wantFailure(t, err, "blob_id")
}

func TestFibreVerifyBadPromiseSignature(t *testing.T) {
	ctx := context.Background()
	e := newFibreEnv(t)
	data := []byte("hello fibre")
	pb, id := e.promise(data)
	pb.Signature[3] ^= 1
	r := baseRecord(fibreTx(t, pffAny(t, pb)), id, fibrearchive.KindPayForFibre, 0, fibrearchive.StatusExpired)
	e.putRecord(r)
	_, err := VerifyFibre(ctx, e.st, fHeight, fTxIndex, 0)
	wantFailure(t, err, "signature")
}

func TestFibreVerifyWrongKindOrPosition(t *testing.T) {
	ctx := context.Background()
	e := newFibreEnv(t)
	pb, id := e.promise([]byte("x"))
	// The record claims a PFF at msg_index 0, the tx holds a timeout there.
	e.putRecord(baseRecord(fibreTx(t, timeoutAny(t, pb)), id, fibrearchive.KindPayForFibre, 0, fibrearchive.StatusExpired))
	_, err := VerifyFibre(ctx, e.st, fHeight, fTxIndex, 0)
	wantFailure(t, err, "MsgPayForFibre")

	// A record stored under another message's key.
	r := baseRecord(fibreTx(t, pffAny(t, pb)), id, fibrearchive.KindPayForFibre, 0, fibrearchive.StatusExpired)
	b, _ := fibrearchive.EncodeRecord(r)
	if err := e.st.Put(ctx, fibrearchive.RecordKey(fHeight, fTxIndex+1, 0), b); err != nil {
		t.Fatal(err)
	}
	_, err = VerifyFibre(ctx, e.st, fHeight, fTxIndex+1, 0)
	wantFailure(t, err, "stored under")
}

func TestFibreVerifyTimeoutAtMsgIndex(t *testing.T) {
	ctx := context.Background()
	e := newFibreEnv(t)
	pb, id := e.promise([]byte("timeout payload"))
	other, _ := e.promise([]byte("other"))
	tx := fibreTx(t, timeoutAny(t, other), timeoutAny(t, pb))
	e.putRecord(baseRecord(tx, id, fibrearchive.KindTimeout, 1, fibrearchive.StatusExpired))
	res, err := VerifyFibre(ctx, e.st, fHeight, fTxIndex, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != "expired" || res.BlobID != hex.EncodeToString(id[:]) {
		t.Fatalf("result = %+v", res)
	}
}

func TestFibreVerifyWithdrawn(t *testing.T) {
	ctx := context.Background()
	e := newFibreEnv(t)
	_, id := e.archived()
	if err := tombstone.AddFibre(ctx, e.st, tombstone.FibreEntry{BlobID: hex.EncodeToString(id[:]), Date: "2026-10-27", Reason: "dmca"}); err != nil {
		t.Fatal(err)
	}
	if err := e.st.Delete(ctx, fibrearchive.DataKey(id)); err != nil {
		t.Fatal(err)
	}
	_, err := VerifyFibre(ctx, e.st, fHeight, fTxIndex, 0)
	if !errors.Is(err, verify.ErrWithdrawn) {
		t.Fatalf("err = %v, want ErrWithdrawn", err)
	}
	if code, _ := ExitStatus(err); code != ExitVerifyFailed {
		t.Fatalf("exit %d, want %d as for a withdrawn blob", code, ExitVerifyFailed)
	}
	if !strings.Contains(err.Error(), "blob withdrawn, see tombstones.json") {
		t.Fatalf("message %q", err)
	}
}

func TestFibreVerifyExpiredReportsStatus(t *testing.T) {
	e := newFibreEnv(t)
	pb, id := e.promise([]byte("never downloaded"))
	e.putRecord(baseRecord(fibreTx(t, pffAny(t, pb)), id, fibrearchive.KindPayForFibre, 0, fibrearchive.StatusExpired))
	res, err := VerifyFibre(context.Background(), e.st, fHeight, fTxIndex, 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != "expired" || res.DataSize != 0 {
		t.Fatalf("result = %+v", res)
	}
	out, err := runCLI(t, e.dir, fibreArgs...)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out, "expired: ") || strings.Contains(out, "OK") {
		t.Fatalf("output %q, want the status reported", out)
	}
}

func TestFibreVerifyUnrecoverableMalformedPromise(t *testing.T) {
	e := newFibreEnv(t)
	pb, _ := e.promise([]byte("y"))
	pb.Commitment = pb.Commitment[:31]
	r := baseRecord(fibreTx(t, pffAny(t, pb)), [33]byte{}, fibrearchive.KindPayForFibre, 0, fibrearchive.StatusUnrecoverable)
	r.Error = "commitment is 31 bytes, want 32"
	e.putRecord(r)
	res, err := VerifyFibre(context.Background(), e.st, fHeight, fTxIndex, 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != "unrecoverable" || res.PromiseOK {
		t.Fatalf("result = %+v", res)
	}
}

func TestFibreVerifyRunErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("fibre_format", func(t *testing.T) {
		e := newFibreEnv(t)
		e.archived()
		e.manifest(2)
		_, err := VerifyFibre(ctx, e.st, fHeight, fTxIndex, 0)
		if code, _ := ExitStatus(err); err == nil || code != ExitError || !strings.Contains(err.Error(), "fibre_format 2") {
			t.Fatalf("err = %v (exit %d), want a run error naming fibre_format 2", err, code)
		}
	})
	t.Run("no manifest", func(t *testing.T) {
		e := newFibreEnv(t)
		if err := e.st.Delete(ctx, fibrearchive.ManifestKey); err != nil {
			t.Fatal(err)
		}
		_, err := VerifyFibre(ctx, e.st, fHeight, fTxIndex, 0)
		if code, _ := ExitStatus(err); err == nil || code != ExitError {
			t.Fatalf("err = %v (exit %d), want a run error", err, code)
		}
	})
	t.Run("not scanned", func(t *testing.T) {
		e := newFibreEnv(t)
		_, err := VerifyFibre(ctx, e.st, 1497201, 0, 0)
		if code, _ := ExitStatus(err); err == nil || code != ExitError || !strings.Contains(err.Error(), "not scanned") {
			t.Fatalf("err = %v (exit %d), want a run error", err, code)
		}
	})
	t.Run("no record in scanned range", func(t *testing.T) {
		e := newFibreEnv(t)
		_, err := VerifyFibre(ctx, e.st, fHeight, 7, 0)
		wantFailure(t, err, "no Fibre record")
	})
}

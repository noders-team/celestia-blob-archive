package process

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/celestiaorg/celestia-app/v10/pkg/proof"
	fibretypes "github.com/celestiaorg/celestia-app/v10/x/fibre/types"
	blobtx "github.com/celestiaorg/go-square/v4/tx"
	"github.com/celestiaorg/nmt"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/source"
)

const fixtureDir = "../../testdata/blocks"

type fixture struct {
	Cur  *source.Block `json:"cur"`
	Next *source.Block `json:"next"`
}

func loadFixture(t *testing.T, path string) (cur, next *source.Block) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f fixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if f.Cur == nil || f.Next == nil {
		t.Fatalf("%s: missing cur or next", path)
	}
	return f.Cur, f.Next
}

func fixturePaths(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(fixtureDir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) < 6 {
		t.Fatalf("expected at least 6 fixtures, found %d", len(paths))
	}
	return paths
}

type wantBlob struct {
	txIndex, blobIndex int
	ns                 []byte
	shareVersion       uint8
	data, signer       []byte
}

// expectedBlobs decodes the BlobTxs independently of the code under test.
func expectedBlobs(t *testing.T, b *source.Block) []wantBlob {
	t.Helper()
	var out []wantBlob
	for i, raw := range b.Txs {
		btx, isBlobTx, err := blobtx.UnmarshalBlobTx(raw)
		if !isBlobTx {
			continue
		}
		if err != nil {
			t.Fatalf("tx %d: %v", i, err)
		}
		for j, bl := range btx.Blobs {
			out = append(out, wantBlob{i, j, bl.Namespace().Bytes(), bl.ShareVersion(), bl.Data(), bl.Signer()})
		}
	}
	return out
}

func TestProcessFixtures(t *testing.T) {
	for _, path := range fixturePaths(t) {
		name := strings.TrimSuffix(filepath.Base(path), ".json")
		t.Run(name, func(t *testing.T) {
			cur, next := loadFixture(t, path)
			hr, blobs, err := Process(cur, next)
			if err != nil {
				t.Fatalf("Process: %v", err)
			}
			want := expectedBlobs(t, cur)

			if hr.Height != uint64(cur.Height) || hr.AppVersion != cur.AppVersion ||
				hr.SquareSize != cur.SquareSize || !bytes.Equal(hr.DataHash, cur.DataHash) ||
				!hr.Time.AsTime().Equal(cur.Time) {
				t.Errorf("HeightRecord header fields wrong: %+v", hr)
			}
			if int(hr.BlobCount) != len(want) || len(blobs) != len(want) {
				t.Fatalf("blob count: record %d, returned %d, BlobTx blobs %d", hr.BlobCount, len(blobs), len(want))
			}
			if !bytes.Equal(hr.Commit, next.LastCommitProto) {
				t.Error("HeightRecord.Commit != next.LastCommitProto")
			}
			if !bytes.Equal(hr.Header, cur.HeaderProto) {
				t.Error("HeightRecord.Header != cur.HeaderProto")
			}

			for i, br := range blobs {
				w := want[i]
				if br.Height != hr.Height || int(br.TxIndex) != w.txIndex || int(br.BlobIndex) != w.blobIndex {
					t.Errorf("blob %d: position (%d,%d,%d), want (%d,%d,%d)", i, br.Height, br.TxIndex, br.BlobIndex, hr.Height, w.txIndex, w.blobIndex)
				}
				if len(br.Namespace) != 29 || !bytes.Equal(br.Namespace, w.ns) {
					t.Errorf("blob %d: namespace %x, want %x", i, br.Namespace, w.ns)
				}
				if br.ShareVersion != uint32(w.shareVersion) {
					t.Errorf("blob %d: share version %d, want %d", i, br.ShareVersion, w.shareVersion)
				}
				if !bytes.Equal(br.Data, w.data) {
					t.Errorf("blob %d: data differs from BlobTx", i)
				}
				if len(br.Commitment) != 32 {
					t.Errorf("blob %d: commitment len %d", i, len(br.Commitment))
				}
				if (br.ShareVersion == 1) != (len(br.Signer) > 0) {
					t.Errorf("blob %d: share version %d with signer len %d", i, br.ShareVersion, len(br.Signer))
				}
				if !bytes.Equal(br.Signer, w.signer) {
					t.Errorf("blob %d: signer %x, want %x", i, br.Signer, w.signer)
				}
				if br.ShareEnd <= br.ShareStart {
					t.Errorf("blob %d: share range [%d,%d)", i, br.ShareStart, br.ShareEnd)
				}
				verifyProof(t, i, br, cur.DataHash)
			}
		})
	}
}

func verifyProof(t *testing.T, i int, br *archivev1.BlobRecord, dataHash []byte) {
	t.Helper()
	var sp proof.ShareProof
	if err := sp.Unmarshal(br.ShareProof); err != nil {
		t.Errorf("blob %d: unmarshal ShareProof: %v", i, err)
		return
	}
	if err := sp.Validate(dataHash); err != nil {
		t.Errorf("blob %d: ShareProof.Validate: %v", i, err)
	}
	if len(sp.Data) != int(br.ShareEnd-br.ShareStart) {
		t.Errorf("blob %d: proof carries %d shares, range is %d", i, len(sp.Data), br.ShareEnd-br.ShareStart)
	}
	ns := br.Namespace
	off := 0
	for r, p := range sp.ShareProofs {
		n := int(p.End - p.Start)
		leaves := sp.Data[off : off+n]
		off += n
		np := nmt.NewInclusionProof(int(p.Start), int(p.End), p.Nodes, true)
		if !np.VerifyInclusion(sha256.New(), ns, leaves, sp.RowProof.RowRoots[r]) {
			t.Errorf("blob %d: nmt row proof %d does not verify", i, r)
		}
	}
	if off != len(sp.Data) {
		t.Errorf("blob %d: row proofs cover %d of %d shares", i, off, len(sp.Data))
	}
}

func TestProcessMochaSameNamespace(t *testing.T) {
	cur, next := loadFixture(t, filepath.Join(fixtureDir, "mocha-1834013.json"))
	_, blobs, err := Process(cur, next)
	if err != nil {
		t.Fatal(err)
	}
	wantNS, _ := hexNamespace("636c6235656637626138")
	var n int
	var ranges [][2]uint32
	for _, b := range blobs {
		if bytes.Equal(b.Namespace, wantNS) {
			n++
			ranges = append(ranges, [2]uint32{b.ShareStart, b.ShareEnd})
		}
	}
	if n != 2 {
		t.Fatalf("blobs in namespace ...636c6235656637626138: %d, want 2", n)
	}
	if ranges[0][1] > ranges[1][0] && ranges[1][1] > ranges[0][0] {
		t.Errorf("share ranges overlap: %v", ranges)
	}
}

func hexNamespace(idHex string) ([]byte, error) {
	id := make([]byte, 0, 10)
	for i := 0; i+1 < len(idHex); i += 2 {
		var b byte
		for _, c := range idHex[i : i+2] {
			b <<= 4
			switch {
			case c >= '0' && c <= '9':
				b |= byte(c - '0')
			default:
				b |= byte(c-'a') + 10
			}
		}
		id = append(id, b)
	}
	ns := make([]byte, 29)
	copy(ns[29-len(id):], id)
	return ns, nil
}

func TestProcessDataRootMismatch(t *testing.T) {
	cur, next := loadFixture(t, filepath.Join(fixtureDir, "mocha-1834013.json"))
	cur.DataHash = bytes.Clone(cur.DataHash)
	cur.DataHash[0] ^= 0xff
	hr, blobs, err := Process(cur, next)
	if !errors.Is(err, ErrDataRootMismatch) {
		t.Fatalf("err = %v, want ErrDataRootMismatch", err)
	}
	if hr != nil || blobs != nil {
		t.Error("records returned together with an error")
	}
}

func TestProcessCommitMismatch(t *testing.T) {
	cur, next := loadFixture(t, filepath.Join(fixtureDir, "mocha-1834013.json"))
	var c cmtproto.Commit
	if err := c.Unmarshal(next.LastCommitProto); err != nil {
		t.Fatal(err)
	}
	c.BlockID.Hash = bytes.Clone(c.BlockID.Hash)
	c.BlockID.Hash[0] ^= 1
	b, err := c.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	next.LastCommitProto = b
	hr, blobs, err := Process(cur, next)
	if !errors.Is(err, ErrCommitMismatch) {
		t.Fatalf("err = %v, want ErrCommitMismatch", err)
	}
	if errors.Is(err, ErrDataRootMismatch) {
		t.Errorf("commit mismatch reported as data root mismatch: %v", err)
	}
	if hr != nil || blobs != nil {
		t.Error("records returned together with an error")
	}
}

func TestProcessEmptyBlock(t *testing.T) {
	var found bool
	for _, path := range fixturePaths(t) {
		cur, next := loadFixture(t, path)
		if len(expectedBlobs(t, cur)) != 0 {
			continue
		}
		found = true
		hr, blobs, err := Process(cur, next)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if hr == nil || hr.BlobCount != 0 || len(blobs) != 0 {
			t.Errorf("%s: want one HeightRecord with blob_count 0, got %+v and %d blobs", path, hr, len(blobs))
		}
	}
	if !found {
		t.Fatal("no fixture without blobs")
	}
}

func TestProcessInvalidInput(t *testing.T) {
	load := func(t *testing.T) (*source.Block, *source.Block) {
		return loadFixture(t, filepath.Join(fixtureDir, "mocha-1834013.json"))
	}
	tests := []struct {
		name   string
		mutate func(cur, next **source.Block)
	}{
		{"nil cur", func(cur, _ **source.Block) { *cur = nil }},
		{"nil next", func(_, next **source.Block) { *next = nil }},
		{"non-consecutive next", func(_, next **source.Block) { (*next).Height++ }},
		{"empty last commit", func(_, next **source.Block) { (*next).LastCommitProto = nil }},
		{"short data hash", func(cur, _ **source.Block) { (*cur).DataHash = (*cur).DataHash[:31] }},
		{"empty data hash", func(cur, _ **source.Block) { (*cur).DataHash = nil }},
		{"undecodable header", func(cur, _ **source.Block) { (*cur).HeaderProto = []byte{0xff} }},
		{"undecodable last commit", func(_, next **source.Block) { (*next).LastCommitProto = []byte{0xff} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cur, next := load(t)
			tt.mutate(&cur, &next)
			hr, blobs, err := Process(cur, next)
			if err == nil {
				t.Fatal("expected error")
			}
			if errors.Is(err, ErrDataRootMismatch) {
				t.Errorf("input error reported as data root mismatch: %v", err)
			}
			if hr != nil || blobs != nil {
				t.Error("records returned together with an error")
			}
		})
	}
}

func TestCheckCommitmentMismatch(t *testing.T) {
	cur, _ := loadFixture(t, filepath.Join(fixtureDir, "mocha-1834013.json"))
	btx, _, err := decodeBlobTx(cur.Txs[0], cur.AppVersion)
	if err != nil {
		t.Fatal(err)
	}
	declared, err := payForBlobsCommitments(btx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := checkCommitment(btx.Blobs[0], declared[0]); err != nil {
		t.Fatalf("matching commitment rejected: %v", err)
	}
	bad := bytes.Clone(declared[0])
	bad[0] ^= 1
	if _, err := checkCommitment(btx.Blobs[0], bad); err == nil {
		t.Fatal("mismatching commitment accepted")
	}
}

// nonCanonicalBlobTx returns the first BlobTx of b with an unknown protobuf
// field (number 15, varint 1) appended: go-square v2/v3 ignore it, v4 rejects it.
func nonCanonicalBlobTx(t *testing.T, b *source.Block) []byte {
	t.Helper()
	for _, raw := range b.Txs {
		if _, ok, _ := blobtx.UnmarshalBlobTx(raw); ok {
			return append(bytes.Clone(raw), 0x78, 0x01)
		}
	}
	t.Fatal("no BlobTx in fixture")
	return nil
}

func TestDecodeBlobTxUsesAppVersionDecoder(t *testing.T) {
	cur, _ := loadFixture(t, filepath.Join(fixtureDir, "mainnet-4499999.json"))
	bad := nonCanonicalBlobTx(t, cur)

	if _, _, err := blobtx.UnmarshalBlobTx(bad); !errors.Is(err, blobtx.ErrNonCanonicalBlobTx) {
		t.Fatalf("v4 decoder: err = %v, want ErrNonCanonicalBlobTx", err)
	}
	want := len(expectedBlobs(t, &source.Block{Txs: [][]byte{bad[:len(bad)-2]}}))
	for _, app := range []uint64{1, 3, 5, 6, 7} {
		btx, ok, err := decodeBlobTx(bad, app)
		if err != nil || !ok {
			t.Fatalf("app %d: ok=%t err=%v, want accepted", app, ok, err)
		}
		if len(btx.Blobs) != want {
			t.Errorf("app %d: %d blobs, want %d", app, len(btx.Blobs), want)
		}
	}
	for _, app := range []uint64{8, 9, 10} {
		if _, ok, err := decodeBlobTx(bad, app); !ok || !errors.Is(err, blobtx.ErrNonCanonicalBlobTx) {
			t.Errorf("app %d: ok=%t err=%v, want ErrNonCanonicalBlobTx from the v4 decoder", app, ok, err)
		}
	}
}

func TestDecodeBlobTxMatchesV4ForCanonical(t *testing.T) {
	for _, name := range []string{"mainnet-1500004", "mainnet-4499999", "mainnet-10000000", "mainnet-15300000"} {
		cur, _ := loadFixture(t, filepath.Join(fixtureDir, name+".json"))
		want := expectedBlobs(t, cur)
		var got int
		for _, raw := range cur.Txs {
			btx, ok, err := decodeBlobTx(raw, cur.AppVersion)
			if !ok {
				continue
			}
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			for _, b := range btx.Blobs {
				w := want[got]
				if !bytes.Equal(b.Namespace().Bytes(), w.ns) || !bytes.Equal(b.Data(), w.data) ||
					b.ShareVersion() != w.shareVersion || !bytes.Equal(b.Signer(), w.signer) {
					t.Errorf("%s: blob %d differs from v4 decode", name, got)
				}
				got++
			}
		}
		if got != len(want) {
			t.Errorf("%s: %d blobs, want %d", name, got, len(want))
		}
	}
}

func TestBlobCommitmentMatchesProcess(t *testing.T) {
	n := 0
	for _, p := range fixturePaths(t) {
		cur, next := loadFixture(t, p)
		_, blobs, err := Process(cur, next)
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		for _, b := range blobs {
			got, err := BlobCommitment(b.Namespace, b.Data, b.ShareVersion, b.Signer)
			if err != nil {
				t.Fatalf("%s: %v", p, err)
			}
			if !bytes.Equal(got, b.Commitment) {
				t.Fatalf("%s: commitment %x, want %x", p, got, b.Commitment)
			}
			n++
		}
	}
	if n == 0 {
		t.Fatal("no blobs in fixtures")
	}
	if _, err := BlobCommitment(make([]byte, 29), []byte("x"), 300, nil); err == nil {
		t.Fatal("share version 300 accepted")
	}
}

// The -fibre fixture must really carry a MsgPayForFibre next to BlobTxs, so
// TestProcessFixtures covers a square with a fibre system blob.
func TestFibreFixtureHasPayForFibre(t *testing.T) {
	cur, _ := loadFixture(t, filepath.Join(fixtureDir, "mocha-2035911-fibre.json"))
	fibre, blobTxs := 0, 0
	for _, tx := range cur.Txs {
		if _, ok, _ := blobtx.UnmarshalBlobTx(tx); ok {
			blobTxs++
			continue
		}
		if _, ok, err := fibretypes.TryParseFibreTx(tx); ok && err == nil {
			fibre++
		}
	}
	if fibre == 0 || blobTxs == 0 {
		t.Fatalf("fixture has %d PayForFibre and %d BlobTx", fibre, blobTxs)
	}
}

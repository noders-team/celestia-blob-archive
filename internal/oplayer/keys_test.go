package oplayer_test

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"golang.org/x/crypto/sha3"
	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/archivetest"
	"github.com/noders-team/celestia-blob-archive/internal/oplayer"
	"github.com/noders-team/celestia-blob-archive/internal/process"
	"github.com/noders-team/celestia-blob-archive/internal/square"
)

// The compact commitment from op-alt-da's fallback/s3/s3_test.go (439e788):
// 0x01 0x0c ‖ height 11334562 u64 LE ‖ commitment.
const (
	opAltDACommitment = "010ca2f3ac00000000007b156d9cdc8698d364787f4ae8a8e18346e55756533f6a4562a199b13d60548e"
	opAltDANamespace  = "00000000000000000000000000000000000000ca1de12ac5a629c3c42f"
	// keccak256 of opAltDACommitment's 42 bytes, computed once with
	// sha3.NewLegacyKeccak256.
	opAltDAKeyHash = "47df61f28c24d5cd376ad7de3c2fcd35098c45add9e283b1a162c449585f85b4"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func vectorID(t *testing.T) oplayer.BlobID {
	id := oplayer.BlobID{Height: 11334562}
	copy(id.Commitment[:], mustHex(t, opAltDACommitment)[10:])
	return id
}

func keccakHex(b []byte) string {
	h := sha3.NewLegacyKeccak256()
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

func TestOPKeyVectors(t *testing.T) {
	// The hash is the legacy Keccak, not SHA3-256.
	if got := keccakHex(nil); got != "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470" {
		t.Fatalf("keccak256(\"\") = %s", got)
	}

	// (a) compact ID and commitment bytes.
	id := vectorID(t)
	compact := id.Compact()
	if len(compact) != 40 {
		t.Fatalf("compact ID is %d bytes", len(compact))
	}
	cm := oplayer.Commitment(compact)
	if want := mustHex(t, opAltDACommitment); !bytes.Equal(cm, want) {
		t.Fatalf("commitment %x, want %x", cm, want)
	}

	// (b) the key under the vector's namespace.
	var ns [29]byte
	copy(ns[:], mustHex(t, opAltDANamespace))
	if got, want := oplayer.Key(ns, cm), "op/"+opAltDANamespace+"/"+opAltDAKeyHash; got != want {
		t.Errorf("key %s, want %s", got, want)
	}

	// (c) the full ID appends offset and size, both u32 LE.
	id.ShareOffset, id.ShareSize = 0x01020304, 5
	full := id.Full()
	if len(full) != 48 || !bytes.Equal(full[:40], compact) {
		t.Fatalf("full ID %x does not extend compact %x", full, compact)
	}
	if want := []byte{0x04, 0x03, 0x02, 0x01, 0x05, 0, 0, 0}; !bytes.Equal(full[40:], want) {
		t.Errorf("full ID tail %x, want %x", full[40:], want)
	}

	// (d) a real blob: offset is its EDS index, size its share count.
	blocks := archivetest.Blocks(t, 2, map[int64]string{1: "mocha-1834013"})
	hr, blobs, err := process.Process(blocks[1], blocks[2])
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) < 3 {
		t.Fatalf("fixture has %d blobs, want at least 3", len(blobs))
	}
	rec := blobs[2]
	b, err := oplayer.FromRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	rid, err := b.ID()
	if err != nil {
		t.Fatal(err)
	}
	eds, err := square.EDSIndex(rec.ShareStart, hr.SquareSize)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(rid.ShareOffset) != eds || rid.ShareSize != rec.ShareEnd-rec.ShareStart {
		t.Errorf("offset %d size %d; want EDS index %d, size %d", rid.ShareOffset, rid.ShareSize, eds, rec.ShareEnd-rec.ShareStart)
	}
	// Blob 2 starts in row 0, where EDS and ODS indexes agree; a blob below
	// row 0 tells them apart.
	below := 0
	for i, r := range blobs {
		if uint64(r.ShareStart) < hr.SquareSize {
			continue
		}
		below++
		b, err := oplayer.FromRecord(r)
		if err != nil {
			t.Fatal(err)
		}
		id, err := b.ID()
		if err != nil {
			t.Fatal(err)
		}
		w := hr.SquareSize
		if want := uint64(r.ShareStart)/w*2*w + uint64(r.ShareStart)%w; uint64(id.ShareOffset) != want || want == uint64(r.ShareStart) {
			t.Errorf("blob %d at ODS share %d: offset %d, want EDS index %d", i, r.ShareStart, id.ShareOffset, want)
		}
	}
	if below == 0 {
		t.Fatal("no blob below row 0 in the fixture")
	}
	if rid.Height != rec.Height || !bytes.Equal(rid.Commitment[:], rec.Commitment) {
		t.Errorf("ID %d/%x, record %d/%x", rid.Height, rid.Commitment, rec.Height, rec.Commitment)
	}
}

func TestKeyIsKeccakOfPrefixedID(t *testing.T) {
	id := vectorID(t)
	var ns [29]byte
	copy(ns[:], mustHex(t, opAltDANamespace))
	key := oplayer.Key(ns, oplayer.Commitment(id.Compact()))
	hash := key[strings.LastIndexByte(key, '/')+1:]
	if hash == keccakHex(id.Compact()) {
		t.Error("key is the keccak of the bare ID")
	}
	if hash != strings.ToLower(hash) || strings.HasPrefix(hash, "0x") || len(hash) != 64 {
		t.Errorf("key hash %q is not 64 lowercase hex digits", hash)
	}
	if !strings.HasPrefix(key, "op/"+opAltDANamespace+"/") {
		t.Errorf("key %s", key)
	}
}

func TestKeysCompactThenFull(t *testing.T) {
	blocks := archivetest.Blocks(t, 2, map[int64]string{1: "mocha-1834013"})
	hr, blobs, err := process.Process(blocks[1], blocks[2])
	if err != nil {
		t.Fatal(err)
	}
	for i, rec := range blobs {
		b, err := oplayer.FromRecord(rec)
		if err != nil {
			t.Fatal(err)
		}
		keys, err := b.Keys()
		if err != nil {
			t.Fatal(err)
		}
		id, err := b.ID()
		if err != nil {
			t.Fatal(err)
		}
		if keys[0] != oplayer.Key(b.Namespace, oplayer.Commitment(id.Compact())) ||
			keys[1] != oplayer.Key(b.Namespace, oplayer.Commitment(id.Full())) || keys[0] == keys[1] {
			t.Errorf("blob %d: keys %v", i, keys)
		}
		// A takedown derives the same keys from the tombstone.
		tb, err := oplayer.FromTombstone(tombstoneOf(rec), hr.SquareSize)
		if err != nil {
			t.Fatal(err)
		}
		if tb != b {
			t.Errorf("blob %d: tombstone %+v, record %+v", i, tb, b)
		}
	}
}

func tombstoneOf(r *archivev1.BlobRecord) *archivev1.TombstoneRecord {
	return &archivev1.TombstoneRecord{
		Height: r.Height, Namespace: r.Namespace, Commitment: r.Commitment,
		ShareStart: r.ShareStart, ShareEnd: r.ShareEnd,
	}
}

func TestFromTombstoneRejectsMalformed(t *testing.T) {
	blocks := archivetest.Blocks(t, 2, map[int64]string{1: "mocha-1834013"})
	hr, blobs, err := process.Process(blocks[1], blocks[2])
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		mut func(*archivev1.TombstoneRecord)
		w   uint64
	}{
		"short namespace":  {func(r *archivev1.TombstoneRecord) { r.Namespace = r.Namespace[1:] }, hr.SquareSize},
		"long namespace":   {func(r *archivev1.TombstoneRecord) { r.Namespace = append(r.Namespace, 0) }, hr.SquareSize},
		"short commitment": {func(r *archivev1.TombstoneRecord) { r.Commitment = r.Commitment[1:] }, hr.SquareSize},
		"no commitment":    {func(r *archivev1.TombstoneRecord) { r.Commitment = nil }, hr.SquareSize},
		"zero square":      {func(*archivev1.TombstoneRecord) {}, 0},
		"square not 2^n":   {func(*archivev1.TombstoneRecord) {}, hr.SquareSize + 1},
		"empty range":      {func(r *archivev1.TombstoneRecord) { r.ShareEnd = r.ShareStart }, hr.SquareSize},
		"range off square": {func(r *archivev1.TombstoneRecord) { r.ShareEnd = uint32(hr.SquareSize * hr.SquareSize * 2) }, hr.SquareSize},
	} {
		ts := tombstoneOf(blobs[0])
		ts.Namespace = bytes.Clone(ts.Namespace)
		c.mut(ts)
		if b, err := oplayer.FromTombstone(ts, c.w); err == nil {
			t.Errorf("%s: %+v, want an error", name, b)
		}
	}
}

func TestBlobIDRejectsBadRange(t *testing.T) {
	for _, b := range []oplayer.Blob{
		{ShareStart: 3, ShareEnd: 3, SquareSize: 4},
		{ShareStart: 4, ShareEnd: 3, SquareSize: 4},
		{ShareStart: 0, ShareEnd: 1, SquareSize: 3},
		{ShareStart: 16, ShareEnd: 17, SquareSize: 4},
		{ShareStart: 15, ShareEnd: 17, SquareSize: 4},
	} {
		if id, err := b.ID(); err == nil {
			t.Errorf("%+v: ID %+v, want an error", b, id)
		}
		if _, err := b.Keys(); err == nil {
			t.Errorf("%+v: Keys without an error", b)
		}
	}
}

func TestFromRecordRejectsMalformed(t *testing.T) {
	blocks := archivetest.Blocks(t, 2, map[int64]string{1: "mocha-1834013"})
	_, blobs, err := process.Process(blocks[1], blocks[2])
	if err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*archivev1.BlobRecord){
		"short namespace":  func(r *archivev1.BlobRecord) { r.Namespace = r.Namespace[1:] },
		"short commitment": func(r *archivev1.BlobRecord) { r.Commitment = r.Commitment[1:] },
		"bad proof":        func(r *archivev1.BlobRecord) { r.ShareProof = []byte{0xff} },
		"no proof":         func(r *archivev1.BlobRecord) { r.ShareProof = nil },
	} {
		r := proto.Clone(blobs[0]).(*archivev1.BlobRecord)
		mut(r)
		if _, err := oplayer.FromRecord(r); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestParseNamespaces(t *testing.T) {
	const (
		a  = opAltDANamespace
		b  = "000000000000000000000000000000000000000000000000000000beef"
		tx = "0000000000000000000000000000000000000000000000000000000001"
	)
	got, err := oplayer.ParseNamespaces(" " + a + ", " + strings.ToUpper(b) + " ")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || hex.EncodeToString(got[0][:]) != a || hex.EncodeToString(got[1][:]) != b {
		t.Errorf("parsed %x", got)
	}
	if got, err := oplayer.ParseNamespaces(""); err != nil || len(got) != 0 {
		t.Errorf("empty: %x, %v", got, err)
	}
	for name, csv := range map[string]string{
		"duplicate":      a + "," + a,
		"duplicate case": b + "," + strings.ToUpper(b),
		"reserved tx":    tx,
		"28 bytes":       a[2:],
		"30 bytes":       a + "00",
		"bad hex":        "zz" + a[2:],
		"0x prefix":      "0x" + a,
		"empty element":  a + ",," + b,
	} {
		if got, err := oplayer.ParseNamespaces(csv); err == nil {
			t.Errorf("%s: parsed %x, want an error", name, got)
		}
	}
}

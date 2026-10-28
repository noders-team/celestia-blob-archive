package segment_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
)

func tombstoneBlob() *archivev1.BlobRecord {
	return &archivev1.BlobRecord{
		Height: 1834013, TxIndex: 3, BlobIndex: 1, Namespace: bytes.Repeat([]byte{7}, 29), ShareVersion: 1,
		Data: []byte("withdrawn"), Commitment: bytes.Repeat([]byte{9}, 32), Signer: []byte("signer"),
		ShareStart: 70, ShareEnd: 200, ShareProof: []byte("proof"),
	}
}

// minTombstone is the length of the tombstone for b with no padding or filler.
func minTombstone(b *archivev1.BlobRecord) int {
	return proto.Size(&archivev1.Record{Kind: &archivev1.Record_Tombstone{Tombstone: &archivev1.TombstoneRecord{
		Height: b.Height, Namespace: b.Namespace, Commitment: b.Commitment, ShareStart: b.ShareStart, ShareEnd: b.ShareEnd,
	}}})
}

func TestTombstoneExactLength(t *testing.T) {
	b := tombstoneBlob()
	min := minTombstone(b)
	lengths := []int{min}
	for l := min + 2; l <= min+400; l++ {
		lengths = append(lengths, l)
	}
	// Lengths where a length varint (the Record's prefix for the tombstone,
	// or the padding's) grows a byte.
	lengths = append(lengths, 127, 128, 129, 130, 131, 133)
	for l := 16380; l <= 16391; l++ {
		lengths = append(lengths, l)
	}
	lengths = append(lengths, 2097150, 2097155, 2097156, 2097157, 1<<20)
	for _, l := range lengths {
		if l < min+2 && l != min {
			continue
		}
		out, err := segment.TombstoneFor(b, uint32(l))
		if err != nil {
			t.Fatalf("L=%d: %v", l, err)
		}
		if len(out) != l {
			t.Fatalf("L=%d: got %d bytes", l, len(out))
		}
		var r archivev1.Record
		if err := proto.Unmarshal(out, &r); err != nil {
			t.Fatalf("L=%d: decode: %v", l, err)
		}
		ts := r.GetTombstone()
		if ts == nil {
			t.Fatalf("L=%d: record kind %T, want tombstone", l, r.Kind)
		}
		if ts.Height != b.Height || !bytes.Equal(ts.Namespace, b.Namespace) || !bytes.Equal(ts.Commitment, b.Commitment) ||
			ts.ShareStart != b.ShareStart || ts.ShareEnd != b.ShareEnd {
			t.Fatalf("L=%d: tombstone %v does not match blob", l, ts)
		}
		canon, err := proto.MarshalOptions{Deterministic: true}.Marshal(&r)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(canon, out) {
			t.Fatalf("L=%d: not the canonical encoding (re-encodes to %d bytes)", l, len(canon))
		}
		if len(ts.Padding) > 0 && !bytes.Equal(ts.Padding, make([]byte, len(ts.Padding))) {
			t.Fatalf("L=%d: padding is not zero", l)
		}
		if bytes.Contains(out, b.Data) || bytes.Contains(out, b.ShareProof) || bytes.Contains(out, b.Signer) {
			t.Fatalf("L=%d: tombstone carries blob payload", l)
		}
	}
}

// Every length from min+2 past the 16384 varint boundaries is reachable.
func TestTombstoneEveryLength(t *testing.T) {
	b := tombstoneBlob()
	for l := minTombstone(b) + 2; l <= 17000; l++ {
		out, err := segment.TombstoneFor(b, uint32(l))
		if err != nil || len(out) != l {
			t.Fatalf("L=%d: %d bytes, %v", l, len(out), err)
		}
	}
}

func TestTombstoneDeterministic(t *testing.T) {
	for _, l := range []uint32{130, 500, 16387} {
		a, err := segment.TombstoneFor(tombstoneBlob(), l)
		if err != nil {
			t.Fatal(err)
		}
		other := tombstoneBlob()
		other.Data, other.TxIndex, other.ShareProof = []byte("other"), 9, nil
		b, err := segment.TombstoneFor(other, l)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("L=%d: tombstones differ", l)
		}
	}
}

// Every added field costs at least two bytes, so min+1 has no canonical
// encoding.
func TestTombstoneUnreachable(t *testing.T) {
	b := tombstoneBlob()
	if _, err := segment.TombstoneFor(b, uint32(minTombstone(b)+1)); !errors.Is(err, segment.ErrTombstoneUnreachable) {
		t.Fatalf("err = %v, want ErrTombstoneUnreachable", err)
	}
}

// Record.filler is used only where padding alone cannot hit the length.
func TestTombstoneFillerOnlyWhenNeeded(t *testing.T) {
	b := tombstoneBlob()
	for _, c := range []struct {
		length int
		filler bool
	}{{minTombstone(b) + 3, false}, {500, false}, {130, true}, {16387, true}} {
		out, err := segment.TombstoneFor(b, uint32(c.length))
		if err != nil {
			t.Fatal(err)
		}
		var r archivev1.Record
		if err := proto.Unmarshal(out, &r); err != nil {
			t.Fatal(err)
		}
		if (r.Filler != 0) != c.filler {
			t.Fatalf("L=%d: filler %d, want set=%v", c.length, r.Filler, c.filler)
		}
	}
}

// TestTombstoneBytes pins the canonical bytes documented in docs/format.md:
// protobuf-go marshals oneof fields last, so Record.filler (tag 0x78) comes
// before Record.tombstone (tag 0x1a); inside TombstoneRecord the fields go in
// number order, padding (tag 0x7a) last.
func TestTombstoneBytes(t *testing.T) {
	// TombstoneRecord fields 1..5 of tombstoneBlob, 74 bytes.
	const fields = "089df86f" + // height 1834013
		"121d" + "0707070707070707070707070707070707070707070707070707070707" + // namespace
		"1a20" + "0909090909090909090909090909090909090909090909090909090909090909" + // commitment
		"2046" + "28c801" // share_start 70, share_end 200
	for _, c := range []struct {
		length int
		want   string
	}{
		{78, "7801" + "1a4a" + fields},   // min+2: filler 1 first, no padding
		{79, "1a4d" + fields + "7a0100"}, // min+3: one byte of padding, no filler
	} {
		out, err := segment.TombstoneFor(tombstoneBlob(), uint32(c.length))
		if err != nil {
			t.Fatal(err)
		}
		if got := hex.EncodeToString(out); got != c.want {
			t.Errorf("L=%d:\n got %s\nwant %s", c.length, got, c.want)
		}
	}
}

func TestTombstoneTooSmall(t *testing.T) {
	if _, err := segment.TombstoneFor(tombstoneBlob(), 10); !errors.Is(err, segment.ErrTombstoneTooLarge) {
		t.Fatalf("err = %v, want ErrTombstoneTooLarge", err)
	}
}

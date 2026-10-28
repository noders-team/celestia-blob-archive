package segment_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
)

func testRecords() []*archivev1.Record {
	return []*archivev1.Record{
		{Kind: &archivev1.Record_Height{Height: &archivev1.HeightRecord{
			Height: 100, Time: timestamppb.New(time.Unix(1700000000, 5)), AppVersion: 4,
			DataHash: []byte{1, 2, 3}, SquareSize: 8, BlobCount: 2, Header: []byte("hdr"), Commit: []byte("cmt"),
		}}},
		{Kind: &archivev1.Record_Blob{Blob: &archivev1.BlobRecord{
			Height: 100, TxIndex: 1, BlobIndex: 0, Namespace: make([]byte, 29), Data: make([]byte, 300),
			Commitment: make([]byte, 32), ShareStart: 4, ShareEnd: 6, ShareProof: []byte("proof"),
		}}},
		{Kind: &archivev1.Record_Blob{Blob: &archivev1.BlobRecord{
			Height: 100, TxIndex: 1, BlobIndex: 1, Namespace: make([]byte, 29), Data: []byte("x"),
			Commitment: make([]byte, 32), Signer: []byte("signer"), ShareStart: 6, ShareEnd: 7,
		}}},
	}
}

func TestWriterRoundTrip(t *testing.T) {
	w := &segment.Writer{}
	type loc struct {
		off uint64
		n   uint32
	}
	var locs []loc
	recs := testRecords()
	for _, r := range recs {
		off, n, err := w.Append(r)
		if err != nil {
			t.Fatal(err)
		}
		locs = append(locs, loc{off, n})
	}
	if w.Size() != len(w.Bytes()) {
		t.Fatalf("Size %d != len(Bytes) %d", w.Size(), len(w.Bytes()))
	}
	for i, r := range recs {
		got, err := segment.ReadAt(w.Bytes(), locs[i].off, locs[i].n)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(got, r) {
			t.Fatalf("record %d mismatch: %v != %v", i, got, r)
		}
	}
}

func TestReadAtBadInput(t *testing.T) {
	w := &segment.Writer{}
	off, n, err := w.Append(testRecords()[0])
	if err != nil {
		t.Fatal(err)
	}
	seg := w.Bytes()
	cases := map[string]struct {
		off uint64
		n   uint32
	}{
		"offset beyond":   {uint64(len(seg)) + 10, n},
		"length beyond":   {off, n + 1000},
		"offset overflow": {^uint64(0), 10},
		"max length":      {off, ^uint32(0)},
	}
	for name, c := range cases {
		if _, err := segment.ReadAt(seg, c.off, c.n); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := segment.ReadAt([]byte{0xff, 0xff, 0xff}, 0, 3); err == nil {
		t.Error("garbage: expected decode error")
	}
}

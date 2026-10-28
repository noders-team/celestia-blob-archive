package segment_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	archivev1 "github.com/noders-team/celestia-blob-archive/gen/archive/v1"
	"github.com/noders-team/celestia-blob-archive/internal/segment"
)

func TestIndexRoundTrip(t *testing.T) {
	idx := &archivev1.SegmentIndex{
		FirstHeight: 100, LastHeight: 101,
		Entries: []*archivev1.IndexEntry{
			{Height: 100, Offset: 1, Length: 50, Kind: 1},
			{Height: 100, Namespace: make([]byte, 29), Commitment: make([]byte, 32), Offset: 53, Length: 400, Kind: 2},
			{Height: 101, Namespace: make([]byte, 29), Commitment: make([]byte, 32), Offset: 455, Length: 9, Kind: 2},
		},
	}
	b, err := segment.EncodeIndex(idx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := segment.DecodeIndex(b)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, idx) {
		t.Fatalf("mismatch: %v != %v", got, idx)
	}
	if _, err := segment.DecodeIndex([]byte{0xff, 0xff}); err == nil {
		t.Error("expected error on garbage")
	}
}

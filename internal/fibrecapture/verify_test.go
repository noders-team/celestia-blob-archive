package fibrecapture

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"

	"github.com/celestiaorg/celestia-app/v10/fibre"
)

// testBlob returns n random bytes with their Fibre blob ID and upload size.
func testBlob(t *testing.T, n int) (data []byte, id [33]byte, blobSize uint32) {
	t.Helper()
	data = make([]byte, n)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	cfg, err := fibre.BlobConfigForVersion(0)
	if err != nil {
		t.Fatal(err)
	}
	b, err := fibre.NewBlob(bytes.Clone(data), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Free()
	copy(id[:], b.ID())
	return data, id, uint32(cfg.UploadSize(len(data)))
}

func TestVerifyPayload(t *testing.T) {
	data, id, size := testBlob(t, 1000)
	if err := VerifyPayload(data, id, size); err != nil {
		t.Fatalf("matching payload: %v", err)
	}

	flipped := bytes.Clone(data)
	flipped[500] ^= 0xff
	if err := VerifyPayload(flipped, id, size); !errors.Is(err, ErrMismatch) {
		t.Errorf("flipped byte: %v, want ErrMismatch", err)
	}
	if err := VerifyPayload(data, id, size+4096); !errors.Is(err, ErrMismatch) {
		t.Errorf("wrong blob size: %v, want ErrMismatch", err)
	}
	v1 := id
	v1[0] = 1
	if err := VerifyPayload(data, v1, size); err == nil {
		t.Error("blob version 1 accepted")
	}
	if err := VerifyPayload(nil, id, size); err == nil {
		t.Error("empty payload accepted")
	}
}

func TestVerifyPayloadKeepsData(t *testing.T) {
	data, id, size := testBlob(t, 1000)
	want := bytes.Clone(data)
	if err := VerifyPayload(data, id, size); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, want) {
		t.Error("VerifyPayload modified the caller's bytes")
	}
}
